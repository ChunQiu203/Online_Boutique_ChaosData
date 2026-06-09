#!/usr/bin/env bash
set -uo pipefail

config=""
run_id=""
protocol="http"
host="127.0.0.1"
port="8080"
base_path=""
skip_environment_check="false"
extra_jmeter_props=()

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/run-test.sh --config ./config/baseline-30.properties --run-id OB-BASELINE-30U-R01 [options]

Options:
  --protocol VALUE
  --host VALUE
  --port VALUE
  --base-path VALUE
  --jmeter-property key=value
  -Jkey=value
  --skip-environment-check
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) config="${2:-}"; shift 2 ;;
    --run-id) run_id="${2:-}"; shift 2 ;;
    --protocol) protocol="${2:-}"; shift 2 ;;
    --host) host="${2:-}"; shift 2 ;;
    --port) port="${2:-}"; shift 2 ;;
    --base-path) base_path="${2:-}"; shift 2 ;;
    --jmeter-property)
      if [[ "${2:-}" != *=* ]]; then
        echo "--jmeter-property expects key=value" >&2
        exit 2
      fi
      extra_jmeter_props+=("-J${2}")
      shift 2
      ;;
    -J*)
      prop="${1#-J}"
      if [[ "$prop" != *=* ]]; then
        echo "-J expects key=value" >&2
        exit 2
      fi
      extra_jmeter_props+=("$1")
      shift
      ;;
    --skip-environment-check) skip_environment_check="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$config" || -z "$run_id" ]]; then
  usage >&2
  exit 2
fi

if [[ ! "$run_id" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Run ID may contain only letters, numbers, dot, underscore and hyphen." >&2
  exit 2
fi

if ! command -v jmeter >/dev/null 2>&1; then
  echo "Apache JMeter is not available in PATH. Install JMeter 5.6.3+ and retry." >&2
  exit 2
fi

if ! command -v python >/dev/null 2>&1; then
  echo "Python is not available in PATH. Python 3.10+ is required." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
base_dir="$(cd "$script_dir/.." && pwd)"
original_dir="$(pwd)"

resolve_input_path() {
  local value="$1"
  if [[ "$value" = /* || "$value" =~ ^[A-Za-z]:[\\/] ]]; then
    [[ -f "$value" ]] || return 1
    cd "$(dirname "$value")" && printf '%s/%s\n' "$(pwd)" "$(basename "$value")"
    return
  fi
  if [[ -f "$original_dir/$value" ]]; then
    cd "$(dirname "$original_dir/$value")" && printf '%s/%s\n' "$(pwd)" "$(basename "$value")"
    return
  fi
  if [[ -f "$base_dir/$value" ]]; then
    cd "$(dirname "$base_dir/$value")" && printf '%s/%s\n' "$(pwd)" "$(basename "$value")"
    return
  fi
  return 1
}

config_path="$(resolve_input_path "$config")" || {
  echo "Config file not found: $config" >&2
  exit 2
}

jmx_path="$base_dir/online-boutique.jmx"
summarizer_path="$base_dir/tools/summarize_results.py"

[[ -f "$jmx_path" ]] || { echo "JMX not found: $jmx_path" >&2; exit 2; }
[[ -f "$summarizer_path" ]] || { echo "Summarizer not found: $summarizer_path" >&2; exit 2; }

experiment_dir="$base_dir/experiments/$run_id"
if [[ -e "$experiment_dir" ]]; then
  echo "Experiment directory already exists; refusing to overwrite Run ID '$run_id': $experiment_dir" >&2
  exit 2
fi

jmeter_dir="$experiment_dir/jmeter"
mkdir -p "$jmeter_dir" "$experiment_dir/monitoring/prometheus" "$experiment_dir/monitoring/grafana" "$experiment_dir/chaos"

cd "$base_dir" || exit 2

if [[ "$skip_environment_check" != "true" ]]; then
  "$base_dir/scripts/check-environment.sh" --protocol "$protocol" --host "$host" --port "$port" --base-path "$base_path"
  env_exit=$?
  if [[ "$env_exit" -ne 0 ]]; then
    echo "Environment check failed; JMeter run was not started." >&2
    exit "$env_exit"
  fi
fi

get_prop() {
  local key="$1"
  local default_value="$2"
  local line
  line="$(awk -F= -v k="$key" 'BEGIN{v=""} /^[[:space:]]*[#!]/ {next} $1 ~ "^[[:space:]]*" k "[[:space:]]*$" {sub(/^[^=]*=/,""); v=$0} END{print v}' "$config_path")"
  if [[ -n "$line" ]]; then
    echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
  else
    echo "$default_value"
  fi
}

csv_quote() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

try_text() {
  "$@" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]*$//' || true
}

start_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
jtl_path="$jmeter_dir/result.jtl"
jmeter_log_path="$jmeter_dir/jmeter.log"
report_dir="$jmeter_dir/report"
manifest_path="$experiment_dir/manifest.csv"
events_path="$experiment_dir/events.csv"
exit_code_path="$jmeter_dir/exit-code.txt"

"$base_dir/scripts/mark-event.sh" --run-id "$run_id" --event TEST_START --details "JMeter run started"

repo_url="$(try_text git remote get-url origin)"
repo_commit="$(try_text git rev-parse HEAD)"
kube_context="$(try_text kubectl config current-context)"
minikube_profile="$(try_text minikube profile)"
frontend_image="$(try_text kubectl get deployment frontend -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")"
review_image="$(try_text kubectl get deployment reviewservice -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")"
jmeter_version="$(try_text jmeter --version)"
java_version="$(try_text java -version)"

{
  printf 'run_id,system_repository,system_commit,test_package_commit,kubernetes_context,minikube_profile,deployment_manifest,frontend_image,reviewservice_image,scenario,start_utc,end_utc,users,rampup_s,duration_s,host,port,checkout_percent,currency_percent,review_write_percent,target_service,fault_type,fault_parameters,operator,notes,jmeter_version,java_version\n'
  csv_quote "$run_id"; printf ','
  csv_quote "$repo_url"; printf ','
  csv_quote "$repo_commit"; printf ','
  csv_quote "$repo_commit"; printf ','
  csv_quote "$kube_context"; printf ','
  csv_quote "$minikube_profile"; printf ','
  csv_quote "deploy-all.yaml"; printf ','
  csv_quote "$frontend_image"; printf ','
  csv_quote "$review_image"; printf ','
  csv_quote "$(get_prop scenario shopping)"; printf ','
  csv_quote "$start_utc"; printf ','
  csv_quote ""; printf ','
  csv_quote "$(get_prop users 1)"; printf ','
  csv_quote "$(get_prop rampup 1)"; printf ','
  csv_quote "$(get_prop duration 60)"; printf ','
  csv_quote "$host"; printf ','
  csv_quote "$port"; printf ','
  csv_quote "$(get_prop checkout_percent 30)"; printf ','
  csv_quote "$(get_prop currency_percent 20)"; printf ','
  csv_quote "$(get_prop review_write_percent 10)"; printf ','
  csv_quote ""; printf ','
  csv_quote ""; printf ','
  csv_quote ""; printf ','
  csv_quote "${USERNAME:-${USER:-}}"; printf ','
  csv_quote ""; printf ','
  csv_quote "$jmeter_version"; printf ','
  csv_quote "$java_version"; printf '\n'
} > "$manifest_path"

jmeter_args=(
  -n
  -t "$jmx_path"
  -q "$config_path"
  "-Jjmeter_base_dir=$base_dir"
  "-Jrun_id=$run_id"
  "-Jprotocol=$protocol"
  "-Jhost=$host"
  "-Jport=$port"
  "-Jbase_path=$base_path"
  -l "$jtl_path"
  -j "$jmeter_log_path"
  -e
  -o "$report_dir"
)

for prop in "${extra_jmeter_props[@]}"; do
  jmeter_args+=("$prop")
done

jmeter "${jmeter_args[@]}"
jmeter_exit=$?

end_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
"$base_dir/scripts/mark-event.sh" --run-id "$run_id" --event TEST_END --details "JMeter exit code $jmeter_exit"
python - "$manifest_path" "$end_utc" <<'PY'
import csv
import sys
path, end_utc = sys.argv[1], sys.argv[2]
with open(path, newline='', encoding='utf-8-sig') as f:
    rows = list(csv.DictReader(f))
fields = rows[0].keys() if rows else []
for row in rows:
    row['end_utc'] = end_utc
with open(path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
PY

{
  echo "jmeter_exit_code=$jmeter_exit"
} > "$exit_code_path"

summary_exit=0
if [[ -f "$jtl_path" ]]; then
  python "$summarizer_path" --jtl "$jtl_path" --events "$events_path" --output-dir "$jmeter_dir"
  summary_exit=$?
else
  echo "WARNING: JTL was not created; summary was skipped." >&2
  summary_exit=1
fi
echo "summary_exit_code=$summary_exit" >> "$exit_code_path"

cd "$original_dir" || true

if [[ "$jmeter_exit" -ne 0 ]]; then
  exit "$jmeter_exit"
fi
exit "$summary_exit"
