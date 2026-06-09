#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/mark-event.sh --run-id RUN_ID --event EVENT [--details TEXT]

Events:
  TEST_START WARMUP_END FAULT_START FAULT_END TEST_END NOTE
USAGE
}

run_id=""
event=""
details=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      run_id="${2:-}"
      shift 2
      ;;
    --event)
      event="${2:-}"
      shift 2
      ;;
    --details)
      details="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$event" in
  TEST_START|WARMUP_END|FAULT_START|FAULT_END|TEST_END|NOTE) ;;
  *)
    echo "Invalid or missing --event" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ -z "$run_id" ]]; then
  echo "Missing --run-id" >&2
  usage >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
base_dir="$(cd "$script_dir/.." && pwd)"
experiment_dir="$base_dir/experiments/$run_id"
events_path="$experiment_dir/events.csv"

mkdir -p "$experiment_dir"

if [[ ! -f "$events_path" ]]; then
  printf 'run_id,event,timestamp_utc,details\n' > "$events_path"
fi

csv_quote() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
{
  csv_quote "$run_id"
  printf ','
  csv_quote "$event"
  printf ','
  csv_quote "$timestamp"
  printf ','
  csv_quote "$details"
  printf '\n'
} >> "$events_path"

echo "Recorded event $event for $run_id at $timestamp"
