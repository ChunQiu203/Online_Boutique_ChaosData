#!/usr/bin/env bash
set -uo pipefail

protocol="http"
host="127.0.0.1"
port="8080"
base_path=""
product_id="OLJCESPC7Z"
scale_loadgenerator="false"
failures=0

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/check-environment.sh [--protocol http] [--host 127.0.0.1] [--port 8080] [--base-path PATH] [--product-id ID] [--scale-loadgenerator]
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --protocol) protocol="${2:-}"; shift 2 ;;
    --host) host="${2:-}"; shift 2 ;;
    --port) port="${2:-}"; shift 2 ;;
    --base-path) base_path="${2:-}"; shift 2 ;;
    --product-id) product_id="${2:-}"; shift 2 ;;
    --scale-loadgenerator) scale_loadgenerator="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

fail() {
  echo "ERROR: $*" >&2
  failures=$((failures + 1))
}

run_step() {
  local name="$1"
  shift
  echo "== $name =="
  if ! "$@"; then
    fail "$name failed"
  fi
}

kubectl_value() {
  kubectl "$@" 2>/dev/null || true
}

if ! command -v kubectl >/dev/null 2>&1; then
  fail "kubectl is not available in PATH."
  exit 1
fi

run_step "kubectl config current-context" kubectl config current-context
run_step "kubectl get nodes" kubectl get nodes
run_step "kubectl get deployment frontend reviewservice" kubectl get deployment frontend reviewservice
run_step "kubectl get service frontend frontend-external reviewservice" kubectl get service frontend frontend-external reviewservice
run_step "kubectl get pods" kubectl get pods

frontend_ready="$(kubectl_value get deployment frontend -o 'jsonpath={.status.readyReplicas}/{.status.replicas}')"
review_ready="$(kubectl_value get deployment reviewservice -o 'jsonpath={.status.readyReplicas}/{.status.replicas}')"

if [[ ! "$frontend_ready" =~ ^[1-9][0-9]*/[1-9][0-9]*$ ]]; then
  fail "frontend Deployment is not Ready. Current ready/desired: '$frontend_ready'"
else
  echo "frontend Ready: $frontend_ready"
fi

if [[ ! "$review_ready" =~ ^[1-9][0-9]*/[1-9][0-9]*$ ]]; then
  fail "reviewservice Deployment is not Ready. Current ready/desired: '$review_ready'"
else
  echo "reviewservice Ready: $review_ready"
fi

frontend_image="$(kubectl_value get deployment frontend -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")"
review_image="$(kubectl_value get deployment reviewservice -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")"
review_addr="$(kubectl_value get deployment frontend -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].env[?(@.name=='REVIEW_SERVICE_ADDR')].value}")"

echo "frontend image: $frontend_image"
echo "reviewservice image: $review_image"
echo "frontend REVIEW_SERVICE_ADDR: $review_addr"

[[ "$frontend_image" == *"frontend:local"* ]] || fail "frontend image is not frontend:local. Current image: '$frontend_image'"
[[ "$review_image" == *"reviewservice:local"* ]] || fail "reviewservice image is not reviewservice:local. Current image: '$review_image'"
[[ "$review_addr" == "reviewservice:8080" ]] || fail "frontend REVIEW_SERVICE_ADDR is not reviewservice:8080. Current value: '$review_addr'"

loadgen_replicas="$(kubectl_value get deployment loadgenerator -o 'jsonpath={.spec.replicas}')"
if [[ -n "$loadgen_replicas" ]]; then
  echo "loadgenerator replicas: $loadgen_replicas"
  if [[ "$loadgen_replicas" != "0" ]]; then
    echo "WARNING: 正式 JMeter 压测前请执行: kubectl scale deployment/loadgenerator --replicas=0" >&2
    if [[ "$scale_loadgenerator" == "true" ]]; then
      run_step "Scale loadgenerator to 0" kubectl scale deployment/loadgenerator --replicas=0
    fi
  fi
else
  echo "WARNING: loadgenerator Deployment not found or not readable." >&2
fi

if ! command -v curl >/dev/null 2>&1; then
  fail "curl is not available in PATH."
else
  base_url="${protocol}://${host}:${port}${base_path}"
  echo "Base URL: $base_url"

  home_body="$(curl -fsS --max-time 10 "$base_url/" 2>/tmp/ob-jmeter-home.err)"
  if [[ $? -ne 0 ]]; then
    fail "GET / failed at $base_url/: $(cat /tmp/ob-jmeter-home.err 2>/dev/null)"
  elif [[ "$home_body" != *"Hot Products"* ]]; then
    fail "GET / did not contain stable text 'Hot Products'."
  else
    echo "GET / OK"
  fi

  health_body="$(curl -fsS --max-time 10 "$base_url/_healthz" 2>/tmp/ob-jmeter-health.err)"
  if [[ $? -ne 0 ]]; then
    fail "GET /_healthz failed at $base_url/_healthz: $(cat /tmp/ob-jmeter-health.err 2>/dev/null)"
  elif [[ "$(echo "$health_body" | tr -d '\r\n')" != "ok" ]]; then
    fail "GET /_healthz did not return body 'ok'."
  else
    echo "GET /_healthz OK"
  fi

  product_body="$(curl -fsS --max-time 15 "$base_url/product/$product_id" 2>/tmp/ob-jmeter-product.err)"
  if [[ $? -ne 0 ]]; then
    fail "GET /product/$product_id failed at $base_url/product/$product_id: $(cat /tmp/ob-jmeter-product.err 2>/dev/null)"
  else
    [[ "$product_body" == *"Customer Reviews"* ]] || fail "Product detail page did not contain 'Customer Reviews'."
    [[ "$product_body" == *"Write a Review"* ]] || fail "Product detail page did not contain 'Write a Review'."
    [[ "$product_body" == *"Add To Cart"* ]] || fail "Product detail page did not contain 'Add To Cart'."
    [[ "$product_body" != *"Something has failed"* && "$product_body" != *"Uh, oh!"* ]] || fail "Product detail page appears to be the error template."
    echo "GET /product/$product_id checked"
  fi
fi

if [[ "$failures" -gt 0 ]]; then
  echo "$failures environment check(s) failed." >&2
  exit 1
fi

echo "Environment check passed."
exit 0
