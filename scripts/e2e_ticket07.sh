#!/usr/bin/env bash
# 票 07 验收脚本：Admin Token + healthz + metrics
# 覆盖 .scratch/config-center-mvp/issues/07-auth-health-metrics.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e07.log
NS=dev
ID=db.password

fail=0
check() {
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}
code_of() {
  curl -s -o /tmp/gcc-resp.json -w '%{http_code}' -X "$1" "$HTTP$2" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' ${3:+-d "$3"}
}

echo "=== 构建与准备 ==="
go build -o "$BIN" ./cmd/config-center; check $? "go build"
rm -rf data
$PG "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE" >/dev/null

"$BIN" --config config.yaml --bootstrap >"$LOG" 2>&1 &
SRV=$!
for _ in $(seq 1 120); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -sf "$HTTP/healthz" >/dev/null; check $? "服务就绪"

echo "=== 验收 1：无 token / 错 token 401，正确 token 通过 ==="
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$HTTP/v1/namespaces/$NS/configs/$ID" -d '{"content":"x","data_type":"TEXT"}')
[ "$code" = "401" ]; check $? "无 token PUT → 401（实际 $code）"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$HTTP/v1/namespaces/$NS/configs/$ID" -H "Authorization: Bearer wrong-token" -d '{"content":"x","data_type":"TEXT"}')
[ "$code" = "401" ]; check $? "错 token PUT → 401（实际 $code）"
for _ in $(seq 1 100); do
  code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"v1","data_type":"TEXT"}')
  [ "$code" != "503" ] && break
  sleep 0.1
done
[ "$code" = "200" ]; check $? "正确 token PUT → 200（$code）"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$HTTP/v1/namespaces/$NS/configs/$ID/rollback" -H "Authorization: Bearer wrong" -d '{"to_version":1}')
[ "$code" = "401" ]; check $? "错 token rollback → 401（实际 $code）"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$HTTP/v1/cluster/snapshot" -H "Authorization: Bearer wrong")
[ "$code" = "401" ]; check $? "错 token snapshot → 401（实际 $code）"
# 读接口不认证
code=$(curl -s -o /dev/null -w '%{http_code}' "$HTTP/v1/namespaces/$NS/configs/$ID")
[ "$code" = "200" ]; check $? "读接口无 token → 200（实际 $code）"

echo "=== 验收 4 准备：制造四类 Apply 结果 ==="
code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"x","data_type":"TEXT","expected_version":99}' >/dev/null # version_conflict
code_of DELETE "/v1/namespaces/$NS/configs/never.exists" >/dev/null                                                # not_found
code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":1}' >/dev/null                               # invalid_rollback（= 当前版本）

echo "=== 验收 2：/healthz 反映 PG 连通（当前健康） ==="
body=$(curl -s "$HTTP/healthz")
echo "$body" | grep -q '"status":"ok"'; check $? "healthz → ok（$body）"

echo "=== 验收 3 + 2：/readyz ready ==="
body=$(curl -s "$HTTP/readyz")
echo "$body" | grep -q '"status":"ready"'; check $? "readyz → ready（$body）"

echo "=== 验收 4：/metrics 含 §7 最低集合，fsm_apply_results_total 按 result 区分 ==="
metrics=$(curl -s "$HTTP/metrics")
for name in raft_role raft_term raft_commit_index \
            fsm_last_applied_index fsm_apply_duration_seconds \
            watch_waiters watch_notifications_total \
            snapshot_duration_seconds snapshot_size_bytes restore_skipped_total \
            http_request_duration_seconds; do
  echo "$metrics" | grep -q "^# HELP $name " ; check $? "指标存在: $name"
done
for result in ok version_conflict not_found invalid_rollback; do
  echo "$metrics" | grep -q "fsm_apply_results_total{result=\"$result\"}"; check $? "fsm_apply_results_total{result=\"$result\"} 存在"
done
echo "$metrics" | grep -q 'http_request_duration_seconds_count{route="PUT /v1/namespaces/{ns}/configs/{data_id}"}'
check $? "http_request_duration_seconds 带 route 标签"

kill -9 $SRV 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
