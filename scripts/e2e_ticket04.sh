#!/usr/bin/env bash
# 票 04 验收脚本：Rollback
# 覆盖 .scratch/config-center-mvp/issues/04-rollback.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e04.log
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

echo "=== 准备数据：v1/v2/v3 ==="
for _ in $(seq 1 100); do
  code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"content-v1","data_type":"TEXT","tags":{"rev":"1"}}')
  [ "$code" != "503" ] && break
  sleep 0.1
done
[ "$code" = "200" ]; check $? "v1 发布"
code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"content-v2","data_type":"TEXT"}' >/dev/null
code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"content-v3","data_type":"TEXT"}' >/dev/null

echo "=== 验收 1：rollback to v1 → v4，内容与 v1 相同，History 追加 publish ==="
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$HTTP/v1/namespaces/$NS/configs/$ID/rollback" -d '{"to_version":1}')
[ "$code" = "401" ]; check $? "无 token 回滚 → 401（实际 $code）"
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":1}')
[ "$code" = "200" ] && grep -q '"version":4' /tmp/gcc-resp.json
check $? "rollback to v1 → 200 v4（$code $(cat /tmp/gcc-resp.json)）"
body=$(curl -s "$HTTP/v1/namespaces/$NS/configs/$ID")
echo "$body" | grep -q '"content":"content-v1"' && echo "$body" | grep -q '"version":4'
check $? "GET 内容回到 v1 且 version=4"
op=$($PG "SELECT op FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID' AND version=4")
[ "$op" = "publish" ]; check $? "History v4 为 publish 记录（实际 $op）"

echo "=== 验收 2：链式回滚 ==="
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":2}')
[ "$code" = "200" ] && grep -q '"version":5' /tmp/gcc-resp.json
check $? "回滚后的条目可继续回滚到 v2 → 200 v5（$code）"
body=$(curl -s "$HTTP/v1/namespaces/$NS/configs/$ID")
echo "$body" | grep -q '"content":"content-v2"'; check $? "GET 内容为 v2"

echo "=== 验收 3：无效目标 404/409 ==="
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":99}')
[ "$code" = "404" ]; check $? "to_version=99 不存在 → 404（实际 $code）"
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":5}')
[ "$code" = "409" ] && grep -q '"error":"invalid_rollback"' /tmp/gcc-resp.json
check $? "to_version=5（≥ 当前版本）→ 409 invalid_rollback（$code）"
code=$(code_of DELETE "/v1/namespaces/$NS/configs/$ID")
[ "$code" = "200" ] && grep -q '"version":6' /tmp/gcc-resp.json
check $? "删除 → v6 墓碑（$code）"
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":6}')
[ "$code" = "409" ]; check $? "to_version=6 指向墓碑 → 409（实际 $code）"
code=$(code_of POST "/v1/namespaces/$NS/configs/$ID/rollback" '{"to_version":3}')
[ "$code" = "200" ] && grep -q '"version":7' /tmp/gcc-resp.json
check $? "删除后回滚到墓碑之前的 v3 → 重建为 v7（$code）"

echo "=== 验收 4：并发 Publish 与 Rollback 竞态，终态与日志顺序一致（无 TOCTOU） ==="
pids=""
for i in 1 2 3 4 5 6 7 8; do
  curl -s --max-time 15 -o /dev/null -X PUT "$HTTP/v1/namespaces/$NS/configs/$ID" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"content\":\"conc-$i\",\"data_type\":\"TEXT\"}" &
  pids="$pids $!"
done
curl -s --max-time 15 -o /dev/null -X POST "$HTTP/v1/namespaces/$NS/configs/$ID/rollback" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"to_version":1}' &
pids="$pids $!"
# 只等待请求进程，不能裸 wait（会把服务器进程一起等挂）
for p in $pids; do wait "$p"; done
sleep 0.5
consistent=$($PG "SELECT (SELECT version FROM biz_configs WHERE namespace='$NS' AND data_id='$ID') = (SELECT max(version) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID')")
[ "$consistent" = "t" ]; check $? "竞态后 biz_configs.version == max(History.version)"
nogap=$($PG "SELECT (SELECT max(version) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID') = (SELECT count(*) FROM biz_config_history WHERE namespace='$NS' AND data_id='$ID')")
[ "$nogap" = "t" ]; check $? "版本序列无空洞：max(version) == History 行数"
final_content=$($PG "SELECT h.content FROM biz_config_history h WHERE h.namespace='$NS' AND h.data_id='$ID' AND h.version = (SELECT version FROM biz_configs WHERE namespace='$NS' AND data_id='$ID')")
cur_content=$($PG "SELECT content FROM biz_configs WHERE namespace='$NS' AND data_id='$ID'")
[ "$final_content" = "$cur_content" ]; check $? "当前内容与最终版本的 History 记录一致"

kill -9 $SRV 2>/dev/null
echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
