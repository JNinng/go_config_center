#!/usr/bin/env bash
# 票 05 验收脚本：Watch 长轮询
# 覆盖 .scratch/config-center-mvp/issues/05-watch-long-polling.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e05.log
NS=dev
ID=db.password

fail=0
check() {
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}
now_ms() { date +%s%N | cut -c1-13; }

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

for _ in $(seq 1 100); do
  code=$(code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"watch-v1","data_type":"TEXT"}')
  [ "$code" != "503" ] && break
  sleep 0.1
done
[ "$code" = "200" ]; check $? "发布 v1"

echo "=== 验收 4（前半）：v 已是最新 → 挂起等待，至 timeout 收场；验收 3：timeout 回显 v 供原样重挂 ==="
start=$(now_ms)
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' "$HTTP/v1/namespaces/$NS/configs/$ID/watch?v=1&timeout=1s")
ms=$(( $(now_ms) - start ))
[ "$code" = "200" ] && grep -q '"status":"timeout"' /tmp/gcc-resp.json && grep -q '"version":1' /tmp/gcc-resp.json
check $? "v=1 最新 → timeout 状态 + version=1 回显（${ms}ms $(cat /tmp/gcc-resp.json)）"
[ "$ms" -ge 900 ]; check $? "确实挂起了 timeout 时长（${ms}ms ≥ 900ms）"

echo "=== 验收 4（后半）：v 落后 → 立即返回当前状态 ==="
start=$(now_ms)
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' "$HTTP/v1/namespaces/$NS/configs/$ID/watch?v=0&timeout=10s")
ms=$(( $(now_ms) - start ))
grep -q '"status":"active"' /tmp/gcc-resp.json && grep -q '"content":"watch-v1"' /tmp/gcc-resp.json && grep -q '"version":1' /tmp/gcc-resp.json
check $? "v=0 落后 → 立即 active + v1 条目（${ms}ms）"
[ "$ms" -lt 2000 ]; check $? "立即返回（${ms}ms < 2s）"

echo "=== 验收 1：挂起期间 Publish → 立即返回 active + 新条目 ==="
start=$(now_ms)
curl -s -o /tmp/gcc-watch1.json "$HTTP/v1/namespaces/$NS/configs/$ID/watch?v=1&timeout=10s" &
WPID=$!
sleep 0.4
code_of PUT "/v1/namespaces/$NS/configs/$ID" '{"content":"watch-v2","data_type":"TEXT"}' >/dev/null
wait $WPID
ms=$(( $(now_ms) - start ))
grep -q '"status":"active"' /tmp/gcc-watch1.json && grep -q '"content":"watch-v2"' /tmp/gcc-watch1.json && grep -q '"version":2' /tmp/gcc-watch1.json
check $? "挂起被 Publish 唤醒 → active + v2 条目（$(cat /tmp/gcc-watch1.json)）"
[ "$ms" -lt 4000 ]; check $? "唤醒及时（${ms}ms < 4s）"

echo "=== 验收 2：挂起期间 Delete → 立即返回 deleted + 墓碑版本 ==="
start=$(now_ms)
curl -s -o /tmp/gcc-watch2.json "$HTTP/v1/namespaces/$NS/configs/$ID/watch?v=2&timeout=10s" &
WPID=$!
sleep 0.4
code_of DELETE "/v1/namespaces/$NS/configs/$ID" >/dev/null
wait $WPID
ms=$(( $(now_ms) - start ))
grep -q '"status":"deleted"' /tmp/gcc-watch2.json && grep -q '"version":3' /tmp/gcc-watch2.json
check $? "挂起被 Delete 唤醒 → deleted + 墓碑版本 3（$(cat /tmp/gcc-watch2.json)）"
[ "$ms" -lt 4000 ]; check $? "唤醒及时（${ms}ms < 4s）"

echo "=== 验收 3（补充）：不存在的条目挂起等待创建，超时收场 ==="
code=$(curl -s -o /tmp/gcc-resp.json -w '%{http_code}' "$HTTP/v1/namespaces/$NS/configs/ghost.id/watch?v=0&timeout=500ms")
grep -q '"status":"timeout"' /tmp/gcc-resp.json
check $? "ghost 条目 → timeout（$code）"

echo "=== 验收 5：服务端关闭时挂起连接断连收场，不悬挂 ==="
curl -s --max-time 10 -o /tmp/gcc-watch3.json "$HTTP/v1/namespaces/$NS/configs/$ID/watch?v=3&timeout=30s" &
WPID=$!
sleep 0.5
start=$(now_ms)
kill -9 $SRV 2>/dev/null
wait $WPID; rc=$?
ms=$(( $(now_ms) - start ))
[ "$rc" -ne 0 ]; check $? "服务端被杀后挂起 curl 以错误退出（rc=$rc）"
[ "$ms" -lt 5000 ]; check $? "断连及时（${ms}ms < 5s），未悬挂到 30s timeout"

echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
