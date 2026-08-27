#!/usr/bin/env bash
# 票 01 验收脚本：单节点 Publish/Get tracer
# 覆盖 .scratch/config-center-mvp/issues/01-single-node-publish-tracer.md 全部验收项。
set -u
cd "$(dirname "$0")/.."

BIN=./config-center.exe
HTTP=http://127.0.0.1:8080
TOKEN=change-me
PG="psql -h 127.0.0.1 -U postgresql -d config_center_1 -tAc"
LOG=/tmp/gcc-e2e.log

fail=0
check() { # $1=退出码 $2=描述
  if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; fail=1; fi
}

put() { # $1=body → stdout
  curl -s -X PUT "$HTTP/v1/namespaces/dev/configs/db.password" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d "$1"
}

echo "=== 准备：清空 data 目录与 PG 表 ==="
rm -rf data
$PG "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE" >/dev/null

echo "=== 验收 6 + 1：--bootstrap 拉起单节点，空库启动到 HTTP 可服务 ≤3s ==="
start=$(date +%s%N)
"$BIN" --config config.yaml --bootstrap >"$LOG" 2>&1 &
SRV=$!
for _ in $(seq 1 120); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
[ "$ms" -le 3000 ]; check $? "空库从启动到 HTTP 可服务 ≤3s（实测 ${ms}ms）"
curl -sf "$HTTP/healthz" >/dev/null; check $? "--bootstrap 拉起单节点成功，/healthz 200"

echo "=== 验收 2：同一 DataId 连续 PUT 两次，version 依次 1、2 ==="
# 等待单节点完成自选举（期间 PUT 得 503，不产生日志）
for _ in $(seq 1 100); do
  code=$(curl -s -o /tmp/gcc-put.json -w '%{http_code}' -X PUT "$HTTP/v1/namespaces/dev/configs/db.password" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"content":"s3cret-v1","data_type":"TEXT","tags":{"team":"core"}}')
  [ "$code" = "200" ] && break
  sleep 0.1
done
[ "$code" = "200" ]; check $? "首次 PUT 200（选举完成后）"
grep -q '"version":1' /tmp/gcc-put.json && grep -q '"created":true' /tmp/gcc-put.json
check $? "首次 PUT 返回 version=1, created=true（$(cat /tmp/gcc-put.json)）"

put '{"content":"s3cret-v2","data_type":"TEXT"}' >/tmp/gcc-put.json
grep -q '"version":2' /tmp/gcc-put.json && grep -q '"created":false' /tmp/gcc-put.json
check $? "第二次 PUT 返回 version=2, created=false（$(cat /tmp/gcc-put.json)）"

hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='dev' AND data_id='db.password' AND op='publish'")
[ "$hist" = "2" ]; check $? "History 追加 2 条 publish 记录（实际 $hist）"

echo "=== 验收 3：GET 返回条目与 version；不存在返回 404 ==="
body=$(curl -s "$HTTP/v1/namespaces/dev/configs/db.password")
echo "$body" | grep -q '"version":2' && echo "$body" | grep -q 's3cret-v2'
check $? "GET 返回条目内容与 version=2"
echo "$body" | grep -q '"tags":{}'
check $? "Publish 为全量替换：第二次未带 tags，GET tags 为空对象"
code=$(curl -s -o /dev/null -w '%{http_code}' "$HTTP/v1/namespaces/dev/configs/missing")
[ "$code" = "404" ]; check $? "不存在的条目返回 404（实际 $code）"

echo "=== DESIGN §3.3：CAS 冲突在 Apply 阶段产生 409 ==="
code=$(curl -s -o /tmp/gcc-cas.json -w '%{http_code}' -X PUT "$HTTP/v1/namespaces/dev/configs/db.password" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"content":"nope","data_type":"TEXT","expected_version":1}')
[ "$code" = "409" ] && grep -q '"current_version":2' /tmp/gcc-cas.json
check $? "expected_version=1 与当前 2 冲突 → 409 + current_version=2（$code $(cat /tmp/gcc-cas.json)）"
hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='dev' AND data_id='db.password'")
[ "$hist" = "2" ]; check $? "冲突的已提交日志不追加 History（仍为 $hist 条）"

echo "=== 认证：无 token 的 PUT 返回 401 ==="
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$HTTP/v1/namespaces/dev/configs/x" -d '{}')
[ "$code" = "401" ]; check $? "无 Authorization 返回 401（实际 $code）"

echo "=== 验收 5：三表同事务，业务表与 raft_fsm_meta 进度一致 ==="
meta_before=$($PG "SELECT last_applied_index FROM raft_fsm_meta")
consistent=$($PG "SELECT (SELECT version FROM biz_configs WHERE namespace='dev' AND data_id='db.password') = (SELECT max(version) FROM biz_config_history WHERE namespace='dev' AND data_id='db.password')")
[ "$consistent" = "t" ]; check $? "biz_configs.version == max(History.version)"
[ "$meta_before" -ge 2 ] 2>/dev/null; check $? "raft_fsm_meta.last_applied_index=$meta_before ≥ 业务日志数"

echo "=== 验收 4：杀进程后重启，数据完整且重放不产生重复写入 ==="
kill -9 $SRV 2>/dev/null
sleep 0.5
curl -sf --max-time 1 "$HTTP/healthz" >/dev/null 2>&1; [ $? -ne 0 ]; check $? "进程已被杀死"

"$BIN" --config config.yaml >>"$LOG" 2>&1 &
SRV=$!
for _ in $(seq 1 120); do
  curl -sf "$HTTP/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done
body=$(curl -s "$HTTP/v1/namespaces/dev/configs/db.password")
echo "$body" | grep -q '"version":2' && echo "$body" | grep -q 's3cret-v2'
check $? "重启后 GET 读到相同数据（version=2）"

# 等单节点重选举完成、Raft 重放已提交日志（FSM 按 last_applied 跳过）
sleep 2.5
hist=$($PG "SELECT count(*) FROM biz_config_history WHERE namespace='dev' AND data_id='db.password'")
[ "$hist" = "2" ]; check $? "重放跳过，History 仍为 $hist 条（无重复写入）"
ver=$($PG "SELECT version FROM biz_configs WHERE namespace='dev' AND data_id='db.password'")
[ "$ver" = "2" ]; check $? "biz_configs.version 仍为 $ver"
meta_after=$($PG "SELECT last_applied_index FROM raft_fsm_meta")
[ "$meta_after" = "$meta_before" ]; check $? "重放未推进 raft_fsm_meta（重启前后均 $meta_after）"

echo "=== 验收 1（后半）：对已有集群态再次 --bootstrap 报错退出 ==="
kill -9 $SRV 2>/dev/null
sleep 0.5
"$BIN" --config config.yaml --bootstrap >/tmp/gcc-reboot.log 2>&1
rc=$?
[ "$rc" -ne 0 ]; check $? "再次 --bootstrap 非零退出（rc=$rc）"
grep -q "已存在集群状态" /tmp/gcc-reboot.log
check $? "错误信息明确指出已有集群状态（$(tail -1 /tmp/gcc-reboot.log)）"

echo
if [ "$fail" -eq 0 ]; then echo "全部验收项通过"; else echo "存在失败项"; fi
exit "$fail"
