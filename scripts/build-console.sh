#!/usr/bin/env bash
# 构建控制台（ADR-0012）：前端产物拷入 internal/console/dist，随后 go build 即得全功能二进制。
# 用法：scripts/build-console.sh            # 构建前端 + go build 出 ./config-center(.exe)
#       scripts/build-console.sh --web-only # 只构建前端产物，不编译 Go
set -euo pipefail
cd "$(dirname "$0")/.."

cd web/console
if [ -f package-lock.json ]; then
  npm ci --no-audit --no-fund
else
  npm install --no-audit --no-fund
fi
npm run build
cd ../..

# dist 整体不入库（.gitignore 白名单 .keep 保证 embed 目录存在）；
# 构建产物属本地文件，占位页在 internal/console/placeholder.html，不受构建影响。
rm -rf internal/console/dist
mkdir -p internal/console/dist
cp -r web/console/dist/. internal/console/dist/
touch internal/console/dist/.keep

echo "PASS: 控制台产物已就位 internal/console/dist"
if [ "${1:-}" != "--web-only" ]; then
  bin="config-center$(go env GOEXE)"
  go build -o "$bin" ./cmd/config-center
  echo "PASS: 二进制已产出 ./$bin"
fi
