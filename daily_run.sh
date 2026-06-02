#!/bin/bash
# ============================================================
# 每日自动选股脚本
# 定时任务: 每天 10:00 由 Windows Task Scheduler 触发
# ============================================================
set -e

# 日志
LOG_FILE="$HOME/stock-screener/daily_run.log"
exec 2>&1
exec 1>>"$LOG_FILE"

echo "=============================================="
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# 进入项目目录
cd "$HOME/stock-screener"

# 运行选股脚本
echo "[1/3] 运行选股..."
PYTHONIOENCODING=utf-8 /c/Users/lenovo/AppData/Local/Programs/Python/Python311/python.exe -u stock_screener_v6_async.py

# 提交结果到 git
echo "[2/3] 提交到 git..."
git add results/
git commit -m "auto: daily stock screener $(date '+%Y%m%d')" || echo "  (无变更或提交失败)"

# 推送
echo "[3/3] 推送到 GitHub..."
git push || echo "  (推送失败，稍后可手动 git push)"

echo "完成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
