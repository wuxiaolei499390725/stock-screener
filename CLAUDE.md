# stock-screener — A股量化选股筛选器

## 项目概述
多数据源 A 股量化筛选：PE<20 + MACD向上 + 5/10/15/20日主力资金净流入均为正。

## 文件说明
- `stock_screener_v4_baidu.py` — 原始串行版本（基线）
- `stock_screener_v5_optimized.py` — ThreadPool 并发版（O(1)查表、重试）
- `stock_screener_v6_async.py` — asyncio/aiohttp 终极版（K线缓存、PE缓存、多路并发）**推荐使用**

## 运行
```bash
cd ~/stock-screener
python stock_screener_v6_async.py
```

## 技术栈
- Python 3.11, akshare, pandas, numpy, aiohttp
- 代理: 127.0.0.1:7890
- 数据源: 新浪(列表) + 腾讯(PE) + 百度(K线/MACD) + 东方财富(资金流)

## Git
- 仓库: https://github.com/wuxiaolei499390725/stock-screener
- 用户: wuxiaolei499390725 / 499390725@qq.com
- SSH 已配置

## 缓存目录
- 股票列表: `stock_codes_cache.csv`
- K线缓存: `stock_cache/kline/`
- PE缓存: `stock_cache/pe_cache.csv`
- 输出: `选股结果_v6_YYYYMMDD.csv`
