# A股量化选股筛选器

多数据源 A 股量化筛选工具，筛选条件：

- **PE < 20**（腾讯财经）
- **MACD 向上**（百度财经 K 线）
- **5/10/15/20 日主力资金净流入均为正**（东方财富）

## 版本

| 版本 | 文件 | 特点 |
|------|------|------|
| v4 | `stock_screener_v4_baidu.py` | 原始串行版本 |
| v5 | `stock_screener_v5_optimized.py` | ThreadPool 并发 + O(1)查表 |
| v6 | `stock_screener_v6_async.py` | asyncio/aiohttp + K线缓存 + PE缓存 |
| — | `capital_flow_ranking.py` | 实时主力资金流向 TOP 20 排名 |

## 运行

```bash
# 完整筛选（推荐使用 v6）
python stock_screener_v6_async.py

# 实时主力资金流向 TOP 20 排名
python capital_flow_ranking.py
```

首次运行约 10 分钟（含网络请求），二次运行利用缓存可降至 ~9 分钟。

资金流向排名脚本仅需 ~2 秒（单次批量 API 调用）。

## 数据源

- 股票列表：新浪财经 (AKShare)
- PE & 市值：腾讯财经 (qt.gtimg.cn)
- K线 & MACD：百度财经 (finance.pae.baidu.com)
- 主力资金流向：东方财富 (push2his.eastmoney.com)

## 依赖

```bash
pip install akshare pandas numpy aiohttp
```

## 代理

需要本地代理 `127.0.0.1:7890`（百度财经和东方财富需要）
