"""
实时主力资金流向排名
======================================================================
功能:
  通过东方财富 push2 API 批量获取全市场 A 股实时主力资金流向，
  展示净流入 TOP 20 和净流出 TOP 20，并保存 CSV 到 results/ 目录。

数据来源: 东方财富 (push2.eastmoney.com)
特点: 批量获取全市场 ~5000+ 只股票的实时主力资金流向

用法:
  python capital_flow_ranking.py

依赖: pandas, urllib (标准库)
======================================================================
"""
import urllib.request
import urllib.parse
import json
import time
import socket
import pandas as pd
from pathlib import Path
from datetime import datetime

# 强制 IPv4（push2.eastmoney.com 的 IPv6 会拒绝连接）
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_v4(host, port, family=0, *args, **kwargs):
    return _orig_getaddrinfo(host, port, socket.AF_INET, *args, **kwargs)
socket.getaddrinfo = _getaddrinfo_v4

# ============================================================
# 配置
# ============================================================
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
PROXY = None  # 国内API直连即可，无需代理
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
PAGE_DELAY = 1.5  # 分页间延迟（秒），防限流

RESULTS_DIR = Path(__file__).parent / 'results'
RESULTS_DIR.mkdir(exist_ok=True)

# 东方财富 push2 批量接口
PUSH2_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
PUSH2_UT = 'bd1d9ddb04089700cf9c27f6f7426281'
# 覆盖全部A股: 深市主板+创业板 + 沪市主板+科创板
FS_ALL_A = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23'
# 请求字段: 价格/涨跌幅/代码/名称/主力净流入/占比/超大单/大单/中单/小单
FIELDS = 'f2,f3,f12,f14,f15,f16,f17,f18,f20,f21,f62,f66,f72,f78,f84,f184'

# ============================================================
# 工具函数
# ============================================================

_proxy_handler = urllib.request.ProxyHandler({'http': PROXY, 'https': PROXY})
_no_proxy_handler = urllib.request.ProxyHandler({})  # 空 dict = 直连，绕过系统代理
_direct_opener = urllib.request.build_opener(_no_proxy_handler)


def safe_float(val, default=0.0):
    """安全转换为 float，处理 '-' 和 None"""
    if val is None or val == '-' or val == '':
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def http_get(url, params=None, timeout=REQUEST_TIMEOUT,
             referer='https://data.eastmoney.com/', decode='utf-8'):
    """HTTP GET，直连优先，失败走代理"""
    if params:
        url = url + '?' + urllib.parse.urlencode(params)
    headers = {'User-Agent': UA}
    if referer:
        headers['Referer'] = referer
    req = urllib.request.Request(url, headers=headers)

    # 先试直连（绕过系统代理）
    try:
        with _direct_opener.open(req, timeout=timeout) as resp:
            return resp.read().decode(decode)
    except Exception:
        pass

    # 直连失败，走代理
    try:
        proxy_opener = urllib.request.build_opener(_proxy_handler)
        with proxy_opener.open(req, timeout=timeout) as resp:
            return resp.read().decode(decode)
    except Exception:
        return None


def http_get_with_retry(url, params=None, timeout=REQUEST_TIMEOUT,
                        referer='https://data.eastmoney.com/', decode='utf-8',
                        max_retries=MAX_RETRIES):
    """带重试的 HTTP GET（指数退避）"""
    for attempt in range(max_retries + 1):
        result = http_get(url, params, timeout, referer, decode)
        if result is not None:
            return result
        if attempt < max_retries:
            time.sleep(1.0 * (attempt + 1))  # 1s, 2s, 3s 退避
    return None


# ============================================================
# 数据获取
# ============================================================

FIELD_MAP = {
    'f2': '最新价',
    'f3': '涨跌幅%',
    'f12': '代码',
    'f14': '名称',
    'f15': '最高',
    'f16': '最低',
    'f17': '开盘',
    'f18': '昨收',
    'f20': '总市值',
    'f21': '流通市值',
    'f62': '主力净流入(万)',
    'f66': '超大单净流入(万)',
    'f72': '大单净流入(万)',
    'f78': '中单净流入(万)',
    'f84': '小单净流入(万)',
    'f184': '净流入占比%',
}


PAGE_SIZE = 100  # API 每页最大 100 条


def fetch_fund_flow_ranking():
    """从东方财富 push2 批量接口分页获取全市场主力资金流向排名

    API 每页最多返回 100 条，需循环拉取所有分页。

    Returns:
        (DataFrame, timestamp_str) — 排序后的全量数据 + 时间戳
        失败时返回 (None, None)
    """
    params = {
        'pn': '1',
        'pz': str(PAGE_SIZE),
        'po': '1',           # 按 f62 降序（流入最大在前）
        'np': '1',
        'fid': 'f62',
        'fs': FS_ALL_A,
        'fields': FIELDS,
        'ut': PUSH2_UT,
        'fltt': '2',
        'invt': '2',
    }

    # 先请求第一页，获取 total
    text = http_get_with_retry(PUSH2_URL, params)
    if text is None:
        print("[错误] 无法连接东方财富 API，请检查网络连接")
        return None, None

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        print(f"[错误] API 返回格式异常，无法解析 JSON")
        return None, None

    if data.get('data') is None:
        print(f"[错误] API 返回数据为空，可能 ut 参数已过期")
        return None, None

    total = data['data'].get('total', 0)
    if total == 0:
        print("[错误] total=0，无数据返回")
        return None, None

    timestamp = ''
    server_time = data['data'].get('servertime')
    if server_time:
        timestamp = datetime.fromtimestamp(server_time).strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    all_rows = []

    # 解析当前页
    diff = data['data'].get('diff')
    if diff:
        all_rows.extend(_parse_diff(diff))

    # 拉取剩余分页（页间加延迟防限流）
    for page in range(2, total_pages + 1):
        time.sleep(PAGE_DELAY)
        params['pn'] = str(page)
        text = http_get_with_retry(PUSH2_URL, params)
        if text is None:
            print(f"\n  [警告] 第 {page}/{total_pages} 页请求失败，跳过")
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            print(f"\n  [警告] 第 {page}/{total_pages} 页解析失败，跳过")
            continue
        diff = data.get('data', {}).get('diff')
        if diff:
            all_rows.extend(_parse_diff(diff))
        # 轻量进度提示（每 10 页打印一次）
        if page % 10 == 0:
            print(f'\r  已拉取 {page}/{total_pages} 页...', end='', flush=True)

    if total_pages >= 10:
        print(f'\r  已拉取 {total_pages}/{total_pages} 页', flush=True)

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("[警告] 解析后数据为空")
        return None, None

    return df, timestamp


def _parse_diff(diff):
    """将 API 返回的 diff 列表解析为 dict 列表"""
    rows = []
    for item in diff:
        row = {}
        for field, name in FIELD_MAP.items():
            val = item.get(field)
            if field in ('f62', 'f66', 'f72', 'f78', 'f84'):
                # 资金字段: 元 -> 万元，保留两位小数
                row[name] = round(safe_float(val) / 10000, 2)
            elif field == 'f12':
                row[name] = str(val) if val is not None else ''
            elif field == 'f14':
                row[name] = str(val) if val is not None else ''
            else:
                row[name] = safe_float(val)
        rows.append(row)
    return rows


# ============================================================
# 输出格式化
# ============================================================

def format_output(df, timestamp):
    """打印流入/流出 TOP 20 表格，保存 CSV"""
    print()
    print('=' * 72)
    print('  实时主力资金流向排名')
    print(f'  数据来源: 东方财富  |  更新时间: {timestamp}')
    print(f'  全市场共 {len(df)} 只股票')
    print('=' * 72)

    # 检查是否为非交易时段（所有股票 f62 都为 0）
    total_flow = df['主力净流入(万)'].abs().sum()
    if total_flow == 0:
        print()
        print('  [注意] 当前主力净流入数据全为 0，可能为非交易时段')
        print('  数据为最近交易日收盘数据，仅供参考')
        print()

    # --- 流入 TOP 20 ---
    df_inflow = df[df['主力净流入(万)'] > 0].head(20)

    print()
    print('  ─── 主力资金净流入 TOP 20 ───')
    print()
    if df_inflow.empty:
        print('  (暂无净流入股票)')
    else:
        _print_table(df_inflow, ascending=False)

    # --- 流出 TOP 20 ---
    df_outflow = df[df['主力净流入(万)'] < 0].tail(20).iloc[::-1]  # 最负的20个，从最负到最不负

    print()
    print('  ─── 主力资金净流出 TOP 20 ───')
    print()
    if df_outflow.empty:
        print('  (暂无净流出股票)')
    else:
        _print_table(df_outflow, ascending=True)

    # --- 保存 CSV ---
    csv_path = RESULTS_DIR / f'资金流向排名_{timestamp.replace(":", "").replace(" ", "_")}.csv'
    # 合并流入+流出，按净流入降序排列
    df_export = df[df['主力净流入(万)'] != 0].copy()
    df_export.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print()
    print(f'  完整排名已保存至: {csv_path}')
    print(f'  (共 {len(df_export)} 只有效数据)')
    print()


def _print_table(df_subset, ascending):
    """格式化打印表格"""
    cols = ['代码', '名称', '最新价', '涨跌幅%', '主力净流入(万)', '净流入占比%']
    # 只选存在的列
    avail_cols = [c for c in cols if c in df_subset.columns]

    # 表头
    header = f'  {"排名":<5}' + ''.join(f'{c:<14}' for c in avail_cols[1:])  # 跳过代码，代码和名称合并
    print(header)
    print('  ' + '-' * (len(header) - 2))

    for i, (_, row) in enumerate(df_subset.iterrows()):
        rank = i + 1
        code = str(row.get('代码', '-'))
        name = str(row.get('名称', '-'))
        price = row.get('最新价', 0)
        change = row.get('涨跌幅%', 0)
        flow = row.get('主力净流入(万)', 0)
        ratio = row.get('净流入占比%', 0)

        # 涨跌幅颜色标记
        if isinstance(change, (int, float)):
            if change > 0:
                change_str = f'\033[31m+{change:.2f}\033[0m'   # 红涨
            elif change < 0:
                change_str = f'\033[32m{change:.2f}\033[0m'    # 绿跌
            else:
                change_str = f'{change:.2f}'
        else:
            change_str = str(change)

        # 资金流颜色
        if isinstance(flow, (int, float)):
            if flow > 0:
                flow_str = f'\033[31m+{flow:,.2f}\033[0m'
            elif flow < 0:
                flow_str = f'\033[32m{flow:,.2f}\033[0m'
            else:
                flow_str = f'{flow:,.2f}'
        else:
            flow_str = str(flow)

        print(f'  {rank:<5}{code:<10}{name:<10}'
              f'{price:<14.2f}'
              f'{change_str:<24}'
              f'{flow_str:<24}'
              f'{ratio:<14.2f}')


# ============================================================
# 主入口
# ============================================================

def main():
    print('正在获取全市场主力资金流向数据...', end='', flush=True)
    df, timestamp = fetch_fund_flow_ranking()
    if df is None:
        return

    print(f' 完成 ({len(df)} 只股票)')
    format_output(df, timestamp)


if __name__ == '__main__':
    main()
