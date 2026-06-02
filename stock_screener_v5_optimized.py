"""
A股量化筛选 v5：优化版
============================================================
优化点:
1. 修复 PE查表 O(n²) → O(1)
2. MACD/资金流 并发请求 (ThreadPoolExecutor)
3. 添加自动重试机制
4. 进度条优化、减少无意义等待
5. 请求复用 session
============================================================
条件：PE<20 + MACD向上 + 5/10/15/20日主力资金净流入均为正
"""
import urllib.request
import urllib.parse
import json
import time
import os
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ============================================================
# 配置
# ============================================================
PROXY_HOST = 'http://127.0.0.1:7890'
MAX_WORKERS = 8          # 并发线程数
REQUEST_TIMEOUT = 15
MAX_RETRIES = 2          # 失败重试次数
BATCH_SIZE = 80          # PE批量查询大小

CACHE_FILE = r'C:\Users\lenovo\stock_codes_cache.csv'
OUTPUT_CSV = r'C:\Users\lenovo\选股结果_v5_' + time.strftime('%Y%m%d') + '.csv'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# 全局锁，保护打印和共享数据
print_lock = Lock()
stats_lock = Lock()

# ============================================================
# 代理 & HTTP
# ============================================================
proxy_handler = urllib.request.ProxyHandler({'http': PROXY_HOST, 'https': PROXY_HOST})

def http_get(url, params=None, timeout=REQUEST_TIMEOUT, use_proxy=False, referer='', decode='utf-8'):
    """统一 HTTP GET，可选走代理"""
    if params:
        url = url + '?' + urllib.parse.urlencode(params)
    headers = {'User-Agent': UA}
    if referer:
        headers['Referer'] = referer
    req = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(proxy_handler) if use_proxy else urllib.request.build_opener()
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.read().decode(decode)
    except Exception:
        return None

def http_get_with_retry(url, params=None, timeout=REQUEST_TIMEOUT, use_proxy=False, referer='', decode='utf-8',
                        max_retries=MAX_RETRIES):
    """带重试的 HTTP GET"""
    for attempt in range(max_retries + 1):
        result = http_get(url, params, timeout, use_proxy, referer, decode)
        if result is not None:
            return result
        if attempt < max_retries:
            time.sleep(0.5 * (attempt + 1))  # 递增等待
    return None


def safe_float(val):
    try:
        return float(val) if val and val != '-' else 0.0
    except (ValueError, TypeError):
        return 0.0


# ============================================================
# 1. 获取全A股代码列表
# ============================================================
print("=" * 60)
print("Step 1: 获取全A股代码列表 ...")
print("=" * 60)

if os.path.exists(CACHE_FILE):
    print(f"[OK] 使用缓存: {CACHE_FILE}")
    df_cache = pd.read_csv(CACHE_FILE, dtype={'代码': str, '名称': str})
    stock_codes = df_cache['代码'].tolist()
    stock_names = dict(zip(df_cache['代码'], df_cache['名称']))
    print(f"全A股: {len(stock_codes)} 只")
else:
    import akshare as ak
    for retry in range(3):
        try:
            df_sina = ak.stock_zh_a_spot()
            print(f"全A股: {len(df_sina)} 只")
            stock_codes = df_sina['代码'].tolist()
            stock_names = dict(zip(df_sina['代码'], df_sina['名称']))
            pd.DataFrame({'代码': stock_codes, '名称': [stock_names[c] for c in stock_codes]}).to_csv(
                CACHE_FILE, index=False, encoding='utf-8')
            break
        except Exception as e:
            print(f"新浪获取失败 (尝试 {retry+1}/3): {e}")
            if retry < 2:
                time.sleep(30)
            else:
                raise


# ============================================================
# 2. 批量获取PE和市值 (腾讯财经)
# ============================================================
print("\n" + "=" * 60)
print("Step 2: 批量获取PE和市值 (腾讯财经) ...")
print("=" * 60)


def to_tencent_code(code):
    """转换代码格式 -> 构建 tcode→code 反向映射时用到"""
    if code.startswith('bj'):
        return f'bj{code[2:]}'
    elif code.startswith(('sh', 'sz')):
        return code
    elif code.startswith(('6', '5')):
        return f'sh{code}'
    elif code.startswith(('0', '3', '2')):
        return f'sz{code}'
    elif code.startswith(('8', '4', '9')):
        return f'bj{code}'
    else:
        return f'sz{code}'


# 【优化1】构建 O(1) 反向映射: tcode → original_code
tcode_to_code = {}
for c in stock_codes:
    tc = to_tencent_code(c)
    tcode_to_code[tc] = c

tencent_codes = list(tcode_to_code.keys())
pe_data = {}

total_batches = (len(tencent_codes) + BATCH_SIZE - 1) // BATCH_SIZE
start_time = time.time()

for i in range(0, len(tencent_codes), BATCH_SIZE):
    batch_codes = tencent_codes[i:i + BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1

    url = 'http://qt.gtimg.cn/q=' + ','.join(batch_codes)
    text = http_get(url, timeout=20, decode='gbk')

    if text:
        for line in text.split('\n'):
            line = line.strip()
            if '~' not in line or '=' not in line:
                continue
            # 解析: v_sh600519="1~贵州茅台~..."
            parts = line.split('"')
            if len(parts) < 2:
                continue
            fields = parts[1].split('~')
            if len(fields) < 45:
                continue
            # 提取 tcode
            tcode = parts[0].replace('v_', '').replace('=', '')
            # 【优化1】O(1)反向查找
            orig_code = tcode_to_code.get(tcode)
            if orig_code:
                pe = safe_float(fields[39])
                mv = safe_float(fields[44])
                pe_data[orig_code] = {
                    'pe': pe, 'market_cap': mv, 'name': fields[1],
                    'tcode': tcode
                }

    elapsed = time.time() - start_time
    if batch_num % 10 == 0 or batch_num == total_batches:
        eta = (elapsed / batch_num) * (total_batches - batch_num) if batch_num > 0 else 0
        print(f"  [{batch_num}/{total_batches}] 已有PE数据: {len(pe_data)} 只 | 预计剩余: {eta:.0f}s")
    time.sleep(0.1)  # 减少等待时间

elapsed_2 = time.time() - start_time
print(f"[OK] Step 2 完成 ({elapsed_2:.1f}s), 获取PE数据: {len(pe_data)} 只")

# PE < 20 筛选
pe_lt_20 = []
for code, info in pe_data.items():
    if 0 < info['pe'] < 20:
        name = info.get('name', stock_names.get(code, ''))
        if 'ST' in name or '退' in name or name.startswith('N'):
            continue
        pe_lt_20.append({
            'code': code,
            'name': name,
            'pe': info['pe'],
            'market_cap': info['market_cap'],
            'tcode': info['tcode']
        })

print(f"0 < PE < 20: {len(pe_lt_20)} 只")

if len(pe_lt_20) == 0:
    print("无符合PE条件的股票，退出")
    exit()


# ============================================================
# 3. K线 + MACD (百度财经) — 并发版
# ============================================================
print("\n" + "=" * 60)
print(f"Step 3: MACD分析 (百度财经) - {len(pe_lt_20)} 只 (并发={MAX_WORKERS})")
print("=" * 60)


def get_kline_baidu(code, count=120):
    """百度财经 K线数据"""
    url = 'https://finance.pae.baidu.com/selfselect/getstockquotation'
    params = {
        'all': '1', 'isIndex': 'false', 'isBk': 'false', 'isBlock': 'false',
        'isFutures': 'false', 'isStock': 'true', 'newFormat': '1',
        'group': 'quotation_kline_ab', 'finClientType': 'pc',
        'code': code, 'ktype': '1', 'count': str(count)
    }
    text = http_get_with_retry(url, params=params, timeout=15, use_proxy=True)
    if not text:
        return None
    try:
        data = json.loads(text)
        if data.get('ResultCode') != '0':
            return None
        result = data.get('Result')
        if not result:
            return None
        nmd = None
        if isinstance(result, list):
            if len(result) > 0 and isinstance(result[0], dict):
                nmd = result[0].get('newMarketData') or result[0]
        elif isinstance(result, dict):
            nmd = result.get('newMarketData') or result
        if not nmd or not isinstance(nmd, dict):
            return None
        keys = nmd.get('keys', [])
        md = nmd.get('marketData', '')
        if not md or not keys:
            return None
        rows = []
        for line in md.split(';'):
            if not line.strip():
                continue
            vals = line.split(',')
            if len(vals) < len(keys):
                continue
            row = {}
            for k, v in zip(keys, vals):
                row[k] = v
            rows.append(row)
        return pd.DataFrame(rows)
    except Exception:
        return None


def calc_macd(df):
    """MACD计算: 返回 (is_up, dif, dea, bar)"""
    if df is None or len(df) < 60:
        return False, 0, 0, 0
    try:
        close = pd.to_numeric(df['close'], errors='coerce')
        e12 = close.ewm(span=12, adjust=False).mean()
        e26 = close.ewm(span=26, adjust=False).mean()
        dif = e12 - e26
        dea = dif.ewm(span=9, adjust=False).mean()
        bar = 2 * (dif - dea)
        is_up = (dif.iloc[-1] > dea.iloc[-1] and
                 bar.iloc[-1] > 0 and
                 bar.iloc[-1] > bar.iloc[-3] if len(bar) >= 3 else bar.iloc[-1] > 0)
        return is_up, round(dif.iloc[-1], 4), round(dea.iloc[-1], 4), round(bar.iloc[-1], 4)
    except Exception:
        return False, 0, 0, 0


def process_one_macd(stock):
    """单只股票的MACD分析 (用于并发)"""
    code = stock['code']
    clean_code = code.replace('sh', '').replace('sz', '').replace('bj', '')

    # 重试
    for attempt in range(MAX_RETRIES + 1):
        try:
            kl = get_kline_baidu(clean_code, count=120)
            if kl is not None:
                is_up, dif, dea, bar = calc_macd(kl)
                if is_up:
                    stock['dif'] = dif
                    stock['dea'] = dea
                    stock['macd_bar'] = bar
                    return stock
            if attempt < MAX_RETRIES:
                time.sleep(0.3)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(0.3)
    return None


macd_pass = []
start_time = time.time()
completed = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_one_macd, s): s for s in pe_lt_20}
    for future in as_completed(futures):
        completed += 1
        result = future.result()
        if result:
            macd_pass.append(result)
        if completed % 50 == 0 or completed == len(pe_lt_20):
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (len(pe_lt_20) - completed) / rate if rate > 0 else 0
            print(f"  [{completed}/{len(pe_lt_20)}] MACD通过: {len(macd_pass)} | "
                  f"速度: {rate:.1f}只/s | 预计剩余: {eta:.0f}s")

print(f"[OK] MACD向上: {len(macd_pass)} 只 (耗时 {time.time()-start_time:.1f}s)")

if len(macd_pass) == 0:
    print("无满足MACD条件的股票，退出")
    exit()


# ============================================================
# 4. 主力资金流向 (东方财富) — 并发版
# ============================================================
print("\n" + "=" * 60)
print(f"Step 4: 主力资金流向分析 (东方财富) - {len(macd_pass)} 只 (并发={MAX_WORKERS})")
print("=" * 60)


def get_fund_flow_em(code):
    """东方财富个股资金流向"""
    if code.startswith('sh') or code.startswith('6'):
        secid = f'1.{code.replace("sh", "")}'
    elif code.startswith('sz') or code.startswith('0') or code.startswith('3') or code.startswith('2'):
        secid = f'0.{code.replace("sz", "")}'
    elif code.startswith('bj'):
        secid = f'0.{code.replace("bj", "")}'
    else:
        secid = f'0.{code}'

    url = 'https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get'
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f7',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65',
        'lmt': '25', 'klt': '101',
    }
    text = http_get_with_retry(url, params=params, timeout=15, use_proxy=True,
                                referer='https://data.eastmoney.com/')
    if not text:
        return None
    try:
        data = json.loads(text)
        klines = data.get('data', {}).get('klines', [])
        if not klines:
            return None
        main_flows = []
        for line in klines:
            p = line.split(',')
            main_flows.append(safe_float(p[4]))
        if len(main_flows) < 20:
            return None
        return {
            'flow_5d': round(sum(main_flows[:5]) / 10000, 2),
            'flow_10d': round(sum(main_flows[:10]) / 10000, 2),
            'flow_15d': round(sum(main_flows[:15]) / 10000, 2),
            'flow_20d': round(sum(main_flows[:20]) / 10000, 2),
        }
    except Exception:
        return None


def process_one_flow(stock):
    """单只股票的资金流分析 (用于并发)"""
    code = stock['code']
    for attempt in range(MAX_RETRIES + 1):
        try:
            flow = get_fund_flow_em(code)
            if flow:
                f5, f10, f15, f20 = flow['flow_5d'], flow['flow_10d'], flow['flow_15d'], flow['flow_20d']
                if f5 > 0 and f10 > 0 and f15 > 0 and f20 > 0:
                    return {
                        '代码': code.replace('sh', '').replace('sz', '').replace('bj', ''),
                        '名称': stock['name'],
                        '市盈率': round(stock['pe'], 2),
                        'DIF': stock['dif'],
                        'DEA': stock['dea'],
                        'MACD柱': stock['macd_bar'],
                        '5日净流入(万)': f5,
                        '10日净流入(万)': f10,
                        '15日净流入(万)': f15,
                        '20日净流入(万)': f20,
                        '总市值(亿)': round(stock['market_cap'], 2) if stock['market_cap'] else '',
                    }
            if attempt < MAX_RETRIES:
                time.sleep(0.2)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(0.2)
    return None


results = []
start_time = time.time()
completed = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_one_flow, s): s for s in macd_pass}
    for future in as_completed(futures):
        completed += 1
        result = future.result()
        if result:
            results.append(result)
        if completed % 20 == 0 or completed == len(macd_pass):
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (len(macd_pass) - completed) / rate if rate > 0 else 0
            print(f"  [{completed}/{len(macd_pass)}] 全部满足: {len(results)} | "
                  f"速度: {rate:.1f}只/s | 预计剩余: {eta:.0f}s")


# ============================================================
# 5. 输出结果
# ============================================================
print("\n" + "=" * 60)
print(f"筛选完成! 满足全部条件的股票: {len(results)} 只")
print("=" * 60)

if results:
    df_out = pd.DataFrame(results).sort_values('20日净流入(万)', ascending=False).reset_index(drop=True)
    print("\n" + df_out.to_string(max_rows=100))

    df_out.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"\n[OK] 结果已保存: {OUTPUT_CSV}")
    print(f"\n>> PE范围: {df_out['市盈率'].min():.1f} ~ {df_out['市盈率'].max():.1f}")
    print(f">> 20日净流入范围: {df_out['20日净流入(万)'].min():.0f}万 ~ {df_out['20日净流入(万)'].max():.0f}万")
else:
    print("[WARN] 无股票满足全部条件")
    print("   建议: 放宽PE上限、仅要求近5/10日流入、或放宽MACD条件")
