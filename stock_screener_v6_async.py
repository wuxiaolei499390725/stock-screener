"""
A股量化筛选 v6：终极优化版 (asyncio + aiohttp)
======================================================================
优化点:
1. asyncio + aiohttp 异步并发 (替代 ThreadPoolExecutor)
2. K线数据本地缓存 (同日重复运行秒级完成 Step 3)
3. Step2 PE批量查询并行化 (70批 -> 5路并发)
4. 智能信号量控制 (不同API不同并发上限)
5. PE数据缓存 (1小时有效)
6. 自动重试 + 指数退避
======================================================================
条件：PE<20 + MACD向上 + 5/10/15/20日主力资金净流入均为正
"""
import asyncio
import aiohttp
import json
import time
import os
import csv
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path

# ============================================================
# 配置
# ============================================================
PROXY = 'http://127.0.0.1:7890'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# 并发控制 (不同数据源不同限制)
BAIDU_CONCURRENT = 12      # 百度财经并发数
EM_CONCURRENT = 10         # 东方财富并发数
TENCENT_CONCURRENT = 5     # 腾讯财经并发数 (批量已有80只/批，5路并发=400只同时)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)
MAX_RETRIES = 2

# 缓存配置
CACHE_DIR = Path(r'C:\Users\lenovo\stock_cache')
CACHE_DIR.mkdir(exist_ok=True)
KLINE_CACHE_DIR = CACHE_DIR / 'kline'
KLINE_CACHE_DIR.mkdir(exist_ok=True)
STOCK_CODES_CACHE = Path(r'C:\Users\lenovo\stock_codes_cache.csv')
PE_CACHE_FILE = CACHE_DIR / 'pe_cache.csv'
RESULTS_DIR = Path(__file__).parent / 'results'
RESULTS_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = RESULTS_DIR / f'选股结果_v6_{time.strftime("%Y%m%d")}.csv'

# ============================================================
# 工具函数
# ============================================================
def safe_float(val):
    try:
        return float(val) if val and val != '-' else 0.0
    except (ValueError, TypeError):
        return 0.0

def is_today(path):
    """检查文件是否是今天创建的"""
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path)).date()
    return mtime == date.today()

def kline_cache_path(code):
    return KLINE_CACHE_DIR / f'{code}.csv'

# ============================================================
# HTTP 异步请求
# ============================================================
async def async_http_get(session, url, params=None, headers_extra=None,
                         decode='utf-8', sem=None):
    """异步 HTTP GET，可选信号量限流"""
    if sem is None:
        return await _do_get(session, url, params, headers_extra, decode)
    else:
        async with sem:
            return await _do_get(session, url, params, headers_extra, decode)

async def _do_get(session, url, params, headers_extra, decode):
    headers = {'User-Agent': UA}
    if headers_extra:
        headers.update(headers_extra)
    try:
        async with session.get(url, params=params, headers=headers,
                               proxy=PROXY, timeout=REQUEST_TIMEOUT) as resp:
            raw = await resp.read()
            return raw.decode(decode, errors='replace')
    except Exception:
        return None

async def async_http_get_with_retry(session, url, params=None, headers_extra=None,
                                    decode='utf-8', sem=None, max_retries=MAX_RETRIES):
    """带重试的异步 HTTP GET"""
    for attempt in range(max_retries + 1):
        result = await async_http_get(session, url, params, headers_extra, decode, sem)
        if result is not None:
            return result
        if attempt < max_retries:
            await asyncio.sleep(0.5 * (attempt + 1))
    return None


# ============================================================
# 1. 获取全A股代码列表
# ============================================================
def load_stock_codes():
    print("=" * 60)
    print("Step 1: 获取全A股代码列表 ...")
    print("=" * 60)

    if STOCK_CODES_CACHE.exists():
        print(f"[OK] 使用缓存: {STOCK_CODES_CACHE}")
        df = pd.read_csv(STOCK_CODES_CACHE, dtype={'代码': str, '名称': str})
        codes = df['代码'].tolist()
        names = dict(zip(df['代码'], df['名称']))
        print(f"全A股: {len(codes)} 只")
        return codes, names

    import akshare as ak
    for retry in range(3):
        try:
            df_sina = ak.stock_zh_a_spot()
            print(f"全A股: {len(df_sina)} 只")
            codes = df_sina['代码'].tolist()
            names = dict(zip(df_sina['代码'], df_sina['名称']))
            pd.DataFrame({'代码': codes, '名称': [names[c] for c in codes]}).to_csv(
                STOCK_CODES_CACHE, index=False, encoding='utf-8')
            return codes, names
        except Exception as e:
            print(f"新浪获取失败 (尝试 {retry+1}/3): {e}")
            if retry < 2:
                time.sleep(30)
            else:
                raise


# ============================================================
# 2. 批量获取PE和市值 (腾讯财经) — 异步并行版
# ============================================================
def to_tencent_code(code):
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


async def fetch_pe_batch(session, batch_codes, tcode_to_code, sem):
    """异步获取一批PE数据"""
    url = 'http://qt.gtimg.cn/q=' + ','.join(batch_codes)
    text = await async_http_get(session, url, decode='gbk', sem=sem)
    if not text:
        return {}

    batch_result = {}
    for line in text.split('\n'):
        line = line.strip()
        if '~' not in line or '=' not in line:
            continue
        parts = line.split('"')
        if len(parts) < 2:
            continue
        fields = parts[1].split('~')
        if len(fields) < 45:
            continue
        tcode = parts[0].replace('v_', '').replace('=', '')
        orig_code = tcode_to_code.get(tcode)
        if orig_code:
            batch_result[orig_code] = {
                'pe': safe_float(fields[39]),
                'market_cap': safe_float(fields[44]),
                'name': fields[1]
            }
    return batch_result


async def step2_fetch_pe(stock_codes, stock_names):
    """Step 2: 异步并行获取PE"""
    print("\n" + "=" * 60)
    print("Step 2: 批量获取PE和市值 (腾讯财经, 异步并行) ...")
    print("=" * 60)

    # 检查PE缓存 (1小时内有效)
    if PE_CACHE_FILE.exists():
        cache_age = time.time() - os.path.getmtime(PE_CACHE_FILE)
        if cache_age < 3600:
            print(f"[OK] PE缓存有效 ({(cache_age/60):.0f}分钟前)")
            df = pd.read_csv(PE_CACHE_FILE, dtype={'code': str})
            pe_data = {}
            for _, row in df.iterrows():
                pe_data[row['code']] = {
                    'pe': row['pe'], 'market_cap': row['market_cap'],
                    'name': row['name']
                }
            print(f"PE数据: {len(pe_data)} 只 (来自缓存)")
            return pe_data

    # 构建 O(1) 映射
    tcode_to_code = {}
    for c in stock_codes:
        tcode_to_code[to_tencent_code(c)] = c

    tencent_codes = list(tcode_to_code.keys())
    BATCH_SIZE = 80
    batches = [tencent_codes[i:i+BATCH_SIZE] for i in range(0, len(tencent_codes), BATCH_SIZE)]
    total_batches = len(batches)

    pe_data = {}
    start_time = time.time()
    completed = 0
    lock = asyncio.Lock()

    sem = asyncio.Semaphore(TENCENT_CONCURRENT)

    async def fetch_and_collect(batch, idx):
        nonlocal completed
        result = await fetch_pe_batch(session, batch, tcode_to_code, sem)
        async with lock:
            pe_data.update(result)
            nonlocal completed
            completed += 1
            if completed % 10 == 0 or completed == total_batches:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total_batches - completed) / rate if rate > 0 else 0
                print(f"  [{completed}/{total_batches}] 已有PE数据: {len(pe_data)} 只 | "
                      f"速度: {rate:.1f}批/s | 预计剩余: {eta:.0f}s")
        return result

    connector = aiohttp.TCPConnector(limit=TENCENT_CONCURRENT * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_collect(b, i) for i, b in enumerate(batches)]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    print(f"[OK] Step 2 完成 ({elapsed:.1f}s), 获取PE数据: {len(pe_data)} 只")

    # 保存PE缓存
    rows = [{'code': c, 'pe': i['pe'], 'market_cap': i['market_cap'], 'name': i['name']}
            for c, i in pe_data.items()]
    pd.DataFrame(rows).to_csv(PE_CACHE_FILE, index=False, encoding='utf-8')
    print(f"[OK] PE缓存已保存: {PE_CACHE_FILE}")

    return pe_data


# ============================================================
# 3. K线 + MACD (百度财经) — 异步 + 本地缓存
# ============================================================
async def fetch_kline_baidu(session, code, sem):
    """异步获取百度K线 (带缓存)"""
    # 先查缓存
    cache_path = kline_cache_path(code)
    if is_today(cache_path):
        try:
            df = pd.read_csv(cache_path)
            if len(df) >= 60:
                return df
        except Exception:
            pass

    # 请求API
    url = 'https://finance.pae.baidu.com/selfselect/getstockquotation'
    params = {
        'all': '1', 'isIndex': 'false', 'isBk': 'false', 'isBlock': 'false',
        'isFutures': 'false', 'isStock': 'true', 'newFormat': '1',
        'group': 'quotation_kline_ab', 'finClientType': 'pc',
        'code': code, 'ktype': '1', 'count': '120'
    }
    text = await async_http_get_with_retry(session, url, params=params, sem=sem)
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
        df = pd.DataFrame(rows)
        # 保存缓存
        df.to_csv(cache_path, index=False)
        return df
    except Exception:
        return None


def calc_macd(df):
    """MACD计算"""
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


async def process_one_macd(session, stock, sem, lock, stats):
    """单只股票MACD分析"""
    code = stock['code']
    clean_code = code.replace('sh', '').replace('sz', '').replace('bj', '')

    for attempt in range(MAX_RETRIES + 1):
        try:
            kl = await fetch_kline_baidu(session, clean_code, sem)
            if kl is not None:
                is_up, dif, dea, bar = calc_macd(kl)
                if is_up:
                    stock['dif'] = dif
                    stock['dea'] = dea
                    stock['macd_bar'] = bar
                    return stock
                return None  # K线正常但不满足MACD条件
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5)
        except Exception:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.5)
    return None


async def step3_macd(pe_lt_20):
    """Step 3: 异步MACD分析"""
    print("\n" + "=" * 60)
    print(f"Step 3: MACD分析 (百度财经) - {len(pe_lt_20)} 只 (并发={BAIDU_CONCURRENT})")
    print("=" * 60)

    sem = asyncio.Semaphore(BAIDU_CONCURRENT)
    lock = asyncio.Lock()
    stats = {'completed': 0, 'passed': 0, 'cached': 0}
    start_time = time.time()

    # 统计缓存命中
    cached_count = 0
    for s in pe_lt_20:
        clean = s['code'].replace('sh', '').replace('sz', '').replace('bj', '')
        if is_today(kline_cache_path(clean)):
            cached_count += 1
    if cached_count > 0:
        print(f"  今日K线缓存命中: {cached_count}/{len(pe_lt_20)} 只")

    macd_pass = []

    async def worker(stock):
        result = await process_one_macd(session, stock, sem, lock, stats)
        async with lock:
            stats['completed'] += 1
            if result:
                macd_pass.append(result)
                stats['passed'] += 1
            c = stats['completed']
            if c % 50 == 0 or c == len(pe_lt_20):
                elapsed = time.time() - start_time
                rate = c / elapsed if elapsed > 0 else 0
                eta = (len(pe_lt_20) - c) / rate if rate > 0 else 0
                print(f"  [{c}/{len(pe_lt_20)}] MACD通过: {stats['passed']} | "
                      f"速度: {rate:.1f}只/s | 预计剩余: {eta:.0f}s")

    connector = aiohttp.TCPConnector(limit=BAIDU_CONCURRENT * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [worker(s) for s in pe_lt_20]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    print(f"[OK] MACD向上: {len(macd_pass)} 只 (耗时 {elapsed:.1f}s)")
    return macd_pass


# ============================================================
# 4. 主力资金流向 (东方财富) — 异步版
# ============================================================
async def fetch_fund_flow(session, code, sem):
    """异步获取资金流向"""
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
        'secid': secid, 'lmt': '25', 'klt': '101',
        'fields1': 'f1,f2,f3,f7',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65',
    }
    headers = {'Referer': 'https://data.eastmoney.com/'}
    text = await async_http_get_with_retry(session, url, params=params,
                                            headers_extra=headers, sem=sem)
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
            'flow_5d': round(sum(main_flows[:5])  / 10000, 2),
            'flow_10d': round(sum(main_flows[:10]) / 10000, 2),
            'flow_15d': round(sum(main_flows[:15]) / 10000, 2),
            'flow_20d': round(sum(main_flows[:20]) / 10000, 2),
        }
    except Exception:
        return None


async def process_one_flow(session, stock, sem):
    """单只股票资金流分析"""
    code = stock['code']
    for attempt in range(MAX_RETRIES + 1):
        try:
            flow = await fetch_fund_flow(session, code, sem)
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
                await asyncio.sleep(0.3)
        except Exception:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(0.3)
    return None


async def step4_fund_flow(macd_pass):
    """Step 4: 异步资金流分析"""
    print("\n" + "=" * 60)
    print(f"Step 4: 主力资金流向分析 (东方财富) - {len(macd_pass)} 只 (并发={EM_CONCURRENT})")
    print("=" * 60)

    sem = asyncio.Semaphore(EM_CONCURRENT)
    lock = asyncio.Lock()
    results = []
    completed = 0
    start_time = time.time()

    async def worker(stock):
        nonlocal completed
        result = await process_one_flow(session, stock, sem)
        async with lock:
            nonlocal completed
            completed += 1
            if result:
                results.append(result)
            if completed % 20 == 0 or completed == len(macd_pass):
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(macd_pass) - completed) / rate if rate > 0 else 0
                print(f"  [{completed}/{len(macd_pass)}] 全部满足: {len(results)} | "
                      f"速度: {rate:.1f}只/s | 预计剩余: {eta:.0f}s")

    connector = aiohttp.TCPConnector(limit=EM_CONCURRENT * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [worker(s) for s in macd_pass]
        await asyncio.gather(*tasks)

    return results


# ============================================================
# 5. 主流程
# ============================================================
async def main():
    total_start = time.time()

    # Step 1: 股票列表
    stock_codes, stock_names = load_stock_codes()

    # Step 2: PE查询 (异步并行批次)
    pe_data = await step2_fetch_pe(stock_codes, stock_names)

    # PE < 20 筛选
    pe_lt_20 = []
    for code, info in pe_data.items():
        if 0 < info['pe'] < 20:
            name = info.get('name', stock_names.get(code, ''))
            if 'ST' in name or '退' in name or name.startswith('N'):
                continue
            pe_lt_20.append({
                'code': code, 'name': name,
                'pe': info['pe'], 'market_cap': info['market_cap']
            })

    print(f"\n0 < PE < 20: {len(pe_lt_20)} 只")
    if len(pe_lt_20) == 0:
        print("无符合PE条件的股票，退出")
        return

    # Step 3: MACD分析 (异步 + 缓存)
    macd_pass = await step3_macd(pe_lt_20)

    if len(macd_pass) == 0:
        print("无满足MACD条件的股票，退出")
        return

    # Step 4: 资金流 (异步)
    results = await step4_fund_flow(macd_pass)

    # Step 5: 输出
    print("\n" + "=" * 60)
    print(f"筛选完成! 满足全部条件的股票: {len(results)} 只")
    print(f"总耗时: {time.time()-total_start:.1f}s")
    print("=" * 60)

    if results:
        df_out = pd.DataFrame(results).sort_values(
            '20日净流入(万)', ascending=False).reset_index(drop=True)
        print("\n" + df_out.to_string(max_rows=100))

        df_out.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"\n[OK] 结果已保存: {OUTPUT_CSV}")
        print(f"\n>> PE范围: {df_out['市盈率'].min():.1f} ~ {df_out['市盈率'].max():.1f}")
        print(f">> 20日净流入范围: {df_out['20日净流入(万)'].min():.0f}万 ~ {df_out['20日净流入(万)'].max():.0f}万")

        # 缓存统计
        kline_cached = sum(1 for f in KLINE_CACHE_DIR.iterdir() if f.suffix == '.csv')
        print(f">> K线缓存文件数: {kline_cached}")
    else:
        print("[WARN] 无股票满足全部条件")
        print("   建议: 放宽PE上限、仅要求近5/10日流入、或放宽MACD条件")


if __name__ == '__main__':
    asyncio.run(main())
