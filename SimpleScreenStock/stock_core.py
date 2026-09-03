# -*- coding: utf-8 -*-
"""
stock_core.py —— A股多条件组合选股核心库（不依赖 Streamlit，可复用）
=====================================================================
架构：本地 SQLite 缓存（stock_db.py）+ 增量更新 + 多条件并行筛选
  - 数据同步  : prepare_meta() 刷新基础信息；sync_database() 增量拉取行情/估值/分红入库
  - 组合筛选  : run_screening() 对勾选条件（"且"关系取交集）用 ThreadPoolExecutor 并行计算
  - 13个条件 : 每个条件一个独立函数（CONDITIONS 注册表），阈值全部可调（见 CONFIG）

数据源（本机 AKShare 1.18.94 实测可用，均带自动兜底）：
  全市场快照 : 新浪（失败回退名称代码表）    | 日K线 : 新浪（失败回退腾讯）
  估值历史   : 东财F10 stock_value_em（失败回退百度估值）
  分红事件   : 新浪 stock_history_dividend_detail（用于计算最近一年股息率）
  上市日期   : 上交所/深交所/北交所官方列表  | 板块 : 申万一级行业成分
"""
import concurrent.futures
import html as _html
import re
import socket
import threading
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

import stock_db as db

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None

# ---- JS 引擎线程安全补丁 ---------------------------------------------------
# akshare 多处接口（新浪日K前复权因子、巨潮分红等）内部使用 V8(py_mini_racer)。
# 多线程并发实例化/执行 V8 在部分环境（如 Streamlit 脚本线程）会触发原生崩溃，
# 这里对 py_mini_racer.MiniRacer 做全局锁包装：实例化与 eval/call 全部串行化，
# JS 执行仅毫秒级，不影响并发吞吐。
_JS_LOCK = threading.Lock()
try:
    import py_mini_racer as _pmr
    _OrigMiniRacer = _pmr.MiniRacer

    class _SafeMiniRacer:
        def __init__(self, inner):
            self._inner = inner

        def eval(self, *args, **kwargs):
            with _JS_LOCK:
                return self._inner.eval(*args, **kwargs)

        def call(self, *args, **kwargs):
            with _JS_LOCK:
                return self._inner.call(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def _locked_mini_racer(*args, **kwargs):
        with _JS_LOCK:
            return _SafeMiniRacer(_OrigMiniRacer(*args, **kwargs))

    _pmr.MiniRacer = _locked_mini_racer
except Exception:  # pragma: no cover
    pass

# ============================================================================
# 配置区 —— 所有可调参数（侧边栏默认值来源于此，中文注释）
# ============================================================================
CONFIG = {
    # ---------- 数据与同步 ----------
    "DB_PATH": "stock_data.db",      # SQLite 数据库路径（可配置，支持相对/绝对路径）
    "MAX_WORKERS": 16,               # 数据同步/条件计算的并发线程数（接口限流时调小，如6~8）
    "REQUEST_TIMEOUT": 10,           # 单次请求超时秒数
    "RETRIES": 2,                    # 单只股票数据获取失败重试次数
    "PER_REQUEST_DELAY": 0.0,        # 每次请求后的额外等待秒数（被限流时调大）
    "AUTO_SYNC_MINUTES": 30,         # 启动时自动增量更新的最小间隔（分钟），0=每次运行都自动同步
    "VAL_REFRESH_DAYS": 3,           # 估值数据新鲜度（天）：超过则重拉（估值历史API无日期参数，返回全量后仅追加新日期）
    "DIV_REFRESH_DAYS": 7,           # 分红事件新鲜度（天）：超过则重拉
    "LIMIT_STOCKS": 0,               # 调试用：同步与筛选仅处理范围内前N只股票（按代码排序），0=全部
    # ---------- 通用排除（始终生效） ----------
    "NEW_STOCK_DAYS": 365,           # 排除上市不满该自然天数的次新股
    "EXCLUDE_ST": True,              # 排除名称含 ST / *ST 的股票
    "EXCLUDE_EXTRA_KEYWORDS": ("退",),  # 名称含这些关键词一并排除（可改空元组）
    # ---------- 条件阈值 ----------
    "CHANNEL_DAYS": 60,              # 条件1：上升通道回归窗口（交易日）
    "CHANNEL_R2": 0.6,               # 条件1：上升通道线性拟合 R² 下限
    "PULLBACK_DAYS": 20,             # 条件2：回踩支撑观察窗口（交易日）
    "SUPPORT_MA": 20,                # 条件2：支撑均线周期（日）
    "PULLBACK_TOL": 2.0,             # 条件2：最低价与均线差值容差（%）
    "SMALL_YANG_DAYS": 5,            # 条件3：碎步小阳连续阳线天数
    "SMALL_YANG_MAX": 3.0,           # 条件3：每日涨幅上限（%）
    "W_BOTTOM_DAYS": 60,             # 条件4：W底形态观察窗口（交易日）
    "W_BOTTOM_TOL": 3.0,             # 条件4：双低点价格差容差（%）
    "DIVERGENCE_DAYS": 60,           # 条件6：底背离观察窗口（交易日）
    "RSI_DAYS": 14,                  # 条件6：RSI 计算周期
    "VAL_YEARS": 10,                 # 条件8/9：估值分位数回看年数
    "PE_PERCENTILE": 10.0,           # 条件8：PE-TTM 分位数阈值（%）
    "PB_PERCENTILE": 10.0,           # 条件9：PB-MRQ 分位数阈值（%）
    "MIN_VAL_ROWS": 120,             # 条件8/9：估值历史最少行数（约半年），不足则跳过
    "DIV_YIELD_MIN": 3.0,            # 条件10：最近一年股息率下限（%）
    "BAR_WINDOW": 300,               # 技术指标计算加载的K线行数（含均线/MACD/RSI预热，勿过小）
    # ---------- 缓存与链接 ----------
    "CACHE_SPOT": 1800,              # 全市场快照内存缓存秒数（结果展示的当前价/涨跌幅）
    "CACHE_LISTING": 86400,          # 上市日期内存缓存秒数
    "CACHE_SECTOR": 86400,           # 板块成分内存缓存秒数
    "STOCK_URL": "https://stockpage.10jqka.com.cn/{code}/",  # 股票名称点击跳转链接（同花顺个股页，code=6位代码）
}

# ============================================================================
# 基础工具
# ============================================================================
_cache = {}
_cache_lock = threading.Lock()
_last_sync = {}          # {db_path: 上次自动同步时间戳}
_last_sync_lock = threading.Lock()


def _cache_get(key, ttl_seconds, loader):
    with _cache_lock:
        hit = _cache.get(key)
        if hit and (time.time() - hit[0]) < ttl_seconds:
            return hit[1]
    value = loader()
    if value is not None:
        with _cache_lock:
            _cache[key] = (time.time(), value)
    return value


def _set_socket_timeout(timeout):
    try:
        socket.setdefaulttimeout(timeout)
    except Exception:
        pass


def _call_ak(name, fn, retries, timeout, delay):
    """带重试与退避的 AKShare 调用封装。"""
    _set_socket_timeout(timeout)
    last = None
    for i in range(max(1, retries + 1)):
        try:
            result = fn()
            if delay > 0:
                time.sleep(delay)
            return result
        except Exception as e:
            last = e
            time.sleep(min(2.0, 0.4 * (i + 1)))
    raise RuntimeError(f"{name} 获取失败: {type(last).__name__}: {last}")


def _warn(logs, lock, msg):
    if logs is None:
        return
    with lock:
        if len(logs) < 50:
            logs.append(msg)


def _need_ak():
    if ak is None:
        raise RuntimeError("未安装 akshare，请先执行: pip install akshare")


def norm_code(code):
    return re.sub(r"^[a-zA-Z]+", "", str(code)).strip()


def exchange_of(code):
    """代码 -> 交易所（SH 沪 / SZ 深 / BJ 北）。"""
    c = norm_code(code)
    if c.startswith(("4", "8", "92")):
        return "BJ"
    if c.startswith("6"):
        return "SH"
    if c.startswith(("0", "2", "3")):
        return "SZ"
    return "?"


def to_sina_symbol(code):
    c = norm_code(code)
    if c.startswith(("92", "4", "8")):
        return "bj" + c
    if c.startswith("6"):
        return "sh" + c
    return "sz" + c


def stock_url(code):
    return CONFIG["STOCK_URL"].format(code=norm_code(code))


def stock_link_html(code, name):
    """股票名称超链接（新标签页打开同花顺个股页，不显示原始URL）。"""
    return ('<a href="{url}" target="_blank" rel="noopener" '
            'style="color:#1a5fd0;text-decoration:none;font-weight:600;">{name}</a>').format(
        url=stock_url(code), name=_html.escape(str(name)))


def df_to_html_table(df, columns=None, link_code_col="代码", link_name_col="名称",
                     num_fmt="{:.2f}", max_rows=None):
    """把结果 DataFrame 渲染为 HTML 表格；股票名称列为可点击超链接。"""
    if df is None or df.empty:
        return "<p style='color:#888'>暂无符合条件的结果。</p>"
    d = df.copy()
    if link_name_col in d.columns and link_code_col in d.columns:
        d[link_name_col] = [stock_link_html(str(r[link_code_col]), str(r[link_name_col]))
                            for _, r in d.iterrows()]
    cols = [link_name_col] + [c for c in (columns or []) if c != link_name_col]
    cols = [c for c in cols if c in d.columns]
    if link_code_col in d.columns and link_code_col not in cols:
        cols.append(link_code_col)
    if max_rows:
        d = d.head(max_rows)
    parts = [
        "<div style='overflow-x:auto;'><table style='border-collapse:collapse;"
        "font-size:13px;font-family:Consolas,Microsoft YaHei,sans-serif;'>",
        "<thead><tr style='background:#f0f4f8;'>"
        + "".join(f"<th style='border:1px solid #d5dce5;padding:6px 10px;white-space:nowrap;'>{_html.escape(str(c))}</th>"
                  for c in cols)
        + "</tr></thead><tbody>",
    ]
    for _, row in d.iterrows():
        tds = []
        for c in cols:
            if c == link_name_col:
                s = str(row[c])  # 预构建的超链接HTML，原样输出保证可点击
                tds.append(f"<td style='border:1px solid #e3e9f0;padding:5px 10px;"
                           f"white-space:normal;'>{s}</td>")
                continue
            v = row[c]
            if pd.isna(v):
                s = ""
            elif isinstance(v, (int, float, np.floating, np.integer)):
                if float(v).is_integer():
                    s = str(int(v))
                else:
                    s = num_fmt.format(v)
            else:
                s = _html.escape(str(v)).replace("\n", "<br>")
            tds.append(f"<td style='border:1px solid #e3e9f0;padding:5px 10px;"
                       f"white-space:normal;'>{s}</td>")
        parts.append("<tr style='background:#fff;'>" + "".join(tds) + "</tr>")
    parts.append("</tbody></table></div>")
    return "".join(parts)


# ============================================================================
# 数据获取（AKShare）
# ============================================================================
def get_spot_list(timeout=None, retries=None, delay=None, logs=None):
    """全部A股快照：code/name/price(最新价)/change_pct(涨跌幅%)。

    新浪快照失败时自动回退到名称代码表（价格与涨跌幅为空）。
    """
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay
    lock = threading.Lock()

    def loader():
        try:
            df = _call_ak("新浪全市场快照", ak.stock_zh_a_spot, retries, timeout, delay)
            df = df.rename(columns={"代码": "raw_code", "名称": "name",
                                    "最新价": "price", "涨跌幅": "change_pct"})
            df = df[df["raw_code"].astype(str).str.match(r"^(sh|sz|bj)\d{6}$", na=False)]
            df["code"] = df["raw_code"].apply(norm_code)
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df["change_pct"] = pd.to_numeric(df["change_pct"], errors="coerce")
            return df[["code", "name", "price", "change_pct"]].reset_index(drop=True)
        except Exception as e:
            _warn(logs, lock, f"新浪全市场快照失败，回退到名称代码表（价格/涨跌幅将按日K计算）: {e}")
            df = _call_ak("A股名称代码表(快照兜底)", ak.stock_info_a_code_name,
                          retries, timeout, delay)
            df = df.rename(columns={"code": "code", "name": "name"})
            df["code"] = df["code"].apply(norm_code)
            df["name"] = df["name"].astype(str).str.strip()
            df["price"] = float("nan")
            df["change_pct"] = float("nan")
            return df[["code", "name", "price", "change_pct"]].reset_index(drop=True)

    return _cache_get("spot", CONFIG["CACHE_SPOT"], loader)


def get_listing_dates(timeout=None, retries=None, delay=None, logs=None):
    """{6位代码: datetime.date 上市日期}（上交所/深交所/北交所官方列表）。"""
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay
    lock = threading.Lock()

    def loader():
        mapping = {}
        for symbol in ("主板A股", "科创板"):
            try:
                df = _call_ak(f"上交所上市列表({symbol})",
                              lambda s=symbol: ak.stock_info_sh_name_code(symbol=s),
                              retries, timeout, delay)
                if "证券代码" in df.columns and "上市日期" in df.columns:
                    for _, r in df.iterrows():
                        d = pd.to_datetime(r["上市日期"], errors="coerce")
                        if pd.notna(d):
                            mapping[norm_code(r["证券代码"])] = d.date()
            except Exception as e:
                _warn(logs, lock, f"上交所上市列表({symbol})失败: {e}")
        try:
            df = _call_ak("深交所A股列表", lambda: ak.stock_info_sz_name_code(symbol="A股列表"),
                          retries, timeout, delay)
            if "A股代码" in df.columns and "A股上市日期" in df.columns:
                for _, r in df.iterrows():
                    d = pd.to_datetime(r["A股上市日期"], errors="coerce")
                    if pd.notna(d):
                        mapping[norm_code(r["A股代码"])] = d.date()
        except Exception as e:
            _warn(logs, lock, f"深交所A股列表失败: {e}")
        try:
            df = _call_ak("北交所上市列表", ak.stock_info_bj_name_code, retries, timeout, delay)
            if "证券代码" in df.columns and "上市日期" in df.columns:
                for _, r in df.iterrows():
                    d = pd.to_datetime(r["上市日期"], errors="coerce")
                    if pd.notna(d):
                        mapping[norm_code(r["证券代码"])] = d.date()
        except Exception as e:
            _warn(logs, lock, f"北交所上市列表失败: {e}")
        return mapping

    return _cache_get("listing", CONFIG["CACHE_LISTING"], loader)


_SW_FALLBACK = {
    "801010": "农林牧渔", "801030": "基础化工", "801040": "钢铁", "801050": "有色金属",
    "801080": "电子", "801110": "家用电器", "801120": "食品饮料", "801130": "纺织服饰",
    "801140": "轻工制造", "801150": "医药生物", "801160": "公用事业", "801170": "交通运输",
    "801180": "房地产", "801200": "商贸零售", "801210": "社会服务", "801230": "综合",
    "801710": "建筑材料", "801720": "建筑装饰", "801730": "电力设备", "801740": "国防军工",
    "801750": "计算机", "801760": "传媒", "801770": "通信", "801780": "银行", "801790": "非银金融",
    "801880": "汽车", "801890": "机械设备", "801950": "煤炭", "801960": "石油石化",
    "801970": "环保", "801980": "美容护理",
}


def get_board_list(db_path=None, timeout=None, retries=None, delay=None):
    """申万一级行业列表 [(板块代码, 板块名)]（内存缓存24小时）。

    网络失败时依次回退：本地 SQLite 已存板块（db_path 提供时）→ 内置31行业。
    """
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay

    def loader():
        try:
            info = _call_ak("申万一级行业列表", ak.sw_index_first_info, retries, timeout, delay)
            code_col = next((c for c in info.columns if "代码" in str(c)), None)
            name_col = next((c for c in info.columns if "名称" in str(c)), None)
            if code_col and name_col:
                return [(str(r[code_col]).split(".")[0], str(r[name_col]))
                        for _, r in info.iterrows()]
        except Exception:
            pass
        if db_path:
            try:
                conn = db.connect(db_path)
                names = db.get_stored_boards(conn)
                conn.close()
                code_of = {n: c for c, n in _SW_FALLBACK.items()}
                if names:
                    return [(code_of.get(n, ""), n) for n in names]
            except Exception:
                pass
        return list(_SW_FALLBACK.items())

    return _cache_get("board_list", CONFIG["CACHE_SECTOR"], loader)


def ensure_sector_map(db_path, cfg, boards=None, refresh=False,
                      progress_cb=None, logs=None):
    """板块映射（代码->申万一级行业）落盘缓存，只抓取缺失板块的成分。

    boards: 板块名列表；None=全部行业。refresh=True 清空缓存重抓。
    抓取结果存入 SQLite（sector_map/sector_boards 表），跨进程、跨天复用，
    之后每次运行无需再网络抓取（准备阶段从约40秒降至毫秒级）。
    返回 {代码: 板块名}（库中全量映射）。
    """
    logs = [] if logs is None else logs
    lock = threading.Lock()
    conn = db.connect(db_path)
    try:
        db.ensure_schema(conn)
        all_boards = get_board_list(db_path, cfg.get("REQUEST_TIMEOUT"), cfg.get("RETRIES"),
                                    cfg.get("PER_REQUEST_DELAY"))
        code_of = {name: code for code, name in all_boards}
        need_names = ([n for _, n in all_boards] if not boards
                      else [b for b in boards if b in code_of])
        if refresh:
            # 定向刷新：只清除所选板块的缓存（未选板块不受影响）
            db.remove_sectors(conn, need_names)
            _warn(logs, lock, f"已清除板块缓存：{'、'.join(need_names) or '全部'}，将重新抓取")
        stored = set(db.get_stored_boards(conn))
        missing = [n for n in need_names if n not in stored]
        failed = []
        if missing:
            timeout = cfg.get("REQUEST_TIMEOUT")
            retries = cfg.get("RETRIES")
            delay = cfg.get("PER_REQUEST_DELAY")
            total = len(missing)
            for i, bname in enumerate(missing):
                if progress_cb:
                    progress_cb(i + 1, total, f"准备板块映射：抓取「{bname}」成分")
                mapping = {}
                try:
                    cons = _call_ak(f"申万行业成分({bname})",
                                    lambda c=code_of[bname]: ak.index_component_sw(symbol=c),
                                    retries, timeout, delay)
                    cc = next((c for c in cons.columns if "代码" in str(c)), None)
                    if cc:
                        for _, r in cons.iterrows():
                            mapping[norm_code(r[cc])] = bname
                except Exception as e:
                    _warn(logs, lock, f"申万行业成分({bname})失败: {e}")
                if mapping:
                    db.upsert_sector_map(conn, mapping)
                    db.mark_boards(conn, [bname])
                else:
                    failed.append(bname)
            if failed:
                # 申万接口间歇性失败常见，对失败板块再补一轮重试
                retry_failed = []
                for bname in failed:
                    mapping = {}
                    try:
                        time.sleep(0.8)
                        cons = _call_ak(f"申万行业成分·重试({bname})",
                                        lambda c=code_of[bname]: ak.index_component_sw(symbol=c),
                                        retries, timeout, delay)
                        cc = next((c for c in cons.columns if "代码" in str(c)), None)
                        if cc:
                            for _, r in cons.iterrows():
                                mapping[norm_code(r[cc])] = bname
                    except Exception as e:
                        _warn(logs, lock, f"申万行业成分({bname})二次重试失败: {e}")
                    if mapping:
                        db.upsert_sector_map(conn, mapping)
                        db.mark_boards(conn, [bname])
                    else:
                        retry_failed.append(bname)
                if retry_failed:
                    _warn(logs, lock,
                          f"⚠️ 以下板块成分抓取失败（对应板块的范围筛选将不可用，"
                          f"请点击「🔄 刷新板块映射」重试）：{'、'.join(retry_failed)}")
            _warn(logs, lock, f"板块映射：本次抓取 {len(missing)} 个行业（{len(stored)} 个来自本地缓存）")
        # 北交所官方"所属行业"兜底（首次建库时补一次，覆盖申万未收录的北证股票）
        if refresh or not stored:
            try:
                bj = _call_ak("北交所上市列表(行业兜底)", ak.stock_info_bj_name_code,
                              cfg.get("RETRIES"), cfg.get("REQUEST_TIMEOUT"),
                              cfg.get("PER_REQUEST_DELAY"))
                bj_map = {}
                if "证券代码" in bj.columns and "所属行业" in bj.columns:
                    for _, r in bj.iterrows():
                        ind = str(r["所属行业"]).strip()
                        if ind and ind.lower() != "nan":
                            bj_map[norm_code(r["证券代码"])] = ind
                if bj_map:
                    existing = db.read_sector_map(conn)
                    bj_map = {c: existing.get(c, s) for c, s in bj_map.items()}
                    db.upsert_sector_map(conn, bj_map)
            except Exception:
                pass
        return db.read_sector_map(conn)
    finally:
        conn.close()


def fetch_daily_bars(code, start_date=None, end_date=None,
                     timeout=None, retries=None, delay=None):
    """日K线（前复权，新浪源，失败回退腾讯）。

    start_date/end_date: datetime.date 或 None（None=默认取近约420自然日）。
    返回 DataFrame[date, open, high, low, close, volume]，失败返回 None。
    """
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay
    end = (end_date or date.today()).strftime("%Y%m%d")
    start = (start_date or (date.today() - timedelta(days=420))).strftime("%Y%m%d")

    def normalize(df):
        if df is None or "date" not in df.columns or df.empty:
            return None
        out = df[["date", "open", "high", "low", "close", "volume"]].copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        for c in ("open", "high", "low", "close", "volume"):
            out[c] = pd.to_numeric(out[c], errors="coerce")
        out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        return out if len(out) else None

    try:
        df = _call_ak(f"日K线({code})",
                      lambda: ak.stock_zh_a_daily(symbol=to_sina_symbol(code),
                                                  start_date=start, end_date=end,
                                                  adjust="qfq"),
                      retries, timeout, delay)
        return normalize(df)
    except Exception:
        try:
            df = _call_ak(f"日K线·腾讯({code})",
                          lambda: ak.stock_zh_a_hist_tx(symbol=to_sina_symbol(code),
                                                        start_date=start, end_date=end,
                                                        adjust="qfq"),
                          retries, timeout, delay)
            return normalize(df)
        except Exception:
            return None


def fetch_valuation_history(code, timeout=None, retries=None, delay=None):
    """估值历史 DataFrame[数据日期, PE(TTM), 市净率]（东财F10，失败回退百度估值周频）。"""
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay
    try:
        df = _call_ak(f"个股估值({code})", lambda: ak.stock_value_em(symbol=norm_code(code)),
                      retries, timeout, delay)
        need = ["数据日期", "PE(TTM)", "市净率"]
        if df is None or not all(c in df.columns for c in need):
            raise RuntimeError("估值数据列缺失")
        out = df[need].copy()
        out["数据日期"] = pd.to_datetime(out["数据日期"], errors="coerce")
        for c in ("PE(TTM)", "市净率"):
            out[c] = pd.to_numeric(out[c], errors="coerce")
        out = out.dropna(subset=["数据日期"]).sort_values("数据日期").reset_index(drop=True)
        return out if len(out) else None
    except Exception:
        try:
            pe = _call_ak(f"百度估值PE({code})",
                          lambda: ak.stock_zh_valuation_baidu(symbol=norm_code(code),
                                                              indicator="市盈率(TTM)", period="全部"),
                          retries, timeout, delay)
            pb = _call_ak(f"百度估值PB({code})",
                          lambda: ak.stock_zh_valuation_baidu(symbol=norm_code(code),
                                                              indicator="市净率", period="全部"),
                          retries, timeout, delay)
            pe = pe.rename(columns={"date": "数据日期", "value": "PE(TTM)"})
            pb = pb.rename(columns={"date": "数据日期", "value": "市净率"})
            out = pe.merge(pb, on="数据日期", how="outer").sort_values("数据日期")
            out["数据日期"] = pd.to_datetime(out["数据日期"], errors="coerce")
            for c in ("PE(TTM)", "市净率"):
                out[c] = pd.to_numeric(out[c], errors="coerce")
            out = out.dropna(subset=["数据日期"]).reset_index(drop=True)
            return out if len(out) else None
        except Exception:
            return None


def fetch_dividend_events(code, timeout=None, retries=None, delay=None):
    """分红事件 [(datetime.date 除权日/派息日, float 每10股派息)]。

    主源：巨潮 stock_dividend_cninfo；兜底：新浪分红明细。全部失败返回 None。
    """
    _need_ak()
    timeout = timeout or CONFIG["REQUEST_TIMEOUT"]
    retries = CONFIG["RETRIES"] if retries is None else retries
    delay = CONFIG["PER_REQUEST_DELAY"] if delay is None else delay
    try:
        df = _call_ak(f"巨潮分红({code})",
                      lambda: ak.stock_dividend_cninfo(symbol=norm_code(code)),
                      retries, timeout, delay)
        if df is None or df.empty or "派息比例" not in df.columns:
            return []
        out = []
        for _, r in df.iterrows():
            cash = pd.to_numeric(r["派息比例"], errors="coerce")
            if pd.isna(cash) or cash <= 0:
                continue
            d = pd.to_datetime(r.get("除权日"), errors="coerce")
            if pd.isna(d):
                d = pd.to_datetime(r.get("派息日"), errors="coerce")
            if pd.notna(d):
                out.append((d.date(), float(cash)))
        out.sort()
        return out
    except Exception:
        try:
            df = _call_ak(f"新浪分红({code})",
                          lambda: ak.stock_history_dividend_detail(symbol=norm_code(code),
                                                                   indicator="分红"),
                          retries, timeout, delay)
            if df is None or df.empty or "除权除息日" not in df.columns or "派息" not in df.columns:
                return []
            out = []
            for _, r in df.iterrows():
                if "进度" in df.columns and str(r.get("进度") or "").strip() not in ("实施", ""):
                    continue
                d = pd.to_datetime(r["除权除息日"], errors="coerce")
                cash = pd.to_numeric(r["派息"], errors="coerce")
                if pd.notna(d) and pd.notna(cash) and cash > 0:
                    out.append((d.date(), float(cash)))
            out.sort()
            return out
        except Exception:
            return None


# ============================================================================
# 指标计算
# ============================================================================
def _pct_rank(hist, current):
    """当前值在历史序列中的分位数（0~100）。"""
    s = pd.Series(hist, dtype="float64").dropna()
    if s.empty or pd.isna(current):
        return None
    return float((s <= current).mean() * 100.0)


def _calc_macd(close):
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return dif, dea, dif - dea


def _rsi(close, n=14):
    """RSI（SMA 方式）。"""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    ag = gain.rolling(n).mean()
    al = loss.rolling(n).mean()
    rsi = 100.0 - 100.0 / (1.0 + ag / al.replace(0, np.nan))
    rsi[(al == 0) & (ag > 0)] = 100.0
    rsi[(al == 0) & (ag == 0)] = 50.0
    return rsi


def _linreg_slope_r2(y):
    """线性回归斜率与 R²。"""
    y = np.asarray(y, dtype=float)
    if len(y) < 2 or np.isnan(y).any():
        return None, None
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = float(((y - y_pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return float(slope), r2


# ============================================================================
# 13 个筛选条件（每个条件一个独立函数，ctx 为单只股票的缓存数据）
# ctx = {"bars": 日K(近BAR_WINDOW行), "val": 估值历史, "div": 分红事件,
#        "meta": {code,name,sector,exchange,listing_date}}
# ============================================================================
def cond_rising_channel(ctx, cfg):
    """条件1 上升通道：近 CHANNEL_DAYS 日收盘价线性回归斜率>0 且 R²≥CHANNEL_R2。"""
    bars = ctx["bars"]
    n = int(cfg["CHANNEL_DAYS"])
    if bars is None or bars.empty or len(bars) < n:
        return False
    close = bars["close"].astype(float).iloc[-n:]
    slope, r2 = _linreg_slope_r2(close)
    return slope is not None and slope > 0 and r2 >= float(cfg["CHANNEL_R2"])


def cond_pullback_support(ctx, cfg):
    """条件2 回踩支撑确认：近 PULLBACK_DAYS 日内最低价曾贴近 SUPPORT_MA 均线
    （差值≤PULLBACK_TOL%），且最近一日收盘价重新站上该均线。"""
    bars = ctx["bars"]
    pn, ma_n = int(cfg["PULLBACK_DAYS"]), int(cfg["SUPPORT_MA"])
    if bars is None or bars.empty or len(bars) < pn + ma_n:
        return False
    close = bars["close"].astype(float)
    low = bars["low"].astype(float)
    ma = close.rolling(ma_n).mean()
    ma_w = ma.iloc[-pn:]
    low_w = low.iloc[-pn:]
    valid = (ma_w > 0) & ((low_w - ma_w).abs() / ma_w * 100.0 <= float(cfg["PULLBACK_TOL"]))
    dipped = bool(valid.any())
    last_ok = (pd.notna(ma.iloc[-1]) and pd.notna(close.iloc[-1])
               and float(close.iloc[-1]) > float(ma.iloc[-1]))
    return dipped and last_ok


def cond_small_yang(ctx, cfg):
    """条件3 碎步小阳：近 SMALL_YANG_DAYS 日连续阳线（收盘>开盘），每日涨幅≤SMALL_YANG_MAX%。
    注：涨幅按相对前一交易日收盘计算；如需排除假阳线可在调用处自行加"涨幅>0"约束。"""
    bars = ctx["bars"]
    n = int(cfg["SMALL_YANG_DAYS"])
    if bars is None or bars.empty or len(bars) < n + 1:
        return False
    seg = bars.iloc[-(n + 1):]
    close = seg["close"].astype(float)
    open_ = seg["open"].astype(float)
    yang = (close > open_).iloc[1:]
    pct = (close.pct_change() * 100.0).iloc[1:]
    return bool(yang.all()) and bool((pct <= float(cfg["SMALL_YANG_MAX"])).all())


def cond_w_bottom(ctx, cfg):
    """条件4 小步上扬W底：近 W_BOTTOM_DAYS 日内两个低点价格差≤W_BOTTOM_TOL%，
    间隔≥10个交易日，且当前价突破两低点之间的反弹高点。"""
    bars = ctx["bars"]
    n = int(cfg["W_BOTTOM_DAYS"])
    if bars is None or bars.empty or len(bars) < max(n, 12):
        return False
    seg = bars.iloc[-n:]
    low = seg["low"].astype(float).to_numpy()
    high = seg["high"].astype(float).to_numpy()
    close_last = float(bars["close"].iloc[-1])
    tol = float(cfg["W_BOTTOM_TOL"]) / 100.0
    order = low.argsort()
    i1 = int(order[0])
    for k in range(1, len(order)):
        i2 = int(order[k])
        if abs(i1 - i2) < 10:
            continue
        base = float(low[i1])
        if base <= 0 or abs(float(low[i2]) - base) / base > tol:
            continue
        lo, hi = sorted((i1, i2))
        if hi - lo < 2:
            continue
        rebound = float(high[lo + 1:hi].max())
        if close_last > rebound:
            return True
    return False


def cond_three_lines_bloom(ctx, cfg):
    """条件5 三线开花：5日均线>10日均线>20日均线（多头排列），且三线斜率均>0。"""
    bars = ctx["bars"]
    if bars is None or bars.empty or len(bars) < 22:
        return False
    close = bars["close"].astype(float)
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    if any(pd.isna(x) for x in (ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1],
                                ma5.iloc[-2], ma10.iloc[-2], ma20.iloc[-2])):
        return False
    aligned = ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]
    slopes = (ma5.iloc[-1] - ma5.iloc[-2] > 0 and
              ma10.iloc[-1] - ma10.iloc[-2] > 0 and
              ma20.iloc[-1] - ma20.iloc[-2] > 0)
    return bool(aligned and slopes)


def cond_divergence(ctx, cfg):
    """条件6 日线底背离：近 DIVERGENCE_DAYS 日内股价创出新低，但 MACD 的 DIF
    或 RSI 未创新低（当前低点处指标值 > 前一个低点处指标值）。"""
    bars = ctx["bars"]
    n, rn = int(cfg["DIVERGENCE_DAYS"]), int(cfg["RSI_DAYS"])
    if bars is None or bars.empty or len(bars) < n + 60:
        return False
    close = bars["close"].astype(float)
    low = bars["low"].astype(float)
    dif, _, _ = _calc_macd(close)
    rsi = _rsi(close, rn)
    seg = slice(-n, None)
    arr = low.iloc[seg].to_numpy()
    if len(arr) < 3:
        return False
    valleys = [i for i in range(1, len(arr) - 1)
               if arr[i] <= arr[i - 1] and arr[i] < arr[i + 1]]
    if arr[-1] < arr[-2]:  # 窗口末端允许作为当前低点
        valleys.append(len(arr) - 1)
    valleys = sorted(set(valleys))
    if len(valleys) < 2:
        return False
    j, i = valleys[-2], valleys[-1]
    if not (arr[i] < arr[j]):  # 股价创出新低
        return False
    di, dj = dif.iloc[seg].iloc[i], dif.iloc[seg].iloc[j]
    ri, rj = rsi.iloc[seg].iloc[i], rsi.iloc[seg].iloc[j]
    if any(pd.isna(x) for x in (di, dj, ri, rj)):
        return False
    return bool(di > dj or ri > rj)


def cond_low_golden_cross(ctx, cfg):
    """条件7 日线低位金叉：MACD 的 DIF 与 DEA 在零轴下方（DIF<0 且 DEA<0）
    发生金叉（前一日 DIF<DEA，当日 DIF≥DEA）。"""
    bars = ctx["bars"]
    if bars is None or bars.empty or len(bars) < 40:
        return False
    close = bars["close"].astype(float)
    dif, dea, _ = _calc_macd(close)
    d1, d0 = dif.iloc[-2], dif.iloc[-1]
    e1, e0 = dea.iloc[-2], dea.iloc[-1]
    if any(pd.isna(x) for x in (d1, d0, e1, e0)):
        return False
    return bool(d1 < e1 and d0 >= e0 and d0 < 0 and e0 < 0)


def cond_pe_low(ctx, cfg):
    """条件8 PE低估：当前 PE-TTM 在近 VAL_YEARS 年历史分位数 ≤ PE_PERCENTILE 且 PE>0。"""
    val = ctx["val"]
    if val is None or val.empty:
        return False
    n = int(float(cfg["VAL_YEARS"]) * 250)
    v = val.tail(n)
    if len(v) < int(cfg["MIN_VAL_ROWS"]):
        return False
    cur = float(v["pe_ttm"].iloc[-1])
    if pd.isna(cur) or cur <= 0:
        return False
    pct = _pct_rank(v["pe_ttm"].iloc[:-1], cur)
    return pct is not None and pct <= float(cfg["PE_PERCENTILE"])


def cond_pb_low(ctx, cfg):
    """条件9 PB低估：当前 PB-MRQ 在近 VAL_YEARS 年历史分位数 ≤ PB_PERCENTILE 且 PB>0。"""
    val = ctx["val"]
    if val is None or val.empty:
        return False
    n = int(float(cfg["VAL_YEARS"]) * 250)
    v = val.tail(n)
    if len(v) < int(cfg["MIN_VAL_ROWS"]):
        return False
    cur = float(v["pb"].iloc[-1])
    if pd.isna(cur) or cur <= 0:
        return False
    pct = _pct_rank(v["pb"].iloc[:-1], cur)
    return pct is not None and pct <= float(cfg["PB_PERCENTILE"])


def cond_high_dividend(ctx, cfg):
    """条件10 高股息率：最近一年（相对最新K线日期）现金股息/当前价 ≥ DIV_YIELD_MIN%。"""
    bars, div = ctx["bars"], ctx["div"]
    if bars is None or bars.empty or not div:
        return False
    last_date = bars["date"].iloc[-1].date()
    cutoff = last_date - timedelta(days=365)
    dps = sum(c for d, c in div if cutoff <= d <= last_date) / 10.0
    close = float(bars["close"].iloc[-1])
    if dps <= 0 or pd.isna(close) or close <= 0:
        return False
    return dps / close * 100.0 >= float(cfg["DIV_YIELD_MIN"])


def _exchange_cond(ctx, target):
    meta = ctx["meta"] or {}
    ex = str(meta.get("exchange") or "") or exchange_of(meta.get("code") or "")
    return ex == target


def cond_exchange_sz(ctx, cfg):
    """条件11 深证：深交所股票（代码以 0/3 开头）。"""
    return _exchange_cond(ctx, "SZ")


def cond_exchange_sh(ctx, cfg):
    """条件12 沪证：上交所股票（代码以 6 开头）。"""
    return _exchange_cond(ctx, "SH")


def cond_exchange_bj(ctx, cfg):
    """条件13 北证：北交所股票（代码以 8/4 开头，含92新代码段）。"""
    return _exchange_cond(ctx, "BJ")


# 条件注册表：key -> {label, desc, fn}（页面多选框顺序与此一致）
CONDITIONS = {
    "rising_channel":   {"label": "① 上升通道", "desc": f"近{CONFIG['CHANNEL_DAYS']}日收盘价线性回归斜率>0且R²≥{CONFIG['CHANNEL_R2']}",
                         "fn": cond_rising_channel},
    "pullback_support": {"label": "② 回踩支撑确认", "desc": f"近{CONFIG['PULLBACK_DAYS']}日内下探{CONFIG['SUPPORT_MA']}日均线({CONFIG['PULLBACK_TOL']}%容差)后收盘重新站上",
                         "fn": cond_pullback_support},
    "small_yang":       {"label": "③ 碎步小阳", "desc": f"近{CONFIG['SMALL_YANG_DAYS']}日连续阳线且每日涨幅≤{CONFIG['SMALL_YANG_MAX']}%",
                         "fn": cond_small_yang},
    "w_bottom":         {"label": "④ 小步上扬W底", "desc": f"近{CONFIG['W_BOTTOM_DAYS']}日双低点差≤{CONFIG['W_BOTTOM_TOL']}%、间隔≥10日且突破颈线",
                         "fn": cond_w_bottom},
    "three_lines_bloom": {"label": "⑤ 三线开花", "desc": "5日>10日>20日均线多头排列且三线斜率均>0",
                          "fn": cond_three_lines_bloom},
    "divergence":       {"label": "⑥ 日线底背离", "desc": f"近{CONFIG['DIVERGENCE_DAYS']}日股价新低但DIF/RSI未新低",
                         "fn": cond_divergence},
    "low_golden_cross": {"label": "⑦ 日线低位金叉", "desc": "MACD在零轴下方金叉（DIF<0且DEA<0）",
                         "fn": cond_low_golden_cross},
    "pe_low":           {"label": "⑧ PE低估", "desc": f"PE-TTM近{CONFIG['VAL_YEARS']}年分位≤{CONFIG['PE_PERCENTILE']}%且PE>0",
                         "fn": cond_pe_low},
    "pb_low":           {"label": "⑨ PB低估", "desc": f"PB-MRQ近{CONFIG['VAL_YEARS']}年分位≤{CONFIG['PB_PERCENTILE']}%且PB>0",
                         "fn": cond_pb_low},
    "high_dividend":    {"label": "⑩ 高股息率", "desc": f"最近一年股息率≥{CONFIG['DIV_YIELD_MIN']}%",
                         "fn": cond_high_dividend},
    "exchange_sz":      {"label": "⑪ 深证", "desc": "深交所股票（代码以0/3开头）", "fn": cond_exchange_sz},
    "exchange_sh":      {"label": "⑫ 沪证", "desc": "上交所股票（代码以6开头）", "fn": cond_exchange_sh},
    "exchange_bj":      {"label": "⑬ 北证", "desc": "北交所股票（代码以8/4开头）", "fn": cond_exchange_bj},
}


# ============================================================================
# 数据同步：基础信息刷新 + 增量入库
# ============================================================================
def prepare_meta(db_path, cfg, refresh_sector=False, progress_cb=None, logs=None):
    """刷新 meta 表：**全市场常驻**（范围不裁剪 meta）。

    交易所/板块范围在 sync_database / run_screening 时作为过滤器生效：
    切换范围零重拉、秒级生效，且不存在"板块成分缺失导致空表"的失效路径。
    板块映射走本地 SQLite 缓存，重复运行几乎零网络开销。
    """
    logs = [] if logs is None else logs
    lock = threading.Lock()
    timeout, retries, delay = (cfg.get("REQUEST_TIMEOUT"), cfg.get("RETRIES"),
                               cfg.get("PER_REQUEST_DELAY"))
    if progress_cb:
        progress_cb(0, 4, "准备基础数据：加载全市场快照")
    try:
        spot = get_spot_list(timeout, retries, delay, logs=logs)
    except Exception as e:
        raise RuntimeError(f"全市场快照获取失败: {e}")
    if progress_cb:
        progress_cb(1, 4, "准备基础数据：加载上市日期")
    try:
        listing = get_listing_dates(timeout, retries, delay, logs=logs)
    except Exception as e:
        listing = {}
        _warn(logs, lock, f"上市日期获取失败，次新股过滤将放宽: {e}")
    if progress_cb:
        progress_cb(2, 4, "准备基础数据：加载板块映射（本地缓存）")
    try:
        sector_map = ensure_sector_map(db_path, cfg, boards=None,
                                       refresh=refresh_sector,
                                       progress_cb=progress_cb, logs=logs)
    except Exception as e:
        sector_map = {}
        _warn(logs, lock, f"板块映射获取失败，板块列将为'未分类': {e}")
    rows = []
    for _, r in spot.iterrows():
        code, name = str(r["code"]), str(r["name"])
        rows.append({
            "code": code, "name": name,
            "sector": sector_map.get(code, "未分类"),
            "exchange": exchange_of(code),
            "listing_date": listing.get(code),
        })
    meta = pd.DataFrame(rows, columns=["code", "name", "sector", "exchange", "listing_date"])
    meta = meta.sort_values("code").reset_index(drop=True)
    if meta.empty:
        raise RuntimeError("全市场快照为空：请检查网络后点击「🔄 增量更新数据」重试。"
                           "本地原有数据未被改动。")
    conn = db.connect(db_path)
    try:
        db.ensure_schema(conn)
        db.replace_meta(conn, meta)
    finally:
        conn.close()
    if progress_cb:
        progress_cb(4, 4, "准备基础数据：完成")
    # 板块覆盖率预警：映射缺失会导致"选板块=0只"，此处给出醒目提示
    if len(meta):
        n_un = int((meta["sector"] == "未分类").sum())
        cov = (1 - n_un / len(meta)) * 100
        if cov < 80:
            _warn(logs, lock,
                  f"⚠️ 板块覆盖率仅 {cov:.0f}%（{n_un} 只未分类）：板块成分抓取可能受限，"
                  f"选板块范围将无法匹配这些股票。请点击侧边栏「🔄 刷新板块映射」重试。")
    _warn(logs, lock, f"基础信息已刷新：全市场 {len(meta)} 只股票入库（范围在同步/筛选时过滤，切换无需重拉）")
    return meta


def _sync_one(args):
    """单只股票增量同步（线程池 worker）：日K增量 + 估值/分红按新鲜度刷新。

    kinds 控制同步内容（"daily"/"valuation"/"dividend"），可在侧边栏开关。
    """
    code, row, db_path, cfg, wconn, wlock, counters, kinds = args
    today = date.today()
    listing = None
    if row.get("listing_date"):
        try:
            listing = datetime.strptime(str(row["listing_date"]), "%Y-%m-%d").date()
        except ValueError:
            listing = None

    # ---- 日K线：本地最新日期之后的增量；无数据则从上市日起全量 ----
    if "daily" in kinds:
        with wlock:
            last = db.last_date(wconn, "daily", code)
        try:
            if last is None:
                start = listing or (today - timedelta(days=3650))
                bars = fetch_daily_bars(code, start_date=start,
                                        timeout=cfg.get("REQUEST_TIMEOUT"),
                                        retries=cfg.get("RETRIES"), delay=cfg.get("PER_REQUEST_DELAY"))
                if bars is not None and len(bars):
                    with wlock:
                        db.insert_daily(wconn, code, bars)
                    counters["daily_new"] += len(bars)
                    counters["full"] += 1
                elif bars is not None:
                    counters["daily_none"] += 1
                else:
                    counters["daily_fail"] += 1
            else:
                if (today - last).days >= 1:
                    start = last + timedelta(days=1)
                    if start >= today:
                        counters["daily_skip"] += 1  # 当日数据尚未生成（盘中/未收盘），跳过
                    else:
                        bars = fetch_daily_bars(code, start_date=start,
                                                timeout=cfg.get("REQUEST_TIMEOUT"),
                                                retries=cfg.get("RETRIES"), delay=cfg.get("PER_REQUEST_DELAY"))
                        if bars is not None and len(bars):
                            with wlock:
                                db.insert_daily(wconn, code, bars)
                            counters["daily_new"] += len(bars)
                        elif bars is not None:
                            counters["daily_none"] += 1
                        else:
                            counters["daily_fail"] += 1
                else:
                    counters["daily_skip"] += 1
        except Exception:
            counters["daily_fail"] += 1

    # ---- 估值：按 VAL_REFRESH_DAYS 新鲜度刷新（API 返回全量，仅追加新日期） ----
    if "valuation" in kinds:
        with wlock:
            lastv = db.last_date(wconn, "valuation", code)
        try:
            if lastv is None or (today - lastv).days > int(cfg.get("VAL_REFRESH_DAYS", 3)):
                val = fetch_valuation_history(code, cfg.get("REQUEST_TIMEOUT"),
                                              cfg.get("RETRIES"), cfg.get("PER_REQUEST_DELAY"))
                if val is not None and len(val):
                    if lastv is not None:
                        val = val[val["数据日期"].dt.date > lastv]
                    if len(val):
                        with wlock:
                            db.insert_valuation(wconn, code, val)
                        counters["val_new"] += len(val)
                    counters["val_full"] += 1
                else:
                    counters["val_fail"] += 1
            else:
                counters["val_skip"] += 1
        except Exception:
            counters["val_fail"] += 1

    # ---- 分红事件：按 DIV_REFRESH_DAYS 新鲜度刷新 ----
    if "dividend" in kinds:
        with wlock:
            lastd = db.last_date(wconn, "dividend", code)
        try:
            if lastd is None or (today - lastd).days > int(cfg.get("DIV_REFRESH_DAYS", 7)):
                events = fetch_dividend_events(code, cfg.get("REQUEST_TIMEOUT"),
                                               cfg.get("RETRIES"), cfg.get("PER_REQUEST_DELAY"))
                if events is None:
                    counters["div_fail"] += 1
                elif events:
                    with wlock:
                        db.insert_dividend(wconn, code, events)
                    counters["div_new"] += len(events)
                else:
                    counters["div_none"] += 1
            else:
                counters["div_skip"] += 1
        except Exception:
            counters["div_fail"] += 1
    return code


def filter_meta(meta, exchange=None, sectors=None):
    """按 交易所/板块 过滤 meta 行（范围过滤器，纯本地计算，零网络）。"""
    if exchange in ("SZ", "SH", "BJ"):
        meta = meta[meta["exchange"] == exchange]
    if sectors:
        meta = meta[meta["sector"].isin(set(sectors))]
    return meta.reset_index(drop=True)


def sync_database(db_path, cfg, force=False, kinds=("daily", "valuation", "dividend"),
                  exchange=None, sectors=None, progress_cb=None, logs=None):
    """按范围增量同步数据入库（范围外的数据保留在库中，切换范围无需重拉）。

    force=True 时仅清空**当前范围内**所选 kinds 的数据再全量拉取（不影响范围外缓存）。
    kinds 控制同步内容，可只同步日K以加快首次拉取。
    """
    logs = [] if logs is None else logs
    lock = threading.Lock()
    kinds = tuple(k for k in kinds if k in ("daily", "valuation", "dividend"))
    conn = db.connect(db_path)
    try:
        meta = db.read_meta(conn)
        if meta.empty:
            raise RuntimeError("meta 表为空：基础数据未就绪，请先点击侧边栏「🔄 增量更新数据」")
        meta = filter_meta(meta, exchange, sectors)
        scope_desc = (f"范围：{'全部A股' if not exchange else exchange}"
                      + (f" + 板块[{','.join(sectors)}]" if sectors else ""))
        rows = meta.to_dict("records")
        total = len(rows)
        limit = int(cfg.get("LIMIT_STOCKS") or 0)
        if limit > 0:
            rows = rows[:limit]
            total = len(rows)
        if total == 0:
            _warn(logs, lock, f"{scope_desc} 范围内没有股票（无需同步）")
            return {"full": 0, "daily_new": 0, "daily_none": 0, "daily_fail": 0, "daily_skip": 0,
                    "val_new": 0, "val_fail": 0, "val_skip": 0, "val_full": 0,
                    "div_new": 0, "div_none": 0, "div_skip": 0, "div_fail": 0}
        if force:
            codes = [r["code"] for r in rows]
            for t in kinds:
                db.wipe_table_codes(conn, t, codes)
            _warn(logs, lock, f"{scope_desc}：已清空范围内 {'/'.join(kinds)} 数据，开始强制全量刷新")
        counters = {"full": 0, "daily_new": 0, "daily_none": 0, "daily_fail": 0, "daily_skip": 0,
                    "val_new": 0, "val_fail": 0, "val_skip": 0, "val_full": 0,
                    "div_new": 0, "div_none": 0, "div_skip": 0, "div_fail": 0}
        mode = "全量" if force else "增量"
        scope_txt = "+".join(kinds) if kinds else "无"
        workers = max(1, int(cfg.get("MAX_WORKERS", 16)))
        if progress_cb:
            progress_cb(0, total, f"数据同步（{mode}·{scope_txt}）")
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_sync_one, (r["code"], r, db_path, cfg, conn, lock, counters, kinds)): r["code"]
                       for r in rows}
            for fut in concurrent.futures.as_completed(futures):
                done += 1
                if progress_cb and (done % 5 == 0 or done == total):
                    progress_cb(done, total, f"数据同步（{mode}·{scope_txt}）")
        if progress_cb:
            progress_cb(total, total, f"数据同步（{mode}·{scope_txt}）：完成")
        _warn(logs, lock,
              f"同步统计（{scope_desc}，{total} 只）：全量日K {counters['full']} 只；"
              f"新增日K {counters['daily_new']} 行；估值刷新 {counters['val_full']} 只({counters['val_new']} 行)；"
              f"分红刷新 {counters['div_new']} 条；失败：日K {counters['daily_fail']}/估值 {counters['val_fail']}/分红 {counters['div_fail']}")
        return counters
    finally:
        conn.close()


def should_auto_sync(db_path, cfg, scope_sig="*"):
    """启动时是否需要自动增量同步（数据库为空、范围变更或距上次同步超时）。"""
    with _last_sync_lock:
        t = _last_sync.get(db_path)
    if t is None:
        return True
    ts, sig = t
    if sig != scope_sig:  # 用户修改了数据范围 → 立即同步
        return True
    minutes = int(cfg.get("AUTO_SYNC_MINUTES", 30))
    if minutes <= 0:
        return True
    return (time.time() - ts) > minutes * 60


def mark_synced(db_path, scope_sig="*"):
    with _last_sync_lock:
        _last_sync[db_path] = (time.time(), scope_sig)


# ============================================================================
# 组合筛选：多条件并行计算 + 取交集
# ============================================================================
def _load_ctx(db_path, code, cfg, cache, clock):
    """加载单只股票的条件计算上下文（进程内缓存）。"""
    with clock:
        if code in cache:
            return cache[code]
    rconn = db.reader(db_path)
    bars = db.load_daily(rconn, code, int(cfg.get("BAR_WINDOW", 300)))
    val = db.load_valuation(rconn, code, int(float(cfg.get("VAL_YEARS", 10)) * 250) + 2)
    div = db.load_dividend_events(rconn, code)
    meta = db.load_meta_row(rconn, code) or {"code": code}
    ctx = {"bars": bars, "val": val, "div": div, "meta": meta}
    with clock:
        cache[code] = ctx
    return ctx


def _compute_condition(key, codes, db_path, cfg, cache, clock):
    """并行任务：计算单个条件命中的股票集合。"""
    fn = CONDITIONS[key]["fn"]
    hits = set()
    n_errors = 0
    for code in codes:
        try:
            ctx = _load_ctx(db_path, code, cfg, cache, clock)
            if fn(ctx, cfg):
                hits.add(code)
        except Exception:
            n_errors += 1
    return key, hits, n_errors


def load_universe(db_path, cfg, exchange=None, sectors=None, logs=None):
    """从 meta 表读取股票池：先按 交易所/板块 范围过滤，再应用通用排除规则。

    LIMIT_STOCKS 在范围内生效（调试用）。
    """
    logs = [] if logs is None else logs
    lock = threading.Lock()
    conn = db.reader(db_path)
    meta = db.read_meta(conn)
    if meta.empty:
        raise RuntimeError("数据库为空：请先在侧边栏执行「增量更新数据」或等待自动同步完成")
    meta = filter_meta(meta, exchange, sectors)
    today = date.today()
    new_stock_days = int(cfg.get("NEW_STOCK_DAYS", 365))
    exclude_st = bool(cfg.get("EXCLUDE_ST", True))
    extra_kw = tuple(cfg.get("EXCLUDE_EXTRA_KEYWORDS", ()) or ())
    n_st = n_new = n_extra = n_unknown = 0
    keep = []
    for _, r in meta.iterrows():
        name = str(r["name"])
        if exclude_st and "ST" in name.upper():
            n_st += 1
            continue
        if extra_kw and any(k in name for k in extra_kw):
            n_extra += 1
            continue
        ld = r.get("listing_date")
        if not ld:
            n_unknown += 1
        else:
            try:
                ld_d = datetime.strptime(str(ld), "%Y-%m-%d").date()
                if (today - ld_d).days < new_stock_days:
                    n_new += 1
                    continue
            except ValueError:
                n_unknown += 1
        keep.append(r["code"])
    limit = int(cfg.get("LIMIT_STOCKS") or 0)
    if limit > 0:
        keep = keep[:limit]
    _warn(logs, lock, f"通用排除：剔除ST {n_st} 只；上市不足{new_stock_days}天 {n_new} 只；"
                     f"其他关键词 {n_extra} 只；上市日期未知 {n_unknown} 只（保留）")
    return keep


def run_screening(db_path, condition_keys, cfg, exchange=None, sectors=None,
                  progress_cb=None, logs=None):
    """多条件组合筛选：先按 交易所/板块 范围过滤，再对勾选条件（"且"关系）并行计算取交集。

    返回 DataFrame[代码,名称,板块,当前价,涨跌幅(%),PE-TTM,触发条件列表,数据日期]，
    按 PE-TTM 由低到高排序（无PE的排最后）。
    """
    logs = [] if logs is None else logs
    lock = threading.Lock()
    cfg = {**CONFIG, **(cfg or {})}  # 合并默认值，保证所有条件阈值键齐全
    keys = [k for k in (condition_keys or []) if k in CONDITIONS]
    if not keys:
        raise ValueError("请至少勾选一个筛选条件")
    codes = load_universe(db_path, cfg, exchange, sectors, logs)
    total_codes = len(codes)
    scope_desc = (f"范围：{'全部A股' if not exchange else exchange}"
                  + (f" + 板块[{','.join(sectors)}]" if sectors else ""))
    _warn(logs, lock, f"筛选范围：{scope_desc}，{total_codes} 只股票，条件 {len(keys)} 个（且关系取交集）")

    cache, clock = {}, threading.Lock()
    n_workers = min(len(keys), max(1, int(cfg.get("MAX_WORKERS", 16))))
    cond_hits = {}
    n_errors = 0
    if progress_cb:
        progress_cb(0, len(keys), "并行计算条件")
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_compute_condition, k, codes, db_path, cfg, cache, clock): k
                   for k in keys}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            key, hits, errs = fut.result()
            cond_hits[key] = hits
            n_errors += errs
            done += 1
            if progress_cb:
                progress_cb(done, len(keys), f"并行计算条件：{CONDITIONS[key]['label']}")
    if progress_cb:
        progress_cb(len(keys), len(keys), "并行计算完成，汇总交集")

    hit_codes = sorted(set.intersection(*(cond_hits.values())))
    _warn(logs, lock, "各条件命中：" + "；".join(
        f"{CONDITIONS[k]['label']} {len(cond_hits[k])}只" for k in keys) +
        f"；交集 {len(hit_codes)} 只")
    if n_errors:
        _warn(logs, lock, f"条件计算中有 {n_errors} 只股票因数据缺失/异常被跳过")

    # ---- 组装结果行 ----
    rconn = db.reader(db_path)
    rows = []
    for code in hit_codes:
        bars = db.load_daily(rconn, code, 2)
        meta = db.load_meta_row(rconn, code) or {"code": code}
        price = change = pe = last_date = None
        if bars is not None and len(bars) >= 2:
            price = float(bars["close"].iloc[-1])
            prev = float(bars["close"].iloc[-2])
            if prev > 0:
                change = (price - prev) / prev * 100.0
            last_date = bars["date"].iloc[-1].strftime("%Y-%m-%d")
        val = db.load_valuation(rconn, code, 2)
        if val is not None and not val.empty:
            pe_v = float(val["pe_ttm"].iloc[-1])
            if pd.notna(pe_v):
                pe = pe_v
        rows.append({
            "代码": code, "名称": meta.get("name") or code,
            "板块": meta.get("sector") or "未分类",
            "当前价": price, "涨跌幅(%)": change, "PE-TTM": pe,
            "触发条件列表": "、".join(CONDITIONS[k]["label"] for k in keys),
            "数据日期": last_date,
        })
    out = pd.DataFrame(rows, columns=["代码", "名称", "板块", "当前价", "涨跌幅(%)",
                                      "PE-TTM", "触发条件列表", "数据日期"])

    # ---- 用实时快照刷新当前价/涨跌幅（失败时保留按日K计算的值） ----
    try:
        spot = get_spot_list(logs=logs)
        spot = spot.set_index("code")
        for i, r in out.iterrows():
            if r["代码"] in spot.index:
                sp = spot.loc[r["代码"]]
                if pd.notna(sp["price"]):
                    out.at[i, "当前价"] = float(sp["price"])
                if pd.notna(sp["change_pct"]):
                    out.at[i, "涨跌幅(%)"] = float(sp["change_pct"])
    except Exception as e:
        _warn(logs, lock, f"实时快照获取失败，当前价/涨跌幅按最新日K计算: {e}")

    if not out.empty:
        out = out.sort_values("PE-TTM", ascending=True, na_position="last").reset_index(drop=True)
    return out
