# -*- coding: utf-8 -*-
"""
app.py —— A股多条件组合选股（Streamlit 界面）
=============================================
启动：streamlit run app.py  或双击 maidenstart.bat

架构：本地 SQLite 缓存（stock_data.db，路径可配置）+ 增量更新 + 13条件并行筛选
  - 首次运行自动全量拉取历史数据入库（进度条展示，约10~20分钟）
  - 之后每次运行仅拉取本地最新日期之后的增量交易日数据
  - 页面顶部勾选条件（"且"关系取交集）→ 点击「开始筛选」→ 结果按 PE-TTM 升序展示
  - 股票名称为超链接，点击新标签页打开同花顺个股页；所有阈值在侧边栏可调

环境变量（可选）：STOCK_DB_PATH 覆盖数据库路径；STOCK_LIMIT 覆盖调试限量。
"""
import os
import random
import time

import pandas as pd
import streamlit as st

import stock_core as core
import stock_db as db

st.set_page_config(page_title="A股多条件组合选股", page_icon="📈", layout="wide")

_DB_DEFAULT = os.environ.get("STOCK_DB_PATH", core.CONFIG["DB_PATH"])
_LIMIT_DEFAULT = int(os.environ.get("STOCK_LIMIT", core.CONFIG["LIMIT_STOCKS"]))

_EXCHANGE_MAP = {"深证": "SZ", "沪证": "SH", "北证": "BJ"}

# 加载金句（进度条下方每5秒轮动一条）
SLOGANS = [
    "Dancing in roses",
    "做好人，买好股，得好报",
    "Made by P",
    "观察期货/可转债或可套利",
    "余钱好股不要慌，急钱差股需理性",
    "咨询机构会给予前瞻",
    "全面视察再买基",
    "来吾导夫先路",
    "Watching from TELESCOPE",
]


def _slogan():
    """按5秒时间窗随机轮动一条金句。"""
    return random.Random(int(time.time() // 5)).choice(SLOGANS)


def _make_progress(with_slogan=True):
    """构建 进度条 + 状态行 + 金句行 + 进度回调。"""
    prog = st.progress(0.0)
    hint = st.empty()
    slogan_ph = st.empty() if with_slogan else None

    def cb(done, total, msg=""):
        prog.progress(min(float(done) / max(total, 1), 1.0))
        hint.caption(f"{msg}　{done}/{total}")
        if slogan_ph is not None:
            slogan_ph.caption(f"✨ {_slogan()}")

    return prog, hint, slogan_ph, cb


def _scope_sig():
    ex = _EXCHANGE_MAP.get(st.session_state.get("scope_exchange", "全部A股"))
    sec = ",".join(sorted(st.session_state.get("scope_sectors", []) or []))
    return f"{ex or 'ALL'}|{sec}"


_stats_cache = {}


def _get_db_stats(db_path):
    """数据库统计（带进程内TTL缓存：2.8GB级库 COUNT 较慢，避免每次交互重复计算）。"""
    now = time.time()
    hit = _stats_cache.get(db_path)
    if hit and now - hit[0] < 30:
        return hit[1]
    conn = db.connect(db_path)
    try:
        db.ensure_schema(conn)
        stats = db.db_stats(conn)
    finally:
        conn.close()
    _stats_cache[db_path] = (now, stats)
    return stats


def _sync_kinds():
    kinds = []
    if st.session_state.get("sync_daily", True):
        kinds.append("daily")
    if st.session_state.get("sync_valuation", True):
        kinds.append("valuation")
    if st.session_state.get("sync_dividend", True):
        kinds.append("dividend")
    return tuple(kinds)


def _run_sync(db_path, cfg, force=False, refresh_sector=False):
    """执行 基础信息刷新 + 范围化数据同步（进度条 + 金句轮动 + 日志 + 异常处理）。"""
    logs = []
    with st.status("数据同步中…（范围越小越快；已缓存的部分自动跳过）", expanded=True) as status_ctx:
        prog, hint, slogan_ph, cb = _make_progress()
        try:
            core.prepare_meta(db_path, cfg, refresh_sector=refresh_sector,
                              progress_cb=cb, logs=logs)
            exchange = _EXCHANGE_MAP.get(st.session_state.get("scope_exchange", "全部A股"))
            sectors = st.session_state.get("scope_sectors", []) or None
            counters = core.sync_database(db_path, cfg, force=force, kinds=_sync_kinds(),
                                          exchange=exchange, sectors=sectors,
                                          progress_cb=cb, logs=logs)
            core.mark_synced(db_path, _scope_sig())
            status_ctx.update(label="✅ 数据同步完成", state="complete", expanded=False)
            st.session_state["sync_logs"] = logs
            st.session_state["last_sync_counters"] = counters
        except Exception as e:
            status_ctx.update(label=f"❌ 数据同步失败：{type(e).__name__}", state="error")
            st.error(f"❌ 同步失败：{type(e).__name__}: {e}")
            st.session_state["sync_logs"] = logs
        finally:
            prog.empty()
            hint.empty()
            slogan_ph.empty()


# ============================================================================
# 侧边栏：参数配置区（所有可调参数，带中文标签与默认值）
# ============================================================================
cfg = {}
db_path = str(st.session_state.get("cfg_DB_PATH", _DB_DEFAULT))
with st.sidebar:
    st.header("⚙️ 参数配置")
    with st.expander("🎯 数据范围（先选范围，拉取与检索更快）", expanded=True):
        st.selectbox(
            "证券市场", ["全部A股", "深证", "沪证", "北证"], key="scope_exchange",
            help="仅同步与筛选所选市场的股票：深证=0/3开头、沪证=6开头、北证=8/4/92开头。"
                 "已缓存的数据不会被删除，切换范围无需重拉。")
        board_options = []
        try:
            board_options = [n for _, n in core.get_board_list(db_path=db_path)]
        except Exception:
            pass
        st.multiselect(
            "所属板块（申万一级，可多选，留空=不限）", board_options, key="scope_sectors",
            help="板块与市场范围为且关系，多选板块为或关系；选板块后仅拉取这些行业的股票"
                 "（如「银行」仅约42只，秒级完成）")
        try:
            _conn = db.connect(db_path)
            _meta_all = db.read_meta(_conn)
            _conn.close()
            if not _meta_all.empty:
                _scoped = core.filter_meta(_meta_all,
                                           _EXCHANGE_MAP.get(st.session_state.get("scope_exchange", "全部A股")),
                                           st.session_state.get("scope_sectors") or None)
                if _scoped.empty and (st.session_state.get("scope_sectors") or []):
                    n_un = int((_meta_all["sector"] == "未分类").sum())
                    st.warning(f"所选板块在当前数据库无匹配（板块数据可能未就绪，"
                               f"全市场 {n_un} 只未分类）。请点击下方「🔄 刷新板块映射」重试。")
                else:
                    st.caption(f"当前范围：{len(_scoped)} 只（切换范围即时生效；数据未缓存时自动拉取，已有数据零重拉）")
            else:
                st.caption("数据库暂无基础信息：运行一次「🔄 增量更新数据」后此处显示范围股票数")
        except Exception:
            pass
        if st.button("🔄 刷新板块映射", key="btn_refresh_sector", use_container_width=True,
                     help="重新从网络抓取所选板块成分（一般无需使用；结果中板块异常时点击）"):
            _run_sync(db_path, cfg, force=False, refresh_sector=True)
            st.rerun()
        st.caption("范围作为过滤器生效：大范围已抓取的数据在小范围内直接复用，不会重新拉取。")
    with st.expander("🗄️ 数据与同步", expanded=True):
        cfg["DB_PATH"] = st.text_input(
            "SQLite 数据库路径", value=_DB_DEFAULT, key="cfg_DB_PATH",
            help="相对/绝对路径均可；更换路径后请执行「增量更新数据」或「强制全量刷新」")
        cfg["AUTO_SYNC_MINUTES"] = st.number_input(
            "自动增量更新间隔（分钟，0=每次运行都更新）", 0, 1440,
            int(core.CONFIG["AUTO_SYNC_MINUTES"]), 5, key="cfg_AUTO_SYNC_MINUTES")
        cfg["MAX_WORKERS"] = st.number_input(
            "并发线程 MAX_WORKERS", 1, 32, int(core.CONFIG["MAX_WORKERS"]), 1,
            key="cfg_MAX_WORKERS", help="数据同步与条件并行计算共用；接口限流时调小（如4~6）")
        cfg["RETRIES"] = st.number_input(
            "失败重试次数 RETRIES", 0, 5, int(core.CONFIG["RETRIES"]), 1, key="cfg_RETRIES")
        cfg["VAL_REFRESH_DAYS"] = st.number_input(
            "估值刷新间隔（天）", 1, 30, int(core.CONFIG["VAL_REFRESH_DAYS"]), 1,
            key="cfg_VAL_REFRESH_DAYS")
        cfg["DIV_REFRESH_DAYS"] = st.number_input(
            "分红刷新间隔（天）", 1, 90, int(core.CONFIG["DIV_REFRESH_DAYS"]), 1,
            key="cfg_DIV_REFRESH_DAYS")
        cfg["LIMIT_STOCKS"] = st.number_input(
            "调试限量 LIMIT_STOCKS（0=全部约5500只）", 0, 10000, _LIMIT_DEFAULT, 10,
            key="cfg_LIMIT_STOCKS", help="仅处理按代码排序的前N只，用于快速测试")
        st.caption("同步内容开关（关闭某项可显著加快拉取；对应的筛选条件将无法命中）")
        kc1, kc2, kc3 = st.columns(3)
        kc1.checkbox("日K线", value=True, key="sync_daily",
                     help="技术形态类条件（①~⑦）依赖日K线")
        kc2.checkbox("估值PE/PB", value=True, key="sync_valuation",
                     help="⑧⑨低估条件依赖估值历史")
        kc3.checkbox("分红股息率", value=True, key="sync_dividend",
                     help="⑩高股息率条件依赖分红事件")
    with st.expander("🚫 通用排除规则（始终生效）", expanded=False):
        cfg["NEW_STOCK_DAYS"] = st.number_input(
            "排除上市不足天数 NEW_STOCK_DAYS", 0, 5000,
            int(core.CONFIG["NEW_STOCK_DAYS"]), 5, key="cfg_NEW_STOCK_DAYS",
            help="默认365天：排除上市不满一年的次新股")
        cfg["EXCLUDE_ST"] = st.checkbox(
            "排除名称含 ST / *ST 的股票", value=bool(core.CONFIG["EXCLUDE_ST"]),
            key="cfg_EXCLUDE_ST")
    with st.expander("📐 条件阈值（全部可调）", expanded=False):
        c1, c2 = st.columns(2)
        cfg["CHANNEL_DAYS"] = c1.number_input("① 上升通道窗口(日)", 10, 500,
                                              int(core.CONFIG["CHANNEL_DAYS"]), 5, key="cfg_CHANNEL_DAYS")
        cfg["CHANNEL_R2"] = c1.number_input("① R² 下限", 0.0, 1.0,
                                            float(core.CONFIG["CHANNEL_R2"]), 0.05, key="cfg_CHANNEL_R2")
        cfg["PULLBACK_DAYS"] = c1.number_input("② 回踩观察窗口(日)", 3, 120,
                                               int(core.CONFIG["PULLBACK_DAYS"]), 1, key="cfg_PULLBACK_DAYS")
        cfg["SUPPORT_MA"] = c1.number_input("② 支撑均线周期(日)", 3, 120,
                                            int(core.CONFIG["SUPPORT_MA"]), 1, key="cfg_SUPPORT_MA")
        cfg["PULLBACK_TOL"] = c1.number_input("② 回踩容差(%)", 0.1, 20.0,
                                              float(core.CONFIG["PULLBACK_TOL"]), 0.1, key="cfg_PULLBACK_TOL")
        cfg["SMALL_YANG_DAYS"] = c1.number_input("③ 碎步小阳天数", 2, 20,
                                                 int(core.CONFIG["SMALL_YANG_DAYS"]), 1, key="cfg_SMALL_YANG_DAYS")
        cfg["SMALL_YANG_MAX"] = c1.number_input("③ 每日涨幅上限(%)", 0.1, 20.0,
                                                float(core.CONFIG["SMALL_YANG_MAX"]), 0.1, key="cfg_SMALL_YANG_MAX")
        cfg["W_BOTTOM_DAYS"] = c2.number_input("④ W底窗口(日)", 20, 250,
                                               int(core.CONFIG["W_BOTTOM_DAYS"]), 5, key="cfg_W_BOTTOM_DAYS")
        cfg["W_BOTTOM_TOL"] = c2.number_input("④ 双低点容差(%)", 0.1, 20.0,
                                              float(core.CONFIG["W_BOTTOM_TOL"]), 0.1, key="cfg_W_BOTTOM_TOL")
        cfg["DIVERGENCE_DAYS"] = c2.number_input("⑥ 底背离窗口(日)", 20, 250,
                                                 int(core.CONFIG["DIVERGENCE_DAYS"]), 5, key="cfg_DIVERGENCE_DAYS")
        cfg["RSI_DAYS"] = c2.number_input("⑥ RSI 周期", 3, 60,
                                          int(core.CONFIG["RSI_DAYS"]), 1, key="cfg_RSI_DAYS")
        cfg["VAL_YEARS"] = c2.number_input("⑧⑨ 估值分位回看年数", 1, 10,
                                           int(core.CONFIG["VAL_YEARS"]), 1, key="cfg_VAL_YEARS")
        cfg["PE_PERCENTILE"] = c2.number_input("⑧ PE 分位阈值(%)", 0.1, 100.0,
                                               float(core.CONFIG["PE_PERCENTILE"]), 0.5, key="cfg_PE_PERCENTILE")
        cfg["PB_PERCENTILE"] = c2.number_input("⑨ PB 分位阈值(%)", 0.1, 100.0,
                                               float(core.CONFIG["PB_PERCENTILE"]), 0.5, key="cfg_PB_PERCENTILE")
        cfg["DIV_YIELD_MIN"] = c2.number_input("⑩ 股息率下限(%)", 0.1, 30.0,
                                               float(core.CONFIG["DIV_YIELD_MIN"]), 0.1, key="cfg_DIV_YIELD_MIN")

    st.divider()
    st.subheader("🗃️ 数据库状态")
    try:
        _stats = _get_db_stats(db_path)
        st.caption(f"股票 {_stats['stocks']} 只 · 日K {_stats['daily_rows']} 行（最新 {_stats['daily_max']}）"
                   f" · 估值 {_stats['val_rows']} 行（最新 {_stats['val_max']}） · 分红 {_stats['div_rows']} 条")
    except Exception as e:
        st.caption(f"数据库不可用：{type(e).__name__}")
    auto_sync = st.checkbox("启动时自动增量更新", value=True, key="auto_sync",
                            help="勾选后每次打开页面自动拉取最新交易日数据入库")
    if st.button("🔄 增量更新数据", use_container_width=True, key="btn_inc_sync",
                 help="仅拉取本地最新日期之后的数据；首次运行等价于全量初始化"):
        _run_sync(db_path, cfg, force=False)
        st.rerun()
    force_confirm = st.checkbox("⚠️ 确认清空当前范围内数据并全量重拉", value=False,
                                key="force_confirm")
    if st.button("🗑️ 强制全量刷新", use_container_width=True, key="btn_force_sync",
                 disabled=not force_confirm,
                 help="应对数据异常：仅清空**当前范围内**已选同步内容的数据后重新全量拉取，"
                      "范围外缓存不受影响"):
        _run_sync(db_path, cfg, force=True)
        st.rerun()
    st.caption("提示：增量更新不影响技术指标计算（历史数据完整留存于本地库）；"
               "「数据范围」外的数据保留在库中，切换范围无需重拉；"
               "「调试限量」>0 时仅处理范围内按代码排序的前N只。")

# ============================================================================
# 启动时自动增量同步（数据库为空=首次全量初始化；范围变更或超时触发）
# 外层兜底：任何意外异常都不阻断主区域（右半边）渲染
# ============================================================================
if auto_sync and core.should_auto_sync(db_path, cfg, scope_sig=_scope_sig()):
    try:
        _run_sync(db_path, cfg, force=False)
    except Exception as e:
        st.error(f"❌ 自动同步异常：{type(e).__name__}: {e}")

# ============================================================================
# 主区域：条件多选 + 开始筛选 + 结果展示
# ============================================================================
st.title("📈 A股多条件组合选股")
st.caption("本地 SQLite 缓存 · 先选数据范围（交易所/板块）再增量拉取 · 13个条件并行计算取交集（且关系）· "
           "结果按 PE-TTM 由低到高排序 · 股票名称点击跳转同花顺个股页")

with st.container(border=True):
    st.markdown("**① 勾选筛选条件（可多选；多个条件之间为「且」关系，取交集）**")
    ca, cb_, cc = st.columns([1, 1, 8])
    if ca.button("全选", key="btn_all"):
        st.session_state["sel_conds"] = list(core.CONDITIONS.keys())
    if cb_.button("清空", key="btn_none"):
        st.session_state["sel_conds"] = []
    sel = cc.multiselect(
        "筛选条件", options=list(core.CONDITIONS.keys()), key="sel_conds",
        format_func=lambda k: core.CONDITIONS[k]["label"],
        label_visibility="collapsed",
        help="可多选；条件之间为且关系（取交集）。阈值在左侧边栏「条件阈值」中调整。")
    if sel:
        with st.expander("📖 已选条件说明", expanded=False):
            for k in sel:
                st.markdown(f"- **{core.CONDITIONS[k]['label']}**：{core.CONDITIONS[k]['desc']}")
    run_btn = st.button("🚀 开始筛选", type="primary", use_container_width=True, key="btn_run")

if run_btn:
    sel = st.session_state.get("sel_conds", [])
    if not sel:
        st.error("请先勾选至少一个筛选条件，再点击「开始筛选」")
    else:
        logs = []
        prog, hint, slogan_ph, cb = _make_progress()
        try:
            with st.spinner("条件并行计算中（基于本地缓存数据）…"):
                exchange = _EXCHANGE_MAP.get(st.session_state.get("scope_exchange", "全部A股"))
                sectors = st.session_state.get("scope_sectors", []) or None
                df = core.run_screening(db_path, sel, cfg, exchange=exchange, sectors=sectors,
                                        progress_cb=cb, logs=logs)
            st.session_state["result"] = (df, list(sel), logs)
        except Exception as e:
            st.error(f"❌ 筛选失败：{type(e).__name__}: {e}")
            st.session_state["result"] = None
        finally:
            prog.empty()
            hint.empty()
            slogan_ph.empty()

if "result" in st.session_state and st.session_state["result"]:
    df, used_sel, logs = st.session_state["result"]
    labels = "、".join(core.CONDITIONS[k]["label"] for k in used_sel)
    st.markdown(f"### ② 筛选结果　<span style='font-size:15px;color:#666'>"
                f"条件：{labels}（交集） · 共 {len(df)} 只 · 按 PE-TTM 由低到高排序</span>",
                unsafe_allow_html=True)
    if df.empty:
        st.info("没有股票同时满足所选条件。可尝试减少条件数量或放宽阈值（侧边栏「条件阈值」）。")
    else:
        disp = df.copy()
        disp["当前价"] = [f"{v:.2f}" if pd.notna(v) else "" for v in df["当前价"]]
        disp["涨跌幅(%)"] = [f"{v:+.2f}%" if pd.notna(v) else "" for v in df["涨跌幅(%)"]]
        disp["PE-TTM"] = [f"{v:.2f}" if pd.notna(v) else "" for v in df["PE-TTM"]]
        st.markdown(core.df_to_html_table(
            disp, columns=["板块", "当前价", "涨跌幅(%)", "PE-TTM", "触发条件列表", "数据日期", "代码"]),
            unsafe_allow_html=True)
        st.download_button("⬇️ 下载筛选结果 CSV", df.to_csv(index=False).encode("utf-8-sig"),
                           file_name="筛选结果.csv", mime="text/csv", key="dl_result")
    with st.expander("⚠️ 运行日志（数据缺失/接口异常/命中统计）"):
        st.text("\n".join(str(x) for x in logs) if logs else "（无）")

st.divider()
st.caption("数据源：AKShare（新浪行情/日K · 东财F10估值 · 新浪分红明细 · 申万一级行业 · 交易所上市日期），"
           "全部落盘于本地 SQLite，网络中断时仍可基于缓存数据筛选。个股数据个别缺失属正常现象，详见运行日志。")
