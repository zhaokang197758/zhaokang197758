# -*- coding: utf-8 -*-
"""每日动量选股 -> 微信推送（中证800 · 评级 · 持仓诊断 · 策略自检）"""

import os
import sys
import threading
import time
import statistics
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ 参数区：只改这里 ============
INDEX = "000906"     # 000300=沪深300 | 000906=中证800 | 000852=中证1000 | 000985=全A
INDEX_NAME = {"000300": "沪深300", "000905": "中证500", "000906": "中证800",
              "000852": "中证1000", "000985": "中证全指"}
POOL = "index"       # index = 用上面的 INDEX | etf = 用下面的 ETF 池
TOP_N = 5            # 建议建仓几只
SHOW_N = 15          # 推送展示前几名
MOM_WIN = 20         # 动量回看天数
MAX_RET5 = 0.15      # 剔除 5 日涨幅超 15% 的过热股
MAX_MOM = 0.60       # 剔除 20 日涨幅超 60% 的妖股（动量顶部的暴跌风险）
MIN_AMT = 2.0        # 20 日均成交额下限（亿元）
WORKERS = 4          # 并发数，报错多就改成 2
STALE_DAYS = 7       # 最后交易日超过 N 天没更新 = 停牌，剔除
DEADLINE = 480       # 取数总时长上限（秒），超时就用已拿到的数据先出结果
BAR = 200            # 每只取多少根日线（回测要往前推 120 天，别调小）
HOLD = []            # 兜底持仓，优先读 hold.txt

# ---- 早盘卖出阈值（一夜持股法）----
SELL_FALL = 3.0      # 从当日最高点回落超过 3% → 全部卖出（冲高失败）
SELL_BREAK = 3.0     # 跌破昨收 3% → 全部卖出（低开低走）
STOP_LOSS = 5.0      # 亏损超过 5% → 全部卖出（止损）
TAKE_ALL = 15.0      # 盈利超过 15% → 全部卖出（落袋）
TAKE_HALF = 8.0      # 盈利超过 8% → 卖出一半（锁一半利润）

ETF = {"510300": "沪深300ETF", "510500": "中证500ETF", "159915": "创业板ETF",
       "588000": "科创50ETF", "512880": "证券ETF", "512480": "半导体ETF",
       "515030": "新能源车ETF", "512690": "酒ETF", "512170": "医疗ETF",
       "513050": "中概互联ETF"}
# ==========================================

NOW = datetime.now(ZoneInfo("Asia/Shanghai"))
# 模式：morning=早盘只看持仓 | close=收盘选股
# 优先用 MODE 环境变量；其次按「是哪个 cron 触发的」判断；最后才看当前时间。
# 不能只看小时：10:00 的任务若排队拖到 12 点后才跑，会被误判成收盘模式，去扫 800 只。
CRON = (os.getenv("CRON") or "").strip()
_CP = CRON.split()
if os.getenv("MODE"):
    MODE = os.getenv("MODE").strip()
elif len(_CP) >= 2 and _CP[1] in ("0", "2"):
    MODE = "morning"                    # UTC 00/02 点起跑 -> 北京 10:00 执行早盘
elif len(_CP) >= 2 and _CP[1] in ("4", "6"):
    MODE = "close"                      # UTC 04/06 点起跑 -> 北京 14:30 执行收盘
else:
    MODE = "morning" if NOW.hour < 12 else "close"

# 准点由程序自己等，不指望定时器准时。
# GitHub 自带 cron 实测会延迟 3-60 分钟，所以让它提前两小时把机器叫起来，
# 程序 sleep 到目标时刻再开跑。这样延迟 59 分钟也照样 10:00 出结果。
TARGET_HM = {"morning": (10, 0), "close": (14, 30)}
WAIT = (os.getenv("WAIT") or "auto").strip().lower()
KEY = (os.getenv("SERVERCHAN_SENDKEY") or "").strip()
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
ERR = []
COVER_MIN = 0.70     # 取数覆盖率低于此值，不输出选股信号（避免用残缺数据发假信号）
SRC_BAN = 15         # 某个源连续失败 N 次后，本次运行不再尝试它
_LK = threading.Lock()
SRC_FAIL = {}


def _json(r):
    """只在真的是 JSON 时才解析：WAF 拦截页、空响应一律算失败"""
    t = (r.text or "").lstrip()
    if not t or t[0] not in "[{":
        return None
    try:
        return r.json()
    except Exception:
        return None


def s_ban(name):
    with _LK:
        return SRC_FAIL.get(name, 0) >= SRC_BAN


def s_mark(name, ok):
    with _LK:
        SRC_FAIL[name] = 0 if ok else SRC_FAIL.get(name, 0) + 1

# 复用连接：800 只 × 3 源，新连接每次都要握手，慢且容易被风控断连
SESSION = requests.Session()
SESSION.headers.update(UA)
SESSION.trust_env = False


def px(code):
    return ("sh" if code[0] in "65" else "sz" if code[0] in "0123" else "bj") + code


def src_tx(code, n):                    # 腾讯：海外机房最稳
    u = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,day,,,%d,qfq"
         % (px(code), n))
    j = _json(SESSION.get(u, timeout=12))
    if not j:
        return None
    d = (j.get("data") or {}).get(px(code)) or {}
    arr = d.get("qfqday") or d.get("day") or []
    if len(arr) < 30:
        return None
    # 腾讯成交量单位：科创板(688)是「股」，其余是「手」(100 股)
    mult = 1 if code.startswith("688") else 100
    return [(x[0], float(x[2]), float(x[5]) * mult * float(x[2])) for x in arr]


def src_sina(code, n):                  # 新浪：前复权，量单位为「股」
    u = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
         "?symbol=%s&scale=240&ma=no&datalen=%d" % (px(code), n))
    arr = _json(SESSION.get(u, timeout=12))
    if not isinstance(arr, list) or len(arr) < 30:
        return None
    return [(x["day"][:10], float(x["close"]), float(x["volume"]) * float(x["close"]))
            for x in arr]


def src_em(code, n):                    # 东方财富：国内首选，海外常被拦
    secid = ("1." if code[0] in "56" else "0.") + code
    u = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s"
         "&fields1=f1,f2,f3&fields2=f51,f53,f57&klt=101&fqt=1&end=20500101&lmt=%d" % (secid, n))
    j = _json(SESSION.get(u, timeout=12))
    if not j:
        return None
    kl = ((j.get("data") or {}).get("klines")) or []
    if len(kl) < 30:
        return None
    out = []
    for s in kl:
        p = s.split(",")
        out.append((p[0], float(p[1]), float(p[2])))
    return out


SOURCES = (("腾讯", src_tx), ("新浪", src_sina), ("东财", src_em))


def hist(code, n=None):
    """按顺序尝试各数据源，连续失灵的源本次运行内跳过（避免无谓请求拖慢全局）"""
    for name, fn in SOURCES:
        if s_ban(name):
            continue
        try:
            rows = fn(code, n or BAR)
            if rows and len(rows) >= MOM_WIN + 5:
                s_mark(name, True)
                return rows
            s_mark(name, False)
            if len(ERR) < 4:
                ERR.append("%s/%s:%s" % (code, name,
                                         "接口无有效返回" if not rows else "K线不足"))
        except Exception as e:
            s_mark(name, False)
            if len(ERR) < 3:
                ERR.append("%s/%s:%s" % (code, name, str(e)[:40]))
    return None


def one(item):
    code, name = item
    rows = hist(code)
    if not rows:
        return None
    rows = sorted(rows, key=lambda x: x[0])
    close = [r[1] for r in rows]
    amt = [r[2] for r in rows]
    m = pd.Series(amt[-20:]).median()
    if m and m < 1e5:
        amt = [a * 1e4 for a in amt]
    if close[-1] <= 0 or close[-1 - MOM_WIN] <= 0:
        return None
    return {"code": code, "name": name, "close": round(close[-1], 3),
            "chg": round((close[-1] / close[-2] - 1) * 100, 2)
                   if len(close) > 1 and close[-2] > 0 else 0.0,   # 今日涨跌幅，供「今天涨了多少」统计
            "mom": round((close[-1] / close[-1 - MOM_WIN] - 1) * 100, 2),
            "ret5": round((close[-1] / close[-6] - 1) * 100, 2),
            "amt": round(sum(amt[-20:]) / 20 / 1e8, 2),
            "last": rows[-1][0],              # 最后交易日：用来识别停牌
            "_close": close, "_amt": amt}     # 回测用，打印前会删掉


def parse_hold(txt):
    """解析持仓文本：代码,成本价,股数  一行一只，支持 # 注释"""
    out = []
    for line in txt.splitlines():
        t = line.split("#")[0].strip()
        if not t:
            continue
        p = [x for x in t.replace("，", ",").replace("\t", ",").replace(" ", ",").split(",") if x]
        if not p or len(p[0]) < 6 or not p[0][:6].isdigit():
            continue

        def num(x):
            try:
                return float(x)
            except Exception:
                return 0.0

        cd = p[0][:6]
        if any(x["code"] == cd for x in out):
            print("hold.txt 里 %s 重复，已忽略后面那行" % cd)
            continue
        out.append({"code": cd,
                    "cost": num(p[1]) if len(p) > 1 else 0.0,
                    "shares": int(num(p[2])) if len(p) > 2 else 0})
    return out


def read_hold():
    """优先读本地 hold.txt（Actions checkout 后就在根目录），失败再走网络"""
    here = os.path.dirname(os.path.abspath(__file__))
    for path in ("hold.txt", os.path.join(here, "hold.txt")):
        if os.path.exists(path):
            try:
                out = parse_hold(open(path, encoding="utf-8").read())
                if out:
                    return out
            except Exception as e:
                print("本地 hold.txt 解析失败:", str(e)[:60])
    try:
        repo = os.getenv("GITHUB_REPOSITORY", "zhaokang197758/zhaokang197758")
        u = "https://raw.githubusercontent.com/%s/main/hold.txt" % repo
        out = parse_hold(SESSION.get(u, timeout=10).text)
        if out:
            return out
    except Exception as e:
        print("hold.txt 读取失败（持仓诊断可能不准）:", str(e)[:80])
    return [{"code": str(c).strip()[:6], "cost": 0.0, "shares": 0}
            for c in HOLD if str(c).strip()]


def read_pool_file(path):
    """读固化名单：一行一只，格式 代码,名称，支持 # 注释"""
    pairs = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                t = line.split("#")[0].strip()
                if not t:
                    continue
                parts = t.replace("\t", ",").replace("，", ",").replace(" ", ",").split(",")
                c = parts[0].strip()
                if len(c) >= 6 and c[:6].isdigit():
                    nm = parts[1].strip() if len(parts) > 1 and parts[1].strip() else c[:6]
                    pairs.append((c[:6], nm))
    except Exception:
        pass
    return pairs


def get_pool():
    if POOL == "etf":
        return list(ETF.items())
    # 1) 优先读仓库里的固化名单：零网络依赖，绝不会因为接口挂掉而缩水
    here = os.path.dirname(os.path.abspath(__file__))
    for path in ("pool_%s.txt" % INDEX, os.path.join(here, "pool_%s.txt" % INDEX)):
        if os.path.exists(path):
            pairs = read_pool_file(path)
            if pairs:
                print("池子来源：固化名单 %s（%d 只）" % (os.path.basename(path), len(pairs)))
                return pairs
    # 2) 兜底：在线接口
    try:
        import akshare as ak
        r = ak.index_stock_cons_csindex(symbol=INDEX)
        pairs = [(str(a).zfill(6), str(b)) for a, b in zip(r["成分券代码"], r["成分券名称"])]
        if pairs:
            print("池子来源：在线接口（%d 只）" % len(pairs))
            return pairs
    except Exception as e:
        print("在线成分股获取失败:", str(e)[:100])
    # 宁可报错停跑，也不退回 ETF 小池子——小池子算出的「全市场中位数」是错的，会误导仓位
    print("【致命】没有可用股票池，今日不生成信号。")
    print("请确认仓库根目录存在 pool_%s.txt（800 行左右的代码名单）。" % INDEX)
    sys.exit(1)


def market_level(med):
    """按全池动量中位数分档，返回 (档位, 仓位建议, 最多建仓只数)
    两头都管：跌太多不买，涨太多也不追"""
    if med < -3:
        return "极弱", "空仓观望，不要买", 0
    if med < -1:
        return "弱势", "最多试仓 1 只", 1
    if med > 15:
        return "过热", "只减不加，别追高", 2
    return "正常", "可按计划建仓", TOP_N


def rate(rank, total, mom, ret5, held=False):
    """评级。held=False 时把「减仓」换成「观望」——没买的股票没有减仓一说"""
    pct = rank / total
    if mom < 0:
        return "卖出" if held else "回避"     # 20 日动量为负 = 趋势已破
    if ret5 < -6:
        return "减仓" if held else "观望"     # 短期破位
    if ret5 > 12:
        return "减仓" if held else "观望"     # 过热，接近 15% 剔除线
    if pct > 0.10:
        return "减仓" if held else "观望"     # 掉出前 10%，趋势弱化
    if ret5 > 6:
        return "继续持有" if held else "加速中，别追"   # 涨太快，没买的别追
    return "继续持有" if held else "买入"             # 趋势健康：手里的是拿住，空手的是建仓


def backtest(rows, hold=20, rounds=6):
    """简化回测：模拟「过去每个时点按当时市况决策」的真实结果
    含弱市空仓规则、过热/泡沫过滤，未扣手续费与滑点"""
    out = []
    for k in range(1, rounds + 1):
        off = k * hold
        cand, moms, allr = [], [], []
        for r in rows:
            c = r["_close"]
            if len(c) < off + hold + MOM_WIN + 2:
                continue
            p0 = c[-1 - off]                    # 该时点价格
            pb = c[-1 - off - MOM_WIN]          # 20 天前
            p5 = c[-1 - off - 5]
            if p0 <= 0 or pb <= 0:
                continue
            mom = p0 / pb - 1
            moms.append(mom)
            allr.append(c[-1 - off + hold] / p0 - 1)
            if mom <= 0:
                continue
            if p5 > 0 and p0 / p5 - 1 > MAX_RET5:
                continue                        # 5 日过热
            if mom > MAX_MOM:
                continue                        # 20 日泡沫
            cand.append((mom, r["code"], r["name"], c[-1 - off + hold] / p0 - 1))
        if len(moms) < 50:
            continue
        med_t = statistics.median(moms) * 100
        bench = sum(allr) / len(allr) * 100
        _, _, mb = market_level(med_t)
        if mb == 0:
            out.append({"days": off, "ret": 0.0, "names": [], "cash": True,
                        "med": med_t, "bench": bench})
            continue
        if len(cand) < 20:
            out.append({"days": off, "ret": 0.0, "names": [], "cash": True,
                        "med": med_t, "bench": bench})
            continue
        cand.sort(key=lambda x: -x[0])
        top = cand[:max(int(len(cand) * 0.1), TOP_N)][:mb]
        avg = sum(t[3] for t in top) / len(top)
        out.append({"days": off, "ret": avg * 100, "med": med_t, "bench": bench,
                    "names": [t[2] for t in top]})
    return out


def quote(codes):
    """批量取实时报价（腾讯），一次请求拿全部持仓"""
    if not codes:
        return {}
    u = "https://qt.gtimg.cn/q=" + ",".join(px(c) for c in codes)
    txt = SESSION.get(u, timeout=12).content.decode("gbk", "ignore")
    out = {}
    for seg in txt.split(";"):
        if '="' not in seg:
            continue
        body = seg.split('="', 1)[1].strip().strip('"')
        f = body.split("~")
        if len(f) < 40 or not f[2] or not f[3]:
            continue
        try:
            out[f[2]] = {"name": f[1], "price": float(f[3]), "prev": float(f[4]),
                         "open": float(f[5]), "vol": float(f[6]),
                         "high": float(f[33]), "low": float(f[34]),
                         "amount": float(f[37]) * 1e4, "time": f[30]}
        except Exception:
            continue
    return out


def time_progress(dt=None):
    """按 A 股 U 型成交节奏，估算此刻理论上应完成全天成交量的占比(0~1)。
    早盘判断「是否放量」必须跟这个比，而不是跟 100% 比 —— 9:52 只过了 22 分钟，
    成交额本该只有全天的两成，拿 60% 当门槛等于这条规则永远不触发。"""
    dt = dt or NOW
    t = dt.hour * 60 + dt.minute
    if t < 9 * 60 + 30:
        return 0.0
    if t > 15 * 60:
        return 1.0
    if 11 * 60 + 30 < t < 13 * 60:
        return 0.55                      # 午休，按上午收盘时的进度
    el = (t - (9 * 60 + 30)) if t <= 11 * 60 + 30 else 120 + (t - 13 * 60)
    el = min(max(el, 0), 240)
    pts = [(0, 0.0), (30, 0.25), (120, 0.55), (210, 0.80), (240, 1.0)]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= el <= x1:
            return y0 + (y1 - y0) * (el - x0) / (x1 - x0)
    return 1.0


def prev_amount(code):
    """昨日成交额（元），用来算今天开盘至今的量能进度"""
    rows = hist(code, 40)   # 不能取太少：数据源有「少于 30 根判无效」的保护
    if not rows:
        return 0.0
    today = NOW.strftime("%Y-%m-%d")
    for r in reversed(rows):
        if r[0][:10] < today:
            return r[2]
    return 0.0


def judge_morning(q, cost, y_amt):
    """一夜持股：早盘该不该卖。返回 (动作, 理由)"""
    price, prev, hi = q["price"], q["prev"], q["high"]
    if price <= 0 or prev <= 0:
        return "数据异常", "取不到有效价，手动看一眼"
    chg = (price / prev - 1) * 100
    pnl = (price / cost - 1) * 100 if cost > 0 else None
    fall = (hi - price) / hi * 100 if hi > 0 else 0.0
    vr = q["amount"] / y_amt if y_amt > 0 else 0.0      # 今日已成量 / 昨日全天
    ep = time_progress()
    pace = vr / ep if ep > 0.02 else 0.0                 # 相对正常节奏的倍数

    if pnl is not None and pnl <= -STOP_LOSS:
        return "全部卖出", "亏损 %.1f%%，触及止损线" % abs(pnl)
    if chg <= -SELL_BREAK:
        return "全部卖出", "跌破昨收 %.1f%%，低开低走" % abs(chg)
    if chg <= -1.5 and pace >= 1.8:
        # 只看「相对正常节奏」，不看绝对占比：14:00 成交 70% 是常态，
        # 拿 60% 当门槛会在盘尾天天误报放量。
        return "全部卖出", "放量下跌：量能已是正常节奏的 %.1f 倍" % pace
    if pnl is not None and pnl >= TAKE_ALL:
        return "全部卖出", "赚 %.1f%%，到位先落袋" % pnl
    if fall >= SELL_FALL:
        return "全部卖出", "从当日高点 %.2f 回落 %.1f%%，上攻失败" % (hi, fall)
    if pnl is not None and pnl >= TAKE_HALF:
        return "卖出一半", "赚 %.1f%%，先锁一半" % pnl
    # 没触发卖出时也把量能露出来：10:00 的放量门槛是「正常节奏的 1.8 倍」，
    # 偏严，给你这个数字，你可以自己判断要不要提前走。
    vs = "，量能 %.1f 倍" % pace if pace > 0 else ""
    return "继续持有", "%s %.1f%%，形态没破%s" % ("涨" if chg >= 0 else "跌", abs(chg), vs)


def run_morning(holds):
    """早盘模式：只看持仓，给出卖 / 持决策"""
    if not holds:
        if not push("%s 无持仓" % NOW.strftime("%m-%d"),
                    "## 早盘\n\nhold.txt 里没有记录，今天不用操作。\n\n"
                    "买入后记得把「代码,成本价,股数」写进去。"):
            sys.exit(1)
        return

    codes = [h["code"] for h in holds]
    qt = quote(codes)
    if not qt:
        print("实时行情全部失败")
        sys.exit(1)

    # 取所有持仓里最新的行情日期。只看第一只的话，它若停牌就会让整条推送变成「休市」，
    # 其他该卖的股票就被漏掉了。
    tms = [v["time"] for v in qt.values() if len(v.get("time", "")) >= 12]
    qtime = max(tms) if tms else ""
    if qtime[:8] != NOW.strftime("%Y%m%d"):
        print("今日休市（行情日期 %s）" % qtime[:8])
        if not push("%s 休市" % NOW.strftime("%m-%d"),
                    "## 今天休市\n\n不开盘，不用操作。\n\n<sub>行情日期 %s</sub>" % qtime[:8]):
            sys.exit(1)
        return

    rows, tcost, tval = [], 0.0, 0.0
    for h in holds:
        q = qt.get(h["code"])
        if not q:
            rows.append({"act": "无行情", "name": h["code"], "cost": h["cost"],
                         "price": 0.0, "pnl": None, "why": "代码有误或已退市"})
            continue
        if q.get("time", "")[:8] != qtime[:8]:
            if h["cost"] > 0 and h["shares"] > 0 and q["price"] > 0:
                tcost += h["cost"] * h["shares"]
                tval += q["price"] * h["shares"]      # 停牌也按最后价计入总市值
            rows.append({"act": "无行情", "name": q["name"], "cost": h["cost"],
                         "price": q["price"], "pnl": None,
                         "why": "停牌，最后成交 %s-%s" % (q["time"][4:6], q["time"][6:8])})
            continue
        act, why = judge_morning(q, h["cost"], prev_amount(h["code"]))
        pnl = (q["price"] / h["cost"] - 1) * 100 if h["cost"] > 0 else None
        if h["cost"] > 0 and h["shares"] > 0:
            tcost += h["cost"] * h["shares"]
            tval += q["price"] * h["shares"]
        rows.append({"act": act, "name": q["name"], "cost": h["cost"],
                     "price": q["price"], "pnl": pnl, "why": why})

    order = {"全部卖出": 0, "卖出一半": 1, "继续持有": 2, "无行情": 3, "数据异常": 3}
    rows.sort(key=lambda r: order.get(r["act"], 9))

    act_rows = [r for r in rows if r["act"] in ("全部卖出", "卖出一半")]
    odd_rows = [r for r in rows if r["act"] in ("无行情", "数据异常")]
    keep_rows = [r for r in rows if r["act"] == "继续持有"]

    body = []
    head = "%d 只要动" % len(act_rows) if act_rows else "全部继续持有"
    body.append("## 【早盘】%s\n" % head)
    body.append("行情时间 **%s:%s**　盘中价，不是收盘价\n" % (qtime[8:10], qtime[10:12]))
    # 10:00 的任务要排队，实际可能 10:08 才跑。不告诉你这一点，你会以为看的是 10:00 的盘。
    tgt = NOW.replace(hour=10, minute=0, second=0, microsecond=0)
    late = int((NOW - tgt).total_seconds() // 60)
    if 5 < late <= 90:
        body.append("> **比 10:00 晚 %d 分钟启动**，以下是 %s 的价。"
                    "下单前扫一眼现价，若已明显走样以盘面为准。\n"
                    % (late, qtime[8:10] + ":" + qtime[10:12]))
    if not act_rows:
        body.append("**今天什么都不用做，继续拿着。**\n")

    if act_rows:
        body.append("**要卖的**\n")
        for r in act_rows:
            c_s = "%.2f" % r["cost"] if r["cost"] > 0 else "未填"
            p_s = "%+.1f%%" % r["pnl"] if r["pnl"] is not None else "—"
            warn = "　⚠ 成本价可能填错，请核对" if (r["pnl"] or 0) > 100 or (r["pnl"] or 0) < -90 else ""
            body.append("- **%s** %s　成本 %s → 现价 %.2f（%s）%s\n  %s"
                        % (r["act"], r["name"], c_s, r["price"], p_s, warn, r["why"]))

    if keep_rows:
        body.append("\n**不用动的**\n")
        for r in keep_rows:
            c_s = "%.2f" % r["cost"] if r["cost"] > 0 else "未填"
            p_s = "%+.1f%%" % r["pnl"] if r["pnl"] is not None else "—"
            body.append("- %s　成本 %s → 现价 %.2f（%s）　%s"
                        % (r["name"], c_s, r["price"], p_s, r["why"]))

    if odd_rows:
        body.append("\n**需要你自己看一眼的**\n")
        for r in odd_rows:
            body.append("- %s：%s" % (r["name"], r["why"]))

    if tcost > 0:
        body.append("\n**合计**：成本 %.0f 元 → 市值 %.0f 元，浮动 **%+.1f%%**"
                    % (tcost, tval, (tval / tcost - 1) * 100))

    body += ["", "**执行**：看到就挂单，不用等整点，卖完把 hold.txt 里那行删掉。",
             "「继续持有」也别一直盯，收盘那次推送还会再判断一次。"]
    text = "\n".join(body)
    print(text)

    title = "%s 早盘 %s" % (NOW.strftime("%m-%d"), head)
    if not push(title, text):
        sys.exit(1)


def push(title, text):
    """推送，失败重试 3 次。Server酱偶尔 5xx，一次失败就放弃太脆弱"""
    if not KEY:
        print("未配置 SERVERCHAN_SENDKEY，仅打印")
        return False
    for i in range(3):
        try:
            r = SESSION.post("https://sctapi.ftqq.com/%s.send" % KEY,
                             data={"title": title[:32], "desp": text}, timeout=20)
            print("推送返回 HTTP=%s %s" % (r.status_code, r.text[:200]))
            if r.status_code == 200 and '"errno":0' in r.text.replace(" ", ""):
                return True
        except Exception as e:
            print("推送第 %d 次异常: %s" % (i + 1, str(e)[:80]))
        time.sleep(5)
    return False


def wait_to_target():
    """定时器只负责把机器叫起来；准点由程序自己等。
    返回：实际等待的分钟数（0 = 没等）"""
    global NOW
    if WAIT in ("0", "no", "false", "off"):
        return 0
    hh, mm = TARGET_HM.get(MODE, (10, 0))
    tz = ZoneInfo("Asia/Shanghai")
    tgt = NOW.replace(hour=hh, minute=mm, second=0, microsecond=0)
    gap = (tgt - NOW).total_seconds()
    if gap <= 20:
        return 0
    if gap > 10800:                     # 超过 3 小时不等，说明配错了，直接跑
        print("距目标 %02d:%02d 还有 %.0f 分钟，超过上限，直接执行" % (hh, mm, gap / 60))
        return 0
    print("已启动，等待到 %02d:%02d 执行（还需 %.1f 分钟）" % (hh, mm, gap / 60), flush=True)
    last = 0
    while True:
        remain = (tgt - datetime.now(tz)).total_seconds()
        if remain <= 2:
            break
        time.sleep(min(max(remain - 1.5, 1), 120))
        m = int(remain // 60)
        if m >= 1 and m % 10 == 0 and m != last:   # 每 10 分钟打一次心跳，防日志静默
            print("  ... 还有 %d 分钟" % m, flush=True)
            last = m
    NOW = datetime.now(tz)
    print("到点，开始执行（%s）" % NOW.strftime("%H:%M:%S"), flush=True)
    return int(gap // 60)


def main():
    wait_to_target()
    holds = read_hold()
    if MODE == "morning":
        run_morning(holds)
        return
    items = get_pool()
    print("池子 %s，标的 %d，数据源 腾讯/新浪/东财" % (INDEX, len(items)))

    rows, t0 = [], time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(one, it) for it in items]
        for f in as_completed(futs):
            if time.time() - t0 > DEADLINE:
                print("取数超时（>%d 秒），用已取到的 %d 只继续" % (DEADLINE, len(rows)))
                ex.shutdown(wait=False, cancel_futures=True)
                break
            try:
                r = f.result()
                if r:
                    rows.append(r)
            except Exception:
                pass
    print("取数成功 %d / %d，耗时 %.0f 秒" % (len(rows), len(items), time.time() - t0))
    used = set(SRC_FAIL)
    print("数据源状态:", "  ".join(
        "%s=%s" % (n, "熔断" if s_ban(n) else ("正常" if n in used else "未使用"))
        for n, _ in SOURCES))
    raw_ok = len(rows)          # 过滤停牌前的成功数，用来判断数据是否完整
    if ERR:
        print("失败样例:", " | ".join(ERR))
    if not rows:
        print("所有数据源都失败")
        sys.exit(1)

    # ---- 数据不完整就不发信号 ----
    # 只取到一部分标的时，中位数和排名都建立在残缺样本上，推出去的是假信号。
    # 宁可空一天，也不能让你照着错的数据下单。
    if raw_ok < len(items) * COVER_MIN:
        msg = ("## 【数据异常】今天不操作\n\n"
               "只取到 **%d/%d** 只标的（%.0f%%），低于 %.0f%% 的安全线。\n\n"
               "在这种样本上算出的排名和中位数都不可信，**今天不发选股信号**，"
               "按原持仓继续，不做新买入。\n\n"
               "<sub>环境已刷新，明天多半自动恢复；连续两天这样请告诉我。</sub>"
               % (raw_ok, len(items), raw_ok * 100.0 / len(items), COVER_MIN * 100))
        print(msg)
        if not push("%s 数据异常，今日不操作" % NOW.strftime("%m-%d"), msg):
            sys.exit(1)
        return

    # ---- 停牌剔除：最后交易日明显落后的，买不到，不该进榜单 ----
    latest = max(datetime.strptime(r["last"][:10], "%Y-%m-%d") for r in rows)
    alive = [r for r in rows if (latest - datetime.strptime(r["last"][:10], "%Y-%m-%d")).days <= STALE_DAYS]
    stale = len(rows) - len(alive)
    if stale:
        print("剔除停牌/长期无成交 %d 只" % stale)
    rows = alive

    df = pd.DataFrame(rows).sort_values("mom", ascending=False).reset_index(drop=True)
    total = len(df)
    med = float(df["mom"].median())
    up = int((df["mom"] > 0).sum())
    df["rank"] = df.index + 1
    level, advice, max_buy = market_level(med)

    # ---- 目标组合（过滤后取 TOP_N）----
    cand = df.head(max(int(total * 0.1), TOP_N))
    pick = cand[(cand["ret5"] <= MAX_RET5 * 100) & (cand["amt"] >= MIN_AMT)
                & (cand["mom"] <= MAX_MOM * 100)].head(TOP_N)
    if pick.empty:
        print("今日无合格标的（全部过热或流动性不足）")
    show = df.drop(columns=["_close", "_amt"])
    print(show.head(SHOW_N).to_string(index=False))

    heldset = {h["code"] for h in holds}
    bt = backtest(rows)

    # ---- 组装推送 ----
    body = []
    tag = "【%s】%s" % (level, advice)
    body.append("## %s\n" % tag)
    if raw_ok < len(items) * 0.95:
        body.append("**⚠ 今天只取到 %d/%d 只数据，排名可能略有偏差**\n"
                    % (raw_ok, len(items)))
    if max_buy == 0:
        body.append("**今天不建议买入任何股票。** 留着现金，等市场转暖。\n")
    elif max_buy <= 2:
        body.append("**弱市试仓：只买 %d 只，每只不超过总资金 5%%。**\n" % max_buy)
    else:
        body.append("**每只不超过总资金 10%%，今天最多买 %d 只。**\n" % max_buy)
    # 这两个数必须分开口径：「上涨 236/800」曾被读成「今天只有 236 只在涨」，
    # 其实那是 20 日动量为正的只数。今天涨跌单独列一行，免得和市场行情对不上。
    df["chg"] = df["chg"].fillna(0)
    up_today = int((df["chg"] > 0).sum())
    body.append("**20 日**动量中位数 **%+.2f%%**，动量为正 %d/%d 只（%.0f%%）\n"
                % (med, up, total, up * 100.0 / total))
    body.append("**今日**上涨 %d/%d 只（%.0f%%），中位数 %+.2f%%\n"
                % (up_today, total, up_today * 100.0 / total, float(df["chg"].median())))
    # 盘中跑（14:30）时不能写「收盘」——那时候还没收盘，写错会让人以为拿到的是定盘价
    intraday = (latest.date() == NOW.date() and NOW.hour < 15)
    if intraday:
        stamp = "**%s %s 盘中价**" % (latest.strftime("%m-%d"), NOW.strftime("%H:%M"))
    else:
        stamp = "**%s** 收盘" % latest.strftime("%m-%d")
    if stale:
        body.append("数据截至 %s · 有效 %d 只（剔除停牌 %d 只）\n"
                    % (stamp, total, stale))
    else:
        body.append("数据截至 %s · %d/%d 只取数成功\n"
                    % (stamp, total, len(items)))
    if max_buy:
        n_pick = min(len(pick), max_buy)
        body.append("**今天买这 %d 只：%s**\n"
                    % (n_pick, "、".join(pick["name"].head(n_pick))))
        if intraday:
            mins = int((NOW.replace(hour=15, minute=0, second=0, microsecond=0)
                        - NOW).total_seconds() // 60)
            if mins > 0:
                body.append("离收盘还有 **%d 分钟**，按现价挂单；表里的价是 %s 的，会变。\n"
                            % (mins, NOW.strftime("%H:%M")))

    body += ["\n**今日目标组合**\n",
             "| 评级 | 代码 | 名称 | 现价 | 20日动量 | 5日涨幅 | 均额亿 |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    for _, r in show.head(SHOW_N).iterrows():
        is_held = r["code"] in heldset
        g = rate(int(r["rank"]), total, r["mom"], r["ret5"], is_held)
        if max_buy == 0 and not is_held:
            g = "暂缓"      # 极弱市里不给「买入」字样，免得和头顶的「空仓观望」互相打架
        mark = "**%s**" % g if g in ("买入", "卖出") else g
        body.append("| %s | %s | %s | %s | %+.2f%% | %+.2f%% | %.2f |"
                    % (mark, r["code"], r["name"], r["close"], r["mom"], r["ret5"], r["amt"]))
    body.append("\n<sub>「观望」= 趋势已弱，没买的别追；「回避」= 趋势破了，不要碰；「加速中，别追」= 涨得太急，等回调；"
                "「暂缓」= 今天整体不买</sub>")

    # ---- 策略自检：过去几期同样规则的实际收益 ----
    if bt:
        body += ["", "**这套规则最近半年实际表现**（每 20 天换一次，含弱市空仓、未扣手续费）", ""]
        for b in bt:
            if b.get("cash"):
                body.append("- %d 天前 → **空仓**　当时全池 %+.1f%%，中位数 %+.1f%%"
                            % (b["days"], b["bench"], b["med"]))
            else:
                sg = "+" if b["ret"] >= 0 else ""
                body.append("- %d 天前 → **%s%.1f%%**　同期全池 %+.1f%%"
                            % (b["days"], sg, b["ret"], b["bench"]))
        avg = sum(b["ret"] for b in bt) / len(bt)
        avgb = sum(b["bench"] for b in bt) / len(bt)
        body.append("- %d 期平均 **%+.1f%%**　全池 %+.1f%%　**%s %.1f 个点**（空仓期按 0%% 计）"
                    % (len(bt), avg, avgb, "跑赢" if avg > avgb else "跑输", abs(avg - avgb)))

    # ---- 持仓诊断 ----
    if holds:
        body += ["", "**你的持仓**", "",
                 "| 评级 | 名称 | 成本 | 现价 | 盈亏 | 20日动量 |",
                 "| --- | --- | --- | --- | --- | --- |"]
        tcost, tval = 0.0, 0.0
        for h in holds:
            c = h["code"]
            hit = show[show["code"] == c]
            if hit.empty:
                body.append("| **卖出** | %s | %s | — | — | — |"
                            % (c, ("%.2f" % h["cost"]) if h["cost"] > 0 else "—"))
                continue
            r = hit.iloc[0]
            g = rate(int(r["rank"]), total, r["mom"], r["ret5"], True)
            if h["cost"] > 0:
                pnl = (r["close"] / h["cost"] - 1) * 100
                p_s = "%+.1f%%" % pnl
                c_s = "%.2f" % h["cost"]
                if h["shares"] > 0:
                    tcost += h["cost"] * h["shares"]
                    tval += r["close"] * h["shares"]
            else:
                p_s, c_s = "—", "—"
            body.append("| **%s** | %s | %s | %s | %s | %+.2f%% |"
                        % (g, r["name"], c_s, r["close"], p_s, r["mom"]))
        if tcost > 0:
            body.append("| | **合计** | **%.0f** | **%.0f** | **%+.1f%%** | |"
                        % (tcost, tval, (tval / tcost - 1) * 100))

    body += ["", "**卖出纪律（照做，不要临场判断）**",
             "1. 评级变「减仓」→ 卖一半",
             "2. 评级变「卖出」→ 清仓",
             "3. 组合从最高点回撤 10% → 全部清仓，停止一个月",
             "", "调仓日：每周一；限价单挂昨收 ±1%，开盘 15 分钟后下单。",
             "", "<sub>池子 %s（%d 只）| 数据源：腾讯/新浪/东财 | 仅供参考，不构成投资建议</sub>"
             % (INDEX_NAME.get(INDEX, INDEX), total)]

    text = "\n".join(body)
    print(text)

    if max_buy == 0:
        title = "%s %s 空仓观望" % (NOW.strftime("%m-%d"), level)
    else:
        title = "%s %s %d只 %s" % (NOW.strftime("%m-%d"), level, min(len(pick), max_buy),
                                   "、".join(pick["name"].head(2)))
    ok = push(title, text)
    if not ok:
        sys.exit(1)


main()
