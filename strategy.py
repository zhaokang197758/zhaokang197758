# -*- coding: utf-8 -*-
"""每日动量选股 -> 微信推送（中证800 · 带评级 · 持仓诊断）"""

import os
import sys
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
SHOW_N = 15          # 推送展示前几名（前 15 名足够覆盖你的持仓）
MOM_WIN = 20         # 动量回看天数
MAX_RET5 = 0.15      # 剔除 5 日涨幅超 15% 的过热股
MIN_AMT = 2.0        # 20 日均成交额下限（亿元）
WORKERS = 4          # 并发数，报错多就改成 2

# 你当前持有的股票（6 位代码，逗号分隔）。买入或清仓后改这一行。
HOLD = []            # 例：["688981", "002311", "600887"]

ETF = {"510300": "沪深300ETF", "510500": "中证500ETF", "159915": "创业板ETF",
       "588000": "科创50ETF", "512880": "证券ETF", "512480": "半导体ETF",
       "515030": "新能源车ETF", "512690": "酒ETF", "512170": "医疗ETF",
       "513050": "中概互联ETF"}
# ==========================================

NOW = datetime.now(ZoneInfo("Asia/Shanghai"))
KEY = (os.getenv("SERVERCHAN_SENDKEY") or "").strip()
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
BAR = 120
ERR = []


def px(code):
    return ("sh" if code[0] in "65" else "sz" if code[0] in "0123" else "bj") + code


def src_tx(code, n):                    # 腾讯：海外机房最稳
    u = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,day,,,%d,qfq"
         % (px(code), n))
    j = requests.get(u, headers=UA, timeout=12).json()
    d = (j.get("data") or {}).get(px(code)) or {}
    arr = d.get("qfqday") or d.get("day") or []
    if len(arr) < 30:
        return None
    # 腾讯成交量单位：科创板(688)是「股」，其余是「手」(100 股)
    mult = 1 if code.startswith("688") else 100
    return [(x[0], float(x[2]), float(x[5]) * mult * float(x[2])) for x in arr]


def src_sina(code, n):                  # 新浪
    u = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
         "?symbol=%s&scale=240&ma=no&datalen=%d" % (px(code), n))
    arr = requests.get(u, headers=UA, timeout=12).json()
    if not isinstance(arr, list) or len(arr) < 30:
        return None
    return [(x["day"][:10], float(x["close"]), float(x["volume"]) * float(x["close"]))
            for x in arr]


def src_em(code, n):                    # 东方财富：国内首选，海外常被拦
    secid = ("1." if code[0] in "56" else "0.") + code
    u = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s"
         "&fields1=f1,f2,f3&fields2=f51,f53,f57&klt=101&fqt=1&end=20500101&lmt=%d" % (secid, n))
    j = requests.get(u, headers=UA, timeout=12).json()
    kl = ((j.get("data") or {}).get("klines")) or []
    if len(kl) < 30:
        return None
    out = []
    for s in kl:
        p = s.split(",")
        out.append((p[0], float(p[1]), float(p[2])))
    return out


SOURCES = (("腾讯", src_tx), ("新浪", src_sina), ("东财", src_em))


def hist(code):
    for name, fn in SOURCES:
        try:
            rows = fn(code, BAR)
            if rows and len(rows) >= MOM_WIN + 5:
                return rows
        except Exception as e:
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
    return {"code": code, "name": name, "close": round(close[-1], 3),
            "mom": round((close[-1] / close[-1 - MOM_WIN] - 1) * 100, 2),
            "ret5": round((close[-1] / close[-6] - 1) * 100, 2),
            "amt": round(sum(amt[-20:]) / 20 / 1e8, 2)}


def read_hold():
    """优先读仓库里的 hold.txt（一行一个 6 位代码），失败则用 HOLD 列表"""
    try:
        repo = os.getenv("GITHUB_REPOSITORY", "zhaokang197758/zhaokang197758")
        u = "https://raw.githubusercontent.com/%s/main/hold.txt" % repo
        txt = requests.get(u, headers=UA, timeout=10).text
        codes = []
        for line in txt.splitlines():
            t = line.split("#")[0].strip()
            if len(t) >= 6 and t[:6].isdigit():
                codes.append(t[:6])
        if codes:
            return codes
    except Exception:
        pass
    return [str(c).strip()[:6] for c in HOLD if str(c).strip()]


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
    # 宁可报错停跑，也不退回 ETF 小池子——小池子算出的"全市场中位数"是错的，会误导仓位
    print("【致命】没有可用股票池，今日不生成信号。")
    print("请确认仓库根目录存在 pool_%s.txt（800 行左右的代码名单）。" % INDEX)
    sys.exit(1)


def rate(rank, total, mom, ret5):
    """评级：买入 / 继续持有 / 减仓 / 卖出"""
    pct = rank / total
    if mom < 0:
        return "卖出"          # 20 日动量为负 = 趋势已破，无条件离场
    if ret5 < -6:
        return "减仓"          # 短期破位
    if ret5 > 12:
        return "减仓"          # 过热，接近 15% 剔除线，先落袋
    if pct > 0.10:
        return "减仓"          # 掉出前 10%，趋势弱化
    if ret5 > 6:
        return "继续持有"      # 加速中，持有但不追
    return "买入"              # 趋势健康


def main():
    items = get_pool()
    print("池子 %s，标的 %d，数据源 腾讯/新浪/东财" % (INDEX, len(items)))

    rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(one, it) for it in items]
        for f in as_completed(futs):
            try:
                r = f.result()
                if r:
                    rows.append(r)
            except Exception:
                pass
    print("取数成功 %d / %d" % (len(rows), len(items)))
    if ERR:
        print("失败样例:", " | ".join(ERR))
    if not rows:
        print("所有数据源都失败")
        sys.exit(1)

    df = pd.DataFrame(rows).sort_values("mom", ascending=False).reset_index(drop=True)
    total = len(df)
    med = float(df["mom"].median())
    up = int((df["mom"] > 0).sum())
    df["rank"] = df.index + 1

    # ---- 目标组合（过滤后取 TOP_N）----
    cand = df.head(max(int(total * 0.1), TOP_N))
    pick = cand[(cand["ret5"] <= MAX_RET5 * 100) & (cand["amt"] >= MIN_AMT)].head(TOP_N)
    if pick.empty:
        pick = cand.head(TOP_N)
    print(pick.to_string(index=False))

    # ---- 组装推送 ----
    weak = med < -1
    head = []
    if weak:
        head.append("## 【弱市】建议半仓或空仓\n")
        head.append("**每只仓位不超过 10%，不要满仓。**\n")
    else:
        head.append("## 【正常】可按计划建仓\n")
    head.append("全池动量中位数 **%+.2f%%**，上涨 %d/%d 只（%.0f%%）\n"
                % (med, up, total, up * 100.0 / total))

    body = ["\n".join(head), "\n**今日目标组合（前 %d 名）**\n" % min(SHOW_N, total),
            "| 评级 | 代码 | 名称 | 现价 | 20日动量 | 5日涨幅 | 均额亿 |",
            "| --- | --- | --- | --- | --- | --- | --- |"]
    for _, r in df.head(SHOW_N).iterrows():
        g = rate(int(r["rank"]), total, r["mom"], r["ret5"])
        mark = "**%s**" % g if g in ("买入", "卖出") else g
        body.append("| %s | %s | %s | %s | %+.2f%% | %+.2f%% | %.2f |"
                    % (mark, r["code"], r["name"], r["close"], r["mom"], r["ret5"], r["amt"]))

    # ---- 持仓诊断 ----
    holds = read_hold()
    if holds:
        body += ["", "**你的持仓**", "",
                 "| 评级 | 代码 | 名称 | 20日动量 | 5日涨幅 | 排名 |",
                 "| --- | --- | --- | --- | --- | --- |"]
        for c in holds:
            hit = df[df["code"] == c]
            if hit.empty:
                body.append("| 卖出 | %s | 不在池内 | — | — | — |" % c)
            else:
                r = hit.iloc[0]
                g = rate(int(r["rank"]), total, r["mom"], r["ret5"])
                body.append("| **%s** | %s | %s | %+.2f%% | %+.2f%% | %d/%d |"
                            % (g, r["code"], r["name"], r["mom"], r["ret5"], int(r["rank"]), total))

    body += ["", "**卖出纪律（照做，不要临场判断）**",
             "1. 评级变「减仓」→ 卖一半",
             "2. 评级变「卖出」→ 清仓",
             "3. 组合从最高点回撤 10% → 全部清仓，停止一个月",
             "", "调仓日：每周一；限价单挂昨收 ±1%，开盘 15 分钟后下单。",
             "", "<sub>池子 %s（%d 只）| 数据源：腾讯/新浪/东财 | 仅供参考，不构成投资建议</sub>"
             % (INDEX_NAME.get(INDEX, INDEX), total)]

    text = "\n".join(body)
    print(text)

    if not KEY:
        print("未配置 SERVERCHAN_SENDKEY，仅打印")
        sys.exit(1)
    title = "%s %s 信号%d只 %s" % (NOW.strftime("%m-%d"), "弱市" if weak else "正常",
                                   len(pick), "、".join(pick["name"].head(2)))
    r = requests.post("https://sctapi.ftqq.com/%s.send" % KEY,
                      data={"title": title[:32], "desp": text}, timeout=20)
    print("推送返回 HTTP=%s %s" % (r.status_code, r.text[:200]))
    if r.status_code != 200 or '"errno":0' not in r.text.replace(" ", ""):
        sys.exit(1)


main()
