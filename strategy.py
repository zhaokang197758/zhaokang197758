# -*- coding: utf-8 -*-
"""每日动量选股信号 -> 微信推送（多数据源版，适配 GitHub 海外服务器）"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ 参数区：只改这里 ============
POOL = "hs300"      # hs300 = 沪深300成分股 | etf = 下面的 ETF 池
TOP_N = 5           # 持有几只
MOM_WIN = 20        # 动量回看天数
MAX_RET5 = 0.15     # 剔除 5 日涨幅超 15% 的过热股
MIN_AMT = 2.0       # 20 日均成交额下限（亿元）
WORKERS = 4         # 并发数，报错多就改成 2

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
                ERR.append("%s/%s:%s" % (code, name, str(e)[:50]))
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


def get_pool():
    if POOL == "etf":
        return list(ETF.items())
    try:
        import akshare as ak
        r = ak.index_stock_cons_csindex(symbol="000300")
        return [(str(a).zfill(6), str(b)) for a, b in zip(r["成分券代码"], r["成分券名称"])]
    except Exception as e:
        print("成分股获取失败，退回 ETF 池:", str(e)[:80])
        return list(ETF.items())


def main():
    items = get_pool()
    print("标的数 %d，数据源顺序 腾讯/新浪/东财" % len(items))

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
    med = float(df["mom"].median())
    cand = df.head(max(int(len(df) * 0.1), TOP_N))
    pick = cand[(cand["ret5"] <= MAX_RET5 * 100) & (cand["amt"] >= MIN_AMT)].head(TOP_N)
    if pick.empty:
        pick = cand.head(TOP_N)

    lines = ["### %s 目标组合" % NOW.strftime("%m-%d"), "",
             "| 代码 | 名称 | 收盘 | 20日动量 | 5日涨幅 | 20日均额(亿) |",
             "| --- | --- | --- | --- | --- | --- |"]
    for _, r in pick.iterrows():
        lines.append("| %s | %s | %s | %+.2f%% | %+.2f%% | %.2f |"
                     % (r["code"], r["name"], r["close"], r["mom"], r["ret5"], r["amt"]))
    if med < -1:
        lines += ["", "**弱市**：全池动量中位数 %+.2f%%，建议半仓或空仓（每只≤10%%）。" % med]
    else:
        lines += ["", "全池动量中位数 %+.2f%%，建议每只 %d%%。" % (med, 100 // max(len(pick), 1))]
    lines += ["", "调仓建议：每周一执行，开盘 15 分钟后挂限价单（昨收±1%），不追高。",
              "", "<sub>数据源：腾讯/新浪/东财 | 仅供参考，不构成投资建议</sub>"]

    body = "\n".join(lines)
    print(body)

    if not KEY:
        print("未配置 SERVERCHAN_SENDKEY，仅打印")
        sys.exit(1)
    title = ("%s 信号%d只 " % (NOW.strftime("%m-%d"), len(pick))) + "、".join(pick["name"].head(2))
    r = requests.post("https://sctapi.ftqq.com/%s.send" % KEY,
                      data={"title": title[:32], "desp": body}, timeout=20)
    print("推送返回 HTTP=%s %s" % (r.status_code, r.text[:200]))
    if r.status_code != 200 or '"errno":0' not in r.text.replace(" ", ""):
        sys.exit(1)


main()
