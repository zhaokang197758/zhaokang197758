# -*- coding: utf-8 -*-
"""每日动量选股信号 -> 微信推送（手机精简版）"""

import os
import time
import requests
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
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
KEY = os.getenv("SERVERCHAN_SENDKEY", "")


def tidy(df, d, c, a):
    out = pd.DataFrame({"date": pd.to_datetime(df[d]),
                        "close": pd.to_numeric(df[c], errors="coerce"),
                        "amount": pd.to_numeric(df[a], errors="coerce")}).dropna()
    out = out[out["close"] > 0].sort_values("date").reset_index(drop=True)
    m = out["amount"].tail(20).median()
    if pd.notna(m) and 0 < m < 1e5:
        out["amount"] = out["amount"] * 1e4
    return out


def hist(code, is_etf):
    s = (NOW - timedelta(days=150)).strftime("%Y%m%d")
    e = NOW.strftime("%Y%m%d")
    for _ in range(2):
        try:
            if is_etf:
                raw = ak.fund_etf_hist_em(symbol=code, period="daily",
                                          start_date=s, end_date=e, adjust="qfq")
                df = tidy(raw, "日期", "收盘", "成交额")
            else:
                raw = ak.stock_zh_a_hist(symbol=code, period="daily",
                                         start_date=s, end_date=e, adjust="qfq")
                df = tidy(raw, "日期", "收盘", "成交额")
            if len(df) >= 30:
                return df
        except Exception:
            pass
        time.sleep(0.6)
    try:
        p = "sh" if code[0] in "659" else ("sz" if code[0] in "031" else "bj")
        if is_etf:
            raw = ak.fund_etf_hist_sina(symbol=p + code)
            df = tidy(raw, "date", "close", "amount")
            df = df[df["date"] >= pd.Timestamp((NOW - timedelta(days=150)).date())]
        else:
            raw = ak.stock_zh_a_daily(symbol=p + code, start_date=s, end_date=e, adjust="qfq")
            df = tidy(raw, "date", "close", "amount")
        return df if len(df) >= 30 else None
    except Exception:
        return None


def one(item):
    code, name = item
    df = hist(code, POOL == "etf")
    if df is None or len(df) < MOM_WIN + 5:
        return None
    c = df["close"].values
    return {"code": code, "name": name, "close": round(float(c[-1]), 3),
            "mom": round(float(c[-1] / c[-1 - MOM_WIN] - 1) * 100, 2),
            "ret5": round(float(c[-1] / c[-6] - 1) * 100, 2),
            "amt": round(float(df["amount"].tail(20).mean()) / 1e8, 2)}


def get_pool():
    if POOL == "etf":
        return list(ETF.items())
    try:
        r = ak.index_stock_cons_csindex(symbol="000300")
        return [(str(a).zfill(6), str(b)) for a, b in zip(r["成分券代码"], r["成分券名称"])]
    except Exception:
        return list(ETF.items())


def main():
    items = get_pool()
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
    if not rows:
        print("no data")
        return

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
              "", "<sub>数据源：东财/新浪 | 仅供参考，不构成投资建议</sub>"]

    body = "\n".join(lines)
    print(body)
    if KEY:
        title = ("%s 信号%d只 " % (NOW.strftime("%m-%d"), len(pick))) + "、".join(pick["name"].head(2))
        try:
            requests.post("https://sctapi.ftqq.com/%s.send" % KEY,
                          data={"title": title[:32], "desp": body}, timeout=20)
            print("已推送")
        except Exception as e:
            print("推送失败", e)
    else:
        print("未配置 SERVERCHAN_SENDKEY，仅打印")


main()
