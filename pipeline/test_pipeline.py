"""가짜 pykrx/HTTP 로 update_data.py 의 출력 형식을 검증한다 (실서버 호출 없음)."""
import json, os, sys, tempfile, types
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import update_data as U

days = pd.bdate_range(end=pd.Timestamp.now().normalize(), periods=90)
TICK = {"KOSPI": ["005930", "000660", "105560"], "KOSDAQ": ["247540", "196170"]}
NAMES = {"005930": "삼성전자", "000660": "SK하이닉스", "105560": "KB금융", "247540": "에코프로비엠", "196170": "알테오젠"}


def price(t, d):
    return 10000 + (int(t[-3:]) % 7) * 1000 + 37 * days.get_loc(pd.Timestamp(d))


def parse(d):
    return pd.Timestamp(d)


fake = types.SimpleNamespace()
fake.get_market_ohlcv_by_date = lambda s, e, t: pd.DataFrame({"종가": [1] * len(days)}, index=days)
def ohlcv(d, market):
    t = TICK[market]
    return pd.DataFrame({"종가": [price(x, parse(d)) for x in t], "거래량": [100000 + i * 5000 for i in range(len(t))], "거래대금": [10**10 * (i + 1) for i in range(len(t))]}, index=t)
fake.get_market_ohlcv = ohlcv
def net(s, e, market, inv):
    t = TICK[market]
    return pd.DataFrame({"종목명": [NAMES[x] for x in t], "순매수거래대금": [(-1) ** i * 3 * 10**8 * (i + 1) for i in range(len(t))]}, index=t)
fake.get_market_net_purchases_of_equities = net
fake.get_market_sector_classifications = lambda d, m: pd.DataFrame({"종목명": [NAMES[x] for x in TICK[m]], "업종명": ["전기전자"] * len(TICK[m])}, index=TICK[m])
fake.get_market_ticker_name = lambda t: NAMES[t]

# 가짜 HTTP
def fake_http(url):
    if "fredgraph.csv" in url:
        sid = url.split("id=")[1].split("&")[0]
        if sid == "CPIAUCSL":
            rows = [f"{y}-{m:02d}-01,{100 + (y - 2023) * 3 + m * 0.2:.2f}" for y in (2023, 2024, 2025, 2026) for m in range(1, 13)]
        else:
            rows = [f"{d.strftime('%Y-%m-%d')},{4.0 + 0.01 * i:.3f}" for i, d in enumerate(days)] + [f"{(days[-1] + pd.Timedelta(days=1)).strftime('%Y-%m-%d')},."]
        return "observation_date," + sid + "\n" + "\n".join(rows)
    if "ecos.bok.or.kr" in url:
        cyc = url.split("/")[-4]
        if cyc == "M":
            rows = [{"TIME": f"{y}{m:02d}", "DATA_VALUE": f"{100 + (y - 2023) * 2.2 + m * 0.1:.2f}"} for y in (2023, 2024, 2025, 2026) for m in range(1, 13)]
        else:
            rows = [{"TIME": d.strftime("%Y%m%d"), "DATA_VALUE": f"{2.5 + 0.002 * i:.3f}"} for i, d in enumerate(days)]
        return json.dumps({"StatisticSearch": {"list_total_count": len(rows), "row": rows}})
    raise RuntimeError("unexpected " + url)

U.http_get = fake_http
out = tempfile.mkdtemp()
U.fetch_market(os.path.join(out, "market.csv"), days=80, top_kospi=3, top_kosdaq=2, pause=0, stock=fake)
U.fetch_macro(os.path.join(out, "macro.json"), ecos_key="TESTKEY")
print(out)
