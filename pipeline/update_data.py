#!/usr/bin/env python3
"""운용데스크 데이터 갱신 스크립트

종가 기준으로 두 파일을 만듭니다.
  data/market.csv  종목별 일별 종가·거래량·외국인/기관 순매수 (KRX, pykrx 사용)
  data/macro.json  금리·환율·물가 시계열 (한국은행 ECOS, 미국 FRED)

사용:  python pipeline/update_data.py --out data
환경변수:
  ECOS_KEY  한국은행 ECOS 인증키 (https://ecos.bok.or.kr, 무료). 없으면 한국 금리·물가는 건너뜁니다.
  KRX_ID / KRX_PW  KRX 로그인이 필요한 pykrx 버전에서만 사용합니다.

주의: 실제 KRX/ECOS/FRED 서버에 대한 호출은 이 스크립트를 만든 환경에서 실행해 보지 못했습니다.
(가짜 데이터 소스로 CSV/JSON 형식과 앱 호환성만 검증했습니다.) 처음 실행할 때 로그를 확인하세요.
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.request

KST = dt.timezone(dt.timedelta(hours=9))


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (fund-desk)"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read().decode("utf-8", "replace")


def retry(fn, tries=3, wait=1.5, what=""):
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if i == tries - 1:
                log(f"  실패({what}): {e}")
                return None
            time.sleep(wait * (i + 1))


# ----------------------------------------------------------------------------
# 1) 시장 데이터 (KRX)
# ----------------------------------------------------------------------------
def fetch_market(out_path, days=130, top_kospi=250, top_kosdaq=350, pause=0.35, stock=None):
    if stock is None:
        from pykrx import stock  # noqa: PLC0415  (필요할 때만 불러옴)
    today = dt.datetime.now(KST).date()
    start = (today - dt.timedelta(days=int(days * 1.7) + 10)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    ref = retry(lambda: stock.get_market_ohlcv_by_date(start, end, "005930"), what="거래일 조회")
    if ref is None or len(ref) == 0:
        raise RuntimeError("거래일 목록을 가져오지 못했습니다 (005930 일별 시세 조회 실패).")
    tdays = [d.strftime("%Y%m%d") for d in ref.index][-days:]
    latest = tdays[-1]
    log(f"거래일 {len(tdays)}일 ({tdays[0]} ~ {latest})")

    # 종목 풀: 최근일 거래대금 상위 N개 (시장별)
    universe = {}
    for mk, topn in (("KOSPI", top_kospi), ("KOSDAQ", top_kosdaq)):
        o = retry(lambda mk=mk: stock.get_market_ohlcv(latest, market=mk), what=f"{mk} 최근일 시세")
        if o is None or len(o) == 0:
            raise RuntimeError(f"{mk} 최근일 시세를 가져오지 못했습니다.")
        o = o[o["거래대금"] > 0].sort_values("거래대금", ascending=False).head(topn)
        for t in o.index:
            universe[str(t)] = mk
        log(f"{mk} 종목 풀 {len(o)}개")

    sector = {}
    for mk in ("KOSPI", "KOSDAQ"):
        try:
            sc = stock.get_market_sector_classifications(latest, mk)
            for t, row in sc.iterrows():
                sector[str(t)] = str(row.get("업종명", "-"))
        except Exception as e:  # noqa: BLE001
            log(f"  업종 분류 생략({mk}): {e}")

    names, rows, failed = {}, [], 0
    for di, d in enumerate(tdays):
        day_rows = []
        ok = True
        for mk in ("KOSPI", "KOSDAQ"):
            oh = retry(lambda mk=mk, d=d: stock.get_market_ohlcv(d, market=mk), what=f"{d} {mk} 시세")
            fr = retry(lambda mk=mk, d=d: stock.get_market_net_purchases_of_equities(d, d, mk, "외국인"), what=f"{d} {mk} 외국인")
            ins = retry(lambda mk=mk, d=d: stock.get_market_net_purchases_of_equities(d, d, mk, "기관합계"), what=f"{d} {mk} 기관")
            if oh is None or fr is None or ins is None:
                ok = False
                break
            for t in oh.index:
                t = str(t)
                if universe.get(t) != mk:
                    continue
                close, vol = float(oh.at[t, "종가"]), float(oh.at[t, "거래량"])
                if close <= 0:
                    continue  # 거래정지 등
                f = float(fr.at[t, "순매수거래대금"]) if t in fr.index else 0.0
                i = float(ins.at[t, "순매수거래대금"]) if t in ins.index else 0.0
                if t not in names:
                    nm = fr.at[t, "종목명"] if (t in fr.index and "종목명" in fr.columns) else None
                    names[t] = nm if nm else (retry(lambda t=t: stock.get_market_ticker_name(t), what="종목명") or t)
                day_rows.append((d, t, names[t], mk, sector.get(t, "-"), int(close), int(vol), int(f), int(i)))
        if ok:
            rows.extend(day_rows)
        else:
            failed += 1
            log(f"  {d} 건너뜀")
        if (di + 1) % 20 == 0:
            log(f"  진행 {di + 1}/{len(tdays)}")
        time.sleep(pause)
    if failed > max(3, len(tdays) * 0.3):
        raise RuntimeError(f"실패한 거래일이 너무 많습니다({failed}/{len(tdays)}). 기존 파일을 유지합니다.")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["일자", "종목코드", "종목명", "시장", "업종", "종가", "거래량", "외국인순매수", "기관순매수"])
        w.writerows(rows)
    os.replace(tmp, out_path)
    log(f"market.csv 저장: {len(rows)}행, 종목 {len(names)}개, 순매수 단위는 원")


def fetch_market_fdr(out_path, days=130, top_kospi=1000, top_kosdaq=2000, pause=0.15, fdr=None, top_etf=200):
    """대체 경로: FinanceDataReader (KRX 로그인 불필요). 수급(외국인/기관) 열은 비워 둡니다.
    앱은 수급 열이 없으면 수급 항목을 제외하고 나머지 4개 항목으로 점수를 계산합니다."""
    if fdr is None:
        import FinanceDataReader as fdr  # noqa: PLC0415
    today = dt.datetime.now(KST).date()
    start = (today - dt.timedelta(days=int(days * 1.7) + 10)).strftime("%Y-%m-%d")
    rows, names, seen = [], 0, set()
    plan = [("KOSPI", top_kospi, "KOSPI"), ("KOSDAQ", top_kosdaq, "KOSDAQ")]
    if top_etf > 0:
        plan.append(("ETF", top_etf, "ETF/KR"))
    for mk, topn, listing in plan:
        lst = retry(lambda listing=listing: fdr.StockListing(listing), what=f"{mk} 종목 목록")
        if lst is None or len(lst) == 0:
            raise RuntimeError(f"{mk} 종목 목록을 가져오지 못했습니다.")
        key = "Amount" if "Amount" in lst.columns else ("Marcap" if "Marcap" in lst.columns else None)
        if key:
            lst = lst.sort_values(key, ascending=False)
        lst = lst.head(topn)
        sec_col = "Sector" if "Sector" in lst.columns else None
        code_col = "Code" if "Code" in lst.columns else "Symbol"
        for _, r in lst.iterrows():
            code = str(r[code_col]).zfill(6)
            if code in seen:
                continue
            seen.add(code)
            df = retry(lambda code=code: fdr.DataReader(code, start), tries=2, what=code)
            if df is None or len(df) < 71:
                continue
            df = df.tail(days)
            nm = str(r.get("Name", code))
            sec = str(r[sec_col]) if sec_col and r[sec_col] == r[sec_col] else ("ETF" if mk == "ETF" else "-")
            for idx, x in df.iterrows():
                if float(x["Close"]) <= 0:
                    continue
                rows.append((idx.strftime("%Y%m%d"), code, nm, mk, sec, int(x["Close"]), int(x["Volume"])))
            names += 1
            time.sleep(pause)
        log(f"{mk} 완료 (누적 종목 {names}개)")
    if names < 50:
        raise RuntimeError(f"받은 종목이 너무 적습니다({names}개). 기존 파일을 유지합니다.")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["일자", "종목코드", "종목명", "시장", "업종", "종가", "거래량"])
        w.writerows(rows)
    os.replace(tmp, out_path)
    log(f"market.csv 저장(수급 없음): {len(rows)}행, 종목 {names}개")


# ----------------------------------------------------------------------------
# 2) 매크로 (ECOS / FRED)
# ----------------------------------------------------------------------------
def fred_series(sid, start):
    txt = http_get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}&cosd={start}")
    out = []
    for i, line in enumerate(txt.strip().splitlines()):
        if i == 0:
            continue
        p = line.split(",")
        if len(p) < 2 or p[1] in (".", ""):
            continue
        try:
            out.append((p[0][:10], float(p[1])))
        except ValueError:
            pass
    return out


def ecos_series(key, stat, cycle, start, end, item):
    url = f"https://ecos.bok.or.kr/api/StatisticSearch/{key}/json/kr/1/10000/{stat}/{cycle}/{start}/{end}/{item}"
    data = json.loads(http_get(url))
    rows = data.get("StatisticSearch", {}).get("row", [])
    out = []
    for r in rows:
        t, v = r["TIME"], r.get("DATA_VALUE")
        if v in (None, ""):
            continue
        d = f"{t[:4]}-{t[4:6]}-{t[6:8]}" if len(t) == 8 else f"{t[:4]}-{t[4:6]}-01"
        out.append((d, float(v)))
    return sorted(out)


def to_yoy(pts):
    """월간 지수 -> 전년 동월 대비 상승률(%)"""
    m = {d[:7]: v for d, v in pts}
    out = []
    for d, v in pts:
        y, mo = int(d[:4]), d[5:7]
        prev = m.get(f"{y - 1}-{mo}")
        if prev:
            out.append((d, round((v / prev - 1) * 100, 2)))
    return out


def fetch_macro(out_path, ecos_key=None):
    today = dt.datetime.now(KST).date()
    s_d = (today - dt.timedelta(days=430)).strftime("%Y-%m-%d")
    s_m = (today - dt.timedelta(days=1200)).strftime("%Y-%m-%d")
    e_d, e_m = today.strftime("%Y%m%d"), today.strftime("%Y%m")
    sd, sm = s_d.replace("-", ""), s_m[:7].replace("-", "")
    series = {}

    def put(sid, name, unit, freq, pts, keep):
        if pts:
            series[sid] = {"name": name, "unit": unit, "freq": freq, "pts": [[d, round(v, 4)] for d, v in pts[-keep:]]}
            log(f"  {sid}: {len(pts)}개, 최근 {pts[-1][0]} = {pts[-1][1]}")

    def tryit(label, fn):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            log(f"  {label} 실패: {e}")
            return None

    put("us10y", "미국 국채 10년", "%", "D", tryit("us10y", lambda: fred_series("DGS10", s_d)), 400)
    put("usBase", "미국 기준금리(상단)", "%", "D", tryit("usBase", lambda: fred_series("DFEDTARU", s_d)), 400)
    cpi = tryit("usCpi", lambda: fred_series("CPIAUCSL", s_m))
    put("usCpi", "미국 소비자물가(전년비)", "%", "M", to_yoy(cpi) if cpi else None, 30)

    fx = None
    if ecos_key:
        fx = tryit("usdkrw(ECOS)", lambda: ecos_series(ecos_key, "731Y001", "D", sd, e_d, "0000001"))
    if not fx:
        fx = tryit("usdkrw(FRED)", lambda: fred_series("DEXKOUS", s_d))
    put("usdkrw", "원/달러 환율", "원", "D", fx, 400)

    if ecos_key:
        put("krBase", "한국 기준금리", "%", "D", tryit("krBase", lambda: ecos_series(ecos_key, "722Y001", "D", sd, e_d, "0101000")), 400)
        put("kr3y", "국고채 3년", "%", "D", tryit("kr3y", lambda: ecos_series(ecos_key, "817Y002", "D", sd, e_d, "010200000")), 400)
        put("kr10y", "국고채 10년", "%", "D", tryit("kr10y", lambda: ecos_series(ecos_key, "817Y002", "D", sd, e_d, "010210000")), 400)
        kc = tryit("krCpi", lambda: ecos_series(ecos_key, "901Y009", "M", sm, e_m, "0"))
        put("krCpi", "한국 소비자물가(전년비)", "%", "M", to_yoy(kc) if kc else None, 30)
    else:
        log("  ECOS_KEY가 없어 한국 기준금리·국고채·물가는 건너뜁니다.")

    if not series:
        raise RuntimeError("가져온 매크로 시계열이 없습니다.")
    asof = max(s["pts"][-1][0] for s in series.values())
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"asof": asof, "series": series}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out_path)
    log(f"macro.json 저장: 지표 {len(series)}개, 기준일 {asof}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="운용데스크 데이터 갱신")
    ap.add_argument("--out", default="data")
    ap.add_argument("--days", type=int, default=130, help="가져올 거래일 수 (점수 계산에 최소 70 필요)")
    ap.add_argument("--top-kospi", type=int, default=1000)
    ap.add_argument("--top-kosdaq", type=int, default=2000)
    ap.add_argument("--top-etf", type=int, default=200)
    ap.add_argument("--pause", type=float, default=0.35, help="호출 간 대기(초)")
    ap.add_argument("--skip-market", action="store_true")
    ap.add_argument("--skip-macro", action="store_true")
    a = ap.parse_args(argv)
    ok = 0
    if not a.skip_market:
        log("== 시장 데이터 ==")
        out = os.path.join(a.out, "market.csv")
        try:
            fetch_market(out, a.days, a.top_kospi, a.top_kosdaq, a.pause)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log(f"[KRX(pykrx) 실패] {e}")
            log("→ 대체 경로(FinanceDataReader, 수급 없이 가격만)로 다시 시도합니다.")
            try:
                fetch_market_fdr(out, a.days, a.top_kospi, a.top_kosdaq, top_etf=a.top_etf)
                ok += 1
            except Exception as e2:  # noqa: BLE001
                log(f"[시장 데이터 실패] {e2}")
    if not a.skip_macro:
        log("== 매크로 ==")
        try:
            fetch_macro(os.path.join(a.out, "macro.json"), os.environ.get("ECOS_KEY"))
            ok += 1
        except Exception as e:  # noqa: BLE001
            log(f"[매크로 실패] {e}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
