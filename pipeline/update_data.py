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


def http_bytes(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (fund-desk)"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def http_get(url, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (fund-desk)"}
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h)
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


def fetch_market_fdr(out_path, days=130, top_kospi=1000, top_kosdaq=2000, pause=0.15, fdr=None, top_etf=5000):
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
                if mk == "ETF":
                    log(f"[ETF 제외] {code} {r.get('Name', '')}: 가격 이력 {0 if df is None else len(df)}일 (71일 미만)")
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
    put("vix", "VIX (미국 변동성지수)", "", "D", tryit("vix", lambda: fred_series("VIXCLS", s_d)), 400)
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


# ----------------------------------------------------------------------------
# 3) 보유종목 뉴스 (Google News RSS, 제목·출처·날짜·링크만 저장)
# ----------------------------------------------------------------------------
def read_watchlist(path):
    """data/watchlist.txt: 한 줄에 종목명 하나 (앞에 '코드 ' 를 붙여도 됨, # 은 주석)."""
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            t = line.split("#")[0].strip()
            if not t:
                continue
            p = t.split(None, 1)
            if len(p) == 2 and p[0].isdigit():
                out.append((p[0].zfill(6), p[1].strip()))
            else:
                out.append(("", t))
    return out


def parse_news_rss(xml_text, limit=5):
    import email.utils
    import xml.etree.ElementTree as ET
    items, seen = [], set()
    root = ET.fromstring(xml_text)
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        src_el = it.find("source")
        src = (src_el.text or "").strip() if src_el is not None else ""
        if src and title.endswith(" - " + src):
            title = title[: -len(src) - 3].strip()
        if not title or not link.startswith("http") or title in seen:
            continue
        seen.add(title)
        try:
            d = email.utils.parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST).strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            d = ""
        items.append({"t": title, "u": link, "s": src, "d": d})
        if len(items) >= limit:
            break
    return items


def clean_html(t):
    import html
    import re
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", t or ""))).strip()


def naver_hub_news(name, cid, secret, http, per=5):
    """NAVER API HUB 뉴스 검색: 제목·요약(description)·원문 링크·발행시각. 최근순 30건 중 종목명이 들어간 최신 기사."""
    import email.utils
    import urllib.parse
    url = "https://naverapihub.apigw.ntruss.com/search/v1/news?" + urllib.parse.urlencode({"query": name, "display": 30, "start": 1, "sort": "date", "format": "json"})
    data = json.loads(http(url, headers={"X-NCP-APIGW-API-KEY-ID": cid, "X-NCP-APIGW-API-KEY": secret}))
    today = dt.datetime.now(KST).date()
    keyn = name.replace(" ", "").lower()
    items, seen, n7 = [], set(), 0
    for it in data.get("items", []):
        title, desc = clean_html(it.get("title")), clean_html(it.get("description"))
        if not title or title in seen:
            continue
        if keyn not in (title + desc).replace(" ", "").lower():
            continue
        seen.add(title)
        try:
            d = email.utils.parsedate_to_datetime(it.get("pubDate")).astimezone(KST).date()
        except Exception:  # noqa: BLE001
            d = None
        if d and (today - d).days <= 7:
            n7 += 1
        link = it.get("link") or it.get("originallink") or ""
        if not link.startswith("http"):
            continue
        src = urllib.parse.urlparse(it.get("originallink") or link).netloc.replace("www.", "")
        items.append({"t": title, "u": link, "s": src, "d": d.strftime("%Y-%m-%d") if d else "", "m": desc[:140]})
    return {"items": items[:per], "n7": n7}


def fetch_news(out_path, watch_path, per=5, pause=0.5, http=None, hub=None):
    import urllib.parse
    http = http or http_get
    if hub is None:
        cid, sec = os.environ.get("NAVER_HUB_ID"), os.environ.get("NAVER_HUB_SECRET")
        hub = (cid, sec) if cid and sec else None
    log("  뉴스 출처: " + ("NAVER API HUB (실패 시 Google News로 대체)" if hub else "Google News RSS (NAVER_HUB_ID/SECRET 없음)"))
    wl = read_watchlist(watch_path)
    if not wl:
        log(f"  {watch_path} 가 없거나 비어 있어 뉴스를 건너뜁니다. (한 줄에 종목명 하나씩 적어 올리세요)")
        return False
    res, fail = {}, 0
    for code, name in wl[:60]:
        if hub:
            try:
                r = naver_hub_news(name, hub[0], hub[1], http, per)
                if r["items"]:
                    res[code or name] = {"name": name, "src": "naver", **r}
                    time.sleep(pause)
                    continue
            except Exception as e:  # noqa: BLE001
                log(f"  [NAVER 실패→Google] {name}: {type(e).__name__} {str(e)[:80]}")
        q = urllib.parse.quote(f'"{name}" when:14d')
        url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
        try:
            res[code or name] = {"name": name, "items": parse_news_rss(http(url), per)}
        except Exception as e:  # noqa: BLE001
            fail += 1
            log(f"  [뉴스 실패] {name}: {e}")
        time.sleep(pause)
    if not res:
        raise RuntimeError("뉴스를 한 건도 가져오지 못했습니다. 기존 파일을 유지합니다.")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"asof": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M"), "stocks": res}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out_path)
    log(f"news.json 저장: {len(res)}종목 (실패 {fail})")
    return True


# ----------------------------------------------------------------------------
# 4) 주요 공시 (OpenDART: 고유번호 목록 → 종목코드 연결 → 공시검색)
# ----------------------------------------------------------------------------
DART_API = "https://opendart.fss.or.kr/api"
# 보고서명에 들어 있는 말로 공시 성격을 분류 (앞에서부터 먼저 맞는 것)
DART_KINDS = [
    ("risk", "위험", ("상장폐지", "관리종목", "횡령", "배임", "감사의견", "불성실공시", "거래정지", "회생", "파산", "부도", "소송", "투자주의")),
    ("fund", "자금조달", ("유상증자", "전환사채", "교환사채", "신주인수권", "감자", "무상증자")),
    ("back", "주주환원", ("자기주식", "배당", "주식소각")),
    ("earn", "실적", ("영업(잠정)실적", "잠정실적", "실적", "손익구조")),
    ("deal", "계약·투자", ("단일판매", "공급계약", "시설투자", "신규시설", "타법인주식", "유형자산")),
    ("mna", "지배구조", ("합병", "분할", "영업양수", "영업양도", "최대주주", "주식교환", "공개매수", "경영권")),
    ("rep", "정기보고서", ("사업보고서", "분기보고서", "반기보고서")),
]


def dart_kind(name):
    for k, label, words in DART_KINDS:
        if any(w in name for w in words):
            return k, label
    return "etc", "공시"


def dart_corp_map(key, get_bytes):
    """corpCode.xml(ZIP) → ({종목코드: 고유번호}, {회사명: (고유번호, 종목코드)}) — 상장사만."""
    import io
    import xml.etree.ElementTree as ET
    import zipfile
    raw = get_bytes(f"{DART_API}/corpCode.xml?crtfc_key={key}")
    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise RuntimeError("고유번호 파일을 받지 못했습니다(키 오류·한도 초과 가능): " + raw[:120].decode("utf-8", "replace").replace(key, "***"))
    by_code, by_name = {}, {}
    for el in ET.fromstring(z.read("CORPCODE.xml")).iter("list"):
        sc = (el.findtext("stock_code") or "").strip()
        cc = (el.findtext("corp_code") or "").strip()
        if not sc or not cc:
            continue
        by_code[sc] = cc
        by_name.setdefault((el.findtext("corp_name") or "").strip(), (cc, sc))
    return by_code, by_name


def fetch_dart(out_path, watch_path, key, days=30, per=5, pause=0.2, get_bytes=None, get_text=None):
    import urllib.parse
    get_bytes = get_bytes or http_bytes
    get_text = get_text or http_get
    wl = read_watchlist(watch_path)
    if not wl:
        log(f"  {watch_path} 가 없거나 비어 있어 공시를 건너뜁니다.")
        return False
    by_code, by_name = dart_corp_map(key, get_bytes)
    log(f"  상장사 고유번호 {len(by_code)}건 확인")
    today = dt.datetime.now(KST).date()
    bgn, end = (today - dt.timedelta(days=days)).strftime("%Y%m%d"), today.strftime("%Y%m%d")
    res, skipped = {}, []
    for code, name in wl[:60]:
        cc, sc = by_code.get(code), code
        if not cc and name in by_name:
            cc, sc = by_name[name]
        if not cc:
            skipped.append(name)  # ETF·비상장은 공시 대상이 아님
            continue
        q = urllib.parse.urlencode({"crtfc_key": key, "corp_code": cc, "bgn_de": bgn, "end_de": end, "page_count": 30})
        data = json.loads(get_text(f"{DART_API}/list.json?{q}"))
        st = data.get("status")
        if st == "013":
            res[sc] = {"name": name, "items": []}
        elif st != "000":
            raise RuntimeError(f"DART 오류 {st}: {data.get('message', '')}")
        else:
            items = []
            for it in data.get("list", []):
                nm = (it.get("report_nm") or "").strip()
                if not nm or not it.get("rcept_no"):
                    continue
                k, label = dart_kind(nm)
                d = it.get("rcept_dt") or ""
                items.append({"t": nm, "u": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={it['rcept_no']}", "d": f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d, "k": k, "l": label, "f": (it.get("flr_nm") or "").strip()})
            # 정기보고서·기타보다 중요한 공시를 앞에, 같은 중요도면 최신순
            order = {"risk": 0, "fund": 1, "back": 2, "earn": 2, "deal": 2, "mna": 2, "etc": 3, "rep": 4}
            items.sort(key=lambda x: x["d"], reverse=True)
            items.sort(key=lambda x: order.get(x["k"], 3))
            res[sc] = {"name": name, "items": items[:per], "n": len(items)}
        time.sleep(pause)
    if skipped:
        log(f"  공시 대상이 아니어서 건너뜀(ETF·비상장 등): {', '.join(skipped[:10])}{' …' if len(skipped) > 10 else ''}")
    if not res:
        raise RuntimeError("공시를 조회할 상장사가 하나도 없습니다.")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"asof": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M"), "days": days, "stocks": res}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out_path)
    log(f"dart.json 저장: {len(res)}종목")
    return True


# ----------------------------------------------------------------------------
# 5) 밸류에이션 자동 계산 (OpenDART 재무 + FDR 시가총액 → PER·PBR·ROE)
# ----------------------------------------------------------------------------
class DartLimit(RuntimeError):
    pass


def _amt(x):
    """'1,234' / '(1,234)' / '-1,234' → 정수, 값이 없으면 None."""
    t = str(x or "").replace(",", "").replace(" ", "")
    if not t or t == "-":
        return None
    neg = t.startswith("(") and t.endswith(")")
    t = t.strip("()")
    try:
        v = int(float(t))
    except ValueError:
        return None
    return -v if neg else v


def _dart_rows(url, get_text):
    data = json.loads(get_text(url))
    st = data.get("status")
    if st == "013":
        return []
    if st == "020":
        raise DartLimit("DART 하루 호출 한도(20,000회)를 넘었습니다.")
    if st != "000":
        raise RuntimeError(f"DART 오류 {st}: {data.get('message', '')}")
    return data.get("list", [])


def dart_pick(rows):
    """한 회사의 주요계정 행들 → (당기순이익, 자본총계, 'C'|'O'). 연결(CFS) 우선, 없으면 개별(OFS)."""
    best = {}
    for r in rows:
        if r.get("currency") not in (None, "", "KRW"):
            continue
        fs = r.get("fs_div") or "OFS"
        nm = (r.get("account_nm") or "").replace(" ", "")
        v = _amt(r.get("thstrm_amount"))
        if v is None:
            continue
        d = best.setdefault(fs, {})
        if nm == "자본총계":
            d["eq"] = v
        elif nm.startswith("당기순이익") or nm.startswith("당기순손실") or nm == "분기순이익" or nm == "반기순이익":
            d.setdefault("ni", v)
    for fs, tag in (("CFS", "C"), ("OFS", "O")):
        d = best.get(fs)
        if d and "eq" in d and "ni" in d:
            return d["ni"], d["eq"], tag
    return None


def dart_financials(corp_codes, fy, key, get_text, pause=0.3, batch=100, stock_of=None):
    """{corp_code: (순이익, 자본총계, 구분)}. 다중회사 주요계정(100개씩) 우선, 실패하면 회사별 조회로 대체."""
    import urllib.parse
    out = {}
    for i in range(0, len(corp_codes), batch):
        chunk = corp_codes[i:i + batch]
        rows_by = {}
        q = urllib.parse.urlencode({"crtfc_key": key, "corp_code": ",".join(chunk), "bsns_year": fy, "reprt_code": "11011"})
        try:
            for r in _dart_rows(f"{DART_API}/fnlttMultiAcnt.json?{q}", get_text):
                cc = r.get("corp_code") or (stock_of or {}).get(r.get("stock_code"))
                if cc:
                    rows_by.setdefault(cc, []).append(r)
        except DartLimit:
            raise
        except Exception as e:  # noqa: BLE001
            log(f"  [다중회사 조회 실패→회사별 조회로 대체] {fy}년 {i // batch + 1}번째 묶음: {str(e).replace(key, '***')[:80]}")
            for cc in chunk:
                q1 = urllib.parse.urlencode({"crtfc_key": key, "corp_code": cc, "bsns_year": fy, "reprt_code": "11011"})
                try:
                    rows_by[cc] = _dart_rows(f"{DART_API}/fnlttSinglAcnt.json?{q1}", get_text)
                except DartLimit:
                    raise
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(pause / 2)
        for cc, rows in rows_by.items():
            got = dart_pick(rows)
            if got:
                out[cc] = got
        time.sleep(pause)
    return out


def valuation_metrics(marcap, ni, eq):
    """PER·PBR·ROE. 적자(PER)·자본잠식(PBR)은 -1로 표시."""
    per = round(min(marcap / ni, 9999), 2) if ni > 0 else -1
    if eq > 0:
        return per, round(marcap / eq, 2), round(ni / eq * 100, 1)
    return per, -1, None


def fetch_valuation(out_path, key, fdr=None, get_bytes=None, get_text=None, pause=0.3, year=None):
    get_bytes = get_bytes or http_bytes
    get_text = get_text or http_get
    if fdr is None:
        import FinanceDataReader as fdr  # noqa: N813
    lst = retry(lambda: fdr.StockListing("KRX"), what="KRX 종목 목록(시가총액)")
    code_col = "Code" if "Code" in lst.columns else "Symbol"
    if "Marcap" not in lst.columns:
        raise RuntimeError("종목 목록에 시가총액(Marcap) 열이 없습니다.")
    rows = [(str(r[code_col]).zfill(6), str(r.get("Name", "")), float(r["Marcap"] or 0)) for _, r in lst.iterrows()]
    caps = {c: m for c, n, m in rows if m > 0}
    names = {c: n for c, n, m in rows}
    # 우선주는 같은 회사의 보통주 시가총액에 합산 (코드 앞 5자리가 같고 이름이 '보통주명 + …우…')
    for c, n, m in rows:
        if m > 0 and c[5] != "0":
            base = c[:5] + "0"
            bn = names.get(base)
            if bn and n.startswith(bn) and "우" in n[len(bn):] and base in caps:
                caps[base] += m
    by_code, _ = dart_corp_map(key, get_bytes)
    codes = [c for c in by_code if c in caps]
    log(f"  재무 조회 대상 {len(codes)}종목 (상장사 {len(by_code)}건 중 시가총액 확인분)")
    stock_of = {v: k for k, v in by_code.items()}
    y = year or dt.datetime.now(KST).year - 1
    fin, fy_of = {}, {}
    for fy in (y, y - 1):  # 직전 사업연도 우선, 아직 사업보고서가 없는 회사는 그 전 해로 대체
        todo = [by_code[c] for c in codes if by_code[c] not in fin]
        if not todo:
            break
        got = dart_financials(todo, str(fy), key, get_text, pause=pause, stock_of=stock_of)
        for cc, v in got.items():
            fin[cc] = v
            fy_of[cc] = fy
        log(f"  {fy}년 사업보고서: {len(got)}/{len(todo)}종목 확보")
    res = {}
    for c in codes:
        cc = by_code[c]
        if cc in fin:
            ni, eq, tag = fin[cc]
            per, pbr, roe = valuation_metrics(caps[c], ni, eq)
            res[c] = [per, pbr, roe, fy_of[cc], tag]
    if not res:
        raise RuntimeError("밸류에이션을 한 종목도 계산하지 못했습니다. 기존 파일을 유지합니다.")
    pers = sorted(v[0] for v in res.values() if v[0] > 0)
    pbrs = sorted(v[1] for v in res.values() if v[1] > 0)
    if pers and pbrs:
        mp, mb = pers[len(pers) // 2], pbrs[len(pbrs) // 2]
        log(f"  점검: PER 중앙값 {mp:.1f}배 · PBR 중앙값 {mb:.2f}배 (보통 PER 5~30, PBR 0.5~3 사이면 정상, 이 범위를 크게 벗어나면 알려 주세요)")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"asof": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M"), "stocks": res}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out_path)
    log(f"valuation.json 저장: {len(res)}종목")
    return True


# ---- DART 이벤트(자사주 취득 · 시설투자 · 타법인 지분 취득) -> 투자 아이디어 후보 ----
# 공시목록(list.json)은 회사코드 없이 조회하면 최근 3개월까지만 가능하고, 공시 제목(report_nm)만 제공합니다.
EVENT_RULES = [  # (종류, 라벨, 포함 단어, 제외 단어)
    ("buyback", "자사주 취득", ("자기주식취득결정", "자기주식 취득 결정", "자기주식취득신탁계약체결"), ("처분", "해지", "결과보고")),
    ("capex", "시설투자", ("신규시설투자", "유형자산양수결정", "유형자산 양수 결정", "유형자산취득결정"), ("처분", "양도")),
    ("stake", "타법인 지분 취득", ("타법인주식및출자증권취득결정", "타법인 주식 및 출자증권 취득 결정"), ("처분",)),
]


def event_kind(name):
    n = (name or "").replace(" ", "")
    for k, label, inc, exc in EVENT_RULES:
        if any(w.replace(" ", "") in n for w in inc) and not any(w in n for w in exc):
            return k, label
    return None, None


def fetch_events(out_path, key, days=30, max_pages=15, pause=0.2, get_text=None):
    import urllib.parse
    get_text = get_text or http_get
    today = dt.datetime.now(KST).date()
    bgn, end = (today - dt.timedelta(days=min(days, 90))).strftime("%Y%m%d"), today.strftime("%Y%m%d")
    seen, items, scanned = set(), [], 0
    for ty in ("B", "I"):  # B: 주요사항보고, I: 거래소공시(신규시설투자등 자율공시 포함)
        page, total = 1, 1
        while page <= total and page <= max_pages:
            q = urllib.parse.urlencode({"crtfc_key": key, "bgn_de": bgn, "end_de": end, "pblntf_ty": ty, "page_no": page, "page_count": 100})
            data = json.loads(get_text(f"{DART_API}/list.json?{q}"))
            st = data.get("status")
            if st == "013":
                break
            if st != "000":
                raise RuntimeError(f"DART 오류 {st}: {data.get('message', '')}")
            total = int(data.get("total_page") or 1)
            for it in data.get("list", []):
                scanned += 1
                nm = (it.get("report_nm") or "").strip()
                sc = (it.get("stock_code") or "").strip()
                if not sc or not it.get("rcept_no") or it.get("corp_cls") not in ("Y", "K"):
                    continue  # 상장사(유가·코스닥)만
                k, label = event_kind(nm)
                if not k:
                    continue
                key2 = (sc, k)  # 같은 종목·같은 종류는 가장 최근 1건만(정정공시 중복 방지)
                d = it.get("rcept_dt") or ""
                row = {"c": sc, "n": (it.get("corp_name") or "").strip(), "m": "KOSPI" if it.get("corp_cls") == "Y" else "KOSDAQ", "k": k, "l": label, "t": nm,
                       "u": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={it['rcept_no']}", "d": f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d,
                       "fix": "정정" in nm}
                if key2 in seen:
                    continue
                seen.add(key2)
                items.append(row)
            page += 1
            time.sleep(pause)
    if scanned == 0:
        raise RuntimeError("조회된 공시가 없습니다.")
    items.sort(key=lambda x: x["d"], reverse=True)
    cnt = {}
    for x in items:
        cnt[x["k"]] = cnt.get(x["k"], 0) + 1
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"updated": today.isoformat(), "days": days, "items": items}, f, ensure_ascii=False)
    log(f"  공시 {scanned}건 확인 → 자사주 {cnt.get('buyback', 0)} · 시설투자 {cnt.get('capex', 0)} · 지분취득 {cnt.get('stake', 0)}건 저장 ({out_path})")
    return True


def main(argv=None):
    ap = argparse.ArgumentParser(description="운용데스크 데이터 갱신")
    ap.add_argument("--out", default="data")
    ap.add_argument("--days", type=int, default=130, help="가져올 거래일 수 (점수 계산에 최소 70 필요)")
    ap.add_argument("--top-kospi", type=int, default=1000)
    ap.add_argument("--top-kosdaq", type=int, default=2000)
    ap.add_argument("--top-etf", type=int, default=5000)
    ap.add_argument("--pause", type=float, default=0.35, help="호출 간 대기(초)")
    ap.add_argument("--skip-market", action="store_true")
    ap.add_argument("--skip-macro", action="store_true")
    ap.add_argument("--skip-news", action="store_true")
    ap.add_argument("--skip-dart", action="store_true")
    ap.add_argument("--skip-val", action="store_true")
    ap.add_argument("--skip-events", action="store_true")
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
    if not a.skip_news:
        log("== 뉴스 ==")
        try:
            fetch_news(os.path.join(a.out, "news.json"), os.path.join(a.out, "watchlist.txt"))
        except Exception as e:  # noqa: BLE001
            log(f"[뉴스 실패] {e}")
    if not a.skip_dart:
        log("== 공시 ==")
        dkey = os.environ.get("DART_KEY")
        if not dkey:
            log("  DART_KEY가 없어 공시를 건너뜁니다.")
        else:
            try:
                fetch_dart(os.path.join(a.out, "dart.json"), os.path.join(a.out, "watchlist.txt"), dkey)
            except Exception as e:  # noqa: BLE001
                log(f"[공시 실패] {str(e).replace(dkey, '***')}")
        if dkey and not a.skip_events:
            log("== 공시 이벤트(자사주·시설투자·지분취득) ==")
            try:
                fetch_events(os.path.join(a.out, "events.json"), dkey)
            except Exception as e:  # noqa: BLE001
                log(f"[이벤트 실패] {str(e).replace(dkey, '***')}")
        if dkey and not a.skip_val:
            log("== 밸류에이션 ==")
            try:
                fetch_valuation(os.path.join(a.out, "valuation.json"), dkey)
            except Exception as e:  # noqa: BLE001
                log(f"[밸류에이션 실패] {str(e).replace(dkey, '***')}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
