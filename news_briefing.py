"""
PE Deal Briefing — 딜별 아카이빙 시스템
네이버 뉴스 크롤링 → Claude API 딜 추출/매칭 → deals.json 업데이트 → GitHub Pages 배포
"""

import csv
import io
import json
import os
import random
import subprocess
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ── 설정 ──────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "여기에_API_KEY_입력")
MODEL = "claude-sonnet-4-6"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

MAX_PAGES = 1
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
DEALS_JSON_PATH = os.path.join(OUTPUT_DIR, "deals.json")

# 제외 키워드 — 제목에 포함 시 자동 필터링
BLOCKLIST = [
    # 부동산
    "부동산", "아파트", "분양", "재개발", "재건축", "리츠", "REITs", "REIT",
    "오피스텔", "상가", "토지", "주택", "임대", "빌딩", "부지", "용지",
    "시공사", "시행사", "건설사", "건설사업", "시공", "시행",
    "공사", "착공", "준공", "입주", "분양가", "청약",
    "PF", "프로젝트파이낸싱", "부동산PF",
    # VC / 벤처
    "시리즈A", "시리즈B", "시리즈C", "시리즈D", "시리즈E",
    "프리A", "Pre-A", "프리시리즈",
    "벤처투자", "스타트업 투자", "액셀러레이터", "팁스",
    "벤처캐피탈", "VC 투자", "창업투자",
]

KEYWORDS = [
    # ── PE 행위 / 구조 ──────────────────────────────────
    "사모펀드 인수", "사모펀드 매각", "사모펀드 엑시트",
    "PEF 인수", "PEF 매각", "PEF 결성",
    "바이아웃", "경영권 매각", "경영권 인수",
    "FI 매각", "FI 엑시트", "재무적투자자 매각",
    "공개매수", "우선협상대상자", "우협 선정",
    "인수금융", "세컨더리 펀드", "블라인드 펀드 결성",
    "매각주관사 선정", "원매자 모집",
    "예비입찰", "본입찰",

    # ── 국내 PE 하우스 (전체) ─────────────────────────
    "HB투자파트너스",
    "갤럭시프라이빗에쿼티",
    "게임체인저인베스트먼트",
    "고릴라피이",
    "골든루트인베스트먼트",
    "골든오크인베스트먼트",
    "그래비티프라이빗에쿼티",
    "그레이트스톤파트너스",
    "그루투자파트너스",
    "그리니치프라이빗에쿼티",
    "그리핀인베스트먼트",
    "그린하버",
    "글랜우드프라이빗에쿼티",
    "기앤파트너스",
    "나우아이비캐피탈",
    "나이스투자파트너스",
    "노버스파트너스",
    "노앤파트너스",
    "노틱인베스트먼트",
    "노틱캐피탈코리아",
    "뉴레이크얼라이언스매니지먼트",
    "뉴메인에쿼티",
    "뉴아크파트너스",
    "뉴젠인베스트먼트",
    "다올프라이빗에쿼티",
    "다윈인베스트먼트",
    "다토즈파트너스",
    "대신프라이빗에쿼티",
    "더드림프라이빗에쿼티",
    "더블유제이프라이빗에쿼티",
    "더시드파트너스",
    "더웰스인베스트먼트",
    "더터닝포인트",
    "더함파트너스",
    "데일리파트너스",
    "도미누스에쿼티파트너스",
    "도미누스인베스트먼트",
    "드림스톤프라이빗에쿼티",
    "디비프라이빗에쿼티",
    "디에스프라이빗에쿼티",
    "디인베스트먼트",
    "디케이파트너스",
    "디티씨글로벌파트너스",
    "디티알파트너스",
    "라데팡스파트너스",
    "라이노스자산운용",
    "레드메사",
    "레버런트파트너스",
    "레이크브릿지에쿼티파트너스",
    "로드스톤프라이빗에쿼티",
    "로드인베스트먼트",
    "로이투자파트너스",
    "로터스프라이빗에쿼티",
    "루터프라이빗에쿼티",
    "루하프라이빗에쿼티",
    "리드캐피탈매니지먼트",
    "리오인베스트",
    "린드먼아시아인베스트먼트",
    "릴슨프라이빗에쿼티",
    "마그나인베스트먼트",
    "마이다스프라이빗에쿼티",
    "마이스터프라이빗에쿼티",
    "마일스톤그로쓰파트너스",
    "머큐리밸류파트너스",
    "메이븐그로쓰파트너스",
    "메인스트리트인베스트먼트",
    "메타인베스트먼트",
    "메티스톤에퀴티파트너스",
    "메티스프라이빗에쿼티",
    "모트프라이빗에쿼티",
    "모하비인베스트먼트",
    "뱅커스트릿",
    "베어스톤파트너스",
    "베이사이드프라이빗에쿼티",
    "베저스인베스트먼트",
    "부산에쿼티파트너스",
    "브라만투자자문",
    "브라이트스톤파트너스",
    "브로드써밋파트너스",
    "브릭스캐피탈매니지먼트",
    "브이아이지파트너스",
    "브이엘인베스트먼트",
    "브이원캐피탈파트너스",
    "브이원프라이빗에쿼티",
    "브이인베스트먼트",
    "브이티아이파트너스",
    "블랙펄프라이빗에쿼티",
    "블루닷파트너스",
    "비스톤에쿼티파트너스",
    "비에이프라이빗에쿼티",
    "비엔더블유인베스트먼트",
    "비케이피엘자산운용",
    "서앤컴퍼니",
    "세븐브릿지프라이빗에쿼티",
    "센트로이드인베스트먼트파트너스",
    "소시어스",
    "스마일게이트인베스트먼트",
    "스잔인베스트먼트코리아",
    "스카이레이크에쿼티파트너스",
    "스카이레이크인베스트먼트",
    "스타셋인베스트먼트",
    "스탠다드프라이빗에쿼티",
    "스텔라인베스트먼트",
    "스톤라인에쿼티파트너스",
    "스톤브릿지캐피탈",
    "스트라이커캐피탈매니지먼트",
    "스틱얼터너티브자산운용",
    "스틱인베스트먼트",
    "스프링힐파트너스",
    "시냅틱인베스트먼트",
    "시드프라이빗에쿼티",
    "시몬느인베스트먼트",
    "시몬느자산운용",
    "시에라인베스트먼트",
    "씨브이피프라이빗에쿼티",
    "씨씨지인베스트먼트아시아",
    "씨앤코어파트너스",
    "씨에이씨파트너스",
    "씨엘에스에이캐피탈파트너스코리아",
    "씨엘파트너스",
    "씨제이엘파트너스",
    "씨피파트너스",
    "아든파트너스",
    "아르게스프라이빗에쿼티",
    "아이디지캐피탈파트너스코리아",
    "아이엘씨에쿼티파트너스",
    "아이엠엠인베스트먼트",
    "아이엠엠자산운용",
    "아이엠엠프라이빗에쿼티",
    "아이젠프라이빗에쿼티",
    "아주아이비투자",
    "아크앤파트너스",
    "아틸라에쿼티파트너스",
    "알케미스트캐피탈파트너스코리아",
    "알파비스타인베스트먼트",
    "앰버스톤",
    "어센트프라이빗에쿼티",
    "어펄마캐피탈매니져스코리아",
    "얼라인파트너스자산운용",
    "에버마운트캐피탈매니지먼트",
    "에버베스트파트너스",
    "에벤투스파트너스",
    "에스더블유케이매니지먼트",
    "에스브이인베스트먼트",
    "에스비아이인베스트먼트",
    "에스비케이파트너스",
    "에스씨로이코리아",
    "에스앤제이인베스트",
    "에스에스피파이낸셜",
    "에스오엘캐피탈파트너스",
    "에스제이엘파트너스",
    "에스지프라이빗에쿼티",
    "에스케이에스프라이빗에쿼티",
    "에스투엘파트너스",
    "에스티리더스프라이빗에쿼티",
    "에이비즈파트너스",
    "에이스에쿼티파트너스",
    "에이에프씨코리아",
    "에이원프라이빗에쿼티",
    "에이치비투자파트너스",
    "에이치앤큐에쿼티파트너스",
    "에이치자산운용",
    "에이치지이니셔티브",
    "에이치프라이빗에쿼티",
    "에이투파트너스",
    "에이티유파트너스",
    "에이티피인베스트먼트",
    "에이피씨프라이빗에쿼티",
    "에임인베스트먼트",
    "에코프라임피이",
    "에프티프라이빗에쿼티",
    "엑셀시아캐피탈코리아",
    "엔베스터",
    "엔엘씨파트너스",
    "엔피엑스프라이빗에쿼티",
    "엘리베이션에쿼티파트너스코리아",
    "엘비인베스트먼트",
    "엘비프라이빗에쿼티",
    "엘에스에스프라이빗에쿼티",
    "엘엑스아시아",
    "엘엑스인베스트먼트",
    "엘케이투자파트너스",
    "엠비케이파트너스",
    "엠씨파트너스",
    "엠제이엠인베스트먼트파트너스",
    "연합자산관리",
    "옐로씨매니지먼트",
    "오로라파트너스",
    "오릭스프라이빗에쿼티코리아",
    "오아시스에쿼티파트너스",
    "오에프앤파트너스",
    "오케스트라어드바이저스코리아",
    "오큘러스에쿼티파트너스",
    "오티엄캐피탈",
    "오퍼스프라이빗에퀴티",
    "오페즈인베스트먼트",
    "오픈워터인베스트먼트",
    "오피르에쿼티파트너스",
    "와이어드파트너스",
    "와이제이에이인베스트먼트",
    "우리프라이빗에퀴티자산운용",
    "원데이즈프라이빗에쿼티",
    "원레이크파트너스",
    "원익투자파트너스",
    "웨일인베스트먼트",
    "웰투시인베스트먼트",
    "위더스파트너스코리아지피",
    "윈아시아파트너스",
    "유씨케이파트너스",
    "유안타인베스트먼트",
    "유진자산운용",
    "유진프라이빗에쿼티",
    "유티씨인베스트먼트",
    "이니어스프라이빗에쿼티",
    "이상파트너스",
    "이스트브릿지파트너스",
    "이앤에프프라이빗에퀴티",
    "이앤인베스트먼트",
    "이엠피벨스타",
    "이음프라이빗에쿼티",
    "인마크에쿼티파트너스",
    "인빅터스프라이빗에쿼티아시아",
    "인커스캐피탈파트너스",
    "인터베스트",
    "인텔렉추얼디스커버리",
    "인피너티캐피탈파트너스",
    "인하브파트너스엘티디",
    "제네시스프라이빗에쿼티",
    "제니타스인베스트먼트",
    "제이더블유앤파트너스",
    "제이스퀘어인베스트먼트",
    "제이씨파트너스",
    "제이앤더블유파트너스",
    "제이앤프라이빗에쿼티",
    "제이에스프라이빗에쿼티",
    "제이커브인베스트먼트",
    "제이케이엘파트너스",
    "제이케이위더스",
    "젠파트너스앤컴퍼니",
    "젤코바인베스트먼트",
    "지비알에이치케이",
    "지에스에이프라이빗에쿼티",
    "지오투자파트너스",
    "지투지프라이빗에쿼티",
    "차파트너스자산운용",
    "천일프라이빗에쿼티",
    "천지인엠파트너스",
    "카무르프라이빗에쿼티",
    "카이로스인베스트먼트",
    "카펠라프라이빗에쿼티",
    "칼리스타캐피탈",
    "캑터스프라이빗에쿼티",
    "캡스톤파트너스",
    "컴퍼니케이파트너스",
    "케이디비인베스트먼트",
    "케이비인베스트먼트",
    "케이스톤파트너스",
    "케이씨에이파트너스",
    "케이씨지아이",
    "케이알앤파트너스",
    "케이엔케이파트너스",
    "케이엘앤파트너스",
    "케이와이프라이빗에쿼티",
    "케이클라비스",
    "케이피지파트너스",
    "켈비던글로벌",
    "코리아와이드파트너스",
    "코스톤아시아",
    "쿨리지코너인베스트먼트",
    "쿼크프라이빗에쿼티",
    "퀸버인베스트먼트",
    "퀸테사인베스트먼트",
    "큐리어스파트너스",
    "큐캐피탈파트너스",
    "크레디언파트너스",
    "크레디인베스트",
    "크레비스파트너스",
    "크레센도에쿼티파트너스",
    "크레스코레이크파트너스",
    "크로스로드파트너스",
    "클레어인베스트먼트",
    "키스톤프라이빗에쿼티",
    "키움인베스트먼트",
    "키움프라이빗에쿼티",
    "타이키파트너스",
    "테넷에쿼티파트너스",
    "투썬인베스트",
    "트라이던트프라이빗에쿼티",
    "트루벤인베스트먼트",
    "티더블유에이",
    "티디엠프라이빗에쿼티",
    "티비인베스트먼트",
    "티앤케이프라이빗에쿼티",
    "티인베스트먼트",
    "티케이엘인베스트먼트파트너스",
    "티케인베스트먼트",
    "티티유프라이빗에쿼티",
    "파라투스인베스트먼트",
    "파빌리온프라이빗에쿼티",
    "파인우드프라이빗에쿼티",
    "파인트리자산운용",
    "파인트리파트너스",
    "파트너원인베스트먼트",
    "팍스톤매니지먼트",
    "패스트인베스트먼트",
    "팩텀프라이빗에쿼티",
    "퍼즐인베스트먼트코리아",
    "펄사캐피탈매니지먼트",
    "펄인베스트먼트",
    "포레스트파트너스",
    "포시즌캐피탈파트너스",
    "포어러너캐피탈파트너스",
    "포워드에퀴티파트너스",
    "프라이머시즌5",
    "프랙시스캐피탈파트너스",
    "프리미어파트너스",
    "플로우파트너스",
    "피씨에이치캐피탈파트너스",
    "피아이에이인베스트먼트파트너스",
    "피앤피인베스트먼트",
    "피에스얼라이언스",
    "피에스캐피탈파트너스",
    "피엔인베스트먼트",
    "피티에이에쿼티파트너스",
    "하버브릭스파트너스",
    "하베스트에쿼티파트너스",
    "하이랜드캐피탈매니지먼트코리아",
    "하일랜드에쿼티파트너스",
    "한국투자파트너스",
    "한국투자프라이빗에쿼티",
    "한앤브라더스",
    "한앤컴퍼니",
    "헤임달프라이빗에쿼티",
    "헬리오스프라이빗에쿼티",
    "현대투자파트너스",
    "화이트파인파트너스",
    "화인파트너스",
]


# ── 1단계: 크롤링 ──────────────────────────────────────

def get_cutoff_days() -> int:
    """월요일이면 4일(금~월), 나머지 평일이면 2일(전날~당일)"""
    return 4 if datetime.now().weekday() == 0 else 2


def build_search_url(keyword: str, start: int = 1) -> str:
    now = datetime.now()
    cutoff_days = get_cutoff_days()
    date_from = (now - timedelta(days=cutoff_days - 1)).strftime("%Y.%m.%d")
    date_to = now.strftime("%Y.%m.%d")
    params = (
        f"where=news"
        f"&query={quote(keyword)}"
        f"&sort=1"
        f"&ds={date_from}"
        f"&de={date_to}"
        f"&nso=so:dd,p:from{date_from.replace('.', '')}to{date_to.replace('.', '')}"
        f"&start={start}"
    )
    return f"https://search.naver.com/search.naver?{params}"


def parse_date_text(date_text: str) -> str:
    date_text = date_text.strip()
    now = datetime.now()
    try:
        digits = "".join(filter(str.isdigit, date_text))
        if "분 전" in date_text:
            minutes = int(digits) if digits else 0
            return (now - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M")
        elif "시간 전" in date_text:
            hours = int(digits) if digits else 0
            return (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M")
        elif "일 전" in date_text:
            days = int(digits) if digits else 0
            return (now - timedelta(days=days)).strftime("%Y-%m-%d")
        else:
            cleaned = date_text.rstrip(".")
            return cleaned.replace(".", "-")
    except Exception:
        return now.strftime("%Y-%m-%d")


def is_within_cutoff(date_str: str) -> bool:
    cutoff_days = get_cutoff_days()
    cutoff = datetime.now() - timedelta(days=cutoff_days - 1)
    cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return dt >= cutoff
    except ValueError:
        return True


def fetch_page(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"  [오류] 요청 실패: {e}")
        return None


def extract_articles(soup: BeautifulSoup) -> list[dict]:
    articles = []
    news_items = soup.select("div[class*='sds-comps-vertical-layout']")
    if not news_items:
        news_items = soup.select("div.news_area")
    for item in news_items:
        try:
            title_tag = item.select_one('a[data-heatmap-target=".tit"]')
            if not title_tag:
                title_tag = item.select_one("a.news_tit")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href", "")
            date_str = ""
            subtexts = item.select(".sds-comps-profile-info-subtext")
            for st in subtexts:
                txt = st.get_text(strip=True)
                current_year = str(datetime.now().year)
                if txt and any(k in txt for k in ["전", ".", current_year]):
                    date_str = parse_date_text(txt)
                    break
            if not date_str:
                for info in item.select("span.info"):
                    txt = info.get_text(strip=True)
                    if any(k in txt for k in ["전", ".", "2025", "2026"]):
                        date_str = parse_date_text(txt)
                        break
            if date_str and not is_within_cutoff(date_str):
                continue
            articles.append({"제목": title, "링크": link, "날짜": date_str})
        except Exception as e:
            print(f"  [경고] 기사 파싱 중 오류: {e}")
    return articles


def crawl_keyword(keyword: str) -> list[dict]:
    all_articles = []
    print(f'[검색] "{keyword}"')
    for page in range(MAX_PAGES):
        start = page * 10 + 1
        url = build_search_url(keyword, start)
        soup = fetch_page(url)
        if soup is None:
            break
        articles = extract_articles(soup)
        if not articles:
            break
        all_articles.extend(articles)
        print(f"  페이지 {page + 1}: {len(articles)}건")
        time.sleep(random.uniform(3.0, 6.0))
    return all_articles


def is_blocked(title: str) -> bool:
    return any(kw in title for kw in BLOCKLIST)


def deduplicate(articles: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    blocked = 0
    no_link = 0
    for a in articles:
        link = a.get("링크", "").strip()
        if not link:
            no_link += 1
            continue
        if link not in seen:
            seen.add(link)
            if is_blocked(a["제목"]):
                blocked += 1
                continue
            unique.append(a)
    if no_link:
        print(f"[링크 없음 필터] {no_link}건 제외")
    print(f"[블록리스트 필터] {blocked}건 제외")
    return unique


def crawl_all() -> list[dict]:
    print("=" * 50)
    print("  1단계: 네이버 뉴스 크롤링")
    print(f"  키워드 {len(KEYWORDS)}개 | 범위 {get_cutoff_days()}일")
    print("=" * 50)
    all_articles = []
    for i, kw in enumerate(KEYWORDS, 1):
        print(f"\n--- [{i}/{len(KEYWORDS)}] ---")
        all_articles.extend(crawl_keyword(kw))
        if i < len(KEYWORDS):
            time.sleep(random.uniform(5.0, 10.0))
    before = len(all_articles)
    unique = deduplicate(all_articles)
    print(f"\n[중복 제거] {before}건 → {len(unique)}건")
    return unique


# ── 2단계: Claude API — 딜 추출 및 매칭 ─────────────────

def load_deals() -> dict:
    if os.path.exists(DEALS_JSON_PATH):
        try:
            with open(DEALS_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  [경고] deals.json 로드 실패 ({e}), 빈 상태로 시작")
    return {"deals": [], "lastUpdated": ""}


def save_deals(data: dict):
    data["lastUpdated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    # 기존 파일 백업 후 저장 (손상 방지)
    backup_path = DEALS_JSON_PATH + ".bak"
    if os.path.exists(DEALS_JSON_PATH):
        try:
            import shutil
            shutil.copy2(DEALS_JSON_PATH, backup_path)
        except Exception as e:
            print(f"  [경고] 백업 실패: {e}")
    with open(DEALS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[저장] deals.json 업데이트 완료 (딜 {len(data['deals'])}건)")


def call_claude_batch(articles_batch: list[dict], existing_deals: list[dict], batch_num: int, total_batches: int) -> list[dict]:
    """기사 배치 하나를 Claude API로 처리"""
    today = datetime.now().strftime("%Y년 %m월 %d일")

    existing_summary = ""
    if existing_deals:
        existing_summary = "\n[기존 딜 목록 (매칭 참고용)]\n"
        for i, d in enumerate(existing_deals):
            existing_summary += f"{i}. ID:{d['id']} | {d['name']} | 현재단계:{d['stage']}\n"

    articles_text = "\n".join(
        [f"{a['제목']} | {a['링크']} | {a['날짜']}" for a in articles_batch]
    )

    prompt = f"""당신은 국내 Private Equity 딜 분석 전문가입니다.
아래는 {today} 기준 수집된 PE/M&A 관련 뉴스입니다. (배치 {batch_num}/{total_batches}, {len(articles_batch)}건)
{existing_summary}

[뉴스 목록]
{articles_text}

[지시사항]
1. 위 뉴스 전체를 빠짐없이 분석하여 딜을 MECE하게 추출하세요. 뉴스에 언급된 딜을 임의로 누락하거나 선별하는 것은 절대 금지입니다.
2. 같은 딜을 다루는 여러 기사는 하나로 묶으세요.
3. 기존 딜 목록과 같은 딜이면 해당 딜 ID를 그대로 사용하세요.
4. 각 딜의 "repArticle"(대표 기사)은 반드시 1개만 선정하세요. 해당 딜 관련 기사 중 더벨(thebell.co.kr) 또는 딜사이트(dealsite.co.kr) 기사가 존재하면 반드시 그 중에서 선택하세요. 없을 경우 인베스트조선 > 기타 순으로 선택. 링크는 반드시 뉴스 목록에 실제 존재하는 값이어야 합니다.
5. 딜 단계: "매각 검토중" | "주관사 선정" | "예비입찰" | "본입찰" | "우선협상자 선정" | "실사 진행중" | "SPA 체결" | "딜 클로즈" | "기타"
6. 아래 유형의 딜은 "deals" 배열이 아닌 "realestate" 배열에 넣으세요:
   - 부동산 자산 매매: 아파트, 오피스텔, 상가, 빌딩, 토지, 주택, 리츠(REITs), 물류센터, 호텔, 데이터센터
   - 재개발/재건축: 시공사 선정, 시행사, 정비사업, 도시개발
   - 프로젝트파이낸싱(PF): 부동산PF, 건설 자금 조달
   - 건설/시공: 착공, 준공, 시공자 선정
7. 아래 유형의 뉴스는 "deals"/"realestate" 어디에도 넣지 말고 완전히 무시하세요:
   - VC/벤처 투자: 시리즈A/B/C/D, 프리A, 스타트업 투자유치, 벤처캐피탈 투자
   - 창업투자조합 결성 (VC 운용사 주도)
   - IPO/상장 (PE 엑시트 목적이 아닌 순수 공모)

반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트 일절 금지:

{{
  "deals": [
    {{
      "id": "기존딜이면 기존ID, 신규면 null",
      "name": "딜명",
      "stage": "딜 단계",
      "summary": "3~4줄 요약. 거래 당사자, 추정 규모, 딜 단계, 배경 포함. 음슴체로 작성.",
      "repArticle": {{
        "title": "대표 기사 제목",
        "link": "대표 기사 링크",
        "date": "날짜",
        "source": "thebell 또는 dealsite 또는 기타매체명"
      }},
      "articles": [
        {{
          "title": "기사 제목",
          "link": "기사 링크",
          "date": "날짜"
        }}
      ],
      "isNew": true
    }}
  ],
  "realestate": [
    {{
      "id": null,
      "name": "딜명",
      "stage": "딜 단계",
      "summary": "2~3줄 요약. 음슴체로 작성.",
      "articles": [
        {{
          "title": "기사 제목",
          "link": "기사 링크",
          "date": "날짜"
        }}
      ]
    }}
  ]
}}"""

    res = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": MODEL,
            "max_tokens": 20000,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=180,
    )
    try:
        res.raise_for_status()
    except requests.HTTPError as e:
        print(f"  [API 오류] HTTP {res.status_code}: {res.text}")
        raise RuntimeError(f"API HTTP 오류: {e}") from e
    data = res.json()
    if "error" in data:
        print(f"  [API 오류] {data['error']}")
        raise RuntimeError(f"API 오류: {data['error']['message']}")

    raw = "".join(b.get("text", "") for b in data.get("content", []))
    try:
        s = raw.index("{")
        e = raw.rindex("}") + 1
    except ValueError:
        print(f"  [경고] 배치 {batch_num} 응답에 JSON 없음, 건너뜀")
        print(f"  응답 미리보기: {raw[:200]}")
        return []
    try:
        result = json.loads(raw[s:e])
    except json.JSONDecodeError:
        truncated = raw[s:e].rsplit("},", 1)[0] + "}]}}"
        try:
            result = json.loads(truncated)
        except Exception:
            print(f"  [경고] 배치 {batch_num} JSON 파싱 실패, 건너뜀")
            return []

    deals_out = result.get("deals", [])
    realestate_out = result.get("realestate", [])
    # Claude가 id를 문자열 "null"로 반환하는 경우 None으로 정규화
    for d in deals_out + realestate_out:
        if d.get("id") in ("null", "None", "", "없음"):
            d["id"] = None
        if "articles" not in d:
            d["articles"] = []
    return deals_out, realestate_out


def extract_and_match_deals(articles: list[dict], existing_deals: list[dict]) -> list[dict]:
    print("\n" + "=" * 50)
    print("  2단계: Claude API — 딜 추출 및 매칭")
    print("=" * 50)

    BATCH_SIZE = 60
    batches = [articles[i:i+BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"  총 {len(articles)}건 → {total_batches}개 배치로 처리")

    all_deals = []
    all_realestate = []
    failed_batches = []
    for i, batch in enumerate(batches, 1):
        print(f"  배치 {i}/{total_batches} 처리 중... ({len(batch)}건)")
        try:
            batch_deals, batch_re = call_claude_batch(batch, existing_deals, i, total_batches)
            print(f"  → 딜 {len(batch_deals)}건 추출 / 부동산 {len(batch_re)}건")
            all_deals.extend(batch_deals)
            all_realestate.extend(batch_re)
        except Exception as e:
            print(f"  [오류] 배치 {i} 실패: {e} — 건너뜀")
            failed_batches.append((i, batch))
        if i < total_batches:
            time.sleep(60)  # Rate limit 방지 (Sonnet: 30k TPM, 60초 대기)

    # 실패 배치 1회 재시도
    if failed_batches:
        print(f"\n  [재시도] 실패 배치 {[i for i,_ in failed_batches]}건 재처리 중...")
        time.sleep(60)
        for i, batch in failed_batches:
            print(f"  배치 {i} 재시도 중... ({len(batch)}건)")
            try:
                batch_deals, batch_re = call_claude_batch(batch, existing_deals, i, total_batches)
                print(f"  → 재시도 성공: 딜 {len(batch_deals)}건 / 부동산 {len(batch_re)}건")
                all_deals.extend(batch_deals)
                all_realestate.extend(batch_re)
            except Exception as e:
                print(f"  [경고] 배치 {i} 재시도도 실패, 최종 누락: {e}")

    # 배치 간 중복 딜 병합 (같은 딜명이면 합치기)
    merged = {}
    for deal in all_deals:
        key = deal.get("id") or deal.get("name", "")
        if key in merged:
            existing_links = {a["link"] for a in merged[key].get("articles", [])}
            for art in deal.get("articles", []):
                if art["link"] not in existing_links:
                    merged[key].setdefault("articles", []).append(art)
        else:
            merged[key] = deal

    # 부동산 딜 중복 제거
    re_merged = {}
    for deal in all_realestate:
        key = deal.get("name", "")
        if key not in re_merged:
            re_merged[key] = deal

    result = list(merged.values())
    re_result = list(re_merged.values())
    print(f"\n  최종 딜 {len(result)}건 / 부동산 {len(re_result)}건 (중복 제거 후)")
    return result, re_result


def sort_articles(articles: list[dict]) -> list[dict]:
    def priority(link: str) -> int:
        if not link:
            return 3
        if "thebell.co.kr" in link:
            return 0
        if "dealsite.co.kr" in link:
            return 1
        if "investchosun.com" in link:
            return 2
        return 3
    return sorted(articles, key=lambda a: priority(a.get("link", "")))


def merge_deals(existing_deals: list[dict], new_deals: list[dict]) -> list[dict]:
    """기존 딜에 새 딜 머지. 같은 ID면 업데이트, 신규면 추가."""
    today = datetime.now().strftime("%Y-%m-%d")
    date_prefix = datetime.now().strftime("%Y%m%d")
    merged = {d["id"]: d for d in existing_deals}

    # 신규 딜 ID 카운터 — 기존 ID와 충돌 방지
    existing_serials = []
    for eid in merged:
        if eid.startswith(f"deal_{date_prefix}_"):
            try:
                existing_serials.append(int(eid.split("_")[-1]))
            except ValueError:
                pass
    next_serial = max(existing_serials, default=0) + 1

    for nd in new_deals:
        existing_id = nd.get("id")

        if existing_id and existing_id in merged:
            # 기존 딜 업데이트
            existing = merged[existing_id]
            existing["stage"] = nd.get("stage", existing.get("stage", "기타"))
            existing["summary"] = nd.get("summary", existing.get("summary", ""))
            # 기사 중복 없이 추가
            existing_links = {a["link"] for a in existing.get("articles", [])}
            for art in nd.get("articles", []):
                if art.get("link") and art["link"] not in existing_links:
                    existing.setdefault("articles", []).append(art)
                    existing_links.add(art["link"])
            existing["articles"] = sort_articles(existing.get("articles", []))
            if "history" not in existing:
                existing["history"] = []
            existing["history"].append({
                "date": today,
                "stage": nd.get("stage", "기타"),
                "summary": nd.get("summary", ""),
            })
        else:
            # 신규 딜 추가
            new_id = f"deal_{date_prefix}_{next_serial:03d}"
            next_serial += 1
            nd["id"] = new_id
            nd["createdAt"] = today
            nd["updatedAt"] = today
            nd["articles"] = sort_articles(nd.get("articles", []))
            nd["history"] = [{
                "date": today,
                "stage": nd.get("stage", "기타"),
                "summary": nd.get("summary", ""),
            }]
            nd.pop("isNew", None)
            merged[new_id] = nd

    # 최신 업데이트 순 정렬
    return sorted(merged.values(), key=lambda d: d.get("updatedAt", ""), reverse=True)


# ── 3단계: GitHub Push ─────────────────────────────────

def git_push():
    print("\n" + "=" * 50)
    print("  3단계: GitHub Push")
    print("=" * 50)
    try:
        subprocess.run(["git", "-C", OUTPUT_DIR, "add", "."], check=True)
        status = subprocess.run(
            ["git", "-C", OUTPUT_DIR, "status", "--porcelain"],
            capture_output=True, text=True, check=True
        )
        if not status.stdout.strip():
            print("  변경사항 없음 — push 생략")
            return
        msg = f"briefing: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "-C", OUTPUT_DIR, "commit", "-m", msg], check=True)
        subprocess.run(["git", "-C", OUTPUT_DIR, "push", "origin", "main"], check=True)
        print("  GitHub Push 완료")
    except subprocess.CalledProcessError as e:
        print(f"  [경고] Git push 실패: {e}")


# ── 메인 ───────────────────────────────────────────────

def main():
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "여기에_API_KEY_입력":
        print("[오류] ANTHROPIC_API_KEY를 설정해주세요.")
        return

    # 1) 크롤링
    articles = crawl_all()
    if not articles:
        print("[종료] 수집된 기사가 없습니다.")
        return

    # 2) 기존 딜 로드
    deals_data = load_deals()
    existing_deals = deals_data.get("deals", [])

    # 3) Claude로 딜 추출 및 매칭
    new_deals, new_realestate = extract_and_match_deals(articles, existing_deals)

    # 4) 머지 및 저장
    merged = merge_deals(existing_deals, new_deals)

    # 30일 지난 딜 자동 제거 (scrapped=True 제외)
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    before_cleanup = len(merged)
    merged = [
        d for d in merged
        if d.get("updatedAt", "9999") >= cutoff or d.get("scrapped") == True
    ]
    removed = before_cleanup - len(merged)
    if removed:
        print(f"[정리] 30일 경과 딜 {removed}건 자동 제거")

    deals_data["deals"] = merged

    # 부동산 딜 머지 (기존 + 신규, 이름 기준 중복 제거)
    existing_re = {d["name"]: d for d in deals_data.get("realestate", [])}
    today = datetime.now().strftime("%Y-%m-%d")
    for rd in new_realestate:
        rd.setdefault("updatedAt", today)
        existing_re[rd["name"]] = rd
    deals_data["realestate"] = sorted(existing_re.values(), key=lambda d: d.get("updatedAt", ""), reverse=True)

    save_deals(deals_data)

    # 5) GitHub Push
    git_push()

    print(f"\n{'=' * 50}")
    print(f"  완료! 딜 {len(merged)}건 / 부동산 {len(deals_data['realestate'])}건 아카이빙")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
