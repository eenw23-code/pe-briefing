"""
PE Deal Briefing — 딜별 아카이빙 시스템
네이버 뉴스 크롤링 → Claude API 딜 추출/매칭 → deals.json 업데이트 → GitHub Pages 배포
"""

import json
import os
import random
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ── 설정 ──────────────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

OUTPUT_DIR       = os.path.dirname(os.path.abspath(__file__))
DEALS_JSON_PATH  = os.path.join(OUTPUT_DIR, "deals.json")

MAX_PAGES = 10   # 모든 키워드 동일하게 10페이지
BATCH_SIZE = 30  # Claude API 배치 크기

# ── 키워드 ────────────────────────────────────────────────────────────────────
KEYWORDS = ["프라이빗 에쿼티", "사모펀드", "PE"]

# ── 블록리스트 (제목에 포함 시 수집 제외) ─────────────────────────────────────
BLOCKLIST = ["폴리에틸렌", "폴리머", "PVC", "고분자"]

# ── 출처 우선순위 ──────────────────────────────────────────────────────────────
def source_priority(link: str) -> int:
    if not link:
        return 99
    if "thebell.co.kr"    in link: return 0
    if "dealsite.co.kr"   in link: return 1
    if "investchosun.com" in link: return 2
    return 3


# ══════════════════════════════════════════════════════════════════════════════
# 1단계: 크롤링
# ══════════════════════════════════════════════════════════════════════════════

def get_cutoff_days() -> int:
    """월요일 → 4일(금~월), 나머지 평일 → 2일"""
    return 4 if datetime.now().weekday() == 0 else 2


def build_search_url(keyword: str, start: int = 1) -> str:
    now          = datetime.now()
    cutoff_days  = get_cutoff_days()
    date_from    = (now - timedelta(days=cutoff_days - 1)).strftime("%Y.%m.%d")
    date_to      = now.strftime("%Y.%m.%d")
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


def parse_date_text(text: str) -> str:
    text = text.strip()
    now  = datetime.now()
    try:
        digits = "".join(filter(str.isdigit, text))
        if   "분 전"  in text: return (now - timedelta(minutes=int(digits or 0))).strftime("%Y-%m-%d %H:%M")
        elif "시간 전" in text: return (now - timedelta(hours=int(digits or 0))).strftime("%Y-%m-%d %H:%M")
        elif "일 전"  in text: return (now - timedelta(days=int(digits or 0))).strftime("%Y-%m-%d")
        else:                  return text.rstrip(".").replace(".", "-")
    except Exception:
        return now.strftime("%Y-%m-%d")


def is_within_cutoff(date_str: str) -> bool:
    cutoff = datetime.now() - timedelta(days=get_cutoff_days() - 1)
    cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d") >= cutoff
    except ValueError:
        return True  # 날짜 파싱 실패 시 포함


def is_blocked(title: str) -> bool:
    return any(kw in title for kw in BLOCKLIST)


def fetch_page(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"  [오류] 요청 실패: {e}")
        return None


def extract_snippet(item) -> str:
    """네이버 검색결과 미리보기 텍스트 추출 (다중 셀렉터 fallback)"""
    for sel in [
        "div[class*='news_dsc']",
        "a[class*='news_dsc']",
        "div[class*='sds-comps-text-type-ellipsis']",
        "div[class*='dsc_wrap']",
        ".news_dsc",
        "div[class*='desc']",
        "div[class*='summary']",
    ]:
        tag = item.select_one(sel)
        if tag:
            text = tag.get_text(strip=True)
            if text:
                return text[:200]
    return ""


def extract_articles(soup: BeautifulSoup) -> list[dict]:
    articles = []
    # 신 UI → 구 UI 순서로 fallback
    news_items = soup.select("div[class*='sds-comps-vertical-layout']")
    if not news_items:
        news_items = soup.select("div.news_area")

    for item in news_items:
        try:
            # 제목/링크 추출
            title_tag = item.select_one('a[data-heatmap-target=".tit"]') \
                     or item.select_one("a.news_tit")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link  = title_tag.get("href", "")
            if not link:
                continue
            if is_blocked(title):
                continue

            # 날짜 추출
            date_str   = ""
            cur_year   = str(datetime.now().year)
            for st in item.select(".sds-comps-profile-info-subtext"):
                txt = st.get_text(strip=True)
                if txt and any(k in txt for k in ["전", ".", cur_year]):
                    date_str = parse_date_text(txt)
                    break
            if not date_str:
                for info in item.select("span.info"):
                    txt = info.get_text(strip=True)
                    if any(k in txt for k in ["전", ".", "2025", "2026"]):
                        date_str = parse_date_text(txt)
                        break

            # cutoff 필터 (날짜를 못 읽었으면 포함)
            if date_str and not is_within_cutoff(date_str):
                continue

            snippet = extract_snippet(item)
            articles.append({"제목": title, "링크": link, "날짜": date_str, "스니펫": snippet})

        except Exception as e:
            print(f"  [경고] 기사 파싱 오류: {e}")

    return articles


def crawl_keyword(keyword: str) -> list[dict]:
    results = []
    print(f'[검색] "{keyword}" ({MAX_PAGES}p)')
    for page in range(MAX_PAGES):
        start = page * 10 + 1
        soup  = fetch_page(build_search_url(keyword, start))
        if soup is None:
            break
        articles = extract_articles(soup)
        if not articles:
            break
        results.extend(articles)
        print(f"  페이지 {page + 1}: {len(articles)}건")
        if page < MAX_PAGES - 1:
            time.sleep(random.uniform(3.0, 6.0))
    return results


def crawl_all() -> list[dict]:
    print("=" * 55)
    print("  1단계: 네이버 뉴스 크롤링")
    print(f"  키워드 {len(KEYWORDS)}개 × 최대 {MAX_PAGES}p | 범위 {get_cutoff_days()}일")
    print("=" * 55)

    raw = []
    for i, kw in enumerate(KEYWORDS, 1):
        print(f"\n── [{i}/{len(KEYWORDS)}] ──")
        raw.extend(crawl_keyword(kw))
        if i < len(KEYWORDS):
            time.sleep(random.uniform(5.0, 10.0))

    # 링크 기준 중복 제거 (블록리스트는 extract_articles 단계에서 이미 처리)
    seen, unique = set(), []
    for a in raw:
        lnk = a.get("링크", "").strip()
        if lnk and lnk not in seen:
            seen.add(lnk)
            unique.append(a)

    print(f"\n[중복 제거] {len(raw)}건 → {len(unique)}건")
    return unique


# ══════════════════════════════════════════════════════════════════════════════
# 2단계: Claude API — 딜 추출 및 매칭
# ══════════════════════════════════════════════════════════════════════════════

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
    # 기존 파일 백업
    if os.path.exists(DEALS_JSON_PATH):
        try:
            shutil.copy2(DEALS_JSON_PATH, DEALS_JSON_PATH + ".bak")
        except Exception as e:
            print(f"  [경고] 백업 실패: {e}")
    with open(DEALS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[저장] deals.json 완료 (딜 {len(data['deals'])}건)")


def call_claude_batch(
    batch: list[dict],
    existing_deals: list[dict],
    batch_num: int,
    total_batches: int,
) -> tuple[list[dict], list[dict]]:
    """기사 배치를 Claude API로 처리. 항상 (deals, []) tuple 반환."""

    today = datetime.now().strftime("%Y년 %m월 %d일")

    existing_summary = ""
    if existing_deals:
        existing_summary = "\n[기존 딜 목록 (매칭 참고용)]\n"
        for d in existing_deals:
            existing_summary += f"ID:{d['id']} | {d['name']} | 단계:{d['stage']}\n"

    articles_text = "\n".join(
        f"{a['제목']} | {a['링크']} | {a['날짜']}"
        + (f"\n  └ {a['스니펫']}" if a.get("스니펫") else "")
        for a in batch
    )

    prompt = f"""당신은 국내 Private Equity 딜 분석 전문가입니다.
아래는 {today} 기준 수집된 PE/M&A 관련 뉴스입니다. (배치 {batch_num}/{total_batches}, {len(batch)}건)
{existing_summary}

[뉴스 목록]
{articles_text}

[지시사항]
1. 위 뉴스 전체를 빠짐없이 분석하여 PE/M&A 딜을 MECE하게 추출하세요. 딜을 임의로 누락하거나 선별하는 것은 절대 금지입니다.
2. 같은 딜을 다루는 여러 기사는 하나로 묶으세요.
3. 기존 딜 목록과 같은 딜이면 해당 딜 ID를 그대로 사용하세요.
4. 각 딜의 repArticle(대표 기사)은 반드시 1개만 선정하세요. 더벨(thebell.co.kr) → 딜사이트(dealsite.co.kr) → 인베스트조선(investchosun.com) → 기타 순으로 선택. 링크는 반드시 위 뉴스 목록에 실제로 존재하는 값이어야 합니다.
5. 딜 단계는 다음 중 하나로만 분류하세요: "매각 검토중" | "주관사 선정" | "예비입찰" | "본입찰" | "우선협상자 선정" | "실사 진행중" | "SPA 체결" | "딜 클로즈" | "기타"
6. PE/M&A와 무관한 뉴스(단순 실적 발표, 인사, 주가 등)는 완전히 무시하세요.
7. [엄수] 각 딜의 summary, repArticle, articles는 반드시 그 딜을 실제로 다루는 기사에서만 작성하세요. 다른 딜의 기사 내용을 절대 혼용하지 마세요.
8. summary는 2~3문장 음슴체로 작성하세요. 거래 당사자(매도자·매수자·대상기업), 구체적 수치(금액·지분율·IRR 등), 딜 현황/단계, 배경·맥락을 포함하세요.
   좋은 예시:
   - "풍산이 역대 최대 실적을 기록 중인 방산 사업 매각을 추진 중이나, SI의 JV 선호 경향과 FI의 ESG 규제·회수 불확실성 등 복합적 제약 요인으로 거래 성사 셈법이 고차방정식화됨."
   - "KB PE와 SBI인베스트먼트가 에코프로 투자 1년여 만에 교환사채(EB)를 주식으로 전환 후 전량 매도하여 IRR 39.5%의 우수한 회수 실적을 거둠."
   - "E&F PE 컨소시엄이 JC파트너스로부터 KES환경개발 지분 약 80%를 2,000억 원대에 인수하는 SPA를 체결하며 환경 섹터 포트폴리오 강화에 나섬."

반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트 일절 금지:

{{
  "deals": [
    {{
      "id": "기존딜이면 기존ID, 신규면 null",
      "name": "딜명",
      "stage": "딜 단계",
      "summary": "2~3문장 음슴체 요약",
      "repArticle": {{
        "title": "대표 기사 제목",
        "link": "대표 기사 링크 (뉴스 목록에 실제 존재하는 값)",
        "date": "날짜",
        "source": "thebell 또는 dealsite 또는 investchosun 또는 기타매체명"
      }},
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

    try:
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": MODEL,
                "max_tokens": 16000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=180,
        )
        res.raise_for_status()
    except requests.HTTPError as e:
        print(f"  [API 오류] HTTP {res.status_code}: {res.text[:200]}")
        raise RuntimeError(f"API HTTP 오류: {e}") from e

    data = res.json()
    if "error" in data:
        raise RuntimeError(f"API 오류: {data['error']['message']}")

    raw = "".join(b.get("text", "") for b in data.get("content", []))

    # JSON 추출
    try:
        s = raw.index("{")
        e = raw.rindex("}") + 1
        result = json.loads(raw[s:e])
    except (ValueError, json.JSONDecodeError):
        # 응답이 잘린 경우 복구 시도
        try:
            s = raw.index("{")
            partial = raw[s:]
            # 마지막 완전한 deal 객체까지만 잘라내기
            partial = partial.rsplit('{"', 1)[0].rstrip(", \n")
            if not partial.endswith("]"):
                partial += "]}"
            result = json.loads(partial)
        except Exception:
            print(f"  [경고] 배치 {batch_num} JSON 파싱 실패, 건너뜀")
            print(f"  응답 미리보기: {raw[:200]}")
            return [], []

    deals_out = result.get("deals", [])
    # id 정규화
    for d in deals_out:
        if d.get("id") in ("null", "None", "", "없음", None):
            d["id"] = None
        d.setdefault("articles", [])

    return deals_out, []  # realestate 없음


def extract_and_match_deals(
    articles: list[dict],
    existing_deals: list[dict],
) -> list[dict]:
    print("\n" + "=" * 55)
    print("  2단계: Claude API — 딜 추출 및 매칭")
    print("=" * 55)

    batches       = [articles[i:i+BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"  총 {len(articles)}건 → {total_batches}개 배치")

    all_deals     = []
    failed        = []

    for i, batch in enumerate(batches, 1):
        print(f"\n  배치 {i}/{total_batches} ({len(batch)}건) 처리 중...")
        try:
            deals_out, _ = call_claude_batch(batch, existing_deals, i, total_batches)
            print(f"  → 딜 {len(deals_out)}건 추출")
            all_deals.extend(deals_out)
        except Exception as e:
            print(f"  [오류] 배치 {i} 실패: {e} — 재시도 대기열 추가")
            failed.append((i, batch))
        if i < total_batches:
            time.sleep(60)

    # 실패 배치 1회 재시도
    if failed:
        print(f"\n  [재시도] {len(failed)}개 배치 재처리 중...")
        time.sleep(60)
        for i, batch in failed:
            print(f"  배치 {i} 재시도...")
            try:
                deals_out, _ = call_claude_batch(batch, existing_deals, i, total_batches)
                print(f"  → 재시도 성공: 딜 {len(deals_out)}건")
                all_deals.extend(deals_out)
            except Exception as e:
                print(f"  [경고] 배치 {i} 재시도도 실패, 누락: {e}")

    # 배치 간 중복 딜 병합 (같은 id 또는 같은 name)
    merged: dict[str, dict] = {}
    for deal in all_deals:
        key = deal.get("id") or deal.get("name", "")
        if key in merged:
            existing_links = {a["link"] for a in merged[key].get("articles", [])}
            for art in deal.get("articles", []):
                if art.get("link") and art["link"] not in existing_links:
                    merged[key]["articles"].append(art)
                    existing_links.add(art["link"])
        else:
            merged[key] = deal

    result = list(merged.values())
    print(f"\n  최종 딜 {len(result)}건 (중복 제거 후)")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 딜 머지 및 저장
# ══════════════════════════════════════════════════════════════════════════════

def sort_articles(articles: list[dict]) -> list[dict]:
    return sorted(articles, key=lambda a: source_priority(a.get("link", "")))


def merge_deals(existing_deals: list[dict], new_deals: list[dict]) -> list[dict]:
    """기존 딜에 새 딜 머지. 같은 ID면 업데이트, 신규면 추가. 딜 영구 보존."""
    today       = datetime.now().strftime("%Y-%m-%d")
    date_prefix = datetime.now().strftime("%Y%m%d")
    merged      = {d["id"]: d for d in existing_deals}

    # 오늘 날짜 기준 serial 충돌 방지
    existing_serials = []
    for eid in merged:
        if isinstance(eid, str) and eid.startswith(f"deal_{date_prefix}_"):
            try:
                existing_serials.append(int(eid.split("_")[-1]))
            except ValueError:
                pass
    next_serial = max(existing_serials, default=0) + 1

    for nd in new_deals:
        existing_id = nd.get("id")

        if existing_id and existing_id in merged:
            # 기존 딜 업데이트
            ex = merged[existing_id]
            ex["stage"]   = nd.get("stage", ex.get("stage", "기타"))
            ex["summary"] = nd.get("summary", ex.get("summary", ""))
            if nd.get("repArticle"):
                ex["repArticle"] = nd["repArticle"]
            ex["updatedAt"] = today

            existing_links = {a["link"] for a in ex.get("articles", [])}
            for art in nd.get("articles", []):
                if art.get("link") and art["link"] not in existing_links:
                    ex.setdefault("articles", []).append(art)
                    existing_links.add(art["link"])
            ex["articles"] = sort_articles(ex.get("articles", []))

            ex.setdefault("history", []).append({
                "date":    today,
                "stage":   nd.get("stage", "기타"),
                "summary": nd.get("summary", ""),
            })

        else:
            # 신규 딜
            new_id      = f"deal_{date_prefix}_{next_serial:03d}"
            next_serial += 1
            nd["id"]        = new_id
            nd["createdAt"] = today
            nd["updatedAt"] = today
            nd["articles"]  = sort_articles(nd.get("articles", []))
            nd["history"]   = [{
                "date":    today,
                "stage":   nd.get("stage", "기타"),
                "summary": nd.get("summary", ""),
            }]
            merged[new_id] = nd

    return sorted(merged.values(), key=lambda d: d.get("updatedAt", ""), reverse=True)


# ══════════════════════════════════════════════════════════════════════════════
# 3단계: GitHub Push
# ══════════════════════════════════════════════════════════════════════════════

def git_push():
    print("\n" + "=" * 55)
    print("  3단계: GitHub Push")
    print("=" * 55)
    try:
        subprocess.run(["git", "-C", OUTPUT_DIR, "add", "."], check=True)
        status = subprocess.run(
            ["git", "-C", OUTPUT_DIR, "status", "--porcelain"],
            capture_output=True, text=True, check=True,
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


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

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
    deals_data     = load_deals()
    existing_deals = deals_data.get("deals", [])

    # 3) Claude API — 딜 추출 및 매칭
    new_deals = extract_and_match_deals(articles, existing_deals)

    # 4) 머지 및 저장
    merged             = merge_deals(existing_deals, new_deals)
    deals_data["deals"] = merged
    save_deals(deals_data)

    # 5) GitHub Push
    git_push()

    print(f"\n{'=' * 55}")
    print(f"  완료! 딜 {len(merged)}건 아카이빙")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
