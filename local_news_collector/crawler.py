import asyncio
import os
import requests
import time
from datetime import datetime
from GoogleNews import GoogleNews as OriginalGoogleNews
import dateparser
from newspaper import Article
from bs4 import BeautifulSoup
import random
import httpx
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36"
]

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY","7c134ccdd6ac4ba6aef4d42b639a28c4")  # Set this in your .env

class GoogleNews(OriginalGoogleNews):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": random.choice(USER_AGENTS)
        })
        self.session = sess
        self._session = sess
        self.req = sess

def fetch_newsapi(query, limit=10, api_key=None):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "pageSize": limit,
        "language": "en",
        "sortBy": "publishedAt",
        "apiKey": api_key or NEWSAPI_KEY
    }
    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        print(f"[fetch_newsapi] Error: {r.status_code} {r.text}")
        return []
    articles = []
    for item in r.json().get("articles", []):
        articles.append({
            "source": "newsapi",
            "source_id": item["source"]["name"],
            "id": item["url"],
            "title": item["title"],
            "text": item.get("content", ""),
            "published": item["publishedAt"]
        })
    print(f"[fetch_newsapi] Returning {len(articles)} articles for query '{query}'")
    return articles

async def fetch_news_async(query: str, limit: int = 10, symbol=None):
    # Wrap your sync fetch_news with asyncio.to_thread for compatibility
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fetch_news, query, limit, None, None, symbol)

async def fetch_reddit_async(subreddit: str="CryptoCurrency", limit: int=10, query=None):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
    headers = {"User-Agent": "sentry-news-bot"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
            data = resp.json().get("data", {}).get("children", [])
    except Exception as e:
        print(f"[fetch_reddit_async] Error: {e}")
        return []
    posts = []
    for item in data:
        d = item["data"]
        posts.append({
            "source": "reddit",
            "source_id": f"r/{subreddit}",
            "id": d["id"],
            "title": d["title"],
            "text": d.get("selftext", ""),
            "published": datetime.utcfromtimestamp(d["created_utc"]).isoformat(),
            "query": query
        })
    print(f"[fetch_reddit_async] Returning {len(posts)} posts from r/{subreddit}")
    return posts

def fetch_news(query: str, limit: int = 10, from_date=None, to_date=None, symbol=None):
    print(f"[fetch_news] Searching for news with query '{query}'")
    goog = GoogleNews(lang="en", region="IN")
    try:
        goog.search(query)
        raw = []
        for page in range(1, 2):  # Limit to 1 page
            try:
                goog.getpage(page)
                results = goog.results() or []
                print(f"[fetch_news] Page {page}: {len(results)} results")
                raw.extend(results)
                time.sleep(2)
            except Exception as e:
                print(f"[fetch_news] Error fetching page {page}: {e}")
        articles = []
        for item in raw[:limit]:
            data = {
                "source": "news",
                "symbol": symbol or "UNKNOWN"
            }
            try:
                date = dateparser.parse(item.get("date", "")) or datetime.utcnow()
                link = item["link"].split("&ved=")[0]
                domain = link.split("/")[2].replace("www.", "")
                data.update({
                    "source_id": domain,
                    "id": link,
                    "title": item["title"],
                    "published": date.isoformat(),
                })
                art = Article(link)
                art.download()
                time.sleep(1)
                art.parse()
                data["text"] = art.text.strip()
                data["status"] = "success"
            except Exception as e:
                print(f"[fetch_news] Error parsing article '{item.get('title', '')}': {e}")
                try:
                    r = requests.get(link, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=10)
                    time.sleep(1)
                    soup = BeautifulSoup(r.text, "html.parser")
                    ps = soup.find_all("p")
                    data["text"] = "\n".join(p.text for p in ps[:5])
                    data["status"] = "partial"
                except Exception as e2:
                    print(f"[fetch_news] Fallback failed for '{item.get('title', '')}': {e2}")
                    data["text"] = ""
                    data["status"] = "fail"
            articles.append(data)
        print(f"[fetch_news] Returning {len(articles)} articles for query '{query}'")
        if len(articles) == 0:
            print("[fetch_news] No articles found with Google News, falling back to NewsAPI.org.")
            return fetch_newsapi(query, limit)
        return articles
    except Exception as e:
        # Specifically check for HTTP 429 or other errors
        if "429" in str(e):
            print("[fetch_news] Rate limited by Google News. Falling back to NewsAPI.org.")
        else:
            print(f"[fetch_news] Google News failed: {e}, falling back to NewsAPI.org.")
        return fetch_newsapi(query, limit)


async def crawl_all_async(query: str, limit: int=10, from_date=None, to_date=None):
    print(f"[crawl_all_async] Starting parallel crawl for query: '{query}'")
    # Run both fetches in parallel
    news_task = fetch_news_async(query, limit)
    reddit_task = fetch_reddit_async(limit=limit, query=query)
    news, reddit = await asyncio.gather(news_task, reddit_task)
    print(f"[crawl_all_async] Total: {len(news)} news, {len(reddit)} reddit for '{query}'")
    return news + reddit

if __name__ == "__main__":
    import asyncio
    test_query = "bitcoin"
    results = asyncio.run(crawl_all_async(test_query, limit=3))
    print("=== Results ===")
    for i, article in enumerate(results, 1):
        print(f"\nArticle {i}: {article.get('title', '')[:80]}")
