import os
import json
import requests
from datetime import datetime
from collections import Counter

# Google News
from GoogleNews import GoogleNews as OriginalGoogleNews
import dateparser
from newspaper import Article
from bs4 import BeautifulSoup
import random

# Twitter (v2)
import tweepy

# Reddit via JSON endpoint

# ─── CONFIG ──────────────────────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN")
INFLUX_ORG    = os.getenv("INFLUX_ORG")
NEWS_API_KEY  = os.getenv("NEWS_API_KEY")
TWITTER_TOKEN= os.getenv("TWITTER_BEARER_TOKEN")
REDDIT_AGENT = os.getenv("REDDIT_USER_AGENT", "sentry_news_bot")


# ─── GOOGLE NEWS ────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36"
]

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

def fetch_news(query: str, limit: int = 10, from_date=None, to_date=None, symbol=None):
    goog = GoogleNews(lang="en", region="IN")
    goog.search(query)
    raw = []
    for page in range(1, 2):  # Limit to 1 page
        goog.getpage(page)
        raw.extend(goog.results() or [])
        time.sleep(2)  # Delay between pages

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
        except Exception:
            try:
                r = requests.get(link, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=10)
                time.sleep(1)
                soup = BeautifulSoup(r.text, "html.parser")
                ps = soup.find_all("p")
                data["text"] = "\n".join(p.text for p in ps[:5])
                data["status"] = "partial"
            except:
                data["text"] = ""
                data["status"] = "fail"
        articles.append(data)
    return articles


# ─── TWITTER ───────────────────────────────────────────────────────────
import time


CACHE_PATH = "twitter_cache.json"
CACHE_TTL = 3600  # 1 hour

def fetch_tweets(query: str, limit: int = 10, from_date=None, to_date=None):
    # Use cache if recent
    if os.path.exists(CACHE_PATH):
        age = time.time() - os.path.getmtime(CACHE_PATH)
        if age < CACHE_TTL:
            print(f"[CACHE] Using cached tweets ({round(age)}s old)")
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    if not TWITTER_TOKEN:
        print("[WARN] Twitter token missing")
        return []

    client = tweepy.Client(bearer_token=TWITTER_TOKEN, wait_on_rate_limit=True)

    start_time = from_date.isoformat() + "Z" if from_date else None
    end_time   = to_date.isoformat() + "Z" if to_date else None

    try:
        tweets = client.search_recent_tweets(
            query=query,
            start_time=start_time,
            end_time=end_time,
            max_results=min(limit, 100),
            tweet_fields=["created_at", "text"]
        )
    except Exception as e:
        print("[ERROR] Twitter API error:", e)
        return []

    output = []
    for t in tweets.data or []:
        output.append({
            "source":    "twitter",
            "id":        str(t.id),
            "text":      t.text,
            "published": t.created_at.isoformat()
        })

    # Write to cache
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return output



# ─── REDDIT ────────────────────────────────────────────────────────────

def fetch_reddit(subreddit: str="CryptoCurrency", limit: int=10):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={limit}"
    headers = {"User-Agent": "sentry-news-bot"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        data = resp.json().get("data", {}).get("children", [])
    except Exception:
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
            "published": datetime.utcfromtimestamp(d["created_utc"]).isoformat()
        })
    return posts


# ─── AGGREGATOR ────────────────────────────────────────────────────────
def crawl_all(query: str, limit: int=10, from_date=None, to_date=None):
    return fetch_news(query, limit, from_date, to_date) + fetch_reddit(limit=limit)
