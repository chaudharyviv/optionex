"""
OPTIONEX — News Client
Fetches India-specific index options news via Tavily API.
Same pattern as COMMODEX — adapted queries for Nifty/BankNifty.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from config import TAVILY_API_KEY, CACHE_NEWS_MIN

logger = logging.getLogger(__name__)

NEWS_QUERIES = {
    "NIFTY": [
        "Nifty 50 market outlook India today",
        "RBI monetary policy interest rate India",
        "FII DII activity NSE options today",
    ],
    "BANKNIFTY": [
        "Bank Nifty outlook India today",
        "Indian banking sector news RBI NPA",
        "FII position Indian bank stocks",
    ],
}

DEFAULT_QUERIES = [
    "NSE options market India today",
    "India stock market news",
]


class NewsClient:
    """Fetches and caches options-relevant news."""

    def __init__(self):
        if not TAVILY_API_KEY:
            logger.warning("TAVILY_API_KEY not set — news unavailable")
            self._client = None
        else:
            from tavily import TavilyClient
            self._client = TavilyClient(api_key=TAVILY_API_KEY)
            logger.info("NewsClient initialised")

    def fetch(self, symbol: str, max_results: int = 5, force_refresh: bool = False) -> dict:
        if not force_refresh:
            cached = self._get_cached(symbol)
            if cached:
                return cached

        if not self._client:
            return self._unavailable(symbol, "Tavily not configured")

        try:
            queries  = NEWS_QUERIES.get(symbol, DEFAULT_QUERIES)
            articles = []
            for query in queries[:2]:
                results = self._client.search(query=query, max_results=3, search_depth="basic")
                for r in results.get("results", []):
                    articles.append({
                        "headline": r.get("title", ""),
                        "snippet":  r.get("content", "")[:300],
                        "source":   r.get("source", ""),
                        "url":      r.get("url", ""),
                    })

            seen = set()
            unique = []
            for a in articles:
                if a["headline"] not in seen:
                    seen.add(a["headline"])
                    unique.append(a)
            articles = unique[:max_results]

            response = {
                "symbol":     symbol,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "from_cache": False,
                "available":  True,
                "articles":   articles,
                "summary":    self._build_summary(symbol, articles),
            }
            self._cache_news(symbol, articles)
            return response
        except Exception as e:
            logger.error(f"News fetch failed for {symbol}: {e}")
            return self._unavailable(symbol, str(e))

    def _build_summary(self, symbol, articles):
        if not articles:
            return f"No recent news for {symbol}."
        lines = [f"Recent news for {symbol} ({len(articles)} items):"]
        for i, a in enumerate(articles, 1):
            lines.append(f"{i}. {a['headline']} — {a['source']}")
            if a["snippet"]:
                lines.append(f"   {a['snippet'][:150]}...")
        return "\n".join(lines)

    def _unavailable(self, symbol, reason):
        return {
            "symbol": symbol, "available": False, "articles": [],
            "summary": f"News unavailable: {reason}",
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "from_cache": False,
        }

    def _get_cached(self, symbol):
        try:
            from core.db import get_connection
            conn   = get_connection()
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(minutes=CACHE_NEWS_MIN)).strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                SELECT headline, snippet, source, url, fetched_at
                FROM news_cache WHERE commodity = ? AND fetched_at > ?
                ORDER BY fetched_at DESC LIMIT 5
            """, (symbol, cutoff))
            rows = cursor.fetchall()
            conn.close()
            if not rows:
                return None
            articles = [{"headline": r["headline"], "snippet": r["snippet"] or "",
                         "source": r["source"] or "", "url": r["url"] or ""} for r in rows]
            return {
                "symbol": symbol, "available": True, "articles": articles,
                "summary": self._build_summary(symbol, articles),
                "fetched_at": rows[0]["fetched_at"], "from_cache": True,
            }
        except Exception:
            return None

    def _cache_news(self, symbol, articles):
        try:
            from core.db import get_connection
            conn   = get_connection()
            cursor = conn.cursor()
            now    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for a in articles:
                cursor.execute("""
                    INSERT INTO news_cache (commodity, headline, snippet, source, url, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (symbol, a.get("headline",""), a.get("snippet",""),
                      a.get("source",""), a.get("url",""), now))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"News cache write failed: {e}")
