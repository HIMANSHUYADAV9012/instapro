import asyncio
import time
import re
import io
import json
import random
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = os.getenv("ACTOR_ID")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Upstash Redis (REST)
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN not found in environment variables")
if not ACTOR_ID:
    raise ValueError("ACTOR_ID not found in environment variables")

APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"

# ================= SETTINGS =================
REQUEST_TIMEOUT = 60
POLL_INTERVAL = 1
MAX_WAIT_TIME = 20   # keep < vercel maxDuration

CACHE_TTL = 300
NOT_FOUND_CACHE_TTL = 300
STALE_CACHE_TTL = 3600

MAX_RETRIES = 3
RETRY_BACKOFF = 2
RETRY_ON_STATUS = {502, 503, 504}

# ================= RATE LIMIT (in-memory, best-effort) =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="2.4.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ================= STATE (in-memory, per-instance) =================
STATS = {
    "hits": 0,
    "misses": 0,
    "retries": 0,
    "expired": 0,
    "last_alerts": [],
    "total_response_ms": 0.0,
    "cache_response_ms": 0.0,
    "apify_response_ms": 0.0,
}

_LOCK: Optional[asyncio.Lock] = None
_IN_FLIGHT: Optional[Dict[str, asyncio.Future]] = None


def get_lock() -> asyncio.Lock:
    global _LOCK
    if _LOCK is None:
        _LOCK = asyncio.Lock()
    return _LOCK


def get_inflight() -> Dict[str, asyncio.Future]:
    global _IN_FLIGHT
    if _IN_FLIGHT is None:
        _IN_FLIGHT = {}
    return _IN_FLIGHT


# ================= UPSTASH REDIS HELPERS =================
def _redis_enabled() -> bool:
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


async def _redis_cmd(*args):
    """Run a single Upstash REST command: e.g. _redis_cmd('GET', 'key')"""
    if not _redis_enabled():
        return None
    url = UPSTASH_URL.rstrip("/")
    headers = {"Authorization": f"Bearer {UPSTASH_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.post(url, headers=headers, json=list(args))
            if r.status_code != 200:
                return None
            return r.json().get("result")
    except Exception:
        return None


async def redis_get_json(key: str) -> Optional[dict]:
    raw = await _redis_cmd("GET", key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def redis_set_json(key: str, value: dict, ttl: int):
    await _redis_cmd("SET", key, json.dumps(value), "EX", str(ttl))


async def redis_set_nx(key: str, value: str, ttl: int) -> bool:
    """SET key value NX EX ttl. Returns True if set, False if existed."""
    res = await _redis_cmd("SET", key, value, "NX", "EX", str(ttl))
    return res == "OK"


async def redis_del(key: str):
    await _redis_cmd("DEL", key)


# ================= TELEGRAM =================
async def notify_telegram(message: str):
    STATS["last_alerts"].append({"time": time.time(), "msg": message})
    STATS["last_alerts"] = STATS["last_alerts"][-10:]

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(telegram_url, json=payload)
    except Exception as e:
        print("Telegram send failed:", str(e))


# ================= UTILS =================
def validate_username(username: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9._]{1,30}$", username))


def get_random_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Referer": "https://www.instagram.com/",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
    }


def format_profile(profile: dict) -> dict:
    return {
        "username": profile.get("username"),
        "real_name": profile.get("fullName"),
        "profile_pic": profile.get("profilePicUrl"),
        "followers": profile.get("followersCount"),
        "following": profile.get("followsCount"),
        "post_count": profile.get("postsCount"),
        "bio": profile.get("biography"),
    }


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, HTTPException):
        return exc.status_code in RETRY_ON_STATUS
    if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError)):
        return True
    return False


# ================= CACHE WRAPPERS (Redis + local fallback) =================
_LOCAL_CACHE: Dict[str, dict] = {}


async def cache_get(username: str) -> Optional[dict]:
    entry = None
    if _redis_enabled():
        entry = await redis_get_json(f"ig:{username}")
    if entry is None:
        entry = _LOCAL_CACHE.get(username)
    if not entry:
        return None
    now = time.time()
    entry["fresh"] = entry.get("expiry", 0) > now
    return entry


async def cache_set(username: str, data: Optional[dict], ttl: int):
    now = time.time()
    entry = {
        "data": data,
        "created": now,
        "expiry": now + ttl,
        "ttl": ttl,
    }
    _LOCAL_CACHE[username] = entry
    if _redis_enabled():
        await redis_set_json(f"ig:{username}", entry, ttl)


def is_stale_usable(entry: Optional[dict]) -> bool:
    if not entry or entry.get("data") is None:
        return False
    return (time.time() - entry.get("created", 0)) < STALE_CACHE_TTL


# ================= SINGLE APIFY ATTEMPT =================
async def _apify_attempt(username: str) -> dict:
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        payload = {"usernames": [username]}

        try:
            run_res = await client.post(APIFY_RUN_URL, json=payload)
        except Exception as e:
            raise HTTPException(503, f"APIFY_UNREACHABLE: {str(e)[:120]}")

        if run_res.status_code != 201:
            raise HTTPException(502, f"APIFY_RUN_FAILED: HTTP {run_res.status_code}")

        run_data = run_res.json()
        run_id = run_data["data"]["id"]
        dataset_id = run_data["data"]["defaultDatasetId"]

        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        elapsed = 0

        while elapsed < MAX_WAIT_TIME:
            try:
                status_res = await client.get(status_url)
                status = status_res.json()["data"]["status"]
            except Exception:
                await asyncio.sleep(POLL_INTERVAL)
                elapsed += POLL_INTERVAL
                continue

            if status == "SUCCEEDED":
                break

            if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                raise HTTPException(502, f"APIFY_RUN_FAILED: {status}")

            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        else:
            raise HTTPException(504, "APIFY_TIMEOUT")

        dataset_url = APIFY_DATASET_URL.format(dataset_id=dataset_id, token=APIFY_TOKEN)
        data_res = await client.get(dataset_url)

        if data_res.status_code != 200:
            raise HTTPException(502, "DATASET_FETCH_FAILED")

        items = data_res.json()

        if not items:
            raise HTTPException(404, "PROFILE_NOT_FOUND")

        profile = items[0]

        if profile.get("error") == "not_found":
            raise HTTPException(404, "PROFILE_NOT_FOUND")

        return profile


# ================= FETCH WITH RETRY =================
async def fetch_from_apify_with_retry(username: str) -> dict:
    last_exc: Optional[HTTPException] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await _apify_attempt(username)
        except HTTPException as e:
            last_exc = e
            if not is_retryable(e):
                raise

            STATS["retries"] += 1

            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF ** attempt + random.uniform(0, 1)
                await notify_telegram(
                    f"🔁 RETRY {attempt}/{MAX_RETRIES - 1}\n@{username}\n"
                    f"Reason: {e.detail}\nNext in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
            else:
                await notify_telegram(
                    f"❌ ALL RETRIES FAILED\n@{username}\n"
                    f"Attempts: {MAX_RETRIES}\nLast: {e.detail}"
                )
        except Exception as e:
            last_exc = HTTPException(500, f"UNEXPECTED: {str(e)[:120]}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF ** attempt)
            else:
                await notify_telegram(f"❌ UNEXPECTED FAILURE\n@{username}\n{str(e)[:200]}")

    raise last_exc if last_exc else HTTPException(500, "UNKNOWN_ERROR")


# ================= MAIN SCRAPE =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")
async def get_user(username: str, request: Request):
    start = time.time()

    if not validate_username(username):
        raise HTTPException(400, "INVALID_USERNAME")

    # -------- STEP 1: CACHE LOOKUP --------
    entry = await cache_get(username)

    if entry and entry["fresh"]:
        STATS["hits"] += 1
        elapsed_ms = (time.time() - start) * 1000
        STATS["cache_response_ms"] += elapsed_ms
        STATS["total_response_ms"] += elapsed_ms
        print(f"⚡ CACHE HIT @{username} | took {elapsed_ms:.1f}ms")

        if entry["data"] is None:
            raise HTTPException(404, "PROFILE_NOT_FOUND")
        return entry["data"]

    if entry:
        STATS["expired"] += 1

    # -------- STEP 2: DEDUP via Redis SET NX --------
    lock_key = f"inflight:{username}"
    is_owner = True
    if _redis_enabled():
        is_owner = await redis_set_nx(lock_key, "1", 30)

    if not is_owner:
        # Someone else is fetching — poll cache briefly
        for _ in range(20):
            await asyncio.sleep(0.5)
            entry2 = await cache_get(username)
            if entry2 and entry2["fresh"]:
                STATS["hits"] += 1
                return entry2["data"]
        # Fallback: stale entry if usable
        if entry and is_stale_usable(entry):
            return entry["data"]
        raise HTTPException(503, "IN_FLIGHT")

    # -------- STEP 3: OWNER → FETCH --------
    STATS["misses"] += 1
    apify_start = time.time()

    try:
        try:
            raw_profile = await fetch_from_apify_with_retry(username)
        except HTTPException as e:
            if e.status_code == 404:
                await cache_set(username, None, NOT_FOUND_CACHE_TTL)
                raise

            if entry and is_stale_usable(entry):
                await notify_telegram(f"♻️ STALE CACHE SERVED\n@{username}\nReason: {e.detail}")
                return entry["data"]

            raise

        formatted = format_profile(raw_profile)
        await cache_set(username, formatted, CACHE_TTL)

        apify_ms = (time.time() - apify_start) * 1000
        STATS["apify_response_ms"] += apify_ms
        STATS["total_response_ms"] += (time.time() - start) * 1000

        print(f"🌐 APIFY FETCH @{username} | took {apify_ms:.0f}ms")

        return formatted
    finally:
        if _redis_enabled():
            await redis_del(lock_key)


# ================= PROXY IMAGE =================
@app.get("/proxy-image/")
@limiter.limit("50/minute")
async def proxy_image(request: Request, url: str = Query(...)):
    try:
        headers = get_random_headers()
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)

        if resp.status_code == 200:
            return StreamingResponse(
                io.BytesIO(resp.content),
                media_type=resp.headers.get("content-type", "image/jpeg"),
                headers={
                    "Cache-Control": "public, max-age=3600",
                    "Content-Length": str(len(resp.content)),
                },
            )

        if resp.status_code == 404:
            raise HTTPException(404, "Image not found")

        await notify_telegram(f"⚠ IMAGE FETCH FAILED\n{url}\nHTTP {resp.status_code}")
        raise HTTPException(502, "IMAGE_FETCH_FAILED")

    except HTTPException:
        raise
    except Exception as e:
        await notify_telegram(f"🚨 PROXY IMAGE ERROR\n{url}\n{str(e)}")
        raise HTTPException(502, "IMAGE_FETCH_FAILED")


# ================= HEALTH =================
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "time": time.time(),
        "redis": _redis_enabled(),
    }


# ================= STATS =================
@app.get("/stats")
async def stats():
    total = STATS["hits"] + STATS["misses"]
    hit_rate = (STATS["hits"] / total * 100) if total else 0

    avg_cache_ms = STATS["cache_response_ms"] / STATS["hits"] if STATS["hits"] else 0
    avg_apify_ms = STATS["apify_response_ms"] / STATS["misses"] if STATS["misses"] else 0
    avg_total_ms = STATS["total_response_ms"] / total if total else 0

    return {
        "cache": {
            "ttl_seconds": CACHE_TTL,
            "entries_local": len(_LOCAL_CACHE),
            "redis_enabled": _redis_enabled(),
            "hits": STATS["hits"],
            "misses": STATS["misses"],
            "expired": STATS["expired"],
            "hit_rate_percent": round(hit_rate, 2),
        },
        "latency_ms": {
            "avg_cache_hit": round(avg_cache_ms, 2),
            "avg_apify_fetch": round(avg_apify_ms, 2),
            "avg_total": round(avg_total_ms, 2),
        },
        "retries": STATS["retries"],
        "last_alerts": STATS["last_alerts"][-5:],
    }


# ================= CACHE UTILITY =================
@app.get("/cache/clear/{username}")
async def clear_cache(username: str):
    _LOCAL_CACHE.pop(username, None)
    if _redis_enabled():
        await redis_del(f"ig:{username}")
    return {"cleared": True, "username": username}


@app.get("/cache/clear-all")
async def clear_all_cache():
    _LOCAL_CACHE.clear()
    return {"cleared": True}


@app.get("/cache/list")
async def list_cache():
    now = time.time()
    items = []
    for uname, entry in _LOCAL_CACHE.items():
        items.append({
            "username": uname,
            "age_seconds": round(now - entry["created"], 1),
            "ttl_remaining": round(max(0, entry["expiry"] - now), 1),
            "is_fresh": entry["expiry"] > now,
            "has_data": entry["data"] is not None,
        })
    return {"count": len(items), "items": items}
