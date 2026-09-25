import asyncio
import time
import re
import io
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

if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN not found in environment variables")

APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets/{dataset_id}/items?token={token}"

# ================= TELEGRAM =================
TELEGRAM_BOT_TOKEN = "8495512623:AAF6lpsd0vAAfcbCABre05IJ_-_WAdzItYk"
TELEGRAM_CHAT_ID = "5029478739"

# ================= SETTINGS =================
REQUEST_TIMEOUT = 60
POLL_INTERVAL = 1
MAX_WAIT_TIME = 15

# ================= ⭐ CACHE SETTINGS (MAIN) =================
CACHE_TTL = 300              # ✅ 5 minutes — fresh cache
NOT_FOUND_CACHE_TTL = 300    # 5 min negative cache
STALE_CACHE_TTL = 3600       # 1 hour stale fallback

# ================= RETRY SETTINGS =================
MAX_RETRIES = 3
RETRY_BACKOFF = 2
RETRY_ON_STATUS = {502, 503, 504}

# ================= RATE LIMIT =================
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Instagram Profile API", version="2.3.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ================= CORS — OPEN =================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ================= CACHE & STATE =================
CACHE: Dict[str, dict] = {}
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
LOCK = asyncio.Lock()
IN_FLIGHT: Dict[str, asyncio.Future] = {}

# ================= TELEGRAM =================
async def notify_telegram(message: str):
    STATS["last_alerts"].append({"time": time.time(), "msg": message})
    STATS["last_alerts"] = STATS["last_alerts"][-10:]

    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
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

# ================= ⭐ CACHE HELPERS =================
def get_cache_entry(username: str) -> Optional[dict]:
    """
    Fast sync cache lookup. Returns:
      - dict with 'data' if FRESH  → caller returns immediately
      - dict with 'data' if STALE  → caller can use as fallback
      - None if not found
    Also returns the entry with a 'fresh' boolean flag.
    """
    entry = CACHE.get(username)
    if not entry:
        return None
    now = time.time()
    entry["fresh"] = entry["expiry"] > now
    return entry

async def set_cache(username: str, data: Optional[dict], ttl: int):
    """Store entry with explicit created + expiry timestamps."""
    now = time.time()
    async with LOCK:
        CACHE[username] = {
            "data": data,
            "created": now,
            "expiry": now + ttl,
            "ttl": ttl,
        }

def is_stale_usable(entry: Optional[dict]) -> bool:
    """Stale but still usable as fallback (within STALE_CACHE_TTL)."""
    if not entry or entry.get("data") is None:
        return False
    return (time.time() - entry["created"]) < STALE_CACHE_TTL

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

# ================= MAIN SCRAPE (CACHE-FIRST) =================
@app.get("/scrape/{username}")
@limiter.limit("30/minute")
async def get_user(username: str, request: Request):
    start = time.time()

    if not validate_username(username):
        raise HTTPException(400, "INVALID_USERNAME")

    # ==================================================
    # ⭐ STEP 1: FAST CACHE LOOKUP (no lock, no await)
    # ==================================================
    entry = get_cache_entry(username)

    # ✅ FRESH CACHE HIT → instant return
    if entry and entry["fresh"]:
        STATS["hits"] += 1
        elapsed_ms = (time.time() - start) * 1000
        STATS["cache_response_ms"] += elapsed_ms
        STATS["total_response_ms"] += elapsed_ms

        age = int(time.time() - entry["created"])
        print(f"⚡ CACHE HIT @{username} | age={age}s | took {elapsed_ms:.1f}ms")

        if entry["data"] is None:
            raise HTTPException(404, "PROFILE_NOT_FOUND")
        return entry["data"]

    # If entry exists but expired → mark for stats
    if entry:
        STATS["expired"] += 1
        print(f"⌛ CACHE EXPIRED @{username} | age={int(time.time() - entry['created'])}s")

    # ==================================================
    # ⭐ STEP 2: DEDUPLICATE PARALLEL REQUESTS
    # ==================================================
    async with LOCK:
        if username in IN_FLIGHT:
            future = IN_FLIGHT[username]
            is_owner = False
        else:
            future = asyncio.get_event_loop().create_future()
            IN_FLIGHT[username] = future
            is_owner = True

    if not is_owner:
        # Wait for the in-flight fetch to complete (shared result)
        try:
            result = await future
            elapsed_ms = (time.time() - start) * 1000
            STATS["total_response_ms"] += elapsed_ms
            print(f"🤝 DEDUP HIT @{username} | took {elapsed_ms:.1f}ms")
            return result
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(500, "FETCH_FAILED")

    # ==================================================
    # ⭐ STEP 3: OWNER → FETCH FROM APIFY (with retry)
    # ==================================================
    STATS["misses"] += 1
    apify_start = time.time()

    try:
        raw_profile = await fetch_from_apify_with_retry(username)
    except HTTPException as e:
        # --- 404 → negative cache ---
        if e.status_code == 404:
            await set_cache(username, None, NOT_FOUND_CACHE_TTL)
            future.set_exception(e)
            async with LOCK:
                IN_FLIGHT.pop(username, None)
            raise

        # --- Stale cache fallback ---
        if entry and is_stale_usable(entry):
            await notify_telegram(f"♻️ STALE CACHE SERVED\n@{username}\nReason: {e.detail}")
            future.set_result(entry["data"])
            async with LOCK:
                IN_FLIGHT.pop(username, None)
            return entry["data"]

        future.set_exception(e)
        async with LOCK:
            IN_FLIGHT.pop(username, None)
        raise

    except Exception as e:
        if entry and is_stale_usable(entry):
            await notify_telegram(f"♻️ STALE CACHE SERVED\n@{username}\n{str(e)[:120]}")
            future.set_result(entry["data"])
            async with LOCK:
                IN_FLIGHT.pop(username, None)
            return entry["data"]

        err = HTTPException(500, "FETCH_FAILED")
        future.set_exception(err)
        async with LOCK:
            IN_FLIGHT.pop(username, None)
        raise err

    # ==================================================
    # ⭐ STEP 4: STORE IN CACHE (5 min TTL) + RETURN
    # ==================================================
    formatted = format_profile(raw_profile)
    await set_cache(username, formatted, CACHE_TTL)

    apify_ms = (time.time() - apify_start) * 1000
    STATS["apify_response_ms"] += apify_ms

    total_ms = (time.time() - start) * 1000
    STATS["total_response_ms"] += total_ms

    print(f"🌐 APIFY FETCH @{username} | took {apify_ms:.0f}ms | cached for {CACHE_TTL}s")

    future.set_result(formatted)
    async with LOCK:
        IN_FLIGHT.pop(username, None)

    return formatted

# ================= PROXY IMAGE =================
@app.get("/proxy-image/")
@limiter.limit("50/minute")
async def proxy_image(request: Request, url: str = Query(...)):
    try:
        headers = get_random_headers()
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)

        if resp.status_code == 200:
            return StreamingResponse(
                io.BytesIO(resp.content),
                media_type=resp.headers.get("content-type", "image/jpeg")
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
    return {"status": "healthy", "time": time.time()}

# ================= STATS (with avg latency) =================
@app.get("/stats")
async def stats():
    total = STATS["hits"] + STATS["misses"]
    hit_rate = (STATS["hits"] / total * 100) if total else 0

    avg_cache_ms = (
        STATS["cache_response_ms"] / STATS["hits"]
        if STATS["hits"] else 0
    )
    avg_apify_ms = (
        STATS["apify_response_ms"] / STATS["misses"]
        if STATS["misses"] else 0
    )
    avg_total_ms = (
        STATS["total_response_ms"] / total
        if total else 0
    )

    return {
        "cache": {
            "ttl_seconds": CACHE_TTL,
            "entries": len(CACHE),
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
        "in_flight": len(IN_FLIGHT),
        "last_alerts": STATS["last_alerts"][-5:],
    }

# ================= MANUAL CACHE ENDPOINTS (debug/utility) =================
@app.get("/cache/clear/{username}")
async def clear_cache(username: str):
    """Force-remove a username from cache."""
    async with LOCK:
        removed = CACHE.pop(username, None)
    return {"removed": bool(removed), "username": username}

@app.get("/cache/clear-all")
async def clear_all_cache():
    """Clear entire cache."""
    async with LOCK:
        count = len(CACHE)
        CACHE.clear()
    return {"cleared": count}

@app.get("/cache/list")
async def list_cache():
    """List all cached usernames with age + TTL remaining."""
    now = time.time()
    items = []
    for uname, entry in CACHE.items():
        items.append({
            "username": uname,
            "age_seconds": round(now - entry["created"], 1),
            "ttl_remaining": round(max(0, entry["expiry"] - now), 1),
            "is_fresh": entry["expiry"] > now,
            "has_data": entry["data"] is not None,
        })
    return {"count": len(items), "items": items}