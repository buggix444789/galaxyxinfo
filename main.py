# main.py — Hitek Database API
# MADE BY @G4LAXY (BUGGI)

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Hitek Database API", version="3.0.0")

# ═══════════════════════════════════════════════════════════════
#  WATERMARK
# ═══════════════════════════════════════════════════════════════
WATERMARK = "MADE BY @G4LAXY (BUGGI)"

HF_BASE = "https://huggingface.co/datasets/gopallikehack/Hitek_Database_Json/resolve/main"
TOTAL_PARTS = 45
CHUNK_SIZE = 512 * 1024          # 512 KB
MAX_PARALLEL = 6                 # 6 parts ek saath
NO_DETAIL = "No detail found"
ALLOWED_FIELDS = {"name", "email", "phone", "password", "address"}


# ═══════════════════════════════════════════════════════════════
#  Streaming JSON parser (NDJSON + JSON array, chunk-safe)
# ═══════════════════════════════════════════════════════════════
async def _iter_json_objects(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""
    array_mode: Optional[bool] = None
    done = False

    async for chunk in response.aiter_text(CHUNK_SIZE):
        if done:
            break
        buffer += chunk

        if array_mode is None:
            stripped = buffer.lstrip()
            if not stripped:
                continue
            array_mode = stripped[0] == "["
            if array_mode:
                buffer = buffer[buffer.index("[") + 1:]

        while True:
            s = buffer.lstrip()
            offset = len(buffer) - len(s)
            s = s.rstrip()
            if not s:
                buffer = buffer[offset:]
                break
            if s[0] in ",]":
                buffer = buffer[offset + 1:]
                if s[0] == "]":
                    done = True
                continue
            try:
                obj, end = decoder.raw_decode(s)
            except json.JSONDecodeError:
                buffer = buffer[offset:]
                break
            buffer = buffer[offset + end:]
            if isinstance(obj, dict):
                yield obj

    if not done:
        for line in buffer.splitlines():
            line = line.strip().rstrip(",")
            if not line or line[0] in "[],":
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                continue


# ═══════════════════════════════════════════════════════════════
#  Single part search
# ═══════════════════════════════════════════════════════════════
async def _search_part(
    part_num: int,
    field: str,
    query_lc: str,
    results: list[dict[str, Any]],
    limit: int,
    stop: asyncio.Event,
    client: httpx.AsyncClient,
    errors: list[str],
    stats: dict[str, int],
) -> None:
    url = f"{HF_BASE}/part_{str(part_num).zfill(4)}.json"
    try:
        async with client.stream("GET", url, timeout=httpx.Timeout(120.0)) as resp:
            if resp.status_code != 200:
                errors.append(f"part {part_num}: HTTP {resp.status_code}")
                return
            async for obj in _iter_json_objects(resp):
                if stop.is_set():
                    return
                stats["scanned"] += 1
                val = obj.get(field, "")
                if query_lc in str(val).lower():
                    results.append(obj)
                    if len(results) >= limit:
                        stop.set()
                        return
    except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
        errors.append(f"part {part_num}: {type(exc).__name__}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"part {part_num}: {type(exc).__name__}: {exc}")


# ═══════════════════════════════════════════════════════════════
#  Core search
# ═══════════════════════════════════════════════════════════════
async def _run_search(field: str, query: str, limit: int, parts: int) -> JSONResponse:
    if field not in ALLOWED_FIELDS:
        raise HTTPException(400, f"field must be one of {sorted(ALLOWED_FIELDS)}")
    if not query or len(query) < 2:
        raise HTTPException(400, "Query too short — min 2 chars")

    parts = max(1, min(parts, TOTAL_PARTS))
    query_lc = query.lower()
    t0 = time.monotonic()

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    stats = {"scanned": 0}
    stop = asyncio.Event()
    sem = asyncio.Semaphore(MAX_PARALLEL)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "HitekAPI/3.0"},
    ) as client:

        async def bounded(part: int) -> None:
            async with sem:
                if stop.is_set():
                    return
                await _search_part(
                    part, field, query_lc, results, limit,
                    stop, client, errors, stats,
                )

        await asyncio.gather(*(bounded(i) for i in range(parts)))

    elapsed = round(time.monotonic() - t0, 2)

    payload: dict[str, Any] = {
        "watermark": WATERMARK,
        "query": query,
        "field": field,
        "parts_scanned": parts,
        "records_scanned": stats["scanned"],
        "time_sec": elapsed,
        "total_found": len(results),
        "results": results[:limit],
    }

    if not results:
        payload["detail"] = NO_DETAIL
        payload["message"] = f"No record matched {field}={query!r} in parts 0..{parts - 1}."
        if errors:
            payload["errors"] = errors[:5]

    return JSONResponse(payload)


# ═══════════════════════════════════════════════════════════════
#  ENDPOINT 1 — Health / Server Running
# ═══════════════════════════════════════════════════════════════
@app.get("/")
@app.get("/health")
async def health():
    return {
        "watermark": WATERMARK,
        "status": "SERVER RUNNING ✅",
        "api": "Hitek Database Search",
        "version": "3.0.0",
        "endpoints": {
            "health": "/",
            "phone_search": "/search/phone?phone=XXXXXXXXXX",
            "email_search": "/search/email?email=xxx@xxx.com",
            "name_search": "/search/name?name=xxxxx",
            "generic_search": "/search?field=phone&query=XXXXXXXXXX&parts=5",
        },
        "total_parts": TOTAL_PARTS,
        "source": HF_BASE,
    }


# ═══════════════════════════════════════════════════════════════
#  ENDPOINT 2 — Phone Search
# ═══════════════════════════════════════════════════════════════
@app.get("/search/phone")
async def search_phone(
    phone: str = Query(..., description="Phone number to search"),
    parts: int = Query(5, ge=1, le=TOTAL_PARTS, description="Parts to scan (1-45)"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
):
    return await _run_search("phone", phone, limit, parts)


# ═══════════════════════════════════════════════════════════════
#  Extra shortcuts (optional)
# ═══════════════════════════════════════════════════════════════
@app.get("/search/email")
async def search_email(
    email: str = Query(...),
    parts: int = Query(5, ge=1, le=TOTAL_PARTS),
    limit: int = Query(50, ge=1, le=500),
):
    return await _run_search("email", email, limit, parts)


@app.get("/search/name")
async def search_name(
    name: str = Query(...),
    parts: int = Query(5, ge=1, le=TOTAL_PARTS),
    limit: int = Query(50, ge=1, le=500),
):
    return await _run_search("name", name, limit, parts)


@app.get("/search")
async def search(
    field: str = Query(...),
    query: str = Query(...),
    parts: int = Query(5, ge=1, le=TOTAL_PARTS),
    limit: int = Query(50, ge=1, le=500),
):
    return await _run_search(field, query, limit, parts)