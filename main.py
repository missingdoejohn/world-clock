from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import commands, tasks


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("meme_market_watcher_bot")


def read_int_env(*names: str, default: int) -> int:
    for name in names:
        raw = os.getenv(name, "").strip()
        if not raw:
            continue
        try:
            return int(raw)
        except ValueError:
            logger.warning("Invalid integer for %s: %r", name, raw)
    return default


TOKEN = (os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
CHANNEL_ID = read_int_env(
    "MEME_MARKET_CHANNEL_ID",
    "WORLD_CLOCK_CHANNEL_ID",
    "ALERT_CHANNEL_ID",
    default=1485035815024197805,
)
INITIAL_MESSAGE_ID = read_int_env("MEME_MARKET_MESSAGE_ID", default=0) or None
UPDATE_INTERVAL_MINUTES = max(1, min(30, read_int_env("MEME_MARKET_UPDATE_MINUTES", default=1)))
MESSAGE_SEARCH_LIMIT = 25
MAX_HTTP_RETRIES = 3
MAX_ADDRESS_BATCH = 30
MAX_DISPLAY_TOKENS = 5
MIN_DISPLAY_SCORE = 28
ACTIVE_TOKEN_SCORE = 45
HOT_TOKEN_SCORE = 60
EMBED_TITLE = "Global Meme Desk"
LEGACY_EMBED_TITLES = {EMBED_TITLE, "Meme Market Watch", "World Clock"}
REFERENCE_TZ_NAME = os.getenv("DESK_REFERENCE_TZ", "America/Phoenix").strip() or "America/Phoenix"

DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
DEX_TOKENS_BY_ADDRESS_URL = "https://api.dexscreener.com/tokens/v1/solana/{}"
DEX_TOKEN_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_TOKEN_BOOSTS_LATEST_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
DEX_TOKEN_BOOSTS_TOP_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEX_COMMUNITY_TAKEOVERS_URL = "https://api.dexscreener.com/community-takeovers/latest/v1"

DISCOVERY_SEARCH_TERMS = ("solana", "meme", "pump", "bonk", "dog")
SOL_CA_REGEX = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
IGNORE_SYMBOLS = {"SOL", "WSOL", "USDC", "USDT", "BTC", "ETH", "WBTC", "WETH"}
PREFERRED_QUOTES = {"SOL", "USDC", "USDT"}

TIMEZONES = {
    "USA": [
        {"city": "New York", "tz": "America/New_York"},
        {"city": "Phoenix", "tz": "America/Phoenix"},
    ],
    "Europe": [
        {"city": "London", "tz": "Europe/London"},
        {"city": "Paris", "tz": "Europe/Paris"},
    ],
    "Asia": [
        {"city": "Tokyo", "tz": "Asia/Tokyo"},
        {"city": "Dubai", "tz": "Asia/Dubai"},
        {"city": "India", "tz": "Asia/Kolkata"},
    ],
    "Australia": [
        {"city": "Sydney", "tz": "Australia/Sydney"},
    ],
}

REGION_LABELS = {
    "USA": "🇺🇸 USA",
    "Europe": "🇪🇺 Europe",
    "Asia": "🌏 Asia",
    "Australia": "🇦🇺 Australia",
}

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN or DISCORD_BOT_TOKEN is not set.")


intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

http_session: aiohttp.ClientSession | None = None
message_id = INITIAL_MESSAGE_ID


def safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def average(values: Iterable[float]) -> float:
    numbers = [float(value) for value in values]
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def normalize_text(value: str) -> str:
    return " ".join(value.strip().casefold().split())


def normalize_items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("pairs"), list):
            return [item for item in data["pairs"] if isinstance(item, dict)]
        if isinstance(data.get("items"), list):
            return [item for item in data["items"] if isinstance(item, dict)]
        if data:
            return [data]
    return []


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def iter_locations():
    for region, locations in TIMEZONES.items():
        for location in locations:
            yield region, location


CITY_LOOKUP = {
    normalize_text(location["city"]): location for _, location in iter_locations()
}


def is_valid_solana_ca(value: str) -> bool:
    return bool(value and SOL_CA_REGEX.match(value))


def should_ignore_symbol(symbol: str) -> bool:
    return symbol.strip().upper() in IGNORE_SYMBOLS


def merge_sources(existing: Iterable[str], extra: Iterable[str]) -> list[str]:
    return sorted(set(existing) | set(extra))


def with_sources(pair: dict[str, Any], sources: Iterable[str]) -> dict[str, Any]:
    tagged = dict(pair)
    tagged["_sources"] = merge_sources(tagged.get("_sources", []), sources)
    return tagged


def get_token_address_from_item(item: dict[str, Any]) -> str:
    return str(
        item.get("tokenAddress")
        or item.get("address")
        or item.get("mint")
        or (item.get("token") or {}).get("address")
        or (item.get("baseToken") or {}).get("address")
        or ""
    ).strip()


def get_pair_url(token: dict[str, Any]) -> str:
    pair_url = str(token.get("pair_url") or "").strip()
    if pair_url:
        return pair_url

    pair_address = str(token.get("pair_address") or "").strip()
    if pair_address:
        return f"https://dexscreener.com/solana/{pair_address}"

    return ""


def format_usd(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def format_signed_pct(value: float) -> str:
    return f"{value:+.1f}%"


def get_buy_sell_ratio(token: dict[str, Any]) -> float:
    return (token["buys"] + 1) / (token["sells"] + 1)


def get_buys_per_minute(token: dict[str, Any]) -> float:
    return token["buys"] / 5.0


def resolve_location(query: str):
    normalized_query = normalize_text(query)

    if normalized_query in CITY_LOOKUP:
        return CITY_LOOKUP[normalized_query]

    for city, location in CITY_LOOKUP.items():
        if normalized_query in city:
            return location

    return None


def get_city_status_emoji(now: datetime) -> str:
    hour = now.hour

    if now.weekday() >= 5:
        if 8 <= hour < 18:
            return "🟣"
        return "🌙"

    if 8 <= hour < 18:
        return "🟢"
    if 6 <= hour < 8 or 18 <= hour < 22:
        return "🟡"
    return "🌙"


def get_city_session_label(now: datetime) -> str:
    icon = get_city_status_emoji(now)
    return {
        "🟢": "Prime Hours",
        "🟡": "Shoulder Hours",
        "🌙": "Late Hours",
        "🟣": "Weekend Daytime",
    }.get(icon, "Local Time")


def get_day_offset_suffix(now: datetime, reference_now: datetime) -> str:
    day_delta = (now.date() - reference_now.date()).days
    if day_delta > 0:
        return f" (+{day_delta}d)"
    if day_delta < 0:
        return f" ({day_delta}d)"
    return ""


def build_clock_block(locations: list[dict[str, str]], reference_now: datetime) -> str:
    lines = []

    for location in locations:
        now = datetime.now(ZoneInfo(location["tz"]))
        status = get_city_status_emoji(now)
        suffix = get_day_offset_suffix(now, reference_now)
        lines.append(f"{status} `{now.strftime('%I:%M %p')}` {location['city']}{suffix}")

    return "\n".join(lines)


def build_clock_fields(embed: discord.Embed) -> None:
    reference_now = datetime.now(ZoneInfo(REFERENCE_TZ_NAME))

    for region, locations in TIMEZONES.items():
        embed.add_field(
            name=REGION_LABELS.get(region, region),
            value=build_clock_block(locations, reference_now),
            inline=False,
        )


def token_heat_badge(score: int) -> str:
    if score >= 75:
        return "🚨"
    if score >= 60:
        return "🔥"
    if score >= 45:
        return "🟢"
    if score >= 28:
        return "🟡"
    return "❄️"


def embed_color_for_label(label: str) -> discord.Color:
    return {
        "Mania": discord.Color.red(),
        "Hot": discord.Color.orange(),
        "Active": discord.Color.green(),
        "Watching": discord.Color.gold(),
        "Inactive": discord.Color.light_grey(),
    }.get(label, discord.Color.blurple())


async def ensure_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None or http_session.closed:
        timeout = aiohttp.ClientTimeout(total=15)
        http_session = aiohttp.ClientSession(timeout=timeout)
    return http_session


async def fetch_json(url: str, *, params: dict[str, str] | None = None) -> Any:
    session = await ensure_http_session()
    headers = {
        "User-Agent": "Codex Meme Market Watcher",
        "Accept": "application/json",
    }

    last_error: Exception | None = None
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    await asyncio.sleep(float(attempt))
                    continue

                if response.status >= 400:
                    body = (await response.text())[:160]
                    raise RuntimeError(f"HTTP {response.status} for {url}: {body}")

                return await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            last_error = exc
            if attempt >= MAX_HTTP_RETRIES:
                break
            await asyncio.sleep(min(2**attempt, 6))

    raise RuntimeError(f"Request failed for {url}") from last_error


async def fetch_search_pairs(query: str) -> list[dict[str, Any]]:
    payload = await fetch_json(DEX_SEARCH_URL, params={"q": query})
    pairs = payload.get("pairs", []) if isinstance(payload, dict) else []
    return [with_sources(pair, {f"search:{query}"}) for pair in pairs if isinstance(pair, dict)]


def extract_addresses_from_payload(
    source_name: str,
    payload: Any,
    address_sources: defaultdict[str, set[str]],
) -> None:
    for item in normalize_items(payload):
        chain_id = str(item.get("chainId") or item.get("chain") or "").lower()
        if chain_id and chain_id != "solana":
            continue

        token_address = get_token_address_from_item(item)
        if not is_valid_solana_ca(token_address):
            continue

        address_sources[token_address].add(source_name)


async def hydrate_pairs_for_addresses(
    address_sources: defaultdict[str, set[str]],
) -> list[dict[str, Any]]:
    addresses = sorted(address_sources.keys())
    if not addresses:
        return []

    tasks = [
        fetch_json(DEX_TOKENS_BY_ADDRESS_URL.format(",".join(batch)))
        for batch in chunked(addresses, MAX_ADDRESS_BATCH)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    hydrated_pairs: list[dict[str, Any]] = []
    for result in results:
        if isinstance(result, Exception):
            logger.exception("Address hydration failed: %s", result)
            continue

        for pair in normalize_items(result):
            base_address = str(((pair.get("baseToken") or {}).get("address")) or "").strip()
            sources = set(address_sources.get(base_address, set()))
            sources.add("tokens:v1")
            hydrated_pairs.append(with_sources(pair, sources))

    return hydrated_pairs


async def collect_raw_pairs() -> list[dict[str, Any]]:
    raw_pairs: list[dict[str, Any]] = []

    search_results = await asyncio.gather(
        *(fetch_search_pairs(query) for query in DISCOVERY_SEARCH_TERMS),
        return_exceptions=True,
    )
    for query, result in zip(DISCOVERY_SEARCH_TERMS, search_results):
        if isinstance(result, Exception):
            logger.exception("Search source failed for %s: %s", query, result)
            continue
        raw_pairs.extend(result)

    source_specs = (
        ("profiles:latest", DEX_TOKEN_PROFILES_URL),
        ("boosts:latest", DEX_TOKEN_BOOSTS_LATEST_URL),
        ("boosts:top", DEX_TOKEN_BOOSTS_TOP_URL),
        ("takeovers:latest", DEX_COMMUNITY_TAKEOVERS_URL),
    )
    payloads = await asyncio.gather(
        *(fetch_json(url) for _, url in source_specs),
        return_exceptions=True,
    )

    address_sources: defaultdict[str, set[str]] = defaultdict(set)
    for (source_name, _), payload in zip(source_specs, payloads):
        if isinstance(payload, Exception):
            logger.exception("Source failed for %s: %s", source_name, payload)
            continue
        extract_addresses_from_payload(source_name, payload, address_sources)

    raw_pairs.extend(await hydrate_pairs_for_addresses(address_sources))
    return raw_pairs


def parse_pair(pair: dict[str, Any]) -> dict[str, Any] | None:
    chain_id = str(pair.get("chainId") or "").lower()
    if chain_id and chain_id != "solana":
        return None

    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    symbol = str(base.get("symbol") or "").strip()
    ca = str(base.get("address") or "").strip()

    if not symbol or not ca or should_ignore_symbol(symbol) or not is_valid_solana_ca(ca):
        return None

    created_raw = pair.get("pairCreatedAt")
    if created_raw in (None, ""):
        return None

    try:
        created_ms = int(float(created_raw))
    except (TypeError, ValueError):
        return None

    age_minutes = max(0.0, (discord.utils.utcnow().timestamp() * 1000 - created_ms) / 60000)
    volume = pair.get("volume") or {}
    price_change = pair.get("priceChange") or {}
    liquidity = pair.get("liquidity") or {}
    txns_m5 = (pair.get("txns") or {}).get("m5") or {}
    boosts = pair.get("boosts") or {}
    info_obj = pair.get("info") or {}
    socials = info_obj.get("socials") or []

    token = {
        "symbol": symbol.upper(),
        "name": str(base.get("name") or symbol).strip(),
        "ca": ca,
        "pair_address": str(pair.get("pairAddress") or "").strip(),
        "pair_url": str(pair.get("url") or "").strip(),
        "quote_symbol": str(quote.get("symbol") or "").strip().upper(),
        "liq": safe_float(liquidity.get("usd")),
        "vol_m5": safe_float(volume.get("m5")),
        "vol_h1": safe_float(volume.get("h1")),
        "vol_h24": safe_float(volume.get("h24")),
        "change_m5": safe_float(price_change.get("m5")),
        "change_h1": safe_float(price_change.get("h1")),
        "buys": safe_int(txns_m5.get("buys")),
        "sells": safe_int(txns_m5.get("sells")),
        "age": age_minutes,
        "market_cap": safe_float(pair.get("marketCap")),
        "fdv": safe_float(pair.get("fdv")),
        "boosts_active": safe_int(boosts.get("active")),
        "social_count": len([item for item in socials if isinstance(item, dict)]),
        "sources": list(pair.get("_sources") or []),
    }
    token["txns_total"] = token["buys"] + token["sells"]
    token["score"] = compute_activity_score(token)
    return token


def compute_activity_score(token: dict[str, Any]) -> int:
    score = 0.0

    if token["liq"] >= 100_000:
        score += 20
    elif token["liq"] >= 50_000:
        score += 16
    elif token["liq"] >= 15_000:
        score += 10
    elif token["liq"] >= 8_000:
        score += 5
    else:
        score -= 8

    if token["vol_m5"] >= 100_000:
        score += 24
    elif token["vol_m5"] >= 50_000:
        score += 20
    elif token["vol_m5"] >= 20_000:
        score += 16
    elif token["vol_m5"] >= 7_500:
        score += 12
    elif token["vol_m5"] >= 2_500:
        score += 8
    elif token["vol_m5"] >= 1_000:
        score += 4

    buys_per_minute = get_buys_per_minute(token)
    if buys_per_minute >= 20:
        score += 18
    elif buys_per_minute >= 10:
        score += 14
    elif buys_per_minute >= 5:
        score += 10
    elif buys_per_minute >= 2:
        score += 6
    elif buys_per_minute >= 1:
        score += 3

    if token["txns_total"] >= 100:
        score += 16
    elif token["txns_total"] >= 50:
        score += 12
    elif token["txns_total"] >= 18:
        score += 8
    elif token["txns_total"] >= 8:
        score += 4

    buy_ratio = get_buy_sell_ratio(token)
    if token["buys"] >= 25 and buy_ratio >= 1.8:
        score += 12
    elif token["buys"] >= 12 and buy_ratio >= 1.25:
        score += 8
    elif token["buys"] >= 6 and buy_ratio >= 1.0:
        score += 4
    elif token["sells"] > token["buys"] and buy_ratio < 0.8:
        score -= 6

    if token["change_m5"] >= 25:
        score += 12
    elif token["change_m5"] >= 10:
        score += 8
    elif token["change_m5"] >= 4:
        score += 5
    elif token["change_m5"] >= 1:
        score += 3
    elif token["change_m5"] <= -20:
        score -= 12
    elif token["change_m5"] <= -8:
        score -= 6

    if token["quote_symbol"] in PREFERRED_QUOTES:
        score += 2
    if token["boosts_active"] > 0:
        score += min(token["boosts_active"], 3)
    if token["social_count"] >= 2:
        score += 2

    if token["age"] <= 60:
        score += 4
    elif token["age"] <= 360:
        score += 2
    elif token["age"] >= 7 * 24 * 60:
        score -= 2

    if token["market_cap"] and token["market_cap"] < 15_000:
        score -= 6

    return int(clamp(round(score), 0, 100))


def choose_best_token(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return candidate

    candidate["sources"] = merge_sources(current.get("sources", []), candidate.get("sources", []))
    candidate_key = (
        candidate["score"],
        candidate["vol_m5"],
        candidate["liq"],
        candidate["txns_total"],
        candidate["change_m5"],
    )
    current_key = (
        current["score"],
        current["vol_m5"],
        current["liq"],
        current["txns_total"],
        current["change_m5"],
    )
    return candidate if candidate_key > current_key else current


def should_consider_token(token: dict[str, Any]) -> bool:
    if token["liq"] < 8_000:
        return False
    if token["txns_total"] < 6 and token["vol_m5"] < 1_000:
        return False
    if token["change_m5"] <= -35:
        return False
    return True


def build_market_pulse(tokens: list[dict[str, Any]]) -> dict[str, float | int | str]:
    active_tokens = [token for token in tokens if token["score"] >= ACTIVE_TOKEN_SCORE]
    hot_tokens = [token for token in tokens if token["score"] >= HOT_TOKEN_SCORE]

    total_buys = sum(token["buys"] for token in tokens)
    total_sells = sum(token["sells"] for token in tokens)
    total_vol_m5 = sum(token["vol_m5"] for token in tokens)
    avg_score = average(token["score"] for token in tokens)
    avg_change_m5 = average(token["change_m5"] for token in tokens)

    label = "Inactive"
    if len(hot_tokens) >= 3 and total_vol_m5 >= 150_000 and avg_score >= 62:
        label = "Mania"
    elif len(active_tokens) >= 3 and total_vol_m5 >= 60_000 and avg_score >= 52:
        label = "Hot"
    elif len(active_tokens) >= 2 and total_vol_m5 >= 25_000 and avg_score >= 40:
        label = "Active"
    elif len(tokens) >= 2 and total_vol_m5 >= 10_000 and avg_score >= 28:
        label = "Watching"

    return {
        "label": label,
        "active_count": len(active_tokens),
        "hot_count": len(hot_tokens),
        "total_buys": total_buys,
        "total_sells": total_sells,
        "total_vol_m5": total_vol_m5,
        "avg_score": avg_score,
        "avg_change_m5": avg_change_m5,
    }


async def fetch_market_snapshot() -> dict[str, Any]:
    raw_pairs = await collect_raw_pairs()

    best_by_ca: dict[str, dict[str, Any]] = {}
    for pair in raw_pairs:
        token = parse_pair(pair)
        if token is None or not should_consider_token(token):
            continue
        best_by_ca[token["ca"]] = choose_best_token(best_by_ca.get(token["ca"]), token)

    ranked = sorted(
        best_by_ca.values(),
        key=lambda token: (
            token["score"],
            token["vol_m5"],
            token["buys"] - token["sells"],
            token["liq"],
            token["change_m5"],
        ),
        reverse=True,
    )

    leaders = [token for token in ranked if token["score"] >= MIN_DISPLAY_SCORE][:MAX_DISPLAY_TOKENS]
    if not leaders:
        leaders = ranked[: min(3, len(ranked))]

    pulse_base = ranked[: min(8, len(ranked))]
    pulse = build_market_pulse(pulse_base)
    top_gainer = max(ranked, key=lambda token: token["change_m5"], default=None)
    top_flow = max(
        ranked,
        key=lambda token: (
            get_buy_sell_ratio(token),
            token["buys"] - token["sells"],
            token["score"],
        ),
        default=None,
    )
    top_volume = max(
        ranked,
        key=lambda token: (token["vol_m5"], token["score"]),
        default=None,
    )
    return {
        "pulse": pulse,
        "leaders": leaders,
        "scanned": len(ranked),
        "top_gainer": top_gainer,
        "top_flow": top_flow,
        "top_volume": top_volume,
    }


def build_status_block(snapshot: dict[str, Any]) -> str:
    pulse = snapshot["pulse"]
    label = str(pulse["label"])
    badge = {
        "Mania": "🚨",
        "Hot": "🔥",
        "Active": "🟢",
        "Watching": "🟡",
        "Inactive": "❄️",
    }.get(label, "🟣")

    if label == "Inactive":
        return "\n".join(
            [
                f"{badge} {label}",
                "No meme coins are clearing the activity floor right now.",
                f"Scanned {snapshot['scanned']} live candidates.",
            ]
        )

    return "\n".join(
        [
            f"{badge} {label}",
            f"{safe_int(pulse['active_count'])} active | {safe_int(pulse['hot_count'])} hot | scanned {snapshot['scanned']}",
            (
                f"m5 vol {format_usd(safe_float(pulse['total_vol_m5']))}"
                f" | buys/sells {safe_int(pulse['total_buys'])}/{safe_int(pulse['total_sells'])}"
            ),
            f"avg score {round(safe_float(pulse['avg_score']))} | avg m5 {format_signed_pct(safe_float(pulse['avg_change_m5']))}",
        ]
    )


def build_leaders_block(tokens: list[dict[str, Any]]) -> str:
    if not tokens:
        return "No tokens are being tracked right now."

    lines = []
    for index, token in enumerate(tokens, start=1):
        symbol = f"${token['symbol']}"
        pair_url = get_pair_url(token)
        if pair_url:
            symbol = f"[${token['symbol']}]({pair_url})"

        lines.append(
            (
                f"{index}. {token_heat_badge(token['score'])} {symbol} "
                f"{format_signed_pct(token['change_m5'])} | "
                f"m5 {format_usd(token['vol_m5'])} | "
                f"{token['buys']}/{token['sells']}"
            )
        )

    return "\n".join(lines)


def format_token_reference(token: dict[str, Any] | None) -> str:
    if not token:
        return "n/a"

    label = f"${token['symbol']}"
    pair_url = get_pair_url(token)
    if pair_url:
        label = f"[${token['symbol']}]({pair_url})"
    return label


def build_fast_reads_block(snapshot: dict[str, Any]) -> str:
    top_gainer = snapshot.get("top_gainer")
    top_flow = snapshot.get("top_flow")
    top_volume = snapshot.get("top_volume")

    if not any((top_gainer, top_flow, top_volume)):
        return "Waiting for enough live flow to build a read."

    lines = []
    if top_gainer:
        lines.append(
            f"Top gainer: {token_heat_badge(top_gainer['score'])} {format_token_reference(top_gainer)} {format_signed_pct(top_gainer['change_m5'])}"
        )
    if top_flow:
        lines.append(
            f"Strongest flow: {token_heat_badge(top_flow['score'])} {format_token_reference(top_flow)} {get_buy_sell_ratio(top_flow):.2f}x buy/sell"
        )
    if top_volume:
        lines.append(
            f"Volume leader: {token_heat_badge(top_volume['score'])} {format_token_reference(top_volume)} {format_usd(top_volume['vol_m5'])} in m5"
        )

    return "\n".join(lines)


def build_embed(snapshot: dict[str, Any]) -> discord.Embed:
    pulse = snapshot["pulse"]
    embed = discord.Embed(
        title=EMBED_TITLE,
        description="World clock + live Solana meme market pulse\n🟢 prime • 🟡 shoulder • 🌙 late • 🟣 weekend",
        color=embed_color_for_label(str(pulse["label"])),
        timestamp=discord.utils.utcnow(),
    )
    build_clock_fields(embed)
    embed.add_field(name="Market Pulse", value=build_status_block(snapshot), inline=False)
    embed.add_field(name="Heat Board", value=build_leaders_block(snapshot["leaders"]), inline=False)
    embed.add_field(name="Fast Reads", value=build_fast_reads_block(snapshot), inline=False)
    return embed


async def get_target_channel():
    channel = bot.get_channel(CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(CHANNEL_ID)
    return channel


async def find_existing_market_message(channel: Any):
    async for message in channel.history(limit=MESSAGE_SEARCH_LIMIT):
        if message.author != bot.user or not message.embeds:
            continue
        if message.embeds[0].title in LEGACY_EMBED_TITLES:
            return message
    return None


async def refresh_market_message(*, force_post: bool = False) -> dict[str, Any]:
    global message_id

    snapshot = await fetch_market_snapshot()
    embed = build_embed(snapshot)
    channel = await get_target_channel()

    if message_id and not force_post:
        try:
            existing_message = await channel.fetch_message(message_id)
            await existing_message.edit(embed=embed)
            return snapshot
        except discord.NotFound:
            message_id = None

    if not force_post:
        existing_message = await find_existing_market_message(channel)
        if existing_message:
            message_id = existing_message.id
            await existing_message.edit(embed=embed)
            return snapshot

    new_message = await channel.send(embed=embed)
    message_id = new_message.id
    return snapshot


@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user)
    await ensure_http_session()
    try:
        await refresh_market_message()
    except Exception:
        logger.exception("Initial market refresh failed.")

    if not auto_market.is_running():
        auto_market.start()


@tasks.loop(minutes=UPDATE_INTERVAL_MINUTES)
async def auto_market():
    try:
        await refresh_market_message()
    except Exception:
        logger.exception("Market refresh failed.")


@auto_market.before_loop
async def before_auto_market():
    await bot.wait_until_ready()


@bot.command(name="desk", aliases=["market", "meme", "pulse"])
async def market_command(ctx: commands.Context):
    try:
        snapshot = await fetch_market_snapshot()
    except Exception:
        logger.exception("Manual market refresh failed.")
        await ctx.send("Could not fetch meme market data right now.")
        return

    await ctx.send(embed=build_embed(snapshot))


@bot.command(name="time")
async def time_command(ctx: commands.Context, *, location: str):
    location_data = resolve_location(location)

    if not location_data:
        await ctx.send(
            "Location not found. Try one of: New York, Phoenix, London, Paris, Tokyo, Dubai, India, or Sydney."
        )
        return

    now = datetime.now(ZoneInfo(location_data["tz"]))
    embed = discord.Embed(
        title=f"Time in {location_data['city']}",
        description="Quick desk lookup",
        color=discord.Color.green(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="Local Time", value=now.strftime("%I:%M %p"), inline=True)
    embed.add_field(name="Local Date", value=now.strftime("%b %d, %Y"), inline=True)
    embed.add_field(
        name="Session",
        value=f"{get_city_status_emoji(now)} {get_city_session_label(now)}",
        inline=True,
    )

    await ctx.send(embed=embed)


@bot.event
async def on_disconnect():
    logger.warning("Disconnected from Discord.")


@bot.event
async def on_resumed():
    logger.info("Discord session resumed.")


bot.run(TOKEN)
