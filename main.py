import logging
import os
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import discord
import holidays
from discord.ext import commands, tasks


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("world_clock_holidays_bot")

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("WORLD_CLOCK_CHANNEL_ID", "1485035815024197805"))
INITIAL_MESSAGE_ID = os.getenv("WORLD_CLOCK_MESSAGE_ID")
EMBED_TITLE = "World Clock"
UPDATE_INTERVAL_MINUTES = 30
MESSAGE_SEARCH_LIMIT = 25
HOLIDAY_LIMIT = 2

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set.")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

message_id = int(INITIAL_MESSAGE_ID) if INITIAL_MESSAGE_ID else None

TIMEZONES = {
    "USA": [
        {"city": "New York", "tz": "America/New_York", "country_code": "US"},
        {"city": "Phoenix", "tz": "America/Phoenix", "country_code": "US"},
    ],
    "Europe": [
        {"city": "London", "tz": "Europe/London", "country_code": "GB"},
        {"city": "Paris", "tz": "Europe/Paris", "country_code": "FR"},
    ],
    "Asia": [
        {"city": "Tokyo", "tz": "Asia/Tokyo", "country_code": "JP"},
        {"city": "Dubai", "tz": "Asia/Dubai", "country_code": "AE"},
        {"city": "India", "tz": "Asia/Kolkata", "country_code": "IN"},
    ],
    "Australia": [
        {"city": "Sydney", "tz": "Australia/Sydney", "country_code": "AU"},
    ],
}

REGION_LABELS = {
    "USA": "🇺🇸 USA",
    "Europe": "🇪🇺 Europe",
    "Asia": "🌏 Asia",
    "Australia": "🇦🇺 Australia",
}

COUNTRIES = {
    "US": {
        "name": "United States",
        "flag": "🇺🇸",
        "reference_tz": "America/New_York",
        "aliases": {"united states", "usa", "us", "america", "new york", "phoenix"},
        "featured_terms": {
            "new year's day",
            "memorial day",
            "independence day",
            "labor day",
            "thanksgiving",
            "christmas day",
        },
    },
    "GB": {
        "name": "United Kingdom",
        "flag": "🇬🇧",
        "reference_tz": "Europe/London",
        "aliases": {"united kingdom", "uk", "britain", "england", "london"},
        "featured_terms": {
            "new year's day",
            "good friday",
            "easter monday",
            "bank holiday",
            "christmas day",
            "boxing day",
        },
    },
    "FR": {
        "name": "France",
        "flag": "🇫🇷",
        "reference_tz": "Europe/Paris",
        "aliases": {"france", "paris"},
        "featured_terms": {
            "new year's day",
            "easter monday",
            "labour day",
            "bastille day",
            "assumption",
            "all saints",
            "christmas day",
        },
    },
    "JP": {
        "name": "Japan",
        "flag": "🇯🇵",
        "reference_tz": "Asia/Tokyo",
        "aliases": {"japan", "tokyo"},
        "featured_terms": {
            "new year's day",
            "coming of age",
            "foundation day",
            "showa day",
            "constitution memorial day",
            "marine day",
            "mountain day",
            "culture day",
        },
    },
    "AE": {
        "name": "United Arab Emirates",
        "flag": "🇦🇪",
        "reference_tz": "Asia/Dubai",
        "aliases": {"united arab emirates", "uae", "dubai"},
        "featured_terms": {
            "new year's day",
            "eid al fitr",
            "eid al-fitr",
            "arafat",
            "eid al adha",
            "eid al-adha",
            "islamic new year",
            "national day",
        },
    },
    "IN": {
        "name": "India",
        "flag": "🇮🇳",
        "reference_tz": "Asia/Kolkata",
        "aliases": {"india"},
        "featured_terms": {
            "republic day",
            "holi",
            "independence day",
            "gandhi jayanti",
            "dussehra",
            "diwali",
            "deepavali",
            "christmas day",
        },
    },
    "AU": {
        "name": "Australia",
        "flag": "🇦🇺",
        "reference_tz": "Australia/Sydney",
        "aliases": {"australia", "sydney"},
        "featured_terms": {
            "new year's day",
            "australia day",
            "good friday",
            "anzac day",
            "christmas day",
            "boxing day",
        },
    },
}


def normalize_text(value: str) -> str:
    return " ".join(value.strip().casefold().split())


def format_holiday_name(name: Any) -> str:
    if isinstance(name, (list, tuple, set)):
        name = "; ".join(str(part) for part in name)

    name = str(name)
    cleaned = name.replace(" (Observed)", "").replace(" (observed)", "")
    cleaned = cleaned.replace(" (estimated)", "").replace(" (Estimated)", "")
    return " ".join(cleaned.split())


def iter_locations():
    for region, locations in TIMEZONES.items():
        for location in locations:
            yield region, location


CITY_LOOKUP = {
    normalize_text(location["city"]): location for _, location in iter_locations()
}

COUNTRY_LOOKUP = {}
for country_code, country in COUNTRIES.items():
    COUNTRY_LOOKUP[normalize_text(country["name"])] = country_code
    COUNTRY_LOOKUP[country_code.casefold()] = country_code

    for alias in country["aliases"]:
        COUNTRY_LOOKUP[normalize_text(alias)] = country_code


def match_featured_holiday(name: str, featured_terms: set[str]) -> bool:
    normalized_name = normalize_text(format_holiday_name(name))
    return any(normalize_text(term) in normalized_name for term in featured_terms)


def get_country_today(country_code: str):
    reference_tz = COUNTRIES[country_code]["reference_tz"]
    return datetime.now(ZoneInfo(reference_tz)).date()


def get_upcoming_holidays(country_code: str, limit: int = HOLIDAY_LIMIT):
    country = COUNTRIES[country_code]
    today = get_country_today(country_code)
    years = [today.year, today.year + 1]

    try:
        calendar = holidays.country_holidays(country_code, years=years)
    except Exception:
        logger.exception("Could not load holiday calendar for %s.", country_code)
        return []

    upcoming = []
    fallback = []
    seen_names = set()

    for holiday_date, holiday_name in sorted(calendar.items()):
        if holiday_date < today:
            continue

        cleaned_name = format_holiday_name(holiday_name)
        normalized_name = normalize_text(cleaned_name)

        if normalized_name in seen_names:
            continue

        seen_names.add(normalized_name)
        fallback.append((holiday_date, cleaned_name))

        if match_featured_holiday(cleaned_name, country["featured_terms"]):
            upcoming.append((holiday_date, cleaned_name))

    selected = upcoming[:limit]

    if len(selected) < limit:
        selected_names = {normalize_text(name) for _, name in selected}

        for holiday_date, holiday_name in fallback:
            normalized_name = normalize_text(holiday_name)
            if normalized_name in selected_names:
                continue

            selected.append((holiday_date, holiday_name))
            selected_names.add(normalized_name)

            if len(selected) == limit:
                break

    return selected


def get_today_holidays(country_code: str):
    today = get_country_today(country_code)

    try:
        calendar = holidays.country_holidays(country_code, years=[today.year])
    except Exception:
        logger.exception("Could not load today's holiday calendar for %s.", country_code)
        return []

    holiday_names = calendar.get(today)
    if not holiday_names:
        return []

    if isinstance(holiday_names, str):
        holiday_names = [holiday_names]

    cleaned_names = []
    seen = set()
    for holiday_name in holiday_names:
        cleaned_name = format_holiday_name(holiday_name)
        normalized_name = normalize_text(cleaned_name)
        if normalized_name in seen:
            continue
        seen.add(normalized_name)
        cleaned_names.append(cleaned_name)

    return cleaned_names


def format_holiday_lines(country_code: str, limit: int = HOLIDAY_LIMIT) -> str:
    country_name = COUNTRIES[country_code]["name"]
    upcoming = get_upcoming_holidays(country_code, limit=limit)

    if not upcoming:
        return f"No upcoming holidays found for {country_name}."

    return "\n".join(
        f"{holiday_date.strftime('%b %d')}: {holiday_name}"
        for holiday_date, holiday_name in upcoming
    )


def get_holiday_emoji(holiday_names: list[str]) -> str:
    if not holiday_names:
        return "😴"

    normalized_names = " ".join(normalize_text(name) for name in holiday_names)

    emoji_rules = [
        ("christmas", "🎄"),
        ("new year", "🎉"),
        ("independence", "🎆"),
        ("thanksgiving", "🦃"),
        ("labour", "🛠️"),
        ("labor", "🛠️"),
        ("memorial", "🎖️"),
        ("bank holiday", "🏖️"),
        ("eid", "🌙"),
        ("ramadan", "🌙"),
        ("diwali", "🪔"),
        ("deepavali", "🪔"),
        ("holi", "🎨"),
        ("republic", "🏛️"),
        ("gandhi", "🕊️"),
        ("anzac", "🎖️"),
        ("bastille", "🎆"),
        ("showa", "🎌"),
        ("constitution", "📜"),
        ("foundation", "🏯"),
    ]

    for keyword, emoji in emoji_rules:
        if keyword in normalized_names:
            return emoji

    return "🎉"


def build_today_holiday_lines(region_locations: list[dict[str, str]]) -> list[str]:
    lines = []
    seen_country_codes = set()

    for location in region_locations:
        country_code = location["country_code"]
        if country_code in seen_country_codes:
            continue

        seen_country_codes.add(country_code)
        country = COUNTRIES[country_code]
        today_holidays = get_today_holidays(country_code)
        if not today_holidays:
            continue

        holiday_emoji = get_holiday_emoji(today_holidays)
        holiday_text = " / ".join(today_holidays)
        lines.append(f"{country['flag']} {holiday_emoji} {holiday_text}")

    return lines


def resolve_country_code(query: str):
    normalized_query = normalize_text(query)

    if normalized_query in COUNTRY_LOOKUP:
        return COUNTRY_LOOKUP[normalized_query]

    if normalized_query in CITY_LOOKUP:
        return CITY_LOOKUP[normalized_query]["country_code"]

    return None


def resolve_location(query: str):
    normalized_query = normalize_text(query)

    if normalized_query in CITY_LOOKUP:
        return CITY_LOOKUP[normalized_query]

    for city, location in CITY_LOOKUP.items():
        if normalized_query in city:
            return location

    return None


def build_embed():
    embed = discord.Embed(
        title=EMBED_TITLE,
        description="Live global time updates",
        color=discord.Color.purple(),
    )

    for region, locations in TIMEZONES.items():
        lines = []

        for location in locations:
            now = datetime.now(ZoneInfo(location["tz"]))
            lines.append(f"`{now.strftime('%I:%M %p')}` {location['city']}")

        today_holiday_lines = build_today_holiday_lines(locations)
        if today_holiday_lines:
            lines.append("")
            lines.extend(today_holiday_lines)

        embed.add_field(
            name=REGION_LABELS.get(region, region),
            value="\n".join(lines),
            inline=False,
        )

    return embed


async def get_target_channel():
    channel = bot.get_channel(CHANNEL_ID)
    if channel is None:
        channel = await bot.fetch_channel(CHANNEL_ID)
    return channel


async def find_existing_world_clock_message(channel: Any):
    async for message in channel.history(limit=MESSAGE_SEARCH_LIMIT):
        if message.author != bot.user or not message.embeds:
            continue

        if message.embeds[0].title == EMBED_TITLE:
            return message

    return None


async def sync_world_clock_message():
    global message_id

    channel = await get_target_channel()
    embed = build_embed()

    if message_id:
        try:
            existing_message = await channel.fetch_message(message_id)
            await existing_message.edit(embed=embed)
            logger.info("Updated existing world clock message: %s", message_id)
            return
        except discord.NotFound:
            logger.info("Stored message id %s was not found. Recreating message.", message_id)
            message_id = None

    existing_message = await find_existing_world_clock_message(channel)
    if existing_message:
        message_id = existing_message.id
        await existing_message.edit(embed=embed)
        logger.info("Recovered and updated world clock message: %s", message_id)
        return

    new_message = await channel.send(embed=embed)
    message_id = new_message.id
    logger.info("Posted new world clock message: %s", message_id)


@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user)
    await sync_world_clock_message()

    if not auto_time.is_running():
        auto_time.start()


@tasks.loop(minutes=UPDATE_INTERVAL_MINUTES)
async def auto_time():
    try:
        await sync_world_clock_message()
    except Exception:
        logger.exception("Failed to update the world clock message.")


@auto_time.before_loop
async def before_auto_time():
    await bot.wait_until_ready()


@bot.command(name="time")
async def time_command(ctx, *, location: str):
    location_data = resolve_location(location)

    if not location_data:
        await ctx.send(
            "Location not found. Try one of: New York, Phoenix, London, Paris, Tokyo, Dubai, India, Sydney."
        )
        return

    now = datetime.now(ZoneInfo(location_data["tz"]))
    country_code = location_data["country_code"]
    country_name = COUNTRIES[country_code]["name"]

    embed = discord.Embed(
        title=f"Time in {location_data['city']}",
        color=discord.Color.green(),
    )
    embed.add_field(name="Local Time", value=now.strftime("%I:%M %p"))
    embed.add_field(name="Local Date", value=now.strftime("%b %d, %Y"))
    embed.add_field(
        name=f"Popular Holidays in {country_name}",
        value=format_holiday_lines(country_code, limit=3),
        inline=False,
    )

    await ctx.send(embed=embed)


@bot.command(name="holidays")
async def holidays_command(ctx, *, country_or_city: str):
    country_code = resolve_country_code(country_or_city)

    if not country_code:
        await ctx.send(
            "Country not found. Try a country or city like: USA, London, France, Tokyo, Dubai, India, or Australia."
        )
        return

    country_name = COUNTRIES[country_code]["name"]
    embed = discord.Embed(
        title=f"Popular Holidays in {country_name}",
        description=format_holiday_lines(country_code, limit=5),
        color=discord.Color.gold(),
    )
    embed.timestamp = datetime.now(timezone.utc)
    await ctx.send(embed=embed)


bot.run(TOKEN)
