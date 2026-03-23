import discord
from discord.ext import commands, tasks
from datetime import datetime
from zoneinfo import ZoneInfo
import traceback
import os

# ===== CONFIG =====
TOKEN =("")  # for Railway
CHANNEL_ID = 1485035815024197805

# ===== BOT SETUP =====
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ===== GLOBAL MESSAGE STORAGE =====
message_id = None

# ===== TIMEZONES =====
TIMEZONES = {
    "🇺🇸 USA": {
        "New York": "America/New_York",
        "Phoenix": "America/Phoenix"
    },
    "🇪🇺 Europe": {
        "London": "Europe/London",
        "Paris": "Europe/Paris"
    },
    "🌏 Asia": {
        "Tokyo": "Asia/Tokyo",
        "Dubai": "Asia/Dubai",
        "India": "Asia/Kolkata"
    },
    "🇦🇺 Australia": {
        "Sydney": "Australia/Sydney"
    }
}

# ===== EVENTS =====
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    
    if not auto_time.is_running():
        auto_time.start()

# ===== EMBED BUILDER =====
def build_embed():
    embed = discord.Embed(
        title="🌍 World Clock",
        description="Live global time updates",
        color=discord.Color.purple()
    )

    for region, cities in TIMEZONES.items():
        block = ""

        for city, tz_name in cities.items():
            tz = ZoneInfo(tz_name)
            now = datetime.now(tz)

            time_str = now.strftime("%I:%M %p")
            block += f"`{time_str}`  {city}\n"

        embed.add_field(name=region, value=block, inline=False)

    embed.set_footer(text="⏰ Updates every 30 minutes")
    embed.timestamp = datetime.utcnow()

    return embed

# ===== AUTO LOOP =====
@tasks.loop(minutes=30)
async def auto_time():
    global message_id

    await bot.wait_until_ready()
    print("⏰ Loop running...")

    try:
        channel = await bot.fetch_channel(CHANNEL_ID)
        embed = build_embed()

        # EDIT EXISTING MESSAGE
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed)
                print("✏️ Message updated")
            except:
                msg = await channel.send(embed=embed)
                message_id = msg.id
                print("♻️ Message recreated")

        # FIRST MESSAGE
        else:
            msg = await channel.send(embed=embed)
            message_id = msg.id
            print("✅ Message sent")

    except Exception:
        print("❌ ERROR:")
        print(traceback.format_exc())

# ===== COMMAND =====
@bot.command()
async def time(ctx, *, location: str):
    location = location.title()

    for region in TIMEZONES.values():
        if location in region:
            tz = ZoneInfo(region[location])
            now = datetime.now(tz)

            embed = discord.Embed(
                title=f"🕒 {location}",
                color=discord.Color.green()
            )

            embed.add_field(name="Time", value=now.strftime("%I:%M %p"))
            embed.add_field(name="Date", value=now.strftime("%b %d, %Y"))

            await ctx.send(embed=embed)
            return

    await ctx.send("❌ Location not found.")

# ===== RUN =====
bot.run(TOKEN)
