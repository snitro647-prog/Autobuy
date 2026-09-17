"""
Discord AutoBuy Bot — LTC Payments
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Products organised by CATEGORY
• Restock via .txt file attachment (one item per line)
• MongoDB storage — Railway-ready
• All config via environment variables
"""

import os
import re
import asyncio
import random
import string
from datetime import datetime

import discord
from discord.ext import commands
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()  # local dev only — Railway injects env vars automatically

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  (set these as env vars — locally use a .env file)
# ═══════════════════════════════════════════════════════════════════════════════

DISCORD_TOKEN       = os.environ["DISCORD_TOKEN"]
MONGO_URI           = os.environ["MONGO_URI"]
ADMIN_ROLE_ID        = int(os.environ.get("ADMIN_ROLE_ID", "0"))
LTC_WALLET_ADDRESS   = os.environ.get("LTC_WALLET_ADDRESS", "") or os.environ.get("LTC_WALLET_ADDREDD", "")  # ADDREDD is the typo'd secret name
APIRONE_ACCOUNT    = os.environ.get("APIRONE_ACCOUNT", "")      # e.g. apr-f9e1211f4b52a50bcf3c36819fdc4ad3
LTC_DEV_MODE         = os.environ.get("LTC_DEV_MODE", "false").lower() == "true"
PAYMENT_TIMEOUT_MIN  = int(os.environ.get("PAYMENT_TIMEOUT_MINUTES", "30"))
DB_NAME              = os.environ.get("DB_NAME", "autobuy")
FEE_TOLERANCE_LTC    = float(os.environ.get("LTC_FEE_TOLERANCE", "0.0005"))

ORDER_CATEGORY_ID    = int(os.environ.get("ORDER_CATEGORY_ID", "0"))
APIRONE_TRANSFER_KEY = os.environ.get("APIRONE_TRANSFER_KEY", "")  # from account creation response
# Channel IDs for logging
LOG_CHANNEL_ID      = int(os.environ.get("LOG_CHANNEL_ID", "0"))       # all bot events
# Panel customisation — managed via &shopname / &shopbanner / &shopicon commands
# These are stored in MongoDB (col_settings) so they persist and update live.
# No env vars needed for these.

# ═══════════════════════════════════════════════════════════════════════════════
#  DATABASE — MongoDB via Motor (async)
# ═══════════════════════════════════════════════════════════════════════════════
#
#  COLLECTIONS
#  ───────────
#  categories  →  { slug, name, description, instruction, price_usd, stock: [] }
#  orders      →  { orderId, userId, categorySlug, categoryName, quantity,
#                   totalUSD, ltcAmount, ltcAddress, txId, status, ... }
#
#  slug = stable lowercase-hyphenated key derived from the name
#  e.g. "Netflix 1 Month"  →  "netflix-1-month"
#
# ═══════════════════════════════════════════════════════════════════════════════

mongo        = AsyncIOMotorClient(MONGO_URI)
db           = mongo[DB_NAME]
col_cats     = db["categories"]
col_orders   = db["orders"]
col_settings = db["settings"]   # { key, value } — shop name/banner/icon
col_toc      = db["toc"]         # { slug, message } — per-category terms of conditions
col_blacklist    = db["blacklist"]    # { userId, reason, addedAt }
col_reservations = db["reservations"] # { slug, orderId, qty, expiresAt } — prevent race condition
col_groups       = db["groups"]       # { slug, name, createdAt } — product groups/categories folders


# ── Settings helpers (shop name, banner, icon stored in DB) ───────────────────

SETTING_DEFAULTS = {
    "shop_name":  "AutoBuy Store",
    "banner_url": "",
    "icon_url":   "https://cryptologos.cc/logos/litecoin-ltc-logo.png",
}

# ── Quantity picker / purchase summary helpers ────────────────────────────────

INFO_IMAGE_URL      = "https://i.ibb.co/C3n6RWX5/1000272252-removebg-preview.png"
LOADING_IMAGE_URL   = "https://i.ibb.co/N6bzmCB1/k-Onzy.gif"
CONFIRMED_IMAGE_URL = "https://i.ibb.co/1tZPjgjs/1000272248-removebg-preview.png"


async def send_payment_detected_card(channel, user, ltc_amount, addr_url: str = ""):
    """Components V2 'Payment Detected' card — matches the yellow processing style."""
    if not channel:
        return
    view = discord.ui.LayoutView(timeout=None)
    container = discord.ui.Container(accent_colour=discord.Colour(0xFAA61A))
    container.add_item(discord.ui.Section(
        discord.ui.TextDisplay("## Payment Detected"),
        discord.ui.TextDisplay("**Status:** `Processing`"),
        accessory=discord.ui.Thumbnail(media=LOADING_IMAGE_URL),
    ))
    container.add_item(discord.ui.Separator())
    container.add_item(discord.ui.TextDisplay(
        f"{user.mention if user else 'Buyer'} — we have detected your payment. "
        "Please wait for **1 confirmation** on the blockchain.\n\n"
        f"**Amount:** `{ltc_amount} LTC`\n"
        "Your items will be delivered automatically once confirmed."
    ))
    if addr_url:
        row = discord.ui.ActionRow()
        row.add_item(discord.ui.Button(
            style=discord.ButtonStyle.link, label="View on Blockchain", url=addr_url, emoji="🔗"
        ))
        container.add_item(row)
    view.add_item(container)
    try:
        await channel.send(view=view)
    except Exception as e:
        print(f"[card] Failed to send Payment Detected card: {e}")


def get_bot_emoji(guild: "discord.Guild | None", name: str, fallback: str) -> str:
    """Return the custom `:name:` emoji uploaded via &setupemojis if it exists on the
    guild, otherwise fall back to a plain unicode emoji."""
    if guild:
        emoji = discord.utils.get(guild.emojis, name=name)
        if emoji:
            return str(emoji)
    return fallback


async def send_payment_confirmed_card(channel, user, addr_url: str = ""):
    """Components V2 'Payment Confirmed' card — matches the green confirmed style."""
    if not channel:
        return
    view = discord.ui.LayoutView(timeout=None)
    container = discord.ui.Container(accent_colour=discord.Colour(0x57F287))
    container.add_item(discord.ui.Section(
        discord.ui.TextDisplay("## Payment Confirmed"),
        discord.ui.TextDisplay("**Status:** `Confirmed`"),
        accessory=discord.ui.Thumbnail(media=CONFIRMED_IMAGE_URL),
    ))
    container.add_item(discord.ui.Separator())
    container.add_item(discord.ui.TextDisplay(
        f"{user.mention if user else 'Buyer'} — transaction verified. Processing your items..."
    ))
    if addr_url:
        row = discord.ui.ActionRow()
        row.add_item(discord.ui.Button(
            style=discord.ButtonStyle.link, label="View on Blockchain", url=addr_url, emoji="🔗"
        ))
        container.add_item(row)
    view.add_item(container)
    try:
        await channel.send(view=view)
    except Exception as e:
        print(f"[card] Failed to send Payment Confirmed card: {e}")


def _build_qty_picker_embed(cat: dict, qty: int) -> discord.Embed:
    """Build the Select Quantity embed (Image 1 style)."""
    is_infinite = bool(cat.get("infinite_stock"))
    stock_count = len(cat.get("stock", [])) if not is_infinite else 9999
    stock_str   = "♾️ Unlimited" if is_infinite else str(stock_count)
    total_usd   = round(cat["price_usd"] * qty, 2)
    stats = (
        f"Available Stock: {stock_str}\n"
        f"Selected Qty: {qty}\n"
        f"Total Price: ${total_usd}"
    )
    embed = discord.Embed(
        title="📌  Select Quantity",
        description=(
            "**ℹ️  Order Summary**\n"
            "• Item:\n"
            f"```\n{cat['name']}\n```\n"
            f"```\n{stats}\n```"
        ),
        color=0x5865F2,
    )
    embed.set_thumbnail(url=INFO_IMAGE_URL)
    embed.set_footer(
        text="📌 Tip: Click the ✏️ Pencil icon above to type an exact quantity easily without using the + / − buttons."
    )
    return embed


def _build_purchase_summary_embed(cat: dict, qty: int) -> discord.Embed:
    """Build the Purchase Summary embed (Image 2 style)."""
    is_infinite = bool(cat.get("infinite_stock"))
    stock_count = len(cat.get("stock", [])) if not is_infinite else 9999
    stock_str   = "♾️ Unlimited" if is_infinite else str(stock_count)
    total_usd   = round(cat["price_usd"] * qty, 2)
    stats = (
        f"Available Stock: {stock_str}\n"
        f"Selected Qty: {qty}\n"
        f"Total Price: ${total_usd}"
    )
    embed = discord.Embed(
        title="Purchase Summary",
        description=(
            "**ℹ️  Order Breakdown**\n"
            "• Item:\n"
            f"```\n{cat['name']}\n```\n"
            f"```\n{stats}\n```"
        ),
        color=0x5865F2,
    )
    embed.set_thumbnail(url=INFO_IMAGE_URL)
    return embed


async def get_toc(slug: str) -> str | None:
    """Get ToC message for a category slug. Returns None if not set."""
    doc = await col_toc.find_one({"slug": slug})
    return doc["message"] if doc else None

async def set_toc(slug: str, message: str):
    """Set or update ToC for a category."""
    await col_toc.update_one({"slug": slug}, {"$set": {"message": message}}, upsert=True)

async def clear_toc(slug: str):
    """Remove ToC for a category."""
    await col_toc.delete_one({"slug": slug})


async def get_setting(key: str) -> str:
    doc = await col_settings.find_one({"key": key})
    return doc["value"] if doc else SETTING_DEFAULTS.get(key, "")

async def set_setting(key: str, value: str):
    await col_settings.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)


def slugify(name: str) -> str:
    """'Netflix 1 Month' → 'netflix-1-month'"""
    return "-".join(name.lower().split())


# ── Category helpers ──────────────────────────────────────────────────────────

async def db_get_categories() -> list:
    return await col_cats.find({}, {"_id": 0}).to_list(length=200)

async def db_get_category(slug: str) -> dict | None:
    return await col_cats.find_one({"slug": slug}, {"_id": 0})

async def db_find_category(query: str) -> dict | None:
    """Find by exact slug OR partial name match."""
    cat = await db_get_category(slugify(query))
    if cat:
        return cat
    all_cats = await db_get_categories()
    return next((c for c in all_cats if query.lower() in c["name"].lower()), None)

async def db_create_category(name: str, price: float, description: str = "", instruction: str = "", group: str | None = None) -> dict:
    cat = {
        "slug":            slugify(name),
        "name":            name,
        "description":     description,
        "instruction":     instruction,     # optional post-delivery instructions shown to buyer
        "price_usd":       price,
        "stock":           [],
        "infinite_stock":  None,            # if set, this string is delivered instead of consuming stock
        "custom_ltc_dest": None,            # if set, auto-transfer goes here instead of LTC_WALLET_ADDRESS
        "min_quantity":    1,               # minimum purchase quantity
        "group":           group,           # group slug this product belongs to, or None = Ungrouped
        "createdAt":       datetime.utcnow().isoformat(),
    }
    await col_cats.insert_one(cat)
    return cat

async def db_restock(slug: str, items: list[str]) -> int:
    """Push new items; silently skip duplicates. Returns count added."""
    cat = await col_cats.find_one({"slug": slug})
    if not cat:
        return 0
    existing  = set(cat.get("stock", []))
    new_items = [i for i in items if i not in existing]
    if not new_items:
        return 0
    await col_cats.update_one({"slug": slug}, {"$push": {"stock": {"$each": new_items}}})
    return len(new_items)

async def db_consume_stock(slug: str, quantity: int) -> list | None:
    """
    Atomically pop `quantity` items from stock using findOneAndUpdate.
    If the category has infinite_stock set, returns that string repeated
    `quantity` times without modifying the DB.
    Returns items list or None if insufficient stock.
    """
    cat = await col_cats.find_one({"slug": slug})
    if not cat:
        return None

    # Infinite stock mode — return the same item repeatedly, never consume
    if cat.get("infinite_stock"):
        return [cat["infinite_stock"]] * quantity

    if len(cat.get("stock", [])) < quantity:
        return None
    items = cat["stock"][:quantity]
    # Atomic: only update if those exact items are still there
    result = await col_cats.find_one_and_update(
        {"slug": slug, "stock.0": {"$exists": True}},
        {"$pull": {"stock": {"$in": items}}},
        return_document=True,
    )
    if result is None:
        return None
    # Verify the items were actually removed (not grabbed by someone else)
    for item in items:
        if item in result.get("stock", []):
            return None  # Race condition — another buyer got them
    return items

async def db_delete_category(slug: str) -> bool:
    r = await col_cats.delete_one({"slug": slug})
    return r.deleted_count > 0


# ── Group helpers ─────────────────────────────────────────────────────────────
# Groups are folders that products can be organised into (e.g. "Bot Src",
# "Tools", "Accounts"). A product with group=None is shown as "Ungrouped".

async def db_get_groups() -> list:
    return await col_groups.find({}, {"_id": 0}).sort("name", 1).to_list(length=200)

async def db_get_group(slug: str | None) -> dict | None:
    if not slug:
        return None
    return await col_groups.find_one({"slug": slug}, {"_id": 0})

async def db_find_group(query: str) -> dict | None:
    """Find by exact slug OR partial name match."""
    grp = await db_get_group(slugify(query))
    if grp:
        return grp
    all_groups = await db_get_groups()
    return next((g for g in all_groups if query.lower() in g["name"].lower()), None)

async def db_create_group(name: str) -> dict | None:
    """Returns the created group, or None if the slug already exists (race-safe)."""
    grp = {
        "slug":      slugify(name),
        "name":      name,
        "createdAt": datetime.utcnow().isoformat(),
    }
    try:
        await col_groups.insert_one(grp)
    except DuplicateKeyError:
        return None
    return grp

async def db_rename_group(old_slug: str, new_name: str) -> str | None:
    """Rename a group and keep every product's `group` field pointed at the new slug.
    Returns the new slug, or None if that slug is already taken by another group (race-safe)."""
    new_slug = slugify(new_name)
    try:
        result = await col_groups.update_one({"slug": old_slug}, {"$set": {"name": new_name, "slug": new_slug}})
    except DuplicateKeyError:
        return None
    if result.matched_count == 0:
        return None
    await col_cats.update_many({"group": old_slug}, {"$set": {"group": new_slug}})
    return new_slug

async def db_delete_group(slug: str) -> bool:
    r = await col_groups.delete_one({"slug": slug})
    return r.deleted_count > 0

async def db_categories_in_group(slug: str | None) -> list:
    """Products in a specific group, or Ungrouped products if slug is None."""
    if slug is None:
        query = {"$or": [{"group": None}, {"group": {"$exists": False}}]}
    else:
        query = {"group": slug}
    return await col_cats.find(query, {"_id": 0}).to_list(length=200)


# ── Order helpers ─────────────────────────────────────────────────────────────

def _gen_order_id() -> str:
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"ORD-{int(datetime.utcnow().timestamp())}-{suffix}"

async def db_create_order(user_id, slug, cat_name, quantity, total_usd, ltc_amount, ltc_address) -> dict:
    order = {
        "orderId":        _gen_order_id(),
        "userId":         user_id,
        "categorySlug":   slug,
        "categoryName":   cat_name,
        "quantity":       quantity,
        "totalUSD":       total_usd,
        "ltcAmount":      ltc_amount,
        "ltcAddress":     ltc_address,
        "status":         "pending",
        "txId":           None,
        "createdAt":      datetime.utcnow().isoformat(),
        "paidAt":         None,
        "deliveredItems": [],
    }
    await col_orders.insert_one(order)
    return order

async def db_get_order(order_id: str) -> dict | None:
    return await col_orders.find_one({"orderId": order_id}, {"_id": 0})

async def db_update_order(order_id: str, updates: dict):
    await col_orders.update_one({"orderId": order_id}, {"$set": updates})

async def db_recent_orders(limit: int = 10) -> list:
    return await col_orders.find({}, {"_id": 0}).sort("createdAt", -1).limit(limit).to_list(limit)


# ═══════════════════════════════════════════════════════════════════════════════
#  LTC UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

async def ltc_get_price() -> float:
    """Live LTC/USD price from CoinGecko (free, no key needed)."""
    url = "https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=usd"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                return float((await r.json())["litecoin"]["usd"])
    except Exception:
        return 80.0     # fallback



# ═══════════════════════════════════════════════════════════════════════════════
#  APIRONE — PER-ORDER LTC ADDRESS + BALANCE POLLING
#  No KYC · Low fees · apirone.com
#  Env vars: APIRONE_ACCOUNT, APIRONE_TRANSFER_KEY
# ═══════════════════════════════════════════════════════════════════════════════

APIRONE_BASE = "https://apirone.com/api/v2"


async def apirone_generate_address(order_id: str) -> str | None:
    """
    Generate a unique LTC deposit address for this order via Apirone.
    Returns the address string or None on failure.
    """
    if LTC_DEV_MODE:
        print(f"[apirone] DEV MODE — using static wallet address")
        return LTC_WALLET_ADDRESS

    if not APIRONE_ACCOUNT:
        print("[apirone] ❌ APIRONE_ACCOUNT not set!")
        return None

    url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/addresses"
    payload = {"currency": "ltc"}

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                text = await r.text()
                print(f"[apirone] generate-address {r.status}: {text[:300]}")
                if r.status != 200:
                    print(f"[apirone] ❌ HTTP {r.status}")
                    return None
                data = await r.json()
                addr = data.get("address", "")
                if addr:
                    print(f"[apirone] ✅ Address generated: {addr}")
                    return addr
                print(f"[apirone] ❌ No address in response: {data}")
                return None
    except Exception as e:
        print(f"[apirone] ❌ Exception: {e}")
        return None


async def apirone_get_address_balance(address: str) -> dict:
    """
    Get current balance of an Apirone LTC address.
    Returns dict with 'available' and 'total' in litoshis (1 LTC = 1e8 litoshis).
    Returns empty dict on error.
    """
    if not APIRONE_ACCOUNT:
        return {}
    url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/addresses/{address}/balance"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    print(f"[apirone] balance HTTP {r.status}")
                    return {}
                data = await r.json()
                # Address balance endpoint returns: {"account":..,"currency":"ltc","address":..,"available":N,"total":N}
                return {
                    "available": data.get("available", 0),
                    "total":     data.get("total", 0),
                }
    except Exception as e:
        print(f"[apirone] balance check error: {e}")
        return {}


async def apirone_get_address_history(address: str) -> list:
    """
    Get transaction history for an Apirone LTC address.
    Returns list of tx dicts with 'amount', 'is_confirmed', 'txid'.
    """
    if not APIRONE_ACCOUNT:
        return []
    url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/addresses/{address}/history"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params={"limit": 5}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                return data.get("txs", [])
    except Exception as e:
        print(f"[apirone] history error: {e}")
        return []


async def _create_invoice_or_fail(order_id: str, ltc_amount: float, total_usd: float, channel, user) -> dict | None:
    """
    Generate a unique Apirone LTC address for this order.
    If it fails, cancel the order and post an error in the channel.
    Returns dict with pay_address and pay_amount, or None on failure.
    """
    addr = await apirone_generate_address(order_id)
    if addr:
        return {"pay_address": addr, "pay_amount": ltc_amount}

    await db_update_order(order_id, {"status": "cancelled"})
    ping = ""
    if ADMIN_ROLE_ID and channel:
        role = channel.guild.get_role(ADMIN_ROLE_ID)
        ping = role.mention if role else ""
    if channel:
        await channel.send(
            content=ping or None,
            embed=discord.Embed(
                title="❌  Payment System Error",
                description=(
                    "Could not generate a deposit address.\n\n"
                    "**Possible causes:**\n"
                    "• `APIRONE_ACCOUNT` env var not set or wrong\n"
                    "• Apirone API temporarily down\n\n"
                    "Check Railway logs for `[apirone]` lines."
                ),
                color=0xE74C3C,
            )
        )
    return None


async def process_payment_confirmed(order_id: str, actually_paid_litoshis: int = 0):
    """
    Called when Apirone confirms payment received for an order.
    Delivers stock, sends DMs, closes channel, updates panel.
    """
    order = await db_get_order(order_id)
    if not order:
        print(f"[apirone] process_payment: order {order_id} not found")
        return
    if order["status"] == "delivered":
        print(f"[apirone] process_payment: {order_id} already delivered")
        return

    # Consume stock
    items = await db_consume_stock(order["categorySlug"], order["quantity"])
    if not items:
        await db_update_order(order_id, {"status": "error"})
        await send_log(discord.Embed(
            title="⚠️  Stock Depleted on Delivery",
            description=f"Order `{order_id}` paid but no stock left for `{order['categoryName']}`.",
            color=0xE74C3C,
        ))
        guild = bot.guilds[0] if bot.guilds else None
        if guild:
            ch_id = order.get("channelId")
            ch    = guild.get_channel(int(ch_id)) if ch_id else None
            user  = guild.get_member(int(order["userId"])) if guild else None
            if ch:
                ping = ""
                if ADMIN_ROLE_ID:
                    role = guild.get_role(ADMIN_ROLE_ID)
                    ping = role.mention if role else ""
                await ch.send(
                    content=ping or None,
                    embed=discord.Embed(
                        title="⚠️  Payment Received — Stock Issue",
                        description=(
                            f"{user.mention if user else 'Buyer'} — Payment confirmed ✅ but stock is unavailable.\n\n"
                            f"An admin will manually deliver your item or issue a refund.\n"
                            f"**Order:** `{order_id}`"
                        ),
                        color=0xA855F7,
                    )
                )
        return

    actually_paid_ltc = actually_paid_litoshis / 1e8 if actually_paid_litoshis else order["ltcAmount"]

    await db_update_order(order_id, {
        "status":         "delivered",
        "paidAt":         datetime.utcnow().isoformat(),
        "deliveredItems": items,
        "actuallyPaid":   actually_paid_ltc,
    })

    cat              = await db_get_category(order["categorySlug"])
    instruction_text = f"\n\n📌 **Instructions:**\n{cat['instruction']}" if cat and cat.get("instruction") else ""
    delivery_lines   = "\n".join(f"**{i+1}.** `{item}`" for i, item in enumerate(items))

    guild = bot.guilds[0] if bot.guilds else None
    if not guild:
        return

    try:
        user = guild.get_member(int(order["userId"])) or await bot.fetch_user(int(order["userId"]))
    except Exception:
        user = None

    channel_id = order.get("channelId")
    channel    = guild.get_channel(int(channel_id)) if channel_id else None

    # ── Channel: delivery confirmation + .ordercomplete message ──────────────
    addr      = order.get("ltcAddress", "")
    addr_url  = f"https://blockchair.com/litecoin/address/{addr}" if addr else ""
    delivery_embed = discord.Embed(
        title="✅  Order Delivered",
        description=(
            f"{user.mention if user else 'Buyer'} — Payment confirmed. Your items have been sent to your DMs."
            + (f"\n\n🔗 [View on Blockchair]({addr_url})" if addr_url else "")
        ),
        color=0x7B2FBE,
    )
    delivery_embed.set_footer(text=f"Order: {order_id}")
    if channel:
        await channel.send(embed=delivery_embed)
        # Send .ordercomplete message in the ticket channel
        buyer_mention = user.mention if user else f"<@{order['userId']}>"
        qty = order['quantity']
        product_field = f"[{order['categoryName']} ({qty})]" if qty > 1 else f"[{order['categoryName']}]"
        await channel.send(
            f".ordercomplete {product_field} [{order['totalUSD']}] {buyer_mention}"
        )
        try:
            await channel.edit(topic=f"Order: {order_id} | Status: delivered | Product: {order['categoryName']}")
        except Exception:
            pass

        # ── Order Finalized card (additional — does not replace .ordercomplete) ──
        tx_id = ""
        try:
            history = await apirone_get_address_history(addr) if addr else []
            confirmed_tx = next((t for t in history if t.get("is_confirmed")), None)
            tx_id = (confirmed_tx or (history[0] if history else {})).get("txid", "") or ""
        except Exception as e:
            print(f"[order-finalized] Could not fetch tx id: {e}")
        await send_order_finalized_card(channel, user, order_id, tx_id)

    # ── DM: items (as .txt file if >4, else embed) ────────────────────────────
    if user:
        if len(items) > 4:
            import io as _io
            txt_content = "\n".join(items)
            if cat and cat.get("instruction"):
                txt_content += f"\n\n--- Instructions ---\n{cat['instruction']}"
            txt_file = discord.File(
                fp=_io.BytesIO(txt_content.encode("utf-8")),
                filename=f"order-{order_id}.txt",
            )
            dm_embed = discord.Embed(
                title="🔑  Your Order Items",
                description=(
                    f"**Product:** {order['categoryName']}\n"
                    f"**Quantity:** {order['quantity']}\n"
                    f"**Order ID:** `{order_id}`\n\n"
                    f"Your items are in the attached `.txt` file (one per line).{instruction_text}"
                ),
                color=0x7B2FBE,
            )
            dm_embed.set_footer(text=f"Order: {order_id}")
            try:
                await user.send(embed=dm_embed, file=txt_file)
            except Exception:
                if channel:
                    import io as _io2
                    txt_file2 = discord.File(
                        fp=_io2.BytesIO(txt_content.encode("utf-8")),
                        filename=f"order-{order_id}.txt",
                    )
                    await channel.send(
                        content=user.mention,
                        embed=discord.Embed(
                            title="⚠️  Could Not Send DM — Items Posted Here",
                            description="Your items are attached below.",
                            color=0xA855F7,
                        ),
                        file=txt_file2,
                    )
        else:
            try:
                dm = discord.Embed(
                    title="🔑  Your Order Items",
                    description=(
                        f"**Product:** {order['categoryName']}\n"
                        f"**Quantity:** {order['quantity']}\n"
                        f"**Order ID:** `{order_id}`\n\n"
                        f"**Your Items:**\n{delivery_lines}{instruction_text}"
                    ),
                    color=0x7B2FBE,
                )
                dm.set_footer(text=f"Order: {order_id}")
                await user.send(embed=dm)
            except Exception:
                if channel:
                    await channel.send(
                        content=user.mention,
                        embed=discord.Embed(
                            title="⚠️  Could Not Send DM — Items Posted Here",
                            description=delivery_lines + instruction_text,
                            color=0xA855F7,
                        )
                    )

    # ── (Sales channel embed removed — .ordercomplete sent in ticket instead) ─

    # ── Admin log ─────────────────────────────────────────────────────────────
    admin_ping = ""
    if ADMIN_ROLE_ID:
        role = guild.get_role(ADMIN_ROLE_ID)
        admin_ping = role.mention if role else ""
    sale_embed = discord.Embed(
        title="🛒  Product Sold",
        description=(
            f"**Product:** {order['categoryName']}\n"
            f"**Qty:** {order['quantity']}\n"
            f"**Amount:** {actually_paid_ltc} LTC (${order['totalUSD']:.2f})\n"
            f"**Buyer:** {user.mention if user else order['userId']}\n"
            f"**Order:** `{order_id}`\n"
            f"**Address:** `{order.get('ltcAddress', 'N/A')}`"
        ),
        color=0x9B59B6,
    )
    sale_embed.set_footer(text=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    await send_to_channel(
        LOG_CHANNEL_ID,
        content=f"{admin_ping} 🛒 New sale!" if admin_ping else None,
        embed=sale_embed,
    )

    # ── Close channel + update panel ──────────────────────────────────────────
    if channel and user:
        asyncio.create_task(close_order_channel(channel, buyer=user))
    asyncio.create_task(auto_update_panel())

    # ── Auto-transfer: after 15s send ALL LTC in account to master wallet ─────
    asyncio.create_task(_auto_transfer_to_master(order_id, channel))


async def _auto_transfer_to_master(order_id: str, ticket_channel=None):
    """
    After delivery, wait 15 seconds then transfer the ENTIRE Apirone account
    LTC balance to the category's custom_ltc_dest if set, otherwise to
    LTC_WALLET_ADDRESS (master wallet set in env).
    ticket_channel: the order's ticket channel — receives a sweep confirmation with the real sweep txid.
    """
    if not APIRONE_ACCOUNT or not APIRONE_TRANSFER_KEY:
        print(f"[auto-transfer] Skipping — missing APIRONE_ACCOUNT or APIRONE_TRANSFER_KEY")
        return

    # Determine destination: per-category override or global wallet
    order    = await db_get_order(order_id)
    cat      = await db_get_category(order["categorySlug"]) if order else None
    dest_addr = (cat.get("custom_ltc_dest") if cat else None) or LTC_WALLET_ADDRESS

    if not dest_addr:
        print(f"[auto-transfer] Skipping — no destination wallet set")
        return

    await asyncio.sleep(15)   # wait 15 seconds for confirmations to settle

    # Get current available balance
    try:
        url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/balance?currency=ltc"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                bal  = next((b for b in data.get("balance", []) if b.get("currency") == "ltc"), {})
                avail_litoshis = bal.get("available", 0)
    except Exception as e:
        print(f"[auto-transfer] ❌ Could not fetch balance: {e}")
        return

    if avail_litoshis <= 0:
        print(f"[auto-transfer] No available balance to transfer for order {order_id}")
        return

    avail_ltc = avail_litoshis / 1e8
    print(f"[auto-transfer] Transferring {avail_ltc} LTC → {dest_addr}")

    try:
        transfer_url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/transfer"
        payload = {
            "currency":              "ltc",
            "transfer-key":          APIRONE_TRANSFER_KEY,
            "destinations":          [{"address": dest_addr, "amount": "100%"}],
            "fee":                   "normal",
            "subtract-fee-from-amount": True,
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(transfer_url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                text = await r.text()
                print(f"[auto-transfer] Response {r.status}: {text[:400]}")
                if r.status == 200:
                    import json as _j
                    d    = _j.loads(text)
                    txs  = d.get("txs", [])
                    txid = txs[0] if txs else "N/A"
                    print(f"[auto-transfer] ✅ Sent! txid={txid} amount={avail_ltc} LTC")
                    await send_log(discord.Embed(
                        title="💸  Auto-Transfer Sent",
                        description=(
                            f"**Order:** `{order_id}`\n"
                            f"**Amount:** {avail_ltc} LTC (entire balance)\n"
                            f"**To:** `{dest_addr}`\n"
                            f"**TX:** `{txid}`"
                        ),
                        color=0x7B2FBE,
                    ))
                    # Post sweep txid in the ticket channel
                    if ticket_channel:
                        try:
                            await ticket_channel.send(embed=discord.Embed(
                                title="🧹  Sweep Successful!",
                                description=(
                                    f"**Amount:** `{avail_ltc} LTC`\n"
                                    f"**To:** `{dest_addr}`\n"
                                    f"**Sweep TX:**\n```\n{txid}\n```"
                                ),
                                color=0x7B2FBE,
                            ))
                        except Exception:
                            pass
                else:
                    import json as _j
                    try:
                        err = _j.loads(text)
                        msg = err.get("message") or err.get("error") or text[:200]
                    except Exception:
                        msg = text[:200]
                    print(f"[auto-transfer] ❌ Failed: {msg}")
                    await send_log(discord.Embed(
                        title="❌  Auto-Transfer Failed",
                        description=(
                            f"**Order:** `{order_id}`\n"
                            f"**Amount:** {avail_ltc} LTC\n"
                            f"**Error:** {msg}"
                        ),
                        color=0xE74C3C,
                    ))
    except Exception as e:
        print(f"[auto-transfer] ❌ Exception: {e}")
        import traceback; traceback.print_exc()


async def poll_apirone_payment(order_id: str, channel, user):
    """
    Poll the Apirone address balance every 30 seconds.
    Delivers order as soon as total balance >= expected amount.
    Stops after PAYMENT_TIMEOUT_MIN minutes.
    """
    payment_window   = PAYMENT_TIMEOUT_MIN * 60
    grace_period     = 10 * 60   # 10 min extra grace after timeout
    extended_timeout = payment_window + grace_period
    elapsed          = 0
    interval         = 30
    detected_notified = False
    timed_out_notified = False

    print(f"[apirone-poll] Starting for order {order_id}")

    while elapsed < extended_timeout:
        await asyncio.sleep(interval)
        elapsed += interval

        order = await db_get_order(order_id)
        if not order:
            break
        if order["status"] in ("delivered", "cancelled", "error"):
            print(f"[apirone-poll] Order {order_id} is {order['status']} — stopping")
            break

        # ── Payment window expired notification ───────────────────────────────
        if elapsed >= payment_window and not timed_out_notified:
            timed_out_notified = True
            await db_update_order(order_id, {"status": "expired"})
            if channel:
                try:
                    mention = user.mention if user else ""
                    await channel.send(
                        content=mention or None,
                        embed=discord.Embed(
                            title="⏰  Payment Window Expired",
                            description=(
                                f"The **{PAYMENT_TIMEOUT_MIN}-minute** payment window has closed.\n\n"
                                "If you already sent LTC — we are still monitoring for 10 more minutes "
                                "and will deliver automatically if confirmed.\n\n"
                                "If you haven't paid yet — open a new ticket."
                            ),
                            color=0xA855F7,
                        )
                    )
                except Exception:
                    pass

        addr = order.get("ltcAddress", "")
        if not addr:
            continue

        # ── Check address balance via Apirone ─────────────────────────────────
        try:
            bal = await apirone_get_address_balance(addr)
            total_litoshis = bal.get("total", 0)       # includes unconfirmed
            avail_litoshis = bal.get("available", 0)   # confirmed only

            expected_litoshis = int(order["ltcAmount"] * 1e8)
            tolerance_litoshis = int(FEE_TOLERANCE_LTC * 1e8)

            print(f"[apirone-poll] order={order_id} expected={expected_litoshis} total={total_litoshis} available={avail_litoshis}")

            addr_url = f"https://blockchair.com/litecoin/address/{addr}" if addr else ""

            # Detected on-chain but unconfirmed
            if total_litoshis >= (expected_litoshis - tolerance_litoshis) and not detected_notified:
                detected_notified = True
                await send_payment_detected_card(channel, user, order["ltcAmount"], addr_url)

            # Confirmed — deliver!
            if avail_litoshis >= (expected_litoshis - tolerance_litoshis):
                print(f"[apirone-poll] ✅ Payment confirmed for {order_id}! Delivering...")
                await send_payment_confirmed_card(channel, user, addr_url)
                await process_payment_confirmed(order_id, avail_litoshis)
                break

        except Exception as e:
            print(f"[apirone-poll] Error checking balance: {e}")
            continue

    else:
        order = await db_get_order(order_id)
        if order and order["status"] not in ("delivered", "cancelled", "error"):
            await db_update_order(order_id, {"status": "expired"})
            print(f"[apirone-poll] Order {order_id} fully expired")


#  ORDER CHANNEL HELPER
# ═══════════════════════════════════════════════════════════════════════════════

# ── Channel send helpers ──────────────────────────────────────────────────────

async def send_to_channel(channel_id: int, **kwargs):
    """Send a message to a channel by ID. Silently does nothing if channel not set/found."""
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if channel:
        try:
            await channel.send(**kwargs)
        except Exception as e:
            print(f"[log] Failed to send to channel {channel_id}: {e}")


async def send_log(embed: discord.Embed):
    """Send an event log embed to the log channel."""
    await send_to_channel(LOG_CHANNEL_ID, embed=embed)


async def create_order_channel(guild: discord.Guild, user: discord.Member, order_id: str) -> discord.TextChannel:
    """
    Create a private text channel for one order.
    Visible to: the buyer + admin role (or server admins) + bot.
    Placed inside ORDER_CATEGORY_ID folder if configured.
    Name format:  order-<shortid>-<username>
    """
    short_id  = order_id.split("-")[-1].lower()
    safe_name = user.display_name.lower().replace(" ", "-")[:20]
    chan_name = f"order-{short_id}-{safe_name}"

    # Block everyone by default, then grant access selectively
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me:           discord.PermissionOverwrite(view_channel=True, send_messages=True, embed_links=True),
        user:               discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
    }

    if ADMIN_ROLE_ID:
        role = guild.get_role(ADMIN_ROLE_ID)
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True, manage_messages=True
            )
    else:
        for member in guild.members:
            if member.guild_permissions.administrator and not member.bot:
                overwrites[member] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True
                )

    category = guild.get_channel(ORDER_CATEGORY_ID) if ORDER_CATEGORY_ID else None

    channel = await guild.create_text_channel(
        name=chan_name,
        overwrites=overwrites,
        category=category,
        topic=f"Order {order_id} | {user.display_name} ({user.id})",
        reason=f"AutoBuy order {order_id}",
    )
    return channel



# ── Transcript generator ──────────────────────────────────────────────────────

async def generate_transcript(channel: discord.TextChannel) -> discord.File | None:
    """Fetch all messages and build a Discord-style HTML transcript file."""
    try:
        import io
        messages = []
        async for msg in channel.history(limit=500, oldest_first=True):
            messages.append(msg)
        if not messages:
            return None

        html_parts = []
        html_parts.append("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Transcript</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#313338;color:#dcddde;font-family:"Whitney","Helvetica Neue",Helvetica,Arial,sans-serif;font-size:14px;padding:20px}
.header{background:#2b2d31;border-bottom:3px solid #5865f2;padding:16px 20px;margin-bottom:20px;border-radius:8px}
.header h1{color:#fff;font-size:18px}
.header p{color:#949ba4;font-size:12px;margin-top:4px}
.message{display:flex;padding:4px 16px;margin:2px 0;border-radius:4px}
.message:hover{background:#2e3035}
.avatar{width:40px;height:40px;border-radius:50%;margin-right:12px;flex-shrink:0;overflow:hidden;background:#5865f2;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:700;font-size:15px}
.avatar img{width:100%;height:100%;border-radius:50%}
.content{flex:1;min-width:0}
.meta{display:flex;align-items:baseline;gap:6px;margin-bottom:2px}
.author{font-weight:600;color:#fff}
.author.bot-author{color:#5865f2}
.badge{background:#5865f2;color:#fff;font-size:9px;padding:1px 4px;border-radius:3px;font-weight:700;letter-spacing:.3px}
.ts{font-size:11px;color:#72767d}
.text{color:#dcddde;line-height:1.5;word-break:break-word}
.embed{background:#2b2d31;border-left:4px solid #5865f2;border-radius:4px;padding:12px 16px;margin-top:6px;max-width:520px}
.etitle{color:#fff;font-weight:600;font-size:15px;margin-bottom:6px}
.edesc{color:#dcddde;font-size:13px;line-height:1.5;white-space:pre-wrap}
.efield{margin-top:8px}
.efname{color:#fff;font-weight:600;font-size:12px;margin-bottom:2px}
.efval{color:#dcddde;font-size:13px}
.efooter{color:#72767d;font-size:11px;margin-top:10px;border-top:1px solid #3f4147;padding-top:8px}
.divider{text-align:center;color:#72767d;font-size:11px;margin:16px 0;display:flex;align-items:center;gap:8px}
.divider::before,.divider::after{content:"";flex:1;height:1px;background:#3f4147}
code{background:#1e1f22;padding:2px 5px;border-radius:3px;font-family:monospace;font-size:12px;color:#e3e5e8}
</style></head><body>
""")

        html_parts.append(
            f'<div class="header"><h1>📋 Order Transcript — #{channel.name}</h1>'
            f'<p>Generated {datetime.utcnow().strftime("%Y-%m-%d %H:%M")} UTC &nbsp;|&nbsp; {len(messages)} messages</p></div>\n'
        )

        prev_author_id = None
        prev_date      = None

        for msg in messages:
            msg_date = msg.created_at.strftime("%Y-%m-%d")
            if msg_date != prev_date:
                label = msg.created_at.strftime("%B %d, %Y")
                html_parts.append(f'<div class="divider">{label}</div>\n')
                prev_date = msg_date

            is_bot   = msg.author.bot
            av_url   = str(msg.author.display_avatar.url) if msg.author.display_avatar else ""
            initials = (msg.author.display_name[:2] or "?").upper()
            av_html  = f'<img src="{av_url}" alt="">' if av_url else initials
            ac       = "author bot-author" if is_bot else "author"
            badge    = '<span class="badge">BOT</span>' if is_bot else ""
            ts       = msg.created_at.strftime("%I:%M %p")

            show_hdr = (msg.author.id != prev_author_id)
            prev_author_id = msg.author.id

            if show_hdr:
                html_parts.append(
                    f'<div class="message"><div class="avatar">{av_html}</div><div class="content">\n'
                    f'<div class="meta"><span class="{ac}">{msg.author.display_name}</span>{badge}<span class="ts">{ts}</span></div>\n'
                )
            else:
                html_parts.append(
                    f'<div class="message"><div class="avatar" style="opacity:0">{av_html}</div><div class="content">\n'
                )

            def esc(s):
                return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

            if msg.content:
                html_parts.append(f'<div class="text">{esc(msg.content)}</div>\n')

            for emb in msg.embeds:
                col = f"#{emb.colour.value:06x}" if emb.colour and emb.colour.value else "#5865f2"
                html_parts.append(f'<div class="embed" style="border-left-color:{col}">\n')
                if emb.title:
                    html_parts.append(f'<div class="etitle">{esc(emb.title)}</div>\n')
                if emb.description:
                    html_parts.append(f'<div class="edesc">{esc(emb.description)}</div>\n')
                for f in emb.fields:
                    html_parts.append(f'<div class="efield"><div class="efname">{esc(f.name)}</div><div class="efval">{esc(f.value)}</div></div>\n')
                if emb.footer and emb.footer.text:
                    html_parts.append(f'<div class="efooter">{esc(emb.footer.text)}</div>\n')
                html_parts.append('</div>\n')

            html_parts.append('</div></div>\n')

        html_parts.append("</body></html>")
        html = "".join(html_parts)
        return discord.File(fp=io.BytesIO(html.encode("utf-8")), filename=f"transcript-{channel.name}.html")
    except Exception as e:
        print(f"[transcript] {e}")
        return None


async def _send_transcript(channel: discord.TextChannel, buyer: discord.Member | None):
    """Generate transcript and send to buyer DM + log channel."""
    t1 = await generate_transcript(channel)
    t2 = await generate_transcript(channel)

    dm_embed = discord.Embed(
        title="📋  Your Order Transcript",
        description=(
            f"Here is the full conversation log for **#{channel.name}**.\n"
            "Open the attached `.html` file in your browser to view it."
        ),
        color=0x7C3AED,
    )
    dm_embed.set_footer(text="Transcript auto-generated after order completion.")

    if buyer and t1:
        try:
            await buyer.send(embed=dm_embed, file=t1)
        except Exception:
            pass

    if t2 and LOG_CHANNEL_ID:
        log_embed = discord.Embed(
            title="📋  Order Transcript",
            description=f"Channel: **#{channel.name}**",
            color=0x7C3AED,
        )
        await send_to_channel(LOG_CHANNEL_ID, embed=log_embed, file=t2)


async def close_order_channel(channel: discord.TextChannel, buyer: discord.Member = None):
    """
    2-phase closure after delivery:
      Phase 1 (10 min)  — buyer loses send permission, transcript sent
      Phase 2 (60 min)  — channel hidden from buyer, admins keep access forever
    """
    try:
        await channel.send(embed=discord.Embed(
            title="🔒  Channel Closing",
            description="This channel will be **locked in 10 minutes** and **hidden in 60 minutes**.\nA transcript will be sent to your DMs.",
            color=0x7C3AED,
        ))

        # Rename to closed- so admins can see at a glance
        try:
            new_name = "closed-" + channel.name.replace("order-", "")
            await channel.edit(name=new_name)
        except Exception:
            pass

        # Rename to closed- so admins can see delivery status
        try:
            new_name = "closed-" + channel.name.replace("order-", "")
            await channel.edit(name=new_name)
        except Exception:
            pass

        # ── Phase 1: lock writing after 10 minutes ────────────────────────────
        await asyncio.sleep(600)

        if buyer:
            await channel.set_permissions(buyer, send_messages=False, read_messages=True)
        # Explicitly keep default_role hidden — just removing send is not enough
        await channel.set_permissions(channel.guild.default_role, view_channel=False, send_messages=False)

        await channel.send(embed=discord.Embed(
            title="🔒  Channel Locked",
            description="This channel has been locked. It will be hidden from your view in 50 minutes.\nYour transcript has been sent to your DMs.",
            color=0x7C3AED,
        ))

        # Send transcript now (while buyer can still see the lock message)
        await _send_transcript(channel, buyer)

        # ── Phase 2: hide from buyer after 60 minutes total ──────────────────
        await asyncio.sleep(3000)   # 50 more minutes

        if buyer:
            await channel.set_permissions(buyer, view_channel=False, send_messages=False)

        # Tag the order as archived in DB
        if channel.topic:
            for part in channel.topic.split("|"):
                part = part.strip()
                if part.startswith("ORD-"):
                    await db_update_order(part, {"channelArchived": True})
                    break

    except Exception as e:
        print(f"[channel close] {e}")


async def close_order_channel_cancel(channel: discord.TextChannel, buyer: discord.Member = None):
    """Faster close for cancelled orders: lock immediately, rename, hide in 10 min."""
    try:
        # Rename to closed- so admins can see status at a glance
        try:
            new_name = "closed-" + channel.name.replace("order-", "")
            await channel.edit(name=new_name)
        except Exception:
            pass

        await channel.send(embed=discord.Embed(
            title="❌  Order Cancelled",
            description="This channel will be hidden in 10 minutes. A transcript has been sent to your DMs.",
            color=0xE74C3C,
        ))

        if buyer:
            await channel.set_permissions(buyer, send_messages=False, read_messages=True)
        # Explicitly keep default_role hidden
        await channel.set_permissions(channel.guild.default_role, view_channel=False, send_messages=False)

        await _send_transcript(channel, buyer)
        await asyncio.sleep(600)

        if buyer:
            await channel.set_permissions(buyer, view_channel=False, send_messages=False)

    except Exception as e:
        print(f"[channel cancel close] {e}")


# ── Order Finalized card (Components V2) ──────────────────────────────────────
# Uses the custom emojis uploaded via &setupemojis (bot_noentry, bot_pin,
# bot_assistance, bot_sweep, bot_lock). Falls back to unicode if not uploaded yet.

class _OrderFinalizedCloseButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.danger,
            label="Close (Admin)",
            emoji="🔒",
            custom_id="orderfinalized:close",
        )

    async def callback(self, interaction: discord.Interaction):
        member = interaction.user
        is_allowed = member.guild_permissions.administrator if isinstance(member, discord.Member) else False
        if not is_allowed and ADMIN_ROLE_ID and isinstance(member, discord.Member):
            role = interaction.guild.get_role(ADMIN_ROLE_ID) if interaction.guild else None
            is_allowed = bool(role and role in member.roles)
        if not is_allowed:
            await interaction.response.send_message(
                "🔒 This button is restricted to administrators.", ephemeral=True
            )
            return

        await interaction.response.send_message("🔒 Closing this ticket...", ephemeral=True)
        buyer = None
        for target, ow in interaction.channel.overwrites.items():
            if isinstance(target, discord.Member) and not target.bot and not target.guild_permissions.administrator:
                buyer = target
                break
        asyncio.create_task(close_order_channel_cancel(interaction.channel, buyer=buyer))


class _OrderFinalizedAssistanceButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            style=discord.ButtonStyle.primary,
            label="Need Assistance",
            emoji="🧑‍💼",
            custom_id="orderfinalized:assist",
        )

    async def callback(self, interaction: discord.Interaction):
        ping = ""
        if ADMIN_ROLE_ID and interaction.guild:
            role = interaction.guild.get_role(ADMIN_ROLE_ID)
            ping = role.mention if role else ""
        await interaction.response.send_message(
            content=(f"{ping} " if ping else "") + f"{interaction.user.mention} needs assistance with this order."
        )


class OrderFinalizedView(discord.ui.View):
    """Persistent view attached to the Order Finalized card."""
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(_OrderFinalizedCloseButton())
        self.add_item(_OrderFinalizedAssistanceButton())


async def send_order_finalized_card(channel, user, order_id: str, tx_id: str = ""):
    """Components V2 'Order Finalized' card — sent after payment is confirmed and
    items are delivered. Does NOT replace the existing `.ordercomplete` ticket
    tool message; this is an additional card shown to the buyer."""
    if not channel:
        return
    guild = getattr(channel, "guild", None)

    e_cart       = get_bot_emoji(guild, "bot_cart", "🛒")
    e_noentry    = get_bot_emoji(guild, "bot_noentry", "🚫")
    e_pin        = get_bot_emoji(guild, "bot_pin", "📌")
    e_assistance = get_bot_emoji(guild, "bot_assistance", "🧑‍💼")
    e_sweep      = get_bot_emoji(guild, "bot_sweep", "🧹")

    view = discord.ui.LayoutView(timeout=None)
    container = discord.ui.Container(accent_colour=discord.Colour(0x5865F2))
    container.add_item(discord.ui.TextDisplay(f"## {e_cart}  Order Finalized"))
    container.add_item(discord.ui.Separator())
    container.add_item(discord.ui.TextDisplay(
        f"{e_noentry}  {user.mention if user else 'Buyer'}, your ticket is now read-only."
    ))
    container.add_item(discord.ui.TextDisplay(
        f"{e_pin}  If you need help with your order, please click the {e_assistance} "
        "Need Assistance button below."
    ))
    container.add_item(discord.ui.Separator())
    container.add_item(discord.ui.TextDisplay(f"{e_sweep}  Payment TX"))
    container.add_item(discord.ui.TextDisplay(f"```\n{tx_id or 'N/A'}\n```"))

    row = discord.ui.ActionRow()
    row.add_item(_OrderFinalizedCloseButton())
    row.add_item(_OrderFinalizedAssistanceButton())
    container.add_item(row)

    view.add_item(container)

    try:
        await channel.send(view=view)
    except Exception as e:
        print(f"[card] Failed to send Order Finalized card: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  DISCORD UI — VIEWS & MODALS
# ═══════════════════════════════════════════════════════════════════════════════


async def handle_product_select(
    interaction: discord.Interaction,
    slug: str,
    home_channel: discord.TextChannel = None,
    buyer: discord.Member = None,
):
    """
    Called when user picks a product from the in-ticket dropdown.
    The ticket channel already exists (home_channel).
    Posts quantity buttons directly in the channel — visible to everyone.
    """
    if slug == "none":
        await interaction.response.send_message("No products available.", ephemeral=True)
        return

    cat = await db_get_category(slug)
    if not cat:
        await interaction.response.send_message("Product not found.", ephemeral=True)
        return

    channel = home_channel or interaction.channel
    user    = buyer or interaction.user

    if len(cat.get("stock", [])) == 0 and not cat.get("infinite_stock"):
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Out of Stock",
                description=f"**{cat['name']}** is currently unavailable.",
                color=0xE74C3C,
            ),
            ephemeral=True,
        )
        return

    # Show ToC if set — post VISIBLY in the channel (not ephemeral)
    toc_message = await get_toc(slug)
    if toc_message:
        await interaction.response.defer(ephemeral=True)
        toc_embed = discord.Embed(
            title="📜  Terms & Conditions",
            description=(
                "**" + cat["name"] + "** — please read and accept the terms below:\n\n"
                + toc_message
            ),
            color=0xA855F7,
        )
        toc_embed.set_footer(text="React with the buttons below to accept or decline.")
        await channel.send(
            content=user.mention,
            embed=toc_embed,
            view=ToCChannelView(slug, cat, channel, user),
        )
        return

    # No ToC — post quantity question in channel
    await interaction.response.defer(ephemeral=True)
    await _ask_quantity_in_channel(channel, cat, user)


async def _ask_quantity_in_channel(channel: discord.TextChannel, cat: dict, user: discord.Member):
    """Post the Select Quantity picker in the ticket channel (button-based, no text listening)."""
    min_qty = int(cat.get("min_quantity") or 1)
    view    = QuantityPickerView(cat=cat, user=user, channel=channel, qty=min_qty)
    await channel.send(content=user.mention, embed=_build_qty_picker_embed(cat, min_qty), view=view)



# ── Panel (persistent — survives bot restart) ──────────────────────────────────
# ── Panel helpers ─────────────────────────────────────────────────────────────

async def build_panel_embed() -> discord.Embed:
    """Build the main shop panel embed — styled to match NoxStore design."""
    cats        = await db_get_categories()
    total_stock = sum(len(c.get("stock", [])) if not c.get("infinite_stock") else 9999 for c in cats)
    active      = len(cats)
    shop_name   = await get_setting("shop_name")
    banner_url  = await get_setting("banner_url")
    icon_url    = await get_setting("icon_url")

    embed = discord.Embed(
        title=f"{shop_name} | Autobuy",
        description=(
            "**Instant Delivery • 24/7 Support**\n\n"
            "Select a product from the dropdown menu below to start.\n"
            "Payments are processed automatically via Litecoin (LTC).\n"
            "\u200b"
        ),
        color=0x9B59B6,
    )

    if icon_url:
        embed.set_thumbnail(url=icon_url)

    # Live stats block — styled like the screenshot
    stock_display = "∞" if total_stock >= 9999 else str(total_stock)
    embed.add_field(
        name="<:statusup:1513043763646431372> Live Store Statistics",
        value=(
            "<a:Green_dot1:1513043761293299903> **System Status:** Online\n"
            "• <a:65023lightning:1513043765940715580> **Active Products:** " + str(active) + "\n"
            "• <:emojigg_box:1513043770357186701> **Total Stock:** " + stock_display + "\n"
            "• <:5636activity:1513043768285073619> **Delivery Speed:** Instant"
        ),
        inline=False,
    )

    if banner_url:
        embed.set_image(url=banner_url)

    embed.set_footer(text=f"{shop_name} Automations • Live Updates")
    return embed


async def build_panel_select(all_cats: bool = True) -> discord.ui.Select:
    """Build the product dropdown with live stock status indicators."""
    cats = await db_get_categories()

    if not cats:
        sel = discord.ui.Select(
            placeholder="No products available",
            options=[discord.SelectOption(label="No products", value="none", emoji="🔴")],
            disabled=True,
            custom_id="panel:select",
        )
        return sel

    options = []
    for c in cats:
        is_infinite = bool(c.get("infinite_stock"))
        count = len(c.get("stock", [])) if not is_infinite else 9999
        if is_infinite:
            emoji = "🟢"
            desc  = f"∞ unlimited • ${c['price_usd']:.2f}"
        elif count == 0:
            emoji = "🔴"
            desc  = f"Out of stock • ${c['price_usd']:.2f}"
        elif count <= 3:
            emoji = "🟡"
            desc  = f"{count} left • ${c['price_usd']:.2f}"
        else:
            emoji = "🟢"
            desc  = f"{count} in stock • ${c['price_usd']:.2f}"

        options.append(discord.SelectOption(
            label=c["name"],
            description=desc,
            value=c["slug"],
            emoji=emoji,
        ))

    sel = discord.ui.Select(
        placeholder="Select a product to purchase...",
        options=options,
        custom_id="panel:select",
    )
    return sel


async def auto_update_panel():
    """Auto-refreshes panel embed stats."""
    try:
        msg_id = await get_setting("panel_message_id")
        ch_id  = await get_setting("panel_channel_id")
        if not msg_id or not ch_id:
            return
        guild = bot.guilds[0] if bot.guilds else None
        if not guild:
            return
        channel = guild.get_channel(int(ch_id))
        if not channel:
            return
        try:
            msg = await channel.fetch_message(int(msg_id))
        except Exception:
            return
        await msg.edit(embed=await build_panel_embed(), view=PanelView())
        print(f"[panel] ✅ Auto-updated in #{channel.name}")
    except Exception as e:
        print(f"[panel] Auto-update failed: {e}")


class PanelView(discord.ui.View):
    """
    Main shop panel — single dropdown with two options:
      1. 📦 Stock         — show live stock (ephemeral)
      2. 🎫 Create Ticket — open an order ticket
    """
    def __init__(self):
        super().__init__(timeout=None)

        self.panel_select = discord.ui.Select(
            placeholder="🛒  Click Here To Select a product.",
            options=[
                discord.SelectOption(
                    label="Stock",
                    description="View live stock counts for all products",
                    value="action:stock",
                    emoji="📦",
                ),
                discord.SelectOption(
                    label="Click to create AutoBuy ticket",
                    description="Open a private order ticket to make a purchase",
                    value="action:ticket",
                    emoji="🎫",
                ),
            ],
            custom_id="panel:main_select",
        )
        self.panel_select.callback = self._on_select
        self.add_item(self.panel_select)

    async def _on_select(self, interaction: discord.Interaction):
        chosen = interaction.data["values"][0]

        if chosen == "action:stock":
            await interaction.response.defer(ephemeral=True)
            cats  = await db_get_categories()
            embed = discord.Embed(
                title="📦  Live Store Stock",
                description="Real-time stock levels across all products.",
                color=0x9B59B6,
            )
            embed.set_footer(text="Stock updates automatically with every purchase")
            if not cats:
                embed.description = "No products are currently available."
            else:
                for c in cats:
                    is_inf = bool(c.get("infinite_stock"))
                    count  = len(c.get("stock", [])) if not is_inf else 9999
                    if is_inf:
                        badge = "♾️  Unlimited"
                        dot   = "🟢"
                    elif count == 0:
                        badge = "❌  Out of Stock"
                        dot   = "🔴"
                    elif count <= 3:
                        badge = f"⚠️  {count} remaining — Low Stock"
                        dot   = "🟡"
                    else:
                        badge = f"✅  {count} available"
                        dot   = "🟢"

                    min_qty = int(c.get("min_quantity") or 1)
                    price_line = f"💲 **Price:** ${c['price_usd']:.2f}"
                    min_line   = f"  •  🔢 **Min Order:** {min_qty}" if min_qty > 1 else ""
                    embed.add_field(
                        name=f"{dot}  {c['name']}",
                        value=f"{price_line}{min_line}\n{badge}",
                        inline=True,
                    )
            await interaction.followup.send(embed=embed, ephemeral=True)

        elif chosen == "action:ticket":
            await interaction.response.defer(ephemeral=True)

            # Check blacklist
            bl = await col_blacklist.find_one({"userId": str(interaction.user.id)})
            if bl:
                await interaction.followup.send(embed=discord.Embed(
                    title="🔒  Access Restricted",
                    description=(
                        "Your account has been restricted from making purchases.\n"
                        "Please contact an admin if you believe this is an error."
                    ),
                    color=0xE74C3C,
                ), ephemeral=True)
                return

            import random as _r, string as _s
            temp_id = "ORD-" + "".join(_r.choices(_s.ascii_uppercase + _s.digits, k=8))
            try:
                channel = await create_order_channel(interaction.guild, interaction.user, temp_id)
            except discord.Forbidden:
                await interaction.followup.send(
                    embed=discord.Embed(
                        title="⚠️  Permission Error",
                        description="The bot lacks the **Manage Channels** permission. Please contact an admin.",
                        color=0xE74C3C,
                    ), ephemeral=True
                )
                return

            open_view = discord.ui.View(timeout=300)
            open_view.add_item(discord.ui.Button(
                label="🎫  Open My Ticket",
                style=discord.ButtonStyle.link,
                url=f"https://discord.com/channels/{interaction.guild.id}/{channel.id}",
            ))
            await interaction.followup.send(
                embed=discord.Embed(
                    title="🎫  Ticket Created",
                    description=(
                        f"Your private order ticket has been opened.\n\n"
                        f"**Channel:** {channel.mention}\n\n"
                        "Head over and select your product to continue."
                    ),
                    color=0x7B2FBE,
                ),
                view=open_view,
                ephemeral=True,
            )
            await _post_ticket_welcome(channel, interaction.user)



async def _post_ticket_welcome(channel: discord.TextChannel, user: discord.Member):
    shop_name = await get_setting("shop_name") or "AutoBuy"
    icon_url  = await get_setting("icon_url")
    banner    = await get_setting("banner_url")

    welcome = discord.Embed(
        title=f"🎫  Welcome to Your Order Ticket",
        description=(
            f"Hey {user.mention}, your private ticket is ready.\n\n"
            "Use the **product dropdown** below to select what you'd like to purchase. "
            "Payment is handled automatically via **Litecoin (LTC)** — no manual steps needed.\n\n"
            "If you need help at any point, click **🆘 Request Admin Support** in the invoice view."
        ),
        color=0x9B59B6,
    )
    if icon_url:
        welcome.set_thumbnail(url=icon_url)
    if banner:
        welcome.set_image(url=banner)
    welcome.set_footer(text=f"{shop_name}  •  All Rights Reserved")

    close_view = discord.ui.View(timeout=None)
    close_btn  = discord.ui.Button(label="🔒  Close Ticket", style=discord.ButtonStyle.danger)

    async def _close(inter: discord.Interaction, _b=None):
        is_owner        = inter.user.id == user.id
        is_admin_member = ADMIN_ROLE_ID and any(r.id == ADMIN_ROLE_ID for r in inter.user.roles)
        if is_owner or is_admin_member or inter.user.guild_permissions.administrator:
            asyncio.create_task(close_order_channel_cancel(channel, buyer=user))
            await inter.response.send_message("🔒 Closing your ticket...", ephemeral=True)
        else:
            await inter.response.send_message("Only the ticket owner or an admin can close this ticket.", ephemeral=True)

    close_btn.callback = _close
    close_view.add_item(close_btn)
    await channel.send(content=user.mention, embed=welcome, view=close_view)

    cats = await db_get_categories()
    if not cats:
        await channel.send(embed=discord.Embed(
            title="📭  No Products Available",
            description="There are currently no products in stock. Please check back later or contact an admin.",
            color=0xE74C3C,
        ))
        return

    options = []
    for c in cats:
        is_infinite = bool(c.get("infinite_stock"))
        count = len(c.get("stock", [])) if not is_infinite else 9999
        if is_infinite:
            emoji = "🟢"
            desc  = "∞ Unlimited  •  $" + str(c["price_usd"])
        elif count == 0:
            emoji = "🔴"
            desc  = "Out of Stock"
        else:
            emoji = "🟡" if count <= 3 else "🟢"
            desc  = str(count) + " in stock  •  $" + str(c["price_usd"])
        options.append(discord.SelectOption(label=c["name"], description=desc, value=c["slug"], emoji=emoji))

    sel = discord.ui.Select(
        placeholder="🛍️  Select a product to purchase...",
        options=options,
    )

    async def _on_select(inter: discord.Interaction):
        await handle_product_select(inter, inter.data["values"][0], home_channel=channel, buyer=user)

    sel.callback = _on_select
    sel_view = discord.ui.View(timeout=PAYMENT_TIMEOUT_MIN * 60)
    sel_view.add_item(sel)

    product_embed = discord.Embed(
        title="🛒  Choose Your Product",
        description=(
            "Select a product from the dropdown below to view pricing and proceed to checkout.\n"
            "Stock levels update in real-time."
        ),
        color=0x9B59B6,
    )
    product_embed.set_footer(text="🟢 In Stock  •  🟡 Low Stock  •  🔴 Out of Stock")
    await channel.send(embed=product_embed, view=sel_view)




class ToCAcceptedView(discord.ui.View):
    """Replaces the ToC buttons after the user accepts — shows a disabled 'Accepted' button."""
    def __init__(self, display_name: str):
        super().__init__(timeout=None)
        accepted_btn = discord.ui.Button(
            label=f"✅  Accepted by {display_name}",
            style=discord.ButtonStyle.success,
            disabled=True,
        )
        self.add_item(accepted_btn)


class ToCChannelView(discord.ui.View):
    """ToC accept/decline posted visibly inside the ticket channel."""
    def __init__(self, slug, cat, channel, user):
        super().__init__(timeout=600)
        self.slug    = slug
        self.cat     = cat
        self.channel = channel
        self.user    = user

    @discord.ui.button(label="✅  I Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, _b):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        cat = await db_get_category(self.slug)
        if not cat or len(cat.get("stock", [])) == 0:
            await interaction.response.send_message("This product is no longer available.", ephemeral=True)
            return
        # Keep the original ToC embed visible — just swap buttons to disabled "ToC Accepted"
        original_embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed(
            title="📜  Terms & Conditions", color=0x7B2FBE
        )
        original_embed.color = 0x2ECC71
        await interaction.response.edit_message(embed=original_embed, view=ToCAcceptedView(interaction.user.display_name))
        await _ask_quantity_in_channel(self.channel, cat, self.user)

    @discord.ui.button(label="❌  Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, _b):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        original_embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed(
            title="📜  Terms & Conditions", color=0xE74C3C
        )
        original_embed.color = 0xE74C3C
        declined_view = discord.ui.View()
        declined_btn  = discord.ui.Button(label="❌  Declined", style=discord.ButtonStyle.danger, disabled=True)
        declined_view.add_item(declined_btn)
        await interaction.response.edit_message(embed=original_embed, view=declined_view)


class ToCView(discord.ui.View):
    """Ephemeral ToC shown before purchase (legacy/fallback path)."""
    def __init__(self, slug: str, cat: dict):
        super().__init__(timeout=120)
        self.slug = slug
        self.cat  = cat

    @discord.ui.button(label="✅  I Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, _b):
        cat = await db_get_category(self.slug)
        if not cat or len(cat.get("stock", [])) == 0:
            await interaction.response.send_message("This product is no longer available.", ephemeral=True)
            return
        # Keep ToC embed, replace buttons with disabled accepted state
        original_embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed(
            title="📜  Terms & Conditions", color=0x7B2FBE
        )
        original_embed.color = 0x2ECC71
        await interaction.response.edit_message(embed=original_embed, view=ToCAcceptedView(interaction.user.display_name))
        await _ask_quantity_in_channel(interaction.channel, cat, interaction.user)

    @discord.ui.button(label="❌  Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, _b):
        original_embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed(
            title="📜  Terms & Conditions", color=0xE74C3C
        )
        original_embed.color = 0xE74C3C
        declined_view = discord.ui.View()
        declined_view.add_item(discord.ui.Button(label="❌  Declined", style=discord.ButtonStyle.danger, disabled=True))
        await interaction.response.edit_message(embed=original_embed, view=declined_view)


class ProductCardView(discord.ui.View):
    """Shown after selecting a product — has a Buy button."""
    def __init__(self, slug: str, can_buy: bool):
        super().__init__(timeout=120)
        self.slug = slug

        buy_btn = discord.ui.Button(
            label="🛒 Buy Now",
            style=discord.ButtonStyle.success,
            disabled=not can_buy,
        )
        buy_btn.callback = self._on_buy
        self.add_item(buy_btn)

    async def _on_buy(self, interaction: discord.Interaction):
        cat = await db_get_category(self.slug)
        if not cat or len(cat.get("stock", [])) == 0:
            await interaction.response.send_message("❌ This product is out of stock.", ephemeral=True)
            return

        # ── Show ToC if set for this category ────────────────────────────────
        toc_message = await get_toc(self.slug)
        if toc_message:
            toc_embed = discord.Embed(
                title="📜  Terms & Conditions",
                description=(
                    f"Before purchasing **{cat['name']}**, please read and accept the terms below:\n\n"
                    f"{toc_message}"
                ),
                color=0xA855F7,
            )
            toc_embed.set_footer(text="You must accept the Terms & Conditions to proceed with your purchase.")
            await interaction.response.send_message(
                embed=toc_embed,
                view=ToCView(self.slug, cat),
                ephemeral=True,
            )
        else:
            # No ToC — create ticket channel and show quantity picker inside it
            await interaction.response.defer(ephemeral=True)
            import random as _r2, string as _s2
            temp_id = "ORD-" + "".join(_r2.choices(_s2.ascii_uppercase + _s2.digits, k=8))
            try:
                channel = await create_order_channel(interaction.guild, interaction.user, temp_id)
            except discord.Forbidden:
                await interaction.followup.send("Bot lacks Manage Channels permission.", ephemeral=True)
                return
            min_qty = int(cat.get("min_quantity") or 1)
            view    = QuantityPickerView(cat=cat, user=interaction.user, channel=channel, qty=min_qty)
            await channel.send(content=interaction.user.mention, embed=_build_qty_picker_embed(cat, min_qty), view=view)
            open_view = discord.ui.View(timeout=300)
            open_view.add_item(discord.ui.Button(
                label="Open Ticket",
                style=discord.ButtonStyle.link,
                url=f"https://discord.com/channels/{interaction.guild.id}/{channel.id}",
            ))
            await interaction.followup.send(
                embed=discord.Embed(title="🎫  Ticket Created", description=channel.mention, color=0x7B2FBE),
                view=open_view, ephemeral=True,
            )



class QuantityPickerView(discord.ui.View):
    """
    Image 1 — Select Quantity.
    −/✏️/+ buttons on row 0, Continue/Cancel Ticket on row 1.
    Order is NOT created until the user clicks Continue.
    Pass order_id to update an existing order (Change Quantity flow from PurchaseSummaryView).
    """
    def __init__(self, cat: dict, user: discord.Member, channel,
                 qty: int = 1, order_id: str | None = None):
        super().__init__(timeout=PAYMENT_TIMEOUT_MIN * 60)
        self.cat      = cat
        self.user     = user
        self.channel  = channel
        self.qty      = max(int(cat.get("min_quantity") or 1), qty)
        self.order_id = order_id
        self._rebuild_buttons()

    def _rebuild_buttons(self):
        self.clear_items()
        is_infinite = bool(self.cat.get("infinite_stock"))
        stock_count = len(self.cat.get("stock", [])) if not is_infinite else 9999
        min_qty     = int(self.cat.get("min_quantity") or 1)

        minus_btn          = discord.ui.Button(label="−", style=discord.ButtonStyle.secondary, row=0)
        minus_btn.disabled = self.qty <= min_qty
        minus_btn.callback = self._on_minus
        self.add_item(minus_btn)

        pencil_btn          = discord.ui.Button(label=f"✏️  {self.qty}", style=discord.ButtonStyle.primary, row=0)
        pencil_btn.callback = self._on_pencil
        self.add_item(pencil_btn)

        plus_btn          = discord.ui.Button(label="+", style=discord.ButtonStyle.secondary, row=0)
        plus_btn.disabled = self.qty >= stock_count
        plus_btn.callback = self._on_plus
        self.add_item(plus_btn)

        cont_btn          = discord.ui.Button(label="Continue", style=discord.ButtonStyle.success, row=1)
        cont_btn.callback = self._on_continue
        self.add_item(cont_btn)

        cancel_btn          = discord.ui.Button(label="Cancel Ticket", style=discord.ButtonStyle.danger, row=1)
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _check_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return False
        return True

    async def _on_minus(self, interaction: discord.Interaction):
        if not await self._check_user(interaction):
            return
        min_qty  = int(self.cat.get("min_quantity") or 1)
        self.qty = max(min_qty, self.qty - 1)
        self._rebuild_buttons()
        await interaction.response.edit_message(embed=_build_qty_picker_embed(self.cat, self.qty), view=self)

    async def _on_plus(self, interaction: discord.Interaction):
        if not await self._check_user(interaction):
            return
        is_infinite = bool(self.cat.get("infinite_stock"))
        stock_count = len(self.cat.get("stock", [])) if not is_infinite else 9999
        self.qty    = min(stock_count, self.qty + 1)
        self._rebuild_buttons()
        await interaction.response.edit_message(embed=_build_qty_picker_embed(self.cat, self.qty), view=self)

    async def _on_pencil(self, interaction: discord.Interaction):
        if not await self._check_user(interaction):
            return
        await interaction.response.send_modal(QtyTypeModal(self))

    async def _on_continue(self, interaction: discord.Interaction):
        if not await self._check_user(interaction):
            return
        await interaction.response.defer()
        cat = await db_get_category(self.cat["slug"])
        if not cat:
            await interaction.followup.send("Product not found.", ephemeral=True)
            return

        is_infinite = bool(cat.get("infinite_stock"))
        stock_count = len(cat.get("stock", [])) if not is_infinite else 9999
        min_qty     = int(cat.get("min_quantity") or 1)
        qty         = self.qty

        if qty < min_qty:
            await interaction.followup.send(f"❌ Minimum order is **{min_qty}**.", ephemeral=True)
            return
        if qty > stock_count:
            await interaction.followup.send(f"❌ Only **{stock_count}** in stock.", ephemeral=True)
            return

        ltc_price  = await ltc_get_price()
        total_usd  = round(cat["price_usd"] * qty, 2)
        ltc_amount = round(total_usd / ltc_price, 6)

        if self.order_id:
            # Change Quantity flow — update existing order
            await db_update_order(self.order_id, {
                "quantity":  qty,
                "totalUSD":  total_usd,
                "ltcAmount": ltc_amount,
            })
            order = await db_get_order(self.order_id)
            if not order:
                await interaction.followup.send("❌ Order not found — it may have expired. Please start a new order.", ephemeral=True)
                return
        else:
            # Fresh order
            order = await db_create_order(
                user_id=str(interaction.user.id), slug=cat["slug"], cat_name=cat["name"],
                quantity=qty, total_usd=total_usd, ltc_amount=ltc_amount, ltc_address=LTC_WALLET_ADDRESS,
            )
            await db_update_order(order["orderId"], {"channelId": str(self.channel.id)})
            np_result = await _create_invoice_or_fail(order["orderId"], ltc_amount, total_usd, self.channel, interaction.user)
            if not np_result:
                return
            await db_update_order(order["orderId"], {
                "ltcAddress": np_result["pay_address"],
                "ltcAmount":  np_result["pay_amount"],
            })

            await send_log(discord.Embed(
                title="🆕  New Order Opened",
                description=(
                    f"**Order:** `{order['orderId']}`\n"
                    f"**User:** {interaction.user.mention}\n"
                    f"**Product:** {cat['name']} x{qty}\n"
                    f"**Amount:** {ltc_amount} LTC (${total_usd})"
                ),
                color=0x9B59B6,
            ))

            try:
                await self.channel.edit(topic=f"Order: {order['orderId']} | Status: pending | Product: {cat['name']}")
            except Exception:
                pass

        await interaction.message.edit(
            embed=_build_purchase_summary_embed(cat, qty),
            view=PurchaseSummaryView(order["orderId"], cat, qty, ltc_amount, total_usd, ltc_price),
        )

    async def _on_cancel(self, interaction: discord.Interaction):
        if not await self._check_user(interaction):
            return
        if self.order_id:
            await db_update_order(self.order_id, {"status": "cancelled"})
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="🚫  Order Cancelled",
                description="Your order has been cancelled successfully. Feel free to open a new ticket at any time.",
                color=0xE74C3C,
            ),
            view=None,
        )
        asyncio.create_task(close_order_channel_cancel(self.channel, buyer=interaction.user))


class QtyTypeModal(discord.ui.Modal, title="Enter Quantity"):
    """Opened by the ✏️ pencil button on QuantityPickerView."""
    def __init__(self, picker: "QuantityPickerView"):
        super().__init__()
        self.picker = picker
        min_q = int(picker.cat.get("min_quantity") or 1)
        self.qty_input = discord.ui.TextInput(
            label=f"Quantity (min {min_q})" if min_q > 1 else "Quantity",
            placeholder=f"Enter a number e.g. {picker.qty}",
            default=str(picker.qty),
            min_length=1, max_length=5, required=True,
        )
        self.add_item(self.qty_input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.qty_input.value.strip()
        if not raw.isdigit() or int(raw) < 1:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
            return
        cat         = await db_get_category(self.picker.cat["slug"])
        is_infinite = bool(cat.get("infinite_stock"))
        stock_count = len(cat.get("stock", [])) if not is_infinite else 9999
        min_qty     = int(cat.get("min_quantity") or 1)
        qty         = int(raw)
        if qty < min_qty:
            await interaction.response.send_message(f"❌ Minimum order is **{min_qty}**.", ephemeral=True)
            return
        if qty > stock_count:
            await interaction.response.send_message(f"❌ Only **{stock_count}** in stock.", ephemeral=True)
            return
        self.picker.qty = qty
        self.picker._rebuild_buttons()
        await interaction.response.edit_message(
            embed=_build_qty_picker_embed(self.picker.cat, qty),
            view=self.picker,
        )


class PurchaseSummaryView(discord.ui.View):
    """Image 2 — Purchase Summary. Shown after the user confirms qty in QuantityPickerView."""
    def __init__(self, order_id, cat, qty, ltc_amount, total_usd, ltc_price):
        super().__init__(timeout=PAYMENT_TIMEOUT_MIN * 60)
        self.order_id   = order_id
        self.cat        = cat
        self.qty        = qty
        self.ltc_amount = ltc_amount
        self.total_usd  = total_usd
        self.ltc_price  = ltc_price

    @discord.ui.button(label="🛒  Change Quantity", style=discord.ButtonStyle.primary)
    async def change_qty(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order or order["userId"] != str(interaction.user.id):
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        # Go back to Image 1 picker with existing order_id so Continue updates instead of creates
        view = QuantityPickerView(
            cat=self.cat, user=interaction.user,
            channel=interaction.channel, qty=self.qty, order_id=self.order_id,
        )
        await interaction.response.edit_message(
            embed=_build_qty_picker_embed(self.cat, self.qty),
            view=view,
        )

    @discord.ui.button(label="💸  Continue to Payment", style=discord.ButtonStyle.success)
    async def continue_payment(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order or order["userId"] != str(interaction.user.id):
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        if order["status"] in ("cancelled", "expired"):
            await interaction.response.send_message("Order no longer active.", ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=discord.Embed(title="⏳  Generating invoice...", color=0x1E0A3C),
            view=None,
        )
        await asyncio.sleep(1)

        addr     = order.get("ltcAddress") or ""
        pay_link = order.get("payLink") or ""
        amt      = order["ltcAmount"]
        usd      = round(order["totalUSD"], 2)

        inv = discord.Embed(
            title="Payment Invoice Generated",
            description="Kindly send the exact amount to the shown LTC address below.",
            color=0x1A1A2E,
        )
        inv.set_author(
            name="Litecoin Payment",
            icon_url="https://cryptologos.cc/logos/litecoin-ltc-logo.png",
        )
        inv.set_thumbnail(url=INFO_IMAGE_URL)

        if addr:
            inv.add_field(name="• LTC Wallet Address", value=f"```\n{addr}\n```", inline=False)
        inv.add_field(name="• Amount to Pay (LTC)", value=f"```\n{amt}\n```", inline=False)
        inv.add_field(name="• Equivalent in USD",   value=f"```\n${usd}\n```", inline=False)
        inv.add_field(
            name="\u200b",
            value="ℹ️ *Double check the amount. Payments are irreversible.*",
            inline=False,
        )
        inv.set_footer(text=f"Order ID: {self.order_id}")
        await db_update_order(self.order_id, {
            "invoiceSentAt": datetime.utcnow().isoformat(),
            "status":        "awaiting_confirmation",
        })

        inv_view = PaymentInvoiceView(self.order_id)
        if pay_link:
            inv_view.add_item(discord.ui.Button(
                label="🌐 Open Payment Page",
                style=discord.ButtonStyle.link,
                url=pay_link,
                row=1,
            ))
        await interaction.channel.send(embed=inv, view=inv_view)

        channel_id = order.get("channelId")
        channel    = interaction.guild.get_channel(int(channel_id)) if channel_id else interaction.channel
        asyncio.create_task(poll_apirone_payment(self.order_id, channel, interaction.user))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order or order["userId"] != str(interaction.user.id):
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        await db_update_order(self.order_id, {"status": "cancelled"})
        await send_log(discord.Embed(
            title="🚫  Order Cancelled",
            description=f"Order: `{self.order_id}` | User: {interaction.user.mention}",
            color=0xE74C3C,
        ))
        await interaction.response.edit_message(
            embed=discord.Embed(title="❌  Order Cancelled", color=0xE74C3C),
            view=None,
        )
        channel_id = order.get("channelId")
        ch = interaction.guild.get_channel(int(channel_id)) if channel_id else None
        if ch:
            asyncio.create_task(close_order_channel_cancel(ch, buyer=interaction.user))


# ── Payment waiting view (shown after invoice) ────────────────────────────────

class PaymentCheckView(discord.ui.View):
    """
    Shown in the order channel after the invoice is sent.
    Payment is now fully automatic via Plisio IPN webhook.
    Buyer just needs to send LTC — no TX ID submission required.
    """
    def __init__(self, order_id: str):
        super().__init__(timeout=PAYMENT_TIMEOUT_MIN * 60)
        self.order_id = order_id

    @discord.ui.button(label="🆘  Request Admin Support", style=discord.ButtonStyle.danger)
    async def contact_admin_btn(self, interaction: discord.Interaction, _b):
        await interaction.response.defer()
        ping = ""
        if ADMIN_ROLE_ID:
            role = interaction.guild.get_role(ADMIN_ROLE_ID)
            ping = role.mention if role else ""
        await interaction.channel.send(
            f"{ping} {interaction.user.mention} has requested admin assistance — Order `{self.order_id}`"
        )
        await send_log(discord.Embed(
            title="🆘  Admin Help Requested",
            description=(
                f"**User:** {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"**Order:** `{self.order_id}`\n"
                f"**Channel:** {interaction.channel.mention}"
            ),
            color=0xE74C3C,
        ))

    @discord.ui.button(label="❌ Cancel Order", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        channel_id = order.get("channelId") if order else None
        channel    = interaction.guild.get_channel(int(channel_id)) if channel_id else interaction.channel
        if order:
            await db_update_order(self.order_id, {"status": "cancelled"})
            await send_log(discord.Embed(
                title="❌  Order Cancelled",
                description=(
                    f"**Order:** `{self.order_id}`\n"
                    f"**User:** {interaction.user.mention}\n"
                    f"**Product:** {order.get('categoryName', 'N/A')}"
                ),
                color=0xE74C3C,
            ))
        await interaction.response.edit_message(
            embed=discord.Embed(title="❌  Order Cancelled", color=0xE74C3C), view=None
        )
        if channel:
            asyncio.create_task(close_order_channel_cancel(channel, buyer=interaction.user))


class PaymentInvoiceView(discord.ui.View):
    def __init__(self, order_id: str):
        super().__init__(timeout=PAYMENT_TIMEOUT_MIN * 60)
        self.order_id = order_id

    @discord.ui.button(label="📋  Paste Ltc Address", style=discord.ButtonStyle.primary)
    async def paste_details(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order:
            await interaction.response.send_message("No order was found with that ID. Please verify and try again.", ephemeral=True)
            return
        addr = order.get("ltcAddress") or LTC_WALLET_ADDRESS
        amt  = order["ltcAmount"]
        usd  = f"${order['totalUSD']:.2f}"
        await interaction.response.defer()
        # Send plain messages — easy to tap and copy on mobile
        await interaction.channel.send(addr)
        await interaction.channel.send(str(amt))
        await interaction.channel.send(usd)

    @discord.ui.button(label="◼️  QR Code", style=discord.ButtonStyle.secondary)
    async def qr_code(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order:
            await interaction.response.send_message("No order was found with that ID. Please verify and try again.", ephemeral=True)
            return
        addr     = order.get("ltcAddress") or ""
        pay_link = order.get("payLink") or ""
        amt      = order["ltcAmount"]
        qr_data  = f"litecoin:{addr}?amount={amt}" if addr else pay_link
        qr       = f"https://api.qrserver.com/v1/create-qr-code/?size=250x250&data={qr_data}"
        emb = discord.Embed(
            title="Scan to Pay",
            description="Scan with your Litecoin wallet app.",
            color=0x1A1A2E,
        )
        emb.set_author(
            name="Litecoin QR Code",
            icon_url="https://cryptologos.cc/logos/litecoin-ltc-logo.png",
        )
        emb.set_thumbnail(url=INFO_IMAGE_URL)
        emb.set_image(url=qr)
        emb.add_field(name="• Amount (LTC)", value=f"```\n{amt}\n```", inline=False)
        emb.set_footer(text="This address is unique to your order — do not share it.")
        await interaction.response.send_message(embed=emb, ephemeral=True)

    @discord.ui.button(label="🚫  Cancel Ticket", style=discord.ButtonStyle.danger)
    async def cancel_ticket(self, interaction: discord.Interaction, _b):
        order = await db_get_order(self.order_id)
        if not order or order["userId"] != str(interaction.user.id):
            await interaction.response.send_message("This order does not belong to your account.", ephemeral=True)
            return
        await db_update_order(self.order_id, {"status": "cancelled"})
        await send_log(discord.Embed(
            title="🚫  Order Cancelled",
            description=f"Order: `{self.order_id}` | User: {interaction.user.mention}",
            color=0xE74C3C,
        ))
        await interaction.response.edit_message(
            embed=discord.Embed(title="Ticket Cancelled", color=0xE74C3C),
            view=None,
        )
        channel_id = order.get("channelId")
        ch = interaction.guild.get_channel(int(channel_id)) if channel_id else None
        if ch:
            asyncio.create_task(close_order_channel_cancel(ch, buyer=interaction.user))


# ═══════════════════════════════════════════════════════════════════════════════
#  BOT + COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True

def get_prefix(bot, message):
    return ["&", "/"]

bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)


def is_admin(ctx: commands.Context) -> bool:
    if ADMIN_ROLE_ID == 0:
        return ctx.author.guild_permissions.administrator
    role = ctx.guild.get_role(ADMIN_ROLE_ID)
    return (role in ctx.author.roles) if role else False


bot_start_time = None

@bot.event
async def on_ready():
    global bot_start_time, LTC_WALLET_ADDRESS, PAYMENT_TIMEOUT_MIN
    bot_start_time = datetime.utcnow()
    bot.add_view(PanelView())  # persistent — survives bot restart
    bot.add_view(OrderFinalizedView())  # persistent — Order Finalized card buttons
    # Load any DB overrides for wallet address and timeout
    addr_override = await get_setting("ltc_wallet_override")
    if addr_override:
        LTC_WALLET_ADDRESS = addr_override
    timeout_override = await get_setting("payment_timeout")
    if timeout_override and timeout_override.isdigit():
        PAYMENT_TIMEOUT_MIN = int(timeout_override)
    # Register PanelView so button interactions work after restart
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name="&help | LTC Shop")
    )
    print(f"✅  {bot.user} online  |  DB: {DB_NAME}  |  Dev mode: {LTC_DEV_MODE}")
    # Ensure group slugs are unique at the DB layer (guards against race conditions
    # between concurrent &creategroup/&renamegroup calls)
    try:
        await col_groups.create_index("slug", unique=True)
    except Exception as e:
        print(f"[startup] Could not ensure unique index on groups.slug: {e}")
    # Auto-resume any in-progress payments from before the restart
    for guild in bot.guilds:
        asyncio.create_task(auto_resume_on_ready(guild))


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f"❌ Missing: `{error.param.name}`. Use `&help` for usage.")
        return
    print(f"[error] {error}")


# ─────────────────────────────────────────────────────────────────────────────
#  &panel
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="panel")
async def cmd_panel(ctx: commands.Context):
    """Send the shop panel with live stats and product dropdown. Admin only."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    embed     = await build_panel_embed()
    panel_msg = await ctx.channel.send(embed=embed, view=PanelView())
    await set_setting("panel_message_id", str(panel_msg.id))
    await set_setting("panel_channel_id",  str(ctx.channel.id))
    try:
        await ctx.message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  &updatepanel <message_id>
#  Refresh the panel embed stats (stock counts etc.) in-place
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="updatepanel")
async def cmd_updatepanel(ctx: commands.Context, message_id: int = None):
    """Admin: Refresh panel embed stats in-place."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    try:
        if message_id:
            msg = await ctx.channel.fetch_message(message_id)
        else:
            msg = None
            async for m in ctx.channel.history(limit=50):
                if m.author == bot.user and m.embeds:
                    msg = m
                    break
            if not msg:
                await ctx.reply("❌ Couldn't find a panel message in this channel.")
                return
        await msg.edit(embed=await build_panel_embed(), view=PanelView())
        await set_setting("panel_message_id", str(msg.id))
        await set_setting("panel_channel_id",  str(ctx.channel.id))
        await ctx.reply("✅ Panel updated.", delete_after=5)
        try:
            await ctx.message.delete()
        except Exception:
            pass
    except discord.NotFound:
        await ctx.reply("❌ Message not found.")
    except Exception as e:
        await ctx.reply(f"❌ Failed to update: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  &shopname <name>   — set the shop display name
#  &shopbanner <url>  — set the panel banner image URL
#  &shopicon <url>    — set the product card thumbnail icon URL
# ─────────────────────────────────────────────────────────────────────────────

@bot.command(name="shopname")
async def cmd_shopname(ctx: commands.Context, *, name: str):
    """
    Admin: Set the shop name shown in the panel header and footer.

    Usage:
      &shopname Blinkit Mart
      &shopname My AutoBuy Store
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if len(name) > 50:
        await ctx.reply("❌ Shop name must be 50 characters or fewer.")
        return

    old_name = await get_setting("shop_name")
    await set_setting("shop_name", name)

    embed = discord.Embed(title="✅  Shop Name Updated", color=0x7B2FBE)
    embed.add_field(name="Old Name", value=old_name or "*(not set)*", inline=True)
    embed.add_field(name="New Name", value=name,                      inline=True)
    embed.set_footer(text="Run &updatepanel to refresh the panel.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="⚙️  Shop Name Changed",
        description=f"**{old_name}** → **{name}** by {ctx.author.mention}",
        color=0x9B59B6,
    ))


@bot.command(name="shopbanner")
async def cmd_shopbanner(ctx: commands.Context, *, url: str = None):
    """
    Admin: Set (or clear) the panel banner image.

    Usage:
      &shopbanner https://i.imgur.com/yourimage.png   — set banner
      &shopbanner clear                                — remove banner
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    # Allow clearing
    if not url or url.lower() == "clear":
        await set_setting("banner_url", "")
        await ctx.reply("✅ Banner cleared. Run `&updatepanel` to refresh the panel.")
        return

    # Basic URL check
    if not (url.startswith("http://") or url.startswith("https://")):
        await ctx.reply("❌ Please provide a valid image URL starting with `https://`.")
        return

    await set_setting("banner_url", url)

    embed = discord.Embed(title="✅  Shop Banner Updated", color=0x7B2FBE)
    embed.set_image(url=url)
    embed.set_footer(text="Run &updatepanel to refresh the panel.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="⚙️  Banner Changed",
        description=f"New banner set by {ctx.author.mention}\n{url}",
        color=0x9B59B6,
    ))


@bot.command(name="shopicon")
async def cmd_shopicon(ctx: commands.Context, *, url: str = None):
    """
    Admin: Set (or clear) the shop icon shown as thumbnail on product cards.

    Usage:
      &shopicon https://i.imgur.com/youricon.png   — set icon
      &shopicon clear                               — remove icon
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if not url or url.lower() == "clear":
        await set_setting("icon_url", "")
        await ctx.reply("✅ Shop icon cleared.")
        return

    if not (url.startswith("http://") or url.startswith("https://")):
        await ctx.reply("❌ Please provide a valid image URL starting with `https://`.")
        return

    await set_setting("icon_url", url)

    embed = discord.Embed(title="✅  Shop Icon Updated", color=0x7B2FBE)
    embed.set_thumbnail(url=url)
    embed.set_footer(text="Changes appear instantly on new product cards.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="⚙️  Icon Changed",
        description=f"New icon set by {ctx.author.mention}\n{url}",
        color=0x9B59B6,
    ))


@bot.command(name="shopsettings")
async def cmd_shopsettings(ctx: commands.Context):
    """Admin: View all current shop display settings."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    shop_name    = await get_setting("shop_name")
    banner_url   = await get_setting("banner_url")
    icon_url     = await get_setting("icon_url")
    order_banner = await get_setting("order_banner_url")

    embed = discord.Embed(title="⚙️  Shop Settings", color=0x9B59B6)
    embed.add_field(name="🏪 Shop Name",      value=shop_name or "*(default)*",                       inline=False)
    embed.add_field(name="🖼️ Panel Banner",   value=banner_url or "*(not set)*",                     inline=False)
    embed.add_field(name="🔷 Shop Icon",      value=icon_url or "*(not set)*",                        inline=False)
    embed.add_field(name="✅ Order Banner",   value=order_banner or "*(not set — no image on orders)*", inline=False)
    embed.set_footer(text="&shopname / &shopbanner / &shopicon / &orderbanner to change")

    if order_banner:
        embed.set_image(url=order_banner)
    elif banner_url:
        embed.set_image(url=banner_url)
    if icon_url:
        embed.set_thumbnail(url=icon_url)

    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &orderbanner <url | clear>
#  Set the banner image shown on Order Completed messages in the sales channel
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="orderbanner")
async def cmd_orderbanner(ctx: commands.Context, *, url: str = None):
    """
    Admin: Set the banner image shown on Order Completed cards in the sales channel.

    Usage:
      &orderbanner https://i.imgur.com/yourimage.png   — set banner
      &orderbanner clear                                — remove banner
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if not url or url.lower() == "clear":
        await set_setting("order_banner_url", "")
        await ctx.reply("✅ Order completed banner cleared.")
        return

    if not (url.startswith("http://") or url.startswith("https://")):
        await ctx.reply("❌ Please provide a valid image URL starting with `https://`.")
        return

    await set_setting("order_banner_url", url)

    embed = discord.Embed(
        title="✅  Order Banner Updated",
        description="This image will appear at the bottom of every Order Completed card.",
        color=0x7B2FBE,
    )
    embed.set_image(url=url)
    embed.set_footer(text="Takes effect immediately on the next completed order.")
    await ctx.reply(embed=embed)

    await send_log(discord.Embed(
        title="⚙️  Order Banner Changed",
        description=f"New order banner set by {ctx.author.mention}\n{url}",
        color=0x9B59B6,
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  &stock [category]
#  Shows all categories, or a single category if name is given
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="stock")
async def cmd_stock(ctx: commands.Context, *, category: str = None):
    """
    &stock              → list all categories with stock counts
    &stock <name>       → show one specific category's stock
    """
    if category:
        cat = await db_find_category(category)
        if not cat:
            await ctx.reply(f"❌ No category matching `{category}`. Use `&stock` to list all.")
            return

        count = len(cat.get("stock", []))
        badge = ("🔴 Out of Stock"         if count == 0 else
                 f"🟡 Low — {count} left"  if count <= 3 else
                 f"🟢 {count} in stock")

        embed = discord.Embed(title=f"📦  {cat['name']}", color=0x7B2FBE)
        embed.add_field(name="💵 Price", value=f"${cat['price_usd']:.2f} USD", inline=True)
        embed.add_field(name="📊 Stock", value=badge,                          inline=True)
        if cat.get("description"):
            embed.add_field(name="📝 Description", value=cat["description"],   inline=False)
        if cat.get("instruction"):
            embed.add_field(name="📌 Instruction", value=cat["instruction"],   inline=False)
        embed.set_footer(text=f"Slug: {cat['slug']}  •  Use &restock {cat['slug']} to add stock")
        await ctx.reply(embed=embed)

    else:
        cats = await db_get_categories()
        if not cats:
            await ctx.reply("❌ No categories yet. Use `&category <n> <price>` to create one.")
            return

        embed = discord.Embed(title="📦  All Stock", color=0x7B2FBE)
        embed.set_footer(text="&stock <name> for details  •  &panel to open the shop")

        for c in cats:
            is_infinite = bool(c.get("infinite_stock"))
            count = len(c.get("stock", [])) if not is_infinite else 9999
            if is_infinite:
                badge = "♾️ Infinite (unlimited)"
            elif count == 0:
                badge = "🔴 Out of Stock"
            elif count <= 3:
                badge = f"🟡 {count} left (low!)"
            else:
                badge = f"🟢 {count} in stock"
            embed.add_field(
                name=c["name"],
                value=f"💵 ${c['price_usd']:.2f} USD  •  {badge}",
                inline=False,
            )
        await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &category <name> <price> [instruction]
#  Creates a new product category
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="category")
async def cmd_category(ctx: commands.Context, name: str, price: str, *, instruction: str = ""):
    """
    Admin: Create a new product category.

    Usage
    ─────
    &category "Netflix 1 Month" 5.99
    &category Spotify 3.99 Login at spotify.com with the credentials below.

    name        — wrap in quotes if it has spaces
    price       — USD price per unit
    instruction — optional text shown to buyer after they receive their items
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    try:
        price_val = float(price)
        if price_val <= 0:
            raise ValueError
    except ValueError:
        await ctx.reply('❌ Invalid price. Usage: `&category "Name" 5.99 [instruction]`')
        return

    slug = slugify(name)
    if await db_get_category(slug):
        await ctx.reply(
            f"❌ Category **{name}** already exists.\n"
            f"Use `&restock {slug}` (with a .txt attachment) to add stock."
        )
        return

    cat = await db_create_category(name=name, price=price_val, instruction=instruction)

    embed = discord.Embed(title="✅  Category Created", color=0x7B2FBE)
    embed.add_field(name="Name",  value=cat["name"],                  inline=True)
    embed.add_field(name="Slug",  value=f"`{cat['slug']}`",           inline=True)
    embed.add_field(name="Price", value=f"${cat['price_usd']:.2f} USD", inline=True)
    if instruction:
        embed.add_field(name="📌 Instruction", value=instruction,     inline=False)
    embed.set_footer(text=f"Next step: &restock {cat['slug']}  (attach a .txt file with one item per line)")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &restock <category>  [attach a .txt file]
#  Reads the attached file line-by-line and pushes each line as a stock item
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="restock")
async def cmd_restock(ctx: commands.Context, *, category: str):
    """
    Admin: Bulk-add stock from an attached .txt file.
    One stock item per line in the file.

    Usage
    ─────
    &restock netflix-1-month        ← slug
    &restock "Netflix 1 Month"      ← name also works

    File format (stock.txt)
    ───────────────────────
    user1@email.com:password1
    user2@email.com:password2
    GIFT-KEY-XXXX-YYYY
    https://gift-link.example/abc
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(
            f"❌ No category matching `{category}`.\n"
            f"Use `&stock` to list all, or `&category` to create one."
        )
        return

    if not ctx.message.attachments:
        await ctx.reply(
            f"❌ No file attached.\n\n"
            f"Attach a `.txt` file with **one item per line**, then run:\n"
            f"```&restock {cat['slug']}```"
        )
        return

    attachment = ctx.message.attachments[0]

    if not attachment.filename.lower().endswith(".txt"):
        await ctx.reply("❌ Only `.txt` files are accepted.")
        return

    if attachment.size > 5_000_000:   # 5 MB cap
        await ctx.reply("❌ File too large (max 5 MB).")
        return

    try:
        raw   = await attachment.read()
        text  = raw.decode("utf-8", errors="replace")
    except Exception as e:
        await ctx.reply(f"❌ Could not read file: {e}")
        return

    all_lines  = [line.strip() for line in text.splitlines()]
    items      = [l for l in all_lines if l]   # drop blank lines

    if not items:
        await ctx.reply("❌ The file is empty or has only blank lines.")
        return

    added   = await db_restock(cat["slug"], items)
    updated = await db_get_category(cat["slug"])
    total   = len(updated.get("stock", []))
    skipped = len(items) - added

    embed = discord.Embed(title="📦  Restock Complete", color=0x7B2FBE)
    embed.add_field(name="Category",       value=cat["name"],        inline=True)
    embed.add_field(name="File",           value=attachment.filename, inline=True)
    embed.add_field(name="\u200b",         value="\u200b",           inline=True)
    embed.add_field(name="📄 Lines in file", value=str(len(items)),  inline=True)
    embed.add_field(name="✅ Added",        value=str(added),         inline=True)
    embed.add_field(name="⏭️ Skipped",      value=f"{skipped} (duplicates)", inline=True)
    embed.add_field(name="📊 New Total",    value=f"**{total}** in stock", inline=False)
    embed.set_footer(text=f"Slug: {cat['slug']}")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &orders
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="orders")
async def cmd_orders(ctx: commands.Context, limit: int = 10):
    """(Admin) View recent orders. &orders 25 shows last 25."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    limit  = max(1, min(limit, 50))  # clamp 1-50
    recent = await db_recent_orders(limit)
    if not recent:
        await ctx.reply("No orders yet.")
        return
    icons = {"pending": "🟡", "awaiting_confirmation": "🔵", "delivered": "🟢",
             "cancelled": "🔴", "expired": "⚫", "error": "🟠"}
    embed = discord.Embed(title="📋  Recent Orders (Last 10)", color=0x9B59B6)
    for o in recent:
        embed.add_field(
            name=f"{icons.get(o['status'], '⚪')} {o['orderId']}",
            value=(
                f"<@{o['userId']}> • {o['categoryName']}\n"
                f"Qty: {o['quantity']} • {o['ltcAmount']} LTC • **{o['status']}**"
            ),
            inline=False,
        )
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &deleteorder
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="deleteorder")
async def cmd_deleteorder(ctx: commands.Context):
    """
    Admin: Delete completed order channels + channels inactive for 1+ hour.
    Leaves channels with active orders (pending / awaiting_confirmation) untouched.
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    # ── Confirmation step ─────────────────────────────────────────────────────
    confirm_embed = discord.Embed(
        title="⚠️  Confirm Bulk Delete",
        description=(
            "This will delete **all completed/inactive order channels**.\n"
            "Active orders are safe. Type `confirm` to proceed or `cancel` to abort."
        ),
        color=0xA855F7,
    )
    await ctx.reply(embed=confirm_embed)

    def _check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ("confirm", "cancel")

    try:
        reply = await bot.wait_for("message", check=_check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.reply("⏰ Timed out — operation cancelled.")
        return

    if reply.content.lower() == "cancel":
        await ctx.reply("✅ Cancelled.")
        return

    status_msg = await ctx.reply("🔍 Scanning order channels...")

    if not ORDER_CATEGORY_ID:
        await status_msg.edit(content="❌ `ORDER_CATEGORY_ID` env var not set.")
        return

    category = ctx.guild.get_channel(ORDER_CATEGORY_ID)
    if not category or not isinstance(category, discord.CategoryChannel):
        await status_msg.edit(content="❌ Order category not found. Check `ORDER_CATEGORY_ID`.")
        return

    deleted = []
    skipped = []
    now     = datetime.utcnow()

    for ch in category.channels:
        if not isinstance(ch, discord.TextChannel):
            continue


        # ── Safety guard: only touch channels the bot created ─────────────────
        # Bot creates channels as 'order-<id>-<name>' and renames to 'closed-<id>-<name>'.
        # Anything else is silently ignored — no need to show in the report.
        if not (ch.name.startswith("order-") or ch.name.startswith("closed-")):
            continue  # silent skip — not a bot-created channel

        order = await col_orders.find_one({"channelId": str(ch.id)}, {"_id": 0})

        if order:
            status = order.get("status", "")
            if status in ("pending", "awaiting_confirmation"):
                skipped.append(f"🔵 #{ch.name} — active ({status})")
                continue
            reason = f"Order {status}"
        else:
            try:
                last_msg = None
                async for m in ch.history(limit=1):
                    last_msg = m
                if last_msg:
                    age = (now - last_msg.created_at.replace(tzinfo=None)).total_seconds()
                    if age < 3600:
                        skipped.append(f"⏳ #{ch.name} — active {int(age//60)}m ago")
                        continue
                reason = "No order + inactive 1h+"
            except Exception:
                skipped.append(f"⚠️ #{ch.name} — could not check")
                continue

        try:
            await ch.delete(reason=f"&deleteorder — {reason}")
            deleted.append(f"🗑️ #{ch.name}")
        except Exception as e:
            skipped.append(f"⚠️ #{ch.name} — delete failed: {e}")

    embed = discord.Embed(title="🗑️  Order Channel Cleanup", color=0xE74C3C)
    embed.add_field(
        name=f"✅ Deleted ({len(deleted)})",
        value=("\n".join(deleted[:20]) + ("\n..." if len(deleted) > 20 else "")) or "Nothing to delete.",
        inline=False,
    )
    if skipped:
        embed.add_field(
            name=f"⏭️ Skipped ({len(skipped)})",
            value="\n".join(skipped[:10]) + ("\n..." if len(skipped) > 10 else ""),
            inline=False,
        )
    embed.set_footer(text=f"Scanned {len(category.channels)} channels.")
    await status_msg.edit(content=None, embed=embed)
    await send_log(discord.Embed(
        title="🗑️  Order Channel Cleanup",
        description=f"{len(deleted)} channels deleted by {ctx.author.mention}",
        color=0xE74C3C,
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  &categoryrename <old name> | <new name>
#  Renames a category. Slug is also updated.
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="categoryrename")
async def cmd_categoryrename(ctx: commands.Context, *, args: str):
    """
    Admin: Rename a category.

    Usage:
      &categoryrename <current name> | <new name>

    Example:
      &categoryrename Netflix 1 Month | Netflix Premium 1M
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if "|" not in args:
        await ctx.reply('❌ Separate old and new names with `|`\nExample: `&categoryrename Netflix 1 Month | Netflix Premium 1M`')
        return

    parts    = args.split("|", 1)
    old_name = parts[0].strip()
    new_name = parts[1].strip()

    if not old_name or not new_name:
        await ctx.reply("❌ Both old name and new name are required.")
        return

    cat = await db_find_category(old_name)
    if not cat:
        await ctx.reply(f"❌ No category matching `{old_name}`. Use `&stock` to list all.")
        return

    new_slug = slugify(new_name)

    # Make sure the new slug isn't already taken by a different category
    existing = await db_get_category(new_slug)
    if existing and existing["slug"] != cat["slug"]:
        await ctx.reply(f"❌ A category named **{new_name}** already exists.")
        return

    old_slug = cat["slug"]
    old_name_display = cat["name"]

    # Update category
    await col_cats.update_one(
        {"slug": old_slug},
        {"$set": {"name": new_name, "slug": new_slug}}
    )

    # Update all orders that reference the old slug so history stays consistent
    await col_orders.update_many(
        {"categorySlug": old_slug},
        {"$set": {"categorySlug": new_slug, "categoryName": new_name}}
    )

    await send_log(discord.Embed(
        title="✏️  Category Renamed",
        description=f"**{old_name_display}** → **{new_name}**\nSlug: `{old_slug}` → `{new_slug}`",
        color=0x9B59B6,
    ))

    embed = discord.Embed(title="✅  Category Renamed", color=0x7B2FBE)
    embed.add_field(name="Old Name", value=old_name_display,        inline=True)
    embed.add_field(name="New Name", value=new_name,                inline=True)
    embed.add_field(name="New Slug", value=f"`{new_slug}`",         inline=True)
    embed.set_footer(text="All existing orders updated to match the new name.")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &removecategory <name>  (alias: &deleteproduct)
#  Deletes a category/product and all its remaining stock, after confirmation.
# ─────────────────────────────────────────────────────────────────────────────
async def _confirm_and_delete_product(ctx: commands.Context, name: str):
    """Shared confirm-then-delete flow used by &removecategory and &deleteproduct."""
    cat = await db_find_category(name)
    if not cat:
        await ctx.reply(f"❌ No product matching `{name}`. Use `&stock` to list all.")
        return

    stock_count = len(cat.get("stock", []))

    # ── Confirmation step ─────────────────────────────────────────────────────
    confirm_embed = discord.Embed(
        title="⚠️  Confirm Deletion",
        description=(
            f"You are about to **permanently delete** product **{cat['name']}** "
            f"and all **{stock_count}** stock items.\n\n"
            "This cannot be undone. Type `confirm` to proceed or `cancel` to abort."
        ),
        color=0xA855F7,
    )
    await ctx.reply(embed=confirm_embed)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ("confirm", "cancel")

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.reply("⏰ Timed out — deletion cancelled.")
        return

    if reply.content.lower() == "cancel":
        await ctx.reply("✅ Deletion cancelled.")
        return

    await db_delete_category(cat["slug"])
    await send_log(discord.Embed(
        title="🗑️  Product Removed",
        description=f"**{cat['name']}** (`{cat['slug']}`) deleted by {ctx.author.mention} — {stock_count} stock items discarded.",
        color=0xE74C3C,
    ))

    embed = discord.Embed(title="🗑️  Product Removed", color=0xE74C3C)
    embed.add_field(name="Name",          value=cat["name"],         inline=True)
    embed.add_field(name="Slug",          value=f"`{cat['slug']}`",  inline=True)
    embed.add_field(name="Stock Deleted", value=str(stock_count),    inline=True)
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


@bot.command(name="removecategory")
async def cmd_removecategory(ctx: commands.Context, *, name: str):
    """
    Admin: Delete a category/product and all its remaining stock.

    Usage:
      &removecategory Netflix 1 Month
      &removecategory netflix-1-month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await _confirm_and_delete_product(ctx, name)


# ═══════════════════════════════════════════════════════════════════════════════
#  PRODUCT GROUPS  (folders like "Bot Src", "Tools", "Accounts")
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
#  &creategroup <name>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="creategroup")
async def cmd_creategroup(ctx: commands.Context, *, name: str):
    """
    Admin: Create a new product group (folder for organising products).

    Usage:
      &creategroup Bot Src
      &creategroup Accounts
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    name = name.strip()
    if not name:
        await ctx.reply("❌ Group name cannot be empty. Usage: `&creategroup <name>`")
        return

    slug = slugify(name)
    if await db_get_group(slug):
        await ctx.reply(f"❌ Group **{name}** already exists.")
        return

    grp = await db_create_group(name)
    if grp is None:
        await ctx.reply(f"❌ Group **{name}** already exists.")
        return
    embed = discord.Embed(title="✅  Group Created", color=0x7B2FBE)
    embed.add_field(name="Name", value=grp["name"],        inline=True)
    embed.add_field(name="Slug", value=f"`{grp['slug']}`", inline=True)
    embed.set_footer(text=f"Use &addtogroup <product> | {grp['name']} to add products to this group.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="🗂️  Group Created",
        description=f"**{grp['name']}** (`{grp['slug']}`) created by {ctx.author.mention}",
        color=0x9B59B6,
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  &renamegroup <old name> | <new name>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="renamegroup")
async def cmd_renamegroup(ctx: commands.Context, *, args: str):
    """
    Admin: Rename an existing product group.

    Usage:
      &renamegroup Tools | Utilities
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if "|" not in args:
        await ctx.reply('❌ Separate old and new names with `|`\nExample: `&renamegroup Tools | Utilities`')
        return

    old_name, new_name = (p.strip() for p in args.split("|", 1))
    if not old_name or not new_name:
        await ctx.reply("❌ Both old name and new name are required.")
        return

    grp = await db_find_group(old_name)
    if not grp:
        await ctx.reply(f"❌ No group matching `{old_name}`. Use `&groups` to list all.")
        return

    new_slug = slugify(new_name)
    existing = await db_get_group(new_slug)
    if existing and existing["slug"] != grp["slug"]:
        await ctx.reply(f"❌ A group named **{new_name}** already exists.")
        return

    old_slug_display = grp["slug"]
    new_slug          = await db_rename_group(grp["slug"], new_name)
    if new_slug is None:
        await ctx.reply(f"❌ A group named **{new_name}** already exists.")
        return

    embed = discord.Embed(title="✅  Group Renamed", color=0x7B2FBE)
    embed.add_field(name="Old Name", value=grp["name"], inline=True)
    embed.add_field(name="New Name", value=new_name,    inline=True)
    embed.add_field(name="New Slug", value=f"`{new_slug}`", inline=True)
    embed.set_footer(text="All products in this group have been updated automatically.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="✏️  Group Renamed",
        description=f"**{grp['name']}** → **{new_name}**\nSlug: `{old_slug_display}` → `{new_slug}`\nBy {ctx.author.mention}",
        color=0x9B59B6,
    ))
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &deletegroup <name>
#  Deletes the group only — products inside are moved to Ungrouped, never deleted.
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="deletegroup")
async def cmd_deletegroup(ctx: commands.Context, *, name: str):
    """
    Admin: Delete a product group. Products inside are moved to Ungrouped
    (they are never deleted).

    Usage:
      &deletegroup Tools
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    grp = await db_find_group(name)
    if not grp:
        await ctx.reply(f"❌ No group matching `{name}`. Use `&groups` to list all.")
        return

    products = await db_categories_in_group(grp["slug"])

    confirm_embed = discord.Embed(
        title="⚠️  Confirm Group Deletion",
        description=(
            f"You are about to delete group **{grp['name']}**.\n"
            f"**{len(products)}** product(s) inside will be moved to *Ungrouped* — they will **not** be deleted.\n\n"
            "Type `confirm` to proceed or `cancel` to abort."
        ),
        color=0xA855F7,
    )
    await ctx.reply(embed=confirm_embed)

    def check(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ("confirm", "cancel")

    try:
        reply = await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.reply("⏰ Timed out — deletion cancelled.")
        return
    if reply.content.lower() == "cancel":
        await ctx.reply("✅ Deletion cancelled.")
        return

    await col_cats.update_many({"group": grp["slug"]}, {"$set": {"group": None}})
    await db_delete_group(grp["slug"])

    await send_log(discord.Embed(
        title="🗑️  Group Deleted",
        description=f"**{grp['name']}** (`{grp['slug']}`) deleted by {ctx.author.mention} — {len(products)} product(s) moved to Ungrouped.",
        color=0xE74C3C,
    ))

    embed = discord.Embed(title="🗑️  Group Deleted", color=0xE74C3C)
    embed.add_field(name="Name",           value=grp["name"],              inline=True)
    embed.add_field(name="Products Moved", value=f"{len(products)} → Ungrouped", inline=True)
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &groups  — list all groups with product counts
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="groups")
async def cmd_groups(ctx: commands.Context):
    """List all product groups with how many products are inside each one."""
    groups    = await db_get_groups()
    ungrouped = await db_categories_in_group(None)

    if not groups and not ungrouped:
        await ctx.reply("❌ No groups or products yet. Use `&creategroup <name>` to start.")
        return

    embed = discord.Embed(title="🗂️  Product Groups", color=0x9B59B6)
    if not groups:
        embed.description = "No groups created yet. Use `&creategroup <name>` to create one."
    for g in groups:
        products = await db_categories_in_group(g["slug"])
        embed.add_field(
            name=f"📁 {g['name']}",
            value=f"`{g['slug']}` • {len(products)} product(s)",
            inline=False,
        )
    if ungrouped:
        embed.add_field(name="📂 Ungrouped", value=f"{len(ungrouped)} product(s)", inline=False)
    embed.set_footer(text="&groupproducts <group> to view products  •  &creategroup <name> to add a group")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &groupproducts <group>  — list products inside one group ("ungrouped" works too)
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="groupproducts")
async def cmd_groupproducts(ctx: commands.Context, *, group: str):
    """
    List all products inside a group.

    Usage:
      &groupproducts Accounts
      &groupproducts ungrouped
    """
    if group.strip().lower() == "ungrouped":
        products = await db_categories_in_group(None)
        title    = "📂  Ungrouped Products"
    else:
        grp = await db_find_group(group)
        if not grp:
            await ctx.reply(f"❌ No group matching `{group}`. Use `&groups` to list all.")
            return
        products = await db_categories_in_group(grp["slug"])
        title    = f"📁  {grp['name']}"

    if not products:
        await ctx.reply(f"ℹ️ No products in **{title.split(chr(32), 1)[-1].strip()}** yet.")
        return

    embed = discord.Embed(title=title, color=0x7B2FBE)
    for p in products:
        count = "♾️ Infinite" if p.get("infinite_stock") else str(len(p.get("stock", [])))
        embed.add_field(name=p["name"], value=f"💵 ${p['price_usd']:.2f}  •  📦 {count}  •  `{p['slug']}`", inline=False)
    embed.set_footer(text=f"{len(products)} product(s)")
    await ctx.reply(embed=embed)


async def _move_product_to_group(ctx: commands.Context, product_query: str, group_query: str):
    """Shared logic for &addtogroup and &moveproduct."""
    cat = await db_find_category(product_query)
    if not cat:
        await ctx.reply(f"❌ No product matching `{product_query}`. Use `&stock` to list all.")
        return

    grp = await db_find_group(group_query)
    if not grp:
        await ctx.reply(
            f"❌ No group matching `{group_query}`. Use `&groups` to list all, "
            f"or `&creategroup {group_query}` to create it first."
        )
        return

    old_group_doc = await db_get_group(cat.get("group"))
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"group": grp["slug"]}})

    embed = discord.Embed(title="✅  Product Moved", color=0x7B2FBE)
    embed.add_field(name="Product", value=cat["name"],                                      inline=True)
    embed.add_field(name="From",    value=old_group_doc["name"] if old_group_doc else "Ungrouped", inline=True)
    embed.add_field(name="To",      value=grp["name"],                                       inline=True)
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="📦  Product Moved Between Groups",
        description=f"**{cat['name']}** → **{grp['name']}**  (by {ctx.author.mention})",
        color=0x9B59B6,
    ))
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &addtogroup <product> | <group>  (alias: &moveproduct)
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="addtogroup")
async def cmd_addtogroup(ctx: commands.Context, *, args: str):
    """
    Admin: Add (or move) a product into a group.

    Usage:
      &addtogroup Netflix 1 Month | Accounts
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if "|" not in args:
        await ctx.reply('❌ Separate product and group with `|`\nExample: `&addtogroup Netflix 1 Month | Accounts`')
        return
    product_q, group_q = (p.strip() for p in args.split("|", 1))
    if not product_q or not group_q:
        await ctx.reply("❌ Both product and group are required.")
        return
    await _move_product_to_group(ctx, product_q, group_q)


# ─────────────────────────────────────────────────────────────────────────────
#  &moveproduct <product> | <group>  — same as &addtogroup, explicit naming
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="moveproduct")
async def cmd_moveproduct(ctx: commands.Context, *, args: str):
    """
    Admin: Move a product to a different group.

    Usage:
      &moveproduct Netflix 1 Month | Bot Src
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if "|" not in args:
        await ctx.reply('❌ Separate product and group with `|`\nExample: `&moveproduct Netflix 1 Month | Bot Src`')
        return
    product_q, group_q = (p.strip() for p in args.split("|", 1))
    if not product_q or not group_q:
        await ctx.reply("❌ Both product and group are required.")
        return
    await _move_product_to_group(ctx, product_q, group_q)


# ─────────────────────────────────────────────────────────────────────────────
#  &removefromgroup <product>  — unassign a product, moves it to Ungrouped
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="removefromgroup")
async def cmd_removefromgroup(ctx: commands.Context, *, product: str):
    """
    Admin: Remove a product from its group (moves it to Ungrouped).

    Usage:
      &removefromgroup Netflix 1 Month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(product)
    if not cat:
        await ctx.reply(f"❌ No product matching `{product}`. Use `&stock` to list all.")
        return
    if not cat.get("group"):
        await ctx.reply(f"ℹ️ **{cat['name']}** is already Ungrouped.")
        return

    old_group_doc = await db_get_group(cat["group"])
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"group": None}})
    await ctx.reply(embed=discord.Embed(
        title="✅  Removed From Group",
        description=f"**{cat['name']}** is now **Ungrouped** (was in **{old_group_doc['name'] if old_group_doc else 'Unknown'}**).",
        color=0x7B2FBE,
    ))
    await send_log(discord.Embed(
        title="📦  Product Removed From Group",
        description=f"**{cat['name']}** removed from **{old_group_doc['name'] if old_group_doc else 'Unknown'}** by {ctx.author.mention}",
        color=0x9B59B6,
    ))
    asyncio.create_task(auto_update_panel())


# ═══════════════════════════════════════════════════════════════════════════════
#  ADDITIONAL PRODUCT MANAGEMENT COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
#  &searchproduct <query>  — search products by name/description
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="searchproduct")
async def cmd_searchproduct(ctx: commands.Context, *, query: str):
    """
    Search for products by name or description.

    Usage:
      &searchproduct netflix
    """
    query_l = query.strip().lower()
    if not query_l:
        await ctx.reply("❌ Enter a search term. Usage: `&searchproduct netflix`")
        return

    cats = await db_get_categories()
    matches = [
        c for c in cats
        if query_l in c["name"].lower() or query_l in (c.get("description") or "").lower()
    ]
    if not matches:
        await ctx.reply(f"❌ No products matching `{query}`.")
        return

    embed = discord.Embed(title=f"🔎  Search Results — \"{query}\"", color=0x9B59B6)
    for c in matches[:20]:
        count = "♾️ Infinite" if c.get("infinite_stock") else str(len(c.get("stock", [])))
        embed.add_field(
            name=c["name"],
            value=f"💵 ${c['price_usd']:.2f}  •  📦 {count}  •  `{c['slug']}`",
            inline=False,
        )
    footer = f"{len(matches)} match(es)"
    if len(matches) > 20:
        footer += " — showing first 20"
    embed.set_footer(text=footer)
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &productinfo <product>  — full detail card for one product
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="productinfo")
async def cmd_productinfo(ctx: commands.Context, *, product: str):
    """
    View full details for a single product: group, price, stock, min quantity,
    custom LTC destination, description, instructions and ToC status.

    Usage:
      &productinfo Netflix 1 Month
    """
    cat = await db_find_category(product)
    if not cat:
        await ctx.reply(f"❌ No product matching `{product}`. Use `&stock` to list all.")
        return

    grp          = await db_get_group(cat.get("group"))
    is_infinite  = bool(cat.get("infinite_stock"))
    stock_display = "♾️ Infinite" if is_infinite else str(len(cat.get("stock", [])))
    toc          = await get_toc(cat["slug"])

    embed = discord.Embed(title=f"ℹ️  {cat['name']}", color=0x7B2FBE)
    embed.add_field(name="Slug",     value=f"`{cat['slug']}`",                inline=True)
    embed.add_field(name="Group",    value=grp["name"] if grp else "Ungrouped", inline=True)
    embed.add_field(name="Price",    value=f"${cat['price_usd']:.2f} USD",     inline=True)
    embed.add_field(name="Stock",    value=stock_display,                     inline=True)
    embed.add_field(name="Min Qty",  value=str(cat.get("min_quantity", 1)),    inline=True)
    embed.add_field(
        name="Custom LTC Dest",
        value=f"`{cat['custom_ltc_dest']}`" if cat.get("custom_ltc_dest") else "None (uses master wallet)",
        inline=True,
    )
    if cat.get("description"):
        embed.add_field(name="Description", value=cat["description"], inline=False)
    if cat.get("instruction"):
        embed.add_field(name="Post-Delivery Instructions", value=cat["instruction"], inline=False)
    embed.add_field(name="Terms & Conditions", value="✅ Set" if toc else "❌ Not set", inline=True)
    embed.set_footer(text=f"Created: {cat.get('createdAt', 'unknown')[:10]}")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &editproduct <product> | <field> | <value>
#  Fields: name, price, description, instruction, minqty
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="editproduct")
async def cmd_editproduct(ctx: commands.Context, *, args: str):
    """
    Admin: Edit a product's name, price, description, instruction or minimum quantity.

    Usage:
      &editproduct <product> | <field> | <new value>
      Fields: name, price, description, instruction, minqty

    Example:
      &editproduct Netflix 1 Month | price | 6.99
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    parts = [p.strip() for p in args.split("|")]
    if len(parts) != 3 or not all(parts):
        await ctx.reply(
            '❌ Usage: `&editproduct <product> | <field> | <new value>`\n'
            'Fields: `name`, `price`, `description`, `instruction`, `minqty`'
        )
        return

    product_q, field, value = parts
    field = field.lower()

    cat = await db_find_category(product_q)
    if not cat:
        await ctx.reply(f"❌ No product matching `{product_q}`. Use `&stock` to list all.")
        return

    if field == "name":
        new_slug = slugify(value)
        existing = await db_get_category(new_slug)
        if existing and existing["slug"] != cat["slug"]:
            await ctx.reply(f"❌ A product named **{value}** already exists.")
            return
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"name": value, "slug": new_slug}})
        await col_orders.update_many({"categorySlug": cat["slug"]}, {"$set": {"categorySlug": new_slug, "categoryName": value}})
        await col_toc.update_one({"slug": cat["slug"]}, {"$set": {"slug": new_slug}})
        result_desc = f"Name changed: **{cat['name']}** → **{value}**"

    elif field == "price":
        try:
            price_val = float(value)
            if price_val <= 0:
                raise ValueError
        except ValueError:
            await ctx.reply("❌ Invalid price. Must be a positive number, e.g. `5.99`.")
            return
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"price_usd": price_val}})
        result_desc = f"Price changed: ${cat['price_usd']:.2f} → ${price_val:.2f}"

    elif field == "description":
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"description": value}})
        result_desc = "Description updated."

    elif field == "instruction":
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"instruction": value}})
        result_desc = "Post-delivery instruction updated."

    elif field in ("minqty", "min_quantity"):
        try:
            minimum = int(value)
            if minimum < 1:
                raise ValueError
        except ValueError:
            await ctx.reply("❌ Minimum quantity must be a whole number ≥ 1.")
            return
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"min_quantity": minimum}})
        result_desc = f"Minimum quantity set to {minimum}."

    else:
        await ctx.reply(f"❌ Unknown field `{field}`. Valid fields: `name`, `price`, `description`, `instruction`, `minqty`.")
        return

    await send_log(discord.Embed(
        title="✏️  Product Edited",
        description=f"**{cat['name']}** — {result_desc}\nEdited by {ctx.author.mention}",
        color=0x9B59B6,
    ))
    embed = discord.Embed(title="✅  Product Updated", description=result_desc, color=0x7B2FBE)
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &deleteproduct <name>  — alias for &removecategory
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="deleteproduct")
async def cmd_deleteproduct(ctx: commands.Context, *, name: str):
    """
    Admin: Delete a product and all its remaining stock. Same as &removecategory.

    Usage:
      &deleteproduct Netflix 1 Month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await _confirm_and_delete_product(ctx, name)


# ─────────────────────────────────────────────────────────────────────────────
#  &removestock <category>
#  Wipes ALL stock from a category without deleting the category itself
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="removestock")
async def cmd_removestock(ctx: commands.Context, *, category: str):
    """
    Admin: Clear all stock from a category (keeps the category, just empties it).

    Usage:
      &removestock Netflix 1 Month
      &removestock netflix-1-month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`. Use `&stock` to list all.")
        return

    stock_count = len(cat.get("stock", []))

    if stock_count == 0:
        await ctx.reply(f"ℹ️ **{cat['name']}** is already empty — nothing to remove.")
        return

    # Wipe the stock array
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"stock": []}})

    await send_log(discord.Embed(
        title="🧹  Stock Cleared",
        description=f"**{cat['name']}** (`{cat['slug']}`) — {stock_count} items removed.",
        color=0xA855F7,
    ))

    embed = discord.Embed(title="🧹  Stock Cleared", color=0xA855F7)
    embed.add_field(name="Category",      value=cat["name"],        inline=True)
    embed.add_field(name="Items Removed", value=str(stock_count),   inline=True)
    embed.add_field(name="Stock Now",     value="0 (empty)",        inline=True)
    embed.set_footer(text=f"Category still exists — use &restock {cat['slug']} to add new stock.")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &toc <category> <message>    — set ToC for a category
#  &toc <category> clear        — remove ToC for a category
#  &toc <category>              — view current ToC for a category
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="toc")
async def cmd_toc(ctx: commands.Context, category: str, *, message: str = None):
    """
    Admin: Set, view, or clear the Terms & Conditions for a category.
    Users must accept the ToC before they can purchase.

    Usage:
      &toc "Netflix 1M" By purchasing you agree that all sales are final.
      &toc netflix-1-month clear    — remove the ToC
      &toc netflix-1-month          — view the current ToC
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`. Use `&stock` to list all.")
        return

    # ── View current ToC ──────────────────────────────────────────────────────
    if not message:
        current = await get_toc(cat["slug"])
        embed = discord.Embed(title=f"📜  ToC — {cat['name']}", color=0xA855F7)
        embed.add_field(
            name="Current Terms & Conditions",
            value=current or "*(No ToC set — buyers go straight to purchase)*",
            inline=False,
        )
        embed.set_footer(text=f"&toc '{cat['name']}' <message> to set  •  &toc '{cat['name']}' clear to remove")
        await ctx.reply(embed=embed)
        return

    # ── Clear ToC ─────────────────────────────────────────────────────────────
    if message.strip().lower() == "clear":
        await clear_toc(cat["slug"])
        embed = discord.Embed(
            title="ToC Cleared",
            description=f"Terms & Conditions removed from **{cat['name']}**.\nBuyers will go straight to purchase.",
            color=0xA855F7,
        )
        await ctx.reply(embed=embed)
        await send_log(discord.Embed(
            title="📜  ToC Cleared",
            description=f"**{cat['name']}** ToC removed by {ctx.author.mention}",
            color=0xA855F7,
        ))
        return

    # ── Set ToC ───────────────────────────────────────────────────────────────
    if len(message) > 2000:
        await ctx.reply("❌ ToC message too long (max 2000 characters).")
        return

    await set_toc(cat["slug"], message)

    embed = discord.Embed(
        title="✅  Terms & Conditions Set",
        description=f"Buyers must now accept the ToC before purchasing **{cat['name']}**.",
        color=0x7B2FBE,
    )
    embed.add_field(name="📜 ToC Message", value=message[:1024], inline=False)
    embed.set_footer(text=f"Category: {cat['name']} ({cat['slug']})")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="📜  ToC Set",
        description=f"**{cat['name']}** ToC updated by {ctx.author.mention}\n\n{message[:500]}",
        color=0x9B59B6,
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  &help [topic]
# ─────────────────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
#  NEW COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
#  &addstock <category> <item>  — add one stock item manually
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="addstock")
async def cmd_addstock(ctx: commands.Context, category: str, *, item: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await db_restock(cat["slug"], [item])
    updated = await db_get_category(cat["slug"])
    embed = discord.Embed(title="✅  Stock Added", color=0x7B2FBE)
    embed.add_field(name="Category",   value=cat["name"],                     inline=True)
    embed.add_field(name="New Total",  value=str(len(updated.get("stock", []))), inline=True)
    embed.add_field(name="Item Added", value=f"`{item[:80]}`",                inline=False)
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &stockcount  — total stock across all categories
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="stockcount")
async def cmd_stockcount(ctx: commands.Context):
    """View total stock across all products, broken down by group."""
    cats = await db_get_categories()
    if not cats:
        await ctx.reply("❌ No products yet. Use `&category \"Name\" <price>` to create one.")
        return

    groups        = await db_get_groups()
    group_lookup  = {g["slug"]: g["name"] for g in groups}
    total         = sum(len(c.get("stock", [])) for c in cats if not c.get("infinite_stock"))

    by_group: dict = {}
    for c in cats:
        by_group.setdefault(c.get("group"), []).append(c)

    embed = discord.Embed(title="📊  Stock Count", color=0x9B59B6)
    for group_slug, group_cats in by_group.items():
        group_name = group_lookup.get(group_slug)
        header = f"📁 {group_name}" if group_name else "📂 Ungrouped"
        lines = [
            f"**{c['name']}** — {'♾️' if c.get('infinite_stock') else len(c.get('stock', []))}"
            for c in group_cats
        ]
        embed.add_field(name=header, value="\n".join(lines), inline=False)

    embed.set_footer(text=f"Total across all products: {total}")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &stockalert <category> <number>  — ping admin when stock drops below number
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="stockalert")
async def cmd_stockalert(ctx: commands.Context, category: str, threshold: int):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"alertThreshold": threshold}})
    await ctx.reply(f"✅ Stock alert set for **{cat['name']}** — admins will be pinged when stock drops below **{threshold}**.")


# ─────────────────────────────────────────────────────────────────────────────
#  &movestock <from_category> <to_category>  — move all stock between categories
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="movestock")
async def cmd_movestock(ctx: commands.Context, from_cat: str, to_cat: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat_from = await db_find_category(from_cat)
    cat_to   = await db_find_category(to_cat)
    if not cat_from:
        await ctx.reply(f"❌ Source category `{from_cat}` not found.")
        return
    if not cat_to:
        await ctx.reply(f"❌ Destination category `{to_cat}` not found.")
        return
    stock = cat_from.get("stock", [])
    if not stock:
        await ctx.reply(f"❌ **{cat_from['name']}** has no stock to move.")
        return
    await col_cats.update_one({"slug": cat_to["slug"]},   {"$push": {"stock": {"$each": stock}}})
    await col_cats.update_one({"slug": cat_from["slug"]}, {"$set":  {"stock": []}})
    await send_log(discord.Embed(
        title="📦  Stock Moved",
        description=f"{len(stock)} items moved from **{cat_from['name']}** → **{cat_to['name']}** by {ctx.author.mention}",
        color=0x9B59B6,
    ))
    embed = discord.Embed(title="✅  Stock Moved", color=0x7B2FBE)
    embed.add_field(name="From",        value=cat_from["name"], inline=True)
    embed.add_field(name="To",          value=cat_to["name"],   inline=True)
    embed.add_field(name="Items Moved", value=str(len(stock)),  inline=True)
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &order <order_id>  — look up a specific order
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="order")
async def cmd_order(ctx: commands.Context, order_id: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(f"❌ Order `{order_id}` not found.")
        return
    icons = {"pending": "🟡", "awaiting_confirmation": "🔵", "delivered": "🟢",
             "cancelled": "🔴", "expired": "⚫", "error": "🟠"}
    embed = discord.Embed(
        title=f"{icons.get(order['status'], '⚪')}  Order — {order['orderId']}",
        color=0x9B59B6,
    )
    embed.add_field(name="User",     value=f"<@{order['userId']}>",              inline=True)
    embed.add_field(name="Product",  value=order["categoryName"],                 inline=True)
    embed.add_field(name="Status",   value=order["status"],                       inline=True)
    embed.add_field(name="🔢  Quantity", value=str(order["quantity"]),                inline=True)
    embed.add_field(name="LTC",      value=str(order["ltcAmount"]),               inline=True)
    embed.add_field(name="USD",      value=f"${order['totalUSD']:.2f}",          inline=True)
    embed.add_field(name="TX ID",    value=f"`{order.get('txId', 'N/A')}`",      inline=False)
    embed.add_field(name="Created",  value=order.get("createdAt", "N/A")[:19],   inline=True)
    embed.add_field(name="Paid",     value=(order.get("paidAt") or "N/A")[:19],  inline=True)
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &refund <order_id>  — mark an order as refunded
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="refund")
async def cmd_refund(ctx: commands.Context, order_id: str, *, reason: str = "Admin refund"):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(f"❌ Order `{order_id}` not found.")
        return
    await db_update_order(order_id, {"status": "refunded", "refundReason": reason, "refundedBy": str(ctx.author.id)})
    await send_log(discord.Embed(
        title="💸  Order Refunded",
        description=f"**Order:** `{order_id}`\n**By:** {ctx.author.mention}\n**Reason:** {reason}",
        color=0xA855F7,
    ))
    embed = discord.Embed(title="✅  Order Marked as Refunded", color=0xA855F7)
    embed.add_field(name="Order ID", value=order_id,          inline=True)
    embed.add_field(name="Product",  value=order["categoryName"], inline=True)
    embed.add_field(name="Reason",   value=reason,            inline=False)
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &forcedelivery <order_id>  — manually deliver a paid order
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="forcedelivery")
async def cmd_forcedelivery(ctx: commands.Context, order_id: str):
    """Admin: Force deliver an order — full flow identical to real payment confirmation."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(f"❌ Order `{order_id}` not found.")
        return
    if order["status"] == "delivered":
        await ctx.reply("❌ Order is already delivered.")
        return
    items = await db_consume_stock(order["categorySlug"], order["quantity"])
    if not items:
        await ctx.reply(f"❌ Not enough stock in **{order['categoryName']}** to fulfil this order.")
        return

    cat              = await db_get_category(order["categorySlug"])
    instruction_text = f"\n\n📌 **Instructions:**\n{cat['instruction']}" if cat and cat.get("instruction") else ""
    delivery_lines   = "\n".join(f"**{i+1}.** `{item}`" for i, item in enumerate(items))

    await db_update_order(order_id, {
        "status":           "delivered",
        "paidAt":           datetime.utcnow().isoformat(),
        "deliveredItems":   items,
        "forceDeliveredBy": str(ctx.author.id),
    })

    # Resolve buyer and ticket channel
    try:
        buyer = ctx.guild.get_member(int(order["userId"])) or await bot.fetch_user(int(order["userId"]))
    except Exception:
        buyer = None

    channel_id = order.get("channelId")
    ch = ctx.guild.get_channel(int(channel_id)) if channel_id and ctx.guild else None

    # Reply to admin
    admin_embed = discord.Embed(title="✅  Force Delivered", color=0x7B2FBE)
    admin_embed.add_field(name="Order ID", value=order_id,              inline=True)
    admin_embed.add_field(name="Product",  value=order["categoryName"], inline=True)
    admin_embed.add_field(name="Buyer",    value=f"<@{order['userId']}>", inline=True)
    await ctx.reply(embed=admin_embed)

    # Post in ticket channel
    if ch:
        try:
            await ch.edit(topic=f"Order: {order_id} | Status: delivered | Product: {order['categoryName']}")
        except Exception:
            pass
        await ch.send(embed=discord.Embed(
            title="✅  Order Delivered",
            description=f"{buyer.mention if buyer else 'Buyer'} — Delivered by admin. Items sent to your DMs.",
            color=0x7B2FBE,
        ))

    # DM: items
    if buyer:
        try:
            dm = discord.Embed(
                title="🔑  Your Order Items",
                description=(
                    f"**Product:** {order['categoryName']}\n"
                    f"**Quantity:** {order['quantity']}\n"
                    f"**Order ID:** `{order_id}`\n\n"
                    f"**Your Items:**\n{delivery_lines}{instruction_text}"
                ),
                color=0x7B2FBE,
            )
            dm.set_footer(text=f"Order: {order_id}")
            await buyer.send(embed=dm)
        except Exception:
            if ch:
                await ch.send(content=buyer.mention, embed=discord.Embed(
                    title="⚠️  Could Not Send DM — Items Posted Here",
                    description=delivery_lines + instruction_text,
                    color=0xA855F7,
                ))

    # Log
    await send_log(discord.Embed(
        title="⚡  Force Delivery",
        description=(
            f"**Order:** `{order_id}`\n"
            f"**Product:** {order['categoryName']} × {order['quantity']}\n"
            f"**By:** {ctx.author.mention}\n"
            f"**Buyer:** <@{order['userId']}>"
        ),
        color=0x9B59B6,
    ))

    # Close ticket channel
    if ch and buyer:
        asyncio.create_task(close_order_channel(ch, buyer=buyer))
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &cancelorder <order_id>  — admin force cancel any order
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="cancelorder")
async def cmd_cancelorder(ctx: commands.Context, order_id: str, *, reason: str = "Cancelled by admin"):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(f"❌ Order `{order_id}` not found.")
        return
    await db_update_order(order_id, {"status": "cancelled", "cancelReason": reason})
    await send_log(discord.Embed(
        title="❌  Order Force Cancelled",
        description=f"**Order:** `{order_id}`\n**By:** {ctx.author.mention}\n**Reason:** {reason}",
        color=0xE74C3C,
    ))
    await ctx.reply(f"✅ Order `{order_id}` cancelled. Reason: {reason}")


# ─────────────────────────────────────────────────────────────────────────────
#  &orderhistory <@user>  — all orders from a specific user
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="orderhistory")
async def cmd_orderhistory(ctx: commands.Context, user: discord.Member):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    orders = await col_orders.find({"userId": str(user.id)}, {"_id": 0}).sort("createdAt", -1).limit(10).to_list(10)
    if not orders:
        await ctx.reply(f"No orders found for {user.mention}.")
        return
    icons = {"pending": "🟡", "awaiting_confirmation": "🔵", "delivered": "🟢",
             "cancelled": "🔴", "expired": "⚫", "error": "🟠"}
    embed = discord.Embed(title=f"📋  Order History — {user.display_name}", color=0x9B59B6)
    for o in orders:
        embed.add_field(
            name=f"{icons.get(o['status'], '⚪')} {o['orderId']}",
            value=f"{o['categoryName']} × {o['quantity']} • {o['ltcAmount']} LTC • **{o['status']}**",
            inline=False,
        )
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &blacklist <@user> [reason]  /  &unblacklist <@user>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="blacklist")
async def cmd_blacklist(ctx: commands.Context, user: discord.Member, *, reason: str = "No reason given"):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await col_blacklist.update_one(
        {"userId": str(user.id)},
        {"$set": {"userId": str(user.id), "reason": reason, "addedAt": datetime.utcnow().isoformat(), "addedBy": str(ctx.author.id)}},
        upsert=True,
    )
    await send_log(discord.Embed(
        title="🚫  User Blacklisted",
        description=f"**User:** {user.mention}\n**Reason:** {reason}\n**By:** {ctx.author.mention}",
        color=0xE74C3C,
    ))
    await ctx.reply(f"🚫 **{user.display_name}** has been blacklisted. Reason: {reason}")

@bot.command(name="unblacklist")
async def cmd_unblacklist(ctx: commands.Context, user: discord.Member):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    result = await col_blacklist.delete_one({"userId": str(user.id)})
    if result.deleted_count == 0:
        await ctx.reply(f"❌ **{user.display_name}** is not blacklisted.")
        return
    await ctx.reply(f"✅ **{user.display_name}** removed from blacklist.")


# ─────────────────────────────────────────────────────────────────────────────
#  &setprice <category> <price>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setprice")
async def cmd_setprice(ctx: commands.Context, category: str, price: float):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    old_price = cat["price_usd"]
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"price_usd": price}})
    await send_log(discord.Embed(
        title="💰  Price Updated",
        description=f"**{cat['name']}**: ${old_price} → ${price} by {ctx.author.mention}",
        color=0x9B59B6,
    ))
    embed = discord.Embed(title="✅  Price Updated", color=0x7B2FBE)
    embed.add_field(name="Category",  value=cat["name"],         inline=True)
    embed.add_field(name="Old Price", value=f"${old_price:.2f}", inline=True)
    embed.add_field(name="New Price", value=f"${price:.2f}",     inline=True)
    embed.set_footer(text="Run &updatepanel to refresh the panel.")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &setaddress <ltc_address>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setaddress")
async def cmd_setaddress(ctx: commands.Context, *, address: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    global LTC_WALLET_ADDRESS
    await set_setting("ltc_wallet_override", address)
    LTC_WALLET_ADDRESS = address
    await send_log(discord.Embed(
        title="💳  LTC Address Updated",
        description=f"New address: `{address}`\nBy: {ctx.author.mention}",
        color=0x9B59B6,
    ))
    embed = discord.Embed(title="✅  LTC Wallet Address Updated", color=0x7B2FBE)
    embed.add_field(name="New Address", value=f"`{address}`", inline=False)
    embed.set_footer(text="Takes effect on the next order immediately.")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &checkwallet
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="checkwallet")
async def cmd_checkwallet(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if not APIRONE_ACCOUNT:
        await ctx.reply("❌ `APIRONE_ACCOUNT` not set in env vars.")
        return
    url = f"https://apirone.com/api/v2/accounts/{APIRONE_ACCOUNT}/balance?currency=ltc"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                text = await r.text()
                if r.status != 200:
                    await ctx.reply(f"❌ Apirone returned HTTP {r.status}: {text[:200]}")
                    return
                import json as _jj
                data = _jj.loads(text)
                # Apirone account balance: {"account":..,"balance":[{"currency":"ltc","available":N,"total":N},...]}
                bal_list = data.get("balance", [])
                if not isinstance(bal_list, list):
                    bal_list = [bal_list]
                ltc_bal = next((b for b in bal_list if str(b.get("currency","")).lower() == "ltc"), {})
                avail_ltc = round(ltc_bal.get("available", 0) / 1e8, 8)
                total_ltc = round(ltc_bal.get("total", 0) / 1e8, 8)
                embed = discord.Embed(title="💳  Apirone Account Balance", color=0x9B59B6)
                embed.add_field(name="Account",      value=f"`{APIRONE_ACCOUNT}`",                inline=False)
                embed.add_field(name="Available",    value=f"{avail_ltc} LTC",                    inline=True)
                embed.add_field(name="💰  Order Total",        value=f"{total_ltc} LTC",                    inline=True)
                embed.add_field(name="Master Wallet",value=f"`{LTC_WALLET_ADDRESS or 'not set'}`",inline=False)
                await ctx.reply(embed=embed)
    except Exception as e:
        await ctx.reply(f"❌ Could not fetch balance: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  &stats  — total orders, revenue, top selling product
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="stats")
async def cmd_stats(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    total_orders    = await col_orders.count_documents({})
    delivered       = await col_orders.count_documents({"status": "delivered"})
    cancelled       = await col_orders.count_documents({"status": "cancelled"})
    revenue_cursor  = col_orders.aggregate([
        {"$match": {"status": "delivered"}},
        {"$group": {"_id": None, "total_usd": {"$sum": "$totalUSD"}, "total_ltc": {"$sum": "$ltcAmount"}}},
    ])
    revenue = await revenue_cursor.to_list(1)
    total_usd = revenue[0]["total_usd"] if revenue else 0
    total_ltc = revenue[0]["total_ltc"] if revenue else 0

    top_cursor = col_orders.aggregate([
        {"$match": {"status": "delivered"}},
        {"$group": {"_id": "$categoryName", "count": {"$sum": "$quantity"}}},
        {"$sort": {"count": -1}},
        {"$limit": 3},
    ])
    top = await top_cursor.to_list(3)

    embed = discord.Embed(title="📊  Store Statistics", color=0x9B59B6)
    embed.add_field(name="Total Orders",    value=str(total_orders), inline=True)
    embed.add_field(name="Delivered",       value=str(delivered),    inline=True)
    embed.add_field(name="Cancelled",       value=str(cancelled),    inline=True)
    embed.add_field(name="Revenue (USD)",   value=f"${total_usd:.2f}", inline=True)
    embed.add_field(name="Revenue (LTC)",   value=f"{round(total_ltc, 4)} LTC", inline=True)
    embed.add_field(name="\u200b",          value="\u200b",          inline=True)
    if top:
        top_str = "\n".join(f"**{i+1}.** {t['_id']} — {t['count']} units" for i, t in enumerate(top))
        embed.add_field(name="🏆 Top Sellers", value=top_str, inline=False)
    embed.set_footer(text=f"All-time stats • {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &revenue  — earnings breakdown
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="revenue")
async def cmd_revenue(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    now   = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    week  = (now - __import__("datetime").timedelta(days=7)).isoformat()

    async def get_rev(match_extra=None):
        match = {"status": "delivered"}
        if match_extra:
            match.update(match_extra)
        cur = col_orders.aggregate([
            {"$match": match},
            {"$group": {"_id": None, "usd": {"$sum": "$totalUSD"}, "ltc": {"$sum": "$ltcAmount"}, "count": {"$sum": 1}}},
        ])
        r = await cur.to_list(1)
        return r[0] if r else {"usd": 0, "ltc": 0, "count": 0}

    r_today    = await get_rev({"paidAt": {"$gte": today}})
    r_week     = await get_rev({"paidAt": {"$gte": week}})
    r_alltime  = await get_rev()

    embed = discord.Embed(title="💰  Revenue Breakdown", color=0x7B2FBE)
    embed.add_field(name="📅 Today",    value=f"${r_today['usd']:.2f} ({r_today['count']} orders)",   inline=False)
    embed.add_field(name="📆 7 Days",   value=f"${r_week['usd']:.2f} ({r_week['count']} orders)",    inline=False)
    embed.add_field(name="🗓️ All Time", value=f"${r_alltime['usd']:.2f} ({r_alltime['count']} orders)", inline=False)
    embed.set_footer(text=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &topsellers
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="topsellers")
async def cmd_topsellers(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cur = col_orders.aggregate([
        {"$match": {"status": "delivered"}},
        {"$group": {"_id": "$categoryName", "units": {"$sum": "$quantity"}, "revenue": {"$sum": "$totalUSD"}}},
        {"$sort": {"units": -1}},
        {"$limit": 10},
    ])
    top = await cur.to_list(10)
    if not top:
        await ctx.reply("No delivered orders yet.")
        return
    embed = discord.Embed(title="🏆  Top Selling Products", color=0x8B5CF6)
    medals = ["🥇", "🥈", "🥉"]
    for i, t in enumerate(top):
        medal = medals[i] if i < 3 else f"#{i+1}"
        embed.add_field(
            name=f"{medal}  {t['_id']}",
            value=f"{t['units']} units sold • ${t['revenue']:.2f} revenue",
            inline=False,
        )
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &closeticket  — admin force closes current ticket channel
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="closeticket")
async def cmd_closeticket(ctx: commands.Context):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if not ctx.channel.name.startswith("order-"):
        await ctx.reply("❌ This command can only be used inside an order ticket channel.")
        return
    await ctx.reply("🔒 Closing this ticket...")
    # Find buyer from channel overwrites
    buyer = None
    for target, ow in ctx.channel.overwrites.items():
        if isinstance(target, discord.Member) and not target.bot and not target.guild_permissions.administrator:
            buyer = target
            break
    asyncio.create_task(close_order_channel_cancel(ctx.channel, buyer=buyer))


# ─────────────────────────────────────────────────────────────────────────────
#  &adduser <@user>  /  &removeuser <@user>  — manage ticket access
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="adduser")
async def cmd_adduser(ctx: commands.Context, user: discord.Member):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await ctx.channel.set_permissions(user, view_channel=True, send_messages=True, read_message_history=True)
    await ctx.reply(f"✅ {user.mention} added to this channel.")

@bot.command(name="removeuser")
async def cmd_removeuser(ctx: commands.Context, user: discord.Member):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await ctx.channel.set_permissions(user, overwrite=None)
    await ctx.reply(f"✅ {user.mention} removed from this channel.")


# ─────────────────────────────────────────────────────────────────────────────
#  &renameticket <new_name>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="renameticket")
async def cmd_renameticket(ctx: commands.Context, *, name: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    safe = name.lower().replace(" ", "-")[:50]
    await ctx.channel.edit(name=safe)
    await ctx.reply(f"✅ Channel renamed to **{safe}**.")


# ─────────────────────────────────────────────────────────────────────────────
#  &setprefix <prefix>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setprefix")
async def cmd_setprefix(ctx: commands.Context, prefix: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if len(prefix) > 3:
        await ctx.reply("❌ Prefix must be 3 characters or fewer.")
        return
    await set_setting("prefix", prefix)
    bot.command_prefix = prefix
    await ctx.reply(f"✅ Prefix changed to `{prefix}`. All commands now use `{prefix}command`.")


# ─────────────────────────────────────────────────────────────────────────────
#  &settimeout <minutes>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="settimeout")
async def cmd_settimeout(ctx: commands.Context, minutes: int):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if minutes < 5 or minutes > 1440:
        await ctx.reply("❌ Timeout must be between 5 and 1440 minutes.")
        return
    global PAYMENT_TIMEOUT_MIN
    PAYMENT_TIMEOUT_MIN = minutes
    await set_setting("payment_timeout", str(minutes))
    await ctx.reply(f"✅ Payment timeout set to **{minutes} minutes**.")


# ─────────────────────────────────────────────────────────────────────────────
#  &setstatus <text>
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setstatus")
async def cmd_setstatus(ctx: commands.Context, *, text: str):
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=text))
    await set_setting("bot_status", text)
    await ctx.reply(f"✅ Bot status set to: *{text}*")


# ─────────────────────────────────────────────────────────────────────────────
#  &botstats
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="botstats")
async def cmd_botstats(ctx: commands.Context):
    uptime_str = "Unknown"
    if bot_start_time:
        delta   = datetime.utcnow() - bot_start_time
        hours   = int(delta.total_seconds() // 3600)
        minutes = int((delta.total_seconds() % 3600) // 60)
        uptime_str = f"{hours}h {minutes}m"

    latency = round(bot.latency * 1000)
    total   = await col_orders.count_documents({})
    delivered = await col_orders.count_documents({"status": "delivered"})

    embed = discord.Embed(title="🤖  Bot Statistics", color=0x7C3AED)
    embed.add_field(name="⏱️ Uptime",          value=uptime_str,       inline=True)
    embed.add_field(name="📶 Ping",             value=f"{latency}ms",   inline=True)
    embed.add_field(name="🏷️ Version",          value="AutoBuy v1.0",   inline=True)
    embed.add_field(name="📦 Total Orders",     value=str(total),       inline=True)
    embed.add_field(name="✅ Delivered",         value=str(delivered),   inline=True)
    embed.add_field(name="🗄️ DB",               value=DB_NAME,          inline=True)
    embed.set_footer(text=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  &help  — all buyer/user commands
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
#  &ping
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="ping")
async def cmd_ping(ctx: commands.Context):
    """Check if the bot is alive and see current latency."""
    latency = round(bot.latency * 1000)
    color   = 0x2ECC71 if latency < 150 else (0xF39C12 if latency < 400 else 0xE74C3C)
    await ctx.reply(embed=discord.Embed(
        title="🏓  Pong!",
        description=f"Latency: **{latency}ms**",
        color=color,
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  &resume  — re-attach payment polling to all awaiting_confirmation orders
#  Run this after a bot restart to resume any in-progress payments
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="resume")
async def cmd_resume(ctx: commands.Context):
    """
    Admin: Resume payment checking for all orders that were awaiting confirmation
    when the bot restarted. Run this once after every restart.
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    pending = await col_orders.find(
        {"status": "awaiting_confirmation"},
        {"_id": 0}
    ).to_list(50)

    if not pending:
        await ctx.reply("✅ No orders awaiting confirmation — nothing to resume.")
        return

    resumed = 0
    skipped = 0
    for order in pending:
        channel_id = order.get("channelId")
        if not channel_id or not ctx.guild:
            skipped += 1
            continue
        channel = ctx.guild.get_channel(int(channel_id))
        if not channel:
            skipped += 1
            continue
        try:
            user = ctx.guild.get_member(int(order["userId"])) or await bot.fetch_user(int(order["userId"]))
        except Exception:
            user = None
        if not user:
            skipped += 1
            continue

        # Actually re-attach polling — this was the bug (it was only counting, not resuming)
        asyncio.create_task(poll_apirone_payment(order["orderId"], channel, user))
        resumed += 1

    embed = discord.Embed(title="✅  Resume Complete", color=0x7B2FBE)
    embed.add_field(name="▶️ Resumed", value=str(resumed), inline=True)
    embed.add_field(name="⏭️ Skipped", value=str(skipped), inline=True)
    embed.set_footer(text="Skipped orders had missing channels or users.")
    await ctx.reply(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
#  Auto-resume on bot start — resume all awaiting_confirmation orders
# ─────────────────────────────────────────────────────────────────────────────
async def auto_resume_on_ready(guild: discord.Guild):
    """
    Called from on_ready.
    Expires timed-out orders, notifies open tickets, and restarts backup polling.
    """
    await asyncio.sleep(5)

    import datetime as _dt
    cutoff = (datetime.utcnow() - _dt.timedelta(minutes=PAYMENT_TIMEOUT_MIN)).isoformat()
    stale = await col_orders.find(
        {"status": "awaiting_confirmation", "createdAt": {"$lt": cutoff}},
        {"_id": 0},
    ).to_list(50)
    for order in stale:
        await db_update_order(order["orderId"], {"status": "expired"})
    if stale:
        print(f"[auto-resume] Expired {len(stale)} timed-out order(s) on startup.")

    pending = await col_orders.find({"status": "awaiting_confirmation"}, {"_id": 0}).to_list(50)
    for order in pending:
        channel_id = order.get("channelId")
        if not channel_id:
            continue
        channel = guild.get_channel(int(channel_id))
        if not channel:
            continue
        try:
            await channel.send(embed=discord.Embed(
                title="🔄  Bot Restarted — Payment Still Monitored",
                description=(
                    "The bot restarted. Your payment is still being monitored.\n\n"
                    f"**Order:** `{order['orderId']}`\n"
                    f"**Address:** `{order.get('ltcAddress', 'N/A')}`\n"
                    f"**Amount:** `{order['ltcAmount']} LTC`"
                ),
                color=0x9B59B6,
            ))
        except Exception:
            pass
        try:
            user = guild.get_member(int(order["userId"])) or await bot.fetch_user(int(order["userId"]))
        except Exception:
            user = None
        asyncio.create_task(poll_apirone_payment(order["orderId"], channel, user))
    if pending:
        print(f"[auto-resume] Resumed polling for {len(pending)} open order(s).")


# ─────────────────────────────────────────────────────────────────────────────
#  &verify <order_id>  — admin force-checks payment right now
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="verify")
async def cmd_verify(ctx: commands.Context, order_id: str):
    """
    Admin: Force an immediate payment check for an order, bypassing the 30s poll interval.
    Useful when buyer says they paid and you want to check right now.

    Usage:
      &verify ORD-1234567890-ABCDEF
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(f"❌ Order `{order_id}` not found.")
        return

    status = order.get("status", "")
    if status == "delivered":
        await ctx.reply(f"✅ Order `{order_id}` is already **delivered**.")
        return
    if status in ("cancelled", "expired", "error"):
        await ctx.reply(f"❌ Order `{order_id}` is **{status}** — cannot verify.")
        return

    addr = order.get("ltcAddress", "")
    msg  = await ctx.reply(embed=discord.Embed(
        title="🔍  Checking Payment via Apirone...",
        description=f"Checking address balance for order `{order_id}`...",
        color=0x9B59B6,
    ))

    # Check balance via Apirone
    if addr and not LTC_DEV_MODE:
        bal = await apirone_get_address_balance(addr)
        avail = bal.get("available", 0)
        expected = int(order["ltcAmount"] * 1e8)
        tolerance = int(FEE_TOLERANCE_LTC * 1e8)
        if avail < expected - tolerance:
            await msg.edit(embed=discord.Embed(
                title="❌  Payment Not Confirmed",
                description=(
                    f"**Order:** `{order_id}`\n"
                    f"**Expected:** {order['ltcAmount']} LTC\n"
                    f"**Available:** {round(avail/1e8, 8)} LTC\n"
                    f"**Address:** `{addr}`\n\n"
                    "Payment not detected or not yet confirmed. Use `&forcedelivery` to deliver manually."
                ),
                color=0xA855F7,
            ))
            return
    # Payment confirmed or dev mode — proceed with delivery

    channel_id = order.get("channelId")
    channel    = ctx.guild.get_channel(int(channel_id)) if channel_id else None
    buyer      = ctx.guild.get_member(int(order["userId"])) if ctx.guild else None

    items = await db_consume_stock(order["categorySlug"], order["quantity"])
    if not items:
        await msg.edit(embed=discord.Embed(
            title="⚠️  No Stock Available",
            description=f"Could not deliver **{order['categoryName']}** — stock is empty. Restock and try again.",
            color=0xA855F7,
        ))
        return

    cat              = await db_get_category(order["categorySlug"])
    instruction_text = f"\n\n📌 **Instructions:**\n{cat['instruction']}" if cat and cat.get("instruction") else ""
    delivery_lines   = "\n".join(f"**{i+1}.** `{item}`" for i, item in enumerate(items))

    await db_update_order(order_id, {
        "status":           "delivered",
        "paidAt":           datetime.utcnow().isoformat(),
        "deliveredItems":   items,
        "manualVerifiedBy": str(ctx.author.id),
        "paidVia":          "manual_verify",
    })

    await msg.edit(embed=discord.Embed(
        title="✅  Order Force-Delivered!",
        description=(
            f"**Order:** `{order_id}`\n"
            f"**Product:** {order['categoryName']} × {order['quantity']}\n"
            f"**Verified by:** {ctx.author.mention}"
        ),
        color=0x7B2FBE,
    ))

    if buyer:
        try:
            dm = discord.Embed(
                title="✅  Your Order Has Been Delivered!",
                description=(
                    f"**Product:** {order['categoryName']}\n"
                    f"**Quantity:** {order['quantity']}\n"
                    f"**Order ID:** `{order_id}`\n\n"
                    f"**Your Items:**\n{delivery_lines}{instruction_text}"
                ),
                color=0x7B2FBE,
            )
            dm.set_footer(text="Thank you for your purchase!")
            await buyer.send(embed=dm)
        except Exception:
            if channel:
                await channel.send(content=buyer.mention, embed=discord.Embed(
                    title="🔑  Your Items",
                    description=delivery_lines + instruction_text,
                    color=0x7B2FBE,
                ))

    if channel:
        try:
            await channel.edit(topic=f"Order: {order_id} | Status: delivered | Product: {order['categoryName']}")
        except Exception:
            pass
        await channel.send(embed=discord.Embed(
            title="✅  Order Delivered",
            description=f"Manually delivered by {ctx.author.mention}. Items sent to DMs.",
            color=0x7B2FBE,
        ))
        asyncio.create_task(close_order_channel(channel, buyer=buyer))

    await send_log(discord.Embed(
        title="💳  Manual Verify — Delivered",
        description=(
            f"**Order:** `{order_id}`\n"
            f"**By:** {ctx.author.mention}\n"
            f"**Product:** {order['categoryName']} × {order['quantity']}"
        ),
        color=0x7B2FBE,
    ))

# ─────────────────────────────────────────────────────────────────────────────
#  &sendfund  — manually transfer all LTC in Apirone account to master wallet
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="sendfund")
async def cmd_sendfund(ctx: commands.Context):
    """Admin: Transfer 100% of available LTC in Apirone account to LTC_WALLET_ADDRESS."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    if not APIRONE_ACCOUNT or not APIRONE_TRANSFER_KEY or not LTC_WALLET_ADDRESS:
        await ctx.reply(embed=discord.Embed(
            title="❌  Config Missing",
            description=(
                "One or more required env vars are not set:\n"
                "• `APIRONE_ACCOUNT`\n"
                "• `APIRONE_TRANSFER_KEY`\n"
                "• `LTC_WALLET_ADDRESS`"
            ),
            color=0xE74C3C,
        ))
        return

    msg = await ctx.reply(embed=discord.Embed(
        title="⏳  Checking balance...",
        color=0x9B59B6,
    ))

    # ── Step 1: fetch available balance ───────────────────────────────────────
    try:
        url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/balance?currency=ltc"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                bal  = next((b for b in data.get("balance", []) if b.get("currency") == "ltc"), {})
                avail_litoshis = int(bal.get("available", 0))
                total_litoshis = int(bal.get("total", 0))
    except Exception as e:
        await msg.edit(embed=discord.Embed(
            title="❌  Balance Fetch Failed",
            description=f"Could not fetch Apirone balance:\n`{e}`",
            color=0xE74C3C,
        ))
        return

    avail_ltc = avail_litoshis / 1e8
    total_ltc = total_litoshis / 1e8

    if avail_litoshis <= 0:
        await msg.edit(embed=discord.Embed(
            title="⚠️  No Available Balance",
            description=(
                f"**Available:** `{avail_ltc} LTC`\n"
                f"**Total (incl. unconfirmed):** `{total_ltc} LTC`\n\n"
                f"Nothing to send — balance is zero or still unconfirmed."
            ),
            color=0xA855F7,
        ))
        return

    await msg.edit(embed=discord.Embed(
        title="⏳  Sending funds...",
        description=f"Transferring `{avail_ltc} LTC` → `{LTC_WALLET_ADDRESS}`",
        color=0x9B59B6,
    ))

    # ── Step 2: transfer 100% to master wallet ────────────────────────────────
    try:
        transfer_url = f"{APIRONE_BASE}/accounts/{APIRONE_ACCOUNT}/transfer"
        payload = {
            "currency":                 "ltc",
            "transfer-key":             APIRONE_TRANSFER_KEY,
            "destinations":             [{"address": LTC_WALLET_ADDRESS, "amount": "100%"}],
            "fee":                      "normal",
            "subtract-fee-from-amount": True,
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(transfer_url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                text = await r.text()
                print(f"[sendfund] Response {r.status}: {text[:400]}")

                if r.status == 200:
                    import json as _j
                    d    = _j.loads(text)
                    txs  = d.get("txs", [])
                    txid = txs[0] if txs else "N/A"
                    sent_litoshis = int(d.get("amount", avail_litoshis))
                    sent_ltc      = sent_litoshis / 1e8

                    await msg.edit(embed=discord.Embed(
                        title="✅  Funds Sent!",
                        description=(
                            f"**Amount:** `{sent_ltc} LTC`\n"
                            f"**To:** `{LTC_WALLET_ADDRESS}`\n"
                            f"**TX:** `{txid}`\n\n"
                            f"Funds are on their way to your master wallet."
                        ),
                        color=0x7B2FBE,
                    ))
                    await send_log(discord.Embed(
                        title="💸  Manual Fund Transfer",
                        description=(
                            f"**By:** {ctx.author.mention}\n"
                            f"**Amount:** {sent_ltc} LTC\n"
                            f"**To:** `{LTC_WALLET_ADDRESS}`\n"
                            f"**TX:** `{txid}`"
                        ),
                        color=0x7B2FBE,
                    ))

                else:
                    import json as _j
                    try:
                        err = _j.loads(text)
                        err_msg = err.get("message") or err.get("error") or text[:300]
                    except Exception:
                        err_msg = text[:300]
                    await msg.edit(embed=discord.Embed(
                        title="❌  Transfer Failed",
                        description=(
                            f"**Amount:** `{avail_ltc} LTC`\n"
                            f"**Error:** {err_msg}\n\n"
                            f"Check Railway logs for `[sendfund]` details."
                        ),
                        color=0xE74C3C,
                    ))

    except Exception as e:
        await msg.edit(embed=discord.Embed(
            title="❌  Transfer Exception",
            description=f"`{e}`\n\nCheck Railway logs.",
            color=0xE74C3C,
        ))
        import traceback; traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
#  &deliver <@user> <category> <quantity>
#  Admin: send stock directly to a user's DMs, no ticket/payment needed
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="deliver")
async def cmd_deliver(ctx: commands.Context, user: discord.Member, category: str, quantity: int = 1):
    """
    Admin: Directly deliver stock to a user's DMs without any order/payment flow.

    Usage:
      &deliver @user netflix-1-month 2
      &deliver @user "Netflix 1 Month" 1
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`. Use `&stock` to list all.")
        return
    if quantity < 1:
        await ctx.reply("❌ Quantity must be at least 1.")
        return

    items = await db_consume_stock(cat["slug"], quantity)
    if not items:
        await ctx.reply(f"❌ Not enough stock in **{cat['name']}** ({quantity} requested, {len(cat.get('stock',[]))} available).")
        return

    cat_obj          = await db_get_category(cat["slug"])
    instruction_text = f"\n\n📌 **Instructions:**\n{cat_obj['instruction']}" if cat_obj and cat_obj.get("instruction") else ""
    delivery_lines   = "\n".join(f"**{i+1}.** `{item}`" for i, item in enumerate(items))

    # Create a manual order record for logging
    order_id = f"MANUAL-{int(datetime.utcnow().timestamp())}-{cat['slug'][:6].upper()}"
    await col_orders.insert_one({
        "orderId":        order_id,
        "userId":         str(user.id),
        "categorySlug":   cat["slug"],
        "categoryName":   cat["name"],
        "quantity":       quantity,
        "totalUSD":       cat["price_usd"] * quantity,
        "ltcAmount":      0,
        "ltcAddress":     "manual",
        "status":         "delivered",
        "txId":           None,
        "createdAt":      datetime.utcnow().isoformat(),
        "paidAt":         datetime.utcnow().isoformat(),
        "deliveredItems": items,
        "manualDeliveredBy": str(ctx.author.id),
    })

    # DM the items — as a .txt file attachment for 5+ items, inline embed otherwise
    try:
        if len(items) >= 5:
            import io as _io
            txt_content = "\n".join(items)
            if cat_obj and cat_obj.get("instruction"):
                txt_content += f"\n\n--- Instructions ---\n{cat_obj['instruction']}"
            txt_file = discord.File(
                fp=_io.BytesIO(txt_content.encode("utf-8")),
                filename=f"order-{order_id}.txt",
            )
            dm = discord.Embed(
                title="🎁  You Have Received an Order",
                description=(
                    f"**Product:** {cat['name']}\n"
                    f"**Quantity:** {quantity}\n"
                    f"**Delivered by:** {ctx.author.display_name}\n\n"
                    f"Your items are in the attached `.txt` file (one per line)."
                ),
                color=0x7B2FBE,
            )
            dm.set_footer(text=f"Order: {order_id}")
            await user.send(embed=dm, file=txt_file)
        else:
            dm = discord.Embed(
                title="🎁  You Have Received an Order",
                description=(
                    f"**Product:** {cat['name']}\n"
                    f"**Quantity:** {quantity}\n"
                    f"**Delivered by:** {ctx.author.display_name}\n\n"
                    f"**Your Items:**\n{delivery_lines}{instruction_text}"
                ),
                color=0x7B2FBE,
            )
            dm.set_footer(text=f"Order: {order_id}")
            await user.send(embed=dm)
        dm_sent = True
    except discord.Forbidden:
        dm_sent = False

    # Reply in channel — NO items shown here, only delivery status
    dm_status = "✅ Product has been delivered to the user's DM." if dm_sent else "❌ DMs closed — items not delivered!"
    embed = discord.Embed(
        title="✅  Delivered",
        description=(
            f"**Product:** {cat['name']} × {quantity}\n"
            f"**To:** {user.mention}\n"
            f"**DM Sent:** {dm_status}"
        ),
        color=0x7B2FBE if dm_sent else 0xE74C3C,
    )
    # Items are intentionally NOT added here — they are only sent to the user's DM
    await ctx.reply(embed=embed)

    if not dm_sent:
        await ctx.reply(f"⚠️ {user.mention} has DMs closed — items could not be delivered. Stock has been consumed.")

    await send_log(discord.Embed(
        title="🎁  Manual Delivery",
        description=(
            f"**By:** {ctx.author.mention}\n"
            f"**To:** {user.mention}\n"
            f"**Product:** {cat['name']} × {quantity}\n"
            f"**DM:** {'sent' if dm_sent else 'FAILED — DMs closed'}\n"
            f"**Order:** `{order_id}`"
        ),
        color=0x9B59B6,
    ))
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &help  — user-facing help
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
#  &setinfinitestock <category> <item>
#  Sets a single item that is delivered for every purchase (infinite, never consumed)
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setinfinitestock")
async def cmd_setinfinitestock(ctx: commands.Context, category: str, *, item: str):
    """
    Admin: Set a single item that is delivered for EVERY purchase of this product.
    Stock is never consumed — the same item is sent every time.

    Usage:
      &setinfinitestock netflix-1-month https://netflix.com/redeem/XXXXXXXXXXX
      &setinfinitestock "Netflix 1M" user:pass123
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"infinite_stock": item}})
    embed = discord.Embed(title="♾️  Infinite Stock Set", color=0x7B2FBE)
    embed.add_field(name="Category", value=cat["name"],      inline=True)
    embed.add_field(name="Item",     value=f"`{item[:80]}`", inline=False)
    embed.set_footer(text="This item will be delivered for every purchase. Normal stock is ignored.")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


@bot.command(name="clearinfinitestock")
async def cmd_clearinfinitestock(ctx: commands.Context, *, category: str):
    """
    Admin: Remove infinite stock from a category. Normal stock list resumes.

    Usage:
      &clearinfinitestock netflix-1-month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"infinite_stock": None}})
    await ctx.reply(f"✅ Infinite stock removed from **{cat['name']}**. Normal stock list is now active.")
    asyncio.create_task(auto_update_panel())


# ─────────────────────────────────────────────────────────────────────────────
#  &setcustomltc <category> <ltc_address>
#  Set a custom LTC destination for auto-transfer on this product's sales
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="setcustomltc")
async def cmd_setcustomltc(ctx: commands.Context, category: str, *, address: str):
    """
    Admin: Set a custom LTC wallet address for auto-transfer on this category.
    When a sale of this product completes, funds go here instead of the global wallet.

    Usage:
      &setcustomltc netflix-1-month LTC1abc...xyz
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"custom_ltc_dest": address}})
    embed = discord.Embed(title="💳  Custom LTC Destination Set", color=0x7B2FBE)
    embed.add_field(name="Category",    value=cat["name"],       inline=True)
    embed.add_field(name="LTC Address", value=f"`{address}`",    inline=False)
    embed.set_footer(text="Auto-transfer for this product will go to this address.")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="💳  Custom LTC Dest Set",
        description=f"**{cat['name']}** → `{address}` by {ctx.author.mention}",
        color=0x9B59B6,
    ))


@bot.command(name="clearcustomltc")
async def cmd_clearcustomltc(ctx: commands.Context, *, category: str):
    """
    Admin: Remove the custom LTC destination for a category. Reverts to global wallet.

    Usage:
      &clearcustomltc netflix-1-month
    """
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(f"❌ No category matching `{category}`.")
        return
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"custom_ltc_dest": None}})
    await ctx.reply(f"✅ Custom LTC destination removed from **{cat['name']}**. Global wallet is now used.")


@bot.command(name="help")
async def cmd_help(ctx: commands.Context):
    """Interactive help — dropdown menu with sections. Admins see admin sections, users see buyer guide."""
    admin = is_admin(ctx)

    if admin:
        select = discord.ui.Select(
            placeholder="📖  Select a help section...",
            options=[
                discord.SelectOption(label="🏪  Shop Configuration",    value="shop",      description="Panel, name, banner, icon, prefix, timeout"),
                discord.SelectOption(label="💎  Products & Stock",       value="stock",     description="Categories, pricing, restock, infinite stock"),
                discord.SelectOption(label="🗂️  Groups",                 value="groups",    description="Organise products into groups/folders"),
                discord.SelectOption(label="📋  Order Management",       value="orders",    description="View, deliver, cancel, refund, force-deliver"),
                discord.SelectOption(label="📊  Analytics & Reporting",  value="analytics", description="Revenue, stats, top sellers, bot info"),
                discord.SelectOption(label="🎟️  Ticket Management",      value="tickets",   description="Close, rename, add/remove users"),
                discord.SelectOption(label="🚫  User Management",        value="users",     description="Blacklist and unblacklist buyers"),
                discord.SelectOption(label="💰  Wallet & Payments",      value="wallet",    description="LTC address, balance, sweep funds"),
            ],
        )
    else:
        select = discord.ui.Select(
            placeholder="📖  Select a help section...",
            options=[
                discord.SelectOption(label="🛒  How to Purchase",        value="buy",     description="Step-by-step buying guide"),
                discord.SelectOption(label="⬡  Payment — Litecoin",      value="payment", description="How LTC payments work"),
                discord.SelectOption(label="📦  Browsing Products",       value="browse",  description="View stock and product info"),
                discord.SelectOption(label="❓  Support",                 value="support", description="Getting help from admins"),
            ],
        )

    ADMIN_SECTIONS = {
        "shop": (
            "🏪  Shop Configuration",
            (
                "`&panel` — Deploy the shop panel in the current channel\n"
                "`&updatepanel` — Refresh product listings & live stock counts\n"
                "`&shopname <name>` — Set the store display name\n"
                "`&shopbanner <url>` — Set the panel banner image\n"
                "`&shopicon <url>` — Set the shop icon / thumbnail\n"
                "`&shopsettings` — View all current shop configuration\n"
                "`&setstatus <text>` — Update the bot's Discord status\n"
                "`&setprefix <prefix>` — Change the command prefix\n"
                "`&settimeout <minutes>` — Set the payment window duration\n"
                "`&setaddress <address>` — Update the LTC receiving wallet"
            ),
        ),
        "stock": (
            "💎  Products & Stock",
            (
                "`&category 'Name' <price> [instruction]` — Create a new product category\n"
                "`&categoryrename <old> | <new>` — Rename an existing category\n"
                "`&editproduct <name> | <field> | <value>` — Edit name/price/description/instruction/minqty\n"
                "`&setprice <name> <price>` — Update a product's price\n"
                "`&setinstruction <name> <text>` — Edit post-delivery instructions\n"
                "`&setminqty <name> <min>` — Set minimum purchase quantity\n"
                "`&removecategory <name>` *(alias `&deleteproduct`)* — Permanently delete a category\n"
                "`&addstock <name> <item>` — Add a single item to stock\n"
                "`&restock <name>` *(+ .txt attachment)* — Bulk import stock\n"
                "`&removestock <name>` — Clear all stock from a category\n"
                "`&setinfinitestock <name> <item>` — Set a repeatable delivery item\n"
                "`&clearinfinitestock <name>` — Remove infinite stock mode\n"
                "`&setcustomltc <name> <address>` — Custom LTC destination per product\n"
                "`&clearcustomltc <name>` — Remove custom LTC destination\n"
                "`&movestock <from> <to>` — Transfer stock between categories\n"
                "`&stockcount` — Full stock overview, grouped by group\n"
                "`&stockalert <name> <n>` — Set low stock notification threshold\n"
                "`&toc <name> <message>` — Set Terms & Conditions for a product\n"
                "`&searchproduct <query>` — Search products by name/description\n"
                "`&productinfo <name>` — Full detail card for one product"
            ),
        ),
        "groups": (
            "🗂️  Groups",
            (
                "Groups are folders used to organise products (e.g. \"Bot Src\", \"Tools\", \"Accounts\").\n\n"
                "`&creategroup <name>` — Create a new group\n"
                "`&renamegroup <old> | <new>` — Rename a group\n"
                "`&deletegroup <name>` — Delete a group (products move to Ungrouped)\n"
                "`&groups` — List all groups with product counts\n"
                "`&groupproducts <group>` — List products in a group (`ungrouped` works too)\n"
                "`&addtogroup <product> | <group>` *(alias `&moveproduct`)* — Assign/move a product into a group\n"
                "`&removefromgroup <product>` — Unassign a product (moves it to Ungrouped)"
            ),
        ),
        "orders": (
            "📋  Order Management",
            (
                "`&orders` — View the last 10 orders\n"
                "`&order <id>` — Look up a specific order by ID\n"
                "`&orderitems <id>` — View exact items delivered for an order\n"
                "`&orderhistory <@user>` — Full purchase history for a user\n"
                "`&deliver <@user> <product> <qty>` — Manually deliver stock to a user\n"
                "`&forcedelivery <id>` — Force-complete an existing order\n"
                "`&cancelorder <id>` — Force-cancel an order\n"
                "`&refund <id> [reason]` — Mark an order as refunded\n"
                "`&verify <id>` — Manually trigger a payment check\n"
                "`&resume` — Re-attach payment polling after a bot restart\n"
                "`&deleteorder` — Archive & close completed order channels\n"
                "`&clearhistory` — ⚠️ Permanently wipe all orders & blacklist entries"
            ),
        ),
        "analytics": (
            "📊  Analytics & Reporting",
            (
                "`&stats` — Total order count & lifetime revenue\n"
                "`&revenue` — Earnings breakdown: today / 7 days / all time\n"
                "`&topsellers` — Best-performing products by volume\n"
                "`&botstats` — Uptime, latency & system info"
            ),
        ),
        "tickets": (
            "🎟️  Ticket Management",
            (
                "`&closeticket` — Force-close the current order ticket\n"
                "`&adduser <@user>` — Add a user to the current ticket\n"
                "`&removeuser <@user>` — Remove a user from the current ticket\n"
                "`&renameticket <name>` — Rename the ticket channel"
            ),
        ),
        "users": (
            "🚫  User Management",
            (
                "`&blacklist <@user> [reason]` — Block a user from making purchases\n"
                "`&unblacklist <@user>` — Remove a user from the blacklist"
            ),
        ),
        "wallet": (
            "💰  Wallet & Payments",
            (
                "`&setaddress <address>` — Update the LTC receiving wallet address\n"
                "`&checkwallet` — View the current LTC wallet balance\n"
                "`&sendfund` — Manually sweep all LTC to the master wallet"
            ),
        ),
    }

    USER_SECTIONS = {
        "buy": (
            "🛒  How to Purchase",
            (
                "**1.** Open the shop panel and use the dropdown to select **🎫 Create AutoBuy Ticket**.\n"
                "**2.** A private order ticket opens just for you.\n"
                "**3.** Pick your product from the in-ticket dropdown.\n"
                "**4.** Type how many units you want and press Enter.\n"
                "**5.** Send the exact LTC amount shown to the provided address.\n"
                "**6.** Payment is confirmed automatically — items land in your DMs instantly."
            ),
        ),
        "payment": (
            "⬡  Payment — Litecoin (LTC)",
            (
                "› Every order generates a **unique** LTC deposit address just for you\n"
                "› Send the **exact** amount shown — the bot checks to the decimal\n"
                "› Confirmation is fully automatic via blockchain polling\n"
                "› Delivery hits your **Discord DMs** within seconds of confirmation\n"
                "› All payments are **irreversible** — verify the address before sending\n"
                "› No transaction ID, no screenshots — just send and wait"
            ),
        ),
        "browse": (
            "📦  Browsing Products",
            (
                "`&stock` — View all products with live stock counts and prices\n"
                "`&stock <name>` — View details for a specific product\n\n"
                "You can also select **📦 Stock** from the shop panel dropdown to see availability at a glance."
            ),
        ),
        "support": (
            "❓  Support",
            (
                "If you run into any issues during your order:\n\n"
                "› Press **🆘 Request Admin Support** in the invoice view inside your ticket\n"
                "› Do **not** open multiple tickets for the same order\n"
                "› Do **not** DM random members — use the ticket system\n\n"
                "An admin will assist you as soon as possible."
            ),
        ),
    }

    sections = ADMIN_SECTIONS if admin else USER_SECTIONS

    async def on_select(interaction: discord.Interaction):
        chosen = interaction.data["values"][0]
        if chosen not in sections:
            await interaction.response.send_message("Unknown section.", ephemeral=True)
            return
        title, body = sections[chosen]
        embed = discord.Embed(title=title, description=body, color=0x9B59B6)
        if admin:
            embed.set_footer(text="🔒 Admin Reference  •  &help to reopen")
        else:
            embed.set_footer(text="💜 AutoBuy  •  &help to reopen this guide")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    select.callback = on_select
    view = discord.ui.View(timeout=120)
    view.add_item(select)

    if admin:
        intro = discord.Embed(
            title="🛡️  Admin Command Reference",
            description=(
                "Select a category from the dropdown below to view its commands.\n"
                "All responses are **private** — only you can see them."
            ),
            color=0x9B59B6,
        )
        intro.set_footer(text="🔒 Admin-only  •  Dropdown expires after 2 minutes")
    else:
        intro = discord.Embed(
            title="🛍️  AutoBuy Help Centre",
            description=(
                "Select a topic from the dropdown below to get started.\n"
                "All responses are **private** — only you can see them."
            ),
            color=0x9B59B6,
        )
        intro.set_footer(text="💜 AutoBuy  •  Dropdown expires after 2 minutes")

    await ctx.reply(embed=intro, view=view)



# ─────────────────────────────────────────────────────────────────────────────
#  &clearhistory — wipe orders, feedback, blacklist
# ─────────────────────────────────────────────────────────────────────────────
@bot.command(name="clearhistory")
async def cmd_clearhistory(ctx: commands.Context):
    """Admin: Permanently delete all order history and blacklist entries."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return

    order_count = await col_orders.count_documents({})
    bl_count    = await col_blacklist.count_documents({})

    await ctx.reply(embed=discord.Embed(
        title="⚠️  Confirm Full History Wipe",
        description=(
            f"This will permanently delete:\n\n"
            f"🗂️ **{order_count}** orders\n"
            f"🚫 **{bl_count}** blacklist entries\n\n"
            "Categories, stock, settings and ToC are **not affected**.\n\n"
            "Type `confirm` or `cancel`."
        ),
        color=0xE74C3C,
    ))

    def chk1(m):
        return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ("confirm", "cancel")
    try:
        r1 = await bot.wait_for("message", check=chk1, timeout=30)
    except asyncio.TimeoutError:
        await ctx.reply("⏰ Timed out.")
        return
    if r1.content.lower() == "cancel":
        await ctx.reply("✅ Cancelled.")
        return

    await ctx.reply(embed=discord.Embed(
        title="⚠️  Final Confirmation",
        description="Type `DELETE` (all caps) to confirm. This cannot be undone.",
        color=0xE74C3C,
    ))
    def chk2(m):
        return m.author == ctx.author and m.channel == ctx.channel
    try:
        r2 = await bot.wait_for("message", check=chk2, timeout=30)
    except asyncio.TimeoutError:
        await ctx.reply("⏰ Timed out.")
        return
    if r2.content.strip() != "DELETE":
        await ctx.reply("✅ Cancelled.")
        return

    await col_orders.delete_many({})
    await col_blacklist.delete_many({})

    await send_log(discord.Embed(
        title="🗑️  Full History Wiped",
        description=f"**By:** {ctx.author.mention} — {order_count} orders, {bl_count} blacklist",
        color=0xE74C3C,
    ))
    await ctx.reply(embed=discord.Embed(
        title="✅  History Cleared",
        description=(
            f"🗂️ **{order_count}** orders deleted\n"
            f"🚫 **{bl_count}** blacklist entries deleted\n\n"
            "Categories, stock, settings and ToC are untouched."
        ),
        color=0x7B2FBE,
    ))





# ═══════════════════════════════════════════════════════════════════════════════
#  &setminqty <category> <minimum>
# ═══════════════════════════════════════════════════════════════════════════════
@bot.command(name="setminqty")
async def cmd_setminqty(ctx: commands.Context, category: str, minimum: int):
    """Admin: Set the minimum purchase quantity for a product.
    Usage: &setminqty netflix-1-month 3"""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    if minimum < 1:
        await ctx.reply(embed=discord.Embed(
            title="⚠️  Invalid Value",
            description="Minimum quantity must be **1** or greater.",
            color=0xE74C3C,
        ))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(embed=discord.Embed(
            title="❌  Product Not Found",
            description=f"No product matched `{category}`.\nUse `&stock` to see all available products.",
            color=0xE74C3C,
        ))
        return
    old = int(cat.get("min_quantity") or 1)
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"min_quantity": minimum}})
    await send_log(discord.Embed(
        title="📦  Minimum Quantity Updated",
        description=f"**Product:** {cat['name']}\n**Changed:** {old} → {minimum}\n**By:** {ctx.author.mention}",
        color=0x9B59B6,
    ))
    embed = discord.Embed(
        title="✅  Minimum Quantity Updated",
        description=f"Buyers must now order at least **{minimum}** unit(s) of **{cat['name']}**.",
        color=0x7B2FBE,
    )
    embed.add_field(name="📦  Product",      value=cat["name"],  inline=True)
    embed.add_field(name="📉  Previous Min", value=str(old),     inline=True)
    embed.add_field(name="📈  New Min",      value=str(minimum), inline=True)
    embed.set_footer(text="Changes take effect immediately for all new orders.")
    await ctx.reply(embed=embed)
    asyncio.create_task(auto_update_panel())


# ═══════════════════════════════════════════════════════════════════════════════
#  &setinstruction <category> <text|clear>
# ═══════════════════════════════════════════════════════════════════════════════
@bot.command(name="setinstruction")
async def cmd_setinstruction(ctx: commands.Context, category: str, *, instruction: str):
    """Admin: Edit the delivery instructions for a category.
    Usage:  &setinstruction netflix Login at netflix.com with the credentials below.
            &setinstruction netflix clear    ← removes instructions"""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    cat = await db_find_category(category)
    if not cat:
        await ctx.reply(embed=discord.Embed(
            title="❌  Product Not Found",
            description=f"No product matched `{category}`.\nUse `&stock` to see all available products.",
            color=0xE74C3C,
        ))
        return
    if instruction.strip().lower() == "clear":
        await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"instruction": ""}})
        await send_log(discord.Embed(
            title="📌  Delivery Instructions Cleared",
            description=f"**Product:** {cat['name']}\n**By:** {ctx.author.mention}",
            color=0xA855F7,
        ))
        await ctx.reply(embed=discord.Embed(
            title="✅  Instructions Cleared",
            description=f"Delivery instructions for **{cat['name']}** have been removed.",
            color=0x7B2FBE,
        ))
        return
    if len(instruction) > 1024:
        await ctx.reply(embed=discord.Embed(
            title="⚠️  Text Too Long",
            description="Instruction text exceeds the **1,024 character** limit. Please shorten and try again.",
            color=0xE74C3C,
        ))
        return
    old = cat.get("instruction") or "*(none)*"
    await col_cats.update_one({"slug": cat["slug"]}, {"$set": {"instruction": instruction}})
    embed = discord.Embed(
        title="✅  Delivery Instructions Updated",
        description=f"Buyers will see this message after their items are delivered for **{cat['name']}**.",
        color=0x7B2FBE,
    )
    embed.add_field(name="📦  Product",          value=cat["name"],                                      inline=False)
    embed.add_field(name="🗑️  Previous",         value=(old[:200] if old != "*(none)*" else "*(none)*"), inline=False)
    embed.add_field(name="📝  New Instructions", value=instruction[:200],                                 inline=False)
    embed.set_footer(text="This message is shown to the buyer upon delivery  •  &setinstruction <name> clear to remove")
    await ctx.reply(embed=embed)
    await send_log(discord.Embed(
        title="📌  Delivery Instructions Updated",
        description=f"**Product:** {cat['name']}\n**By:** {ctx.author.mention}\n\n{instruction[:400]}",
        color=0x9B59B6,
    ))


# ═══════════════════════════════════════════════════════════════════════════════
#  &orderitems <order_id>  — view delivered items for an order
# ═══════════════════════════════════════════════════════════════════════════════
@bot.command(name="orderitems")
async def cmd_orderitems(ctx: commands.Context, order_id: str):
    """Admin: View the exact items that were delivered for an order.
    Usage: &orderitems ORD-1234567890-ABCDEF"""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(title="🔒  Access Denied", description="This command is restricted to administrators.", color=0xE74C3C))
        return
    order = await db_get_order(order_id)
    if not order:
        await ctx.reply(embed=discord.Embed(
            title="❌  Order Not Found",
            description=f"No order was found with ID `{order_id}`.\nPlease verify the ID and try again.",
            color=0xE74C3C,
        ))
        return

    delivered_items = order.get("deliveredItems", [])
    status_icons = {
        "pending":                "🟡",
        "awaiting_confirmation":  "🔵",
        "delivered":              "🟢",
        "cancelled":              "🔴",
        "expired":                "⚫",
        "error":                  "🟠",
        "refunded":               "🟠",
    }
    status_label = f"{status_icons.get(order['status'], '⚪')}  {order['status'].replace('_', ' ').title()}"

    embed = discord.Embed(
        title=f"📦  Delivered Items — Order Lookup",
        description=f"Showing delivery record for order `{order_id}`",
        color=0x9B59B6,
    )
    embed.add_field(name="👤  Buyer",       value=f"<@{order['userId']}>",          inline=True)
    embed.add_field(name="🛍️  Product",    value=order["categoryName"],              inline=True)
    embed.add_field(name="📊  Status",      value=status_label,                      inline=True)
    embed.add_field(name="🔢  Quantity",    value=str(order["quantity"]),            inline=True)
    embed.add_field(name="💰  Order Total", value=f"${order['totalUSD']:.2f} USD",  inline=True)
    embed.add_field(name="🕐  Paid At",     value=(order.get("paidAt") or "N/A")[:19], inline=True)

    if not delivered_items:
        embed.add_field(
            name="📋  Items Delivered",
            value="*No delivery record found. The order may not have been fulfilled yet.*",
            inline=False,
        )
    elif len(delivered_items) <= 10:
        embed.add_field(
            name=f"📋  Items Delivered ({len(delivered_items)})",
            value="\n".join(f"`{item}`" for item in delivered_items)[:1024],
            inline=False,
        )
    else:
        import io as _io
        file_bytes = "\n".join(delivered_items).encode("utf-8")
        f = discord.File(fp=_io.BytesIO(file_bytes), filename=f"delivered-{order_id}.txt")
        embed.add_field(
            name=f"📋  Items Delivered ({len(delivered_items)})",
            value="Too many items to display inline — see the attached file.",
            inline=False,
        )
        embed.set_footer(text=f"Order ID: {order_id}")
        await ctx.reply(embed=embed, file=f)
        return

    embed.set_footer(text=f"Order ID: {order_id}")
    await ctx.reply(embed=embed)


# ═══════════════════════════════════════════════════════════════════════════════
#  RUN
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
#  EMOJI SETUP — bulk-upload all custom emojis used in bot embeds
# ═══════════════════════════════════════════════════════════════════════════════

# Master list of every custom emoji the bot uses in its embeds.
# Add new entries here whenever a new embed emoji is needed.
# Format: { "name": "discord_emoji_name", "url": "image_url" }
BOT_EMBED_EMOJIS: list[dict] = [
    # ── Info / UI ──────────────────────────────────────────────────────────────
    {
        "name": "bot_info",
        "url":  "https://i.ibb.co/C3n6RWX5/1000272252-removebg-preview.png",
    },
    # ── Status indicators ──────────────────────────────────────────────────────
    {
        "name": "bot_online",
        "url":  "https://cdn.discordapp.com/emojis/852541394145427456.png",   # green circle
    },
    # ── Payments ──────────────────────────────────────────────────────────────
    {
        "name": "bot_ltc",
        "url":  "https://cryptologos.cc/logos/litecoin-ltc-logo.png",
    },
    # ── Shopping cart ─────────────────────────────────────────────────────────
    {
        "name": "bot_cart",
        "url":  "https://cdn-icons-png.flaticon.com/512/3144/3144456.png",
    },
    # ── Payment status cards ─────────────────────────────────────────────────
    {
        "name": "bot_loading",
        "url":  LOADING_IMAGE_URL,
    },
    {
        "name": "bot_confirmed",
        "url":  CONFIRMED_IMAGE_URL,
    },
    # ── Order Finalized card ──────────────────────────────────────────────────
    {
        "name": "bot_noentry",
        "url":  "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f6ab.png",
    },
    {
        "name": "bot_pin",
        "url":  "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f4cc.png",
    },
    {
        "name": "bot_assistance",
        "url":  "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f9d1-200d-1f4bc.png",
    },
    {
        "name": "bot_sweep",
        "url":  "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f9f9.png",
    },
    {
        "name": "bot_lock",
        "url":  "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f512.png",
    },
]


EMOJI_PROMPT_TIMEOUT = 60  # seconds to wait for admin's manual image per emoji


async def _resolve_emoji_image_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.read()
    except Exception:
        return None


@bot.command(name="setupemojis")
async def cmd_setupemojis(ctx: commands.Context):
    """Admin: Set up bot embed emojis one by one.
    Usage: &setupemojis
    For each emoji, reply in this channel with an image attachment, an image URL,
    or an existing emoji (e.g. one already added to this server) within 60 seconds.
    Type `skip` (or just wait out the timeout) to use the default image instead.
    Safe to run multiple times — already-existing emojis are skipped."""
    if not is_admin(ctx):
        await ctx.reply(embed=discord.Embed(
            title="🔒  Access Denied",
            description="This command is restricted to administrators.",
            color=0xE74C3C,
        ))
        return

    if not ctx.guild:
        await ctx.reply("This command can only be used inside a server.")
        return

    guild = ctx.guild
    existing_names = {e.name for e in guild.emojis}
    results: list[str] = []

    intro_embed = discord.Embed(
        title="Emoji Setup",
        description=(
            f"Setting up **{len(BOT_EMBED_EMOJIS)}** emoji(s), one at a time.\n"
            "For each one, send an **image attachment, image URL, or an existing emoji** "
            f"in this channel within **{EMOJI_PROMPT_TIMEOUT}s**, or type `skip` to use the default image."
        ),
        color=0x1A1A2E,
    )
    intro_embed.set_author(name="Emoji Setup", icon_url=INFO_IMAGE_URL)
    intro_embed.set_thumbnail(url=INFO_IMAGE_URL)
    await ctx.reply(embed=intro_embed)

    def check(m: discord.Message):
        return m.channel.id == ctx.channel.id and m.author.id == ctx.author.id

    async with aiohttp.ClientSession() as session:
        for entry in BOT_EMBED_EMOJIS:
            name        = entry["name"]
            default_url = entry["url"]

            if name in existing_names:
                results.append(f"⏭️  `:{name}:` — already exists, skipped")
                continue

            ask_embed = discord.Embed(
                title=f"Emoji: {name}",
                description=(
                    f"Send an image (attachment or URL), or an existing emoji, for `:{name}:` now, "
                    f"or type `skip` to use the default.\n**{EMOJI_PROMPT_TIMEOUT}s to respond.**"
                ),
                color=0x1A1A2E,
            )
            ask_embed.set_author(name="Emoji Setup", icon_url=INFO_IMAGE_URL)
            ask_embed.set_thumbnail(url=default_url)
            await ctx.send(embed=ask_embed)

            source_url  = default_url
            used_custom = False
            try:
                msg = await bot.wait_for("message", check=check, timeout=EMOJI_PROMPT_TIMEOUT)
                raw = msg.content.strip()
                emoji_match = re.fullmatch(r"<(a?):(\w+):(\d+)>", raw)
                if msg.attachments:
                    # Attachment sent — grab its file and use it as the emoji image
                    source_url  = msg.attachments[0].url
                    used_custom = True
                elif emoji_match:
                    # An existing emoji (already added to the server, or any custom emoji
                    # the user has access to) was sent directly — reuse its image.
                    animated   = emoji_match.group(1) == "a"
                    emoji_id   = emoji_match.group(3)
                    ext        = "gif" if animated else "png"
                    source_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{ext}"
                    used_custom = True
                elif raw.lower() != "skip" and raw.startswith(("http://", "https://")):
                    source_url  = raw
                    used_custom = True
                # anything else (e.g. "skip" or gibberish) falls back to default_url
            except asyncio.TimeoutError:
                pass  # no response — use default

            image_bytes = await _resolve_emoji_image_bytes(session, source_url)
            if image_bytes is None and used_custom:
                # custom image failed to fetch — fall back to default
                image_bytes = await _resolve_emoji_image_bytes(session, default_url)
                used_custom = False

            if image_bytes is None:
                results.append(f"❌  `:{name}:` — could not fetch image (custom or default)")
                continue

            try:
                emoji = await guild.create_custom_emoji(name=name, image=image_bytes)
                tag   = "custom image" if used_custom else "default image"
                results.append(f"✅  `:{name}:` — uploaded as {emoji} ({tag})")
                existing_names.add(name)
            except discord.Forbidden:
                results.append(f"❌  `:{name}:` — bot lacks Manage Emojis permission")
            except discord.HTTPException as e:
                results.append(f"❌  `:{name}:` — Discord error: {e}")
            except Exception as e:
                results.append(f"❌  `:{name}:` — {e}")

    done_embed = discord.Embed(
        title="Emoji Setup Complete",
        description="\n".join(results) or "No emojis to process.",
        color=0x1A1A2E,
    )
    done_embed.set_author(name="Emoji Setup", icon_url=INFO_IMAGE_URL)
    done_embed.set_thumbnail(url=INFO_IMAGE_URL)
    done_embed.set_footer(text="Run &setupemojis again anytime to fill in remaining or new emojis.")
    await ctx.send(embed=done_embed)


bot.run(DISCORD_TOKEN)
