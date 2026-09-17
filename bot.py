"""
Discord Auto Middleman Bot (LTC)
Production-ready, single-file implementation using discord.py 2.x
Supports: MongoDB persistence, Aprione LTC API, persistent views, full AutoMM flow
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re as _re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import html as html_lib

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from motor.motor_asyncio import AsyncIOMotorClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config loader — config.py values take priority over secrets/env vars
# ---------------------------------------------------------------------------

try:
    import config as _cfg_file  # type: ignore
except ImportError:
    _cfg_file = None


def _cfg(key: str, default: Optional[str] = None) -> Optional[str]:
    if _cfg_file is not None:
        val = getattr(_cfg_file, key, None)
        if val:                        # non-empty string in config.py wins
            return str(val)
    return os.environ.get(key, default) or default


def _cfg_required(key: str) -> str:
    val = _cfg(key)
    if not val:
        raise RuntimeError(f"Missing required setting: {key}")
    return val


# ---------------------------------------------------------------------------
# Environment / secrets
# ---------------------------------------------------------------------------

DISCORD_TOKEN: str        = _cfg_required("DISCORD_TOKEN")
MONGODB_URI: str          = _cfg_required("MONGODB_URI")
APRIONE_ACCOUNT: str      = _cfg_required("APRIONE_ACCOUNT")
APRIONE_TRANSFER_KEY: str = _cfg_required("APRIONE_TRANSFER_KEY")
LOG_LEVEL: str            = (_cfg("LOG_LEVEL") or "INFO").upper()

ADMIN_ROLE_ID: Optional[int] = int(_cfg("ADMIN_ROLE_ID")) if _cfg("ADMIN_ROLE_ID") else None
USER_ROLE_ID:  Optional[int] = int(_cfg("USER_ROLE_ID"))  if _cfg("USER_ROLE_ID")  else None
LOG_CHANNEL_ID: Optional[int] = int(_cfg("LOG_CHANNEL_ID")) if _cfg("LOG_CHANNEL_ID") else None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("automm")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APRIONE_BASE = "https://apirone.com/api/v2"
REQUIRED_CONFIRMATIONS = 2
POLL_INTERVAL = 30          # seconds between payment polls
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2


# ---------------------------------------------------------------------------
# Transaction stage labels
# ---------------------------------------------------------------------------

class Stage:
    ROLE_SELECT    = "role_select"
    TOS            = "tos"
    AMOUNT         = "amount"
    DEPOSIT        = "deposit"
    AWAITING_FUNDS = "awaiting_funds"
    DELIVERY       = "delivery"
    RELEASE        = "release"
    WITHDRAWAL     = "withdrawal"
    FEEDBACK       = "feedback"
    COMPLETED      = "completed"
    CANCELLED      = "cancelled"
    DISPUTED       = "disputed"

    ACTIVE_STAGES = (
        ROLE_SELECT, TOS, AMOUNT, DEPOSIT, AWAITING_FUNDS,
        DELIVERY, RELEASE, WITHDRAWAL, FEEDBACK,
    )


# ---------------------------------------------------------------------------
# Embed colour palette
# ---------------------------------------------------------------------------

COLOR_PRIMARY = 0x9B59B6
COLOR_SUCCESS = 0x9B59B6
COLOR_WARNING = 0x9B59B6
COLOR_DANGER  = 0x9B59B6
COLOR_INFO    = 0x9B59B6


# Thumbnail / icon URLs used in payment embeds
SPINNER_GIF    = "https://i.ibb.co/N6bzmCB1/k-Onzy.gif"
CHECKMARK_IMG  = "https://i.ibb.co/235dBzmj/green-check-mark-with-round-outline-free-png.png"
BADGE_IMG      = "https://i.ibb.co/bMmz2dZ9/1000271835-removebg-preview.png"
LTC_LOGO       = "https://s2.coinmarketcap.com/static/img/coins/64x64/2.png"


def make_embed(
    title: str,
    description: str = "",
    color: int = COLOR_PRIMARY,
    fields: Optional[list[tuple[str, str, bool]]] = None,
    footer: Optional[str] = None,
    footer_icon_url: Optional[str] = None,
    timestamp: bool = True,
    thumbnail_url: Optional[str] = None,
) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    if timestamp:
        embed.timestamp = datetime.now(timezone.utc)
    for name, value, inline in (fields or []):
        embed.add_field(name=name, value=str(value), inline=inline)
    if footer:
        embed.set_footer(text=footer, icon_url=footer_icon_url or discord.utils.MISSING)
    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)
    return embed


# ---------------------------------------------------------------------------
# Database (MongoDB via motor)
# ---------------------------------------------------------------------------

class Database:
    """Async MongoDB wrapper."""

    def __init__(self, uri: str) -> None:
        self.client = AsyncIOMotorClient(uri)
        db = self.client["automm"]
        self.transactions = db["transactions"]
        self.users        = db["users"]
        self.feedback     = db["feedback"]
        self.settings     = db["settings"]
        self.blacklist    = db["blacklist"]
        self.logs         = db["logs"]

    async def setup_indexes(self) -> None:
        await self.transactions.create_index("transaction_id", unique=True)
        await self.transactions.create_index("channel_id")
        await self.transactions.create_index("stage")
        await self.users.create_index("user_id", unique=True)
        await self.blacklist.create_index("user_id", unique=True)
        await self.logs.create_index("timestamp")
        # Unique compound index prevents duplicate feedback inserts under race conditions
        await self.feedback.create_index(
            [("transaction_id", 1), ("reviewer_id", 1)], unique=True
        )
        log.info("Database indexes ensured.")

    # --- Transactions ---

    async def create_transaction(self, data: dict) -> None:
        await self.transactions.insert_one(data)

    async def get_transaction(self, transaction_id: str) -> Optional[dict]:
        return await self.transactions.find_one({"transaction_id": transaction_id})

    async def get_transaction_by_channel(self, channel_id: int) -> Optional[dict]:
        return await self.transactions.find_one({"channel_id": channel_id})

    async def update_transaction(self, transaction_id: str, update: dict) -> None:
        update["updated_at"] = datetime.now(timezone.utc)
        await self.transactions.update_one(
            {"transaction_id": transaction_id},
            {"$set": update},
        )

    async def get_active_transactions(self) -> list[dict]:
        cursor = self.transactions.find({"stage": {"$in": list(Stage.ACTIVE_STAGES)}})
        return await cursor.to_list(length=None)

    async def get_user_active_transaction(self, user_id: int) -> Optional[dict]:
        return await self.transactions.find_one({
            "stage": {"$in": list(Stage.ACTIVE_STAGES)},
            "$or": [
                {"sender_id": user_id},
                {"receiver_id": user_id},
                {"initiator_id": user_id},
                {"other_id": user_id},
            ],
        })

    # --- Users ---

    async def get_user(self, user_id: int) -> dict:
        doc = await self.users.find_one({"user_id": user_id})
        if doc is None:
            doc = {
                "user_id": user_id,
                "completed_deals": 0,
                "total_volume_usd": 0.0,
                "total_volume_ltc": 0.0,
                "feedback_count": 0,
                "rating_sum": 0,
                "average_rating": 0.0,
                "created_at": datetime.now(timezone.utc),
            }
            try:
                await self.users.insert_one(doc)
            except Exception:
                pass
        return doc

    async def update_user(self, user_id: int, update: dict) -> None:
        await self.users.update_one({"user_id": user_id}, {"$set": update}, upsert=True)

    async def increment_user(self, user_id: int, inc: dict) -> None:
        await self.users.update_one({"user_id": user_id}, {"$inc": inc}, upsert=True)

    # --- Feedback ---

    async def add_feedback(self, data: dict) -> None:
        await self.feedback.insert_one(data)

    async def get_user_feedback(self, user_id: int) -> list[dict]:
        cursor = self.feedback.find({"target_id": user_id}).sort("created_at", -1)
        return await cursor.to_list(length=None)

    # --- Settings ---

    async def get_setting(self, key: str) -> Optional[Any]:
        doc = await self.settings.find_one({"key": key})
        return doc["value"] if doc else None

    async def set_setting(self, key: str, value: Any) -> None:
        await self.settings.update_one(
            {"key": key}, {"$set": {"key": key, "value": value}}, upsert=True
        )

    # --- Blacklist ---

    async def blacklist_user(self, user_id: int, reason: str, mod_id: int) -> None:
        await self.blacklist.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "reason": reason,
                "mod_id": mod_id,
                "created_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )

    async def unblacklist_user(self, user_id: int) -> None:
        await self.blacklist.delete_one({"user_id": user_id})

    async def is_blacklisted(self, user_id: int) -> bool:
        return bool(await self.blacklist.find_one({"user_id": user_id}))

    # --- Logs ---

    async def add_log(self, event: str, data: dict) -> None:
        await self.logs.insert_one({
            "event": event,
            "data": data,
            "timestamp": datetime.now(timezone.utc),
        })

    # --- Statistics ---

    async def get_global_stats(self) -> dict:
        total     = await self.transactions.count_documents({})
        completed = await self.transactions.count_documents({"stage": Stage.COMPLETED})
        cancelled = await self.transactions.count_documents({"stage": Stage.CANCELLED})
        disputed  = await self.transactions.count_documents({"stage": Stage.DISPUTED})
        pipeline  = [{"$group": {"_id": None,
                                  "usd": {"$sum": "$amount_usd"},
                                  "ltc": {"$sum": "$amount_ltc"}}}]
        vol = await self.transactions.aggregate(pipeline).to_list(1)
        return {
            "total": total, "completed": completed,
            "cancelled": cancelled, "disputed": disputed,
            "total_usd": vol[0]["usd"] if vol else 0.0,
            "total_ltc": vol[0]["ltc"] if vol else 0.0,
        }


# ---------------------------------------------------------------------------
# Aprione API client
# ---------------------------------------------------------------------------

class AprionClient:
    """Async Aprione LTC API wrapper with exponential backoff."""

    def __init__(self, account: str, transfer_key: str) -> None:
        self.account = account
        self.transfer_key = transfer_key
        self._session: Optional[aiohttp.ClientSession] = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        session = await self._session_get()
        url = f"{APRIONE_BASE}{path}"
        last_exc: Exception = RuntimeError("No attempts made")
        for attempt in range(MAX_RETRIES):
            try:
                async with session.request(
                    method, url, timeout=aiohttp.ClientTimeout(total=15), **kwargs
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    text = await resp.text()
                    log.warning("Aprione %s %s → %s: %s", method, path, resp.status, text[:200])
                    last_exc = RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                log.warning("Aprione error attempt %d/%d: %s", attempt + 1, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
        raise last_exc

    async def create_address(self) -> str:
        """Create a unique LTC deposit address and return the address string."""
        data = await self._request(
            "POST",
            f"/accounts/{self.account}/addresses",
            json={"currency": "ltc"},
        )
        addr = data.get("address") or data.get("id") or data.get("addr")
        if not addr:
            raise RuntimeError(
                f"Apirone create_address returned no address field. Response: {data}"
            )
        return addr

    async def get_address_history(self, address: str) -> dict:
        # Do NOT pass ?currency=ltc — the address is already LTC-specific and
        # adding that param causes Apirone to return an empty txs list.
        return await self._request(
            "GET",
            f"/accounts/{self.account}/addresses/{address}/history",
        )

    async def get_ltc_price_usd(self) -> float:
        """Fetch live LTC/USD price from CoinGecko (no key required)."""
        session = await self._session_get()
        url = "https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=usd"
        last_exc: Exception = RuntimeError("No attempts made")
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        raise RuntimeError(
                            f"CoinGecko returned HTTP {resp.status}: {await resp.text()}"
                        )
                    data = await resp.json(content_type=None)
                    price = data.get("litecoin", {}).get("usd")
                    if price is None:
                        raise RuntimeError(
                            f"CoinGecko response missing litecoin.usd field: {data}"
                        )
                    return float(price)
            except Exception as exc:
                last_exc = exc
                log.warning("CoinGecko attempt %d/%d failed: %s", attempt + 1, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
        raise last_exc

    async def withdraw(self, destination: str, amount_ltc: float) -> dict:
        """
        Send exactly *amount_ltc* LTC to *destination*.
        Fee is subtracted FROM that amount, so the bot never sends more than received.
        Raises ValueError for dust-level amounts (< 1 000 sat).
        """
        amount_sat = int(round(amount_ltc * 1e8))
        if amount_sat < 1000:
            raise ValueError(
                f"Withdrawal too small: {amount_sat} sat ({amount_ltc:.8f} LTC). "
                "Minimum is 1 000 sat."
            )
        log.info(
            "Withdrawal → %s  amount=%.8f LTC (%d sat), fee subtracted from amount",
            destination, amount_ltc, amount_sat,
        )
        return await self._request(
            "POST",
            f"/accounts/{self.account}/transfer",
            json={
                "currency": "ltc",
                "transfer_key": self.transfer_key,
                "destinations": [{"address": destination, "amount": amount_sat}],
                "subtract_fee_from_amount": True,
            },
        )

    async def get_account_info(self) -> dict:
        """Return the Apirone account object (includes balance fields)."""
        return await self._request("GET", f"/accounts/{self.account}")

    @staticmethod
    def _extract_satoshis(value: Any) -> Optional[int]:
        """
        Try to extract a satoshi integer from *value*.
        Accepts: plain int, plain float, numeric string.
        Returns None if the value cannot be interpreted as satoshis.
        """
        if value is None:
            return None
        try:
            f = float(value)
            # Large values are already satoshis; small values are LTC — convert.
            return int(f) if f >= 1000 else int(round(f * 1e8))
        except (TypeError, ValueError):
            return None

    async def get_account_balance_ltc(self) -> tuple[float, dict]:
        """
        Return (spendable_ltc, raw_response) using the correct Apirone v2
        balance endpoint: GET /accounts/{id}/balance?currency=ltc

        Confirmed live response shape:
          {"account": "...", "balance": [{"currency": "ltc", "available": 1250968, "total": 1250968}]}
        """
        data = await self._request(
            "GET",
            f"/accounts/{self.account}/balance",
            params={"currency": "ltc"},
        )
        log.info("Apirone balance response: %s", data)

        sat: int = 0
        bal = data.get("balance", [])

        if isinstance(bal, list):
            for entry in bal:
                if isinstance(entry, dict) and entry.get("currency", "").lower() == "ltc":
                    sat = int(entry.get("available") or entry.get("total") or 0)
                    break
        elif isinstance(bal, dict):
            # Defensive: some future shape might return a plain dict
            sat = int(bal.get("available") or bal.get("total") or 0)

        ltc = sat / 1e8
        log.info("LTC balance: %d sat → %.8f LTC", sat, ltc)
        return ltc, data

    async def sweep_all(self, destination: str, balance_sat: Optional[int] = None) -> dict:
        """
        Sweep funds to *destination*.
        If *balance_sat* is provided, use it explicitly (most reliable).
        Otherwise fall back to the 'all' keyword and let Apirone decide.
        """
        amount: Any = balance_sat if (balance_sat and balance_sat > 0) else "all"
        return await self._request(
            "POST",
            f"/accounts/{self.account}/transfer",
            json={
                "currency": "ltc",
                "transfer_key": self.transfer_key,
                "destinations": [{"address": destination, "amount": amount}],
                "subtract_fee_from_amount": True,
            },
        )

    @staticmethod
    def validate_ltc_address(address: str) -> bool:
        addr = address.strip()
        if not addr:
            return False
        # LTC mainnet: starts with L, M, 3, or ltc1; length 26-90
        if addr.startswith(("L", "M", "3", "ltc1")) and 26 <= len(addr) <= 90:
            return True
        return False

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Base view: persistent, re-validates from DB before every action
# ---------------------------------------------------------------------------

class BaseView(discord.ui.View):
    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        self.transaction_id = transaction_id

    async def get_tx(self) -> Optional[dict]:
        return await self.bot.db.get_transaction(self.transaction_id)

    async def require_stage(
        self,
        interaction: discord.Interaction,
        *stages: str,
    ) -> Optional[dict]:
        tx = await self.get_tx()
        if tx is None:
            await interaction.response.send_message(
                "❌ Transaction not found.", ephemeral=True
            )
            return None
        if tx["stage"] not in stages:
            await interaction.response.send_message(
                "❌ This action is no longer available at this stage.", ephemeral=True
            )
            return None
        if tx.get("frozen") and not is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ This trade is frozen. Contact an admin.", ephemeral=True
            )
            return None
        return tx

    async def require_participant(
        self,
        interaction: discord.Interaction,
        tx: dict,
    ) -> bool:
        uid = interaction.user.id
        if uid not in (
            tx.get("sender_id"),
            tx.get("receiver_id"),
            tx.get("initiator_id"),
            tx.get("other_id"),
        ):
            await interaction.response.send_message(
                "❌ Only trade participants may use these buttons.", ephemeral=True
            )
            return False
        return True

    def disable_all(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Helper: cancel trade
# ---------------------------------------------------------------------------

async def do_cancel(
    bot: "AutoMMBot",
    transaction_id: str,
    cancelled_by: discord.Member,
    reason: str,
    interaction: Optional[discord.Interaction] = None,
    channel: Optional[discord.TextChannel] = None,
    outcome: str = "cancelled",
) -> None:
    await bot.db.update_transaction(transaction_id, {
        "stage": Stage.CANCELLED,
        "cancelled_by": cancelled_by.id,
        "cancel_reason": reason,
        "cancelled_at": datetime.now(timezone.utc),
    })
    embed = make_embed(
        title="❌ Trade Cancelled",
        description=f"**Reason:** {reason}",
        color=COLOR_DANGER,
        fields=[("Transaction ID", f"`{transaction_id}`", False)],
    )
    if interaction:
        try:
            await interaction.response.edit_message(embed=embed, view=None)
        except Exception:
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(embed=embed)
                else:
                    ch = channel or interaction.channel
                    if ch:
                        await ch.send(embed=embed)
            except Exception:
                pass
    elif channel:
        await channel.send(embed=embed)

    tx = await bot.db.get_transaction(transaction_id)
    if tx:
        guild = bot.get_guild(tx["guild_id"])
        if guild:
            await bot.post_log_embed(guild, make_embed(
                title="❌ Trade Cancelled",
                color=COLOR_DANGER,
                fields=[
                    ("Transaction ID", f"`{transaction_id}`", True),
                    ("Cancelled By",   cancelled_by.mention, True),
                    ("Reason",         reason, False),
                ],
            ))
    await bot.db.add_log("trade_cancelled", {
        "transaction_id": transaction_id,
        "cancelled_by": cancelled_by.id,
        "reason": reason,
    })

    # Send transcript on refund / cancel
    tx2 = await bot.db.get_transaction(transaction_id)
    if tx2:
        ch2: Optional[discord.TextChannel] = channel or (interaction.channel if interaction else None)
        if ch2 is None and tx2.get("channel_id"):
            guild2 = bot.get_guild(tx2["guild_id"])
            if guild2:
                ch2 = guild2.get_channel(tx2["channel_id"])  # type: ignore[assignment]
        if ch2:
            await send_deal_transcript(bot, ch2, tx2, outcome=outcome)


# ---------------------------------------------------------------------------
# Stage 1: Open AutoMM modal + panel button
# ---------------------------------------------------------------------------

class OpenAutoMMModal(discord.ui.Modal, title="Open AutoMM Trade"):
    other_user_id = discord.ui.TextInput(
        label="Other User's ID",
        placeholder="Right-click user → Copy ID",
        required=True,
        max_length=25,
    )

    def __init__(self, bot: "AutoMMBot") -> None:
        super().__init__()
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild     = interaction.guild
        initiator = interaction.user

        # Resolve other user
        try:
            other_id = int(self.other_user_id.value.strip())
        except ValueError:
            await interaction.followup.send("❌ Invalid user ID.", ephemeral=True)
            return

        try:
            other = guild.get_member(other_id) or await guild.fetch_member(other_id)
        except discord.NotFound:
            await interaction.followup.send(
                "❌ User not found in this server.", ephemeral=True
            )
            return

        if other.bot:
            await interaction.followup.send("❌ You cannot trade with a bot.", ephemeral=True)
            return
        if other.id == initiator.id:
            await interaction.followup.send(
                "❌ You cannot trade with yourself.", ephemeral=True
            )
            return

        # Blacklist check
        for uid in (initiator.id, other.id):
            if await self.bot.db.is_blacklisted(uid):
                await interaction.followup.send(
                    f"❌ <@{uid}> is blacklisted and cannot participate in trades.",
                    ephemeral=True,
                )
                return

        # Active trade check
        for uid in (initiator.id, other.id):
            existing = await self.bot.db.get_user_active_transaction(uid)
            if existing:
                await interaction.followup.send(
                    f"❌ <@{uid}> is already in an active trade (`{existing['transaction_id']}`).",
                    ephemeral=True,
                )
                return

        # Generate transaction ID
        transaction_id = str(uuid.uuid4())[:8].upper()

        # Create ticket channel
        category_id = await self.bot.db.get_setting("ticket_category_id")
        category = guild.get_channel(int(category_id)) if category_id else None

        overwrites: dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            initiator:          discord.PermissionOverwrite(view_channel=True, send_messages=True),
            other:              discord.PermissionOverwrite(view_channel=True, send_messages=True),
            guild.me:           discord.PermissionOverwrite(
                view_channel=True, send_messages=True, manage_channels=True
            ),
        }
        for role in guild.roles:
            if role.permissions.administrator:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True
                )

        ticket_ch = await guild.create_text_channel(
            name=f"trade-{transaction_id.lower()}",
            category=category,  # type: ignore[arg-type]
            overwrites=overwrites,
            reason=f"AutoMM trade {transaction_id}",
        )

        # Persist transaction document
        now = datetime.now(timezone.utc)
        await self.bot.db.create_transaction({
            "transaction_id":          transaction_id,
            "channel_id":              ticket_ch.id,
            "guild_id":                guild.id,
            "stage":                   Stage.ROLE_SELECT,
            "initiator_id":            initiator.id,
            "other_id":                other.id,
            "sender_id":               None,
            "receiver_id":             None,
            "amount_usd":              None,
            "amount_ltc":              None,
            "ltc_price":               None,
            "deposit_address":         None,
            "deposit_txid":            None,
            "deposit_confirmed":       False,
            "confirmations":           0,
            "wrong_amount_notified":   False,
            "withdrawal_address":      None,
            "withdrawal_txid":         None,
            "sender_tos":              None,
            "receiver_tos":            None,
            "sender_tos_accepted":     False,
            "receiver_tos_accepted":   False,
            "sender_confirmed":        False,
            "receiver_confirmed":      False,
            "release_confirmed":       False,
            "feedback_sent_sender":    False,
            "feedback_sent_receiver":  False,
            "role_select_message_id":  None,
            "deposit_message_id":      None,
            "frozen":                  False,
            "created_at":              now,
            "updated_at":              now,
        })

        await interaction.followup.send(
            f"✅ Trade ticket created: {ticket_ch.mention}", ephemeral=True
        )

        # Notify both participants via DM that a ticket has been opened with them
        await self.bot.notify_user_ticket_opened(other, ticket_ch, transaction_id, initiator)
        await self.bot.notify_user_ticket_opened(initiator, ticket_ch, transaction_id, other)

        # Send role selection embed
        embed = make_embed(
            title=f"🔄 AutoMM Trade — `{transaction_id}`",
            description=(
                f"**Participants:** {initiator.mention} & {other.mention}\n\n"
                "Select your role, then both participants press **Confirm**.\n"
                "**Sender** pays LTC · **Receiver** delivers the goods/service."
            ),
            color=COLOR_PRIMARY,
            fields=[
                ("Sender Role",   "_Not selected_", True),
                ("Receiver Role", "_Not selected_", True),
                ("Transaction ID", f"`{transaction_id}`", False),
            ],
        )
        view = RoleSelectView(self.bot, transaction_id, initiator.id, other.id)
        msg = await ticket_ch.send(
            content=f"{initiator.mention} {other.mention}",
            embed=embed,
            view=view,
        )
        await self.bot.db.update_transaction(transaction_id, {"role_select_message_id": msg.id})
        await self.bot.db.add_log("trade_opened", {
            "transaction_id": transaction_id,
            "initiator_id":   initiator.id,
            "other_id":       other.id,
            "channel_id":     ticket_ch.id,
        })
        await self.bot.post_log_embed(guild, make_embed(
            title="📂 Trade Opened",
            color=COLOR_INFO,
            fields=[
                ("Transaction ID", f"`{transaction_id}`", True),
                ("Initiator",      initiator.mention, True),
                ("Counterparty",   other.mention, True),
                ("Channel",        ticket_ch.mention, False),
            ],
        ))


class PanelView(discord.ui.View):
    """Persistent Open AutoMM button displayed in the panel embed."""

    def __init__(self, bot: "AutoMMBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Open AutoMM",
        style=discord.ButtonStyle.primary,
        emoji="🔄",
        custom_id="panel_open_automm",
    )
    async def open_automm(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        # Check required user role (DB setting takes priority over config/env)
        raw = await self.bot.db.get_setting("user_role_id") or (
            str(USER_ROLE_ID) if USER_ROLE_ID else None
        )
        if raw:
            required_id = int(raw)
            if not any(r.id == required_id for r in interaction.user.roles):
                role = interaction.guild.get_role(required_id)
                mention = role.mention if role else f"<@&{required_id}>"
                await interaction.response.send_message(
                    f"❌ You do not have the required role {mention} to open a trade.",
                    ephemeral=True,
                )
                return
        await interaction.response.send_modal(OpenAutoMMModal(self.bot))


# ---------------------------------------------------------------------------
# Stage 2: Role selection
# ---------------------------------------------------------------------------

class RoleSelectView(BaseView):
    """Single embed view: role selection + confirmation in one place."""

    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        initiator_id: int,
        other_id: int,
    ) -> None:
        super().__init__(bot, transaction_id)
        self.initiator_id = initiator_id
        self.other_id     = other_id

        sender_btn = discord.ui.Button(
            label="Sender",
            style=discord.ButtonStyle.primary,
            emoji="💸",
            custom_id=f"rs_sender_{transaction_id}",
        )
        sender_btn.callback = self._on_sender
        self.add_item(sender_btn)

        receiver_btn = discord.ui.Button(
            label="Receiver",
            style=discord.ButtonStyle.secondary,
            emoji="📦",
            custom_id=f"rs_receiver_{transaction_id}",
        )
        receiver_btn.callback = self._on_receiver
        self.add_item(receiver_btn)

        confirm_btn = discord.ui.Button(
            label="Confirm",
            style=discord.ButtonStyle.success,
            emoji="✅",
            custom_id=f"rs_confirm_{transaction_id}",
        )
        confirm_btn.callback = self._on_confirm
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.danger,
            emoji="❌",
            custom_id=f"rs_cancel_{transaction_id}",
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    def _mention(self, guild: discord.Guild, mid: Optional[int]) -> str:
        if mid is None:
            return "_Not selected_"
        m = guild.get_member(mid)
        return m.mention if m else f"<@{mid}>"

    async def _refresh_embed(
        self,
        interaction: discord.Interaction,
        tx: dict,
        sender_id: Optional[int],
        receiver_id: Optional[int],
    ) -> None:
        """Edit the message in-place to show current role selections."""
        guild = interaction.guild
        if sender_id and receiver_id:
            title       = f"✅ Roles Selected — `{self.transaction_id}`"
            description = (
                "Both roles selected. "
                "Each participant must press **Confirm** to continue."
            )
            color = COLOR_SUCCESS
        else:
            title       = f"🔄 AutoMM Trade — `{self.transaction_id}`"
            description = (
                "Select your role, then both participants press **Confirm**.\n"
                "**Sender** pays LTC · **Receiver** delivers the goods/service."
            )
            color = COLOR_PRIMARY

        embed = make_embed(
            title=title,
            description=description,
            color=color,
            fields=[
                ("Sender Role",   self._mention(guild, sender_id),   True),
                ("Receiver Role", self._mention(guild, receiver_id), True),
                ("Transaction ID", f"`{self.transaction_id}`",       False),
            ],
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _pick_role(self, interaction: discord.Interaction, role: str) -> None:
        tx = await self.require_stage(interaction, Stage.ROLE_SELECT)
        if tx is None:
            return
        uid = interaction.user.id
        if uid not in (tx["initiator_id"], tx["other_id"]):
            await interaction.response.send_message(
                "❌ Only trade participants may choose roles.", ephemeral=True
            )
            return

        sender_id   = tx.get("sender_id")
        receiver_id = tx.get("receiver_id")

        if role == "sender":
            if sender_id is not None and sender_id != uid:
                await interaction.response.send_message("❌ Sender role already taken.", ephemeral=True)
                return
            if receiver_id == uid:
                await interaction.response.send_message("❌ You are already the Receiver.", ephemeral=True)
                return
            sender_id = uid
        else:
            if receiver_id is not None and receiver_id != uid:
                await interaction.response.send_message("❌ Receiver role already taken.", ephemeral=True)
                return
            if sender_id == uid:
                await interaction.response.send_message("❌ You are already the Sender.", ephemeral=True)
                return
            receiver_id = uid

        await self.bot.db.update_transaction(self.transaction_id, {
            "sender_id": sender_id, "receiver_id": receiver_id,
        })
        await self._refresh_embed(interaction, tx, sender_id, receiver_id)

    async def _on_sender(self, interaction: discord.Interaction) -> None:
        await self._pick_role(interaction, "sender")

    async def _on_receiver(self, interaction: discord.Interaction) -> None:
        await self._pick_role(interaction, "receiver")

    async def _on_confirm(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.ROLE_SELECT)
        if tx is None:
            return
        uid         = interaction.user.id
        sender_id   = tx.get("sender_id")
        receiver_id = tx.get("receiver_id")

        # Both roles must be filled first
        if not (sender_id and receiver_id):
            await interaction.response.send_message(
                "❌ Both participants must select their roles before confirming.", ephemeral=True
            )
            return

        if uid not in (sender_id, receiver_id):
            await interaction.response.send_message("❌ Not a participant.", ephemeral=True)
            return

        update: dict[str, Any] = (
            {"sender_confirmed": True} if uid == sender_id
            else {"receiver_confirmed": True}
        )
        await self.bot.db.update_transaction(self.transaction_id, update)
        tx = await self.get_tx()
        assert tx is not None

        if tx["sender_confirmed"] and tx["receiver_confirmed"]:
            await self.bot.db.update_transaction(self.transaction_id, {
                "stage": Stage.TOS,
                "sender_confirmed": False,
                "receiver_confirmed": False,
            })
            self.disable_all()
            await interaction.response.edit_message(
                embed=make_embed(
                    title="✅ Roles Confirmed",
                    color=COLOR_SUCCESS,
                ),
                view=self,
            )
            await _start_tos_stage(self.bot, interaction.channel, tx)
        else:
            who = "Sender" if uid == sender_id else "Receiver"
            await interaction.response.send_message(
                f"✅ {who} confirmed. Waiting for the other participant to also confirm.",
                ephemeral=True,
            )

    async def _on_cancel(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.ROLE_SELECT)
        if tx is None:
            return
        if not await self.require_participant(interaction, tx):
            return
        self.disable_all()
        await interaction.response.edit_message(view=self)
        await do_cancel(
            self.bot, self.transaction_id, interaction.user,
            f"Cancelled by {interaction.user.display_name}", channel=interaction.channel
        )


# ---------------------------------------------------------------------------
# Stage 3: Personal ToS
# ---------------------------------------------------------------------------

async def _start_tos_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    sid = tx["sender_id"]
    rid = tx["receiver_id"]
    tid = tx["transaction_id"]

    await channel.send(
        content=f"<@{sid}>",
        embed=make_embed(
            title="📦 Conditions — Sender",
            description="Set your conditions for this trade, or skip.",
            color=COLOR_PRIMARY,
        ),
        view=TosPromptView(bot, tid, sid, is_sender=True),
    )
    await channel.send(
        content=f"<@{rid}>",
        embed=make_embed(
            title="📋 Terms of Service — Receiver",
            description="Set your Terms of Service for this trade, or skip.",
            color=COLOR_PRIMARY,
        ),
        view=TosPromptView(bot, tid, rid, is_sender=False),
    )


async def _check_tos_complete(
    bot: "AutoMMBot", channel: discord.TextChannel, transaction_id: str
) -> None:
    """Advance to Amount stage once both participants resolved ToS."""
    tx = await bot.db.get_transaction(transaction_id)
    if tx is None or tx["stage"] != Stage.TOS:
        return

    sender_tos   = tx.get("sender_tos")
    receiver_tos = tx.get("receiver_tos")

    sender_done = sender_tos is not None and (
        sender_tos == "__skip__" or tx.get("receiver_tos_accepted")
    )
    receiver_done = receiver_tos is not None and (
        receiver_tos == "__skip__" or tx.get("sender_tos_accepted")
    )

    if sender_done and receiver_done:
        await bot.db.update_transaction(transaction_id, {"stage": Stage.AMOUNT})
        await _start_amount_stage(bot, channel, tx)


class TosModal(discord.ui.Modal, title="Set Conditions"):
    tos_text = discord.ui.TextInput(
        label="Details",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )

    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        user_id: int,
        is_sender: bool = False,
    ) -> None:
        if is_sender:
            super().__init__(title="Set Conditions")
            self.tos_text.label = "Your conditions for this trade"
        else:
            super().__init__(title="Set Terms of Service")
            self.tos_text.label = "Your Terms of Service"
        self.bot            = bot
        self.transaction_id = transaction_id
        self.user_id        = user_id
        self.is_sender      = is_sender

    async def on_submit(self, interaction: discord.Interaction) -> None:
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None:
            await interaction.response.send_message("❌ Transaction not found.", ephemeral=True)
            return

        is_sender  = self.user_id == tx["sender_id"]
        other_id   = tx["receiver_id"] if is_sender else tx["sender_id"]
        field_key  = "sender_tos" if is_sender else "receiver_tos"
        tos_text   = self.tos_text.value

        await self.bot.db.update_transaction(self.transaction_id, {field_key: tos_text})

        if is_sender:
            await interaction.response.send_message("✅ Conditions recorded.", ephemeral=True)
            await interaction.channel.send(
                content=f"<@{other_id}>",
                embed=make_embed(
                    title="📦 Sender's Conditions",
                    description=(
                        f"The **Sender** has set the following conditions:\n\n"
                        f"```\n{tos_text}\n```"
                    ),
                    color=COLOR_PRIMARY,
                ),
                view=TosAcceptView(self.bot, self.transaction_id, self.user_id, other_id, True),
            )
        else:
            await interaction.response.send_message("✅ Terms of Service recorded.", ephemeral=True)
            await interaction.channel.send(
                content=f"<@{other_id}>",
                embed=make_embed(
                    title="📋 Receiver's Terms of Service",
                    description=(
                        f"The **Receiver** has set the following Terms of Service:\n\n"
                        f"```\n{tos_text}\n```"
                    ),
                    color=COLOR_PRIMARY,
                ),
                view=TosAcceptView(self.bot, self.transaction_id, self.user_id, other_id, False),
            )


class TosPromptView(discord.ui.View):
    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        user_id: int,
        is_sender: bool = False,
        edit_mode: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.bot            = bot
        self.transaction_id = transaction_id
        self.user_id        = user_id
        self.is_sender      = is_sender

        set_btn = discord.ui.Button(
            label="Edit" if edit_mode else "Set",
            style=discord.ButtonStyle.primary,
            custom_id=f"tos_set_{transaction_id}_{user_id}",
        )
        set_btn.callback = self._on_set
        self.add_item(set_btn)

        skip_btn = discord.ui.Button(
            label="Skip",
            style=discord.ButtonStyle.secondary,
            custom_id=f"tos_skip_{transaction_id}_{user_id}",
        )
        skip_btn.callback = self._on_skip
        self.add_item(skip_btn)

    def _disable_all(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]

    async def _on_set(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not your prompt.", ephemeral=True)
            return
        tx = await self.bot.db.get_transaction_by_channel(interaction.channel_id)
        is_sender = bool(tx and self.user_id == tx["sender_id"])
        self._disable_all()
        await interaction.response.send_modal(
            TosModal(self.bot, self.transaction_id, self.user_id, is_sender=is_sender)
        )
        await interaction.message.edit(view=self)

    async def _on_skip(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not your prompt.", ephemeral=True)
            return
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None:
            return
        is_sender = self.user_id == tx["sender_id"]
        field = "sender_tos" if is_sender else "receiver_tos"
        await self.bot.db.update_transaction(self.transaction_id, {field: "__skip__"})
        self._disable_all()
        title = "📦 Conditions" if is_sender else "📋 Terms of Service"
        await interaction.response.edit_message(
            embed=make_embed(title, "Skipped.", color=COLOR_PRIMARY), view=self
        )
        await _check_tos_complete(self.bot, interaction.channel, self.transaction_id)


class TosAcceptView(discord.ui.View):
    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        tos_author_id: int,
        acceptor_id: int,
        author_is_sender: bool,
    ) -> None:
        super().__init__(timeout=None)
        self.bot              = bot
        self.transaction_id   = transaction_id
        self.tos_author_id    = tos_author_id
        self.acceptor_id      = acceptor_id
        self.author_is_sender = author_is_sender

        agree_btn = discord.ui.Button(
            label="Agree",
            style=discord.ButtonStyle.success,
            custom_id=f"tos_accept_{transaction_id}_{acceptor_id}",
        )
        agree_btn.callback = self._on_accept
        self.add_item(agree_btn)

        decline_btn = discord.ui.Button(
            label="Decline",
            style=discord.ButtonStyle.danger,
            custom_id=f"tos_decline_{transaction_id}_{acceptor_id}",
        )
        decline_btn.callback = self._on_decline
        self.add_item(decline_btn)

    def _disable_all(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]

    async def _on_accept(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.acceptor_id:
            await interaction.response.send_message("❌ Not your prompt.", ephemeral=True)
            return
        field = "receiver_tos_accepted" if self.author_is_sender else "sender_tos_accepted"
        await self.bot.db.update_transaction(self.transaction_id, {field: True})
        self._disable_all()
        title = "📦 Conditions" if self.author_is_sender else "📋 Terms of Service"
        await interaction.response.edit_message(
            embed=make_embed(title, "Agreed.", color=COLOR_PRIMARY), view=self
        )
        await _check_tos_complete(self.bot, interaction.channel, self.transaction_id)

    async def _on_decline(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.acceptor_id:
            await interaction.response.send_message("❌ Not your prompt.", ephemeral=True)
            return
        self._disable_all()
        title  = "📦 Conditions" if self.author_is_sender else "📋 Terms of Service"
        f_key  = "sender_tos"          if self.author_is_sender else "receiver_tos"
        a_key  = "receiver_tos_accepted" if self.author_is_sender else "sender_tos_accepted"
        await interaction.response.edit_message(
            embed=make_embed(title, "Declined.", color=COLOR_PRIMARY), view=self
        )
        # Reset the author's submission so they can edit and resubmit
        await self.bot.db.update_transaction(self.transaction_id, {f_key: None, a_key: False})
        # Re-prompt the author to edit or skip
        re_view = TosPromptView(
            self.bot, self.transaction_id, self.tos_author_id,
            is_sender=self.author_is_sender, edit_mode=True,
        )
        desc = (
            "Your conditions were declined. Edit and resubmit, or skip."
            if self.author_is_sender else
            "Your Terms of Service were declined. Edit and resubmit, or skip."
        )
        await interaction.channel.send(
            content=f"<@{self.tos_author_id}>",
            embed=make_embed(title, desc, color=COLOR_PRIMARY),
            view=re_view,
        )


# ---------------------------------------------------------------------------
# Emoji helpers — fetch configurable emojis from DB (with defaults)
# ---------------------------------------------------------------------------

async def _get_star_emoji(bot: "AutoMMBot") -> str:
    """Return the server's custom star emoji (default: 🌟)."""
    return await bot.db.get_setting("star_emoji") or "🌟"

async def _get_arrow_emoji(bot: "AutoMMBot") -> str:
    """Return the server's custom arrow emoji (default: >>)."""
    return await bot.db.get_setting("arrow_emoji") or ">>"

async def _get_dot_emoji(bot: "AutoMMBot") -> str:
    """Return the server's custom dot/bullet emoji (default: •)."""
    return await bot.db.get_setting("dot_emoji") or "•"


# ---------------------------------------------------------------------------
# Stage 4: Amount
# ---------------------------------------------------------------------------

async def _start_amount_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    view = AmountInputView(bot, tx["transaction_id"], tx["sender_id"])
    await channel.send(
        content=f"<@{tx['sender_id']}>",
        embed=make_embed(
            title="💵 Enter Trade Amount",
            description="**Sender**, press the button below to enter the USD amount for this trade.",
            color=COLOR_INFO,
        ),
        view=view,
    )


class AmountModal(discord.ui.Modal, title="Enter Trade Amount (USD)"):
    amount = discord.ui.TextInput(
        label="Amount in USD",
        placeholder="e.g. 150.00",
        required=True,
        max_length=15,
    )

    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__()
        self.bot            = bot
        self.transaction_id = transaction_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            amount_usd = float(
                self.amount.value.strip().replace("$", "").replace(",", "")
            )
            if amount_usd < 0.10:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid amount. Minimum trade amount is **$0.10 USD**.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            ltc_price = await self.bot.aprion.get_ltc_price_usd()
        except Exception:
            await interaction.followup.send(
                "❌ Could not fetch LTC price. Try again.", ephemeral=True
            )
            return

        amount_ltc = round(amount_usd / ltc_price, 8)
        tx = await self.bot.db.get_transaction(self.transaction_id)

        await self.bot.db.update_transaction(self.transaction_id, {
            "amount_usd": amount_usd,
            "amount_ltc": amount_ltc,
            "ltc_price":  ltc_price,
        })

        mm_name    = await self.bot.db.get_setting("mm_name") or "AutoMM"
        star       = await _get_star_emoji(self.bot)
        arr        = await _get_arrow_emoji(self.bot)
        guild_icon = (
            interaction.guild.icon.url
            if interaction.guild and interaction.guild.icon
            else None
        )

        view = AmountAgreeView(self.bot, self.transaction_id)
        await interaction.channel.send(
            content=f"<@{tx['sender_id']}> <@{tx['receiver_id']}>",
            embed=make_embed(
                title=f"{star} Deal Amount Confirmation {star}",
                description=(
                    f"{arr} **Amount : ${amount_usd:,.2f} USD**\n\n"
                    f"{arr} Accept Or Reject the Deal"
                ),
                color=COLOR_SUCCESS,
                thumbnail_url=guild_icon,
                footer=mm_name,
                footer_icon_url=BADGE_IMG,
            ),
            view=view,
        )
        await interaction.followup.send("✅ Amount set.", ephemeral=True)


class AmountInputView(discord.ui.View):
    def __init__(self, bot: "AutoMMBot", transaction_id: str, sender_id: int) -> None:
        super().__init__(timeout=None)
        self.bot            = bot
        self.transaction_id = transaction_id
        self.sender_id      = sender_id

        btn = discord.ui.Button(
            label="Enter Amount",
            style=discord.ButtonStyle.primary,
            emoji="💵",
            custom_id=f"amount_enter_{transaction_id}",
        )
        btn.callback = self._on_enter
        self.add_item(btn)

    async def _on_enter(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.sender_id:
            await interaction.response.send_message(
                "❌ Only the Sender may enter the amount.", ephemeral=True
            )
            return
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None or tx["stage"] != Stage.AMOUNT:
            await interaction.response.send_message("❌ Stage mismatch.", ephemeral=True)
            return
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]
        await interaction.response.send_modal(AmountModal(self.bot, self.transaction_id))
        await interaction.message.edit(view=self)


class AmountAgreeView(BaseView):
    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(bot, transaction_id)

        agree_btn = discord.ui.Button(
            label="Accept",
            style=discord.ButtonStyle.success,
            custom_id=f"amount_agree_{transaction_id}",
        )
        agree_btn.callback = self._on_agree
        self.add_item(agree_btn)

        cancel_btn = discord.ui.Button(
            label="Reject",
            style=discord.ButtonStyle.danger,
            custom_id=f"amount_cancel_{transaction_id}",
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _on_agree(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.AMOUNT)
        if tx is None:
            return
        if not await self.require_participant(interaction, tx):
            return
        uid = interaction.user.id
        update: dict[str, Any] = (
            {"sender_confirmed": True} if uid == tx["sender_id"]
            else {"receiver_confirmed": True}
        )
        await self.bot.db.update_transaction(self.transaction_id, update)
        tx = await self.get_tx()
        assert tx is not None

        if tx["sender_confirmed"] and tx["receiver_confirmed"]:
            await self.bot.db.update_transaction(self.transaction_id, {
                "stage": Stage.DEPOSIT,
                "sender_confirmed": False,
                "receiver_confirmed": False,
            })
            self.disable_all()
            await interaction.response.edit_message(view=self)
            await _start_deposit_stage(self.bot, interaction.channel, tx)
        else:
            await interaction.response.send_message(
                "✅ You agreed. Waiting for the other participant.", ephemeral=True
            )

    async def _on_cancel(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.AMOUNT)
        if tx is None:
            return
        if not await self.require_participant(interaction, tx):
            return
        self.disable_all()
        await interaction.response.edit_message(view=self)
        await do_cancel(
            self.bot, self.transaction_id, interaction.user,
            f"Cancelled by {interaction.user.display_name}", channel=interaction.channel
        )


# ---------------------------------------------------------------------------
# Stage 5: Deposit
# ---------------------------------------------------------------------------

async def _start_deposit_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    try:
        deposit_address = await bot.aprion.create_address()
    except Exception as exc:
        log.error("Failed to create deposit address for %s: %s", tx["transaction_id"], exc)
        await channel.send(
            embed=make_embed(
                title="❌ Address Generation Failed",
                description="Failed to generate a deposit address. Please contact an admin.",
                color=COLOR_DANGER,
            )
        )
        return

    await bot.db.update_transaction(tx["transaction_id"], {
        "deposit_address": deposit_address,
        "stage": Stage.AWAITING_FUNDS,
    })

    mm_name = await bot.db.get_setting("mm_name") or "AutoMM"
    star    = await _get_star_emoji(bot)
    arr     = await _get_arrow_emoji(bot)
    view = DepositView(bot, tx["transaction_id"], tx["sender_id"])
    msg = await channel.send(
        content=f"<@{tx['sender_id']}>",
        embed=make_embed(
            title=f"{star} Waiting For Payment {star}",
            description=(
                f"Payment Credentials are Given Below\n\n"
                f"{arr} **Address** : `{deposit_address}`\n"
                f"{arr} **Amount to pay** : {tx['amount_ltc']:.8f} LTC\n\n"
                f"Your payment will be Detected Automatically"
            ),
            color=COLOR_PRIMARY,
            thumbnail_url=LTC_LOGO,
            footer=mm_name,
            footer_icon_url=BADGE_IMG,
        ),
        view=view,
    )
    await bot.db.update_transaction(tx["transaction_id"], {"deposit_message_id": msg.id})

    # Start async payment monitoring
    bot.loop.create_task(bot.monitor_payment(tx["transaction_id"]))


class DepositView(BaseView):
    def __init__(
        self, bot: "AutoMMBot", transaction_id: str, sender_id: int
    ) -> None:
        super().__init__(bot, transaction_id)
        self.sender_id = sender_id

        copy_btn = discord.ui.Button(
            label="Copy Address",
            style=discord.ButtonStyle.primary,
            custom_id=f"dep_copy_{transaction_id}",
        )
        copy_btn.callback = self._on_copy
        self.add_item(copy_btn)

        qr_btn = discord.ui.Button(
            label="QR Code",
            style=discord.ButtonStyle.secondary,
            custom_id=f"dep_qr_{transaction_id}",
        )
        qr_btn.callback = self._on_qrcode
        self.add_item(qr_btn)

        cancel_btn = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.danger,
            custom_id=f"dep_cancel_{transaction_id}",
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _on_copy(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.sender_id:
            await interaction.response.send_message(
                "❌ Only the Sender may copy the address.", ephemeral=True
            )
            return
        tx = await self.get_tx()
        if tx is None:
            await interaction.response.send_message("❌ Transaction not found.", ephemeral=True)
            return
        if tx.get("deposit_confirmed"):
            await interaction.response.send_message(
                "❌ Payment already detected.", ephemeral=True
            )
            return
        await interaction.response.send_message(tx["deposit_address"])
        await interaction.followup.send(f"{tx['amount_ltc']:.8f}")

    async def _on_qrcode(self, interaction: discord.Interaction) -> None:
        tx = await self.get_tx()
        if tx is None:
            await interaction.response.send_message("❌ Transaction not found.", ephemeral=True)
            return
        address = tx.get("deposit_address", "")
        qr_url  = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={address}"
        embed   = discord.Embed(
            title="📱 QR Code — Scan to Pay",
            description=f"```\n{address}\n```",
            color=COLOR_INFO,
        )
        embed.set_image(url=qr_url)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def _on_cancel(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.AWAITING_FUNDS, Stage.DEPOSIT)
        if tx is None:
            return
        if tx.get("deposit_confirmed"):
            await interaction.response.send_message(
                "❌ Cannot cancel — payment already detected.", ephemeral=True
            )
            return
        if not await self.require_participant(interaction, tx):
            return
        self.disable_all()
        await interaction.response.edit_message(view=self)
        await do_cancel(
            self.bot, self.transaction_id, interaction.user,
            f"Cancelled by {interaction.user.display_name}", channel=interaction.channel
        )


# ---------------------------------------------------------------------------
# Stage 7: Payment monitoring
# ---------------------------------------------------------------------------

async def _poll_payment(bot: "AutoMMBot", transaction_id: str) -> bool:
    """
    Poll Aprione for a payment on the deposit address.
    Returns True once payment is fully confirmed.
    """
    tx = await bot.db.get_transaction(transaction_id)
    if tx is None or tx["stage"] not in (Stage.DEPOSIT, Stage.AWAITING_FUNDS):
        return True  # stop monitoring

    if tx.get("frozen"):
        return False

    address = tx.get("deposit_address")
    if not address:
        return False

    try:
        history = await bot.aprion.get_address_history(address)
    except Exception as exc:
        log.warning("Payment poll error for %s: %s", transaction_id, exc)
        return False

    # Aprione returns items under various keys depending on version/response shape.
    # Be defensive: accept a bare list, or a dict wrapping the list under any of
    # several plausible keys.
    items: list[dict]
    if isinstance(history, list):
        items = history
    elif isinstance(history, dict):
        items = (
            history.get("transactions")
            or history.get("data")
            or history.get("items")
            or history.get("history")
            or history.get("results")
            or history.get("txs")
            or history.get("addresses")
            or []
        )
        # Some shapes nest the list one level deeper, e.g. {"address": {...}, "transactions": [...]}
        if not items and isinstance(history.get("address"), dict):
            inner = history["address"]
            items = (
                inner.get("transactions")
                or inner.get("history")
                or inner.get("txs")
                or []
            )
    else:
        items = []

    if not items:
        # Log the raw payload on every poll so admins can diagnose
        # Apirone response-shape mismatches quickly.
        poll_count = tx.get("_poll_count", 0) + 1
        await bot.db.update_transaction(transaction_id, {"_poll_count": poll_count})
        log.info(
            "No payment items found yet for %s (address=%s, poll #%d). "
            "Raw Apirone response keys: %s | Full: %s",
            transaction_id, address, poll_count,
            list(history.keys()) if isinstance(history, dict) else type(history).__name__,
            str(history)[:800],
        )
        return False

    expected_ltc = tx["amount_ltc"]
    guild   = bot.get_guild(tx["guild_id"])
    channel = guild.get_channel(tx["channel_id"]) if guild else None

    _detected_this_poll = False  # guards against double-fire within one poll cycle
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_amount = (
            item.get("amount")
            if item.get("amount") is not None else
            item.get("value")
            if item.get("value") is not None else
            item.get("amount_ltc")
            if item.get("amount_ltc") is not None else
            item.get("received_amount")
            if item.get("received_amount") is not None else
            0
        )
        try:
            if isinstance(raw_amount, int):
                # Heuristic: large integers are satoshis, small ones are already LTC.
                amount_ltc = raw_amount / 1e8 if abs(raw_amount) >= 1000 else float(raw_amount)
            else:
                amount_ltc = float(raw_amount)
        except (TypeError, ValueError):
            amount_ltc = 0.0
        # Skip replaced / deleted transactions
        if item.get("deleted", False):
            continue

        txid = item.get("txid") or item.get("id") or item.get("hash") or item.get("tx_hash") or ""

        # Apirone signals confirmation via a non-null 'block' dict, NOT a numeric
        # confirmations count. Map: block present → fully confirmed, absent → 0.
        block_info = item.get("block")
        confs = REQUIRED_CONFIRMATIONS if (isinstance(block_info, dict) and block_info) else 0

        # Wrong amount guard — allow ±5 % tolerance.  The LTC price moves
        # between when the bot quotes the amount and when the sender actually
        # sends, and minor fee/dust differences can also shift the value.
        amount_tolerance = max(expected_ltc * 0.05, 0.00001)
        if amount_ltc > 0 and abs(amount_ltc - expected_ltc) > amount_tolerance:
            if not tx.get("wrong_amount_notified"):
                await bot.db.update_transaction(transaction_id, {"wrong_amount_notified": True})
                msg = (
                    f"⚠️ **Wrong Amount** in trade `{transaction_id}`: "
                    f"expected {expected_ltc:.8f} LTC, received {amount_ltc:.8f} LTC."
                )
                if channel:
                    await channel.send(embed=make_embed(
                        "⚠️ Wrong Amount Received",
                        f"Expected **{expected_ltc:.8f} LTC** but got **{amount_ltc:.8f} LTC**. Contact an admin.",
                        COLOR_DANGER,
                    ))
                if guild:
                    await bot.notify_admins(guild, msg)
            continue

        if amount_ltc == 0:
            continue

        # Valid payment — determine status
        status_text: str
        if confs == 0:
            status_text = "🔍 Payment Detected — Awaiting Confirmations"
        elif confs < REQUIRED_CONFIRMATIONS:
            status_text = f"🔄 Awaiting Confirmations ({confs}/{REQUIRED_CONFIRMATIONS})"
        else:
            status_text = "✅ Payment Confirmed"

        color = COLOR_SUCCESS if confs >= REQUIRED_CONFIRMATIONS else COLOR_WARNING

        # ── Send a channel message the FIRST time payment is seen on-chain ───
        # Use a local flag so that multiple items in the same poll never
        # double-fire even if the DB write hasn't been read back yet.
        already_notified = tx.get("payment_detected_notified", False) or _detected_this_poll
        if not already_notified and channel:
            mm_name = await bot.db.get_setting("mm_name") or "AutoMM"
            star    = await _get_star_emoji(bot)
            arr     = await _get_arrow_emoji(bot)
            await channel.send(embed=make_embed(
                title=f"{star} Pending Payment Detected {star}",
                description=(
                    f"{arr} A Pending Transaction Is Detected\n\n"
                    f"{arr} **${tx['amount_usd']:,.2f}** ( **{amount_ltc:.8f} LTC** )"
                ),
                color=COLOR_PRIMARY,
                footer=f"{mm_name} • Awaiting confirmation",
                footer_icon_url=BADGE_IMG,
                thumbnail_url=SPINNER_GIF,
            ))
            await bot.db.update_transaction(transaction_id, {"payment_detected_notified": True})
            _detected_this_poll = True  # prevent duplicate within same poll cycle
            log.info("Payment detected notification sent for %s (confs=%d)", transaction_id, confs)

        # ── Update the deposit embed to show current status ───────────────────
        dep_msg_id = tx.get("deposit_message_id")
        if channel and dep_msg_id:
            try:
                mm_name_poll = await bot.db.get_setting("mm_name") or "AutoMM"
                star_poll    = await _get_star_emoji(bot)
                arr_poll     = await _get_arrow_emoji(bot)
                dep_msg  = await channel.fetch_message(dep_msg_id)
                conf_str = f"{confs}/{REQUIRED_CONFIRMATIONS}"
                txid_str = f"`{txid}`" if txid else "_Pending_"
                new_embed = make_embed(
                    title=f"{star_poll} Waiting For Payment {star_poll}",
                    description=(
                        f"Payment Credentials are Given Below\n\n"
                        f"{arr_poll} **Address** : `{address}`\n"
                        f"{arr_poll} **Amount to pay** : {amount_ltc:.8f} LTC\n\n"
                        f"**Status:** {status_text}\n"
                        f"**TXID:** {txid_str}  |  **Confirmations:** {conf_str}"
                    ),
                    color=color,
                    thumbnail_url=LTC_LOGO,
                    footer=mm_name_poll,
                    footer_icon_url=BADGE_IMG,
                )
                # Disable Copy Address button once payment is detected
                updated_view = DepositView(bot, transaction_id, tx["sender_id"])
                for item_btn in updated_view.children:
                    if getattr(item_btn, "label", None) == "Copy Address":
                        item_btn.disabled = True  # type: ignore[union-attr]
                await dep_msg.edit(embed=new_embed, view=updated_view)
            except (discord.NotFound, discord.Forbidden):
                pass
            except Exception as exc:
                log.warning("Failed to update deposit embed: %s", exc)

        await bot.db.update_transaction(transaction_id, {
            "deposit_txid":  txid,
            "confirmations": confs,
        })

        if confs >= REQUIRED_CONFIRMATIONS:
            # Store the ACTUAL received amount so withdrawal uses it exactly —
            # fee is deducted from this amount (subtract_fee_from_amount=True),
            # meaning the bot never sends more than it received.
            received_sat = int(round(amount_ltc * 1e8))
            await bot.db.update_transaction(transaction_id, {
                "deposit_confirmed":    True,
                "stage":                Stage.RELEASE,
                "received_amount_ltc":  amount_ltc,
                "received_amount_sat":  received_sat,
            })
            log.info(
                "Payment confirmed for %s: %.8f LTC (%d sat), TXID=%s",
                transaction_id, amount_ltc, received_sat, txid,
            )
            if channel:
                mm_name = await bot.db.get_setting("mm_name") or "AutoMM"
                star    = await _get_star_emoji(bot)
                arr     = await _get_arrow_emoji(bot)
                dot     = await _get_dot_emoji(bot)
                await channel.send(embed=make_embed(
                    title=f"{star} Payment Received {star}",
                    description=(
                        f"{arr} The Transaction is now Confirmed\n"
                        f"{arr} Refund or Release can be processed now\n\n"
                        f"{dot} **${tx['amount_usd']:,.2f} USD** ( **{amount_ltc:.8f} LTC** )"
                    ),
                    color=COLOR_SUCCESS,
                    footer=f"{mm_name} • Payment Confirmed",
                    footer_icon_url=BADGE_IMG,
                    thumbnail_url=CHECKMARK_IMG,
                ))
            refreshed = await bot.db.get_transaction(transaction_id)
            if refreshed and channel:
                await _start_release_stage(bot, channel, refreshed)
            return True

    return False


# ---------------------------------------------------------------------------
# Stage 8: Delivery
# ---------------------------------------------------------------------------

async def _start_delivery_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    # Mark delivery as started in DB BEFORE sending, so a crash during send
    # is still recoverable (we'd just re-send, which is harmless).
    await bot.db.update_transaction(tx["transaction_id"], {"delivery_stage_started": True})
    view = DeliveryView(bot, tx["transaction_id"])
    await channel.send(
        content=f"<@{tx['receiver_id']}>",
        embed=make_embed(
            title=f"📦 Product Delivery — `{tx['transaction_id']}`",
            description=(
                "**Receiver**, payment has been confirmed.\n"
                "Please deliver the product/service to the Sender.\n\n"
                "Press **Product Delivered** once you have delivered everything."
            ),
            color=COLOR_INFO,
        ),
        view=view,
    )


class DeliveryView(BaseView):
    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(bot, transaction_id)

        done_btn = discord.ui.Button(
            label="Product Delivered",
            style=discord.ButtonStyle.success,
            emoji="✅",
            custom_id=f"del_done_{transaction_id}",
        )
        done_btn.callback = self._on_delivered
        self.add_item(done_btn)

        admin_btn = discord.ui.Button(
            label="Contact Admin",
            style=discord.ButtonStyle.secondary,
            emoji="🛡️",
            custom_id=f"del_admin_{transaction_id}",
        )
        admin_btn.callback = self._on_admin
        self.add_item(admin_btn)

    async def _on_delivered(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.DELIVERY)
        if tx is None:
            return
        if interaction.user.id != tx["receiver_id"]:
            await interaction.response.send_message(
                "❌ Only the Receiver may mark delivery.", ephemeral=True
            )
            return
        await self.bot.db.update_transaction(self.transaction_id, {"stage": Stage.RELEASE})
        self.disable_all()
        await interaction.response.edit_message(
            embed=make_embed(
                "📦 Product Delivered",
                "✅ Receiver marked the product as delivered.",
                COLOR_SUCCESS,
            ),
            view=self,
        )
        await _start_release_stage(self.bot, interaction.channel, tx)

    async def _on_admin(self, interaction: discord.Interaction) -> None:
        tx = await self.get_tx()
        if tx and not await self.require_participant(interaction, tx):
            return
        await interaction.response.send_message("🛡️ An admin has been notified.", ephemeral=False)
        await self.bot.notify_admins(
            interaction.guild,
            f"📢 Admin requested in trade `{self.transaction_id}` by {interaction.user.mention} (delivery)",
        )


# ---------------------------------------------------------------------------
# Stage 9: Release / Dispute
# ---------------------------------------------------------------------------

async def _start_release_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    mm_name = await bot.db.get_setting("mm_name") or "AutoMM"
    star    = await _get_star_emoji(bot)
    arr     = await _get_arrow_emoji(bot)
    dot     = await _get_dot_emoji(bot)
    view = ReleaseView(bot, tx["transaction_id"])
    await channel.send(
        content=f"<@{tx['sender_id']}>",
        embed=make_embed(
            title=f"{star} Payment Received {star}",
            description=(
                f"{arr} The Transaction is now Confirmed\n"
                f"{arr} Refund Or Release Can be Processed Now\n\n"
                f"{dot} **${tx['amount_usd']:,.2f} USD** ( **{tx['amount_ltc']:.8f} LTC** )"
            ),
            color=COLOR_SUCCESS,
            footer=f"{mm_name} • Payment Confirmed",
            footer_icon_url=BADGE_IMG,
            thumbnail_url=CHECKMARK_IMG,
        ),
        view=view,
    )


class RefundConfirmView(discord.ui.View):
    """Ephemeral confirmation shown when a participant presses Refund."""

    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(timeout=60)
        self.bot            = bot
        self.transaction_id = transaction_id

        confirm_btn = discord.ui.Button(
            label="✅ Confirm Refund",
            style=discord.ButtonStyle.success,
        )
        confirm_btn.callback = self._on_confirm
        self.add_item(confirm_btn)

        back_btn = discord.ui.Button(
            label="↩️ Go Back",
            style=discord.ButtonStyle.secondary,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def _disable_all(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]

    async def _on_confirm(self, interaction: discord.Interaction) -> None:
        self._disable_all()
        await interaction.response.edit_message(
            content="✅ Refund confirmed. Cancelling trade and notifying admin…",
            embed=None,
            view=self,
        )
        await do_cancel(
            self.bot, self.transaction_id, interaction.user,
            "Refund requested by participant", channel=interaction.channel,
            outcome="refunded",
        )
        await self.bot.notify_admins(
            interaction.guild,
            f"💰 Refund requested in trade `{self.transaction_id}` by {interaction.user.mention}. "
            f"Please return the LTC to the Sender.",
        )

    async def _on_back(self, interaction: discord.Interaction) -> None:
        self._disable_all()
        await interaction.response.edit_message(content="↩️ Cancelled.", embed=None, view=self)


class ReleaseView(BaseView):
    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(bot, transaction_id)

        release_btn = discord.ui.Button(
            label="Release",
            style=discord.ButtonStyle.success,
            custom_id=f"rel_release_{transaction_id}",
        )
        release_btn.callback = self._on_release
        self.add_item(release_btn)

        refund_btn = discord.ui.Button(
            label="Refund",
            style=discord.ButtonStyle.danger,
            custom_id=f"rel_refund_{transaction_id}",
        )
        refund_btn.callback = self._on_refund
        self.add_item(refund_btn)

        dispute_btn = discord.ui.Button(
            label="Raise Dispute",
            style=discord.ButtonStyle.secondary,
            custom_id=f"rel_dispute_{transaction_id}",
        )
        dispute_btn.callback = self._on_dispute
        self.add_item(dispute_btn)

    async def _on_release(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.RELEASE)
        if tx is None:
            return
        if interaction.user.id != tx["sender_id"]:
            await interaction.response.send_message(
                "❌ Only the Sender may release funds.", ephemeral=True
            )
            return
        if tx.get("release_confirmed"):
            await interaction.response.send_message("❌ Funds already released.", ephemeral=True)
            return
        # Guard: prevent duplicate confirmation messages if Release is pressed twice
        if tx.get("release_pending"):
            await interaction.response.send_message(
                "⏳ A release confirmation is already pending in this channel.", ephemeral=True
            )
            return

        mm_name    = await self.bot.db.get_setting("mm_name") or "AutoMM"
        star       = await _get_star_emoji(self.bot)
        dot        = await _get_dot_emoji(self.bot)
        payout_ltc = tx.get("received_amount_ltc") or tx["amount_ltc"]
        guild_icon = (
            interaction.guild.icon.url
            if interaction.guild and interaction.guild.icon
            else None
        )

        # Mark pending BEFORE sending so concurrent presses are rejected
        await self.bot.db.update_transaction(self.transaction_id, {"release_pending": True})

        view = ReleaseConfirmationView(self.bot, self.transaction_id, tx["sender_id"])
        await interaction.response.defer()
        try:
            msg = await interaction.channel.send(
                embed=make_embed(
                    title=f"{star} Release Payment Confirmation {star}",
                    description=(
                        f"{dot} **Amount to Release**\n"
                        f"{payout_ltc:.8f} LTC\n"
                        f"≈ ${tx['amount_usd']:,.2f} USD\n\n"
                        f"{dot} **Releasing To**\n"
                        f"<@{tx['receiver_id']}>\n\n"
                        f"{dot} **Warning**\n"
                        f"This action cannot be undone.\nPlease confirm carefully."
                    ),
                    color=COLOR_SUCCESS,
                    footer=f"{mm_name} • Confirm button will activate in 5 seconds",
                    footer_icon_url=BADGE_IMG,
                    thumbnail_url=guild_icon,
                ),
                view=view,
            )
        except Exception:
            # Roll back pending flag if send fails so Sender can retry
            await self.bot.db.update_transaction(self.transaction_id, {"release_pending": False})
            raise
        view.message = msg
        view._enable_task = asyncio.create_task(view.enable_after_delay())

    async def _on_refund(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.RELEASE)
        if tx is None:
            return
        if not await self.require_participant(interaction, tx):
            return
        dot  = await _get_dot_emoji(self.bot)
        view = RefundConfirmView(self.bot, self.transaction_id)
        await interaction.response.send_message(
            embed=make_embed(
                "💰 Confirm Refund",
                (
                    f"Are you sure you want to cancel this trade and request a refund?\n\n"
                    f"{dot} **Amount:** {tx['amount_ltc']:.8f} LTC (≈ ${tx['amount_usd']:,.2f} USD)\n\n"
                    f"An admin will be notified to return the LTC to the Sender."
                ),
                COLOR_WARNING,
            ),
            view=view,
            ephemeral=True,
        )

    async def _on_dispute(self, interaction: discord.Interaction) -> None:
        tx = await self.require_stage(interaction, Stage.RELEASE)
        if tx is None:
            return
        if not await self.require_participant(interaction, tx):
            return
        await self.bot.db.update_transaction(self.transaction_id, {"stage": Stage.DISPUTED})
        self.disable_all()
        await interaction.response.edit_message(
            embed=make_embed(
                "⚖️ Dispute Opened",
                "A dispute has been opened. An admin will assist shortly.",
                COLOR_DANGER,
            ),
            view=self,
        )
        await self.bot.notify_admins(
            interaction.guild,
            f"⚖️ Dispute opened in trade `{self.transaction_id}` by {interaction.user.mention}",
        )
        await self.bot.db.add_log("dispute_opened", {
            "transaction_id": self.transaction_id,
            "user_id": interaction.user.id,
        })

    async def _on_admin(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_message("🛡️ Notifying admin...", ephemeral=True)
        await self.bot.notify_admins(
            interaction.guild,
            f"📢 Admin requested in trade `{self.transaction_id}` by {interaction.user.mention} (release)",
        )


class ReleaseConfirmationView(discord.ui.View):
    """Channel-posted confirmation for the Release flow with a 5-second timed button."""

    def __init__(self, bot: "AutoMMBot", transaction_id: str, sender_id: int) -> None:
        super().__init__(timeout=180)
        self.bot            = bot
        self.transaction_id = transaction_id
        self.sender_id      = sender_id
        self.message: Optional[discord.Message] = None
        self._done          = False                  # set on confirm or cancel
        self._enable_task: Optional[asyncio.Task]  = None  # set externally

        self.confirm_btn = discord.ui.Button(
            label="✅ Confirm Release",
            style=discord.ButtonStyle.success,
            disabled=True,                           # enabled after 5 s delay
            custom_id=f"relconf_yes_{transaction_id}",
        )
        self.confirm_btn.callback = self._on_confirm
        self.add_item(self.confirm_btn)

        cancel_btn = discord.ui.Button(
            label="❌ Cancel",
            style=discord.ButtonStyle.danger,
            custom_id=f"relconf_no_{transaction_id}",
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def enable_after_delay(self) -> None:
        await asyncio.sleep(5)
        if self._done:          # cancelled or confirmed before timer fired — no-op
            return
        self.confirm_btn.disabled = False
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    def _finish(self) -> None:
        """Mark done and cancel the pending enable-task if still running."""
        self._done = True
        if self._enable_task and not self._enable_task.done():
            self._enable_task.cancel()

    def _disable_all(self) -> None:
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]

    async def _on_confirm(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.sender_id:
            await interaction.response.send_message(
                "❌ Only the Sender may confirm release.", ephemeral=True
            )
            return
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None or tx["stage"] != Stage.RELEASE:
            await interaction.response.edit_message(content="❌ Stage changed.", view=None)
            return
        if tx.get("release_confirmed"):
            await interaction.response.edit_message(content="❌ Already released.", view=None)
            return
        self._finish()
        await self.bot.db.update_transaction(self.transaction_id, {
            "stage": Stage.WITHDRAWAL, "release_confirmed": True, "release_pending": False,
        })
        self._disable_all()
        await interaction.response.edit_message(view=self)
        if interaction.channel:
            await _start_withdrawal_stage(self.bot, interaction.channel, tx)

    async def _on_cancel(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.sender_id:
            await interaction.response.send_message(
                "❌ Only the Sender may cancel.", ephemeral=True
            )
            return
        self._finish()
        # Clear pending flag so Sender can press Release again
        await self.bot.db.update_transaction(self.transaction_id, {"release_pending": False})
        self._disable_all()
        await interaction.response.edit_message(view=self)


# ---------------------------------------------------------------------------
# Stage 10: Withdrawal
# ---------------------------------------------------------------------------

async def _start_withdrawal_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    view = WithdrawalInputView(bot, tx["transaction_id"], tx["receiver_id"])
    await channel.send(
        content=f"<@{tx['receiver_id']}>",
        embed=make_embed(
            title=f"💸 Withdrawal — `{tx['transaction_id']}`",
            description=(
                "**Receiver**, please enter your LTC address to receive payment.\n"
                "Funds will be sent automatically."
            ),
            color=COLOR_INFO,
        ),
        view=view,
    )


class WithdrawalModal(discord.ui.Modal, title="Enter LTC Withdrawal Address"):
    address = discord.ui.TextInput(
        label="LTC Address",
        placeholder="Your Litecoin address (starts with L, M, 3, or ltc1)",
        required=True,
        max_length=100,
    )

    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__()
        self.bot            = bot
        self.transaction_id = transaction_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        addr = self.address.value.strip()
        if not AprionClient.validate_ltc_address(addr):
            await interaction.response.send_message(
                "❌ Invalid LTC address. Please check and try again.", ephemeral=True
            )
            return

        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None or tx["stage"] != Stage.WITHDRAWAL:
            await interaction.response.send_message("❌ Stage mismatch.", ephemeral=True)
            return
        if tx.get("withdrawal_txid"):
            await interaction.response.send_message(
                "❌ Withdrawal already processed.", ephemeral=True
            )
            return
        if interaction.user.id != tx["receiver_id"]:
            await interaction.response.send_message(
                "❌ Only the Receiver may submit a withdrawal address.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        # Record address immediately to prevent double-withdraw
        await self.bot.db.update_transaction(self.transaction_id, {
            "withdrawal_address": addr,
            "stage": Stage.WITHDRAWAL,  # keep until TX confirmed
        })

        # Use the ACTUAL received amount (stored at confirmation time), not the
        # quoted amount — fee is subtracted from it so the bot never overpays.
        payout_ltc = tx.get("received_amount_ltc") or tx["amount_ltc"]
        payout_sat = int(round(payout_ltc * 1e8))
        log.info(
            "Withdrawal for %s: payout=%.8f LTC (%d sat) to %s "
            "(received=%.8f, quoted=%.8f)",
            self.transaction_id, payout_ltc, payout_sat, addr,
            tx.get("received_amount_ltc", 0), tx["amount_ltc"],
        )

        mm_name = await self.bot.db.get_setting("mm_name") or "AutoMM"
        star    = await _get_star_emoji(self.bot)
        arr     = await _get_arrow_emoji(self.bot)

        # Show "Releasing Funds..." embed while the API call is in flight
        await interaction.channel.send(embed=make_embed(
            title=f"{star} Releasing Funds... {star}",
            description=(
                f"{arr} Sending **{payout_ltc:.8f} LTC** to:\n"
                f"```\n{addr}\n```"
            ),
            color=COLOR_SUCCESS,
            footer=mm_name,
            footer_icon_url=BADGE_IMG,
            thumbnail_url=SPINNER_GIF,
        ))

        try:
            result = await self.bot.aprion.withdraw(addr, payout_ltc)
        except Exception as exc:
            log.error("Withdrawal failed for %s: %s", self.transaction_id, exc)
            await interaction.followup.send(
                embed=make_embed(
                    "❌ Withdrawal Failed",
                    f"`{exc}`\n\nPlease contact an admin.",
                    COLOR_DANGER,
                    fields=[
                        ("Amount", f"{payout_ltc:.8f} LTC", True),
                        ("Address", f"`{addr}`", False),
                    ],
                ),
                ephemeral=True,
            )
            await self.bot.notify_admins(
                interaction.guild,
                f"❌ Withdrawal failed for `{self.transaction_id}`: {exc}",
            )
            return

        withdrawal_txid = (
            result.get("txid")
            or result.get("id")
            or result.get("tx_hash")
            or "pending"
        )

        await self.bot.db.update_transaction(self.transaction_id, {
            "withdrawal_txid": withdrawal_txid,
            "stage":           Stage.COMPLETED,
            "completed_at":    datetime.now(timezone.utc),
        })

        # Update user stats
        for uid in (tx["sender_id"], tx["receiver_id"]):
            await self.bot.db.increment_user(uid, {
                "completed_deals":   1,
                "total_volume_usd":  tx["amount_usd"],
                "total_volume_ltc":  tx["amount_ltc"],
            })

        txid_link = (
            f"[{withdrawal_txid}](https://blockchair.com/litecoin/transaction/{withdrawal_txid})"
            if withdrawal_txid and withdrawal_txid != "pending"
            else f"`{withdrawal_txid}`"
        )
        sent_view = discord.ui.View()
        if withdrawal_txid and withdrawal_txid != "pending":
            sent_view.add_item(discord.ui.Button(
                label="View on Blockchair",
                style=discord.ButtonStyle.link,
                url=f"https://blockchair.com/litecoin/transaction/{withdrawal_txid}",
                emoji="🔗",
            ))
        await interaction.channel.send(
            embed=make_embed(
                title=f"{star} Payment Sent {star}",
                description=f"{arr} The Litecoin payment has been successfully sent.",
                color=COLOR_SUCCESS,
                footer=mm_name,
                footer_icon_url=BADGE_IMG,
                thumbnail_url=CHECKMARK_IMG,
                fields=[
                    ("Deal ID",        self.transaction_id,               False),
                    ("To Address",     f"`{addr}`",                       False),
                    ("Amount Sent",    f"**{payout_ltc:.8f} LTC**",       False),
                    ("Transaction ID", txid_link,                         False),
                ],
            ),
            view=sent_view,
        )

        await interaction.followup.send("✅ Withdrawal submitted!", ephemeral=True)

        # Post completion announcement
        await self.bot.post_completed_announcement(interaction.guild, tx)

        # Feedback stage
        refreshed = await self.bot.db.get_transaction(self.transaction_id)
        if refreshed:
            await _start_feedback_stage(self.bot, interaction.channel, refreshed)


class WithdrawalInputView(discord.ui.View):
    def __init__(self, bot: "AutoMMBot", transaction_id: str, receiver_id: int) -> None:
        super().__init__(timeout=None)
        self.bot            = bot
        self.transaction_id = transaction_id
        self.receiver_id    = receiver_id

        btn = discord.ui.Button(
            label="Enter Withdrawal Address",
            style=discord.ButtonStyle.primary,
            emoji="💸",
            custom_id=f"wd_enter_{transaction_id}",
        )
        btn.callback = self._on_enter
        self.add_item(btn)

    async def _on_enter(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.receiver_id:
            await interaction.response.send_message(
                "❌ Only the Receiver may submit a withdrawal address.", ephemeral=True
            )
            return
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None or tx["stage"] != Stage.WITHDRAWAL:
            await interaction.response.send_message("❌ Stage mismatch.", ephemeral=True)
            return
        if tx.get("withdrawal_txid"):
            await interaction.response.send_message(
                "❌ Withdrawal already processed.", ephemeral=True
            )
            return
        for item in self.children:
            if hasattr(item, "disabled"):
                item.disabled = True  # type: ignore[union-attr]
        await interaction.response.send_modal(WithdrawalModal(self.bot, self.transaction_id))
        await interaction.message.edit(view=self)


# ---------------------------------------------------------------------------
# Stage 11: Feedback
# ---------------------------------------------------------------------------

async def _start_feedback_stage(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    # Already both submitted — nothing to do
    if tx.get("feedback_sent_sender") and tx.get("feedback_sent_receiver"):
        return

    sid = tx["sender_id"]
    rid = tx["receiver_id"]
    view = FeedbackView(bot, tx["transaction_id"], sid, rid, star_emoji="⭐")
    await channel.send(
        content=f"<@{sid}> <@{rid}>",
        embed=make_embed(
            title="⭐ Leave Feedback",
            description=(
                "The deal is complete! Both parties may now leave feedback.\n"
                "Press **Submit Feedback** below to rate your experience."
            ),
            color=COLOR_INFO,
        ),
        view=view,
    )


class FeedbackModal(discord.ui.Modal, title="Leave Feedback"):
    rating = discord.ui.TextInput(
        label="Rating (1 – 5 stars)",
        placeholder="Enter a number from 1 to 5",
        min_length=1,
        max_length=1,
        required=True,
    )
    comment = discord.ui.TextInput(
        label="Comment (optional)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500,
        placeholder="Share your experience…",
    )

    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        reviewer_id: int,
        target_id: int,
    ) -> None:
        super().__init__()
        self.bot            = bot
        self.transaction_id = transaction_id
        self.reviewer_id    = reviewer_id
        self.target_id      = target_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        # Validate rating first
        try:
            rating_val = int(self.rating.value.strip())
            if not 1 <= rating_val <= 5:
                raise ValueError
        except (TypeError, ValueError):
            await interaction.response.send_message(
                "❌ Rating must be a whole number from 1 to 5.", ephemeral=True
            )
            return

        # Server-side double-submit guard (catches race: user opens two modals before first commits)
        tx_check = await self.bot.db.get_transaction(self.transaction_id)
        if tx_check:
            chk_field = (
                "feedback_sent_sender"
                if self.reviewer_id == tx_check["sender_id"]
                else "feedback_sent_receiver"
            )
            if tx_check.get(chk_field):
                await interaction.response.send_message(
                    "❌ You have already submitted feedback for this deal.", ephemeral=True
                )
                return

        comment = self.comment.value.strip() or None
        star_e  = "⭐"
        stars   = star_e * rating_val

        # Respond immediately (within Discord's 3-second window)
        await interaction.response.send_message(
            embed=make_embed(
                "✅ Feedback Submitted",
                f"Thank you! You gave **{stars} ({rating_val}/5)**.",
                COLOR_SUCCESS,
            ),
            ephemeral=True,
        )

        # Persist feedback to DB (unique index on transaction_id+reviewer_id prevents duplicate inserts)
        await self.bot.db.add_feedback({
            "transaction_id": self.transaction_id,
            "reviewer_id":    self.reviewer_id,
            "target_id":      self.target_id,
            "rating":         rating_val,
            "comment":        comment,
            "created_at":     datetime.now(timezone.utc),
        })

        # Update target user aggregate stats
        user      = await self.bot.db.get_user(self.target_id)
        new_count = user.get("feedback_count", 0) + 1
        new_sum   = user.get("rating_sum",     0) + rating_val
        await self.bot.db.update_user(self.target_id, {
            "feedback_count": new_count,
            "rating_sum":     new_sum,
            "average_rating": round(new_sum / new_count, 2),
        })

        # Mark this user's feedback as submitted
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx:
            field = (
                "feedback_sent_sender"
                if self.reviewer_id == tx["sender_id"]
                else "feedback_sent_receiver"
            )
            await self.bot.db.update_transaction(self.transaction_id, {field: True})

        # Post the formatted feedback embed to the feedback log channel
        await self.bot.post_feedback_embed(
            interaction, self.transaction_id, self.reviewer_id, rating_val, comment
        )

        # Check if both parties submitted → finalize the ticket
        refreshed = await self.bot.db.get_transaction(self.transaction_id)
        if (
            refreshed
            and refreshed.get("feedback_sent_sender")
            and refreshed.get("feedback_sent_receiver")
        ):
            ch = interaction.guild.get_channel(refreshed["channel_id"])
            if ch:
                await _finalize_ticket(self.bot, ch, refreshed)


class FeedbackView(discord.ui.View):
    """
    Single "Submit Feedback" button shared by both trade parties.
    Each user can click once; pressing again shows an ephemeral "already submitted" message.
    """

    def __init__(
        self,
        bot: "AutoMMBot",
        transaction_id: str,
        sender_id: int,
        receiver_id: int,
        star_emoji: str = "⭐",
    ) -> None:
        super().__init__(timeout=None)
        self.bot         = bot
        self.sender_id   = sender_id
        self.receiver_id = receiver_id

        btn = discord.ui.Button(
            label="Submit Feedback ⭐",
            style=discord.ButtonStyle.primary,
            custom_id=f"feedback_btn_{transaction_id}",
        )
        btn.callback = self._on_submit
        self.add_item(btn)

    async def _on_submit(self, interaction: discord.Interaction) -> None:
        uid = interaction.user.id
        # Fetch fresh tx from DB (the view may be stale after a restart)
        tx  = await self.bot.db.get_transaction_by_channel(interaction.channel_id)

        if not tx:
            await interaction.response.send_message("❌ Trade not found.", ephemeral=True)
            return

        if uid not in (tx["sender_id"], tx["receiver_id"]):
            await interaction.response.send_message(
                "❌ You are not part of this trade.", ephemeral=True
            )
            return

        field = "feedback_sent_sender" if uid == tx["sender_id"] else "feedback_sent_receiver"
        if tx.get(field):
            await interaction.response.send_message(
                "❌ You have already submitted feedback for this deal.", ephemeral=True
            )
            return

        target_id = tx["receiver_id"] if uid == tx["sender_id"] else tx["sender_id"]
        await interaction.response.send_modal(
            FeedbackModal(self.bot, tx["transaction_id"], uid, target_id)
        )


# ---------------------------------------------------------------------------
# Transcript — full-fidelity Discord-style HTML
# ---------------------------------------------------------------------------

TRANSCRIPT_CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{background:#313338;color:#dbdee1;font-family:'gg sans','Noto Sans','Helvetica Neue',Helvetica,Arial,sans-serif;font-size:16px;line-height:1.375}
a{color:#00a8fc;text-decoration:none}a:hover{text-decoration:underline}

/* ── preamble ── */
.preamble{background:#2b2d31;padding:20px 20px 0}
.preamble__guild{display:flex;align-items:center;gap:16px;padding-bottom:16px;border-bottom:1px solid #3f4147}
.preamble__guild-icon{width:64px;height:64px;border-radius:50%;background:#5865f2;object-fit:cover;flex-shrink:0}
.preamble__guild-name{color:#fff;font-size:18px;font-weight:700}
.preamble__channel{display:flex;align-items:center;gap:6px;margin-top:4px}
.preamble__channel-name{color:#dbdee1;font-size:15px;font-weight:600}
.preamble__meta{color:#949ba4;font-size:13px;padding:10px 0 18px}
.preamble__participants{display:flex;flex-wrap:wrap;gap:8px;margin-top:6px}
.preamble__participant{display:flex;align-items:center;gap:6px;background:#313338;border-radius:20px;padding:4px 10px 4px 4px}
.preamble__participant-avatar{width:24px;height:24px;border-radius:50%;object-fit:cover;background:#5865f2}
.preamble__participant-name{font-size:13px;color:#dbdee1}

/* ── chatlog ── */
.chatlog{padding:16px 20px}

/* ── day separator ── */
.chatlog__day-separator{display:flex;align-items:center;gap:12px;margin:16px 0}
.chatlog__day-separator::before,.chatlog__day-separator::after{content:'';flex:1;height:1px;background:#3f4147}
.chatlog__day-separator-text{color:#949ba4;font-size:12px;font-weight:600;white-space:nowrap}

/* ── message group ── */
.chatlog__message-group{display:flex;gap:0;padding:2px 8px 2px 0;border-radius:4px;position:relative}
.chatlog__message-group:hover{background:rgba(4,4,5,.07)}

/* avatar column */
.chatlog__author-avatar-container{width:72px;flex-shrink:0;padding-top:2px;display:flex;justify-content:center}
.chatlog__author-avatar{width:40px;height:40px;border-radius:50%;object-fit:cover;background:#5865f2;cursor:pointer}
.chatlog__author-avatar-placeholder{width:40px;height:40px;flex-shrink:0}

/* short timestamp on hover for continuations */
.chatlog__short-time{position:absolute;left:0;width:72px;text-align:right;padding-right:8px;font-size:11px;color:transparent;white-space:nowrap;pointer-events:none;top:4px}
.chatlog__message-group:hover .chatlog__short-time{color:#949ba4}

/* messages column */
.chatlog__messages{flex:1;min-width:0;padding:2px 0}

/* header (author + timestamp) */
.chatlog__header{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;margin-bottom:2px}
.chatlog__author{font-size:16px;font-weight:500;color:#fff;cursor:pointer;line-height:1.375}
.chatlog__author:hover{text-decoration:underline}
.chatlog__bot-tag{background:#5865f2;color:#fff;font-size:10px;font-weight:700;padding:1px 4px;border-radius:3px;letter-spacing:.3px;text-transform:uppercase;vertical-align:middle;margin-left:2px;line-height:1.6;display:inline-block}
.chatlog__timestamp{font-size:12px;color:#949ba4}

/* message content */
.chatlog__content{color:#dbdee1;font-size:16px;white-space:pre-wrap;word-wrap:break-word;line-height:1.375}
.chatlog__content strong{font-weight:700}
.chatlog__content em{font-style:italic}
.chatlog__content del{text-decoration:line-through}
.chatlog__content u{text-decoration:underline}
.inline-code{background:#2b2d31;border:1px solid #1e1f22;border-radius:3px;padding:0 4px;font-family:'Consolas','Andale Mono WT','Andale Mono','Lucida Console',monospace;font-size:.875em;color:#dbdee1}
.pre{background:#2b2d31;border:1px solid #1e1f22;border-radius:4px;padding:8px 12px;margin:6px 0;overflow-x:auto;position:relative}
.pre__content{display:block;font-family:'Consolas','Andale Mono WT','Andale Mono','Lucida Console',monospace;font-size:14px;line-height:1.5;white-space:pre;color:#dbdee1}
.pre-lang{position:absolute;top:6px;right:10px;font-size:11px;color:#949ba4;text-transform:uppercase}
.chatlog__content blockquote{border-left:4px solid #4e5058;padding-left:12px;margin:4px 0;color:#dbdee1}
.mention{background:rgba(88,101,242,.3);color:#c9cdfb;border-radius:3px;padding:0 2px;font-weight:500;cursor:pointer}
.mention:hover{background:rgba(88,101,242,.5);color:#fff}
.spoiler{background:#202225;color:transparent;border-radius:3px;padding:0 2px;cursor:pointer}
.spoiler:hover{color:#dbdee1;background:#313338}

/* attachments */
.chatlog__attachment{margin-top:8px}
.chatlog__attachment-media{max-width:520px;max-height:350px;border-radius:3px;display:block;object-fit:contain;cursor:pointer}
.chatlog__attachment-file{display:flex;align-items:center;gap:10px;background:#2b2d31;border:1px solid #1e1f22;border-radius:4px;padding:10px 12px;margin-top:6px;max-width:520px}
.chatlog__attachment-icon{font-size:24px}
.chatlog__attachment-filename{color:#00a8fc;font-size:14px;font-weight:500}
.chatlog__attachment-filesize{color:#949ba4;font-size:12px}

/* embeds */
.chatlog__embed{display:flex;max-width:520px;margin-top:8px;border-radius:0 4px 4px 0;overflow:hidden}
.chatlog__embed-color-pill{width:4px;flex-shrink:0}
.chatlog__embed-content-container{background:#2b2d31;padding:12px 16px;flex:1;min-width:0;display:flex;gap:12px}
.chatlog__embed-inner{flex:1;min-width:0}
.chatlog__embed-author{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.chatlog__embed-author-icon{width:24px;height:24px;border-radius:50%;object-fit:cover}
.chatlog__embed-author-name{font-size:14px;font-weight:600;color:#dbdee1}
.chatlog__embed-title{font-size:16px;font-weight:600;color:#fff;margin-bottom:6px;word-wrap:break-word}
.chatlog__embed-description{font-size:14px;color:#dbdee1;white-space:pre-wrap;word-wrap:break-word;margin-bottom:8px;line-height:1.375}
.chatlog__embed-fields{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:8px}
.chatlog__embed-field--block{grid-column:1/-1}
.chatlog__embed-field-name{font-size:14px;font-weight:600;color:#dbdee1;margin-bottom:2px}
.chatlog__embed-field-value{font-size:14px;color:#dbdee1;white-space:pre-wrap;word-wrap:break-word;line-height:1.375}
.chatlog__embed-image{max-width:400px;max-height:300px;border-radius:4px;margin-top:12px;display:block;object-fit:contain}
.chatlog__embed-thumbnail{width:80px;height:80px;border-radius:4px;object-fit:cover;flex-shrink:0}
.chatlog__embed-footer{display:flex;align-items:center;gap:8px;margin-top:10px}
.chatlog__embed-footer-icon{width:20px;height:20px;border-radius:50%;object-fit:cover}
.chatlog__embed-footer-text{font-size:12px;color:#949ba4}

/* reactions */
.chatlog__reactions{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
.chatlog__reaction{display:flex;align-items:center;gap:4px;background:#2b2d31;border:1px solid #3f4147;border-radius:8px;padding:2px 6px;font-size:13px;cursor:default}
.chatlog__reaction-count{color:#dbdee1;font-size:13px;font-weight:500}

/* components (buttons) */
.chatlog__components{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.chatlog__component-btn{display:inline-flex;align-items:center;gap:6px;padding:5px 16px;border-radius:4px;font-size:14px;font-weight:500;background:#4e5058;color:#dbdee1;cursor:default}
.chatlog__component-btn--primary{background:#5865f2;color:#fff}
.chatlog__component-btn--success{background:#248046;color:#fff}
.chatlog__component-btn--danger{background:#da373c;color:#fff}

/* system messages */
.chatlog__system-message{display:flex;align-items:center;gap:10px;color:#949ba4;font-size:14px;padding:4px 0 4px 72px}

/* postamble */
.postamble{background:#2b2d31;padding:16px 20px;border-top:1px solid #3f4147;color:#949ba4;font-size:13px;text-align:center}
"""


# ── markdown renderer ───────────────────────────────────────────────────────

_CODEBLOCK_RE = _re.compile(r'```(?:(\w+)\n?)?([\s\S]*?)```', _re.DOTALL)


def _render_markdown(text: str, guild: Optional[discord.Guild] = None) -> str:
    """Convert Discord markdown + mentions to safe HTML."""
    phs: list[str] = []

    def ph(html: str) -> str:
        i = len(phs)
        phs.append(html)
        return f"\x00P{i}\x00"

    # 1. Multi-line code blocks
    def _cb(m: _re.Match) -> str:
        lang = html_lib.escape(m.group(1) or "")
        body = html_lib.escape((m.group(2) or "").strip("\n"))
        label = f'<span class="pre-lang">{lang}</span>' if lang else ""
        return ph(f'<pre class="pre">{label}<code class="pre__content">{body}</code></pre>')
    text = _CODEBLOCK_RE.sub(_cb, text)

    # 2. Inline code
    def _ic(m: _re.Match) -> str:
        return ph(f'<code class="inline-code">{html_lib.escape(m.group(1))}</code>')
    text = _re.sub(r'`([^`\n]+?)`', _ic, text)

    # 3. Mentions  <@id>  <#id>  <@&id>
    def _mention(m: _re.Match) -> str:
        uid, cid, rid = m.group(1), m.group(2), m.group(3)
        if uid:
            label = "user"
            if guild:
                member = guild.get_member(int(uid))
                if member:
                    label = html_lib.escape(member.display_name)
            return ph(f'<span class="mention">@{label}</span>')
        if cid:
            label = "channel"
            if guild:
                ch = guild.get_channel(int(cid))
                if ch:
                    label = html_lib.escape(ch.name)
            return ph(f'<span class="mention">#{label}</span>')
        if rid:
            label = "role"
            if guild:
                role = guild.get_role(int(rid))
                if role:
                    label = html_lib.escape(role.name)
            return ph(f'<span class="mention">@{label}</span>')
        return m.group(0)
    text = _re.sub(r'<@!?(\d+)>|<#(\d+)>|<@&(\d+)>', _mention, text)

    # 4. URLs
    def _url_link(m: _re.Match) -> str:
        u = html_lib.escape(m.group(0))
        return ph(f'<a href="{u}" target="_blank" rel="noreferrer">{u}</a>')
    text = _re.sub(r'https?://\S+', _url_link, text)

    # 5. HTML-escape remaining text
    text = html_lib.escape(text)

    # 6. Block formatting  (after escape so < > are safe)
    text = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text, flags=_re.DOTALL)
    text = _re.sub(r'(?<!\*)\*([^*\n]+?)\*(?!\*)', r'<em>\1</em>', text)
    text = _re.sub(r'(?<!_)_([^_\n]+?)_(?!_)', r'<em>\1</em>', text)
    text = _re.sub(r'~~(.+?)~~', r'<del>\1</del>', text, flags=_re.DOTALL)
    text = _re.sub(r'__(.+?)__', r'<u>\1</u>', text, flags=_re.DOTALL)
    text = _re.sub(r'\|\|(.+?)\|\|', r'<span class="spoiler">\1</span>', text, flags=_re.DOTALL)
    text = text.replace('@everyone', '<span class="mention">@everyone</span>')
    text = text.replace('@here', '<span class="mention">@here</span>')

    # 7. Blockquotes  (escape turned '>' → '&gt;')
    out_lines: list[str] = []
    bq_buf: list[str] = []
    consume_all = False
    for line in text.split('\n'):
        if consume_all:
            bq_buf.append(line)
        elif line.startswith('&gt;&gt;&gt; '):
            bq_buf.append(line[13:])
            consume_all = True
        elif line.startswith('&gt; '):
            bq_buf.append(line[5:])
        else:
            if bq_buf:
                out_lines.append(f'<blockquote>{"<br>".join(bq_buf)}</blockquote>')
                bq_buf = []
                consume_all = False
            out_lines.append(line)
    if bq_buf:
        out_lines.append(f'<blockquote>{"<br>".join(bq_buf)}</blockquote>')
    text = '\n'.join(out_lines)

    # 8. Restore placeholders
    for i, html in enumerate(phs):
        text = text.replace(f'\x00P{i}\x00', html)

    return text


# ── timestamp helpers ───────────────────────────────────────────────────────

def _discord_ts(dt: datetime) -> str:
    """Format like Discord: 'Today at 2:30 PM'"""
    now = datetime.now(timezone.utc)
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if aware.date() == now.date():
        prefix = "Today"
    elif (now.date() - aware.date()).days == 1:
        prefix = "Yesterday"
    else:
        prefix = aware.strftime("%m/%d/%Y")
    t = aware.strftime("%I:%M %p").lstrip("0") or "12:00 AM"
    return f"{prefix} at {t}"


def _date_label(dt: datetime) -> str:
    now = datetime.now(timezone.utc)
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    if aware.date() == now.date():
        return "Today"
    if (now.date() - aware.date()).days == 1:
        return "Yesterday"
    return aware.strftime("%B %d, %Y")


# ── embed + component renderers ─────────────────────────────────────────────

def _embed_color_hex(color: Optional[discord.Colour]) -> str:
    return f"#{color.value:06x}" if color is not None else "#5865f2"


_BTN_CLS = {
    discord.ButtonStyle.primary:   "chatlog__component-btn--primary",
    discord.ButtonStyle.success:   "chatlog__component-btn--success",
    discord.ButtonStyle.danger:    "chatlog__component-btn--danger",
    discord.ButtonStyle.secondary: "",
    discord.ButtonStyle.link:      "",
}


def _render_embed_html(embed: discord.Embed, guild: Optional[discord.Guild] = None) -> str:
    color = _embed_color_hex(embed.colour)
    parts = [
        f'<div class="chatlog__embed">',
        f'<div class="chatlog__embed-color-pill" style="background:{color}"></div>',
        '<div class="chatlog__embed-content-container">',
        '<div class="chatlog__embed-inner">',
    ]
    # Author
    if embed.author and embed.author.name:
        icon = f'<img class="chatlog__embed-author-icon" src="{html_lib.escape(embed.author.icon_url or "")}" alt="">' if embed.author.icon_url else ""
        name = html_lib.escape(embed.author.name)
        if embed.author.url:
            name = f'<a href="{html_lib.escape(embed.author.url)}" target="_blank" rel="noreferrer">{name}</a>'
        parts.append(f'<div class="chatlog__embed-author">{icon}<span class="chatlog__embed-author-name">{name}</span></div>')
    # Title
    if embed.title:
        title = html_lib.escape(str(embed.title))
        if embed.url:
            title = f'<a href="{html_lib.escape(str(embed.url))}" target="_blank" rel="noreferrer" style="color:#00a8fc">{title}</a>'
        parts.append(f'<div class="chatlog__embed-title">{title}</div>')
    # Description
    if embed.description:
        parts.append(f'<div class="chatlog__embed-description">{_render_markdown(str(embed.description), guild)}</div>')
    # Fields
    if embed.fields:
        parts.append('<div class="chatlog__embed-fields">')
        for field in embed.fields:
            cls = "" if field.inline else " chatlog__embed-field--block"
            parts.append(
                f'<div class="chatlog__embed-field{cls}">'
                f'<div class="chatlog__embed-field-name">{html_lib.escape(str(field.name))}</div>'
                f'<div class="chatlog__embed-field-value">{_render_markdown(str(field.value), guild)}</div>'
                f'</div>'
            )
        parts.append('</div>')
    # Image
    if embed.image and embed.image.url:
        parts.append(f'<img class="chatlog__embed-image" src="{html_lib.escape(embed.image.url)}" alt="image">')
    # Footer
    if embed.footer and embed.footer.text:
        ficon = f'<img class="chatlog__embed-footer-icon" src="{html_lib.escape(embed.footer.icon_url or "")}" alt="">' if embed.footer.icon_url else ""
        parts.append(
            f'<div class="chatlog__embed-footer">{ficon}'
            f'<span class="chatlog__embed-footer-text">{html_lib.escape(str(embed.footer.text))}</span>'
            f'</div>'
        )
    parts.append('</div>')  # embed-inner
    # Thumbnail (sits beside the inner content)
    if embed.thumbnail and embed.thumbnail.url:
        parts.append(f'<img class="chatlog__embed-thumbnail" src="{html_lib.escape(embed.thumbnail.url)}" alt="thumbnail">')
    parts.append('</div>')  # embed-content-container
    parts.append('</div>')  # chatlog__embed
    return "".join(parts)


def _render_components_html(components: list) -> str:
    if not components:
        return ""
    rows: list[str] = ['<div class="chatlog__components">']
    for row in components:
        for item in getattr(row, "children", []):
            label = getattr(item, "label", None) or getattr(item, "placeholder", None) or "Button"
            style = getattr(item, "style", None)
            extra = _BTN_CLS.get(style, "")
            emoji = getattr(item, "emoji", None)
            emoji_str = f"{emoji} " if emoji else ""
            url = getattr(item, "url", None)
            if url:
                rows.append(f'<a class="chatlog__component-btn" href="{html_lib.escape(url)}" target="_blank" rel="noreferrer">{emoji_str}{html_lib.escape(str(label))} ↗</a>')
            else:
                rows.append(f'<span class="chatlog__component-btn {extra}">{emoji_str}{html_lib.escape(str(label))}</span>')
    rows.append('</div>')
    return "".join(rows)


# ── main transcript generator ────────────────────────────────────────────────

async def generate_channel_transcript_html(
    channel: discord.TextChannel,
    transaction_id: str,
    outcome: str = "completed",
) -> bytes:
    """Render full channel history into a self-contained Discord-style HTML transcript."""
    guild = channel.guild
    messages = [msg async for msg in channel.history(limit=None, oldest_first=True)]

    # ── preamble ──
    guild_icon = str(guild.icon.url) if guild.icon else ""
    guild_name = html_lib.escape(guild.name)
    ch_name    = html_lib.escape(channel.name)
    export_ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    outcome_badge = {
        "completed":      '<span style="color:#3ba55d">✅ Completed</span>',
        "refunded":       '<span style="color:#ed4245">🔄 Refunded</span>',
        "force_refunded": '<span style="color:#ed4245">🔄 Force Refunded</span>',
        "cancelled":      '<span style="color:#ed4245">❌ Cancelled</span>',
    }.get(outcome, html_lib.escape(outcome))

    # Collect unique participants for preamble
    seen_ids: set[int] = set()
    participants: list[discord.abc.User] = []
    for msg in messages:
        if msg.author.id not in seen_ids and not msg.author.bot:
            seen_ids.add(msg.author.id)
            participants.append(msg.author)

    participant_html = ""
    for p in participants:
        av = html_lib.escape(str(p.display_avatar.url)) if p.display_avatar else ""
        participant_html += (
            f'<div class="preamble__participant">'
            f'<img class="preamble__participant-avatar" src="{av}" alt="">'
            f'<span class="preamble__participant-name">{html_lib.escape(p.display_name)}</span>'
            f'</div>'
        )

    body: list[str] = [
        "<!DOCTYPE html><html lang='en'><head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width,initial-scale=1'>",
        f"<title>Transcript · {html_lib.escape(transaction_id)}</title>",
        f"<style>{TRANSCRIPT_CSS}</style>",
        "</head><body>",
        '<div class="preamble">',
        f'<div class="preamble__guild">',
        f'<img class="preamble__guild-icon" src="{html_lib.escape(guild_icon)}" alt="">',
        f'<div><div class="preamble__guild-name">{guild_name}</div>'
        f'<div class="preamble__channel"><span style="color:#949ba4">#</span>'
        f'<span class="preamble__channel-name">{ch_name}</span></div></div>',
        f'</div>',
        f'<div class="preamble__meta">',
        f'Transaction <code style="background:#1e1f22;padding:1px 6px;border-radius:3px">'
        f'{html_lib.escape(transaction_id)}</code> &nbsp;·&nbsp; {outcome_badge}'
        f' &nbsp;·&nbsp; {len(messages)} messages &nbsp;·&nbsp; exported {export_ts}',
        f'</div>',
        f'<div class="preamble__participants">{participant_html}</div>',
        '</div>',  # preamble
        '<div class="chatlog">',
    ]

    # ── messages ──
    last_author_id: Optional[int] = None
    last_msg_time:  Optional[datetime] = None
    last_date_label = ""

    for msg in messages:
        # Day separator
        dl = _date_label(msg.created_at)
        if dl != last_date_label:
            body.append(
                f'<div class="chatlog__day-separator">'
                f'<span class="chatlog__day-separator-text">{dl}</span>'
                f'</div>'
            )
            last_date_label = dl
            last_author_id  = None  # force new group after separator

        # System / join messages
        if msg.type not in (discord.MessageType.default, discord.MessageType.reply) and not msg.content and not msg.embeds:
            body.append(
                f'<div class="chatlog__system-message">'
                f'<span>⚙</span>'
                f'<span>{html_lib.escape(msg.author.display_name)} — '
                f'{html_lib.escape(str(msg.type).replace("MessageType.", ""))} · '
                f'{_discord_ts(msg.created_at)}</span>'
                f'</div>'
            )
            last_author_id = None
            continue

        # Group continuation: same author within 7 minutes
        msg_time = msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at.tzinfo is None else msg.created_at
        is_continuation = (
            last_author_id == msg.author.id
            and last_msg_time is not None
            and (msg_time - last_msg_time).total_seconds() < 420
        )
        last_author_id = msg.author.id
        last_msg_time  = msg_time

        av_url = html_lib.escape(str(msg.author.display_avatar.url)) if msg.author.display_avatar else ""
        short_time = msg_time.strftime("%I:%M %p").lstrip("0") or "12:00 AM"

        if is_continuation:
            # No avatar or header — just content with hover timestamp
            body.append(
                '<div class="chatlog__message-group">'
                f'<span class="chatlog__short-time">{short_time}</span>'
                '<div class="chatlog__author-avatar-container">'
                '<div class="chatlog__author-avatar-placeholder"></div>'
                '</div>'
                '<div class="chatlog__messages">'
            )
        else:
            is_bot = msg.author.bot
            bot_tag = ' <span class="chatlog__bot-tag">BOT</span>' if is_bot else ""
            body.append(
                '<div class="chatlog__message-group">'
                '<div class="chatlog__author-avatar-container">'
                f'<img class="chatlog__author-avatar" src="{av_url}" alt="">'
                '</div>'
                '<div class="chatlog__messages">'
                '<div class="chatlog__header">'
                f'<span class="chatlog__author">{html_lib.escape(msg.author.display_name)}</span>'
                f'{bot_tag}'
                f'<span class="chatlog__timestamp">{_discord_ts(msg.created_at)}</span>'
                '</div>'
            )

        # Reply reference
        if msg.reference and msg.reference.resolved and isinstance(msg.reference.resolved, discord.Message):
            ref = msg.reference.resolved
            ref_av = html_lib.escape(str(ref.author.display_avatar.url)) if ref.author.display_avatar else ""
            ref_content = html_lib.escape((ref.content or "")[:80]) + ("…" if len(ref.content or "") > 80 else "")
            body.append(
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;font-size:13px;color:#949ba4">'
                f'<img src="{ref_av}" style="width:16px;height:16px;border-radius:50%;object-fit:cover" alt="">'
                f'<span style="color:#dbdee1;font-weight:500">{html_lib.escape(ref.author.display_name)}</span>'
                f'<span>{ref_content}</span>'
                f'</div>'
            )

        # Content
        if msg.content:
            rendered = _render_markdown(msg.content, guild)
            body.append(f'<div class="chatlog__content">{rendered}</div>')

        # Attachments
        for att in msg.attachments:
            url   = html_lib.escape(att.url)
            fname = html_lib.escape(att.filename)
            ct    = (att.content_type or "").lower()
            is_img = ct.startswith("image/") or att.filename.lower().endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif")
            )
            if is_img:
                body.append(
                    f'<div class="chatlog__attachment">'
                    f'<a href="{url}" target="_blank" rel="noreferrer">'
                    f'<img class="chatlog__attachment-media" src="{url}" alt="{fname}">'
                    f'</a></div>'
                )
            else:
                size_str = ""
                if att.size:
                    size_str = f'<span class="chatlog__attachment-filesize">{att.size // 1024} KB</span>'
                body.append(
                    f'<div class="chatlog__attachment">'
                    f'<div class="chatlog__attachment-file">'
                    f'<span class="chatlog__attachment-icon">📄</span>'
                    f'<div><a class="chatlog__attachment-filename" href="{url}" target="_blank" rel="noreferrer">{fname}</a>'
                    f'{size_str}</div>'
                    f'</div></div>'
                )

        # Embeds
        for embed in msg.embeds:
            body.append(_render_embed_html(embed, guild))

        # Components (buttons)
        if msg.components:
            body.append(_render_components_html(msg.components))

        # Reactions
        if msg.reactions:
            body.append('<div class="chatlog__reactions">')
            for rxn in msg.reactions:
                emoji = str(rxn.emoji)
                body.append(
                    f'<div class="chatlog__reaction">'
                    f'<span>{html_lib.escape(emoji)}</span>'
                    f'<span class="chatlog__reaction-count">{rxn.count}</span>'
                    f'</div>'
                )
            body.append('</div>')

        body.append('</div></div>')  # chatlog__messages, chatlog__message-group

    body.append('</div>')  # chatlog
    body.append(
        f'<div class="postamble">Generated by AutoMM Bot &nbsp;·&nbsp; {export_ts}</div>'
    )
    body.append('</body></html>')
    return "".join(body).encode("utf-8")


# ── delivery ─────────────────────────────────────────────────────────────────

async def send_deal_transcript(
    bot: "AutoMMBot",
    channel: discord.TextChannel,
    tx: dict,
    outcome: str = "completed",
) -> None:
    """Generate the Discord-style HTML transcript and deliver it to both
    participants (DM) and the configured log channel."""
    transaction_id = tx["transaction_id"]
    try:
        transcript_bytes = await generate_channel_transcript_html(channel, transaction_id, outcome)
    except Exception as exc:
        log.error("Failed to generate transcript for %s: %s\n%s", transaction_id, exc, traceback.format_exc())
        return

    filename = f"transcript_{transaction_id}.html"

    outcome_label = {
        "completed":      "✅ Completed",
        "refunded":       "🔄 Refunded",
        "force_refunded": "🔄 Force Refunded",
        "cancelled":      "❌ Cancelled",
    }.get(outcome, outcome.title())

    outcome_color = {
        "completed":      COLOR_SUCCESS,
        "refunded":       COLOR_WARNING,
        "force_refunded": COLOR_WARNING,
        "cancelled":      COLOR_DANGER,
    }.get(outcome, COLOR_INFO)

    summary_embed = make_embed(
        title=f"📄 Trade Transcript — `{transaction_id}`",
        description=(
            f"Outcome: **{outcome_label}**\n"
            "A full transcript of this trade is attached — open in any browser for the full Discord-style view."
        ),
        color=outcome_color,
        fields=[
            ("Sender",   f"<@{tx.get('sender_id')}>",   True),
            ("Receiver", f"<@{tx.get('receiver_id')}>", True),
            ("Amount",   f"{tx.get('amount_ltc', 0):.8f} LTC (${tx.get('amount_usd', 0):,.2f} USD)", False),
        ],
    )

    guild = channel.guild

    # DM both participants
    for uid in {tx.get("sender_id"), tx.get("receiver_id")}:
        if not uid:
            continue
        try:
            user = guild.get_member(uid) or await bot.fetch_user(uid)
            await user.send(
                embed=summary_embed,
                file=discord.File(fp=io.BytesIO(transcript_bytes), filename=filename),
            )
        except (discord.Forbidden, discord.HTTPException) as exc:
            log.info("Could not DM transcript to %s for %s: %s", uid, transaction_id, exc)

    # Log channel
    log_channel_id = await bot.db.get_setting("log_channel_id") or (str(LOG_CHANNEL_ID) if LOG_CHANNEL_ID else None)
    if log_channel_id:
        log_ch = guild.get_channel(int(log_channel_id))
        if log_ch:
            try:
                await log_ch.send(
                    embed=summary_embed,
                    file=discord.File(fp=io.BytesIO(transcript_bytes), filename=filename),
                )
            except (discord.Forbidden, discord.HTTPException) as exc:
                log.warning("Could not post transcript to log channel for %s: %s", transaction_id, exc)

    await bot.db.update_transaction(transaction_id, {"transcript_sent": True})


# ---------------------------------------------------------------------------
# Stage 13: Ticket finalization
# ---------------------------------------------------------------------------

async def _finalize_ticket(
    bot: "AutoMMBot", channel: discord.TextChannel, tx: dict
) -> None:
    await send_deal_transcript(bot, channel, tx, outcome="completed")

    view = TicketFinalView(bot, tx["transaction_id"])
    await channel.send(
        embed=make_embed(
            title=f"🎉 Trade Completed — `{tx['transaction_id']}`",
            description="This trade has been completed successfully. Thank you for using AutoMM!",
            color=COLOR_SUCCESS,
            fields=[
                ("Sender",         f"<@{tx['sender_id']}>",             True),
                ("Receiver",       f"<@{tx['receiver_id']}>",           True),
                ("USD Amount",     f"${tx['amount_usd']:,.2f}",         True),
                ("LTC Amount",     f"{tx['amount_ltc']:.8f} LTC",       True),
                ("Transaction ID", f"`{tx['transaction_id']}`",         False),
            ],
        ),
        view=view,
    )


class TicketFinalView(discord.ui.View):
    def __init__(self, bot: "AutoMMBot", transaction_id: str) -> None:
        super().__init__(timeout=None)
        self.bot            = bot
        self.transaction_id = transaction_id

        transcript_btn = discord.ui.Button(
            label="Download Transcript",
            style=discord.ButtonStyle.secondary,
            emoji="📄",
            custom_id=f"tf_transcript_{transaction_id}",
        )
        transcript_btn.callback = self._on_transcript
        self.add_item(transcript_btn)

        close_btn = discord.ui.Button(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            emoji="🔒",
            custom_id=f"tf_close_{transaction_id}",
        )
        close_btn.callback = self._on_close
        self.add_item(close_btn)

    async def _on_transcript(self, interaction: discord.Interaction) -> None:
        tx = await self.bot.db.get_transaction(self.transaction_id)
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            transcript_bytes = await generate_channel_transcript_html(
                interaction.channel, self.transaction_id, outcome="completed"
            )
        except Exception as exc:
            log.error("Manual transcript generation failed for %s: %s", self.transaction_id, exc)
            await interaction.followup.send("❌ Failed to generate transcript.", ephemeral=True)
            return
        file = discord.File(
            fp=io.BytesIO(transcript_bytes),
            filename=f"transcript_{self.transaction_id}.html",
        )
        await interaction.followup.send(
            "📄 Your transcript (open in any browser for the full Discord-style view):",
            file=file,
            ephemeral=True,
        )

    async def _on_close(self, interaction: discord.Interaction) -> None:
        tx = await self.bot.db.get_transaction(self.transaction_id)
        uid = interaction.user.id
        is_participant = tx and uid in (
            tx.get("sender_id"), tx.get("receiver_id"), tx.get("initiator_id")
        )
        user_is_admin = is_admin(interaction.user)
        if not is_participant and not user_is_admin:
            await interaction.response.send_message(
                "❌ Only participants or admins may close this ticket.", ephemeral=True
            )
            return
        await interaction.response.defer()
        await interaction.channel.send(
            embed=make_embed(
                "🔒 Closing Ticket",
                "This channel will be deleted in 5 seconds.",
                COLOR_DANGER,
            )
        )
        await asyncio.sleep(5)
        try:
            await interaction.channel.delete(reason=f"Trade {self.transaction_id} closed")
        except discord.Forbidden:
            await interaction.channel.send("❌ Missing permissions to delete channel.")


# ---------------------------------------------------------------------------
# Main bot class
# ---------------------------------------------------------------------------

class AutoMMBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

        self.db         = Database(MONGODB_URI)
        self.aprion     = AprionClient(APRIONE_ACCOUNT, APRIONE_TRANSFER_KEY)
        self._monitoring: set[str] = set()   # TXs actively being polled for payment
        self._recovering: set[str] = set()   # TXs being recovered into delivery stage

    # --- Lifecycle ---

    async def setup_hook(self) -> None:
        await self.db.setup_indexes()
        # Register persistent panel view so its button survives restarts
        self.add_view(PanelView(self))
        # Restore active transaction views
        await self._restore_views()
        # Start background monitor loop
        self._monitor_loop.start()
        # Sync slash commands
        await self.tree.sync()
        log.info("setup_hook complete. Commands synced.")

    async def on_ready(self) -> None:
        log.info("Logged in as %s (ID: %s)", self.user, self.user.id)
        activity_type = await self.db.get_setting("status_type") or "watching"
        activity_text = await self.db.get_setting("status_text") or "trades | /help"
        atype = {
            "watching":  discord.ActivityType.watching,
            "playing":   discord.ActivityType.playing,
            "listening": discord.ActivityType.listening,
            "competing": discord.ActivityType.competing,
        }.get(activity_type, discord.ActivityType.watching)
        await self.change_presence(activity=discord.Activity(type=atype, name=activity_text))

    async def close(self) -> None:
        self._monitor_loop.cancel()
        await self.aprion.close()
        await super().close()

    # --- Restore persistent views after restart ---

    async def _restore_views(self) -> None:
        active = await self.db.get_active_transactions()
        log.info("Restoring views for %d active transactions.", len(active))
        for tx in active:
            tid   = tx["transaction_id"]
            stage = tx["stage"]
            sid   = tx.get("sender_id") or 0
            rid   = tx.get("receiver_id") or 0
            iid   = tx.get("initiator_id") or 0
            oid   = tx.get("other_id") or 0

            if stage == Stage.ROLE_SELECT:
                self.add_view(RoleSelectView(self, tid, iid, oid))
            elif stage == Stage.TOS:
                sender_tos  = tx.get("sender_tos")
                receiver_tos = tx.get("receiver_tos")
                recv_accepted = tx.get("receiver_tos_accepted", False)
                send_accepted = tx.get("sender_tos_accepted", False)
                # Sender side
                if sender_tos is None:
                    self.add_view(TosPromptView(self, tid, sid, is_sender=True))
                elif sender_tos != "__skip__" and not recv_accepted:
                    self.add_view(TosAcceptView(self, tid, sid, rid, author_is_sender=True))
                # Receiver side
                if receiver_tos is None:
                    self.add_view(TosPromptView(self, tid, rid, is_sender=False))
                elif receiver_tos != "__skip__" and not send_accepted:
                    self.add_view(TosAcceptView(self, tid, rid, sid, author_is_sender=False))
            elif stage == Stage.AMOUNT:
                self.add_view(AmountInputView(self, tid, sid))
                self.add_view(AmountAgreeView(self, tid))
            elif stage in (Stage.DEPOSIT, Stage.AWAITING_FUNDS):
                self.add_view(DepositView(self, tid, sid))
                if tid not in self._monitoring:
                    # If deposit was already confirmed but stage update was lost,
                    # jump straight to delivery recovery instead of re-polling.
                    if tx.get("deposit_confirmed"):
                        log.warning(
                            "TX %s has deposit_confirmed=True but stage=%s — recovering delivery.",
                            tid, stage,
                        )
                        self.loop.create_task(self._recover_delivery(tx))
                    else:
                        self.loop.create_task(self.monitor_payment(tid))
            elif stage == Stage.DELIVERY:
                self.add_view(DeliveryView(self, tid))
                # If the bot crashed before the delivery message was sent, re-send it.
                if not tx.get("delivery_stage_started"):
                    log.warning(
                        "TX %s is in DELIVERY stage but delivery_stage_started=False — recovering.",
                        tid,
                    )
                    self.loop.create_task(self._recover_delivery(tx))
            elif stage == Stage.RELEASE:
                self.add_view(ReleaseView(self, tid))
                # Any in-flight confirmation message is gone after restart;
                # clear the guard so Sender can press Release again.
                if tx.get("release_pending"):
                    self.loop.create_task(
                        self.db.update_transaction(tid, {"release_pending": False})
                    )
            elif stage == Stage.WITHDRAWAL:
                self.add_view(WithdrawalInputView(self, tid, rid))
            elif stage in (Stage.FEEDBACK, Stage.COMPLETED):
                # One combined view per transaction; handles both parties internally
                if not (tx.get("feedback_sent_sender") and tx.get("feedback_sent_receiver")):
                    self.add_view(FeedbackView(self, tid, sid, rid, star_emoji="⭐"))
                self.add_view(TicketFinalView(self, tid))
        log.info("View restoration complete.")

    # --- Payment monitoring ---

    async def monitor_payment(self, transaction_id: str) -> None:
        if transaction_id in self._monitoring:
            return
        self._monitoring.add(transaction_id)
        log.info("Monitoring payments for %s", transaction_id)
        try:
            while True:
                tx = await self.db.get_transaction(transaction_id)
                if tx is None or tx["stage"] not in (Stage.DEPOSIT, Stage.AWAITING_FUNDS):
                    break
                done = await _poll_payment(self, transaction_id)
                if done:
                    break
                await asyncio.sleep(POLL_INTERVAL)
        except Exception as exc:
            log.error(
                "monitor_payment error for %s: %s\n%s",
                transaction_id, exc, traceback.format_exc(),
            )
        finally:
            self._monitoring.discard(transaction_id)
            log.info("Stopped monitoring %s", transaction_id)

    async def _recover_delivery(self, tx: dict) -> None:
        """
        Re-send the delivery stage message for a trade that is in DELIVERY stage
        (or deposit_confirmed=True) but whose delivery message was never sent.
        Waits until the bot is fully ready before attempting to fetch the channel.
        """
        await self.wait_until_ready()
        tid = tx["transaction_id"]
        try:
            guild   = self.get_guild(tx["guild_id"])
            channel = guild.get_channel(tx["channel_id"]) if guild else None
            if not channel:
                log.error("_recover_delivery: channel not found for TX %s", tid)
                return

            # Make sure stage is DELIVERY in DB (fix it if it was stuck at DEPOSIT)
            if tx["stage"] != Stage.DELIVERY:
                await self.db.update_transaction(tid, {
                    "stage":             Stage.DELIVERY,
                    "deposit_confirmed": True,
                })
                tx = await self.db.get_transaction(tid)

            log.info("_recover_delivery: resending delivery message for TX %s", tid)

            # Send a catch-up notice so the channel knows what happened
            await channel.send(embed=make_embed(
                title="⚠️ Trade Resumed After Restart",
                description=(
                    "The bot restarted after your payment was confirmed.\n"
                    "Resuming the trade now — please check below."
                ),
                color=COLOR_WARNING,
            ))
            await _start_delivery_stage(self, channel, tx)

        except Exception as exc:
            log.error(
                "_recover_delivery failed for TX %s: %s\n%s",
                tid, exc, traceback.format_exc(),
            )

    @tasks.loop(seconds=POLL_INTERVAL * 2)
    async def _monitor_loop(self) -> None:
        """Safety net: catch transactions whose task died."""
        try:
            active = await self.db.get_active_transactions()
            for tx in active:
                tid   = tx["transaction_id"]
                stage = tx["stage"]
                # Restart payment monitoring if the poll task died
                if (
                    stage in (Stage.DEPOSIT, Stage.AWAITING_FUNDS)
                    and tid not in self._monitoring
                ):
                    if tx.get("deposit_confirmed"):
                        # Confirmed but stage not advanced — recover delivery
                        self.loop.create_task(self._recover_delivery(tx))
                    else:
                        self.loop.create_task(self.monitor_payment(tid))
                # Recover stuck DELIVERY trades where the message was never sent
                elif (
                    stage == Stage.DELIVERY
                    and not tx.get("delivery_stage_started")
                    and tid not in self._recovering
                ):
                    self._recovering.add(tid)
                    self.loop.create_task(self._recover_delivery(tx))
        except Exception as exc:
            log.error("_monitor_loop error: %s", exc)

    @_monitor_loop.before_loop
    async def _before_monitor(self) -> None:
        await self.wait_until_ready()

    # --- Utility helpers ---

    async def notify_admins(self, guild: discord.Guild, message: str) -> None:
        ping = f"<@&{ADMIN_ROLE_ID}> " if ADMIN_ROLE_ID else ""
        full_message = f"{ping}{message}"
        allowed = discord.AllowedMentions(roles=True) if ADMIN_ROLE_ID else discord.AllowedMentions.none()
        log_channel_id = await self.db.get_setting("log_channel_id") or (str(LOG_CHANNEL_ID) if LOG_CHANNEL_ID else None)
        if log_channel_id:
            ch = guild.get_channel(int(log_channel_id))
            if ch:
                try:
                    await ch.send(full_message, allowed_mentions=allowed)
                    return
                except Exception:
                    pass
        if guild.system_channel:
            try:
                await guild.system_channel.send(full_message, allowed_mentions=allowed)
            except Exception:
                pass

    async def notify_user_ticket_opened(
        self, user: discord.abc.User, channel: discord.TextChannel, transaction_id: str, other: discord.abc.User
    ) -> None:
        """DM a user letting them know a trade ticket has been opened with them."""
        try:
            await user.send(
                embed=make_embed(
                    title="📂 A Trade Ticket Was Opened With You",
                    description=(
                        f"**{other.display_name}** opened an AutoMM trade with you in "
                        f"**{channel.guild.name}**.\n\n"
                        f"Head to {channel.mention} to get started."
                    ),
                    color=COLOR_INFO,
                    fields=[("Transaction ID", f"`{transaction_id}`", False)],
                )
            )
        except (discord.Forbidden, discord.HTTPException):
            log.info("Could not DM user %s about new ticket %s (DMs closed).", user.id, transaction_id)

    async def post_log_embed(self, guild: discord.Guild, embed: discord.Embed) -> None:
        log_channel_id = await self.db.get_setting("log_channel_id") or (str(LOG_CHANNEL_ID) if LOG_CHANNEL_ID else None)
        if log_channel_id:
            ch = guild.get_channel(int(log_channel_id))
            if ch:
                try:
                    await ch.send(embed=embed)
                except Exception:
                    pass

    async def post_completed_announcement(
        self, guild: discord.Guild, tx: dict
    ) -> None:
        channel_id = await self.db.get_setting("completed_channel_id")
        if not channel_id:
            return
        ch = guild.get_channel(int(channel_id))
        if not ch:
            return

        mm_name         = await self.db.get_setting("mm_name") or "AutoMM"
        star            = await _get_star_emoji(self)
        arr             = await _get_arrow_emoji(self)
        withdrawal_txid = tx.get("withdrawal_txid") or tx.get("deposit_txid") or ""
        tid             = tx.get("transaction_id", "")
        guild_icon      = guild.icon.url if guild.icon else None

        embed = make_embed(
            title=f"⭐️ Deal Completed ⭐️",
            description=f"{arr} Deal **{tid}** has been successfully completed.",
            color=COLOR_SUCCESS,
            thumbnail_url=CHECKMARK_IMG,
            footer=mm_name,
            footer_icon_url=guild_icon,
            fields=[
                ("Amount (USD)", f"${tx['amount_usd']:,.2f}",    False),
                ("Amount (LTC)", f"{tx['amount_ltc']:.8f} LTC",  False),
                ("Buyer",        f"<@{tx['sender_id']}>",        False),
                ("Seller",       f"<@{tx['receiver_id']}>",      False),
            ],
        )

        view = discord.ui.View()
        if withdrawal_txid and withdrawal_txid not in ("pending", ""):
            view.add_item(discord.ui.Button(
                label="View on Blockchair",
                style=discord.ButtonStyle.link,
                url=f"https://blockchair.com/litecoin/transaction/{withdrawal_txid}",
                emoji="🔗",
            ))

        try:
            await ch.send(embed=embed, view=view)
        except Exception as exc:
            log.warning("Failed to post completion announcement: %s", exc)

    async def post_feedback_embed(
        self,
        interaction: discord.Interaction,
        transaction_id: str,
        reviewer_id: int,
        rating: int,
        comment: Optional[str],
    ) -> None:
        """Post a formatted feedback embed to the configured feedback channel."""
        channel_id = await self.db.get_setting("feedback_channel_id")
        if not channel_id:
            return
        ch = interaction.guild.get_channel(int(channel_id))
        if not ch:
            return

        mm_name  = await self.db.get_setting("mm_name") or "AutoMM"
        star_e   = "⭐"
        reviewer = interaction.guild.get_member(reviewer_id)
        stars    = star_e * rating
        now      = datetime.now(timezone.utc)

        embed = discord.Embed(
            title="Deal Feedback",
            description=f"An user has submitted their feedback for **{mm_name}!**",
            color=COLOR_PRIMARY,
            timestamp=now,
        )

        # Server icon as small author logo
        guild_icon = interaction.guild.icon
        if guild_icon:
            embed.set_author(name=mm_name, icon_url=guild_icon.url)
        else:
            embed.set_author(name=mm_name)

        # Reviewer's avatar as thumbnail (top-right corner)
        if reviewer:
            embed.set_thumbnail(url=reviewer.display_avatar.url)

        embed.add_field(name="Rating", value=f"{stars} ({rating}/5)", inline=False)
        embed.add_field(
            name="User",
            value=reviewer.mention if reviewer else f"<@{reviewer_id}>",
            inline=False,
        )
        embed.add_field(
            name="Submitted At",
            value=discord.utils.format_dt(now, style="F"),
            inline=False,
        )
        if comment:
            embed.add_field(name="Comment", value=comment, inline=False)

        # Footer with server icon
        if guild_icon:
            embed.set_footer(text="Thank you for your feedback!", icon_url=guild_icon.url)
        else:
            embed.set_footer(text="Thank you for your feedback!")

        try:
            await ch.send(embed=embed)
        except Exception as exc:
            log.warning("Failed to post feedback embed: %s", exc)


# ---------------------------------------------------------------------------
# Admin check decorator
# ---------------------------------------------------------------------------

def is_admin(member: discord.Member) -> bool:
    """
    Returns True if the member has admin access.
    Admin access is granted when ANY of the following is true:
      1. ADMIN_ROLE_ID env var is set and the member has that role.
      2. The member has the server Administrator permission.
    """
    if ADMIN_ROLE_ID and any(r.id == ADMIN_ROLE_ID for r in member.roles):
        return True
    return member.guild_permissions.administrator


def admin_only() -> app_commands.check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return False
        return True
    return app_commands.check(predicate)


# ---------------------------------------------------------------------------
# Slash commands
# ---------------------------------------------------------------------------

def register_commands(bot: AutoMMBot) -> None:
    tree = bot.tree

    # ── Panel ──────────────────────────────────────────────────────────────

    @tree.command(name="panel", description="Send the AutoMM panel to this channel")
    @admin_only()
    async def cmd_panel(interaction: discord.Interaction) -> None:
        guild_icon = (
            interaction.guild.icon.url
            if interaction.guild and interaction.guild.icon
            else None
        )
        star = await _get_star_emoji(bot)
        dot  = await _get_dot_emoji(bot)
        embed = make_embed(
            title=f"{star} AutoMM Service {star}",
            description=(
                "**Minimum Amount →** $0.10\n\n"
                "**How to Start**\n"
                f"{dot} Click **Start New Deal** and enter the **User ID** of the person you're dealing with *(not their username)*.\n"
                f"{dot} Both parties must be **in this server** before creating a ticket.\n"
                f"{dot} **Discuss and agree** on all terms before making any payment.\n\n"
                "**Important**\n"
                "\u00a0\u00a0Keep all deal-related chat inside your ticket.\n"
                "\u00a0\u00a0The bot will **never DM you** — report any suspicious DMs to staff immediately.\n"
                "\u00a0\u00a0We can only hold **LTC (Litecoin)**.\n"
                "\u00a0\u00a0Always assign **Sender** and **Receiver** roles carefully."
            ),
            color=COLOR_PRIMARY,
            timestamp=False,
            thumbnail_url=guild_icon,
        )
        await interaction.channel.send(embed=embed, view=PanelView(bot))
        await interaction.response.send_message("✅ Panel sent.", ephemeral=True)

    # ── Configuration ──────────────────────────────────────────────────────

    @tree.command(name="setmmrole", description="Set the role required to open MM trades (omit to remove restriction)")
    @admin_only()
    @app_commands.describe(role="Role that members must have to open a trade (leave empty to allow everyone)")
    async def cmd_setmmrole(
        interaction: discord.Interaction, role: Optional[discord.Role] = None
    ) -> None:
        if role:
            await bot.db.set_setting("user_role_id", str(role.id))
            await interaction.response.send_message(
                f"✅ MM trades are now restricted to {role.mention}.", ephemeral=True
            )
        else:
            await bot.db.set_setting("user_role_id", None)
            await interaction.response.send_message(
                "✅ MM trade restriction removed — any server member can open a trade.",
                ephemeral=True,
            )

    @tree.command(name="setlogchannel", description="Set the channel for bot logs")
    @admin_only()
    @app_commands.describe(channel="The channel for logs")
    async def cmd_setlog(
        interaction: discord.Interaction, channel: discord.TextChannel
    ) -> None:
        await bot.db.set_setting("log_channel_id", str(channel.id))
        await interaction.response.send_message(
            f"✅ Log channel set to {channel.mention}.", ephemeral=True
        )

    @tree.command(name="setticketcategory", description="Set the category for trade ticket channels")
    @admin_only()
    @app_commands.describe(category="Category for ticket channels")
    async def cmd_setcategory(
        interaction: discord.Interaction, category: discord.CategoryChannel
    ) -> None:
        await bot.db.set_setting("ticket_category_id", str(category.id))
        await interaction.response.send_message(
            f"✅ Ticket category set to **{category.name}**.", ephemeral=True
        )

    @tree.command(name="setcompletedchannel", description="Set the channel for completed trade announcements")
    @admin_only()
    @app_commands.describe(channel="Announcement channel")
    async def cmd_setcompleted(
        interaction: discord.Interaction, channel: discord.TextChannel
    ) -> None:
        await bot.db.set_setting("completed_channel_id", str(channel.id))
        await interaction.response.send_message(
            f"✅ Completed announcements → {channel.mention}.", ephemeral=True
        )

    @tree.command(name="setcompletedmessage", description="Set the custom completion announcement message")
    @admin_only()
    @app_commands.describe(message="Custom message text")
    async def cmd_setcompletedmsg(
        interaction: discord.Interaction, message: str
    ) -> None:
        await bot.db.set_setting("completed_message", message)
        await interaction.response.send_message("✅ Message updated.", ephemeral=True)

    @tree.command(name="testcompletedmessage", description="Send a test completion announcement")
    @admin_only()
    async def cmd_testcompleted(interaction: discord.Interaction) -> None:
        fake_tx = {
            "transaction_id": "TEST0000",
            "sender_id":      interaction.user.id,
            "receiver_id":    interaction.user.id,
            "amount_usd":     100.0,
            "amount_ltc":     2.5,
            "withdrawal_txid": "be859d9978893fd98328d7c658a634c5d3c936a5af6df9a447810751d7490f54",
        }
        await bot.post_completed_announcement(interaction.guild, fake_tx)
        await interaction.response.send_message("✅ Test announcement sent.", ephemeral=True)

    @tree.command(name="testfeedback", description="Send a test feedback embed to the feedback channel")
    @admin_only()
    async def cmd_testfeedback(interaction: discord.Interaction) -> None:
        """Post a fake feedback embed so you can preview the feedback channel layout."""
        channel_id = await bot.db.get_setting("feedback_channel_id")
        if not channel_id:
            await interaction.response.send_message(
                "❌ No feedback channel set. Use `/setfeedbackchannel` first.", ephemeral=True
            )
            return

        ch = interaction.guild.get_channel(int(channel_id))
        if not ch:
            await interaction.response.send_message(
                "❌ Feedback channel not found. Set it again with `/setfeedbackchannel`.", ephemeral=True
            )
            return

        mm_name    = await bot.db.get_setting("mm_name") or "AutoMM"
        star_e     = "⭐"
        reviewer   = interaction.user
        rating     = 5
        comment    = "Great service, very smooth and fast! Highly recommend."
        stars      = star_e * rating
        now        = datetime.now(timezone.utc)
        guild_icon = interaction.guild.icon

        embed = discord.Embed(
            title="Deal Feedback",
            description=f"An user has submitted their feedback for **{mm_name}!**",
            color=COLOR_PRIMARY,
            timestamp=now,
        )
        if guild_icon:
            embed.set_author(name=mm_name, icon_url=guild_icon.url)
        else:
            embed.set_author(name=mm_name)

        embed.set_thumbnail(url=reviewer.display_avatar.url)
        embed.add_field(name="Rating",        value=f"{stars} ({rating}/5)",          inline=False)
        embed.add_field(name="User",          value=reviewer.mention,                  inline=False)
        embed.add_field(name="Submitted At",  value=discord.utils.format_dt(now, "F"), inline=False)
        embed.add_field(name="Comment",       value=comment,                           inline=False)

        if guild_icon:
            embed.set_footer(text="Thank you for your feedback!", icon_url=guild_icon.url)
        else:
            embed.set_footer(text="Thank you for your feedback!")

        await ch.send(embed=embed)
        await interaction.response.send_message(
            f"✅ Test feedback embed sent to {ch.mention}.", ephemeral=True
        )

    @tree.command(name="testembed", description="Preview every embed in the full AutoMM trade flow")
    @admin_only()
    async def cmd_testembed(interaction: discord.Interaction) -> None:
        """Send every flow embed with fake data so you can review them all at once."""
        await interaction.response.send_message("✅ Sending full embed flow preview…", ephemeral=True)

        ch      = interaction.channel
        mm_name = await bot.db.get_setting("mm_name") or "AutoMM"
        star    = await _get_star_emoji(bot)
        arr     = await _get_arrow_emoji(bot)
        dot     = await _get_dot_emoji(bot)
        me      = interaction.user.mention

        # ── Fake trade data ────────────────────────────────────────────────
        TID        = "TEST1234"
        USD        = 150.00
        LTC        = 2.12345678
        PRICE      = 70.65
        ADDR       = "LcNibySYh4brMPSXYqJvA5knMudqRAcs6E"
        TXID       = "9dbc2f27f0bae5a35a4508074dcdbaf79daa590f702e900a1c7162057e0ed0d8"
        guild_icon = (
            interaction.guild.icon.url
            if interaction.guild and interaction.guild.icon
            else None
        )

        def _view(*labels_styles: tuple[str, discord.ButtonStyle]) -> discord.ui.View:
            """Return a View with all buttons disabled (for display only)."""
            v = discord.ui.View()
            for label, style in labels_styles:
                b = discord.ui.Button(label=label, style=style, disabled=True)
                v.add_item(b)
            return v

        async def _step(n: int, title: str) -> None:
            await ch.send(f"**— Step {n}: {title} —**")

        # ── Step 1: Panel + Role Selection (combined) ──────────────────────
        await _step(1, "Panel + Role Selection")
        await ch.send(
            embed=make_embed(
                title=f"🔄 AutoMM Trade — `{TID}`",
                description="Both participants must select their roles to continue.",
                color=COLOR_PRIMARY,
                fields=[
                    ("Sender",         "_Not selected_", True),
                    ("Receiver",       "_Not selected_", True),
                    ("Transaction ID", f"`{TID}`",       False),
                ],
            ),
            view=_view(
                ("Sender 💸",   discord.ButtonStyle.primary),
                ("Receiver 📦", discord.ButtonStyle.primary),
                ("Cancel ❌",   discord.ButtonStyle.danger),
            ),
        )

        # ── Step 2: Roles Selected + Both Confirmed (same embed) ─────────────
        await _step(2, "Roles Selected — both press Confirm in same embed")
        await ch.send(
            embed=make_embed(
                title=f"✅ Roles Selected — `{TID}`",
                description=(
                    "Both roles selected. "
                    "Each participant must press **Confirm** to continue."
                ),
                color=COLOR_SUCCESS,
                fields=[
                    ("Sender Role",   me,          True),
                    ("Receiver Role", me,          True),
                    ("Transaction ID", f"`{TID}`", False),
                ],
            ),
            view=_view(
                ("Sender 💸",   discord.ButtonStyle.primary),
                ("Receiver 📦", discord.ButtonStyle.secondary),
                ("Confirm ✅",  discord.ButtonStyle.success),
                ("Cancel ❌",   discord.ButtonStyle.danger),
            ),
        )

        # ── Step 3: Amount Entry ────────────────────────────────────────────
        await _step(3, "Amount Entry")
        await ch.send(
            embed=make_embed(
                title="💵 Enter Trade Amount",
                description="**Sender**, press the button below to enter the USD amount for this trade.",
                color=COLOR_INFO,
            ),
            view=_view(("💵 Enter Amount", discord.ButtonStyle.primary)),
        )

        # ── Step 4: Amount Agreement ────────────────────────────────────────
        await _step(4, f"{star} Deal Amount Confirmation {star}")
        await ch.send(
            embed=make_embed(
                title=f"{star} Deal Amount Confirmation {star}",
                description=(
                    f"{arr} **Amount : ${USD:,.2f} USD**\n\n"
                    f"{arr} Accept Or Reject the Deal"
                ),
                color=COLOR_SUCCESS,
                thumbnail_url=guild_icon,
                footer=mm_name,
                footer_icon_url=BADGE_IMG,
            ),
            view=_view(
                ("Accept", discord.ButtonStyle.success),
                ("Reject", discord.ButtonStyle.danger),
            ),
        )

        # ── Step 5: Waiting For Payment ─────────────────────────────────────
        await _step(5, f"{star} Waiting For Payment {star}")
        await ch.send(
            embed=make_embed(
                title=f"{star} Waiting For Payment {star}",
                description=(
                    f"Payment Credentials are Given Below\n\n"
                    f"{arr} **Address** : `{ADDR}`\n"
                    f"{arr} **Amount to pay** : {LTC:.8f} LTC\n\n"
                    f"Your payment will be Detected Automatically"
                ),
                color=COLOR_PRIMARY,
                thumbnail_url=LTC_LOGO,
                footer=mm_name,
                footer_icon_url=BADGE_IMG,
            ),
            view=_view(
                ("Copy Address", discord.ButtonStyle.primary),
                ("QR Code",      discord.ButtonStyle.secondary),
                ("Cancel",       discord.ButtonStyle.danger),
            ),
        )

        # ── Step 6: Pending Payment Detected ───────────────────────────────
        await _step(6, f"{star} Pending Payment Detected {star}")
        await ch.send(embed=make_embed(
            title=f"{star} Pending Payment Detected {star}",
            description=(
                f"{arr} A Pending Transaction Is Detected\n\n"
                f"{arr} **${USD:,.2f}** ( **{LTC:.8f} LTC** )"
            ),
            color=COLOR_PRIMARY,
            footer=f"{mm_name} • Awaiting confirmation",
            footer_icon_url=BADGE_IMG,
            thumbnail_url=SPINNER_GIF,
        ))

        # ── Step 7: Payment Received — Release / Refund ─────────────────────
        await _step(7, f"{star} Payment Received — Release / Refund {star}")
        await ch.send(
            embed=make_embed(
                title=f"{star} Payment Received {star}",
                description=(
                    f"{arr} The Transaction is now Confirmed\n"
                    f"{arr} Refund Or Release Can be Processed Now\n\n"
                    f"{dot} **${USD:,.2f} USD** ( **{LTC:.8f} LTC** )"
                ),
                color=COLOR_SUCCESS,
                footer=f"{mm_name} • Payment Confirmed",
                footer_icon_url=BADGE_IMG,
                thumbnail_url=CHECKMARK_IMG,
            ),
            view=_view(
                ("Release",       discord.ButtonStyle.success),
                ("Refund",        discord.ButtonStyle.danger),
                ("Raise Dispute", discord.ButtonStyle.secondary),
            ),
        )

        # ── Step 8: After Release → Release Payment Confirmation ────────────
        await _step(8, f"{star} Release Payment Confirmation {star}  (after Release pressed)")
        rel_conf_view = discord.ui.View()
        rel_conf_view.add_item(discord.ui.Button(
            label="✅ Confirm Release",
            style=discord.ButtonStyle.success,
            disabled=True,   # activates after 5 s in the real flow
        ))
        rel_conf_view.add_item(discord.ui.Button(
            label="❌ Cancel",
            style=discord.ButtonStyle.danger,
            disabled=True,
        ))
        await ch.send(
            embed=make_embed(
                title=f"{star} Release Payment Confirmation {star}",
                description=(
                    f"{dot} **Amount to Release**\n"
                    f"{LTC:.8f} LTC\n"
                    f"≈ ${USD:,.2f} USD\n\n"
                    f"{dot} **Releasing To**\n"
                    f"{me}\n\n"
                    f"{dot} **Warning**\n"
                    f"This action cannot be undone.\nPlease confirm carefully."
                ),
                color=COLOR_SUCCESS,
                footer=f"{mm_name} • Confirm button will activate in 5 seconds",
                footer_icon_url=BADGE_IMG,
                thumbnail_url=guild_icon,
            ),
            view=rel_conf_view,
        )

        # ── Step 15: After Refund → Dispute Opened (alternate path) ─────────
        await _step(9, "⚖️ Dispute Opened  (after Refund pressed)")
        await ch.send(embed=make_embed(
            title="⚖️ Dispute Opened",
            description="A dispute has been opened. An admin will assist shortly.",
            color=COLOR_DANGER,
            fields=[("Transaction ID", f"`{TID}`", False)],
        ))

        # ── Step 16: Releasing Funds ─────────────────────────────────────────
        await _step(10, f"{star} Releasing Funds... {star}  (after Confirm Release)")
        await ch.send(embed=make_embed(
            title=f"{star} Releasing Funds... {star}",
            description=(
                f"{arr} Sending **{LTC:.8f} LTC** to:\n"
                f"```\n{ADDR}\n```"
            ),
            color=COLOR_SUCCESS,
            footer=mm_name,
            footer_icon_url=BADGE_IMG,
            thumbnail_url=SPINNER_GIF,
        ))

        # ── Step 17: Payment Sent ────────────────────────────────────────────
        await _step(11, f"{star} Payment Sent {star}")
        txid_link = f"[{TXID[:20]}…](https://blockchair.com/litecoin/transaction/{TXID})"
        sent_view = discord.ui.View()
        sent_view.add_item(discord.ui.Button(
            label="View on Blockchair",
            style=discord.ButtonStyle.link,
            url=f"https://blockchair.com/litecoin/transaction/{TXID}",
            emoji="🔗",
        ))
        await ch.send(
            embed=make_embed(
                title=f"{star} Payment Sent {star}",
                description=f"{arr} The Litecoin payment has been successfully sent.",
                color=COLOR_SUCCESS,
                footer=mm_name,
                footer_icon_url=BADGE_IMG,
                thumbnail_url=CHECKMARK_IMG,
                fields=[
                    ("Deal ID",        TID,        False),
                    ("To Address",     f"`{ADDR}`", False),
                    ("Amount Sent",    f"**{LTC:.8f} LTC**", False),
                    ("Transaction ID", txid_link,   False),
                ],
            ),
            view=sent_view,
        )

        # ── Step 18: Leave Feedback ──────────────────────────────────────────
        await _step(12, "Leave Feedback")
        await ch.send(
            embed=make_embed(
                title="⭐ Leave Feedback",
                description=(
                    "The deal is complete! Both parties may now leave feedback.\n"
                    "Press **Submit Feedback** below to rate your experience."
                ),
                color=COLOR_INFO,
            ),
            view=_view(("⭐ Submit Feedback", discord.ButtonStyle.primary)),
        )

        # ── Step 19: Trade Cancelled (alternate path) ─────────────────────
        await _step(13, "Trade Cancelled (alternate path)")
        await ch.send(embed=make_embed(
            title="❌ Trade Cancelled",
            description="**Reason:** Product condition rejected by Receiver",
            color=COLOR_DANGER,
            fields=[("Transaction ID", f"`{TID}`", False)],
        ))

    @tree.command(name="setfeedbackchannel", description="Set the channel where feedback embeds are posted")
    @admin_only()
    @app_commands.describe(channel="Feedback log channel")
    async def cmd_setfeedbackchannel(
        interaction: discord.Interaction, channel: discord.TextChannel
    ) -> None:
        await bot.db.set_setting("feedback_channel_id", str(channel.id))
        await interaction.response.send_message(
            f"✅ Feedback channel set to {channel.mention}.", ephemeral=True
        )

    @tree.command(name="setmmname", description="Set the middleman service display name (e.g. Frost Auto Middleman)")
    @admin_only()
    @app_commands.describe(name="Display name shown on embeds and announcements")
    async def cmd_setmmname(
        interaction: discord.Interaction, name: str
    ) -> None:
        await bot.db.set_setting("mm_name", name)

        # ── Preview button ─────────────────────────────────────────────────
        class _PreviewView(discord.ui.View):
            def __init__(self) -> None:
                super().__init__(timeout=120)

            @discord.ui.button(label="🔍 Preview Deal Announcement", style=discord.ButtonStyle.secondary)
            async def preview(self, btn_interaction: discord.Interaction, button: discord.ui.Button) -> None:
                button.disabled = True
                await btn_interaction.response.edit_message(view=self)
                fake_tx = {
                    "transaction_id":  "TEST1234",
                    "sender_id":       btn_interaction.user.id,
                    "receiver_id":     btn_interaction.user.id,
                    "amount_usd":      150.0,
                    "amount_ltc":      2.12345678,
                    "withdrawal_txid": "9dbc2f27f0bae5a35a4508074dcdbaf79daa590f702e900a1c7162057e0ed0d8",
                }
                await bot.post_completed_announcement(btn_interaction.guild, fake_tx)

        await interaction.response.send_message(
            f"✅ MM name set to **{name}**.\nPress the button below to preview how the deal announcement looks.",
            view=_PreviewView(),
            ephemeral=True,
        )

    # ── Bot status ─────────────────────────────────────────────────────────

    @tree.command(name="setstatus", description="Set the bot's activity status")
    @admin_only()
    @app_commands.describe(
        type="Activity type",
        text="Status text shown after the activity type",
    )
    @app_commands.choices(type=[
        app_commands.Choice(name="Watching",   value="watching"),
        app_commands.Choice(name="Playing",    value="playing"),
        app_commands.Choice(name="Listening",  value="listening"),
        app_commands.Choice(name="Competing",  value="competing"),
    ])
    async def cmd_setstatus(
        interaction: discord.Interaction, type: str, text: str
    ) -> None:
        await bot.db.set_setting("status_type", type)
        await bot.db.set_setting("status_text", text)
        atype = {
            "watching":  discord.ActivityType.watching,
            "playing":   discord.ActivityType.playing,
            "listening": discord.ActivityType.listening,
            "competing": discord.ActivityType.competing,
        }[type]
        await bot.change_presence(activity=discord.Activity(type=atype, name=text))
        await interaction.response.send_message(
            f"✅ Status set to **{type.capitalize()} {text}**.", ephemeral=True
        )

    # ── Emoji customisation ────────────────────────────────────────────────

    @tree.command(name="setstaremoji", description="Set the star emoji used in embed titles (default: 🌟)")
    @admin_only()
    @app_commands.describe(emoji="Emoji to use as the star (e.g. ✨ 💫 ⭐)")
    async def cmd_setstaremoji(
        interaction: discord.Interaction, emoji: str
    ) -> None:
        await bot.db.set_setting("star_emoji", emoji.strip())
        await interaction.response.send_message(
            f"✅ Star emoji set to **{emoji.strip()}**. Embeds will now use this instead of 🌟/⭐.",
            ephemeral=True,
        )

    @tree.command(name="setarrowemoji", description="Set the arrow/prefix emoji used in embed descriptions (default: >>)")
    @admin_only()
    @app_commands.describe(emoji="Emoji to use as the arrow/prefix (e.g. ➤ ▸ 🔹)")
    async def cmd_setarrowemoji(
        interaction: discord.Interaction, emoji: str
    ) -> None:
        await bot.db.set_setting("arrow_emoji", emoji.strip())
        await interaction.response.send_message(
            f"✅ Arrow emoji set to **{emoji.strip()}**. Embeds will now use this instead of >>.",
            ephemeral=True,
        )

    @tree.command(name="setemojidot", description="Set the bullet/dot emoji used as list pointers in embeds (default: •)")
    @admin_only()
    @app_commands.describe(emoji="Emoji to use as bullet points (e.g. ◆ 🔸 ➜)")
    async def cmd_setemojidot(
        interaction: discord.Interaction, emoji: str
    ) -> None:
        await bot.db.set_setting("dot_emoji", emoji.strip())
        await interaction.response.send_message(
            f"✅ Dot/bullet emoji set to **{emoji.strip()}**. Embeds will now use this instead of •.",
            ephemeral=True,
        )

    # ── Transaction management ─────────────────────────────────────────────

    @tree.command(name="viewtransaction", description="View a transaction's details")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID (8 characters)")
    async def cmd_view(
        interaction: discord.Interaction, transaction_id: str
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await interaction.response.send_message(
            embed=make_embed(
                title=f"📋 Transaction `{tx['transaction_id']}`",
                color=COLOR_INFO,
                fields=[
                    ("Stage",             tx["stage"],                               True),
                    ("Sender",            f"<@{tx.get('sender_id') or 'N/A'}>",     True),
                    ("Receiver",          f"<@{tx.get('receiver_id') or 'N/A'}>",   True),
                    ("USD",               f"${tx.get('amount_usd') or 0:,.2f}",     True),
                    ("LTC",               f"{tx.get('amount_ltc') or 0:.8f}",       True),
                    ("Deposit Confirmed", str(tx.get("deposit_confirmed", False)),   True),
                    ("Frozen",            str(tx.get("frozen", False)),              True),
                    ("Deposit TXID",      tx.get("deposit_txid") or "N/A",          False),
                    ("Withdrawal TXID",   tx.get("withdrawal_txid") or "N/A",       False),
                    ("Deposit Address",   tx.get("deposit_address") or "N/A",       False),
                    ("Created",           str(tx.get("created_at", ""))[:19],       False),
                ],
            ),
            ephemeral=True,
        )

    @tree.command(name="forceconfirm", description="Force-advance a transaction past the current stage")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID")
    async def cmd_forceconfirm(
        interaction: discord.Interaction, transaction_id: str
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        stage = tx["stage"]
        if stage == Stage.ROLE_SELECT:
            await bot.db.update_transaction(transaction_id.upper(), {
                "sender_confirmed": True, "receiver_confirmed": True
            })
        elif stage == Stage.TOS:
            await bot.db.update_transaction(transaction_id.upper(), {
                "sender_tos": "__skip__", "receiver_tos": "__skip__",
            })
            ch = interaction.guild.get_channel(tx["channel_id"])
            if ch:
                await _start_amount_stage(bot, ch, await bot.db.get_transaction(transaction_id.upper()))
        elif stage == Stage.AMOUNT:
            await bot.db.update_transaction(transaction_id.upper(), {
                "sender_confirmed": True, "receiver_confirmed": True
            })
        elif stage in (Stage.DEPOSIT, Stage.AWAITING_FUNDS):
            await bot.db.update_transaction(transaction_id.upper(), {
                "deposit_confirmed": True, "stage": Stage.DELIVERY
            })
            ch = interaction.guild.get_channel(tx["channel_id"])
            if ch:
                await _start_delivery_stage(bot, ch, await bot.db.get_transaction(transaction_id.upper()))
        else:
            await interaction.response.send_message(
                f"❌ Cannot force confirm at stage `{stage}`.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"✅ Force confirmed `{transaction_id.upper()}`.", ephemeral=True
        )
        await bot.db.add_log("admin_force_confirm", {
            "transaction_id": transaction_id.upper(), "admin_id": interaction.user.id
        })

    @tree.command(name="forcerelease", description="Force release funds to the Receiver")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID")
    async def cmd_forcerelease(
        interaction: discord.Interaction, transaction_id: str
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        if tx.get("release_confirmed"):
            await interaction.response.send_message("❌ Already released.", ephemeral=True)
            return
        await bot.db.update_transaction(transaction_id.upper(), {
            "stage": Stage.WITHDRAWAL, "release_confirmed": True,
        })
        ch = interaction.guild.get_channel(tx["channel_id"])
        if ch:
            await _start_withdrawal_stage(bot, ch, tx)
        await interaction.response.send_message(
            f"✅ Funds force-released for `{transaction_id.upper()}`.", ephemeral=True
        )
        await bot.db.add_log("admin_force_release", {
            "transaction_id": transaction_id.upper(), "admin_id": interaction.user.id
        })

    @tree.command(name="forcerefund", description="Force refund (cancel) a trade")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID", reason="Refund reason")
    async def cmd_forcerefund(
        interaction: discord.Interaction,
        transaction_id: str,
        reason: str = "Admin force refund",
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await bot.db.update_transaction(transaction_id.upper(), {
            "stage": Stage.CANCELLED, "cancelled_by": interaction.user.id,
            "cancel_reason": reason, "cancelled_at": datetime.now(timezone.utc),
        })
        ch = interaction.guild.get_channel(tx["channel_id"])
        if ch:
            await ch.send(embed=make_embed(
                "🔄 Trade Force Refunded",
                f"An admin issued a force refund.\n**Reason:** {reason}",
                COLOR_WARNING,
            ))
        await interaction.response.send_message(
            f"✅ Force refunded `{transaction_id.upper()}`.", ephemeral=True
        )
        await bot.db.add_log("admin_force_refund", {
            "transaction_id": transaction_id.upper(),
            "admin_id": interaction.user.id, "reason": reason
        })
        # Send transcript
        refreshed = await bot.db.get_transaction(transaction_id.upper())
        if refreshed and ch:
            await send_deal_transcript(bot, ch, refreshed, outcome="force_refunded")

    @tree.command(name="canceltransaction", description="Cancel an active trade")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID", reason="Cancellation reason")
    async def cmd_cancel(
        interaction: discord.Interaction,
        transaction_id: str,
        reason: str = "Admin cancelled",
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await bot.db.update_transaction(transaction_id.upper(), {
            "stage": Stage.CANCELLED, "cancelled_by": interaction.user.id,
            "cancel_reason": reason, "cancelled_at": datetime.now(timezone.utc),
        })
        ch = interaction.guild.get_channel(tx["channel_id"])
        if ch:
            await ch.send(embed=make_embed(
                "❌ Trade Cancelled by Admin", f"**Reason:** {reason}", COLOR_DANGER,
            ))
        await interaction.response.send_message(
            f"✅ Cancelled `{transaction_id.upper()}`.", ephemeral=True
        )
        # Send transcript
        refreshed2 = await bot.db.get_transaction(transaction_id.upper())
        if refreshed2 and ch:
            await send_deal_transcript(bot, ch, refreshed2, outcome="cancelled")

    @tree.command(name="freezetransaction", description="Freeze an active trade")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID")
    async def cmd_freeze(
        interaction: discord.Interaction, transaction_id: str
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await bot.db.update_transaction(transaction_id.upper(), {"frozen": True})
        ch = interaction.guild.get_channel(tx["channel_id"])
        if ch:
            await ch.send(embed=make_embed(
                "🧊 Trade Frozen",
                "This trade has been frozen by an admin. No actions may proceed until it is unfrozen.",
                COLOR_WARNING,
            ))
        await interaction.response.send_message(
            f"✅ Frozen `{transaction_id.upper()}`.", ephemeral=True
        )

    @tree.command(name="unfreezetransaction", description="Unfreeze a frozen trade")
    @admin_only()
    @app_commands.describe(transaction_id="Transaction ID")
    async def cmd_unfreeze(
        interaction: discord.Interaction, transaction_id: str
    ) -> None:
        tx = await bot.db.get_transaction(transaction_id.upper())
        if tx is None:
            await interaction.response.send_message("❌ Not found.", ephemeral=True)
            return
        await bot.db.update_transaction(transaction_id.upper(), {"frozen": False})
        ch = interaction.guild.get_channel(tx["channel_id"])
        if ch:
            await ch.send(embed=make_embed(
                "✅ Trade Unfrozen", "This trade may now continue.", COLOR_SUCCESS,
            ))
        await interaction.response.send_message(
            f"✅ Unfrozen `{transaction_id.upper()}`.", ephemeral=True
        )

    # ── Blacklist ──────────────────────────────────────────────────────────

    @tree.command(name="blacklist", description="Blacklist a user from AutoMM")
    @admin_only()
    @app_commands.describe(user="User to blacklist", reason="Reason")
    async def cmd_blacklist(
        interaction: discord.Interaction,
        user: discord.Member,
        reason: str = "No reason provided",
    ) -> None:
        await bot.db.blacklist_user(user.id, reason, interaction.user.id)
        await interaction.response.send_message(
            f"✅ {user.mention} blacklisted.\n**Reason:** {reason}", ephemeral=True
        )
        await bot.db.add_log("user_blacklisted", {
            "user_id": user.id, "reason": reason, "admin_id": interaction.user.id
        })

    @tree.command(name="unblacklist", description="Remove a user from the blacklist")
    @admin_only()
    @app_commands.describe(user="User to unblacklist")
    async def cmd_unblacklist(
        interaction: discord.Interaction, user: discord.Member
    ) -> None:
        await bot.db.unblacklist_user(user.id)
        await interaction.response.send_message(
            f"✅ {user.mention} removed from blacklist.", ephemeral=True
        )

    # ── Statistics ─────────────────────────────────────────────────────────

    @tree.command(name="stats", description="View global AutoMM statistics")
    @admin_only()
    async def cmd_stats(interaction: discord.Interaction) -> None:
        s = await bot.db.get_global_stats()
        await interaction.response.send_message(
            embed=make_embed(
                title="📊 AutoMM Statistics",
                color=COLOR_INFO,
                fields=[
                    ("Total Trades",     str(s["total"]),                  True),
                    ("Completed",        str(s["completed"]),               True),
                    ("Cancelled",        str(s["cancelled"]),               True),
                    ("Disputed",         str(s["disputed"]),                True),
                    ("Total USD Volume", f"${s['total_usd']:,.2f}",         True),
                    ("Total LTC Volume", f"{s['total_ltc']:.4f} LTC",       True),
                ],
            ),
            ephemeral=True,
        )

    # ── User profile ───────────────────────────────────────────────────────

    @tree.command(name="profile", description="View a user's trading profile")
    @app_commands.describe(user="User to view (default: yourself)")
    async def cmd_profile(
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ) -> None:
        target = user or interaction.user
        u      = await bot.db.get_user(target.id)
        fbs    = await bot.db.get_user_feedback(target.id)
        avg    = u.get("average_rating", 0)
        star_e = "⭐"
        recent = ""
        for fb in fbs[:3]:
            stars   = star_e * fb["rating"]
            comment = fb.get("comment") or "_No comment_"
            recent += f"{stars} — {comment}\n"
        await interaction.response.send_message(
            embed=make_embed(
                title=f"👤 {target.display_name}",
                color=COLOR_INFO,
                fields=[
                    ("Completed Deals",   str(u.get("completed_deals", 0)),          True),
                    ("Total Volume USD",  f"${u.get('total_volume_usd', 0):,.2f}",   True),
                    ("Total Volume LTC",  f"{u.get('total_volume_ltc', 0):.4f} LTC", True),
                    ("Average Rating",    f"{star_e * round(avg)} ({avg}/5)",         True),
                    ("Feedback Count",    str(u.get("feedback_count", 0)),            True),
                    ("Recent Feedback",   recent or "_No feedback yet_",              False),
                ],
            ),
            ephemeral=True,
        )

    # ── Clear old transaction history ──────────────────────────────────────

    @tree.command(
        name="clearhistory",
        description="Delete transaction records (and their feedback/logs) that are 7+ days old",
    )
    @admin_only()
    async def cmd_clearhistory(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)

        # Only purge finished trades — never touch active ones
        finished_stages = [Stage.COMPLETED, Stage.CANCELLED, Stage.DISPUTED]
        cursor = bot.db.transactions.find({
            "stage":      {"$in": finished_stages},
            "created_at": {"$lt": cutoff},
        })
        old_txs: list[dict] = await cursor.to_list(length=None)

        if not old_txs:
            await interaction.followup.send(
                "✅ No transaction records older than 7 days found.", ephemeral=True
            )
            return

        old_ids = [tx["transaction_id"] for tx in old_txs]

        # Delete transactions, feedback entries, and logs tied to those IDs
        tx_result = await bot.db.transactions.delete_many({"transaction_id": {"$in": old_ids}})
        fb_result = await bot.db.feedback.delete_many({"transaction_id": {"$in": old_ids}})
        log_result = await bot.db.logs.delete_many({"data.transaction_id": {"$in": old_ids}})

        await interaction.followup.send(
            embed=make_embed(
                "🗑️ History Cleared",
                f"Removed records older than **7 days** (finished trades only).",
                COLOR_SUCCESS,
                fields=[
                    ("Transactions Deleted", str(tx_result.deleted_count), True),
                    ("Feedback Entries Deleted", str(fb_result.deleted_count), True),
                    ("Log Entries Deleted", str(log_result.deleted_count), True),
                ],
            ),
            ephemeral=True,
        )
        await bot.db.add_log("admin_clearhistory", {
            "admin_id":    interaction.user.id,
            "deleted_ids": old_ids,
            "cutoff":      cutoff.isoformat(),
        })

    # ── Delete finished MM channels ────────────────────────────────────────

    @tree.command(
        name="deletemm",
        description="Delete ticket channels for all completed or cancelled MMs (skips active trades)",
    )
    @admin_only()
    async def cmd_deletemm(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        finished_stages = (Stage.COMPLETED, Stage.CANCELLED)
        cursor = bot.db.transactions.find({"stage": {"$in": list(finished_stages)}})
        finished: list[dict] = await cursor.to_list(length=None)

        if not finished:
            await interaction.followup.send(
                "✅ No completed/cancelled MM channels to delete.", ephemeral=True
            )
            return

        deleted = 0
        skipped = 0
        current_channel_tx = None  # track if the command channel is one to delete

        for tx in finished:
            ch_id = tx.get("channel_id")
            if not ch_id:
                skipped += 1
                continue
            ch = interaction.guild.get_channel(int(ch_id))
            if ch is None:
                skipped += 1
                continue
            # If this is the channel the command was run from, defer its
            # deletion until AFTER we send the followup response — otherwise
            # Discord returns 404 Unknown Message for the ephemeral reply.
            if ch.id == interaction.channel_id:
                current_channel_tx = tx
                continue
            try:
                await ch.delete(reason=f"AutoMM /deletemm — trade {tx['transaction_id']} ({tx['stage']})")
                deleted += 1
            except discord.Forbidden:
                log.warning("No permission to delete channel %s for trade %s", ch_id, tx["transaction_id"])
                skipped += 1
            except discord.HTTPException as exc:
                log.warning("Failed to delete channel %s: %s", ch_id, exc)
                skipped += 1

        # Count the deferred channel as deleted (it will be removed momentarily)
        if current_channel_tx:
            deleted += 1

        try:
            await interaction.followup.send(
                embed=make_embed(
                    "🗑️ MM Channels Deleted",
                    f"Finished processing **{len(finished)}** completed/cancelled trade(s).",
                    COLOR_SUCCESS,
                    fields=[
                        ("Deleted", str(deleted), True),
                        ("Already Gone / Skipped", str(skipped), True),
                    ],
                ),
                ephemeral=True,
            )
        except Exception as exc:
            log.warning("Could not send deletemm followup: %s", exc)

        await bot.db.add_log("admin_deletemm", {
            "admin_id": interaction.user.id,
            "deleted": deleted,
            "skipped": skipped,
        })

        # Now safely delete the command channel (after the reply was sent)
        if current_channel_tx:
            await asyncio.sleep(1)
            try:
                ch = interaction.guild.get_channel(int(current_channel_tx["channel_id"]))
                if ch:
                    await ch.delete(
                        reason=f"AutoMM /deletemm — trade {current_channel_tx['transaction_id']} ({current_channel_tx['stage']})"
                    )
            except discord.HTTPException as exc:
                log.warning("Failed to delete command channel after reply: %s", exc)

    # ── Sweep account funds ────────────────────────────────────────────────

    @tree.command(
        name="sweepfunds",
        description="Send all LTC in the Apirone account to a destination address (admin only)",
    )
    @admin_only()
    @app_commands.describe(address="Destination LTC address (required)")
    async def cmd_sweepfunds(
        interaction: discord.Interaction,
        address: str,
    ) -> None:
        dest = address.strip()
        if not dest:
            await interaction.response.send_message(
                "❌ You must provide a destination LTC address.", ephemeral=True
            )
            return
        if not AprionClient.validate_ltc_address(dest):
            await interaction.response.send_message(
                "❌ Invalid LTC destination address.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        # Fetch balance so we can report it and pass exact satoshis to the transfer
        balance_ltc: Optional[float] = None
        balance_sat: Optional[int] = None
        raw_info: dict = {}
        try:
            balance_ltc, raw_info = await bot.aprion.get_account_balance_ltc()
            balance_sat = int(round(balance_ltc * 1e8)) if balance_ltc else None
        except Exception as exc:
            log.warning("Could not fetch balance before sweep: %s", exc)

        # Warn but don't block — let Apirone decide if there's anything to send
        if balance_ltc is not None and balance_ltc <= 0:
            await interaction.followup.send(
                embed=make_embed(
                    "⚠️ Balance Appears Zero",
                    (
                        "The account balance reads **0 LTC**. The sweep will still be attempted "
                        "in case the balance field is mis-parsed.\n\n"
                        f"**Raw API response:**\n```json\n{str(raw_info)[:800]}\n```"
                    ),
                    COLOR_WARNING,
                ),
                ephemeral=True,
            )

        try:
            result = await bot.aprion.sweep_all(dest, balance_sat=balance_sat)
        except Exception as exc:
            log.error("sweepfunds failed: %s", exc)
            await interaction.followup.send(
                embed=make_embed(
                    "❌ Sweep Failed",
                    (
                        f"Error: `{exc}`\n\n"
                        f"**Raw account info:**\n```json\n{str(raw_info)[:600]}\n```"
                    ),
                    COLOR_DANGER,
                ),
                ephemeral=True,
            )
            return

        txid = (
            result.get("txid")
            or result.get("id")
            or result.get("tx_hash")
            or str(result)[:100]
        )

        bal_str = f"{balance_ltc:.8f} LTC" if balance_ltc else "_unknown_"
        await interaction.followup.send(
            embed=make_embed(
                "💸 Funds Swept",
                f"All available funds have been sent to `{dest}`.",
                COLOR_SUCCESS,
                fields=[
                    ("Amount Swept", bal_str, True),
                    ("TXID", f"`{txid}`", False),
                    ("Raw Result", f"```json\n{str(result)[:300]}\n```", False),
                ],
            ),
            ephemeral=True,
        )
        await bot.db.add_log("admin_sweep_funds", {
            "admin_id": interaction.user.id,
            "destination": dest,
            "balance_ltc": balance_ltc,
            "txid": txid,
        })
        await bot.post_log_embed(interaction.guild, make_embed(
            title="💸 Funds Swept by Admin",
            color=COLOR_WARNING,
            fields=[
                ("Admin",       interaction.user.mention, True),
                ("Destination", f"`{dest}`",              True),
                ("Amount",      bal_str,                  True),
                ("TXID",        f"`{txid}`",              False),
            ],
        ))

    # ── Check Apirone account balance ──────────────────────────────────────

    @tree.command(
        name="checkfunds",
        description="Show current LTC balance in the Apirone account (admin only)",
    )
    @admin_only()
    async def cmd_checkfunds(interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            balance_ltc, raw_info = await bot.aprion.get_account_balance_ltc()
        except Exception as exc:
            log.error("checkfunds failed: %s", exc)
            await interaction.followup.send(
                embed=make_embed(
                    "❌ Balance Check Failed",
                    f"`{exc}`",
                    COLOR_DANGER,
                ),
                ephemeral=True,
            )
            return

        # Try to get live LTC price for USD equivalent
        usd_val = ""
        try:
            price = await bot.aprion.get_ltc_price_usd()
            usd_val = f"≈ **${balance_ltc * price:,.2f} USD** (@ ${price:,.2f}/LTC)"
        except Exception:
            usd_val = "_(price unavailable)_"

        balance_sat = int(round(balance_ltc * 1e8))

        await interaction.followup.send(
            embed=make_embed(
                "💰 Apirone Account Balance",
                usd_val,
                COLOR_SUCCESS if balance_ltc > 0 else COLOR_WARNING,
                fields=[
                    ("LTC Balance",  f"**{balance_ltc:.8f} LTC**", True),
                    ("Satoshis",     f"{balance_sat:,} sat",        True),
                    ("Account",      f"`{bot.aprion.account}`",     False),
                    ("Raw API Info", f"```json\n{str(raw_info)[:500]}\n```", False),
                ],
            ),
            ephemeral=True,
        )
        await bot.db.add_log("admin_checkfunds", {
            "admin_id":    interaction.user.id,
            "balance_ltc": balance_ltc,
            "balance_sat": balance_sat,
        })

    # ── Help system ────────────────────────────────────────────────────────

    def _help_embed_overview() -> discord.Embed:
        return make_embed(
            title="🏠 AutoMM — Overview",
            description=(
                "**AutoMM** is a fully automated **Litecoin middleman** service.\n"
                "It securely holds funds while both parties complete a deal — "
                "no trust required between Sender and Receiver.\n\n"
                "**Minimum trade amount:** $0.10 USD\n"
                "**Supported currency:** LTC (Litecoin) only\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**Quick Start**\n"
                "1️⃣  Find the AutoMM panel in your server\n"
                "2️⃣  Click **Start New Deal** and enter the other user's **Discord ID**\n"
                "3️⃣  Both users confirm their roles and agree on an amount\n"
                "4️⃣  Sender deposits LTC → bot confirms automatically\n"
                "5️⃣  Receiver delivers — Sender releases funds\n"
                "6️⃣  Both leave feedback — transcript sent to your DMs\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**Safety Rules**\n"
                "⚠️  The bot will **never DM you first** — report suspicious DMs to staff\n"
                "⚠️  Keep all deal chat **inside the ticket**\n"
                "⚠️  Assign Sender/Receiver roles **carefully** before depositing\n\n"
                "Use the dropdown below to explore other sections."
            ),
            color=COLOR_PRIMARY,
        )

    def _help_embed_flow() -> discord.Embed:
        return make_embed(
            title="🔄 How a Trade Works",
            description=(
                "A full trade goes through these stages automatically:\n\n"
                "**Stage 1 — Open Ticket**\n"
                "Click **Start New Deal** on the panel and enter the other user's Discord ID. "
                "A private ticket channel is created for both of you.\n\n"
                "**Stage 2 — Role Selection**\n"
                "Both users choose their role:\n"
                "• **Sender** — pays the LTC into escrow\n"
                "• **Receiver** — delivers the product/service\n\n"
                "**Stage 3 — Terms of Service**\n"
                "Both users must accept the AutoMM ToS before proceeding.\n\n"
                "**Stage 4 — Amount Agreement**\n"
                "Both users agree on a USD amount (minimum $0.10). "
                "The bot converts it to the exact LTC equivalent in real time.\n\n"
                "**Stage 5 — Deposit**\n"
                "The Sender receives a unique LTC deposit address. "
                "The bot monitors the blockchain and auto-confirms when funds arrive.\n\n"
                "**Stage 6 — Delivery**\n"
                "The Receiver delivers their product or service. "
                "Both parties discuss inside the ticket.\n\n"
                "**Stage 7 — Release or Refund**\n"
                "• Sender clicks **Release Funds** → Receiver gets paid ✅\n"
                "• Sender clicks **Request Refund** → Admin returns LTC 🔄\n"
                "• Either party can **Open a Dispute** for admin review ⚖️\n\n"
                "**Stage 8 — Feedback**\n"
                "Both users rate each other. A full **HTML transcript** is sent to your DMs "
                "and the log channel automatically.\n\n"
                "**Stage 9 — Close**\n"
                "The ticket channel can be closed and deleted via the final buttons."
            ),
            color=COLOR_INFO,
        )

    def _help_embed_profile() -> discord.Embed:
        return make_embed(
            title="👤 Profile & Ratings",
            description=(
                "**`/profile [@user]`**\n"
                "View any user's trading profile — or your own if no user is mentioned.\n\n"
                "**What the profile shows:**\n"
                "• Total trades completed\n"
                "• Average star rating (1–5 ⭐)\n"
                "• Total volume traded (LTC & USD)\n"
                "• Feedback count\n"
                "• Most recent feedback comments\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**Rating System**\n"
                "After every completed trade, both parties rate each other on a scale of "
                "**1 to 5 stars**. The average rating is displayed on the profile.\n\n"
                "Feedback is submitted via the interactive buttons inside the ticket — "
                "once both parties submit, the ticket finalises and transcripts are sent.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**`/stats`**\n"
                "View global AutoMM statistics — total trades, volume, active transactions, "
                "and top traders. _(Admin only)_\n\n"
                "**`/help`**\n"
                "Opens this help menu."
            ),
            color=COLOR_SUCCESS,
        )

    def _help_embed_setup() -> discord.Embed:
        return make_embed(
            title="⚙️ Server Setup",
            description=(
                "Configure the bot for your server. All commands are **admin-only**.\n\n"
                "**`/panel`**\n"
                "Post the AutoMM panel embed with the **Start New Deal** button to the current channel.\n\n"
                "**`/setmmrole [@role]`**\n"
                "Restrict who can open trades to a specific role. Omit the role to allow everyone.\n\n"
                "**`/setlogchannel [#channel]`**\n"
                "Set the channel where all trade logs and transcripts are posted.\n\n"
                "**`/setticketcategory [category]`**\n"
                "Set the Discord category where trade ticket channels are created.\n\n"
                "**`/setcompletedchannel [#channel]`**\n"
                "Set the channel where completed trade announcements are posted.\n\n"
                "**`/setcompletedmessage [text]`**\n"
                "Set a custom message for completed trade announcements. "
                "Supports placeholders: `{sender}`, `{receiver}`, `{amount}`, `{id}`.\n\n"
                "**`/testcompletedmessage`**\n"
                "Send a test completion announcement to verify your setup.\n\n"
                "**`/setfeedbackchannel [#channel]`**\n"
                "Set the channel where feedback embeds are posted after trades.\n\n"
                "**`/testfeedback`**\n"
                "Send a test feedback embed to verify the feedback channel.\n\n"
                "**`/testembed`**\n"
                "Preview every embed in the full AutoMM trade flow."
            ),
            color=COLOR_WARNING,
        )

    def _help_embed_admin() -> discord.Embed:
        return make_embed(
            title="🛡️ Trade Management",
            description=(
                "Commands to manage active and past trades. All are **admin-only**.\n\n"
                "**`/viewtransaction [id]`**\n"
                "View full details of any transaction by its ID — stage, participants, amounts, timestamps.\n\n"
                "**`/forceconfirm [id]`**\n"
                "Force-advance a stuck transaction past its current stage. "
                "Use when a user cannot interact with the bot.\n\n"
                "**`/forcerelease [id]`**\n"
                "Force-release funds to the Receiver, bypassing the Sender's confirmation. "
                "Use only after manually verifying delivery.\n\n"
                "**`/forcerefund [id] [reason]`**\n"
                "Force-cancel and refund a trade. Marks it cancelled and sends a transcript immediately.\n\n"
                "**`/canceltransaction [id] [reason]`**\n"
                "Cancel and close a trade. Sends transcript to participants.\n\n"
                "**`/freezetransaction [id]`**\n"
                "Freeze a trade — prevents any buttons from being interacted with. "
                "Useful while investigating a dispute.\n\n"
                "**`/unfreezetransaction [id]`**\n"
                "Unfreeze a previously frozen trade.\n\n"
                "**`/blacklist [@user]`**\n"
                "Blacklist a user from opening AutoMM trades.\n\n"
                "**`/unblacklist [@user]`**\n"
                "Remove a user from the blacklist.\n\n"
                "**`/stats`**\n"
                "View global AutoMM statistics — total trades, volume, and active transactions."
            ),
            color=COLOR_DANGER,
        )

    def _help_embed_customization() -> discord.Embed:
        return make_embed(
            title="🎨 Customization",
            description=(
                "Personalise the look and feel of AutoMM. All are **admin-only**.\n\n"
                "**`/setmmname [name]`**\n"
                "Set the middleman service display name shown on embeds and announcements "
                "_(e.g. `Frost Auto Middleman`)_.\n\n"
                "**`/setstatus [type] [text]`**\n"
                "Set the bot's Discord activity status.\n"
                "Types: `playing`, `watching`, `listening`, `competing`.\n\n"
                "**`/setstaremoji [emoji]`**\n"
                "Set the star emoji used in embed titles. Default: 🌟\n\n"
                "**`/setarrowemoji [emoji]`**\n"
                "Set the arrow/prefix emoji used in embed descriptions. Default: `>>`\n\n"
                "**`/setemojidot [emoji]`**\n"
                "Set the bullet/dot emoji used as list pointers in embeds. Default: `•`\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**Tips**\n"
                "• Custom emojis from your server work — use the full `<:name:id>` format\n"
                "• Use `/testembed` after changes to preview the full flow\n"
                "• Use `/testcompletedmessage` to verify announcement formatting"
            ),
            color=COLOR_PRIMARY,
        )

    def _help_embed_funds() -> discord.Embed:
        return make_embed(
            title="💰 Funds & Finance",
            description=(
                "Commands for managing the Apirone LTC account. All are **admin-only**.\n\n"
                "**`/checkfunds`**\n"
                "Show the current LTC balance in the Apirone escrow account, "
                "including the live USD equivalent and raw API response.\n\n"
                "**`/sweepfunds [ltc_address]`**\n"
                "Send **all available LTC** in the Apirone account to a specified address.\n"
                "⚠️ This action is **irreversible** — double-check the address before confirming.\n"
                "The TXID is logged for your records.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**How escrow works**\n"
                "Every deposit goes to a unique address generated by Apirone. "
                "Funds sit in the Apirone account until released — "
                "the bot calls the Apirone transfer API to pay out the Receiver.\n\n"
                "**Important:** Always keep a small buffer for network fees. "
                "The bot accounts for fees automatically during payouts."
            ),
            color=COLOR_SUCCESS,
        )

    def _help_embed_maintenance() -> discord.Embed:
        return make_embed(
            title="🗑️ Maintenance",
            description=(
                "Housekeeping commands for keeping the server clean. All are **admin-only**.\n\n"
                "**`/deletemm`**\n"
                "Delete all ticket channels for **completed or cancelled** trades. "
                "Active trades are never touched.\n"
                "Useful for bulk-cleaning old MM channels after a period of activity.\n\n"
                "**`/clearhistory`**\n"
                "Delete transaction records (plus their feedback and log entries) "
                "that are **7 or more days old** and in a finished state "
                "(completed, cancelled, or disputed).\n"
                "Active trades are never affected.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "**When to use each**\n"
                "• Run `/deletemm` periodically to keep your channel list tidy\n"
                "• Run `/clearhistory` to prune old database records and keep things fast\n"
                "• Always run `/viewtransaction` on a trade before deleting its channel "
                "if you need the record"
            ),
            color=COLOR_WARNING,
        )

    _HELP_PAGE_BUILDERS = {
        "overview":    _help_embed_overview,
        "flow":        _help_embed_flow,
        "profile":     _help_embed_profile,
        "setup":       _help_embed_setup,
        "admin":       _help_embed_admin,
        "customization": _help_embed_customization,
        "funds":       _help_embed_funds,
        "maintenance": _help_embed_maintenance,
    }

    class HelpSelect(discord.ui.Select):
        def __init__(self, show_admin: bool) -> None:
            options = [
                discord.SelectOption(
                    label="🏠 Overview",
                    value="overview",
                    description="What is AutoMM? Safety rules & quick start",
                ),
                discord.SelectOption(
                    label="🔄 How a Trade Works",
                    value="flow",
                    description="Step-by-step guide through every trade stage",
                ),
                discord.SelectOption(
                    label="👤 Profile & Ratings",
                    value="profile",
                    description="/profile, star ratings, feedback system",
                ),
            ]
            if show_admin:
                options += [
                    discord.SelectOption(
                        label="⚙️ Server Setup",
                        value="setup",
                        description="Channels, roles, categories, announcements",
                    ),
                    discord.SelectOption(
                        label="🛡️ Trade Management",
                        value="admin",
                        description="Force actions, freeze, blacklist, view trades",
                    ),
                    discord.SelectOption(
                        label="🎨 Customization",
                        value="customization",
                        description="Bot name, emojis, status, messages",
                    ),
                    discord.SelectOption(
                        label="💰 Funds & Finance",
                        value="funds",
                        description="/checkfunds, /sweepfunds, Apirone escrow",
                    ),
                    discord.SelectOption(
                        label="🗑️ Maintenance",
                        value="maintenance",
                        description="/deletemm, /clearhistory — clean up old data",
                    ),
                ]
            super().__init__(
                placeholder="📖 Select a help section…",
                min_values=1,
                max_values=1,
                options=options,
            )

        async def callback(self, interaction: discord.Interaction) -> None:
            page = self.values[0]
            builder = _HELP_PAGE_BUILDERS.get(page)
            if builder is None:
                await interaction.response.defer()
                return
            embed = builder()
            await interaction.response.edit_message(embed=embed)

    class HelpView(discord.ui.View):
        def __init__(self, show_admin: bool) -> None:
            super().__init__(timeout=180)
            self.add_item(HelpSelect(show_admin))

    @tree.command(name="help", description="Show AutoMM commands, setup guides, and usage")
    async def cmd_help(interaction: discord.Interaction) -> None:
        show_admin = is_admin(interaction.user)
        embed = _help_embed_overview()
        await interaction.response.send_message(
            embed=embed,
            view=HelpView(show_admin),
            ephemeral=True,
        )


# ---------------------------------------------------------------------------
# Global error handler for slash commands
# ---------------------------------------------------------------------------

async def _on_app_command_error(
    interaction: discord.Interaction, error: app_commands.AppCommandError
) -> None:
    if isinstance(error, app_commands.CheckFailure):
        pass  # already handled in the predicate
    else:
        log.error("App command error: %s", error, exc_info=True)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An unexpected error occurred.", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ An unexpected error occurred.", ephemeral=True
                )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    bot = AutoMMBot()
    register_commands(bot)
    bot.tree.on_error = _on_app_command_error
    log.info("Starting AutoMM bot…")
    bot.run(DISCORD_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
