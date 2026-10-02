#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Threads keyword scraper — portfolio version.

A parallel Playwright scraper that searches Threads for public posts
matching a configurable keyword list, extracts the OP body and replies,
classifies each post through a gated scoring pipeline, and exports a
deduplicated Excel workbook of Hot/Warm/General leads.

Originally written during an internship. Client-specific keywords,
deliverable branding, and lead data have been removed.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse, urlunparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
import pandas as pd
import math
import threading
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

def _build_final_deliverable(
    output_folder: Path,
    lead_cap: int = 99999999,
    recency_days: int = 90,
) -> None:
    """Build the outreach-ready Excel with dedupe + cap baked in.

    Pipeline:
      1. Load classified CSV + reply leads.
      2. Filter to Hot/Warm from last `recency_days`.
      3. Drop meta.ai bot rows.
      4. Join reply context.
      5. Deduplicate by parent post fingerprint (removes thread-view dupes).
      6. Cap Top 100 sheet AFTER dedupe; keep Hot/Warm sheets uncapped.
    """
    import re as _re
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    classified_path = output_folder / "classified_posts.csv"
    reply_leads_path = output_folder / "reply_leads.csv"

    if not classified_path.exists():
        print("[FINAL] No classified posts found; skipping.")
        return

    classified = pd.read_csv(classified_path).fillna("")
    reply_leads = (
        pd.read_csv(reply_leads_path).fillna("")
        if reply_leads_path.exists()
        else pd.DataFrame()
    )

    # -------- Timestamp parsing ------------------------------------
    def _parse_ts(value):
        s = str(value or "").strip()
        if not s:
            return None
        try:
            dt = _dt.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_tz.utc)
            return dt
        except Exception:
            pass

        m = _re.match(
            r"^(\d{1,2})/(\d{1,2})/(\d{4})"
            r"(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$",
            s,
        )
        if m:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            hh = int(m.group(4)) if m.group(4) else 0
            mm = int(m.group(5)) if m.group(5) else 0
            ss = int(m.group(6)) if m.group(6) else 0
            try:
                return (
                    _dt(y, mo, d, hh, mm, ss, tzinfo=_tz.utc)
                    - _td(hours=7)
                )
            except ValueError:
                return None
        return None

    def _age_days(value):
        dt = _parse_ts(value)
        if dt is None:
            return None
        return (_dt.now(_tz.utc) - dt).days

    classified["_age_days"] = classified["timestamp"].apply(_age_days)
    classified["_is_recent"] = classified["_age_days"].apply(
        lambda d: d is not None and d <= recency_days
    )

    hw = classified[
        classified["lead_category"].isin(
            ["Hot research lead", "Warm research lead"]
        )
    ].copy()
    recent = hw[hw["_is_recent"]].copy()

    # Drop bot.
    recent = recent[
        ~recent["username"].str.lower().isin(["meta.ai", "meta_ai"])
    ].copy()

    # -------- Build 'Why (A/B/C/D)' bullets -------------------------
    def _bullets(row):
        parts = [
            p.strip()
            for p in str(row.get("positive_signals", "") or "").split("|")
            if p.strip()
        ]
        parts = [p for p in parts if not p.startswith("explicit KPR context")]
        return "\n".join(f"- {p[:80]}" for p in parts[:4])

    recent["why_bullets"] = recent.apply(_bullets, axis=1)

    # -------- Join reply context -----------------------------------
    if "relevant_reply_text" in recent.columns:
        recent["top_reply_context"] = (
            recent["relevant_reply_text"].fillna("").astype(str)
        )
    else:
        recent["top_reply_context"] = ""

    if not reply_leads.empty and "parent_post_link" in reply_leads.columns:
        reply_ctx = (
            reply_leads.groupby("parent_post_link")["reply_body"]
            .apply(lambda s: " || ".join(s.astype(str).head(3)))
            .reset_index()
            .rename(columns={"reply_body": "reply_lead_context"})
        )
        recent = recent.merge(
            reply_ctx,
            left_on="post_link",
            right_on="parent_post_link",
            how="left",
        )
        recent["reply_lead_context"] = recent["reply_lead_context"].fillna("")
        recent["top_reply_context"] = recent.apply(
            lambda r: r["top_reply_context"] or r["reply_lead_context"],
            axis=1,
        )
        recent = recent.drop(columns=["reply_lead_context", "parent_post_link"])

    # -------- Deduplicate by parent post fingerprint ----------------
    TAYANGAN_RE = _re.compile(
        r"^[\d,\.]+\s*(?:rb|ribu|jt|juta|k|m)?\s*(?:tayangan|views)\s*",
        _re.IGNORECASE,
    )
    USERNAME_LINE = _re.compile(r"^@?[A-Za-z0-9_.]{3,30}$")

    def _parent_key(text, chars=200):
        if not isinstance(text, str):
            return ""
        body = TAYANGAN_RE.sub("", text)
        body = _re.sub(r"<br\s*/?>", "\n", body)
        lines = [l.strip() for l in body.split("\n") if l.strip()]
        if not lines:
            return ""
        idx = 0
        if USERNAME_LINE.match(lines[0]):
            idx = 1
        parts = []
        for line in lines[idx:]:
            if USERNAME_LINE.match(line) and parts:
                break
            parts.append(line)
            if sum(len(p) for p in parts) > chars:
                break
        content = " ".join(parts)
        content = _re.sub(r"\s+", " ", content).strip()
        return content[:chars].lower()

    before_dedup = len(recent)
    recent["_parent_key"] = recent["post_body"].apply(_parent_key)
    recent["_score_num"] = pd.to_numeric(
        recent["lead_score"], errors="coerce"
    ).fillna(0)
    recent["_age_num"] = pd.to_numeric(
        recent["_age_days"], errors="coerce"
    ).fillna(9999)
    recent = recent.sort_values(
        ["_score_num", "_age_num"], ascending=[False, True]
    )
    recent = recent.drop_duplicates(subset=["_parent_key"], keep="first")
    after_dedup = len(recent)
    print(f"[FINAL] Dedup: {before_dedup} -> {after_dedup} unique posts")

    # -------- Split + cap ------------------------------------------
    hot_df = recent[recent["lead_category"] == "Hot research lead"].copy()
    warm_df = recent[recent["lead_category"] == "Warm research lead"].copy()
    top_df = recent.copy()  # no cap — include all Hot/Warm leads

    # -------- Column mapping ---------------------------------------
    col_map = {
        "username": "Username",
        "lead_category": "Category",
        "lead_type": "Lead Type",
        "lead_confidence": "Confidence",
        "lead_score": "Score",
        "post_body": "Post Body",
        "relevant_reply_count": "On-topic Replies",
        "reply_count": "Replies from Others",
        "total_thread_entries": "Total Thread Entries",
        "ui_reply_count": "UI Reply Count",
        "op_engaged": "OP Replied Back",
        "top_reply_context": "Conversation (top replies)",
        "matched_keywords": "Discovery Query",
        "timestamp": "Posted",
        "_age_days": "days_ago",
        "why_bullets": "Why (A / B / C / D)",
        "post_link": "Link",
    }

    def _project(df):
        cols = [c for c in col_map if c in df.columns]
        return df[cols].rename(columns=col_map)

    out = output_folder / "final_leads.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        _project(top_df).to_excel(
            writer, sheet_name="All Leads", index=False
        )
        _project(hot_df).to_excel(writer, sheet_name="Hot", index=False)
        _project(warm_df).to_excel(writer, sheet_name="Warm", index=False)

    print(f"[FINAL] Wrote {len(top_df)} leads to {out}")
    print(f"[FINAL]   All Leads: {len(top_df)}")
    print(f"[FINAL]   Hot:       {len(hot_df)}")
    print(f"[FINAL]   Warm:      {len(warm_df)}")
"""

changes in the code
multiple workers instead of one.(when adding a new worker must make SETUP_WORKER_PROFILES = True and login manaully into threads only once per new worker)
if every worker login is save then SETUP_WORKER_PROFILES  can be = False.

kept the orginal search keywords in comments, to have a shorter list to get more variety for the posts
to use it agian just remove top triple " and the bottom triple " and the remove the shorter search keyword list

the process no longer keeps the data of previous runs once asking for resume and replying n
Changed from linear processing to parallel processing
 - Multiple worker processes can now scrape different keyword subsets simultaneously.
 - Each worker has its own persistent Threads browser profile
 - Added keyword distribution
 - Added global shared state between workers
 - Fixed the cross-worker race condition
 - Improved URL deduplication
 - Added content-level duplicate detection
 - Made the unique-post target global
 - Added a per-keyword maximum
 - Improved post extraction
 - Added stable-body extraction
 - Added extraction validation
 - Added keyword matching/merging
 - Added keyword-progress persistence
 - Added resume functionality
 - Added the stop command(stop request rather using control C)
 
"""
# ============================================================
# 1. PROJECT SETTINGS
# ============================================================

PROJECT_FOLDER = Path(__file__).resolve().parent
DEFAULT_OUTPUT_FOLDER = PROJECT_FOLDER / "output_leads"
PROFILE_FOLDER = PROJECT_FOLDER / "threads_browser_profile"
STATE_FOLDER = PROJECT_FOLDER / "state"
DEBUG_FOLDER = PROJECT_FOLDER / "debug"
CHECKPOINT_FOLDER = STATE_FOLDER / "worker_checkpoints"
MAX_RELEVANT_REPLIES = 3
MAX_CONVERSATION_BONUS = 2
REPLY_RELEVANCE_BONUS = 1

PROCESSED_POSTS_FILE = STATE_FOLDER / "processed_posts.csv"
KEYWORD_PROGRESS_FILE = STATE_FOLDER / "keyword_progress.csv"

MAX_RELEVANT_REPLIES = 8
MAX_CONVERSATION_BONUS = 2
REPLY_RELEVANCE_BONUS = 1

# NEW — productive replies get a bigger bonus because they actually
# advance the conversation (advice, numbers, personal experience).
MAX_PRODUCTIVE_REPLIES = 2
PRODUCTIVE_REPLY_BONUS = 2
MAX_PRODUCTIVE_BONUS = 4
MAX_TOTAL_REPLY_BONUS = 8       # hard ceiling across all reply bonuses
CONTINUITY_BONUS = 1            # OP replied back into their own thread

# Expand nested reply chains ("View replies" / "Lihat balasan") before
# extracting replies. Adds ~5-15s per post but captures the full thread.
EXPAND_NESTED_REPLIES = True

# Hard caps so a post with 100+ nested replies doesn't stall the worker.
MAX_NESTED_EXPAND_ROUNDS = 3
MAX_NESTED_EXPAND_CLICKS_PER_ROUND = 10  

for folder in (
    PROFILE_FOLDER,
    STATE_FOLDER,
    DEBUG_FOLDER,
    CHECKPOINT_FOLDER,
):
    folder.mkdir(parents=True, exist_ok=True)

DEFAULT_TARGET_UNIQUE_POSTS = 2000
HEADLESS = False
CHECKPOINT_EVERY = 5
Max_Posts_Per_Keyword = 250
SETUP_WORKER_PROFILES = True  # True = login manually into threads for each new worker, False = use saved login for each worker
NUM_PROCESSES = 4  

INTENT_TRACK = "Mortgage Financing"

# ============================================================
# 1b. COMMERCIAL USERNAME FILTER (auto-driven, no manual list)
# ============================================================

COMMERCIAL_USERNAME_PATTERNS = [
    r"(?:^|[._\-])jual(?:[._\-]|$)",
    r"properti", r"property", r"realty", r"estate",
    r"(?:^|[._\-])agen(?:[._\-]|$)", r"agent", r"agency",
    r"mortgage",
    r"(?:^|[._\-])kpr(?:[._\-]|$)",
    r"(?:^|[._\-])loan(?:[._\-]|$)",
    r"finance", r"financial", r"invest",
    r"consultant", r"konsultan", r"broker", r"developer",
    r"subsidi",
    r"(?:^|[._\-])home(?:[._\-]|$)",
    r"(?:^|[._\-])house(?:[._\-]|$)",
    r"rumahimpian",
    r"rumahmurah",
    r"jualrumah",
    r"dijualrumah",
    r"belirumah",
    r"kpr",           # catch any username with kpr in it
    r"tanyakeuangan",
    r"tanyaproperti",
]

COMMERCIAL_USERNAMES_MANUAL: set[str] = set()


def username_looks_commercial(username: str) -> bool:
    u = (username or "").lower().lstrip("@").strip()
    if not u:
        return False
    if u in COMMERCIAL_USERNAMES_MANUAL:
        return True
    for pattern in COMMERCIAL_USERNAME_PATTERNS:
        if re.search(pattern, u):
            return True
    return False


def build_auto_blacklist(min_post_count: int = 3) -> set[str]:
    """Any account with >= min_post_count posts in the dataset is almost
    certainly a content creator, seller, or service account. Real leads
    post about KPR once or twice at most."""
    if not PROCESSED_POSTS_FILE.exists():
        return set()
    df = pd.read_csv(PROCESSED_POSTS_FILE).fillna("")
    if "username" not in df.columns:
        return set()
    counts = df["username"].astype(str).str.lower().str.strip().value_counts()
    blacklist = set(counts[counts >= min_post_count].index)
    print(f"[AUTO-BLACKLIST] {len(blacklist)} accounts with >= {min_post_count} posts:")
    for name in sorted(blacklist):
        print(f"  {name} ({counts[name]} posts)")
    return blacklist


# These are safety limits only. They are not the normal reason for scrolling
# or stopping. A keyword normally stops after repeated completed scrolls add
# no new post links.
EMERGENCY_MAX_SCROLLS_PER_KEYWORD = 150
STABLE_END_ATTEMPTS = 3
SEARCH_STATE_CHANGE_TIMEOUT_MS = 9000
POST_READY_TIMEOUT_MS = 20000
POST_STABILITY_TIMEOUT_SECONDS = 12

THREADS_HOME = "https://www.threads.com/"
THREADS_SEARCH_TEMPLATE = (
    "https://www.threads.com/search?q={query}&serp_type=default"
)


# ============================================================
# 2. KPR-ONLY SEARCH PHRASES
# ============================================================

# Generic example keywords. In production these are loaded from a config
# file that is excluded from the repo (see .gitignore).
SEARCH_KEYWORDS = [


    "take over KPR",
    "takeover KPR",
    "kpr ditolak bank",
    "pindah bank KPR",
    "kpr floating naik",
    "takeover dari btn",
    "takeover dari bni",
    "kpr gagal bayar",
    "cicilan KPR naik",
    "syarat take over kpr",

    # keyword list refined iteratively based on early-run hit rates
    "kpr btn mahal",
    "kpr bri mahal",
    "kpr bca mahal",
    "kpr mandiri mahal",
    "kpr btn cicilan naik",
    "kpr bri cicilan naik",
    "kpr bca cicilan naik",
    "kpr mandiri cicilan naik",
    "takeover dari mandiri",
    "takeover dari bca",
    "takeover dari bri",
    "takeover dari permata",
    "takeover bank bri",
    "takeover bank bca",
    "takeover bank mandiri",
    "pindah kpr bri",
    "pindah kpr bca",
    "pindah kpr mandiri",
    "take over kpr nobu",
    "take over kpr cimb",
    "take over kpr ocbc",
    "take over kpr bni",
    "takeover bank btn",
    "floating btn kpr",
    "kpr btn syariah floating",
    "kpr btn syariah naik",
    "restrukturisasi btn",
    "pelunasan kpr btn",
    "migrasi bunga kpr",
    "refinance kpr saya",
    "pengajuan migrasi kpr",
    "mau pindah bank kpr",
    "ngos-ngosan kpr",
    "kpr mencekik",
    "ganti bank kpr",
    "refinancing btn",
    "butuh sales kpr",
    "cari sales kpr",
    "sales kpr dm",
    "mohon info kpr",
    "minta rekomendasi kpr",
    "bunga floating kpr saya",
    "kpr saya floating",
    "kpr masuk floating",-
    "fixed rate kpr habis",
    "masa fixed kpr habis",
]

NORMALIZED_SEARCH_LABELS = {
    re.sub(r"\s+", " ", keyword.lower()).strip()
    for keyword in SEARCH_KEYWORDS
}

# ============================================================
# 3. KPR CONTEXT PATTERNS
# ============================================================


KPR_CONTEXT_PATTERNS = [
    r"\bkpr\b",
    r"\bkredit pemilikan rumah\b",
]

BANK_NAME_PATTERNS = [
    r"\bbca\b",
    r"\bbri\b",
    r"\bbni\b",
    r"\bmandiri\b",
    r"\bbtn\b",
    r"\bcimb\b",
    r"\bcimb\s*niaga\b",
    r"\bpermata\b",
    r"\bdanamon\b",
    r"\bocbc\b",
    r"\bmaybank\b",
    r"\bpanin\b",
    r"\bbsi\b",
    r"\bbank syariah indonesia\b",
    r"\bstandard chartered\b",
    r"\bhsbc\b",
    r"\bbank jago\b",
    r"\bseabank\b",
]

PRODUCTIVE_REPLY_PATTERNS = [
    # Personal experience with a concrete action
    r"\b(?:aku|saya|gue|gw|kami)\b.{0,60}"
    r"\b(?:pakai|pake|ambil|ajukan|mengajukan|proses|pindah|"
    r"take\s*over|takeover|refinance|refinancing)\b.{0,50}\bkpr\b",
    r"\bkpr\b.{0,50}\b(?:aku|saya|gue|gw|kami)\b.{0,35}"
    r"\b(?:pakai|pake|ambil|proses|pindah|ajukan|mengajukan)\b",

    # Specific financial numbers (rates, tenor, DP, installments)
    r"\b(?:bunga|rate|fixed|tenor|plafon|dp|angsuran|cicilan)\b.{0,40}"
    r"\b\d+(?:[.,]\d+)?\s*(?:%|persen|tahun|thn|bulan|jt|juta|miliar)\b",
    r"\b\d+(?:[.,]\d+)?\s*%\b.{0,35}"
    r"\b(?:bunga|rate|fixed|tenor|per\s*tahun|p\.?a\.?)\b",

    # Actionable advice
    r"\b(?:sebaiknya|saran(?:nya|ku|saya)?|mending|lebih baik|"
    r"menurut(?:ku| saya)?|worth it)\b",
    r"\b(?:coba|tanya|hubungi|datang|kunjungi)\b.{0,35}"
    r"\b(?:ke|langsung)\b.{0,25}\b(?:bank|kantor|cabang)\b",

    # Concrete recommendation of a specific bank
    r"\b(?:coba|cobain|pindah ke|ambil di|pakai|pake|ke)\b.{0,30}"
    r"\b(?:bca|bri|bni|mandiri|btn|cimb|permata|danamon|ocbc|"
    r"maybank|panin|bsi|bank\s+[a-z]+)\b",

    # Detailed answers that mention process specifics
    r"\b(?:syaratnya|caranya|prosesnya|dokumennya|biayanya|"
    r"tenornya|bunganya)\b",
]

REPLY_RELEVANCE_PATTERNS = [
    r"\bkpr\b",
    r"\bcicilan kpr\b",
    r"\bangsuran kpr\b",
    r"\bbunga kpr\b",
    r"\btake\s*over kpr\b",
    r"\btakeover kpr\b",
    r"\boper kredit kpr\b",
    r"\brefinancing kpr\b",
    r"\bpengajuan kpr\b",
    r"\bsyarat kpr\b",
    r"\bsimulasi kpr\b",
    r"\bbank mana\b",
    r"\bbank apa\b",
] + BANK_NAME_PATTERNS + [
    # A named bank offered as a recommendation/answer, without repeating "kpr".
    r"\bcoba\b.{0,25}\b(?:bca|bri|bni|mandiri|btn|cimb|permata|danamon|ocbc|maybank|panin|bsi)\b",
    r"\b(?:bca|bri|bni|mandiri|btn|cimb|permata|danamon|ocbc|maybank|panin|bsi)\b.{0,25}\b(?:aja|oke|bagus|recommended|worth it|worth)\b",
    # Shared personal experience continuing the same thread's topic.
    r"\baku (?:juga|sama)\b.{0,40}\b(?:proses|ngajuin|ajuin|nyicil|take\s*over|takeover)\b",
    r"\bsaya (?:juga|sama)\b.{0,40}\b(?:proses|mengajukan|mencicil|take\s*over|takeover)\b",
    r"\bsama nih\b",
    r"\bsenasib\b",
]

PERSONAL_PRONOUNS = r"(?:saya|aku|gue|gw|kami|kita|saya sendiri|aku sendiri)"
ACTION_WORDS = (
    r"(?:mau|ingin|pengen|pingin|berencana|rencana|mempertimbangkan|"
    r"kepikiran|berminat|lagi cari|sedang cari|butuh|perlu)"
)
QUESTION_WORDS = (
    r"(?:gimana|bagaimana|apakah|berapa|kenapa|mengapa|bank apa|bank mana|"
    r"ada yang|ada yg|mohon info|minta saran|minta rekomendasi|spill)"
)

# ============================================================
# 4. SELLER / ADVERTISEMENT PATTERNS
# ============================================================

# High-confidence statements that the author is providing or selling a
# product/service rather than asking for help.
EXPLICIT_SELLER_PATTERNS = [
    r"\bsaya (?:sales|marketing|marketer|agen|agent|broker|developer)\b",
    r"\baku (?:sales|marketing|marketer|agen|agent|broker|developer)\b",
    r"\bkami (?:sales|marketing|marketer|agen|agent|broker|developer)\b",
    r"\bmarketing (?:bank|kpr|property|properti|perumahan)\b",
    r"\bagen (?:property|properti|rumah|kpr)\b",
    r"\bagent (?:property|properti|rumah|kpr)\b",
    r"\bproperty consultant\b",
    r"\bkonsultan properti\b",
    r"\bjasa (?:pengajuan|proses|take\s*over|takeover|refinancing) kpr\b",
    r"\bmelayani (?:pengajuan|proses|take\s*over|takeover|refinancing) kpr\b",
    r"\bopen (?:jasa|konsultasi).{0,30}\bkpr\b",
    r"\b(?:saya|aku|kami) (?:bisa|siap) bantu.{0,45}(?:proses|pengajuan|take\s*over|takeover|refinancing|kpr)\b",
    r"\b(?:proses|pengajuan|take\s*over|takeover|refinancing) kpr.{0,35}(?:bisa|siap) (?:saya|aku|kami) bantu\b",
    r"\bkonsultasi (?:kpr )?gratis\b",
    r"\bfree konsultasi\b",
    r"\bproses kpr dibantu\b",
    r"\bproses bisa dibantu.{0,25}\bkpr\b",
    r"\bkami bantu.{0,35}(?:proses|pengajuan|take\s*over|takeover|kpr)\b",
    r"\bsaya bantu.{0,35}(?:proses|pengajuan|take\s*over|takeover|kpr)\b",
    r"\baku bantu.{0,35}(?:proses|pengajuan|take\s*over|takeover|kpr)\b",
    r"\bbisa kami proses\b",
    r"\bbisa saya proses\b",
    r"\bbisa aku proses\b",
]

# Listing language. A single weak listing term is not always enough, but a
# direct sale/transfer statement is high confidence.
DIRECT_LISTING_PATTERNS = [
    r"\bdijual (?:rumah|unit|apartemen)\b",
    r"\b(?:rumah|unit|apartemen) dijual\b",
    r"\bjual (?:rumah|unit|apartemen)\b",
    r"\brumah oper kredit\b",
    r"\boper kredit rumah\b",
    r"\bsini\s+take\s*over\b",
    r"\bsini\s+takeover\b",
    r"\bmending\s+coba\s+take\s*over\b",
    r"\bcoba\s+take\s*over\s+saja\b",
    r"\btake\s*over rumah\b",
    r"\btakeover rumah\b",
    r"\btake\s*over unit\b",
    r"\btakeover unit\b",
    r"\bmau take\s*over rumah saya\b",
    r"\bmau takeover rumah saya\b",
    r"\blepas rumah\b",
    r"\blepas unit\b",
    r"\bunit ready\b",
    r"\bready stock\b",
    r"\bsiap huni\b",
    r"\bmasuk lagi.{0,20}take\s*over\b",
    r"\btolong\s+lanjutin\b.{0,30}\bkpr\b",
    r"\blanjutin\s+kpr\s+nya\b",
    r"\bkasih\s+gratis\b.{0,60}\bkpr\b",
    r"\baku\s+lepas\s+kpr\b",
    r"\baku\s+lepas\s+rumah\b",
    r"\bdioper\b.{0,30}\bkpr\b",
    r"\bsiapa\s+mau\s+lanjutin\b",

        # -- Sell-side takeover offers (author wants OUT, not in) --
    r"\bsiap\s+over\s*kredit\b",
    r"\bsiap\s+take\s*over\b",
    r"\bsiapp\s+over\s*kredit\b",
    r"\bsiapp\s+take\s*over\b",
    r"\bmau\s+(?:jual|lepas|oper)\b.{0,30}\b(?:kpr|rumah)\b",
    r"\b(?:kpr|rumah).{0,30}\bmau\s+(?:aku|saya|gue|gw)\s+(?:jual|lepas)\b",
    r"\bcari\s+pembeli\b",
    r"\bover\s*kredit\s+(?:rumah|kpr)\b",
    r"\boper\s*kredit\s+(?:rumah|kpr)\b",
    r"\bdijual\s+(?:kpr|rumah|over\s*kredit)\b",
    r"\boverkredit\b",
    r"\boperkredit\b",
]


PROPERTY_DETAIL_PATTERNS = [
    r"\btype\s*\d+[a-z]?\b",
    r"\btipe\s*\d+[a-z]?\b",
    r"\blt\s*[:=/.-]?\s*\d+\b",
    r"\blb\s*[:=/.-]?\s*\d+\b",
    r"\bluas tanah\b",
    r"\bluas bangunan\b",
    r"\bkamar tidur\b",
    r"\bkamar mandi\b",
    r"\b\d+\s*(?:kt|km)\b",
    r"\bcarport\b",
    r"\bsertifikat\b",
    r"\bshm\b",
    r"\bshgb\b",
    r"\blistrik\s*\d+\b",
    r"\bair (?:pdam|sumur|jetpump|jet pump)\b",
    r"\bsisa tenor\b",
    r"\bsudah (?:bayar|byr)\b",
    r"\bangsuran\s*(?:rp)?\s*[\d.,]+\b",
    r"\bcicilan\s*(?:rp)?\s*[\d.,]+\s*(?:\/|per)\s*bulan\b",
    r"\bharga\s*(?:rp)?\s*[\d.,]+\b",
    r"\b\d+\s*(?:jt|juta|miliar|milyar)\b",
    r"\bfasilitas (?:cluster|perumahan)\b",
    r"\bakses tol\b",
    r"\bdekat (?:mall|sekolah|stasiun|rumah sakit|rs|tol)\b",
]

PROMOTION_PATTERNS = [
    r"\bpromo\b",
    r"\bharga perdana\b",
    r"\bdiskon\b",
    r"\bcashback\b",
    r"\bfree (?:biaya|notaris|bphtb|admin)\b",
    r"\bbebas biaya\b",
    r"\bgratis biaya\b",
    r"\bdp ringan\b",
    r"\bminimal dp\b",
    r"\bbooking fee\b",
    r"\bbunga mulai\b",
    r"\bcicilan mulai\b",
    r"\bunit terbatas\b",
    r"\blimited unit\b",
    r"\bslot promo\b",
    r"\bpembeli pertama\b",
    r"\bpenawaran khusus\b",
    r"\bburuan\b",
    r"\bsegera booking\b",
    r"\bkhusus hari ini\b",
    r"\bcocok untuk investasi\b",
    r"\bhunian pertama atau investasi\b",
]

COMMERCIAL_CALL_TO_ACTION_PATTERNS = [
    r"\bhubungi saya\b",
    r"\bhubungi kami\b",
    r"\bkontak saya\b",
    r"\bkontak kami\b",
    r"\bwhatsapp\b",
    r"\bchat wa\b",
    r"\bwa\s*[:=]?\s*(?:\+?62|0)?8\d",
    r"\bmarketing info\b",
    r"\binfo detail (?:boleh )?(?:dm|wa|chat)\b",
    r"\binfo selengkapnya\b",
    r"\blangsung (?:dm|chat|wa|hubungi)\b",
    r"\b(?:dm|chat|wa) (?:saya|aku|kami)\b",
    r"\bboleh (?:dm|chat|wa) (?:saya|aku|kami)\b",
    r"\breply [\"']?minat[\"']?\b",
    r"\bketik [\"']?minat[\"']?\b",
    r"\bminat (?:dm|chat|wa)\b",
    r"\bbooking sekarang\b",
    r"\bsurvey lokasi\b",
    r"\bsurvei lokasi\b",
    r"\bcek lokasi\b",
    r"(?:\+62|62|0)8\d[\d\-\s]{7,}",
]

SELLER_OFFER_PATTERNS = [
    r"\byang mau.{0,55}\bkpr\b",
    r"\byang ingin.{0,55}\bkpr\b",
    r"\bkaka+.{0,20}yang mau\b",
    r"\bbisa ke bank\b",
    r"\bada suku bunga promo\b",
    r"\buntuk info tenor\b",
    r"\bsimulasi cicilan.{0,35}(?:dm|wa|chat|hubungi)\b",
    r"\byuk konsultasikan\b",
    r"\byuk yang mau tanya\b",
    r"\bcukup kirim\b",
    r"\blangsung dihitungkan\b",
    r"\blangsung di hitungin\b",
    r"\bcover (?:area|wilayah)\b",
]



# Extra commercial signals used by the aggressive precision-first filter.
AGGRESSIVE_PROVIDER_PATTERNS = [
    r"\b(?:kami|saya|aku) menawarkan\b",
    r"\b(?:kami|saya|aku) menyediakan\b",
    r"\b(?:kami|saya|aku) melayani\b",
    r"\b(?:kami|saya|aku) siap bantu\b",
    r"\b(?:kami|saya|aku) bantu sampai (?:akad|approve|approval|selesai)\b",
    r"\bsolusi kpr\b",
    r"\bprogram kpr\b",
    r"\bproduk kpr\b",
    r"\bpaket kpr\b",
    r"\bpilihan kpr\b",
    r"\bofficial (?:sales|marketing|developer)\b",
    r"\bkantor pemasaran\b",
    r"\btim (?:sales|marketing|kpr|mortgage)\b",
    r"\bdapatkan simulasi kpr\b",
    r"\b(?:kami|saya|aku) hitungkan\b",
    r"\b(?:kami|saya|aku) menghitungkan\b",
]

# Voice-of-a-provider patterns. Catches single-post sellers whose
# usernames look ordinary and who don't repeat enough to trigger
# the auto-blacklist.
PROVIDER_VOICE_PATTERNS = [
    # "my clients" / "my customers" — strongest single signal
    r"\bklien (?:saya|aku|gue|gw|kami)\b",
    r"\bnasabah (?:saya|aku|gue|gw|kami)\b",
    r"\bcustomer (?:saya|aku|gue|gw|kami)\b",
    r"\byang\s+mau\s+takeover\b.{0,25}\bdibantu\b",
    r"\bboleh\s+yuk\s+dibantu\b",
    r"\byang\s+mau\s+.{0,20}\bkpr\b.{0,25}\bdibantu\b",

    # "we can help / I can help" offers
    r"\b(?:saya|aku|gue|gw|kami|kita)\s+(?:bisa|siap)\s+bantu\b",
    r"\b(?:saya|aku|gue|gw|kami)\s+bantu\b",
    r"\b(?:saya|aku|gue|gw|kami)\s+proses\b",
    r"\b(?:team|tim)\s+(?:kami|kita|saya)\s+(?:bantu|yang bantu)\b",
    r"\bdibantu\s+sampai\s+(?:akad|approved|tuntas|deal|selesai)\b",
    r"\bbantu\s+prosesnya\b",
    r"\bproses\s+dibantu\s+sampai\s+tuntas\b",

    # "come to me" / "chat me" — softer than the CTA patterns
    r"\byuk\s+(?:ngobrol|konsul|tanya)\b",
    r"\bngobrol\s+di\s+dm\b",
    r"\bcerita\s+aja\s+ke\b",
    r"\bcurhat\s+ke\s+@?\w+\b",
    r"\b(?:silakan|silahkan|boleh)\s+(?:dm|wa|chat|hubungi)\b",
    r"\bfeel\s+free\b.{0,40}\b(?:dm|wa|chat|hubungi)\b",

    # Service-account framing
    r"\b(?:kami|saya|aku)\s+melayani\b",
    r"\b(?:kami|saya|aku)\s+menyediakan\b",
    r"\b(?:kami|saya|aku)\s+menawarkan\b",
    r"\b(?:konsultasi|konsul)\s+(?:gratis|dulu)\b",
    r"\bkonsultasi\s+sampai\s+deal\b",
    r"\bgratis\s+sampai\s+deal\b",

    # Professional self-identification
    r"\b(?:saya|aku)\s+(?:mortgage|sales|marketing|agen|agent|broker|consultant)\b",
    r"\bperjalanan\s+karier\s+(?:saya|aku)\s+sebagai\b",
    r"\bmortgage\s+officer\b",

    # Financial services by-name
    r"\b(?:fast\s+loan|kredit\s+usaha|top\s+up|refinancing)\s+by\s+\w+\b",
    r"\b(?:pengajuan|proses)\s+(?:pembiayaan|kredit)\s+(?:by|dengan)\b",
]

AGGRESSIVE_PROMOTION_PATTERNS = [
    r"\bapproval cepat\b",
    r"\bapprove cepat\b",
    r"\bproses cepat\b",
    r"\bproses mudah\b",
    r"\bmudah disetujui\b",
    r"\btanpa ribet\b",
    r"\btanpa biaya\b",
    r"\btanpa penalti\b",
    r"\btanpa dp\b",
    r"\bdp 0\s*%\b",
    r"\bdp mulai\b",
    r"\btenor hingga\b",
    r"\btenor sampai\b",
    r"\bplafon hingga\b",
    r"\bsuku bunga spesial\b",
    r"\bsuku bunga khusus\b",
    r"\bdapatkan sekarang\b",
    r"\bmanfaatkan promo\b",
    r"\bsyarat dan ketentuan berlaku\b",
]

AGGRESSIVE_CTA_PATTERNS = [
    r"\bhubungi admin\b",
    r"\bchat admin\b",
    r"\binbox\b",
    r"\bpm saya\b",
    r"\bdaftar sekarang\b",
    r"\bapply sekarang\b",
    r"\bajukan sekarang\b",
    r"\bklik (?:link|tautan)\b",
    r"\blink (?:di )?bio\b",
    r"\bcek bio\b",
    r"\bisi (?:form|formulir)\b",
    r"\bminta brosur\b",
    r"\bdownload brosur\b",
    r"\bprice\s*list\b",
    r"\bjadwalkan (?:survey|survei)\b",
    r"\bkomen [\"']?minat[\"']?\b",
    r"\bcomment [\"']?minat[\"']?\b",
]

AGGRESSIVE_FINANCIAL_PRODUCT_PATTERNS = [
    r"\bfixed \d+(?:[.,]\d+)?\s*%\b",
    r"\bbunga \d+(?:[.,]\d+)?\s*%\b",
    r"\brate \d+(?:[.,]\d+)?\s*%\b",
    r"\bmargin \d+(?:[.,]\d+)?\s*%\b",
    r"\btenor \d+\s*(?:tahun|thn|bulan)\b",
    r"\bplafon (?:hingga|sampai|mulai)\b",
    r"\bbiaya (?:admin|provisi|appraisal|akad)\b",
    r"\btop up kpr\b",
    r"\bkpr multiguna\b",
]

CUSTOMER_SEEKING_PROVIDER_PATTERNS = [
    r"\bbutuh (?:sales|marketing|agen|agent).{0,35}\bkpr\b",
    r"\bcari (?:sales|marketing|agen|agent).{0,35}\bkpr\b",
    r"\b(?:sales|marketing|agen|agent) kpr (?:boleh )?(?:dm|chat|wa) (?:saya|aku|gue|gw)\b",
    r"\b(?:sales|marketing) (?:boleh )?(?:dm|chat|wa).{0,25}\bkpr\b",
]

EXTERNAL_LINK_PATTERNS = [
    r"https?://",
    r"www\.",
    r"\bbit\.ly/",
    r"\blinktr\.ee/",
    r"\bwa\.me/",
    r"\bforms\.gle/",
]

POLL_PROMPT_PATTERNS = [
    r"\bboleh\s+jujur\s+sharing\b",
    r"\bjujur\s+sharing\b",
    r"\bboleh\s+sharing\b",
    r"\bbuat\s+yang\s+(?:sudah|sedang|lagi|udah)\b.{0,40}\bkpr\b",
    r"\bmenurut\s+(?:kalian|kamu|teman|warga|kalyan)\b.{0,60}\bkpr\b",
    r"\bshare\s+(?:pengalaman|cerita)\b",
    r"\bceritain\s+(?:di|dong)\b",
]

HYPOTHETICAL_PATTERNS = [
    r"\bkalau\s+(?:gw|gue|aku|saya|kamu|lo)\s+jadi\b",
    r"\bkalau\s+(?:saya|aku)\s+(?:misal|umpama|anggap)\b",
    r"\b(?:gue|gw|aku|saya)\s+(?:bisa|boleh)\s+aja\s+bilang\b",
    r"\bseandainya\b",
    r"\bmisalkan\s+(?:kamu|kalian|kita)\b",
]

# ============================================================
# 5. HOT, WARM, AND GENERAL KPR PATTERNS
# ============================================================

TAKEOVER_ACTION_TERMS = (
    r"(?:take\s*over kpr|takeover kpr|refinancing kpr|refinance kpr|"
    r"pindah bank(?: untuk)? kpr|pindah kpr|alih kredit kpr|"
    r"pengalihan kpr|oper kpr)"
)

HOT_PERSONAL_TAKEOVER_PATTERNS = [
    rf"\b{PERSONAL_PRONOUNS}.{{0,70}}{ACTION_WORDS}.{{0,70}}{TAKEOVER_ACTION_TERMS}\b",
    rf"\b{ACTION_WORDS}.{{0,55}}{TAKEOVER_ACTION_TERMS}\b",
    rf"\bkpr {PERSONAL_PRONOUNS}.{{0,75}}(?:take\s*over|takeover|refinanc|pindah bank|alih kredit|oper)\b",
    rf"\b{PERSONAL_PRONOUNS}.{{0,55}}(?:take\s*over|takeover|refinanc|pindah bank|alih kredit).{{0,45}}\bkpr\b",
    r"\bsedang proses.{0,35}(?:take\s*over|takeover|refinancing).{0,25}\bkpr\b",
]

HOT_DIRECT_REQUEST_PATTERNS = [
    r"\bsales kpr (?:boleh )?(?:dm|chat|wa) (?:saya|aku|gue|gw)\b",
    r"\bsales (?:boleh )?(?:dm|chat|wa).{0,20}\bkpr\b",
    r"\bada (?:marketing|sales|agen|agent)\b.{0,40}\b(?:takeover|take\s*over|refinanc)\b",
    r"\bmarketing\b.{0,20}\b(?:bca|bri|bni|mandiri|btn|cimb|permata|danamon|ocbc|maybank|panin|bsi)\b.{0,30}\bkpr\b",
    r"\bbutuh bantuan.{0,45}\bkpr\b",
    r"\bmohon bantuan.{0,45}\bkpr\b",
    r"\bada yang bisa bantu.{0,45}\bkpr\b",
    r"\bada yg bisa bantu.{0,45}\bkpr\b",
    r"\bkpr.{0,45}ada yang bisa bantu\b",
    r"\bminta rekomendasi.{0,45}(?:bank|kpr)\b",
    r"\bbutuh rekomendasi.{0,45}(?:bank|kpr)\b",
    r"\brekomendasi bank.{0,45}\bkpr\b",
    r"\bbank apa.{0,50}\bkpr\b",
    r"\bbank mana.{0,50}\bkpr\b",
    r"\bkpr.{0,50}bank apa\b",
    r"\bkpr.{0,50}bank mana\b",
    r"\bminta simulasi.{0,45}\bkpr\b",
    r"\bbutuh simulasi.{0,45}\bkpr\b",
    r"\bada simulasi.{0,45}\bkpr\b",
    r"\bspill.{0,40}bank.{0,30}\bkpr\b",
    r"\byang sudah take\s*over kpr.{0,50}bank apa\b",
    r"\byang udah take\s*over kpr.{0,50}bank apa\b",
    r"\bada bunga kpr yang lebih rendah\b",
]

HOT_URGENCY_PATTERNS = [
    r"\bsegera.{0,40}\bkpr\b",
    r"\bbulan ini.{0,40}\bkpr\b",
    r"\bminggu ini.{0,40}\bkpr\b",
    r"\bsedang mengajukan kpr\b",
    r"\blagi proses kpr\b",
    r"\bmau mulai proses kpr\b",
]

WARM_FLOATING_PATTERNS = [
    rf"\bkpr {PERSONAL_PRONOUNS}.{{0,70}}(?:floating|bunga naik|cicilan naik|fixed habis|masa fixed habis)\b",
    rf"\b(?:floating|bunga naik|cicilan naik|fixed habis|masa fixed habis).{{0,70}}\bkpr {PERSONAL_PRONOUNS}\b",
    rf"\b{PERSONAL_PRONOUNS}.{{0,55}}\bkpr.{{0,55}}(?:floating|bunga naik|cicilan naik|fixed habis|masa fixed habis)\b",
    r"\bcicilan kpr (?:saya|aku|gue|gw|kami).{0,40}(?:naik|berat|mahal|membengkak)\b",
]

WARM_NEW_KPR_PATTERNS = [
    rf"\b{PERSONAL_PRONOUNS}.{{0,55}}{ACTION_WORDS}.{{0,45}}(?:ajukan|mengajukan|ambil|mengambil|beli rumah dengan).{{0,35}}\bkpr\b",
    rf"\b{ACTION_WORDS}.{{0,40}}(?:ajukan|mengajukan|ambil|mengambil).{{0,35}}\bkpr\b",
    r"\bkpr rumah pertama\b",
    r"\bfirst home.{0,25}\bkpr\b",
]

WARM_APPLICATION_PROBLEM_PATTERNS = [
    r"\bpengajuan kpr (?:saya|aku|gue|gw|kami).{0,50}(?:ditolak|gagal|belum disetujui|belum approve|tidak disetujui)\b",
    r"\bkpr (?:saya|aku|gue|gw|kami).{0,50}(?:ditolak|gagal|belum disetujui|belum approve|tidak disetujui)\b",
    r"\bkenapa pengajuan kpr.{0,35}(?:ditolak|gagal)\b",
    r"\bmasalah pengajuan kpr\b",
]

WARM_COMPARISON_REQUIREMENT_PATTERNS = [
    r"\bbanding(?:kan)? bank.{0,45}\bkpr\b",
    r"\bperbandingan.{0,40}(?:bank|bunga).{0,35}\bkpr\b",
    r"\bpilih bank.{0,40}\bkpr\b",
    r"\bbunga kpr.{0,40}(?:paling rendah|lebih rendah|termurah)\b",
    r"\bsyarat.{0,45}\bpengajuan kpr\b",
    r"\bsyarat kpr\b",
    r"\bdokumen.{0,45}\bkpr\b",
    r"\bberapa lama.{0,45}\bproses kpr\b",
    r"\bbiaya.{0,45}\bpengajuan kpr\b",
]

PERSONAL_CONTEXT_PATTERNS = [
    rf"\bkpr {PERSONAL_PRONOUNS}\b",
    rf"\b{PERSONAL_PRONOUNS}.{{0,65}}\bkpr\b",
    r"\bcicilan (?:saya|aku|gue|gw|kami)\b",
    r"\bpengajuan (?:saya|aku|gue|gw|kami)\b",
    r"\brumah (?:saya|aku|gue|gw|kami)\b",
]

GENERAL_KPR_INTEREST_PATTERNS = [
    rf"\b{QUESTION_WORDS}.{{0,80}}\bkpr\b",
    rf"\bkpr.{{0,80}}{QUESTION_WORDS}\b",
    r"\bada yang punya pengalaman.{0,50}\bkpr\b",
    r"\bada yg punya pengalaman.{0,50}\bkpr\b",
    r"\bpengalaman kalian.{0,50}\bkpr\b",
    r"\bmenurut kalian.{0,50}\bkpr\b",
    r"\bmau tanya.{0,50}\bkpr\b",
    r"\bingin tanya.{0,50}\bkpr\b",
    r"\bpenasaran.{0,50}\bkpr\b",
    r"\blagi pelajari.{0,50}\bkpr\b",
    r"\blagi cari tahu.{0,50}\bkpr\b",
]

EDUCATIONAL_OR_NEWS_PATTERNS = [
    r"\bapa itu kpr\b",
    r"\bpengertian kpr\b",
    r"\bcara kerja kpr\b",
    r"\btips (?:mengajukan )?kpr\b",
    r"\bberikut tips\b",
    r"\bberikut penjelasan\b",
    r"\bsimak penjelasan\b",
    r"\bedukasi kpr\b",
    r"\bwebinar kpr\b",
    r"\bseminar kpr\b",
    r"\bpromo kpr bank\b",
    r"\bsuku bunga kpr turun\b",
    r"\bsuku bunga kpr naik\b",
    r"\bkebijakan kpr\b",
]



NARRATIVE_MARKERS = [
    r"\bmenyambung\s+cerita\b",
    r"\bberbagi\s+pengalaman\b",
    r"\bsharing\s+pengalaman\b",
    r"\bpengalaman\s+pribadi\b",
    r"\bcerita\s+pengalaman\b",
    r"\bmau\s+cerita\b.{0,40}\bkpr\b",
    r"\bcerita\s+yg\s+kemarin\b",
    r"\blanjut\s+cerita\b",
    r"\bsekedar\s+sharing\b",
    r"\bcuma\s+mau\s+share\b",
]


KPR_REGRET_PATTERNS = [
    r"\b(?:nyesel|menyesal|semenyesal|kesalahan\s+terbesar|"
    r"penyesalan|nyesal)\b.{0,60}\b(?:ambil|ngambil|ajuin|punya|kpr)\b",
    r"\b(?:ambil|ngambil|ajuin|punya)\b.{0,40}\bkpr\b.{0,80}"
    r"\b(?:nyesel|menyesal|rugi|salah|fucek|nyesal|sayang)\b",
    r"\b(?:kesalahan\s+terbesar|penyesalan\s+terbesar)\b.{0,60}\bkpr\b",
]

# ============================================================
# 6. TEXT HELPERS
# ============================================================

SLANG_NORMALIZATION = {
    "gak": "tidak",
    "ga": "tidak",
    "ngga": "tidak",
    "nggak": "tidak",
    "blm": "belum",
    "udh": "sudah",
    "udah": "sudah",
    "lg": "lagi",
    "dgn": "dengan",
    "krn": "karena",
    "bgt": "banget",
    "dpt": "dapat",
    "utk": "untuk",
    "yg": "yang",
    "aja": "saja",
    "kalo": "kalau",
    "gmn": "bagaimana",
    "gimana": "bagaimana",
    "nanya": "bertanya",
    "cicil2": "cicilan",

    # --- Colloquial verb forms (ng-/ny- prefixes) ---
    "ngambil":   "ambil",
    "ngambilin": "ambil",
    "ngajukan":  "ajukan",
    "ngajuin":   "ajukan",
    "ajuin":     "ajukan",
    "mengajuin": "mengajukan",
    "nyicil":    "cicil",
    "nyicilin":  "cicil",
    "njual":     "jual",
    "jualin":    "jual",
    "ngontrak":  "kontrak",
    "ngitung":   "menghitung",
    "ngitungin": "menghitung",
    "ngurus":    "mengurus",
    "ngurusin":  "mengurus",
    "ngobrol":   "bicara",
    "ngomong":   "bicara",

    # --- Common Threads abbreviations ---
    "sy":         "saya",
    "aq":         "aku",
    "gua":        "gue",
    "ak":         "aku",
    "skrg":       "sekarang",
    "tp":         "tapi",
    "sm":         "sama",
    "temen":      "teman",
    "temen2":     "teman-teman",
    "overkredit": "over kredit",
    "operkredit": "oper kredit",
    "kprku": "kpr saya",
    "kprnya": "kpr dia",
    "kprmu": "kpr kamu",
    "naek": "naik",
    "naikin": "menaikkan",
    "kenaikan": "kenaikan",  # sanity, already fine
}

UI_NOISE_LINES = {
    "like",
    "reply",
    "repost",
    "share",
    "translate",
    "follow",
    "more",
    "suka",
    "balas",
    "bagikan",
    "ikuti",
    "terjemahkan",
    "lihat aktivitas",
    "view activity",
    "follow back",
}


def normalize_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"[\u200b-\u200d\ufeff]", " ", text)
    text = text.lower()
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    for slang, replacement in SLANG_NORMALIZATION.items():
        text = re.sub(
        rf"(?<!\w){re.escape(slang)}(?!\w)",
        replacement,
        text,
    )
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ============================================================
# SUBJECT-AWARE SENTENCE ANALYSIS
# ============================================================

SENTENCE_SPLIT = re.compile(r"(?<=[.!?\n])\s+")
KPR_TOKEN = re.compile(r"\bkpr\b", re.IGNORECASE)

FIRST_PERSON = re.compile(
    r"\b(?:saya|aku|gue|gw|kami|kita)\b", re.IGNORECASE
)
SECOND_PERSON = re.compile(
    r"\b(?:kamu|anda|lo|lu|elu|kalian|u)\b", re.IGNORECASE
)
THIRD_PERSON = re.compile(
    r"\b(?:dia|mereka)\b",
    re.IGNORECASE,
)

# Requires: first-person pronoun + (optional modal) + verb + KPR
# within 40 chars. This approximates "the user is the subject of
# a KPR clause", not just "a pronoun appears near KPR".
# A sentence counts as a first-person KPR sentence when it contains
# BOTH a first-person pronoun AND a KPR token. That's it. No verb
# requirement, no window size limit. Subject-drop (no pronoun) is
# handled separately by checking second/third-person absence.

# Past-tense KPR markers — "aku ambil KPR dulu", "waktu itu saya KPR".
PAST_TENSE_KPR = re.compile(
    r"\b(?:dulu|kemarin|tadi|barusan|waktu\s+itu|tahun\s+lalu|bulan\s+lalu|"
    r"pas\s+waktu|dulu\s+saya|dulu\s+aku)\b.{0,60}\bkpr\b",
    re.IGNORECASE,
)

# Possessive-of-third-party: "teman saya", "keluarga saya", "adik saya" …
# The first-person pronoun here is a POSSESSOR, not the sentence subject.
# "Teman saya mau take over KPR" is NOT a first-person lead.
THIRD_PARTY_POSSESSIVE = re.compile(
    r"\b(?:teman|sahabat|saudara|sepupu|keponakan|tetangga|"
    r"orang\s+tua|keluarga|adik|kakak|anak|pacar|pasangan|"
    r"rekan|rekan\s+kerja|teman\s+kerja|teman\s+kantor|"
    r"teman-teman|sahabat-sahabat|saudara-saudara|"
    r"mertua|menantu|ipar|besan)"
    r"\b(?:teman|keluarga|adik|kakak|pacar|saudara)(?:ku|mu|nya)\b"
    r"\s+(?:saya|aku|gue|gw|kami|kita)\b",
    re.IGNORECASE,
)


def _count_subject_first_person(text: str) -> int:
    """Count first-person pronouns that are NOT possessors of a
    third-party noun. 'Teman saya' -> not counted.
    'Saya dan teman saya' -> the leading 'saya' IS counted."""
    possessive_positions: set[int] = set()
    for m in THIRD_PARTY_POSSESSIVE.finditer(text):
        inner = m.group(0)
        pm = FIRST_PERSON.search(inner)
        if pm:
            possessive_positions.add(m.start() + pm.start())

    count = 0
    for m in FIRST_PERSON.finditer(text):
        if m.start() not in possessive_positions:
            count += 1
    return count

# Teaching phrases that indicate the author is explaining, not living.
TEACHING_PHRASES = re.compile(
    r"\b(?:yang\s+perlu\s+(?:kamu|anda|kita)\s+tau|"
    r"berikut\s+(?:adalah|ini|beberapa|tips)|"
    r"simak\s+penjelasan|coba\s+pahami|"
    r"perlu\s+dipahami|jadi\s+intinya|"
    r"kesimpulannya|secara\s+umum|pada\s+dasarnya|"
    r"ada\s+beberapa\s+hal|faktor\s+(?:yang|utama)|"
    r"alasan\s+(?:kenapa|mengapa)|"
    r"tips\s+(?:kpr|mengajukan)|"
    r"edukasi\s+kpr)\b",
    re.IGNORECASE,
)


def split_into_sentences(text: str) -> list[str]:
    if not text:
        return []
    parts = SENTENCE_SPLIT.split(str(text))
    return [p.strip() for p in parts if p.strip()]


# ============================================================
# SUBJECT-AWARE POST ANALYSIS (post-level, subject-drop aware)
# ============================================================


def analyze_kpr_subject(text: str) -> dict[str, int]:
    """Determine whether the AUTHOR of this post is personally
    invested in KPR, or whether they're teaching/telling a story
    about someone else.

    Possessive pronouns attached to a third-party noun
    ('teman saya', 'keluarga saya') do NOT count as first-person.
    """
    raw = str(text or "")
    if not KPR_TOKEN.search(raw):
        return {
            "first_person_kpr_sentences": 0,
            "second_person_kpr_sentences": 0,
            "third_person_kpr_sentences": 0,
            "first_person_past_kpr_sentences": 0,
            "has_teaching_phrases": 0,
            "total_kpr_sentences": 0,
        }

    all_first = len(FIRST_PERSON.findall(raw))
    subject_first = _count_subject_first_person(raw)
    second_count = len(SECOND_PERSON.findall(raw))
    third_count = len(THIRD_PERSON.findall(raw))

    # If every first-person pronoun was a possessor of a third party
    # ("teman saya", "keluarga saya"), the subject is third-person.
    if all_first > 0 and subject_first == 0 and second_count == 0:
        third_count = max(third_count, 1)
        first_count = 0
    else:
        first_count = subject_first

    # Default: OP posts are about the OP.
    if first_count == 0 and second_count == 0 and third_count == 0:
        return {
            "first_person_kpr_sentences": 1,
            "second_person_kpr_sentences": 0,
            "third_person_kpr_sentences": 0,
            "first_person_past_kpr_sentences": 0,
            "has_teaching_phrases": int(bool(TEACHING_PHRASES.search(raw))),
            "total_kpr_sentences": 1,
        }

    dominant = max(first_count, second_count, third_count)

    if first_count == dominant:
        return {
            "first_person_kpr_sentences": 1,
            "second_person_kpr_sentences": 0,
            "third_person_kpr_sentences": 0,
            "first_person_past_kpr_sentences": int(
                bool(PAST_TENSE_KPR.search(raw))
            ),
            "has_teaching_phrases": int(bool(TEACHING_PHRASES.search(raw))),
            "total_kpr_sentences": 1,
        }
    if second_count == dominant:
        return {
            "first_person_kpr_sentences": 0,
            "second_person_kpr_sentences": 1,
            "third_person_kpr_sentences": 0,
            "first_person_past_kpr_sentences": 0,
            "has_teaching_phrases": int(bool(TEACHING_PHRASES.search(raw))),
            "total_kpr_sentences": 1,
        }
    return {
        "first_person_kpr_sentences": 0,
        "second_person_kpr_sentences": 0,
        "third_person_kpr_sentences": 1,
        "first_person_past_kpr_sentences": 0,
        "has_teaching_phrases": int(bool(TEACHING_PHRASES.search(raw))),
        "total_kpr_sentences": 1,
    }


def pattern_matches(normalized: str, patterns: list[str]) -> list[str]:
    return [
        pattern
        for pattern in patterns
        if re.search(pattern, normalized, flags=re.IGNORECASE)
    ]


def has_pattern(normalized: str, patterns: list[str]) -> bool:
    return bool(pattern_matches(normalized, patterns))


def canonicalize_post_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")
    path = re.sub(r"/media$", "", path, flags=re.IGNORECASE)

    if "/post/" not in path:
        return ""

    return urlunparse(
        (
            parsed.scheme or "https",
            parsed.netloc or "www.threads.com",
            path,
            "",
            "",
            "",
        )
    )


def content_fingerprint(text: str) -> str:
    normalized = normalize_text(text)
    normalized = re.sub(r"\b\d+\b", "#", normalized)
    normalized = re.sub(r"[^a-z0-9# ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def is_time_or_metric_line(lower: str) -> bool:
    return bool(
        re.fullmatch(r"\d+\s*(?:s|m|h|d|w)", lower)
        or re.fullmatch(
            r"\d+\s*(?:detik|menit|jam|hari|minggu|bulan|tahun)",
            lower,
        )
        or re.fullmatch(r"\d+(?:[.,]\d+)?[km]?", lower)
        or re.fullmatch(
            r"[\d,\.]+\s*(?:rb|ribu|jt|juta|k|m)?\s*(?:tayangan|views?)",
            lower,
        )
    )


def clean_main_post_body(raw_text: Any, username: str = "") -> str:
    """Clean only the main post body and strip likely topic/title lines.

    The matched search keyword is never passed into this function. Leading
    lines are removed only when they exactly match one of the project's known
    short KPR search/topic labels and additional substantive lines remain.
    """

    raw_lines = [
        line.strip()
        for line in str(raw_text or "").splitlines()
        if line.strip()
    ]
    for i, line in enumerate(raw_lines):
        if ("populer" in normalize_text(line)) or ( "lihat aktivitas" in normalize_text(line)):
            raw_lines = raw_lines[:i]
            break

    username_normalized = username.lower().lstrip("@").strip()
    cleaned: list[str] = []

    for line in raw_lines:
        lower = normalize_text(line)

        if not lower:
            continue
        if lower in UI_NOISE_LINES:
            continue
        if username_normalized and lower.lstrip("@") == username_normalized:
            continue
        if is_time_or_metric_line(lower):
            continue
        if re.fullmatch(r"@\w+", lower):
            continue

        # Keep order, but remove exact duplicate UI fragments.
        if line not in cleaned:
            cleaned.append(line)

    # Strip up to two short leading topic/title labels, but only when real
    # content remains beneath them. This prevents a Threads topic label such
    # as "take over KPR" from proving KPR relevance by itself.
    stripped = 0
    while len(cleaned) >= 2 and stripped < 2:
        first_normalized = normalize_text(cleaned[0])
        first_word_count = len(first_normalized.split())
        later_text = normalize_text(" ".join(cleaned[1:]))

        is_known_label = first_normalized in NORMALIZED_SEARCH_LABELS
        looks_like_short_topic = (
            first_word_count <= 5
            and has_pattern(first_normalized, KPR_CONTEXT_PATTERNS)
            and len(later_text.split()) >= 5
        )

        if is_known_label or looks_like_short_topic:
            cleaned.pop(0)
            stripped += 1
        else:
            break

    return "\n".join(cleaned).strip()

def strip_reply_tail(body: str) -> str:
    """Cut a contaminated post_body at the first stranger-reply line.

    Threads renders reply authors as bare username lines. The OP's own
    replies are labelled with 'Pembuat' (ID) or 'Author' (EN) on the
    line before them, so we keep those and cut on anything else.

    Important: we must NOT cut on the OP's own username line, which
    appears near the top of the body (right after the view count).
    So we only cut once we've already seen real OP text.
    """
    if not body:
        return ""

    lines = body.split("\n")
    out: list[str] = []
    seen_op_text = False

    for i, line in enumerate(lines):
        stripped = line.strip()
        prev = lines[i - 1].strip() if i > 0 else ""

        is_bare_username = bool(
            re.fullmatch(r"@?[A-Za-z0-9_.]{3,30}", stripped)
        )
        is_metric_line = bool(
            re.fullmatch(
                r"[\d,\.]+\s*(?:rb|ribu|jt|juta|k|m)?\s*(?:tayangan|views?)",
                stripped,
                re.IGNORECASE,
            )
        )
        prev_is_author_marker = prev.lower() in ("pembuat", "author")

        # Only cut at a stranger's username line if we've already seen
        # real OP text above it.
        if (
            is_bare_username
            and not prev_is_author_marker
            and seen_op_text
        ):
            break

        if stripped.lower() in ("pembuat", "author"):
            continue

        out.append(line)

        # Mark that real OP text has appeared
        if (
            not is_bare_username
            and not is_metric_line
            and len(stripped) > 3
        ):
            seen_op_text = True

    return "\n".join(out).strip()


# ============================================================
# 7. ORDERED CLASSIFIER
# ============================================================


def has_kpr_context(text: str) -> bool:
    normalized = normalize_text(text)
    return has_pattern(normalized, KPR_CONTEXT_PATTERNS)


def detect_seller_or_ad(
    text: str,
) -> tuple[bool, str, str, int, str]:
    normalized = normalize_text(text)
    """Aggressively exclude commercial-looking KPR content.

    The filter intentionally prioritizes precision over lead volume.
    """

    raw_text = str(text or "")

    explicit_seller = pattern_matches(
        raw_text,
        EXPLICIT_SELLER_PATTERNS
        + AGGRESSIVE_PROVIDER_PATTERNS
        + PROVIDER_VOICE_PATTERNS,
    )
    direct_listing = pattern_matches(raw_text, DIRECT_LISTING_PATTERNS)
    property_details = pattern_matches(raw_text, PROPERTY_DETAIL_PATTERNS)
    promotions = pattern_matches(
        raw_text,
        PROMOTION_PATTERNS + AGGRESSIVE_PROMOTION_PATTERNS,
    )
    calls_to_action = pattern_matches(
        raw_text,
        COMMERCIAL_CALL_TO_ACTION_PATTERNS + AGGRESSIVE_CTA_PATTERNS,
    )
    seller_offers = pattern_matches(raw_text, SELLER_OFFER_PATTERNS)
    financial_details = pattern_matches(
        raw_text,
        AGGRESSIVE_FINANCIAL_PRODUCT_PATTERNS,
    )
    customer_seeking_provider = has_pattern(
        raw_text,
        CUSTOMER_SEEKING_PROVIDER_PATTERNS,
    )

    has_external_link = any(
        re.search(pattern, raw_text, flags=re.IGNORECASE)
        for pattern in EXTERNAL_LINK_PATTERNS
    )
    has_phone_number = bool(
        re.search(
            r"(?:\+62|62|0)8\d[\d\-\s]{7,}",
            raw_text,
            flags=re.IGNORECASE,
        )
    )
    hashtag_count = len(re.findall(r"(?<!\w)#[A-Za-z0-9_]+", raw_text))

    signal_groups: list[str] = []
    score = 0

    # A post that ASKS FOR a sales contact is a customer, not a seller.
    asking_for_sales = bool(re.search(
        r"\b(?:halo|hai|hi|adakah|ada|butuh|cari|nyari|mohon|tolong|saya|aku)\b"
        r".{0,40}\b(?:sales|marketing|agen|agent)\b",
        raw_text,
        re.IGNORECASE,
    ))

    if explicit_seller and not asking_for_sales:
        return (
            True,
            "Seller / Service Provider",
            "Seller, marketer, agent, developer, provider, or commercial offer language was detected.",
            10,
            "explicit seller/provider language",
        )

    if direct_listing:
        return (
            True,
            "Property Listing",
            "The post directly offers a house or unit for sale, transfer, or takeover.",
            10,
            "direct property listing",
        )

    if has_phone_number:
        score += 4
        signal_groups.append("phone/WhatsApp number")

    if has_external_link:
        score += 4
        signal_groups.append("external link")

    if property_details:
        score += min(len(property_details), 4)
        signal_groups.append(f"{len(property_details)} property-detail signal(s)")

    if promotions:
        score += min(len(promotions) * 2, 6)
        signal_groups.append(f"{len(promotions)} promotional signal(s)")

    effective_cta_count = len(calls_to_action)
    if (
        customer_seeking_provider
        and not (
            promotions
            or seller_offers
            or property_details
            or financial_details
            or has_external_link
            or has_phone_number
        )
    ):
        effective_cta_count = 0

    if effective_cta_count:
        score += min(effective_cta_count * 2, 6)
        signal_groups.append(
            f"{effective_cta_count} commercial call-to-action signal(s)"
        )

    if seller_offers:
        score += min(len(seller_offers) * 2, 6)
        signal_groups.append(f"{len(seller_offers)} seller-offer signal(s)")

    if financial_details:
        score += min(len(financial_details), 4)
        signal_groups.append(
            f"{len(financial_details)} financial-product detail signal(s)"
        )

    signal_text = " | ".join(signal_groups)

    if len(property_details) >= 2:
        return (
            True,
            "Property Listing",
            "Multiple property specifications, prices, installment details, or payment details were detected.",
            max(score, 4),
            signal_text,
        )

    if promotions and (
        calls_to_action
        or seller_offers
        or financial_details
        or property_details
        or has_phone_number
        or has_external_link
    ):
        return (
            True,
            "KPR Advertisement",
            "Promotional wording appears with contact, offer, product, phone, link, or property signals.",
            max(score, 4),
            signal_text,
        )

    if seller_offers and (
        calls_to_action
        or financial_details
        or property_details
        or promotions
    ):
        return (
            True,
            "KPR Advertisement",
            "The author offers KPR help or a product with supporting commercial language.",
            max(score, 4),
            signal_text,
        )

    if has_phone_number or has_external_link:
        return (
            True,
            "Commercial Contact Content",
            "A phone/WhatsApp number or external link was detected and excluded under the aggressive filter.",
            score,
            signal_text,
        )

    if hashtag_count >= 4 and score >= 2:
        score += 1
        signal_groups.append(f"{hashtag_count} hashtags")
        signal_text = " | ".join(signal_groups)

    if score >= 3:
        return (
            True,
            "Suspected KPR Advertisement",
            "Several commercial signals were detected. The post was excluded to keep the lead pool precise.",
            score,
            signal_text,
        )

    return False, "", "", score, signal_text


# ============================================================
# 7. GATED SCORING CLASSIFIER
# ============================================================

# A single buzzword can never make a Hot lead. Hot requires:
# 1) explicit KPR context in the actual post body,
# 2) non-commercial content,
# 3) customer-owned intent/request,
# 4) a concrete action/request, and
# 5) enough independent signal groups to clear the score threshold.

HOT_SCORE_THRESHOLD = 7
WARM_SCORE_THRESHOLD = 4
GENERAL_SCORE_THRESHOLD = 2

KPR_IMPLIED_PERSONAL_INTENT_PATTERNS = [
    r"\b(?:mau|ingin|pengen|pingin|lagi|sedang|butuh|perlu)\b.{0,55}\b(?:ajukan|mengajukan|ambil|mengambil|proses|take\s*over|takeover|refinanc|pindah bank|alih kredit)\b.{0,35}\bkpr\b",
    r"\b(?:mau|ingin|pengen|pingin|lagi|sedang|butuh|perlu)\b.{0,55}\bkpr\b.{0,40}\b(?:take\s*over|takeover|refinanc|pindah bank|ajukan|mengajukan|proses)\b",
]

KPR_IMMEDIATE_ACTION_PATTERNS = (
    HOT_PERSONAL_TAKEOVER_PATTERNS
    + [
        r"\b(?:mau|ingin|pengen|pingin|butuh|perlu)\b.{0,45}\b(?:ajukan|mengajukan|proses)\b.{0,30}\bkpr\b",
        r"\b(?:mau|ingin|pengen|pingin|butuh|perlu)\b.{0,45}\bkpr\b.{0,30}\b(?:sekarang|segera|bulan ini|minggu ini)\b",
        r"\b(?:sedang|lagi)\s+(?:mengajukan|proses)\b.{0,25}\bkpr\b",
    ]
)

KPR_DIRECT_CUSTOMER_REQUEST_PATTERNS = HOT_DIRECT_REQUEST_PATTERNS + [
    r"\b(?:tolong|mohon)\b.{0,45}\bkpr\b",
    r"\b(?:ada yang|ada yg)\b.{0,50}\b(?:rekomendasi|saran)\b.{0,35}\bkpr\b",
    r"\b(?:rekomendasi|saran)\b.{0,40}\b(?:bank|bunga|kpr)\b",
]

KPR_ACTIVE_PROBLEM_PATTERNS = (
    WARM_FLOATING_PATTERNS
    + WARM_APPLICATION_PROBLEM_PATTERNS
    + [
        r"\bkpr\b.{0,50}\b(?:berat|mahal|membengkak|kesulitan|masalah)\b",
        r"\b(?:bunga|cicilan|angsuran)\b.{0,35}\bkpr\b.{0,35}\b(?:naik|berat|mahal|tinggi)\b",
        # "angsuran kpr dari X jadi Y" — concrete increase story
        r"\b(?:bunga|cicilan|angsuran)\b.{0,25}\bkpr\b.{0,60}\b(?:jadi|naik|sampai)\s*(?:rp)?\s*\d",
        # KPR pain verbs
        r"\bkpr\b.{0,60}\b(?:nangis|sedih|pusing|stres|stress|kenaikan|lonjakan|bengkak)\b",
        r"\bkpr\b.{0,50}\b(?:muak|nyerah|capek|ga\s*sanggup|gak\s*sanggup|udah\s*ga\s*tahan)\b",
        r"\b(?:sisa|outstanding)\s*kpr\b.{0,40}\b(?:ga\s*sanggup|gak\s*sanggup|berat)\b",
    ]
)

KPR_PLANNING_PATTERNS = WARM_NEW_KPR_PATTERNS + [
    r"\b(?:rencana|berencana|mempertimbangkan|kepikiran)\b.{0,45}\bkpr\b",
]

KPR_RESEARCH_PATTERNS = WARM_COMPARISON_REQUIREMENT_PATTERNS + GENERAL_KPR_INTEREST_PATTERNS

KPR_URGENCY_EXTRA_PATTERNS = HOT_URGENCY_PATTERNS + [
    r"\b(?:hari ini|sekarang|secepatnya|dalam waktu dekat|tahun ini)\b.{0,45}\bkpr\b",
    r"\bkpr\b.{0,45}\b(?:hari ini|sekarang|secepatnya|dalam waktu dekat|tahun ini)\b",
]

KPR_THIRD_PERSON_OR_GENERIC_PATTERNS = [
    r"\bteman (?:saya|aku|gue|gw)\b",
    r"\bsaudara (?:saya|aku)\b",
    r"\borang-orang\b",
    r"\bnasabah biasanya\b",
    r"\bmenurut berita\b",
    r"\bberdasarkan berita\b",
    r"\bcontoh kasus\b",
]


def _signal_text(items: list[str]) -> str:
    return " | ".join(items)



def score_replies(reply_items: list[dict[str, Any]]) -> dict[str, Any]:
    """Score replies with author-awareness.

    - OP follow-up posts (multi-part threads 1/6, 2/6, ...) are NOT replies.
    - Seller replies are excluded entirely from the conversation bonus.
    - Only genuine customer replies can add relevance / productivity bonuses.
    - OP replying back into their own thread = continuity signal.
    """
    op_replies = [r for r in reply_items if r.get("is_op")]
    other_replies = [r for r in reply_items if not r.get("is_op")]

    customer_replies: list[dict[str, Any]] = []
    seller_replies: list[dict[str, Any]] = []

    for r in other_replies:
        text = str(r.get("text", ""))
        is_commercial, *_ = detect_seller_or_ad(text)
        (seller_replies if is_commercial else customer_replies).append(r)

    relevant: list[dict[str, Any]] = []
    productive: list[dict[str, Any]] = []
    for r in customer_replies:
        n = normalize_text(r.get("text", ""))
        if has_pattern(n, REPLY_RELEVANCE_PATTERNS):
            relevant.append(r)
            if has_pattern(n, PRODUCTIVE_REPLY_PATTERNS):
                productive.append(r)
        if (len(relevant) >= MAX_RELEVANT_REPLIES
                and len(productive) >= MAX_PRODUCTIVE_REPLIES):
            break

    conversation_bonus = min(
        len(relevant) * REPLY_RELEVANCE_BONUS, MAX_CONVERSATION_BONUS
    )
    productive_bonus = min(
        len(productive) * PRODUCTIVE_REPLY_BONUS, MAX_PRODUCTIVE_BONUS
    )
    continuity_bonus = CONTINUITY_BONUS if op_replies else 0

    # Multi-part threads (OP posts 1/N, 2/N) show up in the DOM as
    # "replies" but they are the same author continuing their post.
    # Give a small credit for long-form posts, capped at +2.
    op_followup_bonus = min(len(op_replies), 2)

    total = min(
        conversation_bonus
        + productive_bonus
        + continuity_bonus
        + op_followup_bonus,
        MAX_TOTAL_REPLY_BONUS,
    )

    # External replies only. Excludes OP's own follow-up posts.
    external_reply_count = len(customer_replies) + len(seller_replies)

    return {
        "reply_count": external_reply_count,
        "total_thread_entries": len(reply_items),
        "customer_reply_count": len(customer_replies),
        "seller_reply_count": len(seller_replies),
        "op_reply_count": len(op_replies),
        "relevant_reply_count": len(relevant),
        "productive_reply_count": len(productive),
        "op_engaged": bool(op_replies),
        "conversation_bonus": conversation_bonus,
        "productive_bonus": productive_bonus,
        "continuity_bonus": continuity_bonus,
        "op_followup_bonus": op_followup_bonus,
        "total_conversation_bonus": total,
        "relevant_reply_text": " || ".join(r["text"] for r in relevant),
        "productive_reply_text": " || ".join(r["text"] for r in productive),
    }


def score_kpr_lead(text: str) -> dict[str, Any]:
    normalized = normalize_text(text)
    analysis = analyze_kpr_subject(text)

    fp = analysis["first_person_kpr_sentences"]
    sp = analysis["second_person_kpr_sentences"]
    tp = analysis["third_person_kpr_sentences"]
    fp_past = analysis["first_person_past_kpr_sentences"]
    teaching = analysis["has_teaching_phrases"]

    # Pull in the existing per-signal matches
    personal = has_pattern(normalized, PERSONAL_CONTEXT_PATTERNS)
    implied_personal = has_pattern(normalized, KPR_IMPLIED_PERSONAL_INTENT_PATTERNS)
    immediate_action = has_pattern(normalized, KPR_IMMEDIATE_ACTION_PATTERNS)
    direct_request = has_pattern(normalized, KPR_DIRECT_CUSTOMER_REQUEST_PATTERNS)
    urgency = has_pattern(normalized, KPR_URGENCY_EXTRA_PATTERNS)
    active_problem = has_pattern(normalized, KPR_ACTIVE_PROBLEM_PATTERNS)
    planning = has_pattern(normalized, KPR_PLANNING_PATTERNS)
    research = has_pattern(normalized, KPR_RESEARCH_PATTERNS)
    question = "?" in str(text)

    score = 1
    positive: list[str] = ["explicit KPR context +1"]
    negative: list[str] = []

    # ---- SUBJECT-AWARE GATES (the new part) ----
    #
    # If the author talks about KPR only in the second or third person,
    # they are teaching or telling a story. No personal signals count.
    #
    # If the author talks about KPR in the past tense only, they have
    # already done the thing — no active need.

    subject_is_third = (fp == 0 and sp == 0 and tp >= 1)
    subject_is_second = (fp == 0 and sp >= 1)
    subject_is_first = (fp >= 1)

    if subject_is_third:
        negative.append(f"KPR discussed in third person only (-6)")
        score -= 6
    elif subject_is_second:
        negative.append(f"KPR addressed to second person only (-6)")
        score -= 6

    if teaching and not subject_is_first:
        negative.append("teaching-content voice (-3)")
        score -= 3

    # Only allow personal / implied / planning signals if we found at
    # least one first-person KPR clause.
    if not subject_is_first:
        personal = False
        implied_personal = False
        planning = False
        immediate_action = False
        direct_request = False
        # Active problem and research can still fire because they may
        # apply to a past-tense KPR ("KPR-nya sudah floating").

    # ---- End of subject gates ----

    if personal:
        score += 2
        positive.append("personal KPR context +2")
    elif implied_personal:
        score += 2
        positive.append("implied first-person KPR intent +2")

    if immediate_action:
        score += 5
        positive.append("concrete KPR action +5")

    if direct_request:
        score += 5
        positive.append("direct customer request +5")

    if urgency:
        score += 2
        positive.append("urgency/timing +2")

    if active_problem:
        score += 2
        positive.append("active KPR problem +2")

    if planning:
        score += 3
        positive.append("planned KPR action +3")

    if research:
        score += 1
        positive.append("comparison/research +1")

    if question and (personal or implied_personal or direct_request or research):
        score += 1
        positive.append("direct question +1")

    if has_pattern(normalized, NARRATIVE_MARKERS):
        score -= 4
        negative.append("narrative/experience-sharing voice -4")    

    # Past-tense KPR regret: the author has already taken and regrets
    # their KPR. They are not a new lead.
    kpr_regret = has_pattern(normalized, KPR_REGRET_PATTERNS)
    if kpr_regret:
        score -= 6
        negative.append("KPR regret (past-tense complaint) -6")

    if has_pattern(normalized, POLL_PROMPT_PATTERNS):
        score -= 5
        negative.append("poll/prompt to others -5")

    if has_pattern(normalized, HYPOTHETICAL_PATTERNS):
        score -= 4
        negative.append("hypothetical framing -4")

    # Past-tense-only first-person KPR is a solved case.
    if fp_past >= 1 and fp_past == fp and not active_problem:
        score -= 3
        negative.append("only past-tense first-person KPR (-3)")


        # Narrative / experience-sharing voice. Posts that frame themselves
    # as "let me tell you about my KPR experience" are not leads.
    if has_pattern(normalized, NARRATIVE_MARKERS):
        score -= 4
        negative.append("narrative/experience-sharing voice -4")
    customer_owned = personal or implied_personal or direct_request
    hot_action_gate = immediate_action or direct_request
    hot_gate = customer_owned and hot_action_gate

    warm_gate = (
        customer_owned
        or active_problem
        or (planning and question)
    )

    meaningful_signal = any(
        [
            personal,
            implied_personal,
            immediate_action,
            direct_request,
            active_problem,
            planning,
            research,
        ]
    )

    return {
        "lead_score": score,
        "positive_signals": _signal_text(positive),
        "negative_signals": _signal_text(negative),
        "hot_gate_passed": hot_gate,
        "warm_gate_passed": warm_gate,
        "meaningful_signal": meaningful_signal,
        "personal_signal": personal or implied_personal,
        "immediate_action_signal": immediate_action,
        "direct_request_signal": direct_request,
        "active_problem_signal": active_problem,
        "planning_signal": planning,
        "research_signal": research,
        # New: expose for debugging
        "first_person_kpr_sentences": fp,
        "second_person_kpr_sentences": sp,
        "third_person_kpr_sentences": tp,
    }


def classify_post(
    text: str,
    reply_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """KPR context -> ad exclusion -> gated score -> Hot/Warm/General."""

    reply_items = reply_items or []
    reply_analysis = score_replies(reply_items)

    base: dict[str, Any] = {
        "kpr_context": False,
        "intent_track": "",
        "lead_category": "Irrelevant",
        "lead_priority": 0,
        "lead_type": "",
        "classification_reason": "",
        "commercial_type": "",
        "commercial_score": 0,
        "ad_signals": "",
        "lead_score": 0,
        "lead_confidence": "None",
        "positive_signals": "",
        "negative_signals": "",
        "hot_gate_passed": False,
        "manual_review_needed": False,
        "reply_count": reply_analysis["reply_count"],
        "total_thread_entries": reply_analysis["total_thread_entries"],     # NEW
        "customer_reply_count": reply_analysis["customer_reply_count"],
        "seller_reply_count": reply_analysis["seller_reply_count"],
        "op_reply_count": reply_analysis["op_reply_count"],
        "relevant_reply_count": reply_analysis["relevant_reply_count"],
        "productive_reply_count": reply_analysis["productive_reply_count"],
        "op_engaged": reply_analysis["op_engaged"],
        "conversation_bonus": reply_analysis["conversation_bonus"],
        "productive_conversation_bonus": reply_analysis["productive_bonus"],
        "continuity_bonus": reply_analysis["continuity_bonus"],
        "op_followup_bonus": reply_analysis["op_followup_bonus"],         # NEW
        "relevant_reply_text": reply_analysis["relevant_reply_text"],
        "productive_reply_text": reply_analysis["productive_reply_text"],
    }

    if not has_kpr_context(text):
        return {
            **base,
            "classification_reason": (
                "The cleaned original post body does not contain explicit KPR context."
            ),
        }

    base["kpr_context"] = True
    base["intent_track"] = INTENT_TRACK

    (
        is_commercial,
        commercial_type,
        commercial_reason,
        commercial_score,
        ad_signals,
    ) = detect_seller_or_ad(text)

    base["commercial_score"] = commercial_score
    base["ad_signals"] = ad_signals

    if is_commercial:
        return {
            **base,
            "lead_category": "Seller or advertisement",
            "lead_priority": -1,
            "lead_type": "Excluded commercial KPR content",
            "commercial_type": commercial_type,
            "lead_confidence": "Excluded",
            "classification_reason": commercial_reason,
        }

    scored = score_kpr_lead(text)

    if scored["meaningful_signal"]:
        scored["lead_score"] += reply_analysis["total_conversation_bonus"]
        if reply_analysis["productive_bonus"] > 0:
            scored["positive_signals"] += (
                f" | productive replies +{reply_analysis['productive_bonus']}"
            )
        if reply_analysis["continuity_bonus"] > 0:
            scored["positive_signals"] += (
                f" | OP engaged +{reply_analysis['continuity_bonus']}"
            )

    base["lead_score"] = scored["lead_score"]
    base["positive_signals"] = scored["positive_signals"]
    base["negative_signals"] = scored["negative_signals"]
    base["hot_gate_passed"] = scored["hot_gate_passed"]

    if (
        scored["hot_gate_passed"]
        and scored["lead_score"] >= HOT_SCORE_THRESHOLD
    ):
        if scored["direct_request_signal"] and scored["immediate_action_signal"]:
            lead_type = "Immediate KPR action and direct request"
        elif scored["immediate_action_signal"]:
            lead_type = "Immediate KPR action"
        else:
            lead_type = "Direct KPR customer request"

        confidence = "Very High" if scored["lead_score"] >= 11 else "High"
        return {
            **base,
            "lead_category": "Hot research lead",
            "lead_priority": 3,
            "lead_type": lead_type,
            "lead_confidence": confidence,
            "classification_reason": (
                f"Hot gate passed with score {scored['lead_score']}: "
                "explicit KPR context plus customer-owned action/request."
            ),
            "manual_review_needed": True,
        }

    if (
        scored["warm_gate_passed"]
        and scored["lead_score"] >= WARM_SCORE_THRESHOLD
    ):
        if scored["active_problem_signal"]:
            lead_type = "Personal KPR problem or pain point"
        elif scored["planning_signal"]:
            lead_type = "Planned KPR action"
        else:
            lead_type = "Active KPR research or comparison"

        confidence = "High" if scored["lead_score"] >= 8 else "Medium"
        return {
            **base,
            "lead_category": "Warm research lead",
            "lead_priority": 2,
            "lead_type": lead_type,
            "lead_confidence": confidence,
            "classification_reason": (
                f"Warm gate passed with score {scored['lead_score']}, "
                "but the required Hot action/request gate was not strong enough."
            ),
            "manual_review_needed": True,
        }

    if (
        scored["meaningful_signal"]
        and scored["lead_score"] >= GENERAL_SCORE_THRESHOLD
    ):
        return {
            **base,
            "lead_category": "General KPR discussion",
            "lead_priority": 1,
            "lead_type": "Lower-intent KPR discussion",
            "lead_confidence": "Low",
            "classification_reason": (
                f"The post has genuine KPR context but only lower-intent "
                f"signals (score {scored['lead_score']})."
            ),
            "manual_review_needed": True,
        }

    return {
        **base,
        "classification_reason": (
            f"The post mentions KPR but lacks enough independent "
            f"customer-intent signals (score {scored['lead_score']})."
        ),
    }

# ============================================================
# 8. STATE MANAGEMENT
# ============================================================


def load_processed_posts() -> dict[str, dict[str, Any]]:
    if not PROCESSED_POSTS_FILE.exists():
        return {}
    print(f"Loading from: {PROCESSED_POSTS_FILE}")
    data = pd.read_csv(PROCESSED_POSTS_FILE).fillna("")
    records: dict[str, dict[str, Any]] = {}

    for record in data.to_dict("records"):
        link = canonicalize_post_url(record.get("post_link", ""))
        if link:
            record["post_link"] = link
            records[link] = record

    return records


def save_processed_posts(records: dict[str, dict[str, Any]]) -> None:
    if records:
        print(f"Saving to: {PROCESSED_POSTS_FILE}")
        pd.DataFrame(list(records.values())).to_csv(
            PROCESSED_POSTS_FILE,
            index=False,
            encoding="utf-8-sig",
        )
def save_worker_checkpoint(
    worker_id: int,
    records: dict[str, dict[str, Any]],
) -> None:

    if not records:
        return

    checkpoint_file = (
        CHECKPOINT_FOLDER /
        f"worker_{worker_id}.csv"
    )
    
    print(f"Checkpoint saved: {checkpoint_file}")

    pd.DataFrame(
        list(records.values())
    ).to_csv(
        checkpoint_file,
        index=False,
        encoding="utf-8-sig",
    )

def load_worker_checkpoints() -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}

    for checkpoint_file in CHECKPOINT_FOLDER.glob("worker_*.csv"):

        if not checkpoint_file.exists():
            continue

        data = pd.read_csv(
            checkpoint_file
        ).fillna("")

        for record in data.to_dict("records"):

            link = canonicalize_post_url(
                record.get("post_link", "")
            )

            if link:
                record["post_link"] = link
                records[link] = record

    return records

def load_keyword_progress() -> dict[str, dict[str, Any]]:
    if not KEYWORD_PROGRESS_FILE.exists():
        return {}

    print(f"Loading from: {KEYWORD_PROGRESS_FILE}")

    data = pd.read_csv(KEYWORD_PROGRESS_FILE).fillna("")

    progress: dict[str, dict[str, Any]] = {}

    for _, row in data.iterrows():
        keyword = str(row.get("keyword", "")).strip()

        if not keyword:
            continue

        status = str(row.get("status", "")).strip()

        try:
            post_count = int(row.get("post_count", 0))
        except (ValueError, TypeError):
            post_count = 0

        progress[keyword] = {
            "status": status,
            "post_count": post_count,
        }

    return progress


def save_keyword_progress(
    progress: dict[str, dict[str, Any]]
) -> None:
    rows = [
        {
            "keyword": keyword,
            "status": data.get("status", ""),
            "post_count": data.get("post_count", 0),
        }
        for keyword, data in progress.items()
    ]

    pd.DataFrame(rows).to_csv(
        KEYWORD_PROGRESS_FILE,
        index=False,
        encoding="utf-8-sig",
    )

def merge_keyword(existing: Any, keyword: str) -> str:
    keywords = {
        item.strip()
        for item in str(existing or "").split("|")
        if item.strip()
    }
    if keyword.strip():
        keywords.add(keyword.strip())
    return " | ".join(sorted(keywords))


# ============================================================
# 9. THREADS PAGE HELPERS
# ============================================================


def save_debug_files(page: Page, prefix: str) -> None:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", prefix)[:80]

    try:
        page.screenshot(
            path=str(DEBUG_FOLDER / f"{safe}.png"),
            full_page=True,
        )
    except Exception:
        pass

    try:
        (DEBUG_FOLDER / f"{safe}.html").write_text(
            page.content(),
            encoding="utf-8",
        )
    except Exception:
        pass


def try_select_recent_results(page: Page) -> None:
    """Click the 'Terbaru' (Recent) sort link.

    Threads renders this as <a role="link" aria-label="Terbaru">.
    It only appears after the search results have rendered, so this
    function must be called AFTER wait_for_initial_search_state().
    """
    # Wait for Threads' splash screen to disappear. It overlays the
    # sort tab and blocks the click.
    try:
        page.wait_for_function(
            "() => !document.querySelector('#barcelona-splash-screen')",
            timeout=6000,
        )
    except Exception:
        pass

    # Primary: exact aria-label Threads uses today.
    try:
        locator = page.locator(
            'a[aria-label="Terbaru"], a[aria-label="Recent"]'
        ).first
        locator.click(timeout=8000)
        page.wait_for_timeout(1500)  # let the re-sorted results settle
        print("  [SORT] Clicked 'Terbaru' / 'Recent' tab.")
        return
    except Exception as e:
        print(f"  [SORT] Primary selector failed: {e}")

    # Fallback: any link whose accessible name is exactly Terbaru / Recent.
    for role in ("link", "button", "tab", "menuitem"):
        try:
            locator = page.get_by_role(
                role,
                name=re.compile(r"^(terbaru|recent)$", re.IGNORECASE),
            )
            if locator.count() > 0:
                locator.first.click(timeout=3000)
                page.wait_for_timeout(1500)
                print(f"  [SORT] Clicked via role={role}.")
                return
        except Exception:
            continue

    print("  [SORT] WARNING: Could not click 'Recent'. "
          "Falling back to relevance sort.")

def dump_sort_candidates(page: Page) -> None:
    """Print every clickable element whose text/aria-label mentions
    a sort-related word, so we can find the real 'Recent' selector."""
    try:
        candidates = page.evaluate(
            r"""
            () => {
              const keywords = [
                'recent','terbaru','latest','newest','top','teratas',
                'populer','popular','relevan','relevance','urutkan','sort',
              ];
              const out = [];
              const nodes = document.querySelectorAll(
                'button, [role="tab"], [role="button"], [role="menuitem"], a'
              );
              for (const n of nodes) {
                const text = (n.innerText || '').trim();
                const aria = (n.getAttribute('aria-label') || '').trim();
                const role = n.getAttribute('role') || '';
                const combined = (text + ' ' + aria).toLowerCase();
                if (!combined) continue;
                if (!keywords.some(k => combined.includes(k))) continue;
                out.push({
                  tag: n.tagName,
                  role,
                  text: text.slice(0, 80),
                  aria: aria.slice(0, 80),
                });
              }
              return out;
            }
            """
        )
        if not candidates:
            print("  [SORT-DUMP] No sort-related clickable elements found.")
            return
        print(f"  [SORT-DUMP] {len(candidates)} candidate(s):")
        for c in candidates[:15]:
            print(
                f"    <{c['tag']}> role={c['role']!r} "
                f"text={c['text']!r} aria={c['aria']!r}"
            )
    except Exception as e:
        print(f"  [SORT-DUMP] failed: {e}")


def wait_for_initial_search_state(page: Page) -> None:
    try:
        page.wait_for_function(
            """
            () => {
              const links = document.querySelectorAll('a[href*="/post/"]');
              const body = (document.body.innerText || '').toLowerCase();
              const emptyState =
                body.includes('no results') ||
                body.includes('tidak ada hasil') ||
                body.includes('coba cari');
              return links.length > 0 || emptyState;
            }
            """,
            timeout=SEARCH_STATE_CHANGE_TIMEOUT_MS,
        )
    except PlaywrightTimeoutError:
        # The caller can still inspect and save a debug page.
        pass


def extract_loaded_post_links(page: Page, keyword: str) -> list[dict[str, str]]:
    return page.evaluate(
        r"""
        (keyword) => {
          const output = [];
          const seen = new Set();
          const anchors = Array.from(
            document.querySelectorAll('a[href*="/post/"]')
          );

          for (const anchor of anchors) {
            const rawHref = anchor.href || anchor.getAttribute('href') || '';
            if (!rawHref.includes('/post/')) continue;

            let url;
            try {
              url = new URL(rawHref, window.location.origin);
            } catch {
              continue;
            }

            url.search = '';
            url.hash = '';
            url.pathname = url.pathname
              .replace(/\/media\/?$/i, '')
              .replace(/\/$/, '');

            const canonical = url.toString();
            if (seen.has(canonical)) continue;
            seen.add(canonical);

            const match = canonical.match(
              /\/@([^/]+)\/post\/([^/?#]+)/
            );

            output.push({
              post_link: canonical,
              username_hint: match ? match[1] : '',
              post_id_hint: match ? match[2] : canonical,
              matched_keyword: keyword,
            });
          }

          return output;
        }
        """,
        keyword,
    )


def search_page_state(page: Page) -> dict[str, int]:
    return page.evaluate(
        r"""
        () => {
          const links = new Set();
          document.querySelectorAll('a[href*="/post/"]').forEach(anchor => {
            try {
              const url = new URL(anchor.href, window.location.origin);
              url.search = '';
              url.hash = '';
              url.pathname = url.pathname
                .replace(/\/media\/?$/i, '')
                .replace(/\/$/, '');
              links.add(url.toString());
            } catch {}
          });

          return {
            link_count: links.size,
            scroll_height: document.documentElement.scrollHeight,
            scroll_y: Math.round(window.scrollY),
          };
        }
        """
    )


def scroll_after_current_results_are_read(page: Page) -> None:
    page.evaluate(
        r"""
        () => {
          const anchors = Array.from(
            document.querySelectorAll('a[href*="/post/"]')
          );

          if (anchors.length > 0) {
            anchors[anchors.length - 1].scrollIntoView({
              behavior: 'instant',
              block: 'end'
            });
          }

          window.scrollTo({
            top: document.documentElement.scrollHeight,
            behavior: 'instant'
          });
        }
        """
    )


def wait_for_search_state_change(
    page: Page,
    previous_state: dict[str, int],
) -> bool:
    """Wait for actual new content or page growth after a completed scroll."""

    try:
        page.wait_for_function(
            r"""
            (previous) => {
              const links = new Set();
              document.querySelectorAll('a[href*="/post/"]').forEach(anchor => {
                try {
                  const url = new URL(anchor.href, window.location.origin);
                  url.search = '';
                  url.hash = '';
                  url.pathname = url.pathname
                    .replace(/\/media\/?$/i, '')
                    .replace(/\/$/, '');
                  links.add(url.toString());
                } catch {}
              });

              const height = document.documentElement.scrollHeight;
              return (
                links.size > previous.link_count ||
                height > previous.scroll_height
              );
            }
            """,
            arg=previous_state,
            timeout=SEARCH_STATE_CHANGE_TIMEOUT_MS,
        )
        return True
    except PlaywrightTimeoutError:
        return False


def wait_for_main_post_container(page: Page, post_link: str) -> None:
    try:
        page.wait_for_function(
            r"""
            (postLink) => {
              const targetPath = new URL(postLink).pathname
                .replace(/\/media\/?$/i, '')
                .replace(/\/$/, '');

              const canonicalPath = (href) => {
                try {
                  return new URL(href, window.location.origin).pathname
                    .replace(/\/media\/?$/i, '')
                    .replace(/\/$/, '');
                } catch {
                  return '';
                }
              };

              const anchors = Array.from(
                document.querySelectorAll('a[href*="/post/"]')
              );
              const matchingAnchor = anchors.find(
                anchor => canonicalPath(anchor.href) === targetPath
              );

              if (!matchingAnchor) return false;

              let node = matchingAnchor;
              for (let level = 0; level < 14 && node; level += 1) {
                const text = (node.innerText || '').trim();
                if (text.length >= 3 && node.querySelector('time')) {
                  return true;
                }
                node = node.parentElement;
              }

              return false;
            }
            """,
            arg=post_link,
            timeout=POST_READY_TIMEOUT_MS,
        )
    except PlaywrightTimeoutError:
        pass


def extract_main_post_body(page: Page, post_link: str) -> dict[str, str]:
    """Extract the main post only, not the search page heading or replies."""

    return page.evaluate(
        r"""
        (postLink) => {
          const targetPath = new URL(postLink).pathname
            .replace(/\/media\/?$/i, '')
            .replace(/\/$/, '');

          const canonicalPath = (href) => {
            try {
              return new URL(href, window.location.origin).pathname
                .replace(/\/media\/?$/i, '')
                .replace(/\/$/, '');
            } catch {
              return '';
            }
          };

          const anchors = Array.from(
            document.querySelectorAll('a[href*="/post/"]')
          );    
          
          function findPostRoot(matchingAnchor) {
              if (!matchingAnchor) {
                  return null;
              }

              let node = matchingAnchor;

              while (node && node !== document.body) {
                  const text = (node.innerText || "").trim();
                  const hasTime = node.querySelector && node.querySelector("time");
                  const timeCount = node.querySelectorAll
                      ? node.querySelectorAll("time").length
                      : 0;
                  const postLinks = node.querySelectorAll
                      ? node.querySelectorAll('a[href*="/post/"]')
                      : [];
                  const matchingLinks = Array.from(postLinks).filter(
                      anchor => canonicalPath(anchor.href) === targetPath
                  );

                  // Exactly ONE <time> means we've isolated the OP card.
                  // Two or more means replies are inside — i.e. this is
                  // the thread container, keep climbing.
                  if (
                      hasTime &&
                      timeCount >= 1 &&
                      matchingLinks.length >= 1 &&
                      text.length > 20 &&
                      text.length < 8000
                  ) {
                      return node;
                  }

                  node = node.parentElement;
              }

              return null;
          }

          const matchingAnchor = anchors.find(
            anchor => canonicalPath(anchor.href) === targetPath
          );

          let container = findPostRoot(matchingAnchor);
          

          if (!container) {
            const candidates = Array.from(
              document.querySelectorAll('article, [role="article"]')
            );

            container = candidates.find(candidate =>
              Array.from(
                candidate.querySelectorAll('a[href*="/post/"]')
              ).some(anchor => canonicalPath(anchor.href) === targetPath)
            ) || null;
          }
          

          if (!container && matchingAnchor) {
            let node = matchingAnchor;
            for (let level = 0; level < 14 && node; level += 1) {
              const text = (node.innerText || '').trim();
              if (
                node.querySelector &&
                node.querySelector('time') &&
                text.length >= 3 &&
                text.length <= 8000
              ) {
                container = node;
                break;
              }
              node = node.parentElement;
            }
          }
                  
          

          if (!container) {
            return {
              body_text: '',
              raw_main_container: '',
              username: '',
              timestamp: '',
              extraction_status: 'main_post_container_not_found',
            };
          }

          const usernameMatch = targetPath.match(/\/@([^/]+)\/post\//);
          const username = usernameMatch ? usernameMatch[1] : '';

          const timeElement = container.querySelector('time');
          const timestamp = timeElement
            ? (
                timeElement.getAttribute('datetime') ||
                timeElement.getAttribute('title') ||
                timeElement.innerText ||
                ''
              )
            : '';

          const rawMainContainer = (container.innerText || '').trim();
          const pagelet = container.querySelector(
              '[data-pagelet="threads_post_page_0"]'
              );

          const postBlocks = pagelet
              ? Array.from(pagelet.children)
              : [];
          let selectedPost = null;

          for (const block of postBlocks) {
                  const links = Array.from(
                      block.querySelectorAll('a[href*="/post/"]')
                      );

                  if (
                          links.some(
                              anchor => canonicalPath(anchor.href) === targetPath
                              )
                          ) {
                              selectedPost = block;
                              break;
                              }
        }

        const clone = (
            selectedPost || container
            ).cloneNode(true);
        
         
          clone.querySelectorAll(
            'button, svg, time, [role="button"]'
          ).forEach(node => node.remove());

          const bodyNodes = Array.from(
            clone.querySelectorAll('div[dir="auto"], span[dir="auto"]')
          );
          
          const parts = [];
          const seen = new Set();

          for (const node of bodyNodes) {
            const text = (node.innerText || '').trim();
            if (!text || text.length < 2) continue;

            const lower = text.toLowerCase();
            if (username && lower.replace(/^@/, '') === username.toLowerCase()) {
              continue;
            }

            if (
              /^(like|reply|repost|share|translate|follow|more|suka|balas|bagikan|ikuti|terjemahkan)$/i.test(text)
            ) {
              continue;
            }

            if (/^\d+[smhdw]$/i.test(text)) continue;
            if (/^\d+(?:[.,]\d+)?[km]?$/i.test(text)) continue;

            // Skip a child whose exact text is already contained in a larger
            // selected block, and skip exact duplicates.
            if (!seen.has(text)) {
              seen.add(text);
              parts.push(text);
            }
          }

          let bodyText = parts.join('\n').trim();

          if (!bodyText) {
            bodyText = (clone.innerText || rawMainContainer).trim();
          }
          return {
            body_text: bodyText,
            raw_main_container: rawMainContainer,
            username,
            timestamp,
            extraction_status: 'ok',
        };
        }
        """,
        post_link,
        
    )

def extract_reply_items(page: Page, post_link: str, op_username: str = "") -> list[dict[str, Any]]:
    """Structured reply extraction. Returns author + text + is_op flag."""
    raw = page.evaluate(
        r"""
        (postLink) => {
          const clean = v => (v || '').replace(/\u00a0/g, ' ').trim();
          const canonicalPath = (href) => {
            try {
              return new URL(href, window.location.origin).pathname
                .replace(/\/media\/?$/i, '').replace(/\/$/, '');
            } catch { return ''; }
          };
          const targetPath = canonicalPath(postLink);
          if (!targetPath) return [];

          const UI_NOISE_EXACT = new Set([
            'like','reply','repost','share','translate','follow','more',
            'suka','balas','bagikan','ikuti','terjemahkan',
            'view','lihat','activity','aktivitas','view activity','lihat aktivitas',
            'view more','show more','see more','load more','read more',
            'view replies','lihat balasan','lihat balasan lainnya',
            'see more replies','see translation','lihat terjemahan',
            'new thread','utas baru','thread','for you','untuk anda',
            'top','teratas','recent','terbaru','follow back','ikuti balik',
            '·','/','\\','|','-','—','–','mute','unmute','report','block',
            'copy link','salin tautan','not interested','tidak tertarik',
          ]);
          const METRIC_PATTERN = /^(?:\d+(?:[.,]\d+)?\s*(?:rb|ribu|jt|juta|k|m|views?|tayangan|likes?|suka|replies?|balasan|reply|balas)?|\d+\s*(?:s|m|h|d|w|menit|jam|hari|minggu|bulan|tahun)|\d+\s*\/\s*\d+|\d+|\/|·)$/i;
          const USERNAME_PATTERN = /^@[\w.]+$/;
          const REPLY_TO_PATTERN = /^(?:reply to|balas ke)\b/i;
          const HASHTAG_ONLY     = /^(?:#[A-Za-z0-9_]+)(?:\s+#[A-Za-z0-9_]+)*$/;

          const isNoise = (text) => {
            const t = clean(text);
            if (!t || t.length < 2) return true;
            const l = t.toLowerCase();
            if (UI_NOISE_EXACT.has(l)) return true;
            if (METRIC_PATTERN.test(t)) return true;
            if (USERNAME_PATTERN.test(t)) return true;
            if (REPLY_TO_PATTERN.test(t)) return true;
            if (HASHTAG_ONLY.test(t)) return true;
            return false;
          };

          const isMainPostCard = (node) => {
            for (const a of node.querySelectorAll('a[href*="/post/"]')) {
              if (canonicalPath(a.href) === targetPath) return true;
            }
            return false;
          };

          const findCard = (timeEl) => {
            let node = timeEl;
            for (let lvl = 0; lvl < 18; lvl += 1) {
              node = node.parentElement;
              if (!node || node === document.body) break;
              const t = clean(node.innerText);
              if (t.length < 5) continue;
              if (t.length > 4000) break;
              if (node.querySelectorAll('button, [role="button"]').length >= 2) return node;
            }
            return null;
          };

          const findAuthor = (card) => {
            // Threads reply cards contain a link to the author's profile.
            const link = card.querySelector('a[href^="/@"]');
            if (!link) return '';
            const m = (link.getAttribute('href') || '').match(/^\/@([^/?#]+)/);
            return m ? m[1] : '';
          };

          const findPermalink = (card) => {
            const link = card.querySelector('a[href*="/post/"]');
            if (!link) return '';
            try {
              const u = new URL(link.href, window.location.origin);
              u.search = ''; u.hash = '';
              u.pathname = u.pathname.replace(/\/media\/?$/i, '').replace(/\/$/, '');
              return u.toString();
            } catch { return ''; }
          };

          const results = [];
          const seenCards = new Set();
          const seenTexts = new Set();

          // Collect ALL post cards (in thread + in "rest of thread" section below).
          // Threads sometimes renders parts 6/6, 7/7 etc. below the reply box.
          const allTimes = Array.from(document.querySelectorAll('time'));

          for (const timeEl of allTimes) {
            const card = findCard(timeEl);
            if (!card || seenCards.has(card)) continue;
            seenCards.add(card);

            // Skip the main post card (the one whose URL matches postLink).
            if (isMainPostCard(card)) continue;

            const leaves = Array.from(
              card.querySelectorAll('div[dir="auto"], span[dir="auto"]')
            ).filter(n => !n.querySelector('div[dir="auto"], span[dir="auto"]'));

            const parts = [];
            const localSeen = new Set();
            for (const leaf of leaves) {
              const t = clean(leaf.innerText);
              if (isNoise(t) || localSeen.has(t)) continue;
              localSeen.add(t);
              parts.push(t);
            }
            const text = parts.join(' ').trim();
            if (text.length < 5 || seenTexts.has(text)) continue;
            seenTexts.add(text);

            results.push({
              text,
              username: findAuthor(card),
              permalink: findPermalink(card),
            });
          }
          return results;
        }
        """,
        post_link,
    )

    op_lower = (op_username or "").lower().lstrip("@")
    for item in raw:
        u = (item.get("username") or "").lower().lstrip("@")
        item["is_op"] = bool(op_lower and u == op_lower)
    return raw

def wait_for_any_replies(page: Page, post_link: str, timeout_ms: int = 10000) -> None:
    """Wait until the DOM contains more than one <time> element.

    The main post contributes exactly one <time>. Every reply adds another.
    Two or more <time> elements therefore means at least one reply has been
    rendered. This is far more reliable than waiting on /post/ links, which
    replies frequently do not carry.
    """

    try:
        page.wait_for_function(
            "() => document.querySelectorAll('time').length >= 2",
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError:
        pass

def expand_nested_replies(
    page: Page,
    max_rounds: int = 3,
    max_clicks_per_round: int = 10,
) -> int:
    """Click every 'Tampilkan balasan' / 'View N replies' button."""
    total = 0
    for round_num in range(max_rounds):
        try:
            clicked = page.evaluate(
                r"""
                ({ maxClicks }) => {
                  const patterns = [
                    /tampilkan\s+balasan/i,
                    /lihat\s+balasan/i,
                    /view\s+replies/i,
                    /view\s+\d+\s+replies/i,
                    /lihat\s+\d+\s+balasan/i,
                  ];
                  const candidates = document.querySelectorAll(
                    'div[role="button"], span[role="button"], button, a[role="button"]'
                  );
                  let count = 0;
                  for (const el of candidates) {
                    const text = (el.innerText || el.textContent || '').trim();
                    const aria = (el.getAttribute('aria-label') || '').trim();
                    const combined = (text + ' ' + aria).trim();
                    if (!combined || combined.length > 80) continue;
                    if (!patterns.some(p => p.test(combined))) continue;
                    try {
                      el.click();
                      count++;
                    } catch (e) {}
                    if (count >= maxClicks) break;
                  }
                  return count;
                }
                """,
                {"maxClicks": max_clicks_per_round},
            )
        except Exception as e:
            print(f"  [NESTED-DEBUG] error: {e}")
            break

        if not clicked:
            break
        total += clicked
        page.wait_for_timeout(1500)

    if total:
        page.wait_for_timeout(1000)
    return total
                
def extract_stable_post_body(page: Page, post_link: str) -> dict[str, str]:
    """Wait until the extracted post body stops changing.

    This is not a fixed post-scroll timer. It checks the actual extracted body
    and returns after the same non-empty body is observed repeatedly.
    """

    deadline = time.monotonic() + POST_STABILITY_TIMEOUT_SECONDS
    last_body = ""
    stable_observations = 0
    best_result: dict[str, str] = {
        "body_text": "",
        "raw_main_container": "",
        "username": "",
        "timestamp": "",
        "extraction_status": "not_extracted",
    }

    while time.monotonic() < deadline:
        result = extract_main_post_body(page, post_link)
        current_body = str(result.get("body_text", "")).strip()

        if len(current_body) > len(str(best_result.get("body_text", ""))):
            best_result = result

        if current_body and current_body == last_body:
            stable_observations += 1
        else:
            stable_observations = 0
            last_body = current_body

        if current_body and stable_observations >= 2:
            return result

        page.wait_for_timeout(300)

    return best_result


# ============================================================
# 10. PROCESS ONE POST
# ============================================================

def read_reply_count(page: Page, post_link: str) -> int:
    """Read reply count for the target post.

    Threads renders multi-part threads with sub-post URLs (2/10, 3/10)
    that have no anchor of their own in the DOM. Only the MAIN post's
    anchor is present.

    - If our URL IS the main post -> return the first reply SVG's count.
    - If our URL is a SUB-post -> return 0 (no external replies to track).
    """
    try:
        return int(
            page.evaluate(
                r"""
                (postLink) => {
                  const targetPath = new URL(
                    postLink, window.location.origin
                  ).pathname.replace(/\/$/, '');

                  const canon = (h) => {
                    try {
                      return new URL(h, window.location.origin)
                        .pathname.replace(/\/$/, '');
                    } catch { return ''; }
                  };

                  // First /post/ anchor on the page.
                  const firstAnchor = document.querySelector(
                    'a[href*="/post/"]'
                  );
                  if (!firstAnchor) return 0;

                  // If the first anchor doesn't match our target,
                  // we're on a sub-post page (2/10, 3/10, ...).
                  if (canon(firstAnchor.href) !== targetPath) return 0;

                  // We're on the main post page. Take the first reply SVG.
                  const svgs = Array.from(
                    document.querySelectorAll('svg[title]')
                  ).filter(s => {
                    const t = (s.getAttribute('title') || '')
                      .toLowerCase().trim();
                    return t === 'reply' || t === 'balas';
                  });
                  if (svgs.length === 0) return 0;

                  const svg = svgs[0];
                  const sib = svg.nextElementSibling;
                  let numText = sib
                    ? (sib.innerText || sib.textContent || '').trim()
                    : '';
                  if (!numText && svg.parentElement) {
                    numText = (
                      svg.parentElement.innerText ||
                      svg.parentElement.textContent || ''
                    ).trim();
                  }
                  if (!numText) return 0;

                  const m = numText.match(
                    /^([\d.,]+)\s*(rb|ribu|jt|juta|k|m)?/i
                  );
                  if (!m) return 0;

                  let n = parseFloat(m[1].replace(',', '.'));
                  const u = (m[2] || '').toLowerCase();
                  if (u === 'rb' || u === 'ribu' || u === 'k') n *= 1000;
                  if (u === 'jt' || u === 'juta' || u === 'm') n *= 1000000;
                  return Math.floor(n);
                }
                """,
                post_link,
            )
        )
    except Exception:
        return 0
    
def load_post_with_replies(
    detail_page: Page,
    post_link: str,
    max_scrolls: int = 8,
) -> list[dict[str, Any]]:
    """Navigate, scroll, accumulate replies.

    Threads virtualizes the reply list — only the visible replies stay in
    the DOM. To capture everything, we extract replies after every scroll
    round and merge results by text. Returns the merged list.
    """

    detail_page.goto(
        post_link,
        wait_until="domcontentloaded",
        timeout=60000,
    )
    wait_for_main_post_container(detail_page, post_link)
    detail_page.wait_for_timeout(1800)

    expected = read_reply_count(detail_page, post_link)
    if expected:
        print(f"  [REPLY-COUNT] UI shows ~{expected} replies.")

    # Determine OP username before we start snapshotting.
    prelim = detail_page.evaluate(
        r"""
        () => {
          const m = window.location.pathname.match(/\/@([^/]+)\/post\//);
          return m ? m[1] : '';
        }
        """
    )

    accumulated: dict[str, dict[str, Any]] = {}

    def snapshot() -> int:
        items = extract_reply_items(
            detail_page, post_link, op_username=prelim
        )
        added = 0
        for item in items:
            key = (item.get("text") or "")[:200]
            if key and key not in accumulated:
                accumulated[key] = item
                added += 1
        return added

    # Initial snapshot
    snapshot()

    last_added_rounds = 0
    for round_idx in range(max_scrolls):
        if EXPAND_NESTED_REPLIES:
            nested_added = expand_nested_replies(
                detail_page,
                max_rounds=MAX_NESTED_EXPAND_ROUNDS,
                max_clicks_per_round=MAX_NESTED_EXPAND_CLICKS_PER_ROUND,
            )
            print(f"  [NESTED] Call returned: {nested_added}")

        detail_page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        detail_page.wait_for_timeout(1200)   # was 1500

        # Only snapshot every 2nd round (halves extraction cost)
        if round_idx % 2 == 0:
            added = snapshot()
        else:
            added = 0

        # Stop immediately if expected is 0 or we've already captured it.
        if expected == 0 and len(accumulated) == 0:
            break

        # Stop early if we've hit the target (with 10% slack).
        if expected and len(accumulated) >= max(1, int(expected * 0.9)):
            print(
                f"  [REPLY-COUNT] Reached {len(accumulated)} "
                f"(expected {expected})."
            )
            break

        # Stop if two scroll rounds added nothing new.
        if added == 0:
            last_added_rounds += 1
            if last_added_rounds >= 2:
                break
        else:
            last_added_rounds = 0

    # One final snapshot after the last scroll.
    snapshot()

    print(f"  [REPLY-COUNT] Total captured: {len(accumulated)}")

    detail_page.evaluate("() => window.scrollTo(0, 0)")
    detail_page.wait_for_timeout(600)

    return list(accumulated.values())

def stop_requested_globally_if_available() -> bool:
    # Placeholder kept tiny so load_post_with_replies stays self-contained.
    # The real stop check happens in the outer loop; this function exists
    # only so the helper is safe to call from any context.
    return False

def process_post(
    detail_page: Page,
    metadata: dict[str, str],
    keyword: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    post_link = canonicalize_post_url(metadata.get("post_link", ""))

    reply_items: list[dict[str, Any]] = []
    extracted: dict[str, Any] = {
        "body_text": "",
        "raw_main_container": "",
        "username": metadata.get("username_hint", ""),
        "timestamp": "",
        "extraction_status": "not_extracted",
    }

        # -------- Step 1: extract OP body (critical, isolated) --------
    try:
        detail_page.goto(
            post_link, wait_until="domcontentloaded", timeout=60000
        )
        wait_for_main_post_container(detail_page, post_link)
        detail_page.wait_for_timeout(1200)
        extracted = extract_stable_post_body(detail_page, post_link)
    except Exception as error:
        save_debug_files(
            detail_page,
            f"post_error_{metadata.get('post_id_hint', '')}",
        )
        extracted = {
            "body_text": "",
            "raw_main_container": "",
            "username": metadata.get("username_hint", ""),
            "timestamp": "",
            "extraction_status": f"fetch_error: {error}",
        }

    # -------- Step 2: replies (non-critical, its failure must not
    #           wipe the extraction from step 1) --------
    ui_reply_count = 0
    reply_items = []
    try:
        ui_reply_count = read_reply_count(detail_page, post_link)
        reply_items = load_post_with_replies(detail_page, post_link)
    except Exception as error:
        print(f"  Reply extraction failed (non-fatal): {error}")

    print("POST:", post_link)
    print("Reply count:", len(reply_items))
    print("Sample replies:", [r["text"][:80] for r in reply_items[:2]])

    username = (
        str(extracted.get("username", "")).strip()
        or str(metadata.get("username_hint", "")).strip()
    )


        # Commercial username filter (runs before content classification).
    if username_looks_commercial(username):
        return (
            {
                "post_id": str(metadata.get("post_id_hint", "")),
                "username": username,
                "timestamp": str(extracted.get("timestamp", "")),
                "post_body": "",
                "raw_main_container": "",
                "post_link": post_link,
                "matched_keywords": keyword,
                "extraction_status": "skipped_commercial_username",
                "content_fingerprint": "",
                "collected_at_utc": datetime.now(timezone.utc).isoformat(),
                "classifier_version": "kpr_scored_parallel_renewed_v3",
                "lead_category": "Seller or advertisement",
                "lead_priority": -1,
                "lead_type": "Excluded commercial KPR content",
                "commercial_type": "Commercial username pattern",
                "commercial_score": 10,
                "ad_signals": "username matched commercial patterns",
                "classification_reason": (
                    f"Username '{username}' matched commercial-account filter."
                ),
                "lead_confidence": "Excluded",
                "lead_score": 0,
                "positive_signals": "",
                "negative_signals": "",
                "hot_gate_passed": False,
                "manual_review_needed": False,
                "reply_count": 0, "total_thread_entries": 0,
                "ui_reply_count": 0, "customer_reply_count": 0,
                "seller_reply_count": 0, "op_reply_count": 0,
                "relevant_reply_count": 0, "productive_reply_count": 0,
                "op_engaged": False,
                "conversation_bonus": 0, "productive_conversation_bonus": 0,
                "continuity_bonus": 0, "op_followup_bonus": 0,
                "relevant_reply_text": "", "productive_reply_text": "",
                "manual_valid_lead": "", "manual_category": "",
                "reviewer_notes": "",
                "contact_status": "No contact — research only",
            },
            [],
        )

    raw_body = extracted.get("body_text", "")

    # Strip replies FIRST. After this, the body should contain only OP text
    # (plus the OP's own "Pembuat" continuations, which strip_reply_tail keeps).
    stripped_body = strip_reply_tail(raw_body)

    # Threads reply permalinks render the parent thread too. Only AFTER
    # stripping should we check: does the FIRST line of the body start with
    # a bare username different from the URL's username? If so, this really
    # is a parent-thread view, not the OP's own post.
    body_first_lines = [
        l.strip() for l in str(stripped_body or "").splitlines() if l.strip()
    ]
    body_first_user = ""
    if body_first_lines:
        first = body_first_lines[0]
        m = re.match(r"^@?([A-Za-z0-9_.]{3,30})$", first)
        if m:
            body_first_user = m.group(1).lower()

    if body_first_user and body_first_user != username.lower():
        # This is a reply inside someone else's thread, not an OP.
        return (
            {
                "post_id": str(metadata.get("post_id_hint", "")),
                "username": username,
                "timestamp": "",
                "post_body": "",
                "raw_main_container": "",
                "post_link": post_link,
                "matched_keywords": keyword,
                "extraction_status": "skipped_reply_thread_view",
                "content_fingerprint": "",
                "collected_at_utc": datetime.now(timezone.utc).isoformat(),
                "classifier_version": "v3",
                "lead_category": "Irrelevant",
                "lead_priority": 0,
            },
            [],
        )

    post_body = clean_main_post_body(stripped_body, username=username)
    # Quote-post guard: if the first non-empty line is a username that
    # doesn't match the URL's OP, this record is a quote-post or reply-
    # permalink view. The content belongs to the first-line username,
    # not to the URL's OP. Skip it.
    first_line = ""
    for line in post_body.splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break

    if re.fullmatch(r"@?[A-Za-z0-9_.]{3,30}", first_line):
        first_user = first_line.lstrip("@").lower()
        if first_user and first_user != username.lower():
            return (
                {
                    "post_id": str(metadata.get("post_id_hint", "")),
                    "username": username,
                    "timestamp": "",
                    "post_body": "",
                    "raw_main_container": "",
                    "post_link": post_link,
                    "matched_keywords": keyword,
                    "extraction_status": "skipped_quote_post",
                    "content_fingerprint": "",
                    "collected_at_utc": datetime.now(timezone.utc).isoformat(),
                    "classifier_version": "v3",
                    "lead_category": "Irrelevant",
                    "lead_priority": 0,
                },
                [],
            )

    classification = classify_post(post_body, reply_items=reply_items)

    op_count = int(classification.get("op_reply_count", 0) or 0)
    if ui_reply_count > 0:
        classification["reply_count"] = max(0, ui_reply_count - op_count)
    # else: keep the scraper's own external count (customer + seller).
    # This is the case for sub-posts, where we can't read a UI number.

    main_record: dict[str, Any] = {
        "post_id": str(metadata.get("post_id_hint", "")),
        "username": username,
        "timestamp": str(extracted.get("timestamp", "")),
        "post_body": post_body,
        "raw_main_container": str(extracted.get("raw_main_container", "")),
        "post_link": post_link,
        "matched_keywords": keyword,
        "extraction_status": str(extracted.get("extraction_status", "")),
        "content_fingerprint": content_fingerprint(post_body) if post_body else "",
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "classifier_version": "kpr_scored_parallel_renewed_v3",   # bumped
        "ui_reply_count": ui_reply_count, 
        **classification,
        "manual_valid_lead": "",
        "manual_category": "",
        "reviewer_notes": "",
        "contact_status": "No contact — research only",
    }

    # ------- Reply-level leads ----------------------------------------
    # A reply that independently qualifies as Hot/Warm is its own lead.
    # Seller replies, OP replies, and non-lead replies are skipped.
    reply_leads: list[dict[str, Any]] = []

    for r in reply_items:
        if r.get("is_op"):
            continue

        reply_text = str(r.get("text", ""))
        is_commercial, *_ = detect_seller_or_ad(reply_text)
        if is_commercial:
            continue

        reply_classification = classify_post(reply_text, reply_items=[])

        if reply_classification["lead_category"] in (
            "Hot research lead",
            "Warm research lead",
        ):
            reply_leads.append({
                "parent_post_link": post_link,
                "parent_username": username,
                "reply_author": r.get("username", ""),
                "reply_permalink": r.get("permalink", ""),
                "reply_body": reply_text,
                "source_type": "reply",
                "matched_keywords": keyword,
                "collected_at_utc": main_record["collected_at_utc"],
                **reply_classification,
            })

    return main_record, reply_leads


def create_workers(context: BrowserContext):
    """Open exactly two tabs: one search page, one detail page.

    Multiple detail pages were never used; the worker only reads
    workers[0]['page']. Opening just two tabs keeps the same
    parallelism (ProcessPoolExecutor still fans out keyword chunks
    across NUM_PROCESSES worker processes) while halving memory and
    avoiding four idle tabs per process.
    """

    search_page = context.pages[0] if context.pages else context.new_page()
    search_page.set_default_timeout(10000)

    detail_page = context.new_page()
    detail_page.set_default_timeout(10000)

    return search_page, [{"id": 0, "page": detail_page}]

def setup_worker_profiles(playwright):

    for worker_id in range(NUM_PROCESSES):

        profile_folder = (
            PROJECT_FOLDER /
            f"threads_browser_profile_worker{worker_id}"
        )

        profile_folder.mkdir(
            parents=True,
            exist_ok=True,
        )

        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_folder),
            headless=False,
            viewport={"width": 1440, "height": 1000},
            locale="id-ID",
        )

        page = (
            context.pages[0]
            if context.pages
            else context.new_page()
        )

        page.goto(
            THREADS_HOME,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        print(f"\nWorker {worker_id}")
        print("Profile folder:", profile_folder)
        print("Current URL:", page.url)

        print("\n" + "=" * 72)
        print(f"WORKER {worker_id}")
        print("=" * 72)
        print("Log into Threads.")
        print("Wait until your Threads home page is visible.")
        input("Press ENTER when finished...")

        print("After login URL:", page.url)
        print("Saving profile...")
        page.wait_for_timeout(5000)
        context.close()

def worker_process(
    worker_id: int,
    keyword_subset: list[str],
    target_unique_posts: int,
    shared_links,
    shared_processing_links,
    shared_fingerprints,
    progress,
    shared_lock,
    shared_count,
    stop_requested,
) -> dict[str, dict[str, Any]]:
    
    
    processed: dict[str, dict[str, Any]] = {}
    checkpoint_counter = {"count": 0}
    reply_leads_accumulator: dict[str, list[dict[str, Any]]] = {}
    
    profile_folder = (
    PROJECT_FOLDER /
    f"threads_browser_profile_worker{worker_id}"
    )

    profile_folder.mkdir(
        parents=True,
        exist_ok=True,
        )
    with sync_playwright() as playwright:

        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_folder),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 1000},
            locale="id-ID",
        )

        search_page, workers = create_workers(context)

        # Keyword loop will go here
        try:
            for keyword_number, keyword in enumerate(keyword_subset, start=1):
                
                if stop_requested.value:
                    break
                
                if  shared_count.value >= target_unique_posts:
                    break

                try:
                    if not scrape_keyword(
                        search_page=search_page,
                        worker_page=workers[0]["page"],
                        worker_id=worker_id,
                        keyword=keyword,
                        keyword_number=keyword_number,
                        processed=processed,
                        checkpoint_counter=checkpoint_counter,
                        reply_leads_accumulator=reply_leads_accumulator,
                        progress=progress,
                        shared_links=shared_links,
                        shared_processing_links=shared_processing_links,
                        shared_fingerprints=shared_fingerprints,
                        shared_lock=shared_lock,
                        shared_count=shared_count,
                        stop_requested=stop_requested,
                        target_unique_posts=target_unique_posts,
                        max_posts_per_keyword=Max_Posts_Per_Keyword,
                    ):
                        continue
                except Exception as error:
                    print(f"[WORKER {worker_id}] Keyword '{keyword}' crashed: {error}")
                    with shared_lock:
                        progress[keyword] = {
                            "status": "error",
                            "post_count": 0,
                        }
                        save_keyword_progress(progress)

                    try:
                        search_page.goto(THREADS_HOME, wait_until="domcontentloaded", timeout=30000)
                        workers[0]["page"].goto(THREADS_HOME, wait_until="domcontentloaded", timeout=30000)
                        search_page.wait_for_timeout(2000)
                    except Exception:
                        pass
                    
                    continue

        finally:
            cleanup_workers(workers)
            context.close()

        return processed, reply_leads_accumulator

def cleanup_workers(workers):
    for worker in workers:
        worker["page"].close()

def split_keywords(
    keywords: list[str],
    num_workers: int,
) -> list[list[str]]:
    """Split the keyword list into roughly equal chunks."""

    return [
        keywords[i::num_workers]
        for i in range(num_workers)
    ]

# ============================================================
# 11. READ ALL LOADED RESULTS BEFORE SCROLLING
# ============================================================
def scrape_keyword(
    search_page: Page,
    worker_page: Page,
    worker_id: int,
    keyword: str,
    keyword_number: int,
    processed: dict[str, dict[str, Any]],
    checkpoint_counter: dict[str, int],
    reply_leads_accumulator: dict[str, list[dict[str, Any]]],
    progress: dict[str, dict[str, Any]],
    shared_links,
    shared_processing_links,
    shared_fingerprints,
    shared_lock,
    shared_count,
    stop_requested,
    target_unique_posts: int,
    max_posts_per_keyword: int,
) -> bool:
    saved_progress = progress.get(keyword, {})
    saved_status = saved_progress.get("status", "")
    saved_post_count = int(saved_progress.get("post_count", 0))
    
    if saved_status == "completed_target" and saved_post_count >= max_posts_per_keyword:
        print(
            f"\n[SKIP {keyword_number}/{len(SEARCH_KEYWORDS)}] "
            f"{keyword} — completed target ({saved_post_count} posts)"
        )
        return False
    
    if saved_status == "exhausted":
        print(
            f"\n[SKIP {keyword_number}/{len(SEARCH_KEYWORDS)}] "
            f"{keyword} — exhausted ({saved_post_count} posts)"
        )
        return False
    

    posts_per_keyword = int(
        saved_progress.get("post_count", 0)
    )
    with shared_lock:
        progress[keyword] = {
            "status": "in_progress",
            "post_count": posts_per_keyword,
        }
        save_keyword_progress(progress)
    
    if posts_per_keyword >= max_posts_per_keyword:
        with shared_lock:
            progress[keyword] = {
                "status": "completed_target",
                "post_count": posts_per_keyword,
            }
            save_keyword_progress(progress)
        print(
            f"\n[SKIP {keyword_number}/{len(SEARCH_KEYWORDS)}] "
            f"{keyword} — already completed"
        )
        return False
    
    print("\n" + "=" * 72)
    print(
        f"[WORKER {worker_id}] "
        f"[SEARCH {keyword_number}/{len(SEARCH_KEYWORDS)}] {keyword}"
    )
    print("=" * 72)
    try:
        search_page.goto(
            THREADS_SEARCH_TEMPLATE.format(query=quote(keyword)),
            wait_until="domcontentloaded",
            timeout=60000,
            )
        # Wait for the results (and the sort tabs) to render before
        # trying to click 'Terbaru'. Clicking too early is why the
        # 'Could not click Recent' warning was firing.
        wait_for_initial_search_state(search_page)
        try_select_recent_results(search_page)
        # Remove dump_sort_candidates after one more verification run.
        # dump_sort_candidates(search_page)
    except Exception as error:
        print(f"Search page failed: {error}")
        save_debug_files(search_page, f"search_error_{keyword_number}_{keyword}")
        with shared_lock:
            progress[keyword] = {
                "status": "error",
                "post_count": posts_per_keyword,
            }
            save_keyword_progress(progress)
        return False

    read_for_keyword: set[str] = set()
    completed_scrolls_without_new_links = 0
    emergency_scroll_count = 0
    keyword_finished = False

    while (
        not stop_requested.value
        and shared_count.value < target_unique_posts
        and posts_per_keyword < max_posts_per_keyword
    ):
            loaded_records = extract_loaded_post_links(search_page, keyword)

            unread_records: list[dict[str, str]] = []
            for record in loaded_records:
                link = canonicalize_post_url(record.get("post_link", ""))
                if link and link not in read_for_keyword:
                    record["post_link"] = link
                    unread_records.append(record)

            # Read every currently loaded unread post before any scroll.
            if unread_records:
                print(
                    f"Reading {len(unread_records)} currently loaded unread posts "
                    f"before scrolling..."
                )

                for record in unread_records:
                    
                    if stop_requested.value:
                        break
                    
                    if (shared_count.value >= target_unique_posts):
                        break

                    link = canonicalize_post_url(record.get("post_link", ""))
                    read_for_keyword.add(link)

                    if link in processed:
                        processed[link]["matched_keywords"] = merge_keyword(
                            processed[link].get("matched_keywords", ""),
                            keyword,
                        )
                        continue
                    
                    with shared_lock:
                        if link in shared_links:
                            continue
                    
                        if link in shared_processing_links:
                            continue
                    
                        shared_processing_links[link] = True
                        
                    
                    try:
                        new_record, reply_leads = process_post(
                            worker_page,
                            record,
                            keyword,
                        )
                        if link not in reply_leads_accumulator:
                            reply_leads_accumulator[link] = reply_leads
                    finally:
                        with shared_lock:
                            shared_processing_links.pop(link, None)
                    status = str(new_record.get("extraction_status", ""))
                    body_len = len(str(new_record.get("post_body", "")).strip())
                    if status != "ok" or body_len == 0:
                        print(
                            f"  SKIP link={link} status={status!r} body_len={body_len}"
                        )
                        continue
                    with shared_lock:
                        shared_links[link] = True
                        
                    # Text-level duplicate detection is shared across all workers.
                    fingerprint = str(
                        new_record.get("content_fingerprint", "")
                    ).strip()
                    
                    has_body = (
                        len(str(new_record.get("post_body", "")).strip()) >= 30
                    )
                    
                    duplicate_of = ""

                    with shared_lock:
                        # Only posts with usable text can be detected as
                        # content duplicates.
                        if fingerprint and has_body:
                            duplicate_of = shared_fingerprints.get(
                                fingerprint,
                                "",
                            )
                    
                        if not duplicate_of:
                            # A non-duplicate post must claim a global
                            # unique-post slot before being saved.
                            if shared_count.value >= target_unique_posts:
                                break
                    
                            shared_count.value += 1
                            posts_per_keyword += 1
                    
                            # Register the fingerprint only after the post
                            # successfully claims a unique slot.
                            if fingerprint and has_body:
                                shared_fingerprints[fingerprint] = link
                    
                    # --------------------------------------------------
                    # DUPLICATE CONTENT
                    # --------------------------------------------------
                    if duplicate_of:
                        print(
                            f"  Duplicate content skipped: "
                            f"{link} -> {duplicate_of}"
                        )
                    
                        # IMPORTANT:
                        # Do NOT add duplicate content to processed.
                        # It does not count toward the unique-post target.
                        continue
                    
                    # --------------------------------------------------
                    # UNIQUE POST
                    # --------------------------------------------------
                    new_record["duplicate_of"] = ""
                    processed[link] = new_record
                    
                    checkpoint_counter["count"] += 1
                    
                    with shared_lock:
                        progress[keyword] = {
                            "status": "in_progress",
                            "post_count": posts_per_keyword,
                        }
                    
                        # Save the worker checkpoint together with the progress
                        # so resume state stays consistent.
                        save_worker_checkpoint(
                            worker_id,
                            processed,
                        )
                    
                        save_keyword_progress(progress)
                    
                    if posts_per_keyword >= max_posts_per_keyword:
                        print(
                            f"Reached {max_posts_per_keyword} posts "
                            f"for '{keyword}'."
                        )
                    
                        with shared_lock:
                            progress[keyword] = {
                                "status": "completed_target",
                                "post_count": posts_per_keyword,
                            }
                            save_keyword_progress(progress)
                    
                        keyword_finished = True
                        break

                    category = new_record["lead_category"]
                    preview = normalize_text(new_record.get("post_body", ""))[:100]
                    print(
                        f"  [WORKER {worker_id}] "
                        f"{posts_per_keyword:02d}/{max_posts_per_keyword} "
                        f"(global {shared_count.value:04d}/{target_unique_posts}): "
                        f"{category} — {preview}"
                    )

                if keyword_finished:
                    break
                if stop_requested.value:
                    with shared_lock:
                        progress[keyword] = {
                            "status": "stopped",
                            "post_count": posts_per_keyword,
                        }
                
                        save_worker_checkpoint(
                            worker_id,
                            processed,
                        )
                
                        save_keyword_progress(progress)
                
                    print(
                        f"[WORKER {worker_id}] "
                        f"Stop requested — saved {posts_per_keyword} posts "
                        f"for '{keyword}'."
                    )
                    break

                # Re-read the page state without scrolling. Threads may have
                # loaded another currently visible result while detail posts
                # were being processed. The loop will process it first.
                continue

            # No unread currently loaded links remain. Only now may we scroll.
            if not loaded_records:
                print("No post links are currently loaded on this search page.")

            previous_links = {
                canonicalize_post_url(record.get("post_link", ""))
                for record in loaded_records
                if canonicalize_post_url(record.get("post_link", ""))
            }
            previous_state = search_page_state(search_page)

            print(
                "All currently loaded posts have been read. "
                "Scrolling once to request more results..."
            )

            scroll_after_current_results_are_read(search_page)
            emergency_scroll_count += 1

            page_changed = wait_for_search_state_change(
                search_page,
                previous_state,
            )

            current_records = extract_loaded_post_links(search_page, keyword)
            current_links = {
                canonicalize_post_url(record.get("post_link", ""))
                for record in current_records
                if canonicalize_post_url(record.get("post_link", ""))
            }

            truly_new_links = current_links - previous_links - read_for_keyword

            if page_changed and truly_new_links:
                completed_scrolls_without_new_links = 0
                print(f"The page loaded {len(truly_new_links)} new unread post links.")
            else:
                completed_scrolls_without_new_links += 1
                print(
                    "This completed scroll produced no new unread post links "
                    f"({completed_scrolls_without_new_links}/{STABLE_END_ATTEMPTS})."
                )

            if completed_scrolls_without_new_links >= STABLE_END_ATTEMPTS:
                print(
                    "The search appears exhausted: every loaded post was read "
                    "and repeated completed scrolls produced nothing new."
                )
                with shared_lock:
                    progress[keyword] = {
                        "status": "exhausted",
                        "post_count": posts_per_keyword,
                    }
                    save_keyword_progress(progress)
                break

            if emergency_scroll_count >= EMERGENCY_MAX_SCROLLS_PER_KEYWORD:
                print(
                    "Emergency safety limit reached for this keyword. "
                    "Moving to the next keyword to prevent an infinite loop."
                )
                with shared_lock:
                    progress[keyword] = {
                        "status": "safety_limit",
                        "post_count": posts_per_keyword,
                    }
                    save_keyword_progress(progress)
                break
    return True



def collect_and_classify_parallel(
    target_unique_posts: int,
) -> dict[str, dict[str, Any]]:
    resume = input("Resume previous scraping? (y/n): ").strip().lower()

    if resume == "y":
        print("Resuming previous scrape.")
    else:
        print("Starting a new scrape.")
        archive_state_before_reset()
    
            
    keyword_chunks = split_keywords(
        SEARCH_KEYWORDS,
        NUM_PROCESSES,
    )
    

    if resume == "y":
        all_processed = load_processed_posts()
    
        checkpoint_posts = load_worker_checkpoints()
    
        all_processed.update(checkpoint_posts)
    
        print(
            f"Loaded {len(all_processed)} previous posts "
            f"({len(checkpoint_posts)} from worker checkpoints)."
        )
    else:
        all_processed = {}
        
    if resume == "y":
        progress = load_keyword_progress()
    else:
        progress = {}

    with Manager() as manager:
        
        shared_links = manager.dict()
        
        shared_processing_links = manager.dict()
        
        shared_fingerprints = manager.dict()
        
        for link in all_processed:
            shared_links[link] = True
            
        for link, record in all_processed.items():
            fingerprint = str(
                record.get("content_fingerprint", "")
            ).strip()
        
            if (
                fingerprint
                and str(record.get("post_body", "")).strip()
            ):
                shared_fingerprints[fingerprint] = link

        shared_progress = manager.dict(progress)
        
        shared_lock = manager.Lock()
        
        initial_unique_count = len(all_processed)
        
        shared_count = manager.Value("i", initial_unique_count)
        
        stop_requested = manager.Value("b", False)
        
        def listen_for_stop():
            while not stop_requested.value:
                try:
                    command = input().strip().lower()
        
                    if command == "stop":
                        print(
                            "\nStop requested. "
                            "Workers will finish their current post, "
                            "save checkpoints, and stop safely."
                        )
                        stop_requested.value = True
                        break
        
                except (EOFError, KeyboardInterrupt):
                    break
        
        print(
            "\nScraping is running."
            "\nType 'stop' and press ENTER at any time to stop safely."
        )
        
        stop_listener = threading.Thread(
            target=listen_for_stop,
            daemon=True,
        )
        
        stop_listener.start()
    
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            futures = []
            for worker_id, chunk in enumerate(keyword_chunks):
                    futures.append(
                   executor.submit(
                        worker_process,
                        worker_id,
                        chunk,
                        target_unique_posts,
                        shared_links,
                        shared_processing_links,
                        shared_fingerprints,
                        shared_progress,
                        shared_lock,
                        shared_count,
                        stop_requested,
                    )
                   )
            all_reply_leads: dict[str, list[dict[str, Any]]] = {}
            for future in as_completed(futures):
    
                try:
                    worker_processed, worker_reply_leads = future.result()
                    all_processed.update(worker_processed)
                    all_reply_leads.update(worker_reply_leads)

                except Exception as error:
                    print(f"A worker crashed: {error}")
    
    save_processed_posts(all_processed)
    return all_processed, all_reply_leads

# ============================================================
# 12. OUTPUTS
# ============================================================


def build_outputs(
    processed: dict[str, dict[str, Any]],
    output_folder: Path,
    requested_target: int,
    reply_leads_accumulator: dict[str, list[dict[str, Any]]] | None = None,
) -> None:
    """Create the same Hot/Warm/General result structure as the earlier code.

    The revised logic may change how many posts fall into each category, but
    the collection target is still the same number of unique posts analyzed.
    """

    output_folder.mkdir(parents=True, exist_ok=True)

    if not processed:
        raise RuntimeError("No posts were processed.")

    posts = pd.DataFrame(list(processed.values())).fillna("")

    category_order = {
        "Hot research lead": 0,
        "Warm research lead": 1,
        "General KPR discussion": 2,
        "Irrelevant": 3,
        "Seller or advertisement": 4,
    }

    posts["_category_sort"] = (
        posts["lead_category"].map(category_order).fillna(9)
    )
    posts = posts.sort_values(
        by=["_category_sort", "timestamp", "collected_at_utc"],
        ascending=[True, False, False],
    ).drop(columns=["_category_sort"])

    hot = posts[
        posts["lead_category"] == "Hot research lead"
    ].copy()
    warm = posts[
        posts["lead_category"] == "Warm research lead"
    ].copy()
    general = posts[
        posts["lead_category"] == "General KPR discussion"
    ].copy()
    sellers = posts[
        posts["lead_category"] == "Seller or advertisement"
    ].copy()
    irrelevant = posts[
        posts["lead_category"] == "Irrelevant"
    ].copy()

    potential = pd.concat([hot, warm], ignore_index=True)
    

    raw_columns = [
        "post_id",
        "username",
        "timestamp",
        "post_body",
        "raw_main_container",
        "post_link",
        "matched_keywords",
        "collected_at_utc",
        "extraction_status",
    ]

    classified_columns = raw_columns + [
        "lead_priority",
        "lead_category",
        "lead_type",
        "lead_confidence",
        "lead_score",
        "classification_reason",
        "positive_signals",
        "negative_signals",
        "hot_gate_passed",
        "commercial_type",
        "commercial_score",
        "ad_signals",
        "reply_count",
        "total_thread_entries",     # NEW
        "ui_reply_count",
        "customer_reply_count",
        "seller_reply_count",
        "op_reply_count",
        "relevant_reply_count",
        "productive_reply_count",
        "op_engaged",
        "conversation_bonus",
        "productive_conversation_bonus",
        "continuity_bonus",
        "op_followup_bonus",        # NEW
        "relevant_reply_text",
        "productive_reply_text",
        "manual_review_needed",
        "manual_valid_lead",
        "manual_category",
        "reviewer_notes",
        "contact_status",
    ]
    # Safety net: ensure every expected column exists before writing CSVs.
    for col in classified_columns:
        if col not in posts.columns:
            posts[col] = ""

    # Original-compatible output names.
    posts[raw_columns].to_csv(
        output_folder / "raw_posts.csv",
        index=False,
        encoding="utf-8-sig",
    )
    posts[classified_columns].to_csv(
        output_folder / "classified_posts.csv",
        index=False,
        encoding="utf-8-sig",
    )

    # Easier review files without changing the main result categories.
    potential[classified_columns].to_csv(
        output_folder / "hot_and_warm_research_leads.csv",
        index=False,
        encoding="utf-8-sig",
    )
    hot[classified_columns].to_csv(
        output_folder / "hot_research_leads.csv",
        index=False,
        encoding="utf-8-sig",
    )
    warm[classified_columns].to_csv(
        output_folder / "warm_research_leads.csv",
        index=False,
        encoding="utf-8-sig",
    )
    general[classified_columns].to_csv(
        output_folder / "general_mortgage_discussion.csv",
        index=False,
        encoding="utf-8-sig",
    )
    sellers[classified_columns].to_csv(
        output_folder / "seller_or_advertisement.csv",
        index=False,
        encoding="utf-8-sig",
    )
    irrelevant[classified_columns].to_csv(
        output_folder / "irrelevant.csv",
        index=False,
        encoding="utf-8-sig",
    )

    category_summary = (
        posts.groupby("lead_category", dropna=False)
        .size()
        .reset_index(name="post_count")
    )
    category_summary["percentage"] = (
        category_summary["post_count"] / len(posts) * 100
    ).round(1)
    category_summary["_sort"] = (
        category_summary["lead_category"].map(category_order).fillna(9)
    )
    category_summary = category_summary.sort_values("_sort").drop(
        columns=["_sort"]
    )

    lead_type_summary = (
        posts.groupby("lead_type", dropna=False)
        .size()
        .reset_index(name="post_count")
        .sort_values("post_count", ascending=False)
    )

    keyword_rows: list[dict[str, Any]] = []
    for _, row in posts.iterrows():
        keywords = [
            item.strip()
            for item in str(row.get("matched_keywords", "")).split("|")
            if item.strip()
        ]
        for keyword in keywords:
            keyword_rows.append(
                {
                    "search_keyword": keyword,
                    "post_link": row["post_link"],
                    "lead_priority": row["lead_priority"],
                    "lead_category": row["lead_category"],
                    "is_hot_lead": (
                        row["lead_category"] == "Hot research lead"
                    ),
                    "is_warm_lead": (
                        row["lead_category"] == "Warm research lead"
                    ),
                    "is_seller_ad": (
                        row["lead_category"] == "Seller or advertisement"
                    ),
                }
            )

    keyword_data = pd.DataFrame(keyword_rows)
    if keyword_data.empty:
        keyword_summary = pd.DataFrame(
            columns=[
                "search_keyword",
                "unique_posts",
                "hot_leads",
                "warm_leads",
                "seller_ads",
                "hot_lead_rate_percent",
                "hot_and_warm_rate_percent",
                "ad_rate_percent",
                "average_priority",
            ]
        )
    else:
        keyword_summary = (
            keyword_data.groupby("search_keyword")
            .agg(
                unique_posts=("post_link", "nunique"),
                hot_leads=("is_hot_lead", "sum"),
                warm_leads=("is_warm_lead", "sum"),
                seller_ads=("is_seller_ad", "sum"),
                average_priority=("lead_priority", "mean"),
            )
            .reset_index()
        )
        keyword_summary["hot_lead_rate_percent"] = (
            keyword_summary["hot_leads"]
            / keyword_summary["unique_posts"]
            * 100
        ).round(1)
        keyword_summary["hot_and_warm_rate_percent"] = (
            (keyword_summary["hot_leads"] + keyword_summary["warm_leads"])
            / keyword_summary["unique_posts"]
            * 100
        ).round(1)
        keyword_summary["ad_rate_percent"] = (
            keyword_summary["seller_ads"]
            / keyword_summary["unique_posts"]
            * 100
        ).round(1)
        keyword_summary["average_priority"] = (
            keyword_summary["average_priority"].round(2)
        )
        keyword_summary = keyword_summary.sort_values(
            by=[
                "hot_leads",
                "warm_leads",
                "ad_rate_percent",
                "unique_posts",
            ],
            ascending=[False, False, True, False],
        )

    category_summary.to_csv(
        output_folder / "summary_by_category.csv",
        index=False,
        encoding="utf-8-sig",
    )
    lead_type_summary.to_csv(
        output_folder / "summary_by_lead_type.csv",
        index=False,
        encoding="utf-8-sig",
    )
    keyword_summary.to_csv(
        output_folder / "summary_by_keyword.csv",
        index=False,
        encoding="utf-8-sig",
    )

    anonymized = potential.copy()
    anonymized["username"] = ""
    anonymized["post_link"] = ""
    anonymized[
        [
            "timestamp",
            "post_body",
            "matched_keywords",
            "lead_category",
            "lead_type",
            "classification_reason",
        ]
    ].to_csv(
        output_folder / "anonymized_hot_and_warm_for_reporting.csv",
        index=False,
        encoding="utf-8-sig",
    )

    hot_count = len(hot)
    warm_count = len(warm)
    hot_rate = hot_count / len(posts) * 100 if len(posts) else 0
    target_status = (
        "Target reached"
        if len(posts) == requested_target
        else "Searches exhausted before target"
    )

    report_lines = [
        "THREADS MORTGAGE SEARCH — HOT/WARM STRICT V3",
        "=" * 61,
        f"Requested unique-post target: {requested_target}",
        f"Unique posts analyzed: {len(posts)}",
        f"Collection status: {target_status}",
        f"Hot research leads: {hot_count}",
        f"Warm research leads: {warm_count}",
        f"General KPR discussions: {len(general)}",
        f"Seller or advertisement: {len(sellers)}",
        f"Irrelevant: {len(irrelevant)}",
        f"Hot-lead rate: {hot_rate:.1f}%",
        "",
        "ORDERED CLASSIFICATION LOGIC",
        "1. Confirm explicit KPR context in the cleaned main post body.",
        "2. Exclude seller, service, property-listing, and advertisement content.",
        "3. Check Hot research lead rules.",
        "4. Check Warm research lead rules.",
        "5. Check General KPR discussion rules.",
        "6. Everything else is Irrelevant.",
        "",
        "COLLECTION METHOD",
        "- The target remains exactly 2,000 unique processed posts by default.",
        "- Only KPR-specific search phrases are used.",
        "- Area and location are not searched, scored, or exported.",
        "- Matched keywords are discovery metadata only.",
        "- The individual post is opened and its cleaned main body is classified.",
        "- Every loaded unread result is processed before the next scroll.",
        "- Scrolling is triggered by page state, not by a fixed scrolling timer.",
        "",
        "IMPORTANT",
        "- Stricter rules can change the category counts from an earlier run.",
        "- Hot/Warm indicate research relevance, not KPR eligibility.",
        "- Manual validation is required before business use.",
        "- The script performs no outreach.",
    ]

    (output_folder / "research_summary.txt").write_text(
        "\n".join(report_lines),
        encoding="utf-8",
    )

    # ------- Reply-level leads ---------------------------------------
    reply_rows: list[dict[str, Any]] = []
    if reply_leads_accumulator:
        for leads in reply_leads_accumulator.values():
            reply_rows.extend(leads)

    if reply_rows:
        reply_df = pd.DataFrame(reply_rows).fillna("")

        # One row per unique reply author across all threads. Threads users
        # often reply to multiple posts; we only want each author once.
        reply_df = reply_df.drop_duplicates(
            subset=["reply_author"],
            keep="first",
        )

        reply_df.to_csv(
            output_folder / "reply_leads.csv",
            index=False,
            encoding="utf-8-sig",
        )
        print(f"Reply-level leads written: {len(reply_df)}")

        # Lightweight per-thread reply activity summary.
        thread_activity = (
            pd.DataFrame(reply_rows)
            .groupby("parent_post_link", dropna=False)
            .size()
            .reset_index(name="reply_lead_count")
        )
        thread_activity.to_csv(
            output_folder / "reply_leads_by_thread.csv",
            index=False,
            encoding="utf-8-sig",
        )
    else:
        print("No reply-level Hot/Warm leads detected in this run.")

    print("\n" + "=" * 72)
    print("COLLECTION AND STRICT HOT/WARM ANALYSIS COMPLETE")
    print("=" * 72)
    print(f"Requested unique-post target: {requested_target}")
    print(f"Unique posts analyzed: {len(posts)}")
    print(f"Collection status: {target_status}")
    print(f"Hot research leads: {hot_count}")
    print(f"Warm research leads: {warm_count}")
    print(f"General KPR discussions: {len(general)}")
    print(f"Seller or advertisement: {len(sellers)}")
    print(f"Irrelevant: {len(irrelevant)}")
    print(f"Output folder: {output_folder}")


    


# ============================================================
# 13. SELF-TESTS
# ============================================================

def run_self_tests() -> None:
    tests = [
        (
            "Saya mau ajukan KPR bulan ini dan butuh rekomendasi bank.",
            "Hot research lead",
        ),
        (
            "Saya marketing KPR, promo bunga rendah, hubungi saya.",
            "Seller or advertisement",
        ),
        (
            "KPR KPR bank bunga rumah cicilan.",
            "Irrelevant",
        ),
    ]

    failures = []
    for text, expected in tests:
        actual = classify_post(text)["lead_category"]
        if actual != expected:
            failures.append(
                f"Expected {expected!r}, got {actual!r}: {text}"
            )

    if failures:
        raise AssertionError("\n".join(failures))

    print("All KPR scored-classifier self-tests passed.")

def postprocess_and_finalize(
    output_folder: Path,
    lead_cap: int = 999999,
    recency_days: int = 90,
) -> None:
    """Run the post-scrape pipeline as three steps:
       1. Reclassify every post with the CURRENT code (quote-post guard,
          username filter, updated patterns).
       2. Export all Hot/Warm rows to candidates_final.csv for review.
       3. Build final_leads.xlsx from the reclassified CSV.
    """
    classified_path = output_folder / "classified_posts.csv"
    if not classified_path.exists():
        print("[POST] No classified posts found; skipping.")
        return

    # -------- Step 1: reclassify -----------------------------------
    print("\n" + "=" * 72)
    print("[POST] Step 1/3 — Reclassifying with current classifier")
    print("=" * 72)

    df = pd.read_csv(classified_path).fillna("")
    blacklist = build_auto_blacklist(min_post_count=3)

    # If the state file is missing (e.g., code handed off without
    # state_kpr_conversation/), derive the blacklist from the CSV itself.
    if not blacklist:
        counts = (
            df["username"].astype(str).str.lower().str.strip().value_counts()
        )
        blacklist = set(counts[counts >= 3].index)
        print(
            f"[POST] State file missing — derived blacklist "
            f"from CSV ({len(blacklist)} accounts)."
        )

    before_hot = (df["lead_category"] == "Hot research lead").sum()
    before_warm = (df["lead_category"] == "Warm research lead").sum()
    print(f"[POST]   Before: Hot={before_hot}  Warm={before_warm}")

    def _reclass(row):
        body = str(row.get("post_body", "") or "")
        username = str(row.get("username", "") or "").lower().lstrip("@").strip()

        # Quote-post guard: first non-empty line is a bare username that
        # doesn't match this row's username => quote-post / reply-permalink.
        first_line = ""
        for line in body.splitlines():
            stripped = line.strip()
            if stripped:
                first_line = stripped
                break

        if re.fullmatch(r"@?[A-Za-z0-9_.]{3,30}", first_line):
            first_user = first_line.lstrip("@").lower()
            if first_user and first_user != username:
                return pd.Series({
                    "lead_category": "Irrelevant",
                    "lead_score": 0,
                    "lead_type": "",
                    "lead_confidence": "None",
                    "positive_signals": "",
                    "classification_reason": "skipped_quote_post",
                })

        # Username filter (Layer 1 + Layer 2)
        if username_looks_commercial(username) or username in blacklist:
            return pd.Series({
                "lead_category": "Seller or advertisement",
                "lead_score": 0,
                "lead_type": "Excluded commercial KPR content",
                "lead_confidence": "Excluded",
                "positive_signals": "",
                "classification_reason": (
                    "Filtered by commercial username / auto-blacklist"
                ),
            })

        r = classify_post(body)
        return pd.Series({
            "lead_category": r["lead_category"],
            "lead_score": r["lead_score"],
            "lead_type": r["lead_type"],
            "lead_confidence": r["lead_confidence"],
            "positive_signals": r["positive_signals"],
            "classification_reason": r["classification_reason"],
        })

    new_cols = df.apply(_reclass, axis=1)
    for c in new_cols.columns:
        df[c] = new_cols[c]

    df.to_csv(classified_path, index=False, encoding="utf-8-sig")

    after_hot = (df["lead_category"] == "Hot research lead").sum()
    after_warm = (df["lead_category"] == "Warm research lead").sum()
    print(f"[POST]   After:  Hot={after_hot}  Warm={after_warm}")

    # -------- Step 2: export candidates ----------------------------
    print("\n" + "=" * 72)
    print("[POST] Step 2/3 — Exporting all Hot/Warm candidates")
    print("=" * 72)

    leads = df[df["lead_category"].isin(
        ["Hot research lead", "Warm research lead"]
    )].copy()

    candidates_path = output_folder / "candidates_final.csv"
    leads[[
        "username", "lead_category", "lead_score", "post_body", "post_link",
    ]].to_csv(candidates_path, index=False, encoding="utf-8-sig")
    print(f"[POST]   Wrote {len(leads)} candidates to {candidates_path}")

    # -------- Step 3: build Excel ----------------------------------
    print("\n" + "=" * 72)
    print("[POST] Step 3/3 — Building final_leads.xlsx")
    print("=" * 72)

    _build_final_deliverable(
        output_folder,
        lead_cap=lead_cap,
        recency_days=recency_days,
    )

# ============================================================
# 14. COMMAND-LINE ENTRY POINT
# ============================================================


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect public Threads posts using the strict KPR Hot/Warm/General funnel."
        )
    )
    parser.add_argument(
        "--target",
        type=int,
        default=DEFAULT_TARGET_UNIQUE_POSTS,
        help="Target number of unique post links to process. Default: 2000.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT_FOLDER),
        help="Output folder path.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Test the strict Hot/Warm classifier without opening Threads.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help=(
            "Delete saved processed-post and keyword-progress state before running. "
            "The saved browser login profile is kept."
        ),
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help=(
            "After scraping, also write final_leads.xlsx (top Hot/Warm "
            "leads from the last 90 days) into the output folder."
        ),
    )

    parser.add_argument(
        "--postprocess-only",
        action="store_true",
        help=(
            "Skip scraping. Only run reclassify + candidates export + "
            "final_leads.xlsx build against the existing CSVs."
        ),
    )

    return parser.parse_args()

def reset_collection_state() -> None:
    for file_path in (PROCESSED_POSTS_FILE, KEYWORD_PROGRESS_FILE):
        if file_path.exists():
            file_path.unlink()
    print("Saved collection state was cleared. Browser login was kept.")

def archive_state_before_reset() -> None:
    """Move state files into a timestamped archive instead of deleting
    them. Called when the user answers 'n' to resume. Prevents the
    accidental data loss that silently drops leads from a previous run."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive = STATE_FOLDER / f"_archive_{stamp}"
    archive.mkdir(parents=True, exist_ok=True)

    moved: list[str] = []
    for path in (PROCESSED_POSTS_FILE, KEYWORD_PROGRESS_FILE):
        if path.exists():
            path.rename(archive / path.name)
            moved.append(path.name)

    for ckpt in CHECKPOINT_FOLDER.glob("worker_*.csv"):
        ckpt.rename(archive / ckpt.name)
        moved.append(ckpt.name)

    print(f"[RESET] Archived {len(moved)} state file(s) to:")
    print(f"[RESET]   {archive}")


def main() -> None:
    args = parse_arguments()
    
    if args.self_test:
        run_self_tests()
        return     

    if args.postprocess_only:
        output_folder = Path(args.output).expanduser().resolve()
        postprocess_and_finalize(output_folder)
        return

    if args.restart:
        reset_collection_state()

    output_folder = Path(args.output).expanduser().resolve()

    with sync_playwright() as playwright:

        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_FOLDER),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 1000},
            locale="id-ID",
        )

        login_page = context.pages[0] if context.pages else context.new_page()

        login_page.goto(
            THREADS_HOME,
            wait_until="domcontentloaded",
            timeout=60000,
        )

        print("\n" + "=" * 72)
        print("LOG IN TO THREADS")
        print("=" * 72)
        print("Log in normally in the Chromium window.")
        print("The Python script does not receive your password.")
        input("\nWhen Threads is visible, return to Terminal and press ENTER: ")
        
        context.close()

        if SETUP_WORKER_PROFILES:
            setup_worker_profiles(playwright)

        

    processed, reply_leads = collect_and_classify_parallel(
        target_unique_posts=args.target,
    )

    build_outputs(
        processed,
        output_folder,
        requested_target=args.target,
        reply_leads_accumulator=reply_leads,
    )

    if args.final:
        postprocess_and_finalize(
            output_folder,
            lead_cap=999999,
            recency_days=90,
        )

if __name__ == "__main__":
    main()
