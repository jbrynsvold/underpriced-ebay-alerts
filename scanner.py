import os
import re
import time
import base64
import logging
import schedule
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from rapidfuzz import fuzz

load_dotenv()

# ===========================================================================
# Config
# ===========================================================================

EBAY_CLIENT_ID     = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY")

EBAY_TOKEN_URL  = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_SCOPE      = "https://api.ebay.com/oauth/api_scope"

DISCOUNT_THRESHOLD = 1   # alert if eBay price <= market * this
MIN_SAVINGS        = 2      # minimum $ savings to alert
MAX_SAVINGS_PCT    = 90     # ignore suspiciously large discounts
MIN_MATCH_SCORE    = 65     # minimum card scorer score to match
MIN_WORD_LEN       = 4      # minimum word length for player name index

BASE_VARIATIONS = {"", "base", "none", "base card", "n/a"}

SET_NOISE_WORDS = {
    "basketball", "football", "baseball", "hockey",
    "trading", "card", "cards", "tcg", "nfl", "nba", "mlb", "nhl",
}

STRONG_NON_BASE = {
    "silver", "gold", "refractor", "prizm", "holo", "foil",
    "rainbow", "atomic", "laser", "hyper", "mojo", "cracked",
    "shimmer", "wave", "pulsar", "disco", "glossy", "lazer",
    "stamped", "prerelease", "shadowless", "cosmos",
    "reverse", "fullart", "altart", "promo",
}

EXCL = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"set lot" -"select a card" -"pick a card" -"pick your card"'
    ' -autograph -auto -signed -"art card" -"custom card"'
)

TEAM_NAMES = {
    # MLB
    "Baltimore Orioles", "Boston Red Sox", "New York Yankees", "Los Angeles Dodgers",
    "Chicago Cubs", "Houston Astros", "Atlanta Braves", "San Francisco Giants",
    "St. Louis Cardinals", "Philadelphia Phillies", "New York Mets", "Los Angeles Angels",
    "Seattle Mariners", "Toronto Blue Jays", "Tampa Bay Rays", "Minnesota Twins",
    "Cleveland Guardians", "Detroit Tigers", "Kansas City Royals", "Chicago White Sox",
    "Texas Rangers", "Oakland Athletics", "San Diego Padres", "Colorado Rockies",
    "Arizona Diamondbacks", "Miami Marlins", "Pittsburgh Pirates", "Cincinnati Reds",
    "Milwaukee Brewers", "Washington Nationals",
    # NBA
    "Los Angeles Lakers", "Boston Celtics", "Chicago Bulls", "Golden State Warriors",
    "Miami Heat", "San Antonio Spurs", "Dallas Mavericks", "Phoenix Suns",
    "Denver Nuggets", "Milwaukee Bucks", "Brooklyn Nets", "Philadelphia 76ers",
    "Toronto Raptors", "New York Knicks", "Cleveland Cavaliers", "Oklahoma City Thunder",
    "Memphis Grizzlies", "New Orleans Pelicans", "Sacramento Kings", "Utah Jazz",
    "Portland Trail Blazers", "Indiana Pacers", "Atlanta Hawks", "Charlotte Hornets",
    "Detroit Pistons", "Washington Wizards", "Orlando Magic", "Minnesota Timberwolves",
    "Houston Rockets", "Los Angeles Clippers",
    # NFL
    "Kansas City Chiefs", "San Francisco 49ers", "Dallas Cowboys", "New England Patriots",
    "Green Bay Packers", "Pittsburgh Steelers", "Baltimore Ravens", "Buffalo Bills",
    "Philadelphia Eagles", "Cincinnati Bengals", "Los Angeles Rams", "Miami Dolphins",
    "Las Vegas Raiders", "Denver Broncos", "Seattle Seahawks", "Tampa Bay Buccaneers",
    "New Orleans Saints", "Minnesota Vikings", "Chicago Bears", "New York Giants",
    "New York Jets", "Washington Commanders", "Carolina Panthers", "Atlanta Falcons",
    "Detroit Lions", "Arizona Cardinals", "Los Angeles Chargers", "Indianapolis Colts",
    "Tennessee Titans", "Jacksonville Jaguars", "Cleveland Browns", "Houston Texans",
    # NHL
    "Toronto Maple Leafs", "Montreal Canadiens", "Boston Bruins", "New York Rangers",
    "Chicago Blackhawks", "Detroit Red Wings", "Philadelphia Flyers", "Edmonton Oilers",
    "Pittsburgh Penguins", "Colorado Avalanche", "Tampa Bay Lightning", "Vegas Golden Knights",
    "Carolina Hurricanes", "Florida Panthers", "New York Islanders", "Washington Capitals",
    "Minnesota Wild", "St. Louis Blues", "Nashville Predators", "Winnipeg Jets",
    "Calgary Flames", "Vancouver Canucks", "Ottawa Senators", "Buffalo Sabres",
    "New Jersey Devils", "Columbus Blue Jackets", "San Jose Sharks", "Anaheim Ducks",
    "Seattle Kraken", "Dallas Stars", "Arizona Coyotes", "Quebec Nordiques",
    "Hartford Whalers", "Atlanta Thrashers",
}

CATEGORIES = {
    "MLB": {
        "sport":         "MLB",
        "ebay_query":    f"baseball card {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Baseball}",
        "discord_env":   "DISCORD_WEBHOOK_MLB_ALERTS",
        "emoji":         "⚾",
        "color":         0x002D72,
    },
    "NBA": {
        "sport":         "NBA",
        "ebay_query":    f"basketball card {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Basketball}",
        "discord_env":   "DISCORD_WEBHOOK_NBA_ALERTS",
        "emoji":         "🏀",
        "color":         0xC9082A,
    },
    "NFL": {
        "sport":         "NFL",
        "ebay_query":    f"football card {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Football}",
        "discord_env":   "DISCORD_WEBHOOK_NFL_ALERTS",
        "emoji":         "🏈",
        "color":         0x013369,
    },
    "NHL": {
        "sport":         "NHL",
        "ebay_query":    f"hockey card {EXCL}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Ice Hockey}",
        "discord_env":   "DISCORD_WEBHOOK_NHL_ALERTS",
        "emoji":         "🏒",
        "color":         0x000000,
    },
}

# ===========================================================================
# Logging
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ===========================================================================
# State (persists across runs since process runs continuously)
# ===========================================================================

from supabase.client import ClientOptions
supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(postgrest_client_timeout=30)
)

seen_urls: set = set()          # in-memory dedup — persists for process lifetime

_ebay_token        = None
_ebay_token_expiry = 0

# sport -> { player_name -> [card dicts] }
_player_card_cache: dict = {}

# sport -> { word -> set(player_names) }
_word_to_players: dict = {}

# sport -> { cleaned_name -> original_name }
_cleaned_to_original: dict = {}

_player_index_loaded: set = set()  # tracks which sports are loaded

# ===========================================================================
# eBay token
# ===========================================================================

def get_ebay_token() -> str:
    global _ebay_token, _ebay_token_expiry
    if _ebay_token and time.time() < _ebay_token_expiry:
        return _ebay_token

    auth = base64.b64encode(
        f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()
    ).decode()
    resp = requests.post(
        EBAY_TOKEN_URL,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=f"grant_type=client_credentials&scope={EBAY_SCOPE}",
    )
    resp.raise_for_status()
    data = resp.json()
    _ebay_token        = data["access_token"]
    _ebay_token_expiry = time.time() + data["expires_in"] - 60
    log.info("Got new eBay token")
    return _ebay_token

# ===========================================================================
# Token helpers
# ===========================================================================

SUFFIX_RE = re.compile(r'\b(II|III|IV|Jr\.?|Sr\.?)$', re.IGNORECASE)

def strip_suffix(name: str) -> str:
    return SUFFIX_RE.sub('', name).strip()

def tokenize(text: str, min_len: int = 3) -> list:
    return [w.lower() for w in re.split(r'[\W_]+', text) if len(w) >= min_len]

def set_tokens(set_name: str) -> list:
    return [t for t in tokenize(set_name) if t not in SET_NOISE_WORDS]

def variation_tokens(variation: str) -> list:
    stop = {"and", "the", "of", "for", "a"}
    return [t for t in tokenize(variation, min_len=2) if t not in stop]

# ===========================================================================
# Player name index
# ===========================================================================

def load_player_index(sport: str):
    if sport in _player_index_loaded:
        return
    log.info(f"Loading {sport} player names...")

    result = supabase.table("player_name_index") \
        .select("player_name") \
        .eq("sport", sport) \
        .limit(50000) \
        .execute()
    all_names = [r["player_name"] for r in (result.data or []) if r.get("player_name")]

    word_map    = {}
    cleaned_map = {}
    seen        = set()

    for name in all_names:
        if name in seen:
            continue
        seen.add(name)
        cleaned = strip_suffix(name).lower()
        cleaned_map[cleaned] = name
        for word in cleaned.split():
            if len(word) >= MIN_WORD_LEN:
                word_map.setdefault(word, set()).add(name)

    _word_to_players[sport]      = word_map
    _cleaned_to_original[sport]  = cleaned_map
    _player_index_loaded.add(sport)
    log.info(f"{sport}: loaded {len(cleaned_map)} players, {len(word_map)} index words")


def get_candidate_players(title: str, sport: str) -> list:
    """Two-stage player matching: fast word index then partial_ratio."""
    title_lower = title.lower()
    word_map    = _word_to_players.get(sport, {})
    
    # Stage 1: inverted index
    title_words   = [w for w in re.split(r'\W+', title_lower) if len(w) >= MIN_WORD_LEN]
    candidate_set = set()
    for word in title_words:
        for player in word_map.get(word, []):
            candidate_set.add(player)
            
    if not candidate_set:
        return []
        
    # Stage 2: partial_ratio on small candidate set, excluding team names
    cleaned_map = _cleaned_to_original.get(sport, {})
    matches = []
    for original_name in candidate_set:
        if original_name in TEAM_NAMES:
            continue
        cleaned = strip_suffix(original_name).lower()
        score   = fuzz.partial_ratio(cleaned, title_lower)
        if score >= 92:
            matches.append((original_name, score))
            
    matches.sort(key=lambda x: -x[1])
    return [m[0] for m in matches]

# ===========================================================================
# Card data loader
# ===========================================================================

def fetch_player_cards(players: list, sport: str):
    cache    = _player_card_cache.setdefault(sport, {})
    uncached = [p for p in players if p not in cache]
    if not uncached:
        return

    try:
        metrics = supabase.table("mv_card_metrics") \
            .select("canonical_name, grade, player_name, current_price, avg_price_30d, "
                    "card_number, last_sale_date, set_name, set_year, variation") \
            .in_("player_name", uncached) \
            .eq("sport", sport) \
            .execute()
    except Exception as e:
        log.error(f"mv_card_metrics error: {e}")
        for p in uncached:
            cache[p] = []
        return

    if not metrics.data:
        for p in uncached:
            cache[p] = []
        return

    grouped = {}
    for row in metrics.data:
        enriched = {
            **row,
            "market_price": row.get("avg_price_30d") or row.get("current_price") or 0,
        }
        grouped.setdefault(row["player_name"], []).append(enriched)

    for player, cards in grouped.items():
        cache[player] = cards

    for p in uncached:
        if p not in cache:
            cache[p] = []

# ===========================================================================
# Card scorer
# ===========================================================================

def parse_title(title: str) -> dict:
    title_lower = title.lower()
    ebay_year   = None
    ebay_year2  = None

    full_year = re.search(r'\b(19|20)\d{2}\b', title)
    if full_year:
        ebay_year = int(full_year.group())
        hockey_year = re.search(r'\b(19|20)(\d{2})-(\d{2})\b', title)
        if hockey_year:
            suffix = int(hockey_year.group(3))
            ebay_year2 = 2000 + suffix if suffix <= 30 else 1900 + suffix
    else:
        short = re.search(r'\b(\d{2})-(\d{2})\b', title)
        if short:
            y1, y2 = int(short.group(1)), int(short.group(2))
            if (y1 >= 90 or y1 <= 26) and (y2 >= 90 or y2 <= 26):
                ebay_year  = (1900 if y1 >= 90 else 2000) + y1
                ebay_year2 = 2000 + y2

    card_num_match = re.search(r'#\s*(\w+)', title)
    ebay_card_num  = card_num_match.group(1).lstrip('0') if card_num_match else None

    return {
        "title_lower":   title_lower,
        "ebay_year":     ebay_year,
        "ebay_year2":    ebay_year2,
        "ebay_card_num": ebay_card_num,
    }

def score_card_match(parsed: dict, card: dict) -> float:
    title_lower   = parsed["title_lower"]
    ebay_year     = parsed["ebay_year"]
    ebay_year2    = parsed["ebay_year2"]
    ebay_card_num = parsed["ebay_card_num"]

    set_year   = card.get("set_year")
    set_name   = card.get("set_name") or ""
    variation  = (card.get("variation") or "").strip()
    db_card_num = (card.get("card_number") or "").lstrip("0")
    is_base    = variation.lower() in BASE_VARIATIONS

    # Hard filter: year
    if set_year and (ebay_year or ebay_year2):
        if ebay_year != set_year and ebay_year2 != set_year:
            return -1.0

    # Hard filter: card number
    if ebay_card_num and db_card_num:
        if ebay_card_num != db_card_num:
            return -1.0

    score = 0.0

    # Set name matching
    s_tokens = set_tokens(set_name)
    if s_tokens:
        found = [t for t in s_tokens if t in title_lower]
        score += (len(found) / len(s_tokens)) * 60
        if len(found) == len(s_tokens):
            score += 20
        if not found:
            score -= 20
    else:
        score += 10

    # Variation matching
    if not is_base:
        v_tokens = variation_tokens(variation)
        if v_tokens:
            found_v = [t for t in v_tokens if t in title_lower]
            score += (len(found_v) / len(v_tokens)) * 60
            if len(found_v) == len(v_tokens):
                score += 20
            if not found_v:
                score -= 50
        else:
            if variation.lower() in title_lower:
                score += 20
            else:
                score -= 10
    else:
        title_tokens = set(tokenize(title_lower))
        if title_tokens & STRONG_NON_BASE:
            score -= 40

    # Year bonus
    if set_year and (ebay_year == set_year or ebay_year2 == set_year):
        score += 10

    return score

# ===========================================================================
# Grade helper
# ===========================================================================

GRADERS_RE = re.compile(
    r'\b(PSA|BGS|SGC|CGC|CSG|HGA|GAI|GMA)\s*(\d+\.?\d*)', re.IGNORECASE
)

def parse_grade(title: str) -> str:
    m = GRADERS_RE.search(title)
    if m:
        return f"{m.group(1).upper()} {m.group(2)}"
    return "Raw"

# ===========================================================================
# eBay search
# ===========================================================================

def search_ebay(cat: dict, listing_type: str) -> list:
    """listing_type: 'bin' or 'auction'"""
    token = get_ebay_token()
    items = []

    for page in range(2):
        if listing_type == "bin":
            filter_str = "buyingOptions:{FIXED_PRICE},price:[10..]"
            sort       = "-newlyListed"
        else:
            ten_min    = (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            filter_str = f"buyingOptions:{{AUCTION}},itemEndDate:[..{ten_min}],price:[5..]"
            sort       = "-endingSoonest"

        params = {
            "q":            cat["ebay_query"],
            "category_ids": cat["ebay_category"],
            "aspect_filter": cat["aspect_filter"],
            "limit":        "100",
            "offset":       str(page * 100),
            "sort":         sort,
            "filter":       filter_str,
        }

        time.sleep(0.5)
        resp = requests.get(
            EBAY_SEARCH_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            },
            params=params,
        )

        if not resp.ok:
            log.error(f"eBay error {resp.status_code}: {resp.text[:200]}")
            break

        batch = resp.json().get("itemSummaries", [])
        items.extend(batch)
        log.info(f"  Fetched {len(batch)} {listing_type} items (page {page + 1})")
        if len(batch) < 100:
            break

    log.info(f"  Total {listing_type}: {len(items)} items")
    return items

# ===========================================================================
# Discord
# ===========================================================================

def post_discord(webhook_url: str, embed: dict):
    if not webhook_url:
        return
    try:
        resp = requests.post(
            webhook_url,
            json={"embeds": [embed]},
            headers={"Content-Type": "application/json"},
        )
        if not resp.ok:
            log.error(f"Discord error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Discord post failed: {e}")

# ===========================================================================
# Process items
# ===========================================================================

def process_items(items: list, listing_type: str, sport: str, cat: dict):
    if not items:
        return

    webhook = os.getenv(cat["discord_env"], "")
    cache   = _player_card_cache.setdefault(sport, {})
    t0      = time.time()

    log.info(f"  --- processItems: type={listing_type} count={len(items)} ---")

    # -----------------------------------------------------------------------
    # Step 1: player matching
    # -----------------------------------------------------------------------
    title_to_player = {}

    for item in items:
        title = item.get("title", "")
        if not title:
            continue

        if parse_grade(title) != "Raw":
            continue

        candidates = get_candidate_players(title, sport)
        if not candidates:
            log.info(f"  NO_CANDIDATE [no_player_match]: {title}")
            continue

        title_to_player[title] = candidates[0]
        log.info(f"  PLAYER MATCH: \"{title}\" → {candidates[0]}")

    log.info(f"  Step 1: {time.time()-t0:.1f}s — matched {len(title_to_player)} items")

    if not title_to_player:
        return

    # -----------------------------------------------------------------------
    # Step 2: fetch card data
    # -----------------------------------------------------------------------
    t1 = time.time()
    unique_players = list(set(title_to_player.values()))
    fetch_player_cards(unique_players, sport)
    log.info(f"  Step 2: {time.time()-t1:.1f}s — fetched {len(unique_players)} players")

    # -----------------------------------------------------------------------
    # Step 3: score and alert
    # -----------------------------------------------------------------------
    t2 = time.time()

    for item in items:
        title          = item.get("title", "")
        matched_player = title_to_player.get(title)
        if not matched_player:
            continue

        cards = cache.get(matched_player, [])
        if not cards:
            continue

        if parse_grade(title) != "Raw":
            continue

        parsed = parse_title(title)

        if not parsed["ebay_year"] and not parsed["ebay_card_num"]:
            log.info(f"  NO_CANDIDATE [no_year_or_cardnum]: {title}")
            continue

        # Score every raw card for this player
        matched_card = None
        best_score   = 0.0

        for card in cards:
            if card.get("grade") != "Raw":
                continue
            s = score_card_match(parsed, card)
            if s < 0:
                continue
            if s > best_score:
                best_score   = s
                matched_card = card

        if not matched_card or best_score < MIN_MATCH_SCORE:
            log.info(
                f"  NO_CARD: \"{title}\" → player={matched_player} "
                f"best_score={best_score:.0f} cards_checked={len(cards)}"
            )
            continue

        log.info(
            f"  CARD MATCH: \"{title}\" → {matched_card['canonical_name']} "
            f"(score={best_score:.0f} variation={matched_card.get('variation') or 'base'})"
        )

        # Price check
        if listing_type == "bin":
            price = float((item.get("price") or {}).get("value", 0))
        else:
            price = float(
                (item.get("currentBidPrice") or item.get("price") or {}).get("value", 0)
            )

        if price <= 0:
            continue

        market_price = float(matched_card.get("market_price") or 0)
        if market_price <= 0:
            continue
        if price >= market_price * DISCOUNT_THRESHOLD:
            log.info(f"  PRICE SKIP: {matched_card['canonical_name']} | eBay: ${price:.2f} | Market: ${market_price:.2f} | Threshold: ${market_price * DISCOUNT_THRESHOLD:.2f}")
            continue

        savings_pct = round((market_price - price) / market_price * 100)
        savings_dol = market_price - price

        if savings_dol < MIN_SAVINGS:
            continue
        if savings_pct > MAX_SAVINGS_PCT:
            continue

        url = item.get("itemWebUrl", "")
        if url in seen_urls:
            continue
        seen_urls.add(url)

        log.info(
            f"  DEAL: {matched_card['canonical_name']} | "
            f"eBay: ${price:.2f} | Market: ${market_price:.2f} | Save: {savings_pct}%"
        )

        # Build Discord embed
        market_source = "30d avg" if matched_card.get("avg_price_30d") else "⚠️ last sale only"
        last_sale_raw = matched_card.get("last_sale_date")
        last_sale     = last_sale_raw[:10] if last_sale_raw else "unknown"

        type_label = "🏷️ Buy It Now" if listing_type == "bin" else "⏱️ Auction"
        has_30d    = bool(matched_card.get("avg_price_30d"))
        if listing_type == "bin":
            color = 0x3498db if has_30d else 0xf39c12
        else:
            color = 0x2ecc71 if has_30d else 0xf1c40f

        set_display = " ".join(filter(None, [
            str(matched_card.get("set_year") or ""),
            matched_card.get("set_name") or "",
        ])).strip()

        embed = {
            "title": (
                f"🚨 {type_label} – {cat['emoji']} {sport}: "
                f"{matched_card['canonical_name']} (Raw)"
            ),
            "description": (
                f"eBay: ${price:.2f} | "
                f"Market: ${market_price:.2f} ({market_source}) | "
                f"Save: {savings_pct}% (${savings_dol:.2f})\n"
                f"Set: {set_display or 'unknown'}\n"
                f"Last Sale: {last_sale}"
            ),
            "url":       url,
            "color":     color,
            "thumbnail": {"url": (item.get("image") or {}).get("imageUrl", "")},
            "footer":    {
                "text": (
                    f"Match score: {best_score:.0f} | "
                    f"Variation: {matched_card.get('variation') or 'base'}"
                )
            },
        }

        post_discord(webhook, embed)
        time.sleep(0.3)

    log.info(f"  Step 3: {time.time()-t2:.1f}s")

# ===========================================================================
# Main scan
# ===========================================================================

def run_scan():
    log.info("=" * 60)
    log.info(f"Starting scan — {datetime.utcnow().isoformat()}")
    log.info("=" * 60)

    for cat_name, cat in CATEGORIES.items():
        sport = cat["sport"]
        log.info(f"\n--- Scanning {cat_name} ---")

        try:
            load_player_index(sport)

            bin_items     = search_ebay(cat, "bin")
            auction_items = search_ebay(cat, "auction")

            process_items(bin_items,     "bin",     sport, cat)
            process_items(auction_items, "auction", sport, cat)

        except Exception as e:
            log.error(f"Error scanning {cat_name}: {e}", exc_info=True)
            continue

        time.sleep(2)

    log.info("\nScan complete.")

# ===========================================================================
# Entry point — runs once immediately then every 10 minutes
# ===========================================================================

if __name__ == "__main__":
    log.info("Card price scanner starting...")

    run_scan()

    schedule.every(10).minutes.do(run_scan)

    while True:
        schedule.run_pending()
        time.sleep(30)
