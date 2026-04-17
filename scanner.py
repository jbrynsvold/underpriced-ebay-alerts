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
BIN_WEBHOOK     = "https://discord.com/api/webhooks/1494769438099243058/HujuFzoBdB0R3643kYj3_d1ydBTWHlwDSD90Nt3ZkmVLn_4HvEMBMl8jTH6ee2rXEngV"

DISCOUNT_THRESHOLD = 1
MIN_SAVINGS        = 3
MAX_SAVINGS_PCT    = 60
MIN_MATCH_SCORE    = 65
MIN_WORD_LEN       = 4

# Minimum actual current price — applied in-process on real bid/listing price,
# not just the eBay query filter (which uses opening bid for auctions).
MIN_PRICE_BIN     = 10.00
MIN_PRICE_AUCTION =  5.00

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
    "reverse", "fullart", "altart", "promo", "kaboom", "horizontal",
}

# Colors that must match exactly between DB variation and eBay title.
# If the DB card has one of these and the title has a *different* one, hard reject.
VARIATION_COLORS = {
    "black", "white", "red", "blue", "green", "yellow", "orange", "purple",
    "pink", "brown", "grey", "gray", "gold", "silver", "bronze",
    "aqua", "teal", "cyan", "magenta", "indigo", "violet", "maroon",
    "platinum", "copper", "ruby", "sapphire", "emerald", "onyx",
}

# Parallel types that must match exactly between DB variation and eBay title.
# Prevents "Red" matching "Red Wave" or "Red Pulsar" etc.
PARALLEL_TYPES = {
    "wave", "pulsar", "refractor", "prizm", "holo", "foil",
    "atomic", "laser", "cracked", "shimmer", "disco", "mosaic",
    "scope", "ice", "lava", "choice", "tiger", "snake",
}

# Pokemon generation prefixes — optional in matching, don't penalize if missing
POKEMON_GENERATION_TOKENS = {
    "scarlet", "violet", "sword", "shield", "sun", "moon",
    "black", "white", "diamond", "pearl", "heartgold", "soulsilver",
    "winds", "waves", "mega", "evolution",
}

# Words that are generic enough to appear in any card title — not sub-product identifiers
TITLE_NOISE_WORDS = SET_NOISE_WORDS | {
    "the", "and", "for", "with", "from", "card", "cards",
    "rookie", "auto", "parallel", "insert", "rare", "ultra",
    "super", "secret", "common", "uncommon", "holo", "foil",
    "mint", "near", "excellent", "good", "poor", "lot",
    "single", "base", "chrome", "topps", "panini", "upper", "deck",
    "bowman", "fleer", "score", "donruss", "leaf", "pacific",
    "jersey", "patch", "relic", "refractor", "prizm", "optic",
    "select", "mosaic", "chronicles", "prestige", "hoops",
    "numbered", "print", "run", "ssp", "variation", "short",
    "rc", "sp", "qc", "bgs", "psa", "sgc", "cgc",
    "raw", "ungraded", "mint", "gem", "psa10", "psa9",
    "first", "edition", "1st", "2nd", "gold", "silver",
    "blue", "red", "green", "orange", "purple", "pink", "black", "white",
    "nmt", "vgex", "vg", "poor", "fair",
    "lot", "pack", "box", "set", "complete", "full",
    "new", "old", "vintage", "modern", "classic",
    "sharp", "clean", "nice", "great", "perfect",
    "look", "see", "buy", "free", "ship", "fast",
}

# Panini sub-brands — if title has one not in DB, reject
PANINI_BRANDS = {
    "prizm", "select", "optic", "mosaic", "chronicles",
    "contenders", "donruss", "prestige", "spectra", "flawless",
    "obsidian", "phoenix", "absolute", "certified", "playbook",
    "national", "treasures", "immaculate", "noir", "impeccable",
    "flux", "hoops", "score", "elite", "origins",
    "revolution", "crown", "royale", "luminance",
}

TOPPS_BRANDS = {
    "finest",      # Topps Finest
    "chrome",      # Topps Chrome
    "heritage",    # Topps Heritage
    "archive",     # Topps Archive
    "archives",    # Topps Archives
    "tribute",     # Topps Tribute
    "stadium",     # Topps Stadium Club
    "gypsy",       # Topps Gypsy Queen
    "ginter",       # Allen & Ginter
    "dynasty",     # Topps Dynasty
    "museum",      # Topps Museum Collection
    "gallery",     # Topps Gallery
    "inception",   # Topps Inception
    "sterling",    # Topps Sterling
    "definitive",  # Topps Definitive
    "transcendent",# Topps Transcendent
    "luminaries",  # Topps Luminaries
}

# Soft keyword filter applied in-process (supplements eBay query exclusions)
EXCL_KEYWORDS = [
    "you pick", "lot of", "choose your", "complete your set", "u pick",
    "card lot", "pack of", "box of", "blaster", "hobby box",
    "factory sealed", "sealed box", "sealed pack", "complete set",
    "mystery", "random", "bundle", "collection", "bulk",
    "pick a card", "pick your card", "you choose", "choose from",
    "art card", "fan art", "custom card", "custom slab",
    "uncut", "panels",
    "tcg pocket", "pocket",
    "japanese", "chinese", "korean",
    "pick your player", "pick & choose", "pick from list",
    "fill your set", "build a lot", "set break",
    "card pick", "singles",
    "see description", "see desc", "see photos", "read description",
    # Pokemon condition filters — standalone LP/HP/DMG only, not NM/LP combos
    " hp ", " dmg ", "damaged", "heavily played", "poor condition",
]

# Pokemon-specific condition filters applied only to TCG listings
POKEMON_CONDITION_EXCL = [
    " lp ", "lightly played", " hp ", "heavily played",
    " dmg", "damaged", "poor condition",
]

# Japanese set codes that slip through the language filter
JAPANESE_SET_CODE_RE = re.compile(
    r'\b(sv\d+[a-zA-Z]*|SV-P|SV[0-9]+[a-zA-Z]|s\d+[a-zA-Z]|SM\d+|XY\d+|BW\d+)\b'
)

EXCL_SPORTS = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"set lot" -"select a card" -"pick a card" -"pick your card"'
    ' -autograph -auto -signed -"art card" -"custom card"'
    ' -"pick your player" -"pick & choose" -"pick from list"'
    ' -"fill your set" -"complete a set" -"complete the set"'
    ' -"take your pick" -"set break"'
)

EXCL_TCG = (
    '-"you pick" -"lot of" -"choose your" -"complete your set" -"u pick"'
    ' -"card lot" -"pack of" -"box of" -"blaster" -"hobby box"'
    ' -"factory sealed" -"sealed box" -"sealed pack" -"complete set"'
    ' -"mystery" -"random" -"bundle" -"bulk"'
    ' -"pick a card" -"pick your card" -"you choose" -"choose from"'
    ' -"art card" -"fan art" -"custom card" -"custom slab"'
    ' -"uncut" -"panels" -"tcg pocket" -"pocket"'
    ' -"japanese" -"chinese" -"korean"'
    ' -PSA -BGS -SGC -CGC -graded -autograph -auto'
    ' -"pick your player" -"pick & choose" -"pick from list"'
    ' -"fill your set" -"complete a set" -"complete the set"'
    ' -"take your pick" -"set break"'
    ' -"deck core" -"deck set" -"card deck" -"unopened" -"insert set"'
)

REQUIRED_SET_TOKENS = {
    # Original
    "sapphire", "inception", "heritage", "luminance",
    "flawless", "sterling", "zenith", "stellar",
    # Panini products
    "obsidian", "immaculate", "spectra", "playbook",
    "chronicles", "absolute", "threads", "revolution",
    "noir", "impeccable", "contenders", "certified",
    # Upper Deck products
    "exquisite", "artifacts", "masterpieces", "ovation",
    "parkhurst", "goodwin", "authentix", "trilogy",
    # Topps products
    "archives", "tribute", "dynasty", "museum",
    "gallery", "gypsy", "finest", "stadium",
    # Other distinct products
    "flair", "illusions", "mystique", "hardcourt",
    "encased", "transcendent", "definitive", "timeless",
    "gridiron", "tiffany", "showcase", "throwback",
    "dominion", "allure",
}

# City/partial team name fragments that pollute the player index
CITY_FRAGMENTS = {
    "Los Angeles", "New York", "San Francisco", "Washington Senators",
    "Washington", "Chicago", "Boston", "Oakland", "Detroit",
    "Cleveland", "Seattle", "Minnesota", "Houston", "Atlanta",
    "Philadelphia", "Cincinnati", "Milwaukee", "Pittsburgh",
    "San Diego", "Colorado", "Arizona", "Miami", "Tampa Bay",
    "Kansas City", "St. Louis", "Toronto", "Baltimore",
    "Golden State", "New Orleans", "Oklahoma City", "Salt Lake",
    "Las Vegas", "Sacramento", "Memphis", "Portland",
}

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

# Sport labels used in embed titles for the combined OtherSports category
OTHER_SPORTS_EMOJIS = {
    "Soccer":    "⚽",
    "UFC/MMA":   "🥊",
    "Golf":      "⛳",
    "Formula 1": "🏎️",
}

CATEGORIES = {
    "MLB": {
        "sport":         "MLB",
        "ebay_query":    f"baseball {EXCL_SPORTS}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Baseball}",
        "discord_env":   "DISCORD_WEBHOOK_MLB_ALERTS",
        "emoji":         "⚾",
        "color":         0x002D72,
        "is_tcg":        False,
    },
    "NBA": {
        "sport":         "NBA",
        "ebay_query":    f"basketball {EXCL_SPORTS}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Basketball}",
        "discord_env":   "DISCORD_WEBHOOK_NBA_ALERTS",
        "emoji":         "🏀",
        "color":         0xC9082A,
        "is_tcg":        False,
    },
    "NFL": {
        "sport":         "NFL",
        "ebay_query":    f"football {EXCL_SPORTS}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Football}",
        "discord_env":   "DISCORD_WEBHOOK_NFL_ALERTS",
        "emoji":         "🏈",
        "color":         0x013369,
        "is_tcg":        False,
    },
    "NHL": {
        "sport":         "NHL",
        "ebay_query":    f"hockey {EXCL_SPORTS}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Ice Hockey}",
        "discord_env":   "DISCORD_WEBHOOK_NHL_ALERTS",
        "emoji":         "🏒",
        "color":         0x000000,
        "is_tcg":        False,
    },
    "Pokemon": {
        "sport":         "Pokemon",
        "ebay_query":    f'pokemon -"magic the gathering" -MTG -yugioh -lorcana -"one piece" -"dragon ball" -vanguard {EXCL_TCG}',
        "ebay_category": "183454",
        "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_env":   "DISCORD_WEBHOOK_POKEMON_ALERTS",
        "emoji":         "⚡",
        "color":         0xFFCC00,
        "is_tcg":        True,
    },
    "Yu-Gi-Oh": {
        "sport":         "Yu-Gi-Oh",
        "ebay_query":    f'yugioh -pokemon -lorcana -"one piece" -"dragon ball" -vanguard {EXCL_TCG}',
        "ebay_category": "183454",
        "aspect_filter": "categoryId:183454,Graded:{No}",
        "discord_env":   "DISCORD_WEBHOOK_YUGIOH_ALERTS",
        "emoji":         "🃏",
        "color":         0x6A0DAD,
        "is_tcg":        True,
    },
    # Soccer, UFC/MMA, Golf, and Formula 1 combined into a single eBay search.
    # All alerts go to one "other-sports" Discord channel.
    # Player matching runs against each sport's index in sequence.
    "OtherSports": {
        "sports":        ["Soccer", "UFC/MMA", "Golf", "Formula 1"],
        "ebay_query":    f"card {EXCL_SPORTS}",
        "ebay_category": "261328",
        "aspect_filter": "categoryId:261328,Sport:{Soccer|Mixed Martial Arts|Golf|Racing}",
        "discord_env":   "DISCORD_WEBHOOK_OTHER_SPORTS_ALERTS",
        "emoji":         "🏅",
        "color":         0x5865F2,
        "is_tcg":        False,
        "is_multi_sport": True,
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
# State
# ===========================================================================

from supabase.client import ClientOptions
supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(postgrest_client_timeout=30)
)

_ebay_token        = None
_ebay_token_expiry = 0

_player_card_cache: dict = {}
_word_to_players:   dict = {}
_cleaned_to_original: dict = {}
_player_index_loaded: set = set()

# ===========================================================================
# Helpers
# ===========================================================================

def fmt(n: float) -> str:
    return f"${n:,.2f}"

def fmt_end_time(iso_str: str) -> str:
    """Convert eBay itemEndDate ISO string to Central time (CDT/CST)."""
    try:
        dt      = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now_utc = datetime.now(timezone.utc)
        year    = now_utc.year
        # DST: second Sunday in March → first Sunday in November
        dst_start = datetime(year, 3,  8, 2, tzinfo=timezone.utc) + timedelta(days=(6 - datetime(year, 3,  8).weekday()) % 7)
        dst_end   = datetime(year, 11, 1, 2, tzinfo=timezone.utc) + timedelta(days=(6 - datetime(year, 11, 1).weekday()) % 7)
        is_cdt    = dst_start <= now_utc < dst_end
        offset    = timedelta(hours=-5 if is_cdt else -6)
        dt_ct     = dt.astimezone(timezone(offset))
        suffix    = "CDT" if is_cdt else "CST"
        return dt_ct.strftime("%-I:%M %p " + suffix)
    except Exception:
        return iso_str

def get_item_url(item: dict) -> str:
    """
    Build a stable direct eBay item URL from itemId.
    itemWebUrl redirects to evergreen/similar listings once an auction ends.
    The /itm/{numeric_id} format is stable and always points to the actual listing.
    """
    item_id = item.get("itemId", "")
    if item_id:
        numeric = re.search(r'\d{8,}', item_id)
        if numeric:
            return f"https://www.ebay.com/itm/{numeric.group()}"
    return item.get("itemWebUrl", "")

def has_alerted(url: str) -> bool:
    item_id = re.search(r'/itm/(\d+)', url)
    item_id = item_id.group(1) if item_id else url
    result = supabase.table("alert_log") \
        .select("item_url") \
        .eq("item_url", item_id) \
        .gte("alerted_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
        .execute()
    return bool(result.data)

def record_alert(url: str):
    item_id = re.search(r'/itm/(\d+)', url)
    item_id = item_id.group(1) if item_id else url
    supabase.table("alert_log") \
        .upsert({"item_url": item_id, "scanner": "underpriced", "alerted_at": datetime.now(timezone.utc).isoformat()}) \
        .execute()

# ===========================================================================
# Title normalization
# ===========================================================================

TITLE_EXPANSIONS = [
    # Pokemon — current/modern series
    (r'\bS&V\b',                        'scarlet violet'),
    (r'\bScarlet\s*&\s*Violet\b',       'scarlet violet'),
    (r'\bSV\s+(?=\d)',                  'scarlet violet '),
    (r'\bSWSH\b',                       'sword shield'),
    (r'\bSword\s*&\s*Shield\b',         'sword shield'),
    (r'\bS&S\b',                        'sword shield'),
    (r'\bSun\s*&\s*Moon\b',             'sun moon'),
    (r'\bS&M\b',                        'sun moon'),
    (r'\bME:\s*',                       'mega evolution '),
    (r'\bMega\s+Evo\b',                 'mega evolution'),
    (r'\bB&W\b',                        'black white'),
    (r'\bBlack\s*&\s*White\b',          'black white'),
    (r'\bBW\b(?=\s)',                   'black white'),
    (r'\bD&P\b',                        'diamond pearl'),
    (r'\bDiamond\s*&\s*Pearl\b',        'diamond pearl'),
    (r'\bHG\s*SS\b',                    'heartgold soulsilver'),
    (r'\bHeartGold\s*&?\s*SoulSilver\b','heartgold soulsilver'),
    (r'\bEvo\s+Skies\b',               'evolving skies'),
    (r'\bPrismatic\s+Evo\b',           'prismatic evolutions'),
    # Pokemon — Winds & Waves generation
    (r'\bW&W\b',                        'winds waves'),
    (r'\bWinds\s*&\s*Waves\b',          'winds waves'),
    (r'\bWW\b(?=\s+\d)',               'winds waves '),
    # Sports — Upper Deck
    (r'\bUD\b',                         'upper deck'),
    (r'\bU\.D\.\b',                     'upper deck'),
    # Sports — Bowman
    (r'\bBCP\b',                        'bowman chrome prospects'),
    (r'\bBDP\b',                        'bowman draft picks'),
    (r'\bBC\b(?=\s+(?:Pros|Draft|Prospect))', 'bowman chrome'),
    # Sports — Topps
    (r'\bA&G\b',                        'allen ginter'),
    (r'\bSP\s+Auth\b',                 'sp authentic'),
    # General
    (r'\s*&\s*',                        ' '),
]

def normalize_title(title: str) -> str:
    result = title
    for pattern, replacement in TITLE_EXPANSIONS:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result

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
            "Content-Type":  "application/x-www-form-urlencoded",
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

def set_tokens(set_name: str, is_tcg: bool = False) -> tuple:
    """Returns (required_tokens, optional_tokens)."""
    if is_tcg:
        # Strip leading year from TCG set names — "2025 Pokemon Scarlet Violet X" → "Pokemon Scarlet Violet X"
        # Also exclude pokemon/yugioh/japanese brand words — they add no signal since we already
        # know the sport and eBay titles don't say "japanese" (filtered out upstream).
        TCG_NOISE     = SET_NOISE_WORDS | {"pokemon", "yugioh", "japanese"}
        all_tokens    = [t for t in tokenize(set_name) if t not in TCG_NOISE and not re.match(r'^\d{4}$', t)]
        year_tokens   = [t for t in tokenize(set_name) if re.match(r'^\d{4}$', t)]
        gen_tokens    = [t for t in all_tokens if t in POKEMON_GENERATION_TOKENS]
        unique_tokens = [t for t in all_tokens if t not in POKEMON_GENERATION_TOKENS]
        if unique_tokens:
            # Named sub-set (e.g. "Prismatic Evolutions", "Surging Sparks") —
            # unique name words are required, generation prefix and year are optional bonuses
            required = unique_tokens
            optional = gen_tokens + year_tokens
        else:
            # Generation-only set (e.g. just "Scarlet Violet") — require generation tokens
            required = gen_tokens
            optional = year_tokens
    else:
        all_tokens = [t for t in tokenize(set_name) if t not in SET_NOISE_WORDS]
        required   = all_tokens
        optional   = []
    return required, optional

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
        cleaned = strip_suffix(name).strip().lower()
        cleaned_map[cleaned] = name
        for word in cleaned.split():
            if len(word) >= MIN_WORD_LEN:
                word_map.setdefault(word, set()).add(name)
    _word_to_players[sport]     = word_map
    _cleaned_to_original[sport] = cleaned_map
    _player_index_loaded.add(sport)
    log.info(f"{sport}: loaded {len(cleaned_map)} players, {len(word_map)} index words")

def get_candidate_players(title: str, sport: str) -> list:
    title_lower = normalize_title(title).lower()
    word_map    = _word_to_players.get(sport, {})
    title_words = [w for w in re.split(r'\W+', title_lower) if len(w) >= MIN_WORD_LEN]
    candidate_set = set()
    for word in title_words:
        for player in word_map.get(word, []):
            candidate_set.add(player)
    if not candidate_set:
        return []
    matches = []
    for original_name in candidate_set:
        if original_name in TEAM_NAMES:
            continue
        if original_name in CITY_FRAGMENTS:
            continue
        cleaned = strip_suffix(original_name).strip().lower()
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
                    "card_number, last_sale_date, set_name, set_year, variation, sport") \
            .in_("player_name", uncached) \
            .eq("sport", sport) \
            .limit(50000) \
            .execute()
    except Exception as e:
        log.error(f"mv_card_metrics error: {e}")
        for p in uncached:
            cache[p] = []
        return
    rows_returned = len(metrics.data) if metrics.data else 0
    log.info(f"  DB fetch: {len(uncached)} players → {rows_returned} rows")
    if rows_returned >= 1000 and rows_returned % 100 == 0:
        log.warning(f"  DB fetch may be truncated — got exactly {rows_returned} rows for {len(uncached)} players")

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
    normalized  = normalize_title(title)
    title_lower = normalized.lower()
    ebay_year   = None
    ebay_year2  = None

    full_year = re.search(r'\b(19|20)\d{2}\b', normalized)
    if full_year:
        ebay_year = int(full_year.group())
        hockey_year = re.search(r'\b(19|20)(\d{2})[-/](\d{2})\b', normalized)
        if hockey_year:
            suffix = int(hockey_year.group(3))
            ebay_year2 = 2000 + suffix if suffix <= 30 else 1900 + suffix
    else:
        short = re.search(r'\b(\d{2})-(\d{2})\b', normalized)
        if short:
            y1, y2 = int(short.group(1)), int(short.group(2))
            if (y1 >= 90 or y1 <= 26) and (y2 >= 90 or y2 <= 26):
                ebay_year  = (1900 if y1 >= 90 else 2000) + y1
                ebay_year2 = 2000 + y2

    card_num_match = re.search(r'#\s*(\w+)', normalized)
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

    set_year    = card.get("set_year")
    set_name    = card.get("set_name") or ""
    variation   = (card.get("variation") or "").strip()
    db_card_num = (card.get("card_number") or "").lstrip("0")
    is_base     = variation.lower() in BASE_VARIATIONS
    sport       = card.get("sport", "")
    is_tcg      = sport in {"Pokemon", "Yu-Gi-Oh", "Other TCG", "Non-Sport Vintage"}

    # --- Auto/autograph hard filter ---
    combined_db   = (set_name + " " + variation).lower()
    db_is_auto    = any(w in combined_db for w in ["autograph", " auto", "/a "])
    title_is_auto = any(w in title_lower for w in ["autograph", "/a ", " auto "])
    if db_is_auto and not title_is_auto:
        return -1.0
    if title_is_auto and not db_is_auto:
        return -1.0

    # --- X-Fractor hard filter ---
    db_is_xfractor    = "x-fractor" in combined_db or "xfractor" in combined_db
    title_is_xfractor = "x-fractor" in title_lower or "xfractor" in title_lower
    if db_is_xfractor and not title_is_xfractor:
        return -1.0
    if title_is_xfractor and not db_is_xfractor:
        return -1.0

    # --- Panini sub-brand hard filter (sports only) ---
    if not is_tcg:
        title_brands = PANINI_BRANDS & set(tokenize(title_lower))
        db_brands    = PANINI_BRANDS & set(tokenize(combined_db))
        if title_brands - db_brands:
            return -1.0
     # --- Topps brand hard filter (sports only) ---
    if not is_tcg:
        title_topps = TOPPS_BRANDS & set(tokenize(title_lower))
        db_topps    = TOPPS_BRANDS & set(tokenize(combined_db))
        if title_topps - db_topps:
            return -1.0       

    # --- Year hard filter (sports only — TCG titles often omit year) ---
    preferred_year = ebay_year2 if ebay_year2 else ebay_year
    if not is_tcg and set_year and (ebay_year or ebay_year2):
        if preferred_year != set_year and ebay_year != set_year:
            return -1.0

    # --- Card number hard filter ---
    if ebay_card_num and db_card_num:
        if ebay_card_num != db_card_num:
            return -1.0

    score = 0.0

    # --- Set name matching ---
    set_name_normalized = normalize_title(set_name)
    required_tokens, optional_tokens = set_tokens(set_name_normalized, is_tcg=is_tcg)
    required_set_distinguishers = REQUIRED_SET_TOKENS & set(required_tokens)
    if required_set_distinguishers:
        if not all(t in title_lower for t in required_set_distinguishers):
            return -1.0

    if required_tokens:
        found_req   = [t for t in required_tokens if t in title_lower]
        match_ratio = len(found_req) / len(required_tokens)
        score += match_ratio * 60
        if match_ratio == 1.0:
            score += 20
        elif match_ratio < 0.5:
            return -1.0
        elif match_ratio < 0.6:
            score -= 10
    else:
        score += 10

    # Optional generation tokens give bonus if present
    if optional_tokens:
        found_opt = [t for t in optional_tokens if t in title_lower]
        score += (len(found_opt) / len(optional_tokens)) * 15

    # --- Variation matching ---
    if not is_base:
        v_tokens = variation_tokens(variation)

        # --- Color hard filter ---
        db_colors    = VARIATION_COLORS & set(v_tokens)
        title_colors = VARIATION_COLORS & set(tokenize(title_lower))
        if db_colors:
            if not (db_colors & title_colors):
                return -1.0
            if title_colors - db_colors:
                return -1.0

        # --- Parallel type hard filter ---
        db_parallels    = PARALLEL_TYPES & set(v_tokens)
        title_parallels = PARALLEL_TYPES & set(tokenize(title_lower))
        if db_parallels:
            if not (db_parallels & title_parallels):
                return -1.0
            if title_parallels - db_parallels:
                return -1.0

        if v_tokens:
            found_v = [t for t in v_tokens if t in title_lower]
            ratio_v = len(found_v) / len(v_tokens)
            if ratio_v <= 0.5:
                return -1.0
            score += ratio_v * 60
            if len(found_v) == len(v_tokens):
                score += 20
        else:
            if variation.lower() in title_lower:
                score += 20
            else:
                score -= 10
    else:
        title_tokens = set(tokenize(title_lower))
        if title_tokens & STRONG_NON_BASE:
            score -= 40

    # --- Canonical sub-product check (all cards) ---
    # Words in canonical_name beyond set name + player name are sub-product
    # identifiers (e.g. "Thunderbirds", "Volcanix", "Pink Fluorescent").
    # If more than half are missing from the eBay title, reject the match.
    canonical = (card.get("canonical_name") or "").lower()
    set_name_lower = set_name.lower()
    player_name_lower = (card.get("player_name") or "").lower()
    canonical_extra = [
        t for t in tokenize(canonical)
        if t not in tokenize(set_name_lower)
        and t not in tokenize(player_name_lower)
        and t not in SET_NOISE_WORDS
        and len(t) >= 4
    ]
    if canonical_extra:
        missing = [t for t in canonical_extra if t not in title_lower]
        if missing and len(missing) / len(canonical_extra) >= 0.5:
            return -1.0

    # Bonus when card numbers match
    if db_card_num and ebay_card_num and db_card_num == ebay_card_num:
        score += 15

    # --- Year bonus (TCG: optional bonus only, not a hard filter) ---
    if set_year and (preferred_year == set_year or ebay_year == set_year):
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
    token = get_ebay_token()
    items = []
    pages = 1 if listing_type == "bin" else 3
    for page in range(pages):
        if listing_type == "bin":
            filter_str = f"buyingOptions:{{FIXED_PRICE}},price:[{int(MIN_PRICE_BIN)}..]"
            sort       = "-newlyListed"
        else:
            ten_min    = (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            filter_str = f"buyingOptions:{{AUCTION}},itemEndDate:[..{ten_min}],price:[{int(MIN_PRICE_AUCTION)}..]"
            sort       = "-endingSoonest"
        params = {
            "q":             cat["ebay_query"],
            "category_ids":  cat["ebay_category"],
            "limit":         "100",
            "offset":        str(page * 100),
            "sort":          sort,
            "filter":        filter_str,
            "fieldgroups":   "EXTENDED",
        }
        if cat.get("aspect_filter"):
            params["aspect_filter"] = cat["aspect_filter"]
        time.sleep(0.5)
        resp = requests.get(
            EBAY_SEARCH_URL,
            headers={
                "Authorization":           f"Bearer {token}",
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
# Process items — single sport
# ===========================================================================

def process_items(items: list, listing_type: str, sport: str, cat: dict):
    """Process eBay items for a single-sport category."""
    if not items:
        return

    is_tcg  = cat.get("is_tcg", False)
    webhook = os.getenv(cat["discord_env"], "")
    cache   = _player_card_cache.setdefault(sport, {})
    t0      = time.time()

    log.info(f"  --- processItems: type={listing_type} sport={sport} count={len(items)} ---")

    # Step 1: pre-filter + player matching
    title_to_player = {}
    for item in items:
        title = item.get("title", "")
        if not title:
            continue

        if listing_type == "bin":
            raw_price = float((item.get("price") or {}).get("value", 0))
        else:
            raw_price = float(
                (item.get("currentBidPrice") or item.get("price") or {}).get("value", 0)
            )
        floor = MIN_PRICE_BIN if listing_type == "bin" else MIN_PRICE_AUCTION
        if raw_price < floor:
            log.info(f"  PRICE_FLOOR [{raw_price:.2f} < {floor}]: {title}")
            continue

        if parse_grade(title) != "Raw":
            continue

        title_lower_check = " " + title.lower() + " "
        if any(kw in title_lower_check for kw in EXCL_KEYWORDS):
            continue

        # Pokemon-specific condition filter — standalone LP/HP/DMG but not NM/LP combos
        if is_tcg and sport == "Pokemon":
            if any(kw in title_lower_check for kw in POKEMON_CONDITION_EXCL):
                # Allow NM/LP and NM-LP combos through
                nm_lp = bool(re.search(r'\bnm[\s/\-]lp\b', title.lower()))
                if not nm_lp:
                    log.info(f"  CONDITION_FILTER: {title}")
                    continue

        if is_tcg and JAPANESE_SET_CODE_RE.search(title):
            continue

        parsed = parse_title(title)
        if not is_tcg and not parsed["ebay_year"] and not parsed["ebay_card_num"]:
            log.info(f"  NO_CANDIDATE [no_year_or_cardnum]: {title}")
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

    # Step 2: fetch card data
    t1 = time.time()
    fetch_player_cards(list(set(title_to_player.values())), sport)
    log.info(f"  Step 2: {time.time()-t1:.1f}s")

    # Step 3: score and alert
    t2    = time.time()
    emoji = cat.get("emoji", "🏅")
    color = cat.get("color", 0x5865F2)

    _score_and_alert(items, title_to_player, cache, listing_type, sport, emoji, color, webhook)
    log.info(f"  Step 3: {time.time()-t2:.1f}s")


# ===========================================================================
# Process items — multi-sport (OtherSports)
# ===========================================================================

def process_items_multi(items: list, listing_type: str, cat: dict):
    """
    Process eBay items for the combined OtherSports category.
    Runs each sport's player index against the same item list,
    takes the first match found across all sports.
    """
    if not items:
        return

    sports  = cat["sports"]
    webhook = os.getenv(cat["discord_env"], "")
    t0      = time.time()

    log.info(f"  --- processItems (multi): type={listing_type} sports={sports} count={len(items)} ---")

    # Pre-filter once — same rules, sport-agnostic
    filtered = []
    for item in items:
        title = item.get("title", "")
        if not title:
            continue
        if listing_type == "bin":
            raw_price = float((item.get("price") or {}).get("value", 0))
        else:
            raw_price = float(
                (item.get("currentBidPrice") or item.get("price") or {}).get("value", 0)
            )
        floor = MIN_PRICE_BIN if listing_type == "bin" else MIN_PRICE_AUCTION
        if raw_price < floor:
            continue
        if parse_grade(title) != "Raw":
            continue
        title_lower_check = " " + title.lower() + " "
        if any(kw in title_lower_check for kw in EXCL_KEYWORDS):
            continue
        parsed = parse_title(title)
        if not parsed["ebay_year"] and not parsed["ebay_card_num"]:
            continue
        filtered.append(item)

    log.info(f"  Pre-filter: {len(filtered)}/{len(items)} items passed")
    if not filtered:
        return

    # Try each sport's player index against the filtered items
    title_to_match: dict = {}
    for sport in sports:
        load_player_index(sport)
        for item in filtered:
            title = item.get("title", "")
            if title in title_to_match:
                continue
            candidates = get_candidate_players(title, sport)
            if candidates:
                title_to_match[title] = (candidates[0], sport)
                log.info(f"  PLAYER MATCH [{sport}]: \"{title}\" → {candidates[0]}")

    log.info(f"  Step 1: {time.time()-t0:.1f}s — matched {len(title_to_match)} items")
    if not title_to_match:
        return

    # Fetch card data per sport
    t1 = time.time()
    by_sport: dict = {}
    for title, (player, sport) in title_to_match.items():
        by_sport.setdefault(sport, set()).add(player)
    for sport, players in by_sport.items():
        fetch_player_cards(list(players), sport)
    log.info(f"  Step 2: {time.time()-t1:.1f}s")

    # Score and alert — use matched card's sport for emoji
    t2 = time.time()
    for item in filtered:
        title = item.get("title", "")
        match = title_to_match.get(title)
        if not match:
            continue
        matched_player, sport = match
        cache = _player_card_cache.setdefault(sport, {})
        cards = cache.get(matched_player, [])
        if not cards:
            continue

        emoji = OTHER_SPORTS_EMOJIS.get(sport, "🏅")
        _score_and_alert(
            [item], {title: matched_player}, cache,
            listing_type, sport, emoji, cat["color"], webhook
        )

    log.info(f"  Step 3: {time.time()-t2:.1f}s")


# ===========================================================================
# Shared score + alert logic
# ===========================================================================

def _score_and_alert(
    items: list, title_to_player: dict, cache: dict,
    listing_type: str, sport: str, emoji: str, color: int, webhook: str
):
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
            log.info(
                f"  PRICE SKIP: {matched_card['canonical_name']} | "
                f"eBay: {fmt(price)} | Market: {fmt(market_price)}"
            )
            continue

        savings_pct = round((market_price - price) / market_price * 100)
        savings_dol = market_price - price
        if savings_dol < MIN_SAVINGS:
            continue
        if savings_pct > MAX_SAVINGS_PCT:
            continue

        url = get_item_url(item)
        if has_alerted(url):
            continue
        record_alert(url)

        log.info(
            f"  DEAL: {matched_card['canonical_name']} | "
            f"eBay: {fmt(price)} | Market: {fmt(market_price)} | Save: {savings_pct}%"
        )

        market_source   = "30d avg" if matched_card.get("avg_price_30d") else "⚠️ last sale only"
        last_sale_raw   = matched_card.get("last_sale_date")
        last_sale       = last_sale_raw[:10] if last_sale_raw else "unknown"
        type_label      = "🏷️ Buy It Now" if listing_type == "bin" else "⏱️ Auction"
        has_30d         = bool(matched_card.get("avg_price_30d"))
        variation_label = matched_card.get("variation") or "Base"

        end_time_raw = item.get("itemEndDate", "")
        end_time_str = fmt_end_time(end_time_raw) if end_time_raw else None

        embed_color = 0x3498db if (listing_type == "bin" and has_30d) \
               else  0xf39c12 if (listing_type == "bin") \
               else  0x2ecc71 if has_30d \
               else  0xf1c40f

        # Fix duplicate year — set_name already contains the year in most cases
        set_name_str = matched_card.get("set_name") or ""
        set_year_str = str(matched_card.get("set_year") or "")
        if set_year_str and set_name_str.startswith(set_year_str):
            set_display = set_name_str
        else:
            set_display = f"{set_year_str} {set_name_str}".strip()

        desc_lines = [
            f"eBay: {fmt(price)} | Market: {fmt(market_price)} ({market_source}) | Save: {savings_pct}% ({fmt(savings_dol)})",
            f"Set: {set_display or 'unknown'}",
            f"Variation: {variation_label}",
            f"Last Sale: {last_sale}",
        ]
        if listing_type == "auction" and end_time_str:
            desc_lines.append(f"⏰ Ends: {end_time_str}")

        embed = {
            "title": (
                f"🚨 {type_label} – {emoji} {sport}: "
                f"{matched_card['canonical_name']} (Raw)"
            ),
            "description": "\n".join(desc_lines),
            "url":         url,
            "color":       embed_color,
            "thumbnail":   {"url": (item.get("image") or {}).get("imageUrl", "")},
        }

        dest_webhook = BIN_WEBHOOK if listing_type == "bin" else webhook
        post_discord(dest_webhook, embed)
        time.sleep(0.3)


# ===========================================================================
# Main scan
# ===========================================================================

def run_scan():
    global seen_urls
    seen_urls = set()
    log.info("=" * 60)
    log.info(f"Starting scan — {datetime.utcnow().isoformat()}")
    log.info("=" * 60)
    for cat_name, cat in CATEGORIES.items():
        log.info(f"\n--- Scanning {cat_name} ---")
        try:
            if cat.get("is_multi_sport"):
                for sport in cat["sports"]:
                    load_player_index(sport)
                bin_items     = search_ebay(cat, "bin")
                auction_items = search_ebay(cat, "auction")
                process_items_multi(bin_items,     "bin",     cat)
                process_items_multi(auction_items, "auction", cat)
            else:
                sport = cat["sport"]
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
# Entry point
# ===========================================================================

if __name__ == "__main__":
    log.info("Card price scanner starting...")
    run_scan()
    schedule.every(10).minutes.do(run_scan)
    while True:
        schedule.run_pending()
        time.sleep(30)
