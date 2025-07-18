import os
import psycopg2
import json
import random
import requests
import threading
import time
import base64
import uuid
import logging
import io
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib.parse import quote, urlparse
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor

# --- CONFIGURATION ---

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
CORS(app, resources={r"/api/*": {"origins": "https://vasiliy-katsyka.github.io"}})

# --- ENVIRONMENT VARIABLES & CONSTANTS ---
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
WEBHOOK_URL = "https://upgrade-a57g.onrender.com"

if not DATABASE_URL or not TELEGRAM_BOT_TOKEN:
    raise ValueError("Missing required environment variables: DATABASE_URL and/or TELEGRAM_BOT_TOKEN")

GIFT_LIMIT_PER_USER = 5000
MAX_COLLECTIBLE_USERNAMES = 10
MIN_SALE_PRICE = 125
MAX_SALE_PRICE = 100000
CDN_BASE_URL = "https://cdn.changes.tg/gifts/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBAPP_URL = "https://vasiliy-katsyka.github.io/upgrade/"
WEBAPP_SHORT_NAME = "upgrade"
BOT_USERNAME = "upgradeDemoBot"
TEST_ACCOUNT_TG_ID = 9999999999
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
GIVEAWAY_UPDATE_THROTTLE_SECONDS = 30

# --- CUSTOM GIFT DATA ---
CUSTOM_GIFTS_DATA = {
    "Dildo": {
        "models": [
            {"name": "She Wants", "rarityPermille": 1, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011319484.png"},
            {"name": "Anal Games", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004252351.png"},
            {"name": "Romance", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011244151.png"},
            {"name": "Ma Boi", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012725065.png"},
            {"name": "Twins 18", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004029718.png"},
            {"name": "Golden Sex", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004223973.png"},
            {"name": "Pixels", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012914349.png"},
            {"name": "Penis Sword", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_014719027.png"},
            {"name": "Water One", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004351728.png"},
            {"name": "Woman Place", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004416012.png"},
            {"name": "Volcano", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011343633.png"},
            {"name": "Telegram", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012648445.png"},
            {"name": "Pinkie Twinkie", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004004128.png"},
            {"name": "Silver Glass", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004200179.png"},
            {"name": "Plush", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011552827.png"},
            {"name": "Plush Cuttie", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011623951.png"},
            {"name": "Spider Fun", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012003197.png"},
            {"name": "Horse", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012355663.png"},
            {"name": "Hand", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_013039090.png"},
            {"name": "Ancient", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_014521352.png"},
            {"name": "Minion", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_014626463.png"},
            {"name": "Skinny Boi", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004055652.png"},
            {"name": "Rainbow", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_004125778.png"},
            {"name": "Russian Wood", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011527878.png"},
            {"name": "Afterparty", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_011822289.png"},
            {"name": "Neon", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_012439323.png"},
            {"name": "Black Jack", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_013144671.png"},
            {"name": "Galaxy", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_014555528.png"},
            {"name": "Hell Red", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/newTacos/main/BackgroundEraser_20250717_014650858.png"}
        ],
        "backdrops_source": "Astral Shard",
        "patterns_source": "Astral Shard"
    },
    "Skebob": {
        "models": [
            {"name": "Nikitka", "rarityPermille": 1, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/refs/heads/main/BackgroundEraser_20250718_145212143.png"},
            {"name": "Gold", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_220944840-min.png"},
            {"name": "Plushy", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_221053786-min.png"},
            {"name": "XXXTentacion", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_222249990-min.png"},
            {"name": "Cactus King", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013002213-min.png"},
            {"name": "354 KANON", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013042799-min.png"},
            {"name": "Duck", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014036288-min.png"},
            {"name": "Spider King", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_220335725-min.png"},
            {"name": "Bitcoin", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_012502725-min.png"},
            {"name": "Move To Heaven", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_012612974-min.png"},
            {"name": "Frogie", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_012824238-min.png"},
            {"name": "The King", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_012931928-min.png"},
            {"name": "Fire On Fire", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013941593-min.png"},
            {"name": "Icy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_220405846-min.png"},
            {"name": "Pick Me", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_220906007-min.png"},
            {"name": "Black Bird", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_221147963-min.png"},
            {"name": "Pavel Durov", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_222706006-min.png"},
            {"name": "Banana", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013851152-min.png"},
            {"name": "Mummy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014247708-min.png"},
            {"name": "Police Man", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014319952-min.png"},
            {"name": "Electric BDSM", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014554522-min.png"},
            {"name": "Glassy", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250717_221020205-min.png"},
            {"name": "Ancient", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_012541910-min.png"},
            {"name": "Business", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013214856-min.png"},
            {"name": "Spookie", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013308503-min.png"},
            {"name": "Minion", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013343118-min.png"},
            {"name": "Oh Shit", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_013417326-min.png"},
            {"name": "Emo Girl", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014533870-min.png"},
            {"name": "Minecraft", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/skebobs/main/BackgroundEraser_20250718_014750440-min.png"}
        ],
        "backdrops_source": "Snoop Dogg",
        "patterns_source": "Snoop Dogg"
    }
}


# --- DATABASE HELPERS ---

def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        app.logger.error(f"Could not connect to database: {e}", exc_info=True)
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        app.logger.warning("Database connection failed during initialization.")
        return

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                tg_id BIGINT PRIMARY KEY,
                username VARCHAR(255) UNIQUE,
                full_name VARCHAR(255),
                avatar_url TEXT,
                bio TEXT,
                phone_number VARCHAR(50),
                bot_state VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='accounts' AND column_name='bot_state') THEN
                    ALTER TABLE accounts ADD COLUMN bot_state VARCHAR(255);
                END IF;
            END $$;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS gifts (
                instance_id VARCHAR(50) PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                gift_type_id VARCHAR(255) NOT NULL, gift_name VARCHAR(255) NOT NULL,
                original_image_url TEXT, lottie_path TEXT, is_collectible BOOLEAN DEFAULT FALSE,
                collectible_data JSONB, collectible_number INT,
                acquired_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_hidden BOOLEAN DEFAULT FALSE, is_pinned BOOLEAN DEFAULT FALSE, is_worn BOOLEAN DEFAULT FALSE,
                pin_order INT
            );
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='gifts' AND column_name='pin_order') THEN
                    ALTER TABLE gifts ADD COLUMN pin_order INT;
                END IF;
            END $$;
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_owner_id ON gifts (owner_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_type_and_number ON gifts (gift_type_id, collectible_number);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_pin_order ON gifts (owner_id, pin_order);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS collectible_usernames (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                username VARCHAR(255) UNIQUE NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                creator_id BIGINT REFERENCES accounts(tg_id) ON DELETE SET NULL,
                channel_id BIGINT,
                end_date TIMESTAMP WITH TIME ZONE,
                winner_rule VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending_setup',
                message_id BIGINT,
                last_update_time TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='giveaways' AND column_name='last_update_time') THEN
                    ALTER TABLE giveaways ADD COLUMN last_update_time TIMESTAMP WITH TIME ZONE;
                END IF;
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='giveaways' AND column_name='channel_username') THEN
                    ALTER TABLE giveaways DROP COLUMN channel_username;
                END IF;
            END $$;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaway_gifts (
                id SERIAL PRIMARY KEY,
                giveaway_id INT REFERENCES giveaways(id) ON DELETE CASCADE,
                gift_instance_id VARCHAR(50) REFERENCES gifts(instance_id) ON DELETE CASCADE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaway_participants (
                id SERIAL PRIMARY KEY,
                giveaway_id INT REFERENCES giveaways(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(giveaway_id, user_id)
            );
        """)

        cur.execute("SELECT 1 FROM accounts WHERE tg_id = %s;", (TEST_ACCOUNT_TG_ID,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tg_id) DO NOTHING;
            """, (TEST_ACCOUNT_TG_ID, 'system_test_account', 'Test Account', 'https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/main/DMJTGStarsEmoji_AgADUhMAAk9WoVI.png', 'This account holds sold gifts.'))

    conn.commit()
    conn.close()
    app.logger.info("Database initialized successfully.")

# --- TELEGRAM BOT HELPERS ---

def send_telegram_message(chat_id, text, reply_markup=None, disable_web_page_preview=False):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': disable_web_page_preview}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to send message to chat_id {chat_id}: {e}", exc_info=True)
        return None

def send_telegram_photo(chat_id, photo, caption=None, reply_markup=None):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = {'chat_id': chat_id}
    files = None
    file_to_close = None

    if isinstance(photo, str) and photo.startswith('http'):
        data['photo'] = photo
    elif isinstance(photo, str):
        try:
            file_to_close = open(photo, 'rb')
            files = {'photo': file_to_close}
        except IOError as e:
            app.logger.error(f"Could not open file {photo} to send to chat_id {chat_id}: {e}", exc_info=True)
            return None
    elif isinstance(photo, (bytes, io.BytesIO)):
        files = {'photo': ('generated_gift.png', photo, 'image/png')}
    else:
        app.logger.error(f"Unsupported photo type for chat_id {chat_id}: {type(photo)}")
        return None

    if caption:
        data['caption'] = caption
        data['parse_mode'] = 'HTML'
    if reply_markup:
        data['reply_markup'] = json.dumps(reply_markup)

    try:
        response = requests.post(url, data=data, files=files, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to send photo to chat_id {chat_id}: {e}", exc_info=True)
        return None
    finally:
        if file_to_close and not file_to_close.closed:
            file_to_close.close()

def edit_telegram_message_text(chat_id, message_id, text, reply_markup=None, disable_web_page_preview=False):
    url = f"{TELEGRAM_API_URL}/editMessageText"
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': disable_web_page_preview}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to edit message {message_id} in chat {chat_id}: {e}", exc_info=True)
        return None

def answer_callback_query(callback_query_id, text=None, show_alert=False):
    url = f"{TELEGRAM_API_URL}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id}
    if text: payload['text'] = text
    if show_alert: payload['show_alert'] = show_alert
    try:
        requests.post(url, json=payload, timeout=5)
    except requests.RequestException as e:
        app.logger.error(f"Failed to answer callback query {callback_query_id}: {e}")

def set_webhook():
    webhook_endpoint = f"{WEBHOOK_URL}/webhook"
    url = f"{TELEGRAM_API_URL}/setWebhook?url={webhook_endpoint}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        app.logger.info(f"Webhook set successfully to {webhook_endpoint}: {response.json()}")
    except requests.RequestException as e:
        app.logger.error(f"Failed to set webhook: {e}")

# --- UTILITY FUNCTIONS ---
def select_weighted_random(items):
    if not items: return None
    total_weight = sum(item.get('rarityPermille', 1) for item in items)
    if total_weight == 0: return random.choice(items) if items else None
    random_num = random.uniform(0, total_weight)
    for item in items:
        weight = item.get('rarityPermille', 1)
        if random_num < weight: return item
        random_num -= weight
    return items[-1]

def fetch_collectible_parts(gift_name):
    # Handle ALL custom gifts defined in the dictionary
    if gift_name in CUSTOM_GIFTS_DATA:
        custom_gift_data = CUSTOM_GIFTS_DATA.get(gift_name, {})
        parts = {
            "models": custom_gift_data.get("models", []),
            "backdrops": fetch_collectible_parts(custom_gift_data.get("backdrops_source", "")).get("backdrops", []),
            "patterns": fetch_collectible_parts(custom_gift_data.get("patterns_source", "")).get("patterns", [])
        }
        return parts

    # Original logic for CDN gifts (this part stays the same)
    # gift_name_encoded = quote(gift_name)
    # ... rest of the function

    # Original logic for CDN gifts
    gift_name_encoded = quote(gift_name)
    urls = {
        "models": f"{CDN_BASE_URL}models/{gift_name_encoded}/models.json",
        "backdrops": f"{CDN_BASE_URL}backdrops/{gift_name_encoded}/backdrops.json",
        "patterns": f"{CDN_BASE_URL}patterns/{gift_name_encoded}/patterns.json"
    }
    parts = {}
    for part_type, url in urls.items():
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            parts[part_type] = response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            app.logger.warning(f"Could not fetch {part_type} from {url}: {e}")
            parts[part_type] = []
    return parts

# --- WEBHOOK & GIVEAWAY BOT LOGIC ---

def update_giveaway_message(giveaway_id):
    conn = get_db_connection()
    if not conn: return
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT g.*, a.username as creator_username FROM giveaways g JOIN accounts a ON g.creator_id = a.tg_id WHERE g.id = %s", (giveaway_id,))
        giveaway = cur.fetchone()
        if not giveaway or not giveaway['message_id']:
            conn.close()
            return

        cur.execute("""SELECT gf.gift_name, gf.collectible_number, gf.gift_type_id FROM gifts gf JOIN giveaway_gifts gg ON gf.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s ORDER BY gf.acquired_date;""", (giveaway_id,))
        gifts = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
        participant_count = cur.fetchone()[0]

        prizes_text = "\n".join([f'🎁 <a href="https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{g["gift_type_id"]}-{g["collectible_number"]}">{g["gift_name"]} #{g["collectible_number"]:,}</a>' for g in gifts])
        end_date_str = giveaway['end_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y at %H:%M')

        giveaway_text = (
            f"🎉 <b>Giveaway by @{giveaway['creator_username']}</b> 🎉\n\n"
            f"<b>Prizes:</b>\n{prizes_text}\n\n"
            f"<b>Ends:</b> {end_date_str} (MSK)\n\n"
            "Good luck to everyone!"
        )

        join_url = f"https://t.me/{BOT_USERNAME}?start=giveaway{giveaway_id}"
        reply_markup = {"inline_keyboard": [[{"text": f"➡️ Join ({participant_count} Participants)", "url": join_url}]]}

        edit_telegram_message_text(giveaway['channel_id'], giveaway['message_id'], giveaway_text, reply_markup, disable_web_page_preview=True)
    conn.close()

def handle_giveaway_setup(conn, cur, user_id, user_state, text):
    state_parts = user_state.split('_')
    giveaway_id = int(state_parts[-1])
    state_name = "_".join(state_parts[:-1])

    if text.lower() == '/cancel':
        cur.execute("UPDATE accounts SET bot_state = NULL WHERE tg_id = %s;", (user_id,))
        cur.execute("DELETE FROM giveaways WHERE id = %s;", (giveaway_id,))
        conn.commit()
        send_telegram_message(user_id, "Giveaway setup cancelled.")
        return

    if state_name == 'awaiting_giveaway_channel':
        try:
            channel_id = int(text.strip())
            if not (text.startswith('-100') and len(text) > 5):
                send_telegram_message(user_id, "That doesn't look like a valid public channel ID. It should start with `-100`.")
                return
        except ValueError:
            send_telegram_message(user_id, "Invalid format. Please provide the numerical Channel ID.")
            return

        cur.execute("UPDATE giveaways SET channel_id = %s WHERE id = %s;", (channel_id, giveaway_id))
        new_state = f"awaiting_giveaway_end_date_{giveaway_id}"
        cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, user_id))
        conn.commit()
        send_telegram_message(user_id, "✅ Channel ID set!\n\n🏆 <b>Giveaway Setup: Step 2 of 2</b>\n\nNow, enter the giveaway end date and time in `DD.MM.YYYY HH:MM` format.\n\n<i>Example: `25.12.2025 18:00`</i>\n\n(All times are in MSK/GMT+3 timezone)")

    elif state_name == 'awaiting_giveaway_end_date':
        try:
            end_date_naive = datetime.strptime(text, '%d.%m.%Y %H:%M')
            end_date_aware = MOSCOW_TZ.localize(end_date_naive)
            if end_date_aware < datetime.now(MOSCOW_TZ):
                send_telegram_message(user_id, "The end date cannot be in the past. Please enter a future date.")
                return

            cur.execute("UPDATE giveaways SET end_date = %s WHERE id = %s;", (end_date_aware, giveaway_id))
            cur.execute("UPDATE accounts SET bot_state = NULL WHERE tg_id = %s;", (user_id,))
            conn.commit()
            reply_markup = {"inline_keyboard": [[{"text": "🚀 Publish Giveaway", "callback_data": f"publish_giveaway_{giveaway_id}"}]]}
            send_telegram_message(user_id, "✅ End date set!\n\nEverything is ready. Press the button below to publish your giveaway.", reply_markup=reply_markup)
        except ValueError:
            send_telegram_message(user_id, "Invalid date format. Please use `DD.MM.YYYY HH:MM`.")

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    update = request.get_json()
    conn = get_db_connection()
    if not conn: return jsonify({"status": "error", "message": "db connection failed"}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            if "callback_query" in update:
                callback_query = update["callback_query"]
                user_id = callback_query["from"]["id"]
                data = callback_query.get("data")

                if data and data.startswith("publish_giveaway_"):
                    giveaway_id = int(data.split('_')[2])
                    answer_callback_query(callback_query['id'], text="Publishing...")
                    cur.execute("SELECT g.*, a.username as creator_username FROM giveaways g JOIN accounts a ON g.creator_id = a.tg_id WHERE g.id = %s AND g.status = 'pending_setup'", (giveaway_id,))
                    giveaway = cur.fetchone()

                    if not giveaway:
                        send_telegram_message(user_id, "This giveaway has already been published or does not exist.")
                        return jsonify({"status": "ok"}), 200

                    cur.execute("""SELECT gf.gift_name, gf.collectible_number, gf.gift_type_id FROM gifts gf JOIN giveaway_gifts gg ON gf.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s ORDER BY gf.acquired_date;""", (giveaway_id,))
                    gifts = cur.fetchall()

                    prizes_text = "\n".join([f'🎁 <a href="https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{g["gift_type_id"]}-{g["collectible_number"]}">{g["gift_name"]} #{g["collectible_number"]:,}</a>' for g in gifts])
                    end_date_str = giveaway['end_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y at %H:%M')

                    giveaway_text = (
                        f"🎉 <b>Giveaway by @{giveaway['creator_username']}</b> 🎉\n\n"
                        f"<b>Prizes:</b>\n{prizes_text}\n\n"
                        f"<b>Ends:</b> {end_date_str} (MSK)\n\n"
                        "Good luck to everyone!"
                    )

                    join_url = f"https://t.me/{BOT_USERNAME}?start=giveaway{giveaway_id}"
                    reply_markup = {"inline_keyboard": [[{"text": f"➡️ Join (0 Participants)", "url": join_url}]]}
                    post_result = send_telegram_message(giveaway['channel_id'], giveaway_text, reply_markup=reply_markup, disable_web_page_preview=True)

                    if post_result and post_result.get('ok'):
                        message_id = post_result['result']['message_id']
                        cur.execute("UPDATE giveaways SET status = 'active', message_id = %s, last_update_time = CURRENT_TIMESTAMP WHERE id = %s;", (message_id, giveaway_id))
                        conn.commit()
                        send_telegram_message(user_id, "✅ Giveaway published successfully!")
                    else:
                        send_telegram_message(user_id, "❌ Failed to publish giveaway. Please check that the Channel ID is correct and that the bot has permission to post in it.")

            elif "message" in update:
                message = update["message"]
                chat_id = message["chat"]["id"]
                text = message.get("text", "")

                cur.execute("SELECT bot_state FROM accounts WHERE tg_id = %s;", (chat_id,))
                user_row = cur.fetchone()
                user_state = user_row['bot_state'] if user_row else None

                if user_state and user_state.startswith("awaiting_giveaway"):
                    handle_giveaway_setup(conn, cur, chat_id, user_state, text)

                elif text.startswith("/start"):
                    if "giveaway" in text:
                        try:
                            giveaway_id = int(text.split('giveaway')[1])
                            cur.execute("SELECT id, last_update_time FROM giveaways WHERE id = %s AND status = 'active'", (giveaway_id,))
                            giveaway = cur.fetchone()
                            if not giveaway:
                                send_telegram_message(chat_id, "This giveaway is no longer active or does not exist.")
                            else:
                                cur.execute("INSERT INTO giveaway_participants (giveaway_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (giveaway_id, chat_id))
                                conn.commit()
                                send_telegram_message(chat_id, "🎉 You have successfully joined the giveaway! Good luck!")

                                now = datetime.now(pytz.utc)
                                last_update = giveaway.get('last_update_time') or (now - timedelta(seconds=GIVEAWAY_UPDATE_THROTTLE_SECONDS + 1))
                                if now - last_update > timedelta(seconds=GIVEAWAY_UPDATE_THROTTLE_SECONDS):
                                    cur.execute("UPDATE giveaways SET last_update_time = CURRENT_TIMESTAMP WHERE id = %s;", (giveaway_id,))
                                    conn.commit()
                                    threading.Thread(target=update_giveaway_message, args=(giveaway_id,)).start()
                        except (IndexError, ValueError):
                            send_telegram_message(chat_id, "Invalid giveaway link.")
                    else:
                        caption = ("<b>Welcome to the Gift Upgrade Demo!</b>\n\n"
                                   "This app is a simulation of Telegram's gift and collectible system. "
                                   "You can buy gifts, upgrade them, and trade them with other users.\n\n"
                                   "Tap the button below to get started!")
                        photo_url = "https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/refs/heads/main/IMG_20250706_195911_731.jpg"
                        reply_markup = {
                            "inline_keyboard": [
                                [{"text": "🎁 Open Gift App", "web_app": {"url": WEBAPP_URL}}],
                                [{"text": "🐞 Report Bug", "url": "https://t.me/Vasiliy939"}]
                            ]
                        }
                        send_telegram_photo(chat_id, photo_url, caption=caption, reply_markup=reply_markup)
    finally:
        if conn:
            conn.close()

    return jsonify({"status": "ok"}), 200


# --- API ENDPOINTS ---

@app.route('/api/profile/<string:username>', methods=['GET'])
def get_user_profile(username):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number FROM accounts WHERE LOWER(username) = LOWER(%s);", (username,))
            user_profile = cur.fetchone()
            if not user_profile: return jsonify({"error": "User profile not found."}), 404

            profile_data = dict(user_profile)
            user_id = profile_data['tg_id']

            cur.execute("""
                SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar
                FROM gifts g
                JOIN accounts a ON g.owner_id = a.tg_id
                WHERE g.owner_id = %s AND g.is_hidden = FALSE
                ORDER BY g.is_pinned DESC, g.pin_order ASC NULLS LAST, g.acquired_date DESC;
            """, (user_id,))
            gifts = [dict(row) for row in cur.fetchall()]
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str):
                    gift['collectible_data'] = json.loads(gift['collectible_data'])
            profile_data['owned_gifts'] = gifts

            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (user_id,))
            profile_data['collectible_usernames'] = [row['username'] for row in cur.fetchall()]
            return jsonify(profile_data), 200
        except Exception as e:
            app.logger.error(f"Error fetching profile for {username}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()

@app.route('/api/account', methods=['POST'])
def get_or_create_account():
    data = request.get_json()
    if not data or 'tg_id' not in data: return jsonify({"error": "Missing tg_id"}), 400
    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT * FROM accounts WHERE tg_id = %s;", (tg_id,))
            account = cur.fetchone()
            if not account:
                cur.execute("""INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio, phone_number) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tg_id) DO NOTHING;""", (tg_id, data.get('username'), data.get('full_name'), data.get('avatar_url'), 'My first account!', 'Not specified'))
                conn.commit()
            cur.execute("SELECT * FROM accounts WHERE tg_id = %s;", (tg_id,))
            account_data = dict(cur.fetchone())

            cur.execute("""
                SELECT * FROM gifts WHERE owner_id = %s
                ORDER BY is_pinned DESC, pin_order ASC NULLS LAST, acquired_date DESC;
            """, (tg_id,))
            gifts = [dict(row) for row in cur.fetchall()]
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str):
                    gift['collectible_data'] = json.loads(gift['collectible_data'])
            account_data['owned_gifts'] = gifts

            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
            account_data['collectible_usernames'] = [row['username'] for row in cur.fetchall()]
            return jsonify(account_data), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error in get_or_create_account for {tg_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/account', methods=['PUT'])
def update_account():
    data = request.get_json();
    if not data or 'tg_id' not in data: return jsonify({"error": "Missing tg_id"}), 400
    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        update_fields, update_values = [], []
        if 'username' in data: update_fields.append("username = %s"); update_values.append(data['username'])
        if 'full_name' in data: update_fields.append("full_name = %s"); update_values.append(data['full_name'])
        if 'avatar_url' in data: update_fields.append("avatar_url = %s"); update_values.append(data['avatar_url'])
        if 'bio' in data: update_fields.append("bio = %s"); update_values.append(data['bio'])
        if 'phone_number' in data: update_fields.append("phone_number = %s"); update_values.append(data['phone_number'])
        if not update_fields: conn.close(); return jsonify({"error": "No fields for update"}), 400
        update_query = f"UPDATE accounts SET {', '.join(update_fields)} WHERE tg_id = %s;"
        update_values.append(tg_id)
        try:
            cur.execute(update_query, tuple(update_values))
            if cur.rowcount == 0: conn.close(); return jsonify({"error": "Account not found"}), 404
            conn.commit()
            return jsonify({"message": "Account updated"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error updating account {tg_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts', methods=['POST'])
def add_gift():
    data = request.get_json()
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'instance_id']
    if not all(field in data for field in required_fields): return jsonify({"error": "Missing data"}), 400
    owner_id = data['owner_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            cur.execute("""INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) VALUES (%s, %s, %s, %s, %s, %s);""", (data['instance_id'], owner_id, data['gift_type_id'], data['gift_name'], data['original_image_url'], data.get('lottie_path')))
            conn.commit()
            return jsonify({"message": "Gift added"}), 201
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error adding gift for {owner_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/upgrade', methods=['POST'])
def upgrade_gift():
    data = request.get_json()
    if 'instance_id' not in data: return jsonify({"error": "instance_id is required"}), 400
    instance_id = data['instance_id']
    custom_model_data, custom_backdrop_data, custom_pattern_data = data.get('custom_model'), data.get('custom_backdrop'), data.get('custom_pattern')
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
            gift_row = cur.fetchone()
            if not gift_row: return jsonify({"error": "Gift not found or already collectible."}), 404
            owner_id, gift_type_id, gift_name = gift_row['owner_id'], gift_row['gift_type_id'], gift_row['gift_name']
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            parts_data = fetch_collectible_parts(gift_name)
            selected_model = custom_model_data or select_weighted_random(parts_data.get('models', []))
            selected_backdrop = custom_backdrop_data or select_weighted_random(parts_data.get('backdrops', []))
            selected_pattern = custom_pattern_data or select_weighted_random(parts_data.get('patterns', []))
            if not all([selected_model, selected_backdrop, selected_pattern]): return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500
            supply = random.randint(2000, 10000)

            pattern_source_name = "Astral Shard" if gift_name.lower() == 'dildo' else gift_name
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"


            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url,
                "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url,
                "backdropColors": selected_backdrop.get('hex'), "supply": supply
            }
            cur.execute("""UPDATE gifts SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s, lottie_path = NULL WHERE instance_id = %s;""", (json.dumps(collectible_data), next_number, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Failed to update gift."}), 404
            conn.commit()
            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (instance_id,))
            upgraded_gift = dict(cur.fetchone())
            if isinstance(upgraded_gift.get('collectible_data'), str): upgraded_gift['collectible_data'] = json.loads(upgraded_gift.get('collectible_data'))
            return jsonify(upgraded_gift), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error upgrading gift {instance_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/clone', methods=['POST'])
def clone_gift():
    data = request.get_json()
    url = data.get('url')
    owner_id = data.get('owner_id')

    if not url or not owner_id: return jsonify({"error": "url and owner_id are required"}), 400
    parsed_url = urlparse(url)
    if not (parsed_url.scheme in ['http', 'https'] and parsed_url.netloc in ['t.me', 'telegram.me']):
         return jsonify({"error": "Invalid Telegram URL provided."}), 400

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        title_el = soup.find('div', class_='tgme_page_title')
        gift_name = title_el.find('span').text.strip() if title_el and title_el.find('span') else "Unknown Gift"

        scraped_parts = {}
        table = soup.find('table', class_='tgme_gift_table')
        if table:
            for row in table.find_all('tr'):
                header = row.find('th').text.strip().lower() if row.find('th') else None
                value = row.find('td').text.strip() if row.find('td') else None
                if header and value:
                    scraped_parts[header] = ' '.join(value.split()[:-1]) if '%' in value else value

        model_name = scraped_parts.get('model')
        backdrop_name = scraped_parts.get('backdrop')
        pattern_name = scraped_parts.get('symbol')

        if not all([model_name, backdrop_name, pattern_name]):
            return jsonify({"error": "Could not scrape all required gift parts."}), 400

        all_parts_data = fetch_collectible_parts(gift_name)
        custom_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
        custom_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
        custom_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None)

        if not all([custom_model, custom_backdrop, custom_pattern]):
            return jsonify({"error": "Could not match scraped part names to available data."}), 500

        new_instance_id = str(uuid.uuid4())

        base_gift_id_to_clone = "1"

        conn = get_db_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name) VALUES (%s, %s, %s, %s);", (new_instance_id, owner_id, base_gift_id_to_clone, gift_name))

            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (base_gift_id_to_clone,))
            next_number = cur.fetchone()[0]
            
            pattern_source_name = "Astral Shard" if gift_name.lower() == 'dildo' else gift_name
            model_image_url = custom_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(custom_model['name'])}.png"
            lottie_model_path = custom_model.get('lottie') if custom_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(custom_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(custom_pattern['name'])}.png"

            collectible_data = {
                "model": custom_model, "backdrop": custom_backdrop, "pattern": custom_pattern,
                "modelImage": model_image_url,
                "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url,
                "backdropColors": custom_backdrop.get('hex'), "supply": random.randint(2000, 10000)
            }
            cur.execute("""UPDATE gifts SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s WHERE instance_id = %s;""", (json.dumps(collectible_data), next_number, new_instance_id))
            conn.commit()

            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (new_instance_id,))
            cloned_gift = dict(cur.fetchone())
            cloned_gift['collectible_data'] = json.loads(cloned_gift['collectible_data'])
        conn.close()
        return jsonify(cloned_gift), 201

    except Exception as e:
        app.logger.error(f"Error cloning gift from {url}: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred during cloning."}), 500

@app.route('/api/gift/<string:gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("""SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE LOWER(g.gift_type_id) = LOWER(%s) AND g.collectible_number = %s AND g.is_collectible = TRUE;""", (gift_type_id, collectible_number))
            gift_data = cur.fetchone()
            if not gift_data: return jsonify({"error": "Collectible gift not found."}), 404
            result = dict(gift_data)
            if isinstance(result.get('collectible_data'), str): result['collectible_data'] = json.loads(result.get('collectible_data'))
            return jsonify(result), 200
        except Exception as e:
            app.logger.error(f"Error fetching deep-linked gift {gift_type_id}-{collectible_number}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/<string:instance_id>', methods=['PUT'])
def update_gift_state(instance_id):
    data = request.get_json(); action, value = data.get('action'), data.get('value')
    if action not in ['pin', 'hide', 'wear'] or not isinstance(value, bool): return jsonify({"error": "Invalid action or value"}), 400
    column_to_update = {'pin': 'is_pinned', 'hide': 'is_hidden', 'wear': 'is_worn'}[action]
    conn = get_db_connection();
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            if action == 'wear' and value is True:
                cur.execute("SELECT owner_id FROM gifts WHERE instance_id = %s;", (instance_id,))
                owner_id_result = cur.fetchone()
                if not owner_id_result: return jsonify({"error": "Gift not found for wear action."}), 404
                cur.execute("UPDATE gifts SET is_worn = FALSE WHERE owner_id = %s AND is_worn = TRUE;", (owner_id_result[0],))

            update_query = f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;"
            if action == 'hide' and value is True:
                update_query = "UPDATE gifts SET is_hidden = TRUE, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s;"
            elif action == 'pin' and value is False:
                 update_query = "UPDATE gifts SET is_pinned = FALSE, pin_order = NULL WHERE instance_id = %s;"

            cur.execute(update_query, (value, instance_id) if (action != 'hide' and (action != 'pin' or value is not False)) else (instance_id,))

            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found or state not changed."}), 404
            conn.commit()
            return jsonify({"message": f"Gift {action} state updated"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"DB error updating gift state for {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/<string:instance_id>', methods=['DELETE'])
def delete_gift(instance_id):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM gifts WHERE instance_id = %s;", (instance_id,))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found."}), 404
            conn.commit()
            return jsonify({"message": "Gift deleted"}), 204
        except Exception as e:
            conn.rollback(); app.logger.error(f"DB error deleting gift {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/sell', methods=['POST'])
def sell_gift():
    data = request.get_json()
    instance_id = data.get('instance_id')
    price = data.get('price')
    owner_id = data.get('owner_id')

    if not all([instance_id, price, owner_id]):
        return jsonify({"error": "instance_id, price, and owner_id are required"}), 400

    try:
        price_int = int(price)
        if not (MIN_SALE_PRICE <= price_int <= MAX_SALE_PRICE):
            return jsonify({"error": f"Price must be between {MIN_SALE_PRICE} and {MAX_SALE_PRICE}."}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid price format."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1 FROM gifts WHERE instance_id = %s AND owner_id = %s;", (instance_id, owner_id))
            if not cur.fetchone():
                return jsonify({"error": "Gift not found or you are not the owner."}), 404

            cur.execute("""
                UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL
                WHERE instance_id = %s;
            """, (TEST_ACCOUNT_TG_ID, instance_id))

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Failed to list gift for sale."}), 500

            conn.commit()
            return jsonify({"message": "Gift listed for sale successfully."}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error selling gift {instance_id} for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/gifts/reorder', methods=['POST'])
def reorder_pinned_gifts():
    data = request.get_json()
    owner_id = data.get('owner_id')
    ordered_ids = data.get('ordered_instance_ids')

    if not owner_id or not isinstance(ordered_ids, list):
        return jsonify({"error": "owner_id and ordered_instance_ids list are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor() as cur:
        try:
            cur.execute("UPDATE gifts SET pin_order = NULL WHERE owner_id = %s AND is_pinned = TRUE;", (owner_id,))
            for index, instance_id in enumerate(ordered_ids):
                cur.execute("UPDATE gifts SET pin_order = %s WHERE instance_id = %s AND owner_id = %s;", (index, instance_id, owner_id))
            conn.commit()
            return jsonify({"message": "Pinned gifts reordered successfully."}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error reordering pinned gifts for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/gifts/batch_action', methods=['POST'])
def batch_gift_action():
    data = request.get_json()
    action = data.get('action')
    instance_ids = data.get('instance_ids')
    owner_id = data.get('owner_id')

    if not all([action, instance_ids, owner_id]) or not isinstance(instance_ids, list):
        return jsonify({"error": "action, instance_ids list, and owner_id are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            if action == 'hide':
                cur.execute("""
                    UPDATE gifts
                    SET is_hidden = TRUE, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL
                    WHERE instance_id = ANY(%s) AND owner_id = %s;
                """, (instance_ids, owner_id))
                conn.commit()
                return jsonify({"message": f"{cur.rowcount} gifts hidden."}), 200

            elif action == 'transfer':
                receiver_username = data.get('receiver_username', '').lstrip('@')
                comment = data.get('comment')
                if not receiver_username:
                    return jsonify({"error": "receiver_username is required for transfer"}), 400

                cur.execute("SELECT tg_id, username FROM accounts WHERE username = %s;", (receiver_username,))
                receiver = cur.fetchone()
                if not receiver: return jsonify({"error": "Receiver username not found."}), 404
                receiver_id, receiver_username = receiver['tg_id'], receiver['username']

                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (owner_id,))
                sender_username = cur.fetchone()['username']

                cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
                receiver_gift_count = cur.fetchone()[0]
                if receiver_gift_count + len(instance_ids) > GIFT_LIMIT_PER_USER:
                    return jsonify({"error": f"Receiver's gift limit would be exceeded."}), 403

                cur.execute("""
                    UPDATE gifts
                    SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL
                    WHERE instance_id = ANY(%s) AND owner_id = %s;
                """, (receiver_id, instance_ids, owner_id))

                if cur.rowcount == 0:
                    conn.rollback()
                    return jsonify({"error": "No gifts were transferred. Check ownership."}), 404

                conn.commit()

                num_transferred = len(instance_ids)
                gift_text = f"{num_transferred} gift" if num_transferred == 1 else f"{num_transferred} gifts"

                sender_text = f'You successfully transferred {gift_text} to @{receiver_username}'
                if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
                send_telegram_message(owner_id, sender_text)

                receiver_text = f'You have received {gift_text} from @{sender_username}'
                if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
                send_telegram_message(receiver_id, receiver_text)

                return jsonify({"message": f"{num_transferred} gifts transferred."}), 200

            else:
                return jsonify({"error": "Invalid action specified."}), 400

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error during batch action '{action}' for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/gifts/transfer', methods=['POST'])
def transfer_gift():
    data = request.get_json()
    instance_id = data.get('instance_id')
    receiver_username = data.get('receiver_username', '').lstrip('@')
    sender_id = data.get('sender_id')
    comment = data.get('comment')

    if not all([instance_id, receiver_username, sender_id]):
        return jsonify({"error": "instance_id, receiver_username, and sender_id are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver: return jsonify({"error": "Receiver username not found."}), 404
            receiver_id = receiver['tg_id']

            cur.execute("SELECT a.username, g.gift_name, g.collectible_number, g.gift_type_id FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE g.instance_id = %s;", (instance_id,))
            sender_info = cur.fetchone()
            if not sender_info: return jsonify({"error": "Sender or gift not found."}), 404
            sender_username, gift_name, gift_number, gift_type_id = sender_info['username'], sender_info['gift_name'], sender_info['collectible_number'], sender_info['gift_type_id']

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            cur.execute("""UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s AND is_collectible = TRUE;""", (receiver_id, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found or could not be transferred."}), 404
            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{gift_number}"
            link_text = f"{gift_name} #{gift_number:,}"

            sender_text = f'You successfully transferred Gift <a href="{deep_link}">{link_text}</a> to @{receiver_username}'
            if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)

            receiver_text = f'You have received Gift <a href="{deep_link}">{link_text}</a> from @{sender_username}'
            if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check out", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)

            return jsonify({"message": "Gift transferred successfully"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error during gift transfer of {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()

@app.route('/api/gifts/send_image', methods=['POST'])
def send_generated_image():
    data = request.get_json()
    if not data: return jsonify({"error": "Invalid JSON payload"}), 400

    image_data_url = data.get('imageDataUrl')
    user_id = data.get('userId')
    caption = data.get('caption', None)

    if not image_data_url or not user_id: return jsonify({"error": "imageDataUrl and userId are required"}), 400

    try:
        header, encoded_data = image_data_url.split(',', 1)
        image_bytes = base64.b64decode(encoded_data)
        result = send_telegram_photo(user_id, image_bytes, caption=caption)

        if result and result.get('ok'):
            return jsonify({"message": "Image sent successfully"}), 200
        else:
            error_message = result.get('description') if result else "Unknown Telegram API error"
            app.logger.error(f"Telegram API failed to send image to {user_id}: {error_message}")
            return jsonify({"error": "Failed to send image via Telegram API", "details": error_message}), 502

    except (ValueError, TypeError, IndexError) as e:
        app.logger.error(f"Error decoding base64 image for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "Invalid base64 image data format"}), 400
    except Exception as e:
        app.logger.error(f"Unexpected error sending generated image to {user_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred"}), 500

@app.route('/api/collectible_usernames', methods=['POST'])
def add_collectible_username():
    data = request.get_json(); owner_id, username = data.get('owner_id'), data.get('username')
    if not owner_id or not username: return jsonify({"error": "owner_id and username are required"}), 400
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1 FROM collectible_usernames WHERE username = %s;", (username,))
            if cur.fetchone(): return jsonify({"error": f"Username @{username} is already taken."}), 409
            cur.execute("SELECT COUNT(*) FROM collectible_usernames WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= MAX_COLLECTIBLE_USERNAMES: return jsonify({"error": f"Username limit of {MAX_COLLECTIBLE_USERNAMES} reached."}), 403
            cur.execute("""INSERT INTO collectible_usernames (owner_id, username) VALUES (%s, %s);""", (owner_id, username))
            conn.commit()
            return jsonify({"message": "Username added"}), 201
        except psycopg2.IntegrityError:
            conn.rollback(); app.logger.warning(f"Integrity error adding username {username}.", exc_info=True)
            return jsonify({"error": f"Username @{username} is already taken."}), 409
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error adding username {username}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/collectible_usernames/<string:username>', methods=['DELETE'])
def delete_collectible_username(username):
    data = request.get_json(); owner_id = data.get('owner_id')
    if not owner_id: return jsonify({"error": "owner_id is required"}), 400
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("""DELETE FROM collectible_usernames WHERE username = %s AND owner_id = %s;""", (username, owner_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Username not found for this user."}), 404
            conn.commit()
            return jsonify({"message": "Username deleted"}), 204
        except Exception as e:
            conn.rollback(); app.logger.error(f"DB error deleting username {username}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/giveaways/create', methods=['POST'])
def create_giveaway():
    data = request.get_json()
    creator_id = data.get('creator_id')
    gift_instance_ids = data.get('gift_instance_ids')
    winner_rule = data.get('winner_rule')

    if not all([creator_id, gift_instance_ids, winner_rule]):
        return jsonify({"error": "creator_id, gift_instance_ids, and winner_rule are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("INSERT INTO giveaways (creator_id, winner_rule) VALUES (%s, %s) RETURNING id;", (creator_id, winner_rule))
            giveaway_id = cur.fetchone()['id']
            for gift_id in gift_instance_ids:
                cur.execute("INSERT INTO giveaway_gifts (giveaway_id, gift_instance_id) VALUES (%s, %s);", (giveaway_id, gift_id))

            new_state = f"awaiting_giveaway_channel_{giveaway_id}"
            cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, creator_id))
            conn.commit()

            send_telegram_message(
                creator_id,
                ("🏆 <b>Giveaway Setup: Step 1 of 2</b>\n\n"
                 "Please send the <b>numerical ID</b> of the public channel for the giveaway.\n\n"
                 "To get the ID, you can forward a message from your channel to a bot like @userinfobot.\n\n"
                 "<i>The bot must be able to post in this channel (i.e., it must be public).</i>\n\n"
                 "To cancel, send /cancel.")
            )

            return jsonify({"message": "Giveaway initiated.", "giveaway_id": giveaway_id}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

import pytz
from psycopg2.extras import DictCursor
import math

# (This assumes the rest of your Flask app setup, get_db_connection(), etc., remains the same)
# (TEST_ACCOUNT_TG_ID should be defined elsewhere in your app)

def get_rarity_tier(model_p, backdrop_p, pattern_p):
    # rarityPermille is parts per 1000. Total space is 1000*1000*1000 = 1e9
    total_prob = (model_p / 1000.0) * (backdrop_p / 1000.0) * (pattern_p / 1000.0)
    one_in_x = 1 / total_prob if total_prob > 0 else float('inf')
    if one_in_x > 5_000_000: return 'Mythic'
    if one_in_x > 1_000_000: return 'Legendary'
    if one_in_x > 200_000: return 'Epic'
    if one_in_x > 50_000: return 'Rare'
    if one_in_x > 10_000: return 'Uncommon'
    return 'Common'


@app.route('/api/stats', methods=['GET'])
def get_stats_ultimate():
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    stats = {}
    now_utc = datetime.now(pytz.utc)
    one_day_ago = now_utc - timedelta(days=1)
    seven_days_ago = now_utc - timedelta(days=7)
    thirty_days_ago = now_utc - timedelta(days=30)

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            # === Core Metrics ===
            cur.execute("SELECT COUNT(*) FROM gifts;")
            total_gifts = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT owner_id) FROM gifts WHERE owner_id != %s;", (TEST_ACCOUNT_TG_ID,))
            unique_owners = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gifts WHERE is_collectible = TRUE;")
            collectible_items = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gifts WHERE is_hidden = TRUE;")
            hidden_gift_count = cur.fetchone()[0]

            # === User & Engagement Metrics ===
            cur.execute("SELECT COUNT(DISTINCT tg_id) FROM accounts WHERE created_at >= %s;", (one_day_ago,))
            dau_new_users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) as count_30d FROM accounts WHERE created_at >= %s;", (thirty_days_ago,))
            new_users_30d = cur.fetchone()['count_30d']
            cur.execute("SELECT COUNT(*) as count_total FROM accounts;")
            total_users = cur.fetchone()['count_total']
            growth_rate = ((new_users_30d / (total_users - new_users_30d)) * 100) if (total_users - new_users_30d) > 0 else 100

            # SIMULATED: Prolific Trader & Top Gifter (requires transfer logs not in schema)
            cur.execute("""
                SELECT a.username, COUNT(g.instance_id) as items_acquired
                FROM gifts g JOIN accounts a ON a.tg_id = g.owner_id
                WHERE g.acquired_date >= %s AND a.tg_id != %s
                GROUP BY a.tg_id ORDER BY items_acquired DESC LIMIT 1;
            """, (seven_days_ago, TEST_ACCOUNT_TG_ID))
            prolific_trader = cur.fetchone()
            
            stats['user_metrics'] = {
                'dau_approx': dau_new_users,
                'monthly_growth_rate': round(growth_rate, 2),
                'avg_gifts_per_user': round(total_gifts / unique_owners) if unique_owners > 0 else 0,
                'prolific_trader_7d': dict(prolific_trader) if prolific_trader else None,
                'top_gifter_7d': dict(prolific_trader) if prolific_trader else None, # Placeholder
            }
            
            # === Economic & Transactional Metrics ===
            cur.execute("SELECT COUNT(*) FROM gifts WHERE acquired_date >= %s;", (one_day_ago,))
            daily_tx_volume = cur.fetchone()[0]
            cur.execute("SELECT gift_name, count(*) as count FROM gifts WHERE acquired_date >= %s GROUP BY gift_name ORDER BY count DESC LIMIT 1;", (seven_days_ago,))
            most_liquid_collection = cur.fetchone()

            stats['economic_metrics'] = {
                'daily_transaction_volume': daily_tx_volume,
                'most_liquid_collection_7d': dict(most_liquid_collection) if most_liquid_collection else None,
                'floor_price_simulated': {'name': 'Snoop Dogg', 'price': 45}, # Placeholder
                'market_cap_simulated': {'name': 'Plush Pepe', 'cap': 12500}, # Placeholder
            }

            # === Gift, Trait & Rarity Metrics ===
            cur.execute("SELECT collectible_data->'backdrop'->>'name' as name, COUNT(*) as count FROM gifts WHERE is_collectible = TRUE GROUP BY name ORDER BY count DESC LIMIT 1;")
            most_popular_backdrop = cur.fetchone()
            cur.execute("SELECT collectible_data->'pattern'->>'name' as name, COUNT(*) as count FROM gifts WHERE is_collectible = TRUE GROUP BY name ORDER BY count DESC LIMIT 1;")
            most_popular_pattern = cur.fetchone()
            cur.execute("SELECT gift_name, collectible_number FROM gifts WHERE is_worn = TRUE LIMIT 1;")
            most_worn_gift = cur.fetchone()

            # Rarity Distribution
            cur.execute("""
                SELECT collectible_data->'model'->>'rarityPermille' as model_p,
                       collectible_data->'backdrop'->>'rarityPermille' as backdrop_p,
                       collectible_data->'pattern'->>'rarityPermille' as pattern_p
                FROM gifts WHERE is_collectible = TRUE AND collectible_data IS NOT NULL
                AND collectible_data->'model'->>'rarityPermille' IS NOT NULL;
            """)
            rarity_tiers = {'Common': 0, 'Uncommon': 0, 'Rare': 0, 'Epic': 0, 'Legendary': 0, 'Mythic': 0}
            for row in cur.fetchall():
                tier = get_rarity_tier(int(row['model_p']), int(row['backdrop_p']), int(row['pattern_p']))
                rarity_tiers[tier] += 1

            stats['gift_metrics'] = {
                'total_collectibles': collectible_items,
                'hidden_gifts': hidden_gift_count,
                'most_popular_backdrop': dict(most_popular_backdrop) if most_popular_backdrop else None,
                'most_popular_pattern': dict(most_popular_pattern) if most_popular_pattern else None,
                'most_worn_gift': dict(most_worn_gift) if most_worn_gift else None,
                'rarity_distribution': rarity_tiers
            }

            # === Temporal & Fun Metrics ===
            cur.execute("SELECT date_trunc('hour', acquired_date) AT TIME ZONE 'UTC' as hour, count(*) FROM gifts GROUP BY hour ORDER BY count DESC LIMIT 1;")
            peak_activity_hour = cur.fetchone()
            cur.execute("SELECT gift_name, MIN(acquired_date) as first_mint FROM gifts WHERE is_collectible = TRUE GROUP BY gift_name ORDER BY first_mint ASC LIMIT 1;")
            first_ever_mint = cur.fetchone()
            cur.execute("""
                SELECT g.gift_name, COUNT(DISTINCT g.owner_id) as owners, COUNT(*) as total
                FROM gifts g WHERE g.is_collectible = TRUE GROUP BY g.gift_name
                HAVING COUNT(*) > 10 ORDER BY (COUNT(DISTINCT g.owner_id)::decimal / COUNT(*)) ASC LIMIT 1;
            """)
            most_hoarded_gift = cur.fetchone()
            cur.execute("SELECT AVG(LENGTH(username)) as avg_len FROM collectible_usernames;")
            avg_username_length = cur.fetchone()[0] or 0

            stats['fun_metrics'] = {
                'peak_activity_hour_utc': peak_activity_hour['hour'].hour if peak_activity_hour else None,
                'first_ever_mint': dict(first_ever_mint) if first_ever_mint else None,
                'most_hoarded_gift': dict(most_hoarded_gift) if most_hoarded_gift else None,
                'avg_username_length': round(avg_username_length, 1)
            }
            
            # === Leaderboards ===
            cur.execute("SELECT a.username, COUNT(g.instance_id) as count FROM gifts g JOIN accounts a ON a.tg_id=g.owner_id GROUP BY a.tg_id ORDER BY count DESC LIMIT 5;")
            top_holders = cur.fetchall()
            cur.execute("SELECT a.username, COUNT(DISTINCT g.collectible_data->'model'->>'name') as count FROM gifts g JOIN accounts a ON a.tg_id=g.owner_id WHERE g.is_collectible=TRUE GROUP BY a.tg_id ORDER BY count DESC LIMIT 5;")
            power_collectors = cur.fetchall()

            stats['leaderboards'] = {
                'top_holders': [dict(r) for r in top_holders],
                'power_collectors': [dict(r) for r in power_collectors],
            }

            return jsonify(stats), 200
        except Exception as e:
            app.logger.error(f"Error gathering ultimate stats: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred while gathering stats."}), 500
        finally:
            conn.close()
            
@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    app.logger.warning(f"Unhandled API call: {request.method} /api/{path}")
    return jsonify({"error": f"The requested API endpoint '/api/{path}' was not found or the method is not allowed."}), 404

# --- GIVEAWAY BACKGROUND WORKER ---

def process_giveaway_winners(giveaway_id):
    app.logger.info(f"Processing winners for giveaway ID: {giveaway_id}")
    conn = get_db_connection()
    if not conn: return

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT g.*, a.username as creator_username FROM giveaways g JOIN accounts a ON g.creator_id = a.tg_id WHERE g.id = %s", (giveaway_id,))
            giveaway = cur.fetchone()
            if not giveaway: return

            cur.execute("SELECT user_id FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
            participants = cur.fetchall()
            cur.execute("SELECT g.* FROM gifts g JOIN giveaway_gifts gg ON g.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s;", (giveaway_id,))
            gifts = cur.fetchall()

            if not participants:
                send_telegram_message(giveaway['creator_id'], f"😔 Your giveaway in channel ID {giveaway['channel_id']} has ended, but there were no participants.")
                cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
                conn.commit()
                return

            results_text = "🏆 <b>Giveaway Results</b> 🏆\n\nCongratulations to our winners:\n\n"
            if giveaway['winner_rule'] == 'single':
                winner_id = random.choice([p['user_id'] for p in participants])
                cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id IN (SELECT gift_instance_id FROM giveaway_gifts WHERE giveaway_id = %s);", (winner_id, giveaway_id))
                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                winner_username = cur.fetchone()['username']
                results_text += f"All prizes go to: @{winner_username}!\n"
            else:
                participant_ids = [p['user_id'] for p in participants]
                num_winners = min(len(gifts), len(participant_ids))
                selected_winner_ids = random.sample(participant_ids, k=num_winners)
                for i, winner_id in enumerate(selected_winner_ids):
                    gift = gifts[i]
                    cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id = %s;", (winner_id, gift['instance_id']))
                    cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                    winner_username = cur.fetchone()['username']
                    deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift['gift_type_id']}-{gift['collectible_number']}"
                    results_text += f'🎁 <a href="{deep_link}">{gift["gift_name"]} #{gift["collectible_number"]:,}</a> ➔ @{winner_username}\n'

            send_telegram_message(giveaway['channel_id'], results_text, disable_web_page_preview=True)
            cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
            conn.commit()

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error processing giveaway {giveaway_id}: {e}", exc_info=True)
            if giveaway:
                send_telegram_message(giveaway['creator_id'], f"An error occurred while processing your giveaway. The bot might not have access to post in the provided channel ID.")
        finally:
            conn.close()

def check_finished_giveaways():
    while True:
        try:
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM giveaways WHERE status = 'active' AND end_date <= CURRENT_TIMESTAMP;")
                    giveaway_ids = [row[0] for row in cur.fetchall()]
                    if giveaway_ids:
                        cur.execute("UPDATE giveaways SET status = 'processing' WHERE id = ANY(%s);", (giveaway_ids,))
                        conn.commit()
                conn.close()

                for gid in giveaway_ids:
                    processing_thread = threading.Thread(target=process_giveaway_winners, args=(gid,))
                    processing_thread.start()
        except Exception as e:
             app.logger.error(f"Critical error in giveaway checker loop: {e}", exc_info=True)
        time.sleep(60)

# --- APP STARTUP ---
if __name__ != '__main__':
    set_webhook()
    init_db()
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()

if __name__ == '__main__':
    print("Starting Flask server for local development...")
    init_db()
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()
    app.run(debug=True, port=5001)
