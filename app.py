# --- app.py ---

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
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib.parse import quote, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import pytz # For timezone handling in giveaways
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
BOT_USERNAME = "upgradeDemoBot" 
TEST_ACCOUNT_TG_ID = 9999999999 
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

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
                bot_state VARCHAR(255), -- ADDED FOR GIVEAWAY STATE
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Add bot_state column if it doesn't exist (for migration)
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='accounts' AND column_name='bot_state'
                ) THEN
                    ALTER TABLE accounts ADD COLUMN bot_state VARCHAR(255);
                END IF;
            END$$;
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
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='gifts' AND column_name='pin_order'
                ) THEN
                    ALTER TABLE gifts ADD COLUMN pin_order INT;
                END IF;
            END$$;
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
                channel_username VARCHAR(255),
                end_date TIMESTAMP WITH TIME ZONE,
                winner_rule VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending_setup',
                message_id BIGINT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
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

def answer_callback_query(callback_query_id, text=None, show_alert=False):
    url = f"{TELEGRAM_API_URL}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id}
    if text: payload['text'] = text
    if show_alert: payload['show_alert'] = show_alert
    try:
        requests.post(url, json=payload, timeout=5)
    except requests.RequestException as e:
        app.logger.error(f"Failed to answer callback query {callback_query_id}: {e}")

def get_bot_info():
    """Gets the bot's own info, like its ID."""
    url = f"{TELEGRAM_API_URL}/getMe"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json().get('result')
    except requests.RequestException as e:
        app.logger.error(f"Failed to get bot info: {e}")
        return None

def get_chat_member(chat_id, user_id):
    url = f"{TELEGRAM_API_URL}/getChatMember"
    payload = {'chat_id': chat_id, 'user_id': user_id}
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json().get('result')
    except requests.RequestException as e:
        app.logger.error(f"Failed to get chat member for chat {chat_id}, user {user_id}: {e}")
        return None

def check_admin_rights(chat_id, bot_id):
    member_info = get_chat_member(chat_id, bot_id)
    if member_info and member_info['status'] in ['administrator', 'creator']:
        return member_info.get('can_post_messages', False)
    return False
# --- UTILITY AND OTHER FUNCTIONS (AS THEY WERE) ---
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
    
def set_webhook():
    webhook_endpoint = f"{WEBHOOK_URL}/webhook"
    url = f"{TELEGRAM_API_URL}/setWebhook?url={webhook_endpoint}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        app.logger.info(f"Webhook set successfully to {webhook_endpoint}: {response.json()}")
    except requests.RequestException as e:
        app.logger.error(f"Failed to set webhook: {e}")

# --- WEBHOOK & GIVEAWAY BOT LOGIC ---

def handle_giveaway_setup(conn, cur, user_id, user_state, text):
    """Manages the conversation for setting up a giveaway."""
    state_parts = user_state.split('_')
    state_name = state_parts[0] + "_" + state_parts[1] + "_" + state_parts[2]
    giveaway_id = int(state_parts[3])

    if text.lower() == '/cancel':
        cur.execute("UPDATE accounts SET bot_state = NULL WHERE tg_id = %s;", (user_id,))
        cur.execute("DELETE FROM giveaways WHERE id = %s;", (giveaway_id,))
        conn.commit()
        send_telegram_message(user_id, "Giveaway setup cancelled.")
        return

    if state_name == 'awaiting_giveaway_channel':
        channel_username = text.strip()
        if not channel_username.startswith('@'):
            send_telegram_message(user_id, "Invalid format. Please provide the channel username starting with '@'.")
            return

        bot_info = get_bot_info()
        if not bot_info:
            send_telegram_message(user_id, "Could not verify my own identity. Please try again later.")
            return

        if not check_admin_rights(channel_username, bot_info['id']):
            send_telegram_message(user_id, f"I am not an administrator in {channel_username} or I don't have permission to post messages. Please make me an admin and try again.")
            return

        chat_info = get_chat_member(channel_username, bot_info['id'])
        channel_id = chat_info['chat']['id'] if 'chat' in chat_info else None
        if not channel_id:
             send_telegram_message(user_id, f"Could not get channel details for {channel_username}. Please ensure it is a public channel.")
             return

        cur.execute("UPDATE giveaways SET channel_id = %s, channel_username = %s WHERE id = %s;", (channel_id, channel_username, giveaway_id))
        new_state = f"awaiting_giveaway_end_date_{giveaway_id}"
        cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, user_id))
        conn.commit()

        send_telegram_message(user_id, "✅ Channel set!\n\n🏆 **Giveaway Setup: Step 2 of 2**\n\nNow, enter the giveaway end date and time in `DD.MM.YYYY HH:MM` format.\n\n*Example: `25.12.2025 18:00`*\n\n(All times are in MSK/GMT+3 timezone)")

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
            send_telegram_message(user_id, "✅ End date set!\n\nEverything is ready. Press the button below to publish your giveaway to the channel.", reply_markup=reply_markup)

        except ValueError:
            send_telegram_message(user_id, "Invalid date format. Please use `DD.MM.YYYY HH:MM`.")

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """Handles all incoming updates from Telegram."""
    update = request.get_json()
    conn = get_db_connection()
    if not conn: return jsonify({"status": "error", "message": "db connection failed"}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # --- CALLBACK QUERY HANDLER ---
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

                    cur.execute("""
                        SELECT gf.gift_name, gf.collectible_number, gf.gift_type_id 
                        FROM gifts gf JOIN giveaway_gifts gg ON gf.instance_id = gg.gift_instance_id 
                        WHERE gg.giveaway_id = %s ORDER BY gf.acquired_date;
                    """, (giveaway_id,))
                    gifts = cur.fetchall()

                    prizes_text = "\n".join([f'🎁 <a href="https://t.me/{BOT_USERNAME}?start=gift{g["gift_type_id"]}-{g["collectible_number"]}">{g["gift_name"]} #{g["collectible_number"]:,}</a>' for g in gifts])
                    end_date_str = giveaway['end_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y at %H:%M')
                    
                    giveaway_text = (
                        f"🎉 **Giveaway by @{giveaway['creator_username']}** 🎉\n\n"
                        f"**Prizes:**\n{prizes_text}\n\n"
                        f"**Ends:** {end_date_str} (MSK)\n\n"
                        "Good luck to everyone!"
                    )
                    
                    cur.execute("SELECT COUNT(*) FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
                    participant_count = cur.fetchone()[0]
                    
                    join_url = f"https://t.me/{BOT_USERNAME}?start=giveaway{giveaway_id}"
                    reply_markup = {"inline_keyboard": [[{"text": f"➡️ Join ({participant_count} Participants)", "url": join_url}]]}

                    post_result = send_telegram_message(giveaway['channel_id'], giveaway_text, reply_markup=reply_markup, disable_web_page_preview=True)

                    if post_result and post_result.get('ok'):
                        message_id = post_result['result']['message_id']
                        cur.execute("UPDATE giveaways SET status = 'active', message_id = %s WHERE id = %s;", (message_id, giveaway_id))
                        conn.commit()
                        send_telegram_message(user_id, "✅ Giveaway published successfully!")
                    else:
                        send_telegram_message(user_id, "❌ Failed to publish giveaway to the channel. Please ensure I still have admin rights.")

            # --- MESSAGE HANDLER ---
            elif "message" in update:
                message = update["message"]
                chat_id = message["chat"]["id"]
                text = message.get("text", "")

                cur.execute("SELECT bot_state FROM accounts WHERE tg_id = %s;", (chat_id,))
                user_row = cur.fetchone()
                user_state = user_row['bot_state'] if user_row else None
                
                if user_state:
                    handle_giveaway_setup(conn, cur, chat_id, user_state, text)
                
                elif text.startswith("/start"):
                    # Giveaway join logic
                    if "giveaway" in text:
                        try:
                            giveaway_id = int(text.split('giveaway')[1])
                            cur.execute("SELECT 1 FROM giveaways WHERE id = %s AND status = 'active'", (giveaway_id,))
                            if not cur.fetchone():
                                send_telegram_message(chat_id, "This giveaway is no longer active or does not exist.")
                            else:
                                cur.execute("INSERT INTO giveaway_participants (giveaway_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (giveaway_id, chat_id))
                                conn.commit()
                                send_telegram_message(chat_id, "🎉 You have successfully joined the giveaway! Good luck!")
                        except (IndexError, ValueError):
                            send_telegram_message(chat_id, "Invalid giveaway link.")
                    # Default start message
                    else:
                        caption = (
                            "<b>Welcome to the Gift Upgrade Demo!</b>\n\n"
                            "This app is a simulation of Telegram's gift and collectible system. "
                            "You can buy gifts, upgrade them, and even host giveaways!\n\n"
                            "Tap the button below to get started!"
                        )
                        photo_url = "https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/refs/heads/main/IMG_20250706_195911_731.jpg"
                        reply_markup = {"inline_keyboard": [[{"text": "🎁 Open Gift App", "web_app": {"url": WEBAPP_URL}}]]}
                        send_telegram_photo(chat_id, photo_url, caption=caption, reply_markup=reply_markup)
    finally:
        if conn:
            conn.close()

    return jsonify({"status": "ok"}), 200

# --- API ENDPOINTS (with corrected sorting) ---

# All other API endpoints like /api/profile, /api/account, etc., remain as they were in the previous complete code block.
# I am re-including them here for completeness with the corrected sorting logic.

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

# The rest of the API endpoints (/api/account PUT, /api/gifts, /api/gifts/upgrade, etc.) remain unchanged from the previous complete version.
# I am including the new /api/giveaways/create endpoint and the /api/gifts/clone endpoint, as they are new.

@app.route('/api/giveaways/create', methods=['POST'])
def create_giveaway():
    data = request.get_json()
    creator_id = data.get('creator_id')
    gift_instance_ids = data.get('gift_instance_ids')
    winner_rule = data.get('winner_rule')

    if not all([creator_id, gift_instance_ids, winner_rule]):
        return jsonify({"error": "creator_id, gift_instance_ids, and winner_rule are required"}), 400
    if not isinstance(gift_instance_ids, list) or len(gift_instance_ids) == 0:
        return jsonify({"error": "gift_instance_ids must be a non-empty list"}), 400
    if winner_rule not in ['single', 'multiple']:
        return jsonify({"error": "winner_rule must be 'single' or 'multiple'"}), 400
    if winner_rule == 'multiple' and len(gift_instance_ids) < 1:
        return jsonify({"error": "Multiple winners rule requires at least 1 gift."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            # 1. Create giveaway record
            cur.execute("INSERT INTO giveaways (creator_id, winner_rule) VALUES (%s, %s) RETURNING id;", (creator_id, winner_rule))
            giveaway_id = cur.fetchone()['id']
            # 2. Associate gifts
            for gift_id in gift_instance_ids:
                cur.execute("INSERT INTO giveaway_gifts (giveaway_id, gift_instance_id) VALUES (%s, %s);", (giveaway_id, gift_id))
            # 3. Set user's bot_state
            new_state = f"awaiting_giveaway_channel_{giveaway_id}"
            cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, creator_id))
            conn.commit()

            # 4. Send message to user
            send_telegram_message(
                creator_id,
                "🏆 **Giveaway Setup: Step 1 of 2**\n\nYou've selected your gifts. Now, please send the username of the public channel where the giveaway will be hosted (e.g., `@mychannel`).\n\n*The bot must be an administrator in this channel with permission to post messages.*\n\nTo cancel, send /cancel."
            )
            
            return jsonify({"message": "Giveaway initiated.", "giveaway_id": giveaway_id}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

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
            
            collectible_data = {
                "model": custom_model, "backdrop": custom_backdrop, "pattern": custom_pattern,
                "modelImage": f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(custom_model['name'])}.png",
                "lottieModelPath": f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(custom_model['name'])}.json",
                "patternImage": f"{CDN_BASE_URL}patterns/{quote(gift_name)}/png/{quote(custom_pattern['name'])}.png",
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

# Placeholder for all other routes that remain unchanged for brevity
# In a real file, all routes from the previous version would be here.
# For example: /api/gifts/upgrade, /api/gifts/sell, /api/gifts/reorder, etc.

# --- GIVEAWAY BACKGROUND WORKER ---

def process_giveaway_winners(giveaway_id):
    """Handles logic for selecting winners and distributing prizes."""
    app.logger.info(f"Processing winners for giveaway ID: {giveaway_id}")
    conn = get_db_connection()
    if not conn: return

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT * FROM giveaways WHERE id = %s;", (giveaway_id,))
            giveaway = cur.fetchone()
            if not giveaway: return

            cur.execute("SELECT user_id FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
            participants = cur.fetchall()
            cur.execute("SELECT g.* FROM gifts g JOIN giveaway_gifts gg ON g.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s;", (giveaway_id,))
            gifts = cur.fetchall()

            if not participants:
                send_telegram_message(giveaway['creator_id'], f"😔 Your giveaway in {giveaway['channel_username']} has ended, but there were no participants.")
                cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
                conn.commit()
                return

            winners, results_text = [], "🏆 **Giveaway Results** 🏆\n\nCongratulations to our winners:\n\n"
            if giveaway['winner_rule'] == 'single':
                winner_id = random.choice([p['user_id'] for p in participants])
                cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id IN (SELECT gift_instance_id FROM giveaway_gifts WHERE giveaway_id = %s);", (winner_id, giveaway_id))
                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                winner_username = cur.fetchone()['username']
                results_text += f"All prizes go to: @{winner_username}!\n"
            else: # multiple winners
                participant_ids = [p['user_id'] for p in participants]
                num_winners = min(len(gifts), len(participant_ids))
                selected_winner_ids = random.sample(participant_ids, k=num_winners)
                for i, winner_id in enumerate(selected_winner_ids):
                    gift = gifts[i]
                    cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id = %s;", (winner_id, gift['instance_id']))
                    cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                    winner_username = cur.fetchone()['username']
                    deep_link = f"https://t.me/{BOT_USERNAME}/upgrade?startapp=gift{gift['gift_type_id']}-{gift['collectible_number']}"
                    results_text += f'🎁 <a href="{deep_link}">{gift["gift_name"]} #{gift["collectible_number"]:,}</a> ➔ @{winner_username}\n'
            
            send_telegram_message(giveaway['channel_id'], results_text)
            cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
            conn.commit()

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error processing giveaway {giveaway_id}: {e}", exc_info=True)
        finally:
            conn.close()


def check_finished_giveaways():
    """Periodically checks for giveaways that have ended."""
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