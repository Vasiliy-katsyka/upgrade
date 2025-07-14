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

# --- CONFIGURATION ---

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Configure CORS to only allow requests from the specified origin
CORS(app, resources={r"/api/*": {"origins": "https://vasiliy-katsyka.github.io"}})

# --- ENVIRONMENT VARIABLES & CONSTANTS ---
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
# This should be your Render app's public URL
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
BOT_USERNAME = "upgradeDemoBot" # Replace with your bot's username
TEST_ACCOUNT_TG_ID = 9999999999 

# --- DATABASE HELPERS ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        app.logger.error(f"Could not connect to database: {e}", exc_info=True)
        return None

def init_db():
    """Initializes the database and ensures all tables exist."""
    conn = get_db_connection()
    if not conn:
        app.logger.warning("Database connection failed during initialization.")
        return
        
    with conn.cursor() as cur:
        # Tables creation
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                tg_id BIGINT PRIMARY KEY,
                username VARCHAR(255) UNIQUE,
                full_name VARCHAR(255),
                avatar_url TEXT,
                bio TEXT,
                phone_number VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
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
        # Add pin_order column if it doesn't exist (for migration)
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

        # --- GIVEAWAY TABLES ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                creator_id BIGINT REFERENCES accounts(tg_id) ON DELETE SET NULL,
                channel_id BIGINT,
                channel_username VARCHAR(255),
                end_date TIMESTAMP WITH TIME ZONE,
                is_paid BOOLEAN DEFAULT FALSE,
                ticket_price INT,
                winner_rule VARCHAR(20) NOT NULL, -- 'single' or 'multiple'
                status VARCHAR(20) NOT NULL DEFAULT 'pending_setup', -- pending_setup, active, finished, cancelled
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
                ticket_count INT DEFAULT 1,
                join_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(giveaway_id, user_id)
            );
        """)

        # Ensure the 'Test Account' for sold gifts exists
        cur.execute("SELECT 1 FROM accounts WHERE tg_id = %s;", (TEST_ACCOUNT_TG_ID,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tg_id) DO NOTHING;
            """, (TEST_ACCOUNT_TG_ID, 'system_test_account', 'Test Account', 'https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/main/DMJTGStarsEmoji_AgADUhMAAk9WoVI.png', 'This account holds sold gifts.'))
            app.logger.info("Created or verified the system 'Test Account'.")
    
    conn.commit()
    conn.close()
    app.logger.info("Database initialized successfully.")

# --- TELEGRAM BOT HELPERS ---

def send_telegram_message(chat_id, text, reply_markup=None):
    """Sends a message via the Telegram Bot API."""
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
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
    """Sends a photo file, URL, or binary data via the Telegram Bot API."""
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = {'chat_id': chat_id}
    files = None
    file_to_close = None  # To handle file paths safely

    if isinstance(photo, str) and photo.startswith('http'):
        data['photo'] = photo
    elif isinstance(photo, str):
        try:
            file_to_close = open(photo, 'rb')
            files = {'photo': file_to_close}
        except IOError as e:
            app.logger.error(f"Could not open file {photo} to send to chat_id {chat_id}: {e}", exc_info=True)
            return None
    elif isinstance(photo, bytes):
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
        response = requests.post(url, data=data, files=files, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to send photo to chat_id {chat_id}: {e}", exc_info=True)
        return None
    finally:
        if file_to_close and not file_to_close.closed:
            file_to_close.close()
            
def get_chat_member(chat_id, user_id):
    """Checks the status of a user in a chat."""
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
    """Checks if the bot is an administrator in the given chat."""
    member_info = get_chat_member(chat_id, bot_id)
    if member_info and member_info['status'] in ['administrator', 'creator']:
        return True
    return False

def set_webhook():
    """Sets the bot's webhook to the application's URL."""
    webhook_endpoint = f"{WEBHOOK_URL}/webhook"
    url = f"{TELEGRAM_API_URL}/setWebhook?url={webhook_endpoint}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        app.logger.info(f"Webhook set successfully to {webhook_endpoint}: {response.json()}")
    except requests.RequestException as e:
        app.logger.error(f"Failed to set webhook: {e}", exc_info=True)

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

# --- API & BOT ROUTES ---

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """Handles incoming updates from Telegram."""
    update = request.get_json()
    if "message" in update:
        message = update["message"]
        chat_id = message["chat"]["id"]
        text = message.get("text", "")

        # --- GIVEAWAY JOIN LOGIC ---
        if text.startswith("/start giveaway"):
            try:
                giveaway_id = int(text.split('giveaway')[1])
                conn = get_db_connection()
                with conn.cursor() as cur:
                    # Check if giveaway exists and is active
                    cur.execute("SELECT creator_id FROM giveaways WHERE id = %s AND status = 'active'", (giveaway_id,))
                    giveaway = cur.fetchone()
                    if not giveaway:
                        send_telegram_message(chat_id, "This giveaway is no longer active or does not exist.")
                        return jsonify({"status": "ok"}), 200
                    
                    # Add participant
                    cur.execute("""
                        INSERT INTO giveaway_participants (giveaway_id, user_id, ticket_count)
                        VALUES (%s, %s, 1) ON CONFLICT (giveaway_id, user_id) DO NOTHING;
                    """, (giveaway_id, chat_id))
                    conn.commit()
                    
                    if cur.rowcount > 0:
                        send_telegram_message(chat_id, "🎉 You have successfully joined the giveaway! Good luck!")
                    else:
                        send_telegram_message(chat_id, "You have already joined this giveaway. Good luck!")

                conn.close()
            except (IndexError, ValueError):
                send_telegram_message(chat_id, "Invalid giveaway link.")
            return jsonify({"status": "ok"}), 200

        # --- DEFAULT START MESSAGE ---
        if text == "/start":
            caption = (
                "<b>Welcome to the Gift Upgrade Demo!</b>\n\n"
                "This app is a simulation of Telegram's gift and collectible system. "
                "You can buy gifts, upgrade them to unique collectibles, and trade them with other users.\n\n"
                "Tap the button below to get started!"
            )
            photo_url = "https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/refs/heads/main/IMG_20250706_195911_731.jpg"
            reply_markup = {
                "inline_keyboard": [
                    [{"text": "🎁 Open Gift App", "web_app": {"url": WEBAPP_URL}}],
                    [{"text": "🐞 Report Bug", "url": "https://t.me/Vasiliy939"}]
                ]
            }
            send_telegram_photo(chat_id, photo_url, caption=caption, reply_markup=reply_markup)
            
    return jsonify({"status": "ok"}), 200

@app.route('/api/profile/<string:username>', methods=['GET'])
def get_user_profile(username):
    """Fetches a user's public profile data and their non-hidden gifts."""
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    with conn.cursor() as cur:
        try:
            # Fetch user profile data
            cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number FROM accounts WHERE LOWER(username) = LOWER(%s);", (username,))
            user_profile = cur.fetchone()
            if not user_profile:
                return jsonify({"error": "User profile not found."}), 404
            
            profile_data = dict(zip([d[0] for d in cur.description], user_profile))
            user_id = profile_data['tg_id']

            # --- MODIFIED QUERY ---
            # Fetch user's non-hidden gifts, now including owner details for consistency
            cur.execute("""
                SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar
                FROM gifts g
                JOIN accounts a ON g.owner_id = a.tg_id
                WHERE g.owner_id = %s AND g.is_hidden = FALSE 
                ORDER BY g.is_pinned DESC, g.pin_order ASC NULLS LAST, g.acquired_date DESC;
            """, (user_id,))
            gifts = []
            for row in cur.fetchall():
                gift_dict = dict(zip([d[0] for d in cur.description], row))
                if gift_dict.get('collectible_data') and isinstance(gift_dict.get('collectible_data'), str):
                    gift_dict['collectible_data'] = json.loads(gift_dict['collectible_data'])
                gifts.append(gift_dict)
            profile_data['owned_gifts'] = gifts

            # Fetch user's collectible usernames
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (user_id,))
            usernames = [row[0] for row in cur.fetchall()]
            profile_data['collectible_usernames'] = usernames

            return jsonify(profile_data), 200
        except Exception as e:
            app.logger.error(f"Error fetching profile for {username}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/account', methods=['POST'])
def get_or_create_account():
    data = request.get_json()
    if not data or 'tg_id' not in data: return jsonify({"error": "Missing tg_id"}), 400
    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT * FROM accounts WHERE tg_id = %s;", (tg_id,))
            account = cur.fetchone()
            if not account:
                cur.execute("""INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio, phone_number) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tg_id) DO NOTHING;""", (tg_id, data.get('username'), data.get('full_name'), data.get('avatar_url'), 'My first account!', 'Not specified'))
                conn.commit()
            cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number FROM accounts WHERE tg_id = %s;", (tg_id,))
            account_data = dict(zip([d[0] for d in cur.description], cur.fetchone()))
            
            # --- MODIFIED QUERY ---
            # Fetch gifts with consistent sorting order
            cur.execute("""
                SELECT * FROM gifts WHERE owner_id = %s 
                ORDER BY is_pinned DESC, pin_order ASC NULLS LAST, acquired_date DESC;
            """, (tg_id,))
            gifts = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str):
                    gift['collectible_data'] = json.loads(gift['collectible_data'])
            account_data['owned_gifts'] = gifts
            
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
            account_data['collectible_usernames'] = [row[0] for row in cur.fetchall()]
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
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'lottie_path', 'instance_id']
    if not all(field in data for field in required_fields): return jsonify({"error": "Missing data"}), 400
    owner_id = data['owner_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            cur.execute("""INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) VALUES (%s, %s, %s, %s, %s, %s);""", (data['instance_id'], owner_id, data['gift_type_id'], data['gift_name'], data['original_image_url'], data['lottie_path']))
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
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
            gift_row = cur.fetchone()
            if not gift_row: return jsonify({"error": "Gift not found or already collectible."}), 404
            owner_id, gift_type_id, gift_name = gift_row
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            parts_data = fetch_collectible_parts(gift_name)
            selected_model = custom_model_data or select_weighted_random(parts_data.get('models', []))
            selected_backdrop = custom_backdrop_data or select_weighted_random(parts_data.get('backdrops', []))
            selected_pattern = custom_pattern_data or select_weighted_random(parts_data.get('patterns', []))
            if not all([selected_model, selected_backdrop, selected_pattern]): return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500
            supply = random.randint(2000, 10000)
            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png",
                "lottieModelPath": f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json",
                "patternImage": f"{CDN_BASE_URL}patterns/{quote(gift_name)}/png/{quote(selected_pattern['name'])}.png",
                "backdropColors": selected_backdrop.get('hex'), "supply": supply
            }
            cur.execute("""UPDATE gifts SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s, lottie_path = NULL WHERE instance_id = %s;""", (json.dumps(collectible_data), next_number, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Failed to update gift."}), 404
            conn.commit()
            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (instance_id,))
            upgraded_gift = dict(zip([d[0] for d in cur.description], cur.fetchone()))
            if isinstance(upgraded_gift.get('collectible_data'), str): upgraded_gift['collectible_data'] = json.loads(upgraded_gift.get('collectible_data'))
            return jsonify(upgraded_gift), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error upgrading gift {instance_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

# --- NEW CLONE GIFT ENDPOINT ---
@app.route('/api/gifts/clone', methods=['POST'])
def clone_gift():
    """Clones a gift by scraping its public URL."""
    data = request.get_json()
    url = data.get('url')
    owner_id = data.get('owner_id')

    if not url or not owner_id:
        return jsonify({"error": "url and owner_id are required"}), 400

    parsed_url = urlparse(url)
    if not (parsed_url.scheme in ['http', 'https'] and parsed_url.netloc in ['t.me', 'telegram.me']):
         return jsonify({"error": "Invalid Telegram URL provided."}), 400
         
    try:
        # Step 1: Scrape the URL to get gift part names
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        gift_name_el = soup.select_one('.tgme_page_title')
        gift_name = gift_name_el.get_text(strip=True) if gift_name_el else None
        if not gift_name:
            return jsonify({"error": "Could not determine gift name from URL."}), 404

        scraped_parts = {}
        table = soup.find('table', class_='tgme_gift_table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                header = row.find('th')
                value = row.find('td')
                if header and value:
                    header_text = header.text.strip().lower()
                    value_text = ' '.join(value.text.split()[:-1]) # Remove rarity part like "(3%)"
                    scraped_parts[header_text] = value_text

        model_name = scraped_parts.get('model')
        backdrop_name = scraped_parts.get('backdrop')
        pattern_name = scraped_parts.get('symbol')

        if not all([model_name, backdrop_name, pattern_name]):
            return jsonify({"error": "Could not scrape all required gift parts (Model, Backdrop, Symbol)."}), 400

        # Step 2: Fetch the full collectible data from CDN
        all_parts_data = fetch_collectible_parts(gift_name)

        # Step 3: Find the specific data objects for the scraped names
        custom_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
        custom_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
        custom_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None)

        if not all([custom_model, custom_backdrop, custom_pattern]):
            return jsonify({"error": "Could not match scraped part names to available collectible data."}), 500
        
        # Step 4: Create a base gift and upgrade it
        new_instance_id = str(uuid.uuid4())
        
        # We can use a dummy base gift since we will immediately upgrade it
        base_gift_id = "1"
        base_gift_name = "Logo"
        base_original_image = f"{CDN_BASE_URL}originals/{base_gift_id}/Original.png"
        base_lottie_path = f"{CDN_BASE_URL}originals/{base_gift_id}/Original.json"

        conn = get_db_connection()
        with conn.cursor() as cur:
            # Add a temporary base gift
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: 
                return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            
            cur.execute("""
                INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) 
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (new_instance_id, owner_id, base_gift_id, base_gift_name, base_original_image, base_lottie_path))
            
            # Now upgrade it with the cloned data
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (base_gift_id,))
            next_number = cur.fetchone()[0]
            supply = random.randint(2000, 10000)

            collectible_data = {
                "model": custom_model, "backdrop": custom_backdrop, "pattern": custom_pattern,
                "modelImage": f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(custom_model['name'])}.png",
                "lottieModelPath": f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(custom_model['name'])}.json",
                "patternImage": f"{CDN_BASE_URL}patterns/{quote(gift_name)}/png/{quote(custom_pattern['name'])}.png",
                "backdropColors": custom_backdrop.get('hex'), "supply": supply
            }

            # Update the temporary gift to be the cloned gift
            cur.execute("""
                UPDATE gifts SET is_collectible = TRUE, gift_name=%s, collectible_data = %s, collectible_number = %s, lottie_path = NULL 
                WHERE instance_id = %s;
            """, (gift_name, json.dumps(collectible_data), next_number, new_instance_id))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Failed to upgrade cloned gift."}), 500

            conn.commit()

            # Return the newly created gift
            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (new_instance_id,))
            cloned_gift = dict(zip([d[0] for d in cur.description], cur.fetchone()))
            if isinstance(cloned_gift.get('collectible_data'), str):
                cloned_gift['collectible_data'] = json.loads(cloned_gift.get('collectible_data'))
        
        conn.close()
        return jsonify(cloned_gift), 201

    except requests.RequestException as e:
        app.logger.error(f"Failed to scrape URL {url}: {e}", exc_info=True)
        return jsonify({"error": "Could not fetch the provided URL."}), 502
    except Exception as e:
        app.logger.error(f"Error cloning gift from {url}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred during cloning."}), 500


@app.route('/api/gift/<string:gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("""SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE LOWER(g.gift_type_id) = LOWER(%s) AND g.collectible_number = %s AND g.is_collectible = TRUE;""", (gift_type_id, collectible_number))
            gift_data = cur.fetchone()
            if not gift_data: return jsonify({"error": "Collectible gift not found."}), 404
            result = dict(zip([d[0] for d in cur.description], gift_data))
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
            # If hiding a gift, also unpin and un-wear it. If unpinning, clear its pin_order.
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
            
            # Transfer ownership to the test account to signify it's "for sale"
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
            # First, clear the pin_order for all of the user's pinned gifts
            cur.execute("UPDATE gifts SET pin_order = NULL WHERE owner_id = %s AND is_pinned = TRUE;", (owner_id,))
            
            # Then, set the new order
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
    owner_id = data.get('owner_id') # or sender_id

    if not all([action, instance_ids, owner_id]) or not isinstance(instance_ids, list):
        return jsonify({"error": "action, instance_ids list, and owner_id are required"}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor() as cur:
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
                receiver_id, receiver_username = receiver

                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (owner_id,))
                sender_username = cur.fetchone()[0]

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
                
                # Notifications
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
    comment = data.get('comment') # Optional comment

    if not all([instance_id, receiver_username, sender_id]):
        return jsonify({"error": "instance_id, receiver_username, and sender_id are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver: return jsonify({"error": "Receiver username not found."}), 404
            receiver_id = receiver[0]

            cur.execute("SELECT a.username, g.gift_name, g.collectible_number, g.gift_type_id FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE g.instance_id = %s;", (instance_id,))
            sender_info = cur.fetchone()
            if not sender_info: return jsonify({"error": "Sender or gift not found."}), 404
            sender_username, gift_name, gift_number, gift_type_id = sender_info

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            
            cur.execute("""UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s AND is_collectible = TRUE;""", (receiver_id, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found or could not be transferred."}), 404
            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/upgrade?startapp=gift{gift_type_id}-{gift_number}"
            link_text = f"{gift_name} #{gift_number:,}"
            
            # Prepare sender notification
            sender_text = f'You successfully transferred Gift <a href="{deep_link}">{link_text}</a> to @{receiver_username}'
            if comment:
                sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)

            # Prepare receiver notification
            receiver_text = f'You have received Gift <a href="{deep_link}">{link_text}</a> from @{sender_username}'
            if comment:
                receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check out", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)
            
            return jsonify({"message": "Gift transferred successfully"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error during gift transfer of {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()

@app.route('/api/gifts/send_image', methods=['POST'])
def send_generated_image():
    """Receives a base64 image and sends it to a user via the bot."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400
        
    image_data_url = data.get('imageDataUrl')
    user_id = data.get('userId')
    caption = data.get('caption', None)

    if not image_data_url or not user_id:
        return jsonify({"error": "imageDataUrl and userId are required"}), 400

    try:
        # Split data URL: "data:image/png;base64,iVBORw0KGgo..."
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

# --- GIVEAWAY API ---
@app.route('/api/giveaways/create', methods=['POST'])
def create_giveaway():
    """Starts the giveaway creation process."""
    data = request.get_json()
    creator_id = data.get('creator_id')
    gift_instance_ids = data.get('gift_instance_ids')
    winner_rule = data.get('winner_rule') # 'single' or 'multiple'

    if not all([creator_id, gift_instance_ids, winner_rule]):
        return jsonify({"error": "creator_id, gift_instance_ids, and winner_rule are required"}), 400
    if not isinstance(gift_instance_ids, list) or len(gift_instance_ids) == 0:
        return jsonify({"error": "gift_instance_ids must be a non-empty list"}), 400
    if winner_rule not in ['single', 'multiple']:
        return jsonify({"error": "winner_rule must be 'single' or 'multiple'"}), 400
    if winner_rule == 'multiple' and len(gift_instance_ids) < 2:
        return jsonify({"error": "Multiple winners rule requires at least 2 gifts."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    with conn.cursor() as cur:
        try:
            # Create the main giveaway record
            cur.execute("""
                INSERT INTO giveaways (creator_id, winner_rule, status) VALUES (%s, %s, 'pending_setup')
                RETURNING id;
            """, (creator_id, winner_rule))
            giveaway_id = cur.fetchone()[0]

            # Associate the gifts with this giveaway
            for gift_id in gift_instance_ids:
                cur.execute("""
                    INSERT INTO giveaway_gifts (giveaway_id, gift_instance_id) VALUES (%s, %s);
                """, (giveaway_id, gift_id))
            
            conn.commit()
            return jsonify({"message": "Giveaway initiated.", "giveaway_id": giveaway_id}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()


@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    app.logger.warning(f"Unhandled API call: {request.method} /api/{path}")
    return jsonify({"error": f"The requested API endpoint '/api/{path}' was not found or the method is not allowed."}), 404


# --- GIVEAWAY BACKGROUND WORKER ---

def process_giveaway_winners(giveaway_id):
    """Handles logic for selecting winners and distributing prizes."""
    app.logger.info(f"Processing winners for giveaway ID: {giveaway_id}")
    conn = get_db_connection()
    if not conn: return

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            # Fetch giveaway details
            cur.execute("SELECT * FROM giveaways WHERE id = %s;", (giveaway_id,))
            giveaway = cur.fetchone()
            if not giveaway:
                app.logger.warning(f"Could not find giveaway {giveaway_id} to process.")
                return

            # Fetch participants and gifts
            cur.execute("SELECT * FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
            participants = cur.fetchall()
            cur.execute("""
                SELECT g.* FROM gifts g 
                JOIN giveaway_gifts gg ON g.instance_id = gg.gift_instance_id 
                WHERE gg.giveaway_id = %s;
            """, (giveaway_id,))
            gifts = cur.fetchall()

            if not participants:
                send_telegram_message(giveaway['creator_id'], f"😔 Your giveaway in {giveaway['channel_username']} has ended, but there were no participants.")
                cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
                conn.commit()
                return

            # Select winner(s)
            winners = []
            if giveaway['winner_rule'] == 'single':
                # For single winner, all gifts go to one person
                winner_id = random.choice([p['user_id'] for p in participants])
                winners.append({'user_id': winner_id, 'gifts': gifts})
            else: # 'multiple'
                participant_ids = [p['user_id'] for p in participants]
                # Ensure we don't try to select more winners than there are participants
                num_winners = min(len(gifts), len(participant_ids))
                selected_winner_ids = random.sample(participant_ids, k=num_winners)
                for i, winner_id in enumerate(selected_winner_ids):
                    winners.append({'user_id': winner_id, 'gifts': [gifts[i]]})

            # Transfer gifts and build results message
            results_text = "🏆 **Giveaway Results** 🏆\n\nCongratulations to our winners:\n\n"
            for win_info in winners:
                winner_id = win_info['user_id']
                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                winner_username = cur.fetchone()['username']
                
                for gift in win_info['gifts']:
                    cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id = %s;", (winner_id, gift['instance_id']))
                    deep_link = f"https://t.me/{BOT_USERNAME}/upgrade?startapp=gift{gift['gift_type_id']}-{gift['collectible_number']}"
                    link_text = f"{gift['gift_name']} #{gift['collectible_number']:,}"
                    results_text += f'🎁 <a href="{deep_link}">{link_text}</a>  ➔  @{winner_username}\n'
                    # Notify winner
                    send_telegram_message(winner_id, f"🎉 Congratulations! You won <a href='{deep_link}'>{link_text}</a> in a giveaway!")

            # Post results to channel
            send_telegram_message(giveaway['channel_id'], results_text)

            # Mark giveaway as finished
            cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
            conn.commit()
            app.logger.info(f"Successfully processed giveaway {giveaway_id}.")

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error processing giveaway {giveaway_id}: {e}", exc_info=True)
            if giveaway:
                send_telegram_message(giveaway['creator_id'], f"An error occurred while processing your giveaway in {giveaway.get('channel_username', 'channel')}. Please contact support.")
        finally:
            conn.close()


def check_finished_giveaways():
    """Periodically checks for giveaways that have ended."""
    while True:
        app.logger.info("Giveaway worker checking for finished giveaways...")
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cur:
                try:
                    # Get IDs of giveaways that have ended and are still active
                    cur.execute("""
                        SELECT id FROM giveaways 
                        WHERE status = 'active' AND end_date <= CURRENT_TIMESTAMP;
                    """)
                    giveaway_ids = [row[0] for row in cur.fetchall()]
                    
                    # Mark them as 'processing' to prevent double-processing
                    if giveaway_ids:
                        cur.execute("UPDATE giveaways SET status = 'processing' WHERE id = ANY(%s);", (giveaway_ids,))
                        conn.commit()

                except Exception as e:
                    giveaway_ids = []
                    app.logger.error(f"Error fetching finished giveaways: {e}", exc_info=True)
                finally:
                    conn.close()

            if giveaway_ids:
                app.logger.info(f"Found {len(giveaway_ids)} finished giveaways to process.")
                for gid in giveaway_ids:
                    # Each giveaway is processed in its own thread to avoid blocking
                    processing_thread = threading.Thread(target=process_giveaway_winners, args=(gid,))
                    processing_thread.start()
        
        # Sleep for 60 seconds before the next check
        time.sleep(60)

# Set webhook and start background threads on startup when run by Gunicorn
if __name__ != '__main__':
    set_webhook()
    init_db()
    # Start the giveaway checker in a background thread
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()

# Run for local development
if __name__ == '__main__':
    print("Starting Flask server for local development...")
    init_db()
    # Start the giveaway checker in a background thread
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()
    app.run(debug=True, port=5001)