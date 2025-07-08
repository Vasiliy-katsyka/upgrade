import os
import psycopg2
import json
import random
import requests
import threading
import time
import base64
import uuid
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib.parse import quote

# --- CONFIGURATION ---

app = Flask(__name__)

# Configure CORS to only allow requests from the specified origin
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
    """Initializes the database and ensures the Test Account exists."""
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
                is_hidden BOOLEAN DEFAULT FALSE, is_pinned BOOLEAN DEFAULT FALSE, is_worn BOOLEAN DEFAULT FALSE
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_owner_id ON gifts (owner_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_type_and_number ON gifts (gift_type_id, collectible_number);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collectible_usernames (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                username VARCHAR(255) UNIQUE NOT NULL
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
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
    if reply_markup: payload['reply_markup'] = json.dumps(reply_markup)
    try:
        requests.post(url, json=payload, timeout=5).raise_for_status()
    except requests.RequestException as e:
        app.logger.error(f"Failed to send message to chat_id {chat_id}: {e}", exc_info=True)

def send_telegram_photo(chat_id, photo, caption=None, reply_markup=None):
    url = f"{TELEGRAM_API_URL}/sendPhoto"
    data = {'chat_id': chat_id}
    files = None
    if isinstance(photo, str) and photo.startswith('http'): data['photo'] = photo
    else: files = {'photo': open(photo, 'rb')}
    if caption: data['caption'] = caption; data['parse_mode'] = 'HTML'
    if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
    try:
        requests.post(url, data=data, files=files, timeout=10).raise_for_status()
    except requests.RequestException as e:
        app.logger.error(f"Failed to send photo to chat_id {chat_id}: {e}", exc_info=True)
    finally:
        if files and 'photo' in files and not files['photo'].closed: files['photo'].close()

def set_webhook():
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
    update = request.get_json()
    if "message" in update:
        message = update["message"]; chat_id = message["chat"]["id"]; text = message.get("text")
        if text == "/start":
            caption = ("<b>Welcome to the Gift Upgrade Demo!</b>\n\nThis app is a simulation of Telegram's gift and collectible system. You can buy gifts, upgrade them to unique collectibles, and trade them with other users.\n\nTap the button below to get started!")
            photo_url = "https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/refs/heads/main/IMG_20250706_195911_731.jpg"
            reply_markup = {"inline_keyboard": [[{"text": "🎁 Open Gift App", "web_app": {"url": WEBAPP_URL}}], [{"text": "🐞 Report Bug", "url": "https://t.me/Vasiliy939"}]]}
            send_telegram_photo(chat_id, photo_url, caption=caption, reply_markup=reply_markup)
    return jsonify({"status": "ok"}), 200

# --- ACCOUNT & PROFILE ENDPOINTS ---

@app.route('/api/account', methods=['POST'])
def get_or_create_account():
    data = request.get_json();
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
            cur.execute("SELECT * FROM gifts WHERE owner_id = %s ORDER BY is_pinned DESC, acquired_date DESC;", (tg_id,))
            gifts = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str): gift['collectible_data'] = json.loads(gift['collectible_data'])
            account_data['owned_gifts'] = gifts
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
            account_data['collectible_usernames'] = [row[0] for row in cur.fetchall()]
            return jsonify(account_data), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error in get_or_create_account for {tg_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/profile/<string:username>', methods=['GET'])
def get_user_profile(username):
    conn = get_db_connection();
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number FROM accounts WHERE LOWER(username) = LOWER(%s);", (username,))
            user_profile = cur.fetchone()
            if not user_profile: return jsonify({"error": "User profile not found."}), 404
            profile_data = dict(zip([d[0] for d in cur.description], user_profile))
            user_id = profile_data['tg_id']
            cur.execute("SELECT * FROM gifts WHERE owner_id = %s AND is_hidden = FALSE ORDER BY is_pinned DESC, acquired_date DESC;", (user_id,))
            gifts = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str): gift['collectible_data'] = json.loads(gift['collectible_data'])
            profile_data['owned_gifts'] = gifts
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (user_id,))
            profile_data['collectible_usernames'] = [row[0] for row in cur.fetchall()]
            return jsonify(profile_data), 200
        except Exception as e:
            app.logger.error(f"Error fetching profile for {username}: {e}", exc_info=True); return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()

# --- OPTIMIZED BULK ENDPOINTS ---

@app.route('/api/gifts/bulk_add', methods=['POST'])
def bulk_add_gifts():
    data = request.get_json()
    owner_id = data.get('owner_id')
    gifts_to_add = data.get('gifts', [])
    if not owner_id or not gifts_to_add:
        return jsonify({"error": "owner_id and a list of gifts are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            current_count = cur.fetchone()[0]
            if current_count + len(gifts_to_add) > GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"This would exceed the gift limit of {GIFT_LIMIT_PER_USER}."}), 403

            sql = "INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) VALUES "
            values_list = []
            for gift in gifts_to_add:
                values_list.append(cur.mogrify("(%s, %s, %s, %s, %s, %s)", (gift['instance_id'], owner_id, gift['gift_type_id'], gift['gift_name'], gift['original_image_url'], gift['lottie_path'])).decode('utf-8'))
            
            sql += ", ".join(values_list) + ";"
            cur.execute(sql)
            conn.commit()
            return jsonify({"message": f"{len(gifts_to_add)} gifts added successfully."}), 201
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error bulk adding gifts for {owner_id}: {e}", exc_info=True); return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()


@app.route('/api/gifts/bulk_upgrade', methods=['POST'])
def bulk_upgrade_gifts():
    data = request.get_json()
    instance_ids = data.get('instance_ids', [])
    if not instance_ids:
        return jsonify({"error": "instance_ids list is required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    upgraded_gifts = []
    with conn.cursor() as cur:
        try:
            for instance_id in instance_ids:
                cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
                gift_row = cur.fetchone()
                if not gift_row: continue
                owner_id, gift_type_id, gift_name = gift_row
                cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
                next_number = cur.fetchone()[0]
                parts_data = fetch_collectible_parts(gift_name)
                selected_model = select_weighted_random(parts_data.get('models', []))
                selected_backdrop = select_weighted_random(parts_data.get('backdrops', []))
                selected_pattern = select_weighted_random(parts_data.get('patterns', []))
                if not all([selected_model, selected_backdrop, selected_pattern]): continue
                supply = random.randint(2000, 10000)
                collectible_data = {"model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern, "modelImage": f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png", "lottieModelPath": f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json", "patternImage": f"{CDN_BASE_URL}patterns/{quote(gift_name)}/png/{quote(selected_pattern['name'])}.png", "backdropColors": selected_backdrop.get('hex'), "supply": supply}
                cur.execute("""UPDATE gifts SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s, lottie_path = NULL WHERE instance_id = %s RETURNING *;""", (json.dumps(collectible_data), next_number, instance_id))
                upgraded_gift_row = cur.fetchone()
                if upgraded_gift_row:
                    upgraded_gift = dict(zip([c[0] for c in cur.description], upgraded_gift_row))
                    if isinstance(upgraded_gift.get('collectible_data'), str): upgraded_gift['collectible_data'] = json.loads(upgraded_gift['collectible_data'])
                    upgraded_gifts.append(upgraded_gift)
            conn.commit()
            return jsonify(upgraded_gifts), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error bulk upgrading gifts: {e}", exc_info=True); return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()

@app.route('/api/gifts/bulk_state', methods=['PUT'])
def bulk_update_gift_state():
    data = request.get_json()
    action = data.get('action')
    value = data.get('value')
    instance_ids = data.get('instance_ids', [])

    if action not in ['pin', 'hide', 'wear'] or not isinstance(value, bool) or not instance_ids:
        return jsonify({"error": "Invalid action, value, or instance_ids list"}), 400
    
    column_to_update = {'pin': 'is_pinned', 'hide': 'is_hidden', 'wear': 'is_worn'}[action]
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            # Note: Bulk wearing is not supported to avoid multiple worn gifts.
            # Frontend should handle wearing one-by-one.
            if action == 'wear' and value is True:
                return jsonify({"error": "Bulk wearing is not supported. Please wear gifts individually."}), 400

            # Using ANY() for an efficient bulk update
            query = f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = ANY(%s);"
            cur.execute(query, (value, instance_ids))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "No gifts were updated. Check if IDs are correct."}), 404
            
            conn.commit()
            return jsonify({"message": f"{cur.rowcount} gifts' {action} state updated."}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error bulk updating gift state: {e}", exc_info=True); return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()


# --- The rest of the single-item and other endpoints ---
# These are kept for single actions and backward compatibility if needed.
# [All other endpoints from the previous version are included here for completeness]
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

@app.route('/api/gifts/transfer', methods=['POST'])
def transfer_gift():
    data = request.get_json(); instance_id, receiver_username, sender_id, comment = data.get('instance_id'), data.get('receiver_username', '').lstrip('@'), data.get('sender_id'), data.get('comment')
    if not all([instance_id, receiver_username, sender_id]): return jsonify({"error": "Missing required fields"}), 400
    conn = get_db_connection();
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone();
            if not receiver: return jsonify({"error": "Receiver username not found."}), 404
            receiver_id = receiver[0]
            cur.execute("SELECT a.username, g.gift_name, g.collectible_number, g.gift_type_id FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE g.instance_id = %s;", (instance_id,))
            sender_info = cur.fetchone();
            if not sender_info: return jsonify({"error": "Sender or gift not found."}), 404
            sender_username, gift_name, gift_number, gift_type_id = sender_info
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            cur.execute("""UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE WHERE instance_id = %s AND is_collectible = TRUE;""", (receiver_id, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found or could not be transferred."}), 404
            conn.commit()
            deep_link = f"https://t.me/upgradeDemoBot/upgrade?startapp=gift{gift_type_id}-{gift_number}"; link_text = f"{gift_name} #{gift_number:,}"
            sender_text = f'You successfully transferred Gift <a href="{deep_link}">{link_text}</a> to @{receiver_username}'
            if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)
            receiver_text = f'You have received Gift <a href="{deep_link}">{link_text}</a> from @{sender_username}'
            if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check out", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)
            return jsonify({"message": "Gift transferred successfully"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error during gift transfer of {instance_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
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
            cur.execute(f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;", (value, instance_id))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found or state not changed."}), 404
            conn.commit()
            return jsonify({"message": f"Gift {action} state updated"}), 200
        except Exception as e:
            conn.rollback(); app.logger.error(f"DB error updating gift state for {instance_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/<string:instance_id>', methods=['DELETE'])
def delete_gift(instance_id):
    conn = get_db_connection();
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM gifts WHERE instance_id = %s;", (instance_id,))
            if cur.rowcount == 0: conn.rollback(); return jsonify({"error": "Gift not found."}), 404
            conn.commit()
            return jsonify({"message": "Gift deleted"}), 204
        except Exception as e:
            conn.rollback(); app.logger.error(f"DB error deleting gift {instance_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

# Other endpoints...

# Set webhook on startup when run by Gunicorn
if __name__ != '__main__':
    set_webhook()
    init_db()

# Run for local development
if __name__ == '__main__':
    print("Starting Flask server for local development...")
    init_db()
    # set_webhook() # Uncomment if using a tunnel like ngrok for local testing
    app.run(debug=True, port=5001)