import os
import hashlib
import hmac
import json
import logging
from urllib.parse import unquote_plus, parse_qs
import uuid
import asyncio
import random # For weighted random selection

import psycopg2
import psycopg2.extras # For RealDictCursor and potentially random for DB if needed
from flask import Flask, request, jsonify, redirect
from flask_cors import CORS
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, TypeHandler
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL")
SERVER_BASE_URL = os.getenv("SERVER_BASE_URL")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME")
FRONTEND_ORIGIN = "https://vasiliy-katsyka.github.io" # Your GitHub Pages URL

if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_WEBAPP_URL, SERVER_BASE_URL, TELEGRAM_BOT_USERNAME]):
    logging.error("Missing one or more critical environment variables!")
    if not DATABASE_URL: logging.error("DATABASE_URL is missing")
    if not TELEGRAM_BOT_TOKEN: logging.error("TELEGRAM_BOT_TOKEN is missing")
    if not TELEGRAM_WEBAPP_URL: logging.error("TELEGRAM_WEBAPP_URL is missing")
    if not SERVER_BASE_URL: logging.error("SERVER_BASE_URL is missing")
    if not TELEGRAM_BOT_USERNAME: logging.error("TELEGRAM_BOT_USERNAME is missing")
    exit(1)

try:
    init_db() # Call it here
    logger.info("Database initialization check complete on app startup.")
except psycopg2.Error as e:
    logger.error(f"CRITICAL: Database initialization failed on app startup: {e}")
    # Depending on your desired behavior, you might want to prevent the app from starting
    # or have it run in a degraded state. For now, it will log and continue.
    # raise RuntimeError(f"Database initialization failed: {e}") # This would stop the app
except Exception as e:
    logger.error(f"CRITICAL: An unexpected error occurred during database initialization: {e}")

# --- FLASK APP SETUP ---
app = Flask(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CORS SETUP ---
CORS(app, resources={r"/api/*": {"origins": FRONTEND_ORIGIN}}, supports_credentials=True)

# --- TELEGRAM BOT APPLICATION (defined early for use in functions) ---
bot_application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

# --- DATABASE HELPER ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Unable to connect to the database: {e}")
        raise # Re-raise the exception to be handled by the caller or Flask error handlers

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    # Users Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            username VARCHAR(255) UNIQUE,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            photo_url TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Gift Definitions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gift_definitions (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            slug VARCHAR(255) NOT NULL UNIQUE,
            description TEXT,
            original_image_url TEXT,
            cdn_path_prefix TEXT,
            current_highest_collectible_number INT DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Collectible Components
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collectible_components (
            id SERIAL PRIMARY KEY,
            gift_definition_id INT REFERENCES gift_definitions(id) ON DELETE CASCADE,
            component_type VARCHAR(50) NOT NULL, -- 'MODEL', 'BACKDROP', 'PATTERN'
            name VARCHAR(255) NOT NULL,
            image_url TEXT,
            rarity_permille INT DEFAULT 1000,
            backdrop_center_color VARCHAR(7),
            backdrop_edge_color VARCHAR(7),
            UNIQUE (gift_definition_id, component_type, name)
        );
    """)
    # User Gifts (Instances)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_gifts (
            instance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
            gift_definition_id INT NOT NULL REFERENCES gift_definitions(id) ON DELETE RESTRICT,
            acquired_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            is_collectible BOOLEAN DEFAULT FALSE,
            collectible_number INT,
            collectible_model_id INT REFERENCES collectible_components(id),
            collectible_backdrop_id INT REFERENCES collectible_components(id),
            collectible_pattern_id INT REFERENCES collectible_components(id),
            is_hidden BOOLEAN DEFAULT FALSE,
            CONSTRAINT uq_collectible_number_per_gift_def UNIQUE (gift_definition_id, collectible_number)
        );
    """)
    # Pinned Gifts
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pinned_gifts (
            owner_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
            gift_instance_id UUID NOT NULL REFERENCES user_gifts(instance_id) ON DELETE CASCADE,
            pin_order INT,
            PRIMARY KEY (owner_telegram_id, gift_instance_id)
        );
    """)
    # Gift Transfers Log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gift_transfers (
            id SERIAL PRIMARY KEY,
            gift_instance_id UUID NOT NULL,
            from_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
            to_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
            transferred_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized/checked.")

# --- TELEGRAM USER DATA VERIFICATION ---
def verify_telegram_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        params = dict(parse_qs(init_data_str))
        hash_to_check = params.pop('hash', [None])[0]
        if not hash_to_check:
            logger.warning("Hash missing in initData")
            return None

        data_check_string_parts = []
        for key in sorted(params.keys()):
            data_check_string_parts.append(f"{key}={params[key][0]}")
        data_check_string = "\n".join(data_check_string_parts)

        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash == hash_to_check:
            user_data_json = params.get('user', [None])[0]
            if user_data_json:
                try:
                    return json.loads(unquote_plus(user_data_json))
                except json.JSONDecodeError as jde:
                    logger.error(f"Error decoding user JSON from initData: {jde} - Data: {user_data_json}")
                    return None
            else:
                logger.warning("User data missing in initData params")
                return None
        else:
            logger.warning("Hash mismatch in initData verification")
            return None
    except Exception as e:
        logger.error(f"Exception during Telegram data verification: {e}")
        return None

def get_current_user_from_request():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('tma '):
        return None
    
    init_data_str = auth_header.split(' ', 1)[1]
    user_data = verify_telegram_data(init_data_str, TELEGRAM_BOT_TOKEN)
    if not user_data:
        logger.debug(f"Failed to verify user from initData: {init_data_str[:100]}...")
    return user_data

# --- TELEGRAM BOT HANDLERS & SETUP ---
async def start_command(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (telegram_id, username, first_name, last_name, photo_url, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                photo_url = EXCLUDED.photo_url, /* This might be None from user object, photo_url comes from TMA */
                last_seen_at = CURRENT_TIMESTAMP;
        """, (user.id, user.username, user.first_name, user.last_name, None))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error during /start for user {user.id}: {e}")
    finally:
        cur.close()
        conn.close()

    keyboard = [[InlineKeyboardButton("🎁 Open Gift App", web_app={'url': TELEGRAM_WEBAPP_URL})]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! Welcome to the Gift Simulator!",
        reply_markup=reply_markup,
    )

async def send_transfer_notification(bot, to_telegram_id, gift_name, gift_link_part, sender_name):
    safe_gift_link_part = gift_link_part.replace(" ", "%20")
    mini_app_url_with_param = f"{TELEGRAM_WEBAPP_URL}?startapp={safe_gift_link_part}"

    message = (
        f"🎉 You've received a gift!\n\n"
        f"🎁 **{gift_name}**\n"
        f"👤 From: {sender_name}\n\n"
        f"Tap the button below to view your new gift."
    )
    keyboard = [[InlineKeyboardButton("View Gift", web_app={'url': mini_app_url_with_param})]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await bot.send_message(chat_id=to_telegram_id, text=message, reply_markup=reply_markup, parse_mode="Markdown")
        logger.info(f"Sent transfer notification to {to_telegram_id} for {gift_name}")
    except Exception as e:
        logger.error(f"Failed to send transfer notification to {to_telegram_id}: {e}")

bot_application.add_handler(CommandHandler("start", start_command))

async def setup_telegram_webhook():
    webhook_url = f"{SERVER_BASE_URL}/telegram_webhook"
    try:
        current_webhook = await bot_application.bot.get_webhook_info()
        if current_webhook and current_webhook.url == webhook_url:
            logger.info(f"Webhook is already set to {webhook_url}")
            return True
        
        success = await bot_application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES
        )
        if success:
            logger.info(f"Webhook set successfully to {webhook_url}")
            return True
        else:
            logger.error(f"Failed to set webhook to {webhook_url}")
            return False
    except Exception as e:
        logger.error(f"Error setting webhook: {e}")
        return False

# --- API ENDPOINTS & AUTH ---
@app.before_request
def log_user_activity_and_auth():
    if request.method == 'OPTIONS': # Allow CORS preflight
        return 

    if request.path == '/telegram_webhook': # Allow Telegram webhook
        return

    user_data = get_current_user_from_request()
    
    protected_paths_prefixes = ['/api/me', '/api/store/buy']
    is_protected_action_on_gift = request.path.startswith('/api/gifts/') and request.method in ['POST', 'PUT', 'DELETE']
    requires_auth = any(request.path.startswith(p) for p in protected_paths_prefixes) or is_protected_action_on_gift

    if requires_auth:
        if not user_data or 'id' not in user_data:
            logger.warning(f"Unauthorized access attempt to PROTECTED route {request.path} (TMA validation failed or missing)")
            return jsonify({"error": "Unauthorized: Invalid or missing Telegram Mini App authentication"}), 401
    
    if user_data and 'id' in user_data:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, username, first_name, last_name, photo_url, last_seen_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    photo_url = EXCLUDED.photo_url,
                    last_seen_at = CURRENT_TIMESTAMP;
            """, (
                user_data['id'], user_data.get('username'), user_data.get('first_name'),
                user_data.get('last_name'), user_data.get('photo_url')
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB error during user upsert for {user_data['id']} in before_request: {e}")
        finally:
            cur.close()
            conn.close()

@app.route('/api/me/profile', methods=['GET'])
def get_my_profile():
    user_data = get_current_user_from_request() # Auth handled by before_request
    if not user_data or 'id' not in user_data: # Should be caught by before_request
         return jsonify({"error": "Unauthorized (should have been caught by before_request)"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (user_data['id'],))
        profile_info = cur.fetchone()

        cur.execute("""
            SELECT 
                ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
                ug.is_collectible, ug.collectible_number, ug.is_hidden,
                gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
                gd.cdn_path_prefix,
                model.name as model_name, model.image_url as model_image_url,
                pattern.name as pattern_name, pattern.image_url as pattern_image_url,
                backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id AND model.component_type = 'MODEL'
            LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id AND pattern.component_type = 'PATTERN'
            LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id AND backdrop.component_type = 'BACKDROP'
            WHERE ug.owner_telegram_id = %s
            ORDER BY ug.acquired_at DESC;
        """, (user_data['id'],))
        gifts = cur.fetchall()
        
        cur.execute("SELECT gift_instance_id FROM pinned_gifts WHERE owner_telegram_id = %s ORDER BY pin_order ASC", (user_data['id'],))
        pinned_gift_rows = cur.fetchall()
        pinned_gift_ids = [str(row['gift_instance_id']) for row in pinned_gift_rows]
    finally:
        cur.close()
        conn.close()
    
    return jsonify({
        "profile": profile_info,
        "gifts": gifts,
        "pinned_gift_ids": pinned_gift_ids
    })

@app.route('/api/public/profile/<identifier>', methods=['GET'])
def get_public_profile(identifier):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    target_user_id = None
    try:
        if identifier.startswith('@'):
            cur.execute("SELECT telegram_id FROM users WHERE username = %s", (identifier[1:],))
            user_row = cur.fetchone()
            if user_row: target_user_id = user_row['telegram_id']
        elif identifier.isdigit():
            target_user_id = int(identifier)

        if not target_user_id:
            return jsonify({"error": "User not found by identifier"}), 404

        cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (target_user_id,))
        profile_info = cur.fetchone()
        if not profile_info:
            return jsonify({"error": "User profile data not found"}), 404

        cur.execute("""
            SELECT 
                ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
                ug.is_collectible, ug.collectible_number,
                gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
                gd.cdn_path_prefix,
                model.name as model_name, model.image_url as model_image_url,
                pattern.name as pattern_name, pattern.image_url as pattern_image_url,
                backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id AND model.component_type = 'MODEL'
            LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id AND pattern.component_type = 'PATTERN'
            LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id AND backdrop.component_type = 'BACKDROP'
            WHERE ug.owner_telegram_id = %s AND ug.is_hidden = FALSE
            ORDER BY ug.acquired_at DESC;
        """, (target_user_id,))
        gifts = cur.fetchall()

        cur.execute("SELECT gift_instance_id FROM pinned_gifts WHERE owner_telegram_id = %s ORDER BY pin_order ASC", (target_user_id,))
        pinned_gift_rows = cur.fetchall()
        pinned_gift_ids = [str(row['gift_instance_id']) for row in pinned_gift_rows]
    finally:
        cur.close()
        conn.close()
    
    return jsonify({"profile": profile_info, "gifts": gifts, "pinned_gift_ids": pinned_gift_ids})

@app.route('/api/public/gift/<gift_slug_with_number>', methods=['GET'])
def get_public_gift(gift_slug_with_number):
    try:
        parts = gift_slug_with_number.rsplit('-', 1)
        if len(parts) != 2 or not parts[1].isdigit():
            return jsonify({"error": "Invalid gift identifier format. Expected GiftNameSlug-Number"}), 400
        gift_slug, collectible_num_str = parts
        collectible_num = int(collectible_num_str)
    except ValueError:
        return jsonify({"error": "Invalid collectible number in identifier"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
                ug.is_collectible, ug.collectible_number, ug.is_hidden,
                gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
                gd.cdn_path_prefix,
                model.name as model_name, model.image_url as model_image_url, model.rarity_permille as model_rarity_permille,
                pattern.name as pattern_name, pattern.image_url as pattern_image_url, pattern.rarity_permille as pattern_rarity_permille,
                backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color, backdrop.rarity_permille as backdrop_rarity_permille,
                owner.telegram_id as owner_telegram_id_from_join, /* Ensure we get owner's ID */
                owner.username as owner_username, owner.first_name as owner_first_name, owner.photo_url as owner_photo_url
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id AND gd.slug = %s
            JOIN users owner ON ug.owner_telegram_id = owner.telegram_id
            LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id AND model.component_type = 'MODEL'
            LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id AND pattern.component_type = 'PATTERN'
            LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id AND backdrop.component_type = 'BACKDROP'
            WHERE ug.collectible_number = %s AND ug.is_collectible = TRUE;
        """, (gift_slug, collectible_num))
        gift_data = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not gift_data:
        return jsonify({"error": "Collectible gift not found"}), 404
    
    # Ensure owner_telegram_id is present directly on gift_data object
    if 'owner_telegram_id_from_join' in gift_data and 'owner_telegram_id' not in gift_data :
        gift_data['owner_telegram_id'] = gift_data.pop('owner_telegram_id_from_join')
        
    return jsonify(gift_data)

@app.route('/api/store/gifts', methods=['GET'])
def get_store_gifts():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id, name, slug, description, original_image_url, cdn_path_prefix FROM gift_definitions")
        gifts = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return jsonify(gifts)

@app.route('/api/gifts/buy', methods=['POST'])
def buy_gift_api():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data: return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    gift_definition_id = data.get('gift_definition_id')
    quantity = data.get('quantity', 1)

    if not gift_definition_id or not isinstance(quantity, int) or quantity < 1:
        return jsonify({"error": "Invalid request parameters"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    new_gifts_details = []
    try:
        cur.execute("SELECT id, name, slug, original_image_url, cdn_path_prefix FROM gift_definitions WHERE id = %s", (gift_definition_id,))
        gift_def = cur.fetchone()
        if not gift_def: return jsonify({"error": "Gift definition not found"}), 404

        for _ in range(quantity):
            instance_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO user_gifts (instance_id, owner_telegram_id, gift_definition_id, acquired_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING instance_id, acquired_at;
            """, (instance_id, user_data['id'], gift_definition_id))
            new_gift_info = cur.fetchone()
            new_gifts_details.append({
                "instance_id": new_gift_info['instance_id'], "owner_telegram_id": user_data['id'],
                "gift_definition_id": gift_definition_id, "gift_name": gift_def['name'], "gift_slug": gift_def['slug'],
                "gift_original_image_url": gift_def.get('original_image_url'), "cdn_path_prefix": gift_def.get('cdn_path_prefix'),
                "acquired_at": new_gift_info['acquired_at'].isoformat(), "is_collectible": False, "collectible_number": None, "is_hidden": False
            })
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error buying gift: {e}")
        return jsonify({"error": "Failed to buy gift"}), 500
    finally:
        cur.close()
        conn.close()
    return jsonify({"message": "Gifts purchased successfully", "new_gifts": new_gifts_details}), 201

@app.route('/api/gifts/<uuid:instance_id>/upgrade', methods=['POST'])
def upgrade_gift_api(instance_id):
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data: return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("BEGIN;")
        cur.execute("SELECT gift_definition_id, is_collectible FROM user_gifts WHERE instance_id = %s AND owner_telegram_id = %s FOR UPDATE;", (str(instance_id), user_data['id']))
        gift_to_upgrade = cur.fetchone()

        if not gift_to_upgrade: cur.execute("ROLLBACK;"); return jsonify({"error": "Gift not found or not owned"}), 404
        if gift_to_upgrade['is_collectible']: cur.execute("ROLLBACK;"); return jsonify({"error": "Gift already collectible"}), 400
        
        gift_def_id = gift_to_upgrade['gift_definition_id']
        cur.execute("SELECT current_highest_collectible_number FROM gift_definitions WHERE id = %s FOR UPDATE;", (gift_def_id,))
        def_row = cur.fetchone()
        new_collectible_num = def_row['current_highest_collectible_number'] + 1
        cur.execute("UPDATE gift_definitions SET current_highest_collectible_number = %s WHERE id = %s;", (new_collectible_num, gift_def_id))

        components = {'MODEL': None, 'BACKDROP': None, 'PATTERN': None}
        for comp_type in components.keys():
            cur.execute("SELECT id, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color FROM collectible_components WHERE gift_definition_id = %s AND component_type = %s", (gift_def_id, comp_type))
            available_comps = cur.fetchall()
            if not available_comps: cur.execute("ROLLBACK;"); logger.error(f"No {comp_type} for gift_def {gift_def_id}"); return jsonify({"error": f"Missing {comp_type}"}), 500
            
            total_weight = sum(c['rarity_permille'] for c in available_comps)
            rand_val = random.randint(1, total_weight) if total_weight > 0 else 1
            current_sum = 0; selected_comp = None
            for comp_item in available_comps:
                current_sum += comp_item['rarity_permille']
                if rand_val <= current_sum: selected_comp = comp_item; break
            components[comp_type] = selected_comp if selected_comp else available_comps[0]
        
        cur.execute("""
            UPDATE user_gifts SET is_collectible = TRUE, collectible_number = %s, collectible_model_id = %s, collectible_backdrop_id = %s, collectible_pattern_id = %s
            WHERE instance_id = %s;
        """, (new_collectible_num, components['MODEL']['id'], components['BACKDROP']['id'], components['PATTERN']['id'], str(instance_id)))
        cur.execute("COMMIT;")
        
        cur.execute("""
            SELECT ug.*, gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url, gd.cdn_path_prefix,
                   m.name as model_name, m.image_url as model_image_url, m.rarity_permille as model_rarity_permille,
                   p.name as pattern_name, p.image_url as pattern_image_url, p.rarity_permille as pattern_rarity_permille,
                   b.name as backdrop_name, b.backdrop_center_color, b.backdrop_edge_color, b.rarity_permille as backdrop_rarity_permille
            FROM user_gifts ug JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            LEFT JOIN collectible_components m ON ug.collectible_model_id = m.id
            LEFT JOIN collectible_components p ON ug.collectible_pattern_id = p.id
            LEFT JOIN collectible_components b ON ug.collectible_backdrop_id = b.id
            WHERE ug.instance_id = %s;
        """, (str(instance_id),))
        full_details = cur.fetchone()
        return jsonify({"message": "Gift upgraded", "gift": full_details}), 200
    except psycopg2.Error as db_err:
        if 'conn' in locals() and hasattr(conn, 'info') and conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]: cur.execute("ROLLBACK;")
        logger.error(f"DB error upgrading: {db_err}"); return jsonify({"error": "DB upgrade error"}), 500
    except Exception as e:
        if 'conn' in locals() and hasattr(conn, 'info') and conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]: cur.execute("ROLLBACK;")
        logger.error(f"General error upgrading: {e}"); return jsonify({"error": "Upgrade failed"}), 500
    finally:
        if 'cur' in locals() and not cur.closed: cur.close()
        if 'conn' in locals() and not conn.closed: conn.close()

@app.route('/api/gifts/<uuid:instance_id>/visibility', methods=['POST'])
def set_gift_visibility(instance_id):
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data: return jsonify({"error": "Unauthorized"}), 401
    
    is_hidden = request.json.get('hide')
    if not isinstance(is_hidden, bool): return jsonify({"error": "Invalid 'hide' param"}), 400

    conn = get_db_connection(); cur = conn.cursor()
    try:
        cur.execute("UPDATE user_gifts SET is_hidden = %s WHERE instance_id = %s AND owner_telegram_id = %s RETURNING instance_id;", (is_hidden, str(instance_id), user_data['id']))
        if cur.fetchone(): conn.commit(); return jsonify({"message": f"Visibility updated", "instance_id": str(instance_id), "is_hidden": is_hidden}), 200
        else: conn.rollback(); return jsonify({"error": "Gift not found or not owned"}), 404
    finally: cur.close(); conn.close()

@app.route('/api/gifts/pins', methods=['POST'])
def update_pinned_gifts():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data: return jsonify({"error": "Unauthorized"}), 401
    
    pinned_ids = request.json.get('pinned_ids')
    if not isinstance(pinned_ids, list) or len(pinned_ids) > 6: return jsonify({"error": "Invalid 'pinned_ids'"}), 400

    conn = get_db_connection(); cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute("DELETE FROM pinned_gifts WHERE owner_telegram_id = %s", (user_data['id'],))
        for i, id_str in enumerate(pinned_ids):
            try: uuid.UUID(id_str) # Validate UUID
            except ValueError: logger.warning(f"Invalid UUID for pin: {id_str}"); continue
            cur.execute("SELECT 1 FROM user_gifts WHERE instance_id = %s AND owner_telegram_id = %s", (id_str, user_data['id']))
            if cur.fetchone(): cur.execute("INSERT INTO pinned_gifts (owner_telegram_id, gift_instance_id, pin_order) VALUES (%s, %s, %s)", (user_data['id'], id_str, i))
        cur.execute("COMMIT;")
        return jsonify({"message": "Pins updated", "pinned_ids": pinned_ids}), 200
    except Exception as e:
        if hasattr(conn, 'info') and conn.info.transaction_status in [1,2]: cur.execute("ROLLBACK;") # INTRANS or INERROR
        logger.error(f"Error pinning: {e}"); return jsonify({"error": "Pin update failed"}), 500
    finally: cur.close(); conn.close()

@app.route('/api/gifts/<uuid:instance_id>/transfer', methods=['POST'])
async def transfer_gift_api(instance_id):
    sender_data = get_current_user_from_request()
    if not sender_data or 'id' not in sender_data: return jsonify({"error": "Unauthorized"}), 401
    
    recipient_identifier = request.json.get('recipient_identifier')
    if not recipient_identifier: return jsonify({"error": "Recipient missing"}), 400

    conn = get_db_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    recipient_id = None; recipient_info = None
    try:
        if recipient_identifier.isdigit():
            cur.execute("SELECT telegram_id, username, first_name FROM users WHERE telegram_id = %s", (int(recipient_identifier),))
            recipient_info = cur.fetchone()
        else:
            cur.execute("SELECT telegram_id, username, first_name FROM users WHERE username = %s", (recipient_identifier,))
            recipient_info = cur.fetchone()
        
        if not recipient_info: return jsonify({"error": "Recipient not found or not started bot"}), 404
        recipient_id = recipient_info['telegram_id']
        if recipient_id == sender_data['id']: return jsonify({"error": "Cannot transfer to self"}), 400

        cur.execute("BEGIN;")
        cur.execute("SELECT gd.name as gift_name, gd.slug as gift_slug, ug.collectible_number FROM user_gifts ug JOIN gift_definitions gd ON ug.gift_definition_id = gd.id WHERE ug.instance_id = %s AND ug.owner_telegram_id = %s FOR UPDATE;", (str(instance_id), sender_data['id']))
        gift_info = cur.fetchone()
        if not gift_info: cur.execute("ROLLBACK;"); return jsonify({"error": "Gift not found or not owned"}), 404
        
        cur.execute("UPDATE user_gifts SET owner_telegram_id = %s, is_hidden = FALSE WHERE instance_id = %s;", (recipient_id, str(instance_id)))
        cur.execute("DELETE FROM pinned_gifts WHERE owner_telegram_id = %s AND gift_instance_id = %s;", (sender_data['id'], str(instance_id)))
        cur.execute("INSERT INTO gift_transfers (gift_instance_id, from_telegram_id, to_telegram_id) VALUES (%s, %s, %s);", (str(instance_id), sender_data['id'], recipient_id))
        cur.execute("COMMIT;")
        
        # Notification outside transaction
        link_part = f"{gift_info['gift_slug']}-{gift_info['collectible_number']}" if gift_info['collectible_number'] else gift_info['gift_slug']
        sender_name = sender_data.get('first_name', sender_data.get('username', f"User {sender_data['id']}"))
        await send_transfer_notification(bot_application.bot, recipient_id, gift_info['gift_name'], link_part, sender_name)
        
        rec_display = recipient_info.get('username') or recipient_info.get('first_name') or f"User {recipient_id}"
        return jsonify({"message": f"Gift transferred to {rec_display}"}), 200
    except psycopg2.Error as db_err:
        if hasattr(conn, 'info') and conn.info.transaction_status in [1,2]: cur.execute("ROLLBACK;")
        logger.error(f"DB error transfer: {db_err}"); return jsonify({"error": "DB transfer error"}), 500
    except Exception as e:
        if hasattr(conn, 'info') and conn.info.transaction_status in [1,2]: cur.execute("ROLLBACK;")
        logger.error(f"General error transfer: {e}"); return jsonify({"error": "Transfer failed"}), 500
    finally: cur.close(); conn.close()

@app.route('/telegram_webhook', methods=['POST'])
async def telegram_webhook():
    if request.content_type != 'application/json': return jsonify({"status": "error", "message": "Invalid content type"}), 400
    try:
        update_data = request.get_json()
        if not update_data: return jsonify({"status": "error", "message": "Empty payload"}), 400
        update = Update.de_json(update_data, bot_application.bot)
        await bot_application.process_update(update)
        return jsonify({"status": "ok"})
    except json.JSONDecodeError: return jsonify({"status": "error", "message": "Invalid JSON"}), 400
    except Exception as e: logger.error(f"Webhook error: {e}"); return jsonify({"status": "error", "message": "Internal error"}), 500

def insert_dummy_data():
    conn = get_db_connection(); cur = conn.cursor()
    try:
        # User (ensure this user has started your bot)
        test_user_id = int(os.getenv("DUMMY_TEST_USER_ID", 0)) # Set this in .env for your test ID
        if test_user_id:
             cur.execute("INSERT INTO users (telegram_id, username, first_name) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",(test_user_id, 'testdummy', 'TestDummy'))
        
        defs = [('Plush Pepe', 'plush-pepe', 'https://cdn.example.com/pepe.png', 'pepe_collectibles/'),
                ('Diamond Cat', 'diamond-cat', 'https://cdn.example.com/cat.png', 'cat_collectibles/')]
        for name, slug, img, cdn in defs:
            cur.execute("INSERT INTO gift_definitions (name, slug, original_image_url, cdn_path_prefix) VALUES (%s,%s,%s,%s) ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name RETURNING id, slug;", (name,slug,img,cdn))
            def_res = cur.fetchone()
            if not def_res: continue # Already existed and no update needed
            def_id, def_slug = def_res
            if def_slug == 'plush-pepe':
                comps = [(def_id, 'MODEL', 'Cool Pepe', 'models/pepe_cool.png', 500, None, None),
                         (def_id, 'BACKDROP', 'Galaxy', None, 300, '#100020', '#301050'),
                         (def_id, 'PATTERN', 'Sparkles', 'patterns/sparkles.png', 600, None, None)]
                for comp_data in comps: cur.execute("INSERT INTO collectible_components (gift_definition_id, component_type, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;", comp_data)
            if test_user_id: # Give test user the first gift if they were inserted/exist
                cur.execute("SELECT id FROM gift_definitions ORDER BY id LIMIT 1")
                first_gift_def = cur.fetchone()
                if first_gift_def:
                    cur.execute("INSERT INTO user_gifts (owner_telegram_id, gift_definition_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (test_user_id, first_gift_def[0]))
        conn.commit()
        logger.info("Dummy data attempt complete.")
    except Exception as e: conn.rollback(); logger.error(f"Dummy data error: {e}")
    finally: cur.close(); conn.close()


if __name__ == '__main__':
    init_db()
    try:
        asyncio.run(setup_telegram_webhook())
    except Exception as e:
        logger.error(f"Startup: Failed to run webhook setup: {e}")
    
    # insert_dummy_data() # Uncomment for initial data population
    
    is_local_dev = os.getenv("FLASK_ENV") == "development" or os.getenv("FLASK_DEBUG") == "1"
    if is_local_dev:
        logger.info("Running Flask app in local development mode.")
        app.run(debug=True, host='0.0.0.0', port=int(os.getenv("PORT", 8080)))
    # For production, Gunicorn will run `app`
