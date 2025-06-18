import os
import hashlib
import hmac
import json
import logging
from urllib.parse import unquote_plus, parse_qs
import uuid
import asyncio # Added for running async set_webhook

import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify, redirect
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, TypeHandler
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file for local dev

# --- CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_WEBAPP_URL = os.getenv("TELEGRAM_WEBAPP_URL") # Your WebApp URL
SERVER_BASE_URL = os.getenv("SERVER_BASE_URL") # Your backend server URL (for webhook)
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME")

if not all([DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_WEBAPP_URL, SERVER_BASE_URL, TELEGRAM_BOT_USERNAME]):
    logging.error("Missing one or more critical environment variables!")
    # In a real app, you might raise an exception or handle this more gracefully
    # For now, we print and exit if critical vars are missing.
    if not DATABASE_URL: logging.error("DATABASE_URL is missing")
    if not TELEGRAM_BOT_TOKEN: logging.error("TELEGRAM_BOT_TOKEN is missing")
    if not TELEGRAM_WEBAPP_URL: logging.error("TELEGRAM_WEBAPP_URL is missing")
    if not SERVER_BASE_URL: logging.error("SERVER_BASE_URL is missing")
    if not TELEGRAM_BOT_USERNAME: logging.error("TELEGRAM_BOT_USERNAME is missing")
    exit(1)

# --- FLASK APP SETUP ---
app = Flask(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATABASE HELPER ---
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

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
            slug VARCHAR(255) NOT NULL UNIQUE, -- For URL linking (e.g., plush-pepe)
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
        # logger.debug("Authorization header missing or not TMA")
        return None
    
    init_data_str = auth_header.split(' ', 1)[1]
    user_data = verify_telegram_data(init_data_str, TELEGRAM_BOT_TOKEN)
    if not user_data:
        logger.warning(f"Failed to verify user from initData: {init_data_str[:100]}...") # Log first 100 chars
    return user_data

# --- TELEGRAM BOT SETUP ---
async def start_command(update: Update, context: CallbackContext) -> None: # Renamed from 'start' to avoid conflict
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
                photo_url = EXCLUDED.photo_url,
                last_seen_at = CURRENT_TIMESTAMP;
        """, (user.id, user.username, user.first_name, user.last_name, None)) # Photo URL from TMA, not here
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
    # Ensure gift_link_part doesn't create invalid URL characters if it's just a slug
    safe_gift_link_part = gift_link_part.replace(" ", "%20") # Basic space encoding
    
    # Construct the Web App URL for the specific gift
    # Example: TELEGRAM_WEBAPP_URL might be "https://username.github.io/appname/"
    # We need to append the ?startapp correctly.
    # If TELEGRAM_WEBAPP_URL already has query params, handle it carefully. For now, assume it's clean.
    
    base_app_url_for_bot_link = TELEGRAM_WEBAPP_URL 
    # The WebApp URL passed to InlineKeyboardButton web_app parameter should be the direct link to the app.
    # The startapp parameter is handled by the Mini App itself when it loads.
    # So, the link inside the Mini App will be constructed differently than the button for the bot message.
    
    # The link the Mini App uses to open another part of itself:
    # `https://t.me/YOUR_BOT_USERNAME/YOUR_MINI_APP_SHORT_NAME?startapp={gift_link_part}`
    # For the button in the bot message, it's simpler:
    mini_app_url_with_param = f"{base_app_url_for_bot_link}?startapp={safe_gift_link_part}"


    message = (
        f"🎉 You've received a gift!\n\n"
        f"🎁 **{gift_name}**\n"
        f"👤 From: {sender_name}\n\n"
        f"Tap the button below to view your new gift."
    )
    # The URL for web_app in InlineKeyboardButton MUST be https
    keyboard = [[InlineKeyboardButton("View Gift", web_app={'url': mini_app_url_with_param})]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await bot.send_message(chat_id=to_telegram_id, text=message, reply_markup=reply_markup, parse_mode="Markdown")
        logger.info(f"Sent transfer notification to {to_telegram_id} for {gift_name}")
    except Exception as e:
        logger.error(f"Failed to send transfer notification to {to_telegram_id}: {e}")


bot_application = Application.builder().token(TELEGRAM_BOT_TOKEN).build() # Renamed to bot_application
bot_application.add_handler(CommandHandler("start", start_command))


async def setup_telegram_webhook():
    """Sets the Telegram bot webhook."""
    webhook_url = f"{SERVER_BASE_URL}/telegram_webhook"
    try:
        current_webhook = await bot_application.bot.get_webhook_info()
        if current_webhook and current_webhook.url == webhook_url:
            logger.info(f"Webhook is already set to {webhook_url}")
            return True
        
        success = await bot_application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES # Or specify types like ["message", "callback_query"]
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

# --- API ENDPOINTS ---
@app.before_request
def log_user_activity_and_auth():
    # Allow webhook to pass through without TMA check
    if request.path == '/telegram_webhook':
        return

    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        # For public API endpoints, we might not require TMA if data is public.
        # But for /api/me/* or actions, it's required.
        # This global check can be refined per-route or with decorators.
        if request.path.startswith('/api/me') or \
           request.path.startswith('/api/store/buy') or \
           request.path.startswith('/api/gifts/') and request.method == 'POST': # Actions on gifts
            logger.warning(f"Unauthorized access attempt to {request.path}")
            return jsonify({"error": "Unauthorized: Invalid or missing Telegram Mini App authentication"}), 401
        # else, it's a public path, proceed without user_data necessarily
    
    if user_data and 'id' in user_data: # If TMA is valid, update user
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
                user_data['id'],
                user_data.get('username'),
                user_data.get('first_name'),
                user_data.get('last_name'),
                user_data.get('photo_url') # Comes from TMA
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB error during user upsert for {user_data['id']}: {e}")
        finally:
            cur.close()
            conn.close()
    
    # Store user_data in Flask's g object if needed by routes, for cleaner access
    # from flask import g
    # g.user = user_data


@app.route('/api/me/profile', methods=['GET'])
def get_my_profile():
    user_data = get_current_user_from_request() # Relies on before_request for auth check
    if not user_data or 'id' not in user_data: # This check is now redundant if before_request handles it for /api/me
        return jsonify({"error": "Unauthorized"}), 401


    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Fetch user details
    cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (user_data['id'],))
    profile_info = cur.fetchone()

    # Fetch user gifts
    cur.execute("""
        SELECT 
            ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
            ug.is_collectible, ug.collectible_number, ug.is_hidden,
            gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
            gd.cdn_path_prefix, /* Added cdn_path_prefix */
            model.name as model_name, model.image_url as model_image_url,
            pattern.name as pattern_name, pattern.image_url as pattern_image_url,
            backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color
            /* Removed is_pinned_on_profile here, will get pinned_gift_ids separately */
        FROM user_gifts ug
        JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
        LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id AND model.component_type = 'MODEL'
        LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id AND pattern.component_type = 'PATTERN'
        LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id AND backdrop.component_type = 'BACKDROP'
        WHERE ug.owner_telegram_id = %s
        ORDER BY ug.acquired_at DESC;
    """, (user_data['id'],))
    gifts = cur.fetchall()
    
    # Fetch pinned gift instance_ids
    cur.execute("SELECT gift_instance_id FROM pinned_gifts WHERE owner_telegram_id = %s ORDER BY pin_order ASC", (user_data['id'],))
    pinned_gift_rows = cur.fetchall()
    pinned_gift_ids = [str(row['gift_instance_id']) for row in pinned_gift_rows] # Ensure UUIDs are strings for JSON

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
    if identifier.startswith('@'):
        username = identifier[1:]
        cur.execute("SELECT telegram_id FROM users WHERE username = %s", (username,))
        user_row = cur.fetchone()
        if user_row:
            target_user_id = user_row['telegram_id']
    elif identifier.isdigit():
        target_user_id = int(identifier)

    if not target_user_id:
        cur.close(); conn.close()
        return jsonify({"error": "User not found by identifier"}), 404

    cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (target_user_id,))
    profile_info = cur.fetchone()
    if not profile_info:
        cur.close(); conn.close()
        return jsonify({"error": "User profile data not found"}), 404

    # Fetch public gifts (not hidden)
    cur.execute("""
        SELECT 
            ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
            ug.is_collectible, ug.collectible_number,
            gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
            gd.cdn_path_prefix, /* Added cdn_path_prefix */
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
    
    cur.close()
    conn.close()
    return jsonify({
        "profile": profile_info,
        "gifts": gifts,
        "pinned_gift_ids": pinned_gift_ids
    })


@app.route('/api/public/gift/<gift_slug_with_number>', methods=['GET'])
def get_public_gift(gift_slug_with_number):
    try:
        parts = gift_slug_with_number.rsplit('-', 1)
        if len(parts) != 2 or not parts[1].isdigit():
            return jsonify({"error": "Invalid gift identifier format. Expected GiftNameSlug-Number"}), 400
        gift_slug = parts[0]
        collectible_num = int(parts[1])
    except ValueError:
        return jsonify({"error": "Invalid collectible number"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
            ug.is_collectible, ug.collectible_number, ug.is_hidden,
            gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
            gd.cdn_path_prefix, /* Added cdn_path_prefix */
            model.name as model_name, model.image_url as model_image_url, model.rarity_permille as model_rarity_permille,
            pattern.name as pattern_name, pattern.image_url as pattern_image_url, pattern.rarity_permille as pattern_rarity_permille,
            backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color, backdrop.rarity_permille as backdrop_rarity_permille,
            owner.telegram_id as owner_telegram_id, /* Explicitly select owner_telegram_id */
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
    cur.close()
    conn.close()

    if not gift_data:
        return jsonify({"error": "Collectible gift not found"}), 404
    
    # Add owner_telegram_id if not already present from join (it should be)
    if 'owner_telegram_id' not in gift_data and gift_data.get('ug.owner_telegram_id'): # Check if it was prefixed
        gift_data['owner_telegram_id'] = gift_data['ug.owner_telegram_id']

    return jsonify(gift_data)

@app.route('/api/store/gifts', methods=['GET'])
def get_store_gifts():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, name, slug, description, original_image_url FROM gift_definitions")
    gifts = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(gifts)

@app.route('/api/gifts/buy', methods=['POST'])
def buy_gift_api():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    gift_definition_id = data.get('gift_definition_id')
    quantity = data.get('quantity', 1)

    if not gift_definition_id or not isinstance(quantity, int) or quantity < 1:
        return jsonify({"error": "Invalid request parameters"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("SELECT id, name, slug, original_image_url, cdn_path_prefix FROM gift_definitions WHERE id = %s", (gift_definition_id,))
    gift_def = cur.fetchone()
    if not gift_def:
        cur.close(); conn.close()
        return jsonify({"error": "Gift definition not found"}), 404

    new_gifts_details = []
    try:
        for _ in range(quantity):
            instance_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO user_gifts (instance_id, owner_telegram_id, gift_definition_id, acquired_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING instance_id, acquired_at;
            """, (instance_id, user_data['id'], gift_definition_id))
            new_gift_info = cur.fetchone()
            new_gifts_details.append({
                "instance_id": new_gift_info['instance_id'],
                "owner_telegram_id": user_data['id'], # Add owner_telegram_id
                "gift_definition_id": gift_definition_id,
                "gift_name": gift_def['name'],
                "gift_slug": gift_def['slug'],
                "gift_original_image_url": gift_def.get('original_image_url'),
                "cdn_path_prefix": gift_def.get('cdn_path_prefix'),
                "acquired_at": new_gift_info['acquired_at'].isoformat(),
                "is_collectible": False,
                "collectible_number": None,
                "is_hidden": False
                # Component fields will be null
            })
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error buying gift: {e}")
        cur.close(); conn.close()
        return jsonify({"error": "Failed to buy gift"}), 500
    
    cur.close()
    conn.close()
    return jsonify({"message": "Gifts purchased successfully", "new_gifts": new_gifts_details}), 201


@app.route('/api/gifts/<uuid:instance_id>/upgrade', methods=['POST'])
def upgrade_gift_api(instance_id):
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        cur.execute("BEGIN;") # Start transaction explicitly
        cur.execute("""
            SELECT gift_definition_id, is_collectible 
            FROM user_gifts 
            WHERE instance_id = %s AND owner_telegram_id = %s FOR UPDATE;
        """, (str(instance_id), user_data['id'])) # Lock the user_gift row
        gift_to_upgrade = cur.fetchone()

        if not gift_to_upgrade:
            cur.execute("ROLLBACK;")
            return jsonify({"error": "Gift not found or not owned by user"}), 404
        if gift_to_upgrade['is_collectible']:
            cur.execute("ROLLBACK;")
            return jsonify({"error": "Gift is already collectible"}), 400

        gift_def_id = gift_to_upgrade['gift_definition_id']

        cur.execute("""
            SELECT current_highest_collectible_number 
            FROM gift_definitions 
            WHERE id = %s FOR UPDATE;
        """, (gift_def_id,)) # Lock the gift_definition row
        def_row = cur.fetchone()
        new_collectible_num = def_row['current_highest_collectible_number'] + 1
        
        cur.execute("""
            UPDATE gift_definitions 
            SET current_highest_collectible_number = %s 
            WHERE id = %s;
        """, (new_collectible_num, gift_def_id))
        
        # Weighted random selection of components
        components = {'MODEL': None, 'BACKDROP': None, 'PATTERN': None}
        for comp_type in components.keys():
            cur.execute("""
                SELECT id, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color
                FROM collectible_components 
                WHERE gift_definition_id = %s AND component_type = %s
            """, (gift_def_id, comp_type))
            available_comps = cur.fetchall()
            if not available_comps:
                cur.execute("ROLLBACK;")
                logger.error(f"No components of type {comp_type} found for gift_definition_id {gift_def_id}")
                return jsonify({"error": f"Configuration error: Missing {comp_type} components for this gift type."}), 500
            
            # Simple weighted random (can be improved)
            total_weight = sum(c['rarity_permille'] for c in available_comps)
            rand_val = psycopg2.extras.random.randint(1, total_weight) # Use psycopg2.extras.random if available, else import random
            current_sum = 0
            selected_comp = None
            for comp_item in available_comps:
                current_sum += comp_item['rarity_permille']
                if rand_val <= current_sum:
                    selected_comp = comp_item
                    break
            components[comp_type] = selected_comp if selected_comp else available_comps[0] # Fallback to first

        model_id = components['MODEL']['id']
        backdrop_id = components['BACKDROP']['id']
        pattern_id = components['PATTERN']['id']
        
        cur.execute("""
            UPDATE user_gifts 
            SET is_collectible = TRUE, 
                collectible_number = %s,
                collectible_model_id = %s,
                collectible_backdrop_id = %s,
                collectible_pattern_id = %s
            WHERE instance_id = %s;
        """, (new_collectible_num, model_id, backdrop_id, pattern_id, str(instance_id)))
        
        cur.execute("COMMIT;") # Commit transaction
        
        # Fetch full details for response AFTER commit
        cur.execute("""
            SELECT 
                ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
                ug.is_collectible, ug.collectible_number, ug.is_hidden,
                gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
                gd.cdn_path_prefix,
                model.name as model_name, model.image_url as model_image_url, model.rarity_permille as model_rarity_permille,
                pattern.name as pattern_name, pattern.image_url as pattern_image_url, pattern.rarity_permille as pattern_rarity_permille,
                backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color, backdrop.rarity_permille as backdrop_rarity_permille
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id
            LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id
            LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id
            WHERE ug.instance_id = %s;
        """, (str(instance_id),))
        full_upgraded_gift_details = cur.fetchone()

        return jsonify({"message": "Gift upgraded successfully", "gift": full_upgraded_gift_details}), 200

    except psycopg2.Error as db_err: # Catch specific DB errors for better rollback
        if conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]:
            cur.execute("ROLLBACK;")
        logger.error(f"Database error during upgrade: {db_err}")
        return jsonify({"error": "Failed to upgrade gift due to a database issue. Please try again."}), 500
    except Exception as e:
        # Generic rollback if transaction might be active
        if 'conn' in locals() and hasattr(conn, 'info') and conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]:
             if 'cur' in locals() and not cur.closed : cur.execute("ROLLBACK;") # cur might be closed already if previous error
        logger.error(f"General error upgrading gift: {e}")
        return jsonify({"error": "Failed to upgrade gift"}), 500
    finally:
        if 'cur' in locals() and not cur.closed: cur.close()
        if 'conn' in locals() and not conn.closed: conn.close()


@app.route('/api/gifts/<uuid:instance_id>/visibility', methods=['POST'])
def set_gift_visibility(instance_id):
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    is_hidden = data.get('hide')
    if not isinstance(is_hidden, bool):
        return jsonify({"error": "Invalid 'hide' parameter"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE user_gifts 
        SET is_hidden = %s 
        WHERE instance_id = %s AND owner_telegram_id = %s
        RETURNING instance_id;
    """, (is_hidden, str(instance_id), user_data['id']))
    
    updated_gift = cur.fetchone()
    if updated_gift:
        conn.commit()
        action = "hidden" if is_hidden else "made visible"
        cur.close(); conn.close()
        return jsonify({"message": f"Gift {action} successfully", "instance_id": str(instance_id), "is_hidden": is_hidden}), 200
    else:
        conn.rollback()
        cur.close(); conn.close()
        return jsonify({"error": "Gift not found or not owned by user"}), 404


@app.route('/api/gifts/pins', methods=['POST'])
def update_pinned_gifts():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    pinned_instance_ids_str = data.get('pinned_ids') 
    if not isinstance(pinned_instance_ids_str, list) or len(pinned_instance_ids_str) > 6:
        return jsonify({"error": "Invalid 'pinned_ids' parameter, must be a list of max 6 UUID strings"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute("DELETE FROM pinned_gifts WHERE owner_telegram_id = %s", (user_data['id'],))
        
        for i, instance_id_str_val in enumerate(pinned_instance_ids_str):
            # Validate UUID format before trying to use it in DB
            try:
                uuid.UUID(instance_id_str_val) # Will raise ValueError if not a valid UUID string
            except ValueError:
                logger.warning(f"Invalid UUID string {instance_id_str_val} provided for pinning by user {user_data['id']}")
                continue # Skip invalid UUIDs

            cur.execute("SELECT 1 FROM user_gifts WHERE instance_id = %s AND owner_telegram_id = %s", (instance_id_str_val, user_data['id']))
            if cur.fetchone():
                cur.execute("""
                    INSERT INTO pinned_gifts (owner_telegram_id, gift_instance_id, pin_order)
                    VALUES (%s, %s, %s)
                """, (user_data['id'], instance_id_str_val, i))
            else:
                logger.warning(f"User {user_data['id']} attempted to pin unowned or non-existent gift {instance_id_str_val}")

        cur.execute("COMMIT;")
        return jsonify({"message": "Pinned gifts updated successfully", "pinned_ids": pinned_instance_ids_str}), 200
    except Exception as e:
        if conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]:
            cur.execute("ROLLBACK;")
        logger.error(f"Error updating pinned gifts: {e}")
        return jsonify({"error": "Failed to update pinned gifts"}), 500
    finally:
        cur.close()
        conn.close()


@app.route('/api/gifts/<uuid:instance_id>/transfer', methods=['POST'])
async def transfer_gift_api(instance_id): # Made async for bot notification
    sender_user_data = get_current_user_from_request()
    if not sender_user_data or 'id' not in sender_user_data:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    recipient_identifier = data.get('recipient_identifier') 
    
    if not recipient_identifier:
        return jsonify({"error": "Recipient identifier missing"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    recipient_telegram_id = None
    recipient_user_info_for_notif = None # To store recipient's details for notification

    if recipient_identifier.isdigit():
        cur.execute("SELECT telegram_id, username, first_name FROM users WHERE telegram_id = %s", (int(recipient_identifier),))
        recipient_user_info_for_notif = cur.fetchone()
        if recipient_user_info_for_notif:
            recipient_telegram_id = recipient_user_info_for_notif['telegram_id']
        else:
             cur.close(); conn.close()
             return jsonify({"error": "Recipient user must start the bot first to be registered."}), 404
    else: 
        cur.execute("SELECT telegram_id, username, first_name FROM users WHERE username = %s", (recipient_identifier,))
        recipient_user_info_for_notif = cur.fetchone()
        if recipient_user_info_for_notif:
            recipient_telegram_id = recipient_user_info_for_notif['telegram_id']
        else:
             cur.close(); conn.close()
             return jsonify({"error": "Recipient user with that username not found or hasn't started the bot."}), 404
             
    if not recipient_telegram_id: 
        cur.close(); conn.close()
        return jsonify({"error": "Recipient user could not be resolved."}), 404
        
    if recipient_telegram_id == sender_user_data['id']:
        cur.close(); conn.close()
        return jsonify({"error": "Cannot transfer gift to yourself."}), 400

    try:
        cur.execute("BEGIN;")
        cur.execute("""
            SELECT ug.gift_definition_id, gd.name as gift_name, gd.slug as gift_slug, ug.collectible_number
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            WHERE ug.instance_id = %s AND ug.owner_telegram_id = %s FOR UPDATE; 
        """, (str(instance_id), sender_user_data['id']))
        gift_to_transfer = cur.fetchone()

        if not gift_to_transfer:
            cur.execute("ROLLBACK;")
            cur.close(); conn.close()
            return jsonify({"error": "Gift not found, not owned by sender, or already being transferred."}), 404
        
        cur.execute("""
            UPDATE user_gifts 
            SET owner_telegram_id = %s, is_hidden = FALSE
            WHERE instance_id = %s;
        """, (recipient_telegram_id, str(instance_id)))
        
        cur.execute("""
            DELETE FROM pinned_gifts 
            WHERE owner_telegram_id = %s AND gift_instance_id = %s;
        """, (sender_user_data['id'], str(instance_id)))
        
        cur.execute("""
            INSERT INTO gift_transfers (gift_instance_id, from_telegram_id, to_telegram_id, transferred_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
        """, (str(instance_id), sender_user_data['id'], recipient_telegram_id))
        
        cur.execute("COMMIT;")

        # Prepare for notification
        gift_name_for_notif = gift_to_transfer['gift_name']
        gift_link_part = f"{gift_to_transfer['gift_slug']}-{gift_to_transfer['collectible_number']}" if gift_to_transfer['collectible_number'] else gift_to_transfer['gift_slug']
        sender_name_for_notif = sender_user_data.get('first_name', sender_user_data.get('username', f"User {sender_user_data['id']}"))
        
        cur.close(); conn.close() # Close DB before await

        await send_transfer_notification(
            bot_application.bot, 
            recipient_telegram_id, 
            gift_name_for_notif, 
            gift_link_part, 
            sender_name_for_notif
        )
        
        recipient_display_name = recipient_user_info_for_notif.get('username') or recipient_user_info_for_notif.get('first_name') or f"User {recipient_telegram_id}"
        return jsonify({"message": f"Gift '{gift_name_for_notif}' transferred successfully to {recipient_display_name}."}), 200

    except psycopg2.Error as db_err:
        if conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]:
            cur.execute("ROLLBACK;")
        logger.error(f"Database error during transfer: {db_err}")
        cur.close(); conn.close()
        return jsonify({"error": "Failed to transfer gift due to a database issue."}), 500
    except Exception as e:
        if 'conn' in locals() and hasattr(conn, 'info') and conn.info.transaction_status in [psycopg2.extensions.TRANSACTION_STATUS_INERROR, psycopg2.extensions.TRANSACTION_STATUS_INTRANS]:
             if 'cur' in locals() and not cur.closed : cur.execute("ROLLBACK;")
        logger.error(f"General error transferring gift: {e}")
        cur.close(); conn.close()
        return jsonify({"error": "Failed to transfer gift"}), 500


# --- TELEGRAM WEBHOOK HANDLER ---
@app.route('/telegram_webhook', methods=['POST'])
async def telegram_webhook():
    if request.content_type != 'application/json':
        logger.warning(f"Webhook received non-JSON content type: {request.content_type}")
        return jsonify({"status": "error", "message": "Invalid content type"}), 400
    try:
        update_data = request.get_json()
        if not update_data:
            logger.warning("Webhook received empty JSON payload.")
            return jsonify({"status": "error", "message": "Empty payload"}), 400
            
        # logger.debug(f"Webhook received: {json.dumps(update_data, indent=2)}") # Can be very verbose
        update = Update.de_json(update_data, bot_application.bot)
        await bot_application.process_update(update)
        return jsonify({"status": "ok"})
    except json.JSONDecodeError as jde:
        logger.error(f"Webhook JSONDecodeError: {jde} - Data: {request.data[:500]}") # Log first 500 chars of raw data
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500


# --- DUMMY DATA INSERTION (FOR TESTING) ---
def insert_dummy_data(): # Make sure this is idempotent or guarded
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Test User (replace with your actual test user ID after they /start the bot)
        test_user_id = 123456789 # Example ID - MAKE SURE THIS USER EXISTS or /start first
        cur.execute("SELECT 1 FROM users WHERE telegram_id = %s", (test_user_id,))
        if not cur.fetchone():
            logger.info(f"Test user {test_user_id} not found. Please /start the bot with this user first.")
            # return # Or insert a basic entry:
            cur.execute("""
                INSERT INTO users (telegram_id, username, first_name) VALUES (%s, %s, %s)
                ON CONFLICT (telegram_id) DO NOTHING;
            """, (test_user_id, 'dummyuserfordata', 'Dummy'))


        # Gift Definitions
        gift_defs_data = [
            ('Plush Pepe', 'plush-pepe', 'https://telegram.org/img/t_logo.png', 'pepe_collectibles/'), # Replace with actual URLs
            ('Diamond Cat', 'diamond-cat', 'https://telegram.org/img/t_logo.png', 'cat_collectibles/')
        ]
        for name, slug, img_url, cdn_prefix in gift_defs_data:
            cur.execute("""
                INSERT INTO gift_definitions (name, slug, original_image_url, cdn_path_prefix) VALUES (%s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name RETURNING id, slug;
            """, (name, slug, img_url, cdn_prefix))
            def_res = cur.fetchone()
            if def_res:
                def_id, def_slug = def_res
                logger.info(f"Upserted gift_definition: {def_slug} (ID: {def_id})")
                
                # Components for Plush Pepe
                if def_slug == 'plush-pepe':
                    pepe_comps = [
                        (def_id, 'MODEL', 'Cool Pepe', 'pepe_collectibles/models/pepe_cool.png', 500, None, None),
                        (def_id, 'MODEL', 'Sad Pepe', 'pepe_collectibles/models/pepe_sad.png', 500, None, None),
                        (def_id, 'BACKDROP', 'Galaxy', None, 300, '#100020', '#301050'),
                        (def_id, 'BACKDROP', 'Forest', None, 700, '#205010', '#40A030'),
                        (def_id, 'PATTERN', 'Sparkles', 'pepe_collectibles/patterns/sparkles.png', 600, None, None),
                        (def_id, 'PATTERN', 'Hearts', 'pepe_collectibles/patterns/hearts.png', 400, None, None)
                    ]
                    for comp_data in pepe_comps:
                        cur.execute("""INSERT INTO collectible_components 
                                    (gift_definition_id, component_type, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color) VALUES
                                    (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (gift_definition_id, component_type, name) DO NOTHING;
                        """, comp_data)
                
                # Components for Diamond Cat (simplified)
                elif def_slug == 'diamond-cat':
                    cat_comps = [
                        (def_id, 'MODEL', 'Shiny Cat', 'cat_collectibles/models/cat_shiny.png', 1000, None, None),
                        (def_id, 'BACKDROP', 'Velvet', None, 1000, '#400000', '#800000'),
                        (def_id, 'PATTERN', 'Glow', 'cat_collectibles/patterns/glow.png', 1000, None, None)
                    ]
                    for comp_data in cat_comps:
                         cur.execute("""INSERT INTO collectible_components 
                                    (gift_definition_id, component_type, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color) VALUES
                                    (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (gift_definition_id, component_type, name) DO NOTHING;
                        """, comp_data)

        # Give test_user_id the first gift_definition if they exist
        cur.execute("SELECT id FROM gift_definitions ORDER BY id LIMIT 1")
        first_gift_def = cur.fetchone()
        cur.execute("SELECT 1 FROM users WHERE telegram_id = %s", (test_user_id,)) # Re-check test user
        if first_gift_def and cur.fetchone():
            cur.execute("""
                INSERT INTO user_gifts (owner_telegram_id, gift_definition_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING; 
            """, (test_user_id, first_gift_def[0]))
            
        conn.commit()
        logger.info("Dummy data insertion/update attempt complete.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting dummy data: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    # Initialize DB schema if it doesn't exist
    init_db() 
    
    # Attempt to set webhook (best effort on startup)
    # For Render, this might run on every deploy. It's idempotent.
    # Ensure SERVER_BASE_URL is correctly configured.
    try:
        asyncio.run(setup_telegram_webhook())
    except Exception as e:
        logger.error(f"Failed to run webhook setup during startup: {e}")

    # Uncomment to insert/update dummy data (useful for initial setup)
    # Be careful with this in production if you don't want data overwritten or duplicated.
    # insert_dummy_data() 
    
    # Gunicorn will be used by Render.com as specified in Procfile or start command.
    # For local Flask development:
    is_local_dev = os.getenv("FLASK_ENV") == "development" or os.getenv("FLASK_DEBUG") == "1"
    if is_local_dev: # Check if running locally (e.g. FLASK_ENV=development)
        logger.info("Running Flask app in development mode with polling (webhook not typically used locally without ngrok).")
        # To test webhook locally, you would use ngrok and manually set webhook to ngrok URL.
        # The `setup_telegram_webhook()` above would try to set it to SERVER_BASE_URL (your Render URL).
        # For local dev without ngrok, you'd typically comment out webhook setup and use polling:
        # Example for local polling (not recommended for production):
        # loop = asyncio.get_event_loop()
        # loop.create_task(bot_application.updater.start_polling()) # This would run the bot polling
        # app.run(...)
        app.run(debug=True, host='0.0.0.0', port=int(os.getenv("PORT", 8080)))
    # else, Gunicorn will run `app` (the Flask instance)
