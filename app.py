import os
import hashlib
import hmac
import json
import logging
from urllib.parse import unquote_plus, parse_qs
import uuid

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
            gift_instance_id UUID NOT NULL, -- Not FK to allow keeping logs even if gift is deleted (though user_gifts doesn't have ON DELETE SET NULL for instance_id)
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
                return json.loads(unquote_plus(user_data_json))
        return None
    except Exception as e:
        logger.error(f"Error verifying Telegram data: {e}")
        return None

def get_current_user_from_request():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('tma '):
        return None
    
    init_data_str = auth_header.split(' ', 1)[1]
    user_data = verify_telegram_data(init_data_str, TELEGRAM_BOT_TOKEN)
    return user_data

# --- TELEGRAM BOT SETUP ---
async def start(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    # Upsert user into DB
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (telegram_id, username, first_name, last_name, photo_url, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            photo_url = EXCLUDED.photo_url,
            last_seen_at = CURRENT_TIMESTAMP;
    """, (user.id, user.username, user.first_name, user.last_name, None)) # Photo URL not directly available here
    conn.commit()
    cur.close()
    conn.close()

    keyboard = [[InlineKeyboardButton("🎁 Open Gift App", web_app={'url': TELEGRAM_WEBAPP_URL})]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! Welcome to the Gift Simulator!",
        reply_markup=reply_markup,
    )

async def send_transfer_notification(bot, to_telegram_id, gift_name, gift_link_part, sender_name):
    deep_link_url = f"https://t.me/{TELEGRAM_BOT_USERNAME}/{TELEGRAM_WEBAPP_URL.split('/')[-1]}?startapp={gift_link_part}"
    message = (
        f"🎉 You've received a gift!\n\n"
        f"🎁 **{gift_name}**\n"
        f"👤 From: {sender_name}\n\n"
        f"Tap the button below to view your new gift."
    )
    keyboard = [[InlineKeyboardButton("View Gift", web_app={'url': deep_link_url})]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await bot.send_message(chat_id=to_telegram_id, text=message, reply_markup=reply_markup, parse_mode="Markdown")
        logger.info(f"Sent transfer notification to {to_telegram_id} for {gift_name}")
    except Exception as e:
        logger.error(f"Failed to send transfer notification to {to_telegram_id}: {e}")


bot_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
bot_app.add_handler(CommandHandler("start", start))

async def main_bot_loop():
    logger.info("Starting bot polling (though webhook is preferred for production)...")
    # For Render, you'd typically use webhooks. This is more for local dev or simple setups.
    # await bot_app.initialize()
    # await bot_app.start()
    # await bot_app.updater.start_polling()
    # For webhook:
    await bot_app.bot.set_webhook(url=f"{SERVER_BASE_URL}/telegram_webhook")
    logger.info(f"Webhook set to {SERVER_BASE_URL}/telegram_webhook")


# --- API ENDPOINTS ---
@app.before_request
def log_user_activity():
    user_data = get_current_user_from_request()
    if user_data and 'id' in user_data:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (telegram_id, username, first_name, last_name, photo_url, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                photo_url = EXCLUDED.photo_url, -- Assuming photo_url is in user_data from TMA
                last_seen_at = CURRENT_TIMESTAMP
            RETURNING telegram_id;
        """, (
            user_data['id'],
            user_data.get('username'),
            user_data.get('first_name'),
            user_data.get('last_name'),
            user_data.get('photo_url')
        ))
        conn.commit()
        cur.close()
        conn.close()

@app.route('/api/me/profile', methods=['GET'])
def get_my_profile():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Fetch user details
    cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (user_data['id'],))
    profile_info = cur.fetchone()
    if not profile_info: # Should not happen if before_request works
        return jsonify({"error": "User not found"}), 404

    # Fetch user gifts
    cur.execute("""
        SELECT 
            ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
            ug.is_collectible, ug.collectible_number, ug.is_hidden,
            gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
            model.name as model_name, model.image_url as model_image_url,
            pattern.name as pattern_name, pattern.image_url as pattern_image_url,
            backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color,
            EXISTS (SELECT 1 FROM pinned_gifts pg WHERE pg.gift_instance_id = ug.instance_id AND pg.owner_telegram_id = ug.owner_telegram_id) as is_pinned_on_profile
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
    pinned_gift_ids = [row['gift_instance_id'] for row in pinned_gift_rows]

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
        return jsonify({"error": "User not found by identifier"}), 404

    cur.execute("SELECT telegram_id, username, first_name, last_name, photo_url FROM users WHERE telegram_id = %s", (target_user_id,))
    profile_info = cur.fetchone()
    if not profile_info:
        return jsonify({"error": "User profile data not found"}), 404

    # Fetch public gifts (not hidden)
    cur.execute("""
        SELECT 
            ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
            ug.is_collectible, ug.collectible_number,
            gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
            model.name as model_name, model.image_url as model_image_url,
            pattern.name as pattern_name, pattern.image_url as pattern_image_url,
            backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color,
            EXISTS (SELECT 1 FROM pinned_gifts pg WHERE pg.gift_instance_id = ug.instance_id AND pg.owner_telegram_id = ug.owner_telegram_id) as is_pinned_on_profile
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
    pinned_gift_ids = [row['gift_instance_id'] for row in pinned_gift_rows]
    
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
            model.name as model_name, model.image_url as model_image_url,
            pattern.name as pattern_name, pattern.image_url as pattern_image_url,
            backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color,
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

    # For a public link, even if hidden, we show it. The profile view respects is_hidden.
    # If gift is hidden, owner field data could be limited or button inactive on frontend.
    return jsonify(gift_data)

@app.route('/api/store/gifts', methods=['GET'])
def get_store_gifts():
    # This would list gift_definitions that are purchasable
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, name, slug, description, original_image_url FROM gift_definitions") # Add is_purchasable later
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
    
    # Check if gift definition exists
    cur.execute("SELECT id, name FROM gift_definitions WHERE id = %s", (gift_definition_id,))
    gift_def = cur.fetchone()
    if not gift_def:
        cur.close()
        conn.close()
        return jsonify({"error": "Gift definition not found"}), 404

    new_gifts_details = []
    try:
        for _ in range(quantity):
            instance_id = str(uuid.uuid4()) # Generate UUID in Python
            cur.execute("""
                INSERT INTO user_gifts (instance_id, owner_telegram_id, gift_definition_id, acquired_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING instance_id, acquired_at;
            """, (instance_id, user_data['id'], gift_definition_id))
            new_gift_info = cur.fetchone()
            new_gifts_details.append({
                "instance_id": new_gift_info['instance_id'],
                "gift_definition_id": gift_definition_id,
                "gift_name": gift_def['name'], # Add gift name for immediate display
                "original_image_url": gift_def.get('original_image_url'), # Add image for immediate display
                "acquired_at": new_gift_info['acquired_at'].isoformat(),
                "is_collectible": False,
                "collectible_number": None,
                "is_hidden": False,
                "is_pinned_on_profile": False # New gifts aren't pinned by default
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
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        # Verify ownership and get gift_definition_id
        cur.execute("""
            SELECT gift_definition_id, is_collectible 
            FROM user_gifts 
            WHERE instance_id = %s AND owner_telegram_id = %s
        """, (str(instance_id), user_data['id']))
        gift_to_upgrade = cur.fetchone()

        if not gift_to_upgrade:
            return jsonify({"error": "Gift not found or not owned by user"}), 404
        if gift_to_upgrade['is_collectible']:
            return jsonify({"error": "Gift is already collectible"}), 400

        gift_def_id = gift_to_upgrade['gift_definition_id']

        # Transaction for assigning collectible number
        cur.execute("BEGIN;")
        cur.execute("""
            SELECT current_highest_collectible_number 
            FROM gift_definitions 
            WHERE id = %s FOR UPDATE;
        """, (gift_def_id,))
        def_row = cur.fetchone()
        new_collectible_num = def_row['current_highest_collectible_number'] + 1
        
        cur.execute("""
            UPDATE gift_definitions 
            SET current_highest_collectible_number = %s 
            WHERE id = %s;
        """, (new_collectible_num, gift_def_id))

        # TODO: Implement actual weighted random selection of components
        # For now, let's assume we pick the first available of each type for simplicity
        # This needs to be replaced with proper logic based on `collectible_components` and rarity
        
        cur.execute("SELECT id FROM collectible_components WHERE gift_definition_id = %s AND component_type = 'MODEL' ORDER BY rarity_permille DESC, RANDOM() LIMIT 1", (gift_def_id,))
        model_comp = cur.fetchone()
        cur.execute("SELECT id FROM collectible_components WHERE gift_definition_id = %s AND component_type = 'BACKDROP' ORDER BY rarity_permille DESC, RANDOM() LIMIT 1", (gift_def_id,))
        backdrop_comp = cur.fetchone()
        cur.execute("SELECT id FROM collectible_components WHERE gift_definition_id = %s AND component_type = 'PATTERN' ORDER BY rarity_permille DESC, RANDOM() LIMIT 1", (gift_def_id,))
        pattern_comp = cur.fetchone()

        model_id = model_comp['id'] if model_comp else None
        backdrop_id = backdrop_comp['id'] if backdrop_comp else None
        pattern_id = pattern_comp['id'] if pattern_comp else None
        
        if not all([model_id, backdrop_id, pattern_id]):
             cur.execute("ROLLBACK;") # Important: Rollback if components are missing
             logger.error(f"Missing components for gift_definition_id {gift_def_id} during upgrade.")
             return jsonify({"error": "Failed to find all collectible components for this gift type"}), 500


        cur.execute("""
            UPDATE user_gifts 
            SET is_collectible = TRUE, 
                collectible_number = %s,
                collectible_model_id = %s,
                collectible_backdrop_id = %s,
                collectible_pattern_id = %s
            WHERE instance_id = %s
            RETURNING *; 
        """, (new_collectible_num, model_id, backdrop_id, pattern_id, str(instance_id)))
        
        upgraded_gift = cur.fetchone()
        cur.execute("COMMIT;")
        
        # Fetch full details for response
        cur.execute("""
            SELECT 
                ug.instance_id, ug.owner_telegram_id, ug.gift_definition_id, ug.acquired_at, 
                ug.is_collectible, ug.collectible_number, ug.is_hidden,
                gd.name as gift_name, gd.slug as gift_slug, gd.original_image_url as gift_original_image_url,
                model.name as model_name, model.image_url as model_image_url,
                pattern.name as pattern_name, pattern.image_url as pattern_image_url,
                backdrop.name as backdrop_name, backdrop.backdrop_center_color, backdrop.backdrop_edge_color
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            LEFT JOIN collectible_components model ON ug.collectible_model_id = model.id
            LEFT JOIN collectible_components pattern ON ug.collectible_pattern_id = pattern.id
            LEFT JOIN collectible_components backdrop ON ug.collectible_backdrop_id = backdrop.id
            WHERE ug.instance_id = %s;
        """, (str(instance_id),))
        full_upgraded_gift_details = cur.fetchone()

        return jsonify({"message": "Gift upgraded successfully", "gift": full_upgraded_gift_details}), 200

    except psycopg2.IntegrityError as e: # Catch unique constraint violation for collectible number (should be rare with FOR UPDATE)
        cur.execute("ROLLBACK;")
        logger.error(f"Integrity error during upgrade: {e}")
        return jsonify({"error": "Failed to upgrade gift due to a conflict. Please try again."}), 500
    except Exception as e:
        if 'cur' in locals() and cur and not cur.closed and conn and not conn.closed: # Check if transaction is active
            if conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
                 cur.execute("ROLLBACK;")
        logger.error(f"Error upgrading gift: {e}")
        return jsonify({"error": "Failed to upgrade gift"}), 500
    finally:
        if 'cur' in locals() and cur and not cur.closed: cur.close()
        if 'conn' in locals() and conn and not conn.closed: conn.close()


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
        return jsonify({"message": f"Gift {action} successfully", "instance_id": str(instance_id), "is_hidden": is_hidden}), 200
    else:
        conn.rollback()
        return jsonify({"error": "Gift not found or not owned by user"}), 404
    finally:
        cur.close()
        conn.close()

@app.route('/api/gifts/pins', methods=['POST'])
def update_pinned_gifts():
    user_data = get_current_user_from_request()
    if not user_data or 'id' not in user_data:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    pinned_instance_ids = data.get('pinned_ids') # Expecting a list of UUID strings in order
    if not isinstance(pinned_instance_ids, list) or len(pinned_instance_ids) > 6: # Max 6 pins
        return jsonify({"error": "Invalid 'pinned_ids' parameter"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        # Clear existing pins for the user
        cur.execute("DELETE FROM pinned_gifts WHERE owner_telegram_id = %s", (user_data['id'],))
        
        # Add new pins with order
        for i, instance_id_str in enumerate(pinned_instance_ids):
            try:
                # Verify user owns this gift_instance_id before pinning
                cur.execute("SELECT 1 FROM user_gifts WHERE instance_id = %s AND owner_telegram_id = %s", (instance_id_str, user_data['id']))
                if cur.fetchone():
                    cur.execute("""
                        INSERT INTO pinned_gifts (owner_telegram_id, gift_instance_id, pin_order)
                        VALUES (%s, %s, %s)
                    """, (user_data['id'], instance_id_str, i))
                else:
                    logger.warning(f"User {user_data['id']} attempted to pin unowned gift {instance_id_str}")
                    # Optionally, raise an error or just skip
            except Exception as e_inner: # Catch issues with UUID conversion or DB ops
                 logger.error(f"Error pinning gift {instance_id_str}: {e_inner}")
                 # Decide if one failure should rollback all, or just skip the bad one.
                 # For now, we continue and let valid ones be pinned.

        cur.execute("COMMIT;")
        return jsonify({"message": "Pinned gifts updated successfully", "pinned_ids": pinned_instance_ids}), 200
    except Exception as e:
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
    recipient_identifier = data.get('recipient_identifier') # Username (without @) or Telegram ID string
    
    if not recipient_identifier:
        return jsonify({"error": "Recipient identifier missing"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    recipient_telegram_id = None
    if recipient_identifier.isdigit():
        cur.execute("SELECT telegram_id, username, first_name FROM users WHERE telegram_id = %s", (int(recipient_identifier),))
        recipient_user = cur.fetchone()
        if recipient_user:
            recipient_telegram_id = recipient_user['telegram_id']
        else: # User exists in Telegram but not in our DB (hasn't started bot)
             cur.close(); conn.close()
             return jsonify({"error": "Recipient user must start the bot first to be registered."}), 404
    else: # assume username
        cur.execute("SELECT telegram_id, username, first_name FROM users WHERE username = %s", (recipient_identifier,))
        recipient_user = cur.fetchone()
        if recipient_user:
            recipient_telegram_id = recipient_user['telegram_id']
        else:
             cur.close(); conn.close()
             return jsonify({"error": "Recipient user with that username not found or hasn't started the bot."}), 404
             
    if not recipient_telegram_id: # Double check
        cur.close(); conn.close()
        return jsonify({"error": "Recipient user could not be resolved."}), 404
        
    if recipient_telegram_id == sender_user_data['id']:
        cur.close(); conn.close()
        return jsonify({"error": "Cannot transfer gift to yourself."}), 400

    try:
        cur.execute("BEGIN;")
        # Verify sender owns the gift and get gift details for notification
        cur.execute("""
            SELECT ug.gift_definition_id, gd.name as gift_name, gd.slug as gift_slug
            FROM user_gifts ug
            JOIN gift_definitions gd ON ug.gift_definition_id = gd.id
            WHERE ug.instance_id = %s AND ug.owner_telegram_id = %s FOR UPDATE; 
        """, (str(instance_id), sender_user_data['id'])) # Lock the gift row
        gift_to_transfer = cur.fetchone()

        if not gift_to_transfer:
            cur.execute("ROLLBACK;")
            return jsonify({"error": "Gift not found, not owned by sender, or already being transferred."}), 404
        
        # Update owner
        cur.execute("""
            UPDATE user_gifts 
            SET owner_telegram_id = %s, is_hidden = FALSE -- Transferred gifts are not hidden by default for new owner
            WHERE instance_id = %s;
        """, (recipient_telegram_id, str(instance_id)))
        
        # Remove from sender's pins if it was pinned
        cur.execute("""
            DELETE FROM pinned_gifts 
            WHERE owner_telegram_id = %s AND gift_instance_id = %s;
        """, (sender_user_data['id'], str(instance_id)))
        
        # Log transfer
        cur.execute("""
            INSERT INTO gift_transfers (gift_instance_id, from_telegram_id, to_telegram_id, transferred_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
        """, (str(instance_id), sender_user_data['id'], recipient_telegram_id))
        
        cur.execute("COMMIT;")

        # Send Telegram notification (outside DB transaction)
        gift_name_for_notif = gift_to_transfer['gift_name']
        gift_slug_for_notif = gift_to_transfer['gift_slug'] # Assuming collectible for now
        
        # Get collectible number if it exists for the link part
        cur.execute("SELECT collectible_number FROM user_gifts WHERE instance_id = %s", (str(instance_id),))
        updated_gift_for_link = cur.fetchone()
        gift_link_part = f"{gift_slug_for_notif}-{updated_gift_for_link['collectible_number']}" if updated_gift_for_link and updated_gift_for_link['collectible_number'] else gift_slug_for_notif

        sender_name_for_notif = sender_user_data.get('first_name', sender_user_data.get('username', 'A user'))
        
        await send_transfer_notification(
            bot_app.bot, 
            recipient_telegram_id, 
            gift_name_for_notif, 
            gift_link_part, 
            sender_name_for_notif
        )
        
        return jsonify({"message": f"Gift '{gift_name_for_notif}' transferred successfully to {recipient_user.get('username', recipient_telegram_id)}."}), 200

    except Exception as e:
        if conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
            cur.execute("ROLLBACK;")
        logger.error(f"Error transferring gift: {e}")
        return jsonify({"error": "Failed to transfer gift"}), 500
    finally:
        cur.close()
        conn.close()


# --- TELEGRAM WEBHOOK HANDLER ---
@app.route('/telegram_webhook', methods=['POST'])
async def telegram_webhook():
    update_data = request.get_json()
    logger.info(f"Webhook received: {json.dumps(update_data, indent=2)}")
    update = Update.de_json(update_data, bot_app.bot)
    await bot_app.process_update(update)
    return jsonify({"status": "ok"})


# --- DUMMY DATA INSERTION (FOR TESTING) ---
def insert_dummy_data():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Dummy User (replace with your actual test user ID after they /start the bot)
        test_user_id = 123456789 # Example ID
        cur.execute("""
            INSERT INTO users (telegram_id, username, first_name) VALUES (%s, %s, %s)
            ON CONFLICT (telegram_id) DO NOTHING;
        """, (test_user_id, 'testuser', 'Test'))

        # Dummy Gift Definitions
        cur.execute("""
            INSERT INTO gift_definitions (name, slug, original_image_url, cdn_path_prefix) VALUES 
            ('Plush Pepe', 'plush-pepe', 'https://cdn.example.com/pepe.png', 'pepe_collectibles/'),
            ('Diamond Cat', 'diamond-cat', 'https://cdn.example.com/cat.png', 'cat_collectibles/')
            ON CONFLICT (slug) DO NOTHING RETURNING id, slug;
        """)
        pepe_def = cur.fetchone()
        if pepe_def:
            pepe_def_id = pepe_def[0]
            # Dummy components for Pepe
            cur.execute("""INSERT INTO collectible_components 
                        (gift_definition_id, component_type, name, image_url, rarity_permille, backdrop_center_color, backdrop_edge_color) VALUES
                        (%s, 'MODEL', 'Cool Pepe', 'models/pepe_cool.png', 500, NULL, NULL),
                        (%s, 'MODEL', 'Sad Pepe', 'models/pepe_sad.png', 500, NULL, NULL),
                        (%s, 'BACKDROP', 'Galaxy', NULL, 300, '#100020', '#301050'),
                        (%s, 'BACKDROP', 'Forest', NULL, 700, '#205010', '#40A030'),
                        (%s, 'PATTERN', 'Sparkles', 'patterns/sparkles.png', 600, NULL, NULL),
                        (%s, 'PATTERN', 'Hearts', 'patterns/hearts.png', 400, NULL, NULL)
                        ON CONFLICT (gift_definition_id, component_type, name) DO NOTHING;
            """, (pepe_def_id, pepe_def_id, pepe_def_id, pepe_def_id, pepe_def_id, pepe_def_id))
        
        cat_def = cur.fetchone() # This will be None if Pepe was already inserted, fix this logic if running multiple times
        if not cat_def: # Try to fetch it if it was None from previous RETURNING
            cur.execute("SELECT id, slug FROM gift_definitions WHERE slug = 'diamond-cat'")
            cat_def = cur.fetchone()

        if cat_def:
            cat_def_id = cat_def[0]
            # Dummy components for Cat
            cur.execute("""INSERT INTO collectible_components 
                        (gift_definition_id, component_type, name, image_url, rarity_permille) VALUES
                        (%s, 'MODEL', 'Shiny Cat', 'models/cat_shiny.png', 1000, NULL, NULL)
                        ON CONFLICT (gift_definition_id, component_type, name) DO NOTHING;
            """, (cat_def_id,))


        # Dummy owned gift for test user (if they exist)
        cur.execute("SELECT telegram_id FROM users WHERE telegram_id = %s", (test_user_id,))
        if cur.fetchone() and pepe_def: # If test user and pepe_def exist
            cur.execute("""
                INSERT INTO user_gifts (owner_telegram_id, gift_definition_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (test_user_id, pepe_def_id))
            
        conn.commit()
        logger.info("Dummy data insertion attempt complete.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting dummy data: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    init_db()
    # insert_dummy_data() # Uncomment to insert dummy data on first run
    
    # For local development with polling (if not using ngrok for webhook testing)
    # import asyncio
    # asyncio.ensure_future(main_bot_loop()) # For webhook, this just sets it
    
    # For Render.com, gunicorn will run the Flask app. The webhook should be set once.
    # The `main_bot_loop` which calls `set_webhook` might be better run as a one-off script
    # or a startup command that doesn't block the web server.
    # For simplicity here, let's assume you'll manually set webhook or run a script for it.
    
    # If running locally and want to test webhook, you'd need ngrok or similar
    # And then you'd run `asyncio.run(bot_app.bot.set_webhook(url="YOUR_NGROK_URL/telegram_webhook"))`
    # and then `app.run(debug=True, host='0.0.0.0', port=int(os.getenv("PORT", 8080)))`
    
    # Production Gunicorn command would be like: gunicorn --bind 0.0.0.0:$PORT app:app
    # For local testing, simple Flask run:
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv("PORT", 8080)))
