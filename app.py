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
                bot_state VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
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
                channel_id BIGINT NOT NULL,
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

# Other helpers like get_bot_info, send_telegram_photo etc. remain the same

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
    state_name = "_".join(state_parts[:-1]) # e.g., 'awaiting_giveaway_channel'
    giveaway_id = int(state_parts[-1])

    if text.lower() == '/cancel':
        cur.execute("UPDATE accounts SET bot_state = NULL WHERE tg_id = %s;", (user_id,))
        cur.execute("DELETE FROM giveaways WHERE id = %s;", (giveaway_id,))
        conn.commit()
        send_telegram_message(user_id, "Giveaway setup cancelled.")
        return

    if state_name == 'awaiting_giveaway_channel':
        try:
            channel_id = int(text.strip())
            # Basic validation: channel IDs are large negative numbers
            if channel_id > 0:
                send_telegram_message(user_id, "Invalid Channel ID. Channel IDs are usually large negative numbers. Please try again.")
                return

            # Store the ID and a placeholder name
            cur.execute("UPDATE giveaways SET channel_id = %s, channel_username = %s WHERE id = %s;", (channel_id, f"Channel ID: {channel_id}", giveaway_id))
            new_state = f"awaiting_giveaway_end_date_{giveaway_id}"
            cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, user_id))
            conn.commit()

            send_telegram_message(user_id, "✅ Channel ID set!\n\n🏆 **Giveaway Setup: Step 2 of 2**\n\nNow, enter the giveaway end date and time in `DD.MM.YYYY HH:MM` format.\n\n*Example: `25.12.2025 18:00`*\n\n(All times are in MSK/GMT+3 timezone)")
        except ValueError:
            send_telegram_message(user_id, "Invalid format. Please provide the numerical Channel ID.")
            return

    elif state_name == 'awaiting_giveaway_end_date':
        try:
            end_date_naive = datetime.strptime(text.strip(), '%d.%m.%Y %H:%M')
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

                    cur.execute("SELECT gf.gift_name, gf.collectible_number, gf.gift_type_id FROM gifts gf JOIN giveaway_gifts gg ON gf.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s ORDER BY gf.acquired_date;", (giveaway_id,))
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
                        error_desc = post_result.get('description', 'unknown error') if post_result else 'network error'
                        send_telegram_message(user_id, f"❌ Failed to publish giveaway. The bot might not be in the channel, or the Channel ID is incorrect. Error: `{error_desc}`")

            elif "message" in update:
                message = update["message"]
                chat_id = message["chat"]["id"]
                text = message.get("text", "")

                cur.execute("SELECT bot_state FROM accounts WHERE tg_id = %s;", (chat_id,))
                user_row = cur.fetchone()
                user_state = user_row['bot_state'] if user_row else None
                
                if user_state and user_state.startswith('awaiting_giveaway'):
                    handle_giveaway_setup(conn, cur, chat_id, user_state, text)
                
                elif text.startswith("/start"):
                    if "giveaway" in text:
                        # Join giveaway logic (unchanged)
                    else:
                        # Default start message (unchanged)
    finally:
        if conn:
            conn.close()

    return jsonify({"status": "ok"}), 200

# --- API ENDPOINTS ---

@app.route('/api/giveaways/create', methods=['POST'])
def create_giveaway():
    data = request.get_json()
    creator_id = data.get('creator_id')
    gift_instance_ids = data.get('gift_instance_ids')
    winner_rule = data.get('winner_rule')

    if not all([creator_id, gift_instance_ids, winner_rule]):
        return jsonify({"error": "Missing required fields"}), 400
    
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

            # --- MODIFIED PROMPT ---
            send_telegram_message(
                creator_id,
                (
                    "🏆 **Giveaway Setup: Step 1 of 2**\n\n"
                    "You've selected your gifts. Now, please send the **ID** of the public or private channel where the giveaway will be hosted.\n\n"
                    "**How to get the Channel ID:**\n"
                    "1. Forward any message from your channel to a bot like `@userinfobot`.\n"
                    "2. It will reply with the channel's details, including the ID (it will be a negative number, like `-100123456789`).\n\n"
                    "⚠️ **Important:** You must manually add this bot (`@" + BOT_USERNAME + "`) to your channel as an administrator with 'Post Messages' permission *before* continuing.\n\n"
                    "To cancel, send /cancel."
                )
            )
            
            return jsonify({"message": "Giveaway initiated.", "giveaway_id": giveaway_id}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()


# ... ALL OTHER API ENDPOINTS AND THE GIVEAWAY WORKER ...
# (The rest of the file remains exactly as it was in the previous complete version.
# For the sake of brevity, I am not repeating the ~500 lines of code that are unchanged.)


# --- GIVEAWAY BACKGROUND WORKER ---

def process_giveaway_winners(giveaway_id):
    app.logger.info(f"Processing winners for giveaway ID: {giveaway_id}")
    conn = get_db_connection()
    if not conn: return

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT * FROM giveaways WHERE id = %s;", (giveaway_id,))
            giveaway = cur.fetchone()
            if not giveaway:
                app.logger.warning(f"Could not find giveaway {giveaway_id} to process.")
                return

            cur.execute("SELECT user_id FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
            participants = cur.fetchall()
            cur.execute("SELECT g.* FROM gifts g JOIN giveaway_gifts gg ON g.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s;", (giveaway_id,))
            gifts = cur.fetchall()

            if not participants:
                send_telegram_message(giveaway['creator_id'], f"😔 Your giveaway in channel ID {giveaway['channel_id']} has ended, but there were no participants. The gifts have been returned to your account.")
                cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
                conn.commit()
                return

            results_text = "🏆 **Giveaway Results** 🏆\n\nCongratulations to our winners:\n\n"
            if giveaway['winner_rule'] == 'single':
                winner_id = random.choice([p['user_id'] for p in participants])
                cur.execute("UPDATE gifts SET owner_id = %s WHERE instance_id IN (SELECT gift_instance_id FROM giveaway_gifts WHERE giveaway_id = %s);", (winner_id, giveaway_id))
                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                winner_username = cur.fetchone()['username']
                results_text += f"All prizes go to our lucky winner: @{winner_username}!\n"
                for gift in gifts:
                     deep_link = f"https://t.me/{BOT_USERNAME}/upgrade?startapp=gift{gift['gift_type_id']}-{gift['collectible_number']}"
                     send_telegram_message(winner_id, f"🎉 Congratulations! You won <a href='{deep_link}'>{gift['gift_name']}</a> in a giveaway!")
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
                    send_telegram_message(winner_id, f"🎉 Congratulations! You won <a href='{deep_link}'>{gift['gift_name']}</a> in a giveaway!")
            
            send_telegram_message(giveaway['channel_id'], results_text, disable_web_page_preview=True)
            cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
            conn.commit()
            app.logger.info(f"Successfully processed giveaway {giveaway_id}.")

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error processing giveaway {giveaway_id}: {e}", exc_info=True)
            if giveaway:
                send_telegram_message(giveaway['creator_id'], f"An error occurred while processing your giveaway for channel ID {giveaway.get('channel_id')}. Please contact support.")
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
    # To run this locally with ngrok, you'd set the WEBHOOK_URL to your ngrok URL
    # and then uncomment the next line:
    # set_webhook()
    app.run(debug=True, port=5001)