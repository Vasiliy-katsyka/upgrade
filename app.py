import os
import psycopg2
import json
import random
import requests
import threading
import pytz
import time
import base64
import uuid
import logging
import io
import re
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
TRANSFER_API_KEY = os.environ.get('TRANSFER_API_KEY')
WEBHOOK_URL = "https://upgrade-a57g.onrender.com"

if not DATABASE_URL or not TELEGRAM_BOT_TOKEN:
    raise ValueError("Missing required environment variables: DATABASE_URL and/or TELEGRAM_BOT_TOKEN")

GIFT_LIMIT_PER_USER = 5000
MAX_COLLECTIONS_PER_USER = 9
MAX_COLLECTIBLE_USERNAMES = 10
MIN_SALE_PRICE = 125
MAX_SALE_PRICE = 100000
CDN_BASE_URL = "https://cdn.changes.tg/gifts/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBAPP_URL = "https://vasiliy-katsyka.github.io/upgrade/"
WEBAPP_SHORT_NAME = "upgrade"
BOT_USERNAME = "upgradeDemoBot"
TEST_ACCOUNT_TG_ID = 9999999999
ADMIN_TG_ID = 5146625949
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
GIVEAWAY_UPDATE_THROTTLE_SECONDS = 30
REQUIRED_GIVEAWAY_CHANNEL = "@CompactTelegram" # The channel for the custom gift check

collectible_parts_cache = {}
CACHE_DURATION_SECONDS = 3600  # Cache for 1 hour

# --- INLINE BOT CACHE ---
inline_cache = {}


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
    },
    "Baggin' Cat": {
        "defaultImage": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/refs/heads/main/IMG_20250718_234950_164.png",
        "models": [
            {"name": "Redo", "rarityPermille": 1, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_154505502.png"},
            {"name": "Bored Ape", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153421320.png"},
            {"name": "Snoop Dogg", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153800346.png"},
            {"name": "Austronaut", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153211676.png"},
            {"name": "Chinese Dragon", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153332160.png"},
            {"name": "Radioactive", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_154437815.png"},
            {"name": "Pink Guard", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_154725761.png"},
            {"name": "Angel", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155859028.png"},
            {"name": "Devil", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155937967.png"},
            {"name": "Minion", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153059849.png"},
            {"name": "Rainbow", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153251813.png"},
            {"name": "Spookie", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153836181.png"},
            {"name": "Spider", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_154055429.png"},
            {"name": "Dying Light", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_154537813.png"},
            {"name": "Hippo", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155053345.png"},
            {"name": "Poo", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155200011.png"},
            {"name": "Pikachu", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155411045.png"},
            {"name": "XXXTentacion", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155652114.png"},
            {"name": "Electric", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155830968.png"},
            {"name": "Glassy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_160036747.png"},
            {"name": "Alien", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_160243304.png"},
            {"name": "Piggy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_160346910.png"},
            {"name": "Panda", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153136834.png"},
            {"name": "Capybara", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_153629360.png"},
            {"name": "Dolphin", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155125393.png"},
            {"name": "Rabbit", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_155341280.png"},
            {"name": "Elephant", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_160003197.png"},
            {"name": "Bee", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/BagginCat/main/BackgroundEraser_20250720_160317620.png"}
        ],
        "backdrops_source": "Toy Bear",
        "patterns_source": "Toy Bear"
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
        # --- FIX: Change phone_number to default NULL and add UNIQUE constraint correctly ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                tg_id BIGINT PRIMARY KEY,
                username VARCHAR(255) UNIQUE,
                full_name VARCHAR(255),
                avatar_url TEXT,
                bio TEXT,
                phone_number VARCHAR(50) UNIQUE,
                bot_state VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # --- FIX: One-time data migration from 'Not specified' to NULL ---
        cur.execute("""
            DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='accounts' AND column_name='phone_number') THEN
                    UPDATE accounts SET phone_number = NULL WHERE phone_number = 'Not specified';
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
            CREATE TABLE IF NOT EXISTS posts (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                views INT DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_owner_id ON posts (owner_id);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS post_reactions (
                id SERIAL PRIMARY KEY,
                post_id INT REFERENCES posts(id) ON DELETE CASCADE,
                user_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                reaction VARCHAR(10) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(post_id, user_id, reaction)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_post_reactions_post_id ON post_reactions (post_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_post_reactions_user_id ON post_reactions (user_id);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS wall_subscriptions (
                id SERIAL PRIMARY KEY,
                subscriber_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                target_user_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                on_mention BOOLEAN DEFAULT FALSE,
                on_new_post BOOLEAN DEFAULT FALSE,
                UNIQUE(subscriber_id, target_user_id)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wall_subscriptions_target_user ON wall_subscriptions (target_user_id);")


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
                required_channels TEXT,
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
                UNIQUE(giveaway_id, user_id)
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_with_custom_gifts_enabled (
                tg_id BIGINT PRIMARY KEY REFERENCES accounts(tg_id) ON DELETE CASCADE
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                display_order INT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner_id, name)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gift_collections (
                id SERIAL PRIMARY KEY,
                gift_instance_id VARCHAR(50) REFERENCES gifts(instance_id) ON DELETE CASCADE,
                collection_id INT REFERENCES collections(id) ON DELETE CASCADE,
                order_in_collection INT,
                UNIQUE(gift_instance_id, collection_id)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_collections_owner_id ON collections (owner_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gift_collections_collection_id ON gift_collections (collection_id);")

        cur.execute("SELECT 1 FROM accounts WHERE tg_id = %s;", (TEST_ACCOUNT_TG_ID,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tg_id) DO NOTHING;
            """, (TEST_ACCOUNT_TG_ID, 'system_test_account', 'Test Account', 'https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/main/DMJTGStarsEmoji_AgADUhMAAk9WoVI.png', 'This account holds sold gifts.'))

    conn.commit()
    conn.close()
    app.logger.info("Database initialized successfully.")

# --- CUSTOM GIFT HELPERS ---
def is_custom_gift(gift_name):
    return gift_name in CUSTOM_GIFTS_DATA

def has_custom_gifts_enabled(cur, tg_id):
    if not tg_id:
        return False
    try:
        tg_id = int(tg_id)
        cur.execute("SELECT 1 FROM users_with_custom_gifts_enabled WHERE tg_id = %s;", (tg_id,))
        return cur.fetchone() is not None
    except (ValueError, TypeError):
        return False

def get_gift_author(gift_name):
    if gift_name in ["Snoop Dogg", "Swag Bag", "Snoop Cigar", "Low Rider", "Westside Sign"]:
        return "snoopdogg"
    elif gift_name in ["Dildo", "Skebob", "Baggin' Cat"]:
        return "Vasiliy939"
    return None

# --- TELEGRAM BOT HELPERS ---

def get_chat_member(chat_id, user_id):
    url = f"{TELEGRAM_API_URL}/getChatMember"
    payload = {'chat_id': chat_id, 'user_id': user_id}
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to get chat member for user {user_id} in chat {chat_id}: {e}", exc_info=True)
        return None

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

def answer_inline_query(inline_query_id, results, cache_time=300):
    url = f"{TELEGRAM_API_URL}/answerInlineQuery"
    payload = {
        'inline_query_id': inline_query_id,
        'results': json.dumps(results),
        'cache_time': cache_time,
        'is_personal': True
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Failed to answer inline query {inline_query_id}: {e}")
        return None

def set_webhook():
    webhook_endpoint = f"{WEBHOOK_URL}/webhook"
    url = f"{TELEGRAM_API_URL}/setWebhook?url={webhook_endpoint}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        app.logger.info(f"Webhook set successfully to {webhook_endpoint}: {response.json()}")
    except requests.RequestException as e:
        app.logger.error(f"Failed to set webhook: {e}")

# --- NOTIFICATION HELPERS ---
def send_new_post_notifications(post_data, author_username):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT subscriber_id FROM wall_subscriptions
                WHERE target_user_id = %s AND on_new_post = TRUE
            """, (post_data['owner_id'],))
            subscribers = cur.fetchall()
            profile_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=user{author_username}"
            message = f"@{author_username} just created a new post! 🚀"
            reply_markup = {"inline_keyboard": [[{"text": "View Post", "url": profile_link}]]}
            for sub in subscribers:
                send_telegram_message(sub['subscriber_id'], message, reply_markup)
    except Exception as e:
        app.logger.error(f"Error sending new post notifications: {e}", exc_info=True)
    finally:
        if conn: conn.close()

def send_mention_notifications(post_data, author_username):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            mentioned_usernames = set(re.findall(r'@([a-zA-Z0-9_]{5,32})', post_data['content']))
            if not mentioned_usernames:
                return

            for username in mentioned_usernames:
                cur.execute("SELECT tg_id FROM accounts WHERE username = %s", (username,))
                mentioned_user = cur.fetchone()
                if not mentioned_user:
                    continue
                
                mentioned_user_id = mentioned_user['tg_id']
                # Check if the mentioned user wants notifications from ANYONE (for simplicity)
                # A more complex system could check for subscriptions to the author.
                cur.execute("""
                    SELECT 1 FROM wall_subscriptions
                    WHERE subscriber_id = %s AND target_user_id = %s AND on_mention = TRUE
                """, (mentioned_user_id, post_data['owner_id']))

                if cur.fetchone():
                    profile_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=user{author_username}"
                    message = f"@{author_username} mentioned you in a post! 💬"
                    reply_markup = {"inline_keyboard": [[{"text": "View Post", "url": profile_link}]]}
                    send_telegram_message(mentioned_user_id, message, reply_markup)
    except Exception as e:
        app.logger.error(f"Error sending mention notifications: {e}", exc_info=True)
    finally:
        if conn: conn.close()


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
    if gift_name in collectible_parts_cache:
        cached_data, timestamp = collectible_parts_cache[gift_name]
        if time.time() - timestamp < CACHE_DURATION_SECONDS:
            return cached_data

    gift_name_encoded = quote(gift_name)
    models_url, backdrops_url, patterns_url = None, None, None
    models_list = []

    if gift_name in CUSTOM_GIFTS_DATA:
        custom_data = CUSTOM_GIFTS_DATA[gift_name]
        models_list = custom_data.get("models", [])
        backdrops_source_encoded = quote(custom_data.get("backdrops_source", gift_name))
        patterns_source_encoded = quote(custom_data.get("patterns_source", gift_name))
        
        backdrops_url = f"{CDN_BASE_URL}backdrops/{backdrops_source_encoded}/backdrops.json"
        patterns_url = f"{CDN_BASE_URL}patterns/{patterns_source_encoded}/patterns.json"
    else:
        models_url = f"{CDN_BASE_URL}models/{gift_name_encoded}/models.json"
        backdrops_url = f"{CDN_BASE_URL}backdrops/{gift_name_encoded}/backdrops.json"
        patterns_url = f"{CDN_BASE_URL}patterns/{gift_name_encoded}/patterns.json"

    if models_url:
        try:
            response = requests.get(models_url, timeout=5)
            response.raise_for_status()
            models_list = response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            app.logger.warning(f"Could not fetch or decode models for {gift_name}: {e}")
            models_list = []

    try:
        response = requests.get(backdrops_url, timeout=5)
        response.raise_for_status()
        backdrops_list = response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        app.logger.warning(f"Could not fetch or decode backdrops for {gift_name}: {e}")
        backdrops_list = []

    try:
        response = requests.get(patterns_url, timeout=5)
        response.raise_for_status()
        patterns_list = response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        app.logger.warning(f"Could not fetch or decode patterns for {gift_name}: {e}")
        patterns_list = []
        
    all_parts = {"models": models_list, "backdrops": backdrops_list, "patterns": patterns_list}
    collectible_parts_cache[gift_name] = (all_parts, time.time())
    return all_parts
    
def normalize_and_build_clone_url(input_str):
    input_str = input_str.strip()
    if input_str.startswith(('http://', 'https://')):
        parsed_url = urlparse(input_str)
        if parsed_url.netloc in ['t.me', 'telegram.me'] and parsed_url.path.startswith('/nft/'):
            return input_str
        else:
            return None
    match = re.match(r'^([\w\s\']{3,20})\s*[#-]?\s*(\d+)$', input_str, re.UNICODE)
    if not match:
        match = re.match(r'^([a-zA-Z\']{3,20})-(\d+)$', input_str, re.UNICODE)
    if match:
        name_part = match.group(1).strip().replace(' ', '')
        number_part = match.group(2).strip()
        return f"https://t.me/nft/{name_part}-{number_part}"
    return None

# --- WEBHOOK & BOT LOGIC ---

def update_giveaway_message(giveaway_id):
    conn = get_db_connection()
    if not conn: return
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT g.*, a.username as creator_username FROM giveaways g JOIN accounts a ON g.creator_id = a.tg_id WHERE g.id = %s", (giveaway_id,))
        giveaway = cur.fetchone()
        if not giveaway or not giveaway['message_id']:
            conn.close()
            return

        cur.execute("""SELECT gf.gift_name, gf.collectible_number FROM gifts gf JOIN giveaway_gifts gg ON gf.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s ORDER BY gf.acquired_date;""", (giveaway_id,))
        gifts = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM giveaway_participants WHERE giveaway_id = %s;", (giveaway_id,))
        participant_count = cur.fetchone()[0]

        rewards = ""
        emojis = ["🥇", "🥈", "🥉"]
        for i, gift in enumerate(gifts):
            emoji = emojis[i] if i < len(emojis) else "🏅"
            rewards += f' {emoji} {gift["gift_name"]} #{gift["collectible_number"]:,}'

        end_date_str = giveaway['end_date'].astimezone(pytz.utc).strftime('%d.%m.%Y %H:%M UTC')
        required_channels_text = giveaway.get('required_channels')
        
        giveaway_text = "<b>Started Gifts Giveaway!</b>\n\n<b>Details:</b>\n"
        if required_channels_text:
            giveaway_text += f"• Subscribe: {required_channels_text}\n"
        giveaway_text += (
            f"• Deadline: {end_date_str}\n"
            f"• Rewards:{rewards}\n\n"
            f"Participants can now join this giveaway. Good luck 🎁"
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

    if state_name == 'awaiting_giveaway_channels':
        cur.execute("UPDATE giveaways SET required_channels = %s WHERE id = %s;", (text.strip(), giveaway_id))
        new_state = f"awaiting_giveaway_end_date_{giveaway_id}"
        cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, user_id))
        conn.commit()
        send_telegram_message(user_id, "✅ Channels to subscribe set!\n\n🏆 <b>Giveaway Setup: Step 3 of 3</b>\n\nNow, enter the giveaway end date and time in `DD.MM.YYYY HH:MM` format.\n\n<i>Example: `25.12.2025 18:00`</i>\n\n(All times are in UTC timezone)")

    elif state_name == 'awaiting_giveaway_channel': 
        try:
            channel_id = int(text.strip())
            if not (text.startswith('-100') and len(text) > 5):
                send_telegram_message(user_id, "That doesn't look like a valid public channel ID. It should start with `-100`.")
                return
            
            bot_member_info = get_chat_member(channel_id, int(TELEGRAM_BOT_TOKEN.split(':')[0]))
            if not bot_member_info or not bot_member_info.get('ok') or bot_member_info['result']['status'] not in ['administrator', 'creator']:
                 send_telegram_message(user_id, f"❌ Error: Please add @{BOT_USERNAME} as an administrator to the channel first.")
                 return

        except ValueError:
            send_telegram_message(user_id, "Invalid format. Please provide the numerical Channel ID.")
            return

        cur.execute("UPDATE giveaways SET channel_id = %s WHERE id = %s;", (channel_id, giveaway_id))
        new_state = f"awaiting_giveaway_channels_{giveaway_id}"
        cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, user_id))
        conn.commit()
        send_telegram_message(user_id, "✅ Posting channel ID set!\n\n🏆 <b>Giveaway Setup: Step 2 of 3</b>\n\nEnter the channel(s) users must subscribe to, separated by commas (e.g., `@channel1, @channel2`).\n\n<b>Important:</b> You must add this bot as an administrator to these channels for the check to work.")
        
    elif state_name == 'awaiting_giveaway_end_date':
        try:
            end_date_naive = datetime.strptime(text, '%d.%m.%Y %H:%M')
            end_date_aware = pytz.utc.localize(end_date_naive)
            if end_date_aware < datetime.now(pytz.utc):
                send_telegram_message(user_id, "The end date cannot be in the past. Please enter a future date.")
                return

            cur.execute("UPDATE giveaways SET end_date = %s WHERE id = %s;", (end_date_aware, giveaway_id))
            cur.execute("UPDATE accounts SET bot_state = NULL WHERE tg_id = %s;", (user_id,))
            conn.commit()
            reply_markup = {"inline_keyboard": [[{"text": "🚀 Publish Giveaway", "callback_data": f"publish_giveaway_{giveaway_id}"}]]}
            send_telegram_message(user_id, "✅ End date set!\n\nEverything is ready. Press the button below to publish your giveaway.", reply_markup=reply_markup)
        except ValueError:
            send_telegram_message(user_id, "Invalid date format. Please use `DD.MM.YYYY HH:MM` (UTC).")

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    update = request.get_json()
    conn = get_db_connection()
    if not conn: return jsonify({"status": "error", "message": "db connection failed"}), 500

    try:
        if "inline_query" in update:
            handle_inline_query(update["inline_query"])
            return jsonify({"status": "ok"}), 200

        if "chosen_inline_result" in update:
            handle_chosen_inline_result(update["chosen_inline_result"])
            return jsonify({"status": "ok"}), 200

        with conn.cursor(cursor_factory=DictCursor) as cur:
            if "callback_query" in update:
                callback_query = update["callback_query"]
                user_id = callback_query["from"]["id"]
                data = callback_query.get("data")

                if data and data.startswith("publish_giveaway_"):
                    giveaway_id = int(data.split('_')[2])
                    answer_callback_query(callback_query['id'], text="Publishing...")
                    cur.execute("SELECT * FROM giveaways WHERE id = %s AND status = 'pending_setup'", (giveaway_id,))
                    giveaway = cur.fetchone()

                    if not giveaway:
                        send_telegram_message(user_id, "This giveaway has already been published or does not exist.")
                        return jsonify({"status": "ok"}), 200

                    post_result = send_telegram_message(giveaway['channel_id'], "Preparing giveaway...")

                    if post_result and post_result.get('ok'):
                        message_id = post_result['result']['message_id']
                        cur.execute("UPDATE giveaways SET status = 'active', message_id = %s, last_update_time = CURRENT_TIMESTAMP WHERE id = %s;", (message_id, giveaway_id))
                        conn.commit()
                        update_giveaway_message(giveaway_id)
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
                            cur.execute("SELECT id, last_update_time, required_channels FROM giveaways WHERE id = %s AND status = 'active'", (giveaway_id,))
                            giveaway = cur.fetchone()
                            if not giveaway:
                                send_telegram_message(chat_id, "This giveaway is no longer active or does not exist.")
                            else:
                                unsubscribed_channels = []
                                if giveaway['required_channels']:
                                    channels_to_check = [c.strip() for c in giveaway['required_channels'].split(',')]
                                    for channel_username in channels_to_check:
                                        member_info = get_chat_member(channel_username, chat_id)
                                        if not member_info or not member_info.get('ok') or member_info['result']['status'] in ['left', 'kicked']:
                                            unsubscribed_channels.append(channel_username)
                                
                                if unsubscribed_channels:
                                    channels_str = ", ".join(unsubscribed_channels)
                                    send_telegram_message(chat_id, f"To participate, you must first subscribe to: {channels_str}\nPlease subscribe and try again.")
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

# --- INLINE BOT HANDLERS ---
def handle_inline_query(inline_query):
    query_id = inline_query['id']
    from_user = inline_query['from']
    query_str = inline_query['query'].strip()

    parts = query_str.split(' ', 2)
    command = parts[0].lower() if parts else ""
    
    results = []
    
    if command == "send" and len(parts) == 3:
        results = handle_inline_send(from_user, parts[1], parts[2])
    elif command == "createandsend" and len(parts) == 3:
        results = handle_inline_create_and_send(from_user, parts[1], parts[2])
    elif command == "image" and len(parts) == 2:
        results = handle_inline_image(from_user, parts[1])
    elif command == "createimage" and len(parts) == 2:
        results = handle_inline_create_image(from_user, parts[1])
    else:
        results = [
            {"type": "article", "id": "help_send", "title": "Send a gift", "description": "e.g., send durov PlushPepe-1", "input_message_content": {"message_text": "Usage: @upgradeDemoBot send <recipient> <GiftName-Number>"}},
            {"type": "article", "id": "help_create_send", "title": "Create and send a gift", "description": "e.g., createAndSend durov Dildo,She Wants,Cosmic,Common", "input_message_content": {"message_text": "Usage: @upgradeDemoBot createAndSend <recipient> <Name,Model,Backdrop,Pattern>"}},
            {"type": "article", "id": "help_image", "title": "Get gift image", "description": "e.g., image PlushPepe-1", "input_message_content": {"message_text": "Usage: @upgradeDemoBot image <GiftName-Number>"}},
            {"type": "article", "id": "help_create_image", "title": "Create gift image", "description": "e.g., createImage Dildo,She Wants,Cosmic,Common", "input_message_content": {"message_text": "Usage: @upgradeDemoBot createImage <Name,Model,Backdrop,Pattern>"}}
        ]
        
    answer_inline_query(query_id, results, cache_time=10)

def handle_chosen_inline_result(chosen_result):
    result_id = chosen_result['result_id']
    from_user = chosen_result['from']
    
    action_details = inline_cache.pop(result_id, None)
    if not action_details:
        return

    if from_user['id'] != action_details['sender_id']:
        return

    if action_details['action'] == 'send':
        _execute_gift_transfer(
            sender_id=action_details['sender_id'], sender_username=action_details['sender_username'],
            receiver_id=action_details['receiver_id'], receiver_username=action_details['recipient_username'],
            instance_id=action_details['instance_id'], gift_name=action_details['gift_name'],
            gift_number=action_details['gift_number'], gift_type_id=action_details['gift_type_id'],
            comment="Sent via inline command."
        )
    elif action_details['action'] == 'create_and_send':
        _execute_create_and_send(
            sender_id=action_details['sender_id'], sender_username=action_details['sender_username'],
            receiver_id=action_details['receiver_id'], receiver_username=action_details['recipient_username'],
            gift_name=action_details['gift_name'], model_name=action_details['model_name'],
            backdrop_name=action_details['backdrop_name'], pattern_name=action_details['pattern_name'],
            comment="Created and sent via inline command."
        )

def handle_inline_send(from_user, recipient_username, gift_str):
    conn = get_db_connection()
    if not conn: return []
    
    results = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            sender_id = from_user['id']
            cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (sender_id,))
            sender = cur.fetchone()
            if not sender: return []

            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (recipient_username,))
            recipient = cur.fetchone()
            if not recipient: return [{"type": "article", "id": "error_recipient", "title": f"Error: User @{recipient_username} not found", "input_message_content": {"message_text": f"Could not find user @{recipient_username}."}}]
            recipient_id = recipient['tg_id']

            match = re.match(r'^(.+?)-(\d+)$', gift_str)
            if not match: return [{"type": "article", "id": "error_gift_format", "title": "Error: Invalid gift format", "description": "Use format like: PlushPepe-1", "input_message_content": {"message_text": "Invalid gift format."}}]
            
            gift_name, collectible_number = match.group(1).strip(), int(match.group(2))

            cur.execute("SELECT instance_id, gift_type_id, collectible_data FROM gifts WHERE owner_id = %s AND gift_name = %s AND collectible_number = %s AND is_collectible = TRUE;", (sender_id, gift_name, collectible_number))
            gift = cur.fetchone()

            if not gift: return [{"type": "article", "id": "error_gift_not_found", "title": "Error: Gift not found in your collection", "description": f"You do not own {gift_name} #{collectible_number}", "input_message_content": {"message_text": "Could not find this gift in your collection."}}]

            result_id = str(uuid.uuid4())
            inline_cache[result_id] = {"action": "send", "sender_id": sender_id, "sender_username": sender['username'], "receiver_id": recipient_id, "recipient_username": recipient_username, "instance_id": gift['instance_id'], "gift_name": gift_name, "gift_number": collectible_number, "gift_type_id": gift['gift_type_id']}
            
            cd = gift.get('collectible_data', {})
            thumb_url = cd.get('modelImage') if isinstance(cd, dict) else ''

            results.append({"type": "article", "id": result_id, "title": f"Send {gift_name} #{collectible_number} to @{recipient_username}", "description": "Click here to confirm and send the gift.", "thumb_url": thumb_url, "input_message_content": {"message_text": f"Preparing to send {gift_name} #{collectible_number} to @{recipient_username}..."}})
    except Exception as e:
        app.logger.error(f"Error in handle_inline_send: {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return results

def handle_inline_create_and_send(from_user, recipient_username, gift_components_str):
    parts = [p.strip() for p in gift_components_str.split(',', 3)]
    if len(parts) < 3: return [{"type": "article", "id": "error_create_format", "title": "Error: Invalid format", "description": "Use: Name,Model,Backdrop,Pattern", "input_message_content": {"message_text": "Invalid format."}}]

    gift_name, model_name, backdrop_name = parts[0], parts[1], parts[2]
    pattern_name = parts[3] if len(parts) > 3 and parts[3] else None
    
    conn = get_db_connection()
    if not conn: return []
    
    results = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            sender_id = from_user['id']
            cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (sender_id,))
            sender = cur.fetchone()
            if not sender: return []
            
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (recipient_username,))
            recipient = cur.fetchone()
            if not recipient: return []
            recipient_id = recipient['tg_id']

            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, sender_id): return [{"type": "article", "id": "error_custom_disabled", "title": "Error: Custom Gifts are disabled", "input_message_content": {"message_text": "You must enable Custom Gifts in settings."}}]
            
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (recipient_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return [{"type": "article", "id": "error_limit_reached", "title": f"Error: @{recipient_username}'s gift box is full", "input_message_content": {"message_text": f"Recipient's inventory is full."}}]
            
            result_id = str(uuid.uuid4())
            inline_cache[result_id] = {"action": "create_and_send", "sender_id": sender_id, "sender_username": sender['username'], "recipient_id": recipient_id, "recipient_username": recipient_username, "gift_name": gift_name, "model_name": model_name, "backdrop_name": backdrop_name, "pattern_name": pattern_name}

            results.append({"type": "article", "id": result_id, "title": f"Create & Send {gift_name} to @{recipient_username}", "description": f"Model: {model_name}, Backdrop: {backdrop_name}, Pattern: {pattern_name or 'Random'}", "input_message_content": {"message_text": f"Preparing to create and send a custom {gift_name} to @{recipient_username}..."}})
    except Exception as e:
        app.logger.error(f"Error in handle_inline_create_and_send: {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return results

def handle_inline_image(from_user, gift_str):
    conn = get_db_connection()
    if not conn: return []
    
    results = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            match = re.match(r'^(.+?)-(\d+)$', gift_str)
            if not match: return []
            
            gift_name, collectible_number = match.group(1).strip(), int(match.group(2))
            
            cur.execute("SELECT g.collectible_data, a.username as owner_username FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE g.gift_name = %s AND g.collectible_number = %s AND g.is_collectible = TRUE;", (gift_name, collectible_number))
            gift = cur.fetchone()

            if not gift or not isinstance(gift.get('collectible_data'), dict): return []
            
            cd = gift['collectible_data']
            model_img = cd.get('modelImage')
            if not model_img: return []
                
            caption = (f"<b>{gift_name} #{collectible_number}</b>\n\n"
                       f"<b>Model:</b> {cd.get('model', {}).get('name', 'N/A')}\n"
                       f"<b>Backdrop:</b> {cd.get('backdrop', {}).get('name', 'N/A')}\n"
                       f"<b>Symbol:</b> {cd.get('pattern', {}).get('name', 'N/A')}\n"
                       f"<b>Owner:</b> @{gift['owner_username']}")

            results.append({"type": "photo", "id": str(uuid.uuid4()), "photo_url": model_img, "thumb_url": model_img, "caption": caption, "parse_mode": "HTML"})
    except Exception as e:
        app.logger.error(f"Error in handle_inline_image: {e}", exc_info=True)
    finally:
        if conn: conn.close()
    return results

def handle_inline_create_image(from_user, gift_components_str):
    parts = [p.strip() for p in gift_components_str.split(',', 3)]
    if len(parts) < 3: return []
    
    gift_name, model_name, backdrop_name = parts[0], parts[1], parts[2]
    pattern_name = parts[3] if len(parts) > 3 and parts[3] else "Random"
    
    try:
        all_parts_data = fetch_collectible_parts(gift_name)
        selected_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
        if not selected_model: return []

        model_img = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"

        caption = (f"<b>Custom Gift Preview: {gift_name}</b>\n\n"
                   f"<b>Model:</b> {model_name}\n"
                   f"<b>Backdrop:</b> {backdrop_name}\n"
                   f"<b>Symbol:</b> {pattern_name}")
        
        return [{"type": "photo", "id": str(uuid.uuid4()), "photo_url": model_img, "thumb_url": model_img, "caption": caption, "parse_mode": "HTML"}]
    except Exception as e:
        app.logger.error(f"Error in handle_inline_create_image: {e}", exc_info=True)
        return []

def _execute_gift_transfer(sender_id, sender_username, receiver_id, receiver_username, instance_id, gift_name, gift_number, gift_type_id, comment):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM gifts WHERE instance_id = %s AND owner_id = %s;", (instance_id, sender_id))
            if not cur.fetchone():
                send_telegram_message(sender_id, "Transfer failed: You no longer own this gift.")
                return

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                send_telegram_message(sender_id, f"Transfer failed: Receiver @{receiver_username}'s gift box is full.")
                return

            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))
            cur.execute("UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL, acquired_date = CURRENT_TIMESTAMP WHERE instance_id = %s;", (receiver_id, instance_id))
            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{gift_number}"
            link_text = f"<b>{gift_name} #{gift_number:,}</b>"
            
            sender_text = f'You successfully sent {link_text} to @{receiver_username}.'
            send_telegram_message(sender_id, sender_text)
            
            receiver_text = f'You have received {link_text} from @{sender_username}!'
            if comment: receiver_text += f'\n\n<i>{comment}</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check Out Gift", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)
    except Exception as e:
        app.logger.error(f"Error in _execute_gift_transfer: {e}", exc_info=True)
        send_telegram_message(sender_id, "An unexpected error occurred during the transfer.")
    finally:
        if conn: conn.close()

def _execute_create_and_send(sender_id, sender_username, receiver_id, receiver_username, gift_name, model_name, backdrop_name, pattern_name, comment):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, sender_id):
                send_telegram_message(sender_id, "Action failed: You have disabled Custom Gifts.")
                return
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                send_telegram_message(sender_id, f"Action failed: Receiver @{receiver_username}'s gift box is now full.")
                return

            all_parts_data = fetch_collectible_parts(gift_name)
            selected_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
            selected_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
            selected_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None) if pattern_name else select_weighted_random(all_parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                send_telegram_message(sender_id, f"Could not create gift. Invalid components specified.")
                return

            gift_type_id = CUSTOM_GIFTS_DATA.get(gift_name, {}).get('id', 'generated_gift')
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            new_instance_id = str(uuid.uuid4())
            
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"
            
            collectible_data = {"model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern, "modelImage": model_image_url, "lottieModelPath": lottie_model_path, "patternImage": pattern_image_url, "backdropColors": selected_backdrop.get('hex'), "supply": random.randint(2000, 10000), "author": get_gift_author(gift_name)}
            
            cur.execute("INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, is_collectible, collectible_data, collectible_number) VALUES (%s, %s, %s, %s, TRUE, %s, %s);", (new_instance_id, receiver_id, gift_type_id, gift_name, json.dumps(collectible_data), next_number))
            conn.commit()
            
            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{next_number}"
            link_text = f"<b>{gift_name} #{next_number:,}</b>"
            
            sender_text = f'You successfully created and sent {link_text} to @{receiver_username}.'
            send_telegram_message(sender_id, sender_text)
            
            receiver_text = f'You have received a new gift, {link_text}, from @{sender_username}!'
            if comment: receiver_text += f'\n\n<i>{comment}</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check Out Gift", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)
    except Exception as e:
        app.logger.error(f"Error in _execute_create_and_send: {e}", exc_info=True)
        send_telegram_message(sender_id, "An unexpected error occurred while creating the gift.")
    finally:
        if conn: conn.close()

# --- API ENDPOINTS ---

@app.route('/api/customization/check_access', methods=['GET'])
def check_customization_access():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400

    member_info = get_chat_member(REQUIRED_GIVEAWAY_CHANNEL, user_id)
    
    if member_info and member_info.get('ok'):
        status = member_info['result']['status']
        if status in ['creator', 'administrator', 'member']:
            return jsonify({"access": True}), 200

    return jsonify({"access": False}), 200

@app.route('/api/profile/<string:username>', methods=['GET'])
def get_user_profile(username):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            viewer_id = request.args.get('viewer_id')
            viewer_can_see_custom = has_custom_gifts_enabled(cur, viewer_id)

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
            
            if not viewer_can_see_custom:
                gifts = [g for g in gifts if not is_custom_gift(g['gift_name'])]

            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str):
                    gift['collectible_data'] = json.loads(gift['collectible_data'])
            profile_data['owned_gifts'] = gifts

            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (user_id,))
            profile_data['collectible_usernames'] = [row['username'] for row in cur.fetchall()]
            
            cur.execute("SELECT id, name FROM collections WHERE owner_id = %s ORDER BY display_order ASC, name ASC;", (user_id,))
            collections_raw = cur.fetchall()
            collections_with_order = []
            for coll in collections_raw:
                cur.execute("SELECT gift_instance_id FROM gift_collections WHERE collection_id = %s ORDER BY order_in_collection ASC;", (coll['id'],))
                ordered_ids = [row['gift_instance_id'] for row in cur.fetchall()]
                collections_with_order.append({ "id": coll['id'], "name": coll['name'], "ordered_instance_ids": ordered_ids })
            profile_data['collections'] = collections_with_order
            
            cur.execute("""
                SELECT 
                    p.id, p.content, p.views, p.created_at,
                    (SELECT jsonb_object_agg(pr.reaction, pr.count)
                     FROM (SELECT reaction, count(*) FROM post_reactions WHERE post_id = p.id GROUP BY reaction) as pr) as reactions
                FROM posts p
                WHERE p.owner_id = %s
                ORDER BY p.created_at DESC;
            """, (user_id,))
            profile_data['posts'] = [dict(row) for row in cur.fetchall()]

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
                cur.execute("""INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio, phone_number) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tg_id) DO NOTHING;""", (tg_id, data.get('username'), data.get('full_name'), data.get('avatar_url'), 'My first account!', None))
                conn.commit()
            
            cur.execute("""
                SELECT a.*, (ucge.tg_id IS NOT NULL) as custom_gifts_enabled
                FROM accounts a
                LEFT JOIN users_with_custom_gifts_enabled ucge ON a.tg_id = ucge.tg_id
                WHERE a.tg_id = %s;
            """, (tg_id,))
            account_data = dict(cur.fetchone())

            cur.execute("""
                SELECT * FROM gifts WHERE owner_id = %s
                ORDER BY is_pinned DESC, pin_order ASC NULLS LAST, acquired_date DESC;
            """, (tg_id,))
            gifts = [dict(row) for row in cur.fetchall()]

            if not account_data.get('custom_gifts_enabled'):
                gifts = [g for g in gifts if not is_custom_gift(g['gift_name'])]
            
            for gift in gifts:
                if gift.get('collectible_data') and isinstance(gift.get('collectible_data'), str):
                    gift['collectible_data'] = json.loads(gift['collectible_data'])
            account_data['owned_gifts'] = gifts

            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
            account_data['collectible_usernames'] = [row['username'] for row in cur.fetchall()]

            cur.execute("SELECT id, name FROM collections WHERE owner_id = %s ORDER BY display_order ASC, name ASC;", (tg_id,))
            collections_raw = cur.fetchall()
            collections_with_order = []
            for coll in collections_raw:
                cur.execute("SELECT gift_instance_id FROM gift_collections WHERE collection_id = %s ORDER BY order_in_collection ASC;", (coll['id'],))
                ordered_ids = [row['gift_instance_id'] for row in cur.fetchall()]
                collections_with_order.append({ "id": coll['id'], "name": coll['name'], "ordered_instance_ids": ordered_ids })
            account_data['collections'] = collections_with_order
            
            cur.execute("""
                SELECT 
                    p.id, p.content, p.views, p.created_at,
                    (SELECT jsonb_object_agg(pr.reaction, pr.count)
                     FROM (SELECT reaction, count(*) FROM post_reactions WHERE post_id = p.id GROUP BY reaction) as pr) as reactions
                FROM posts p
                WHERE p.owner_id = %s
                ORDER BY p.created_at DESC;
            """, (tg_id,))
            account_data['posts'] = [dict(row) for row in cur.fetchall()]

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
        # --- FIX: Handle setting phone number to NULL vs a value
        if 'phone_number' in data:
            phone_val = data['phone_number'] if data['phone_number'] != 'Not specified' else None
            update_fields.append("phone_number = %s")
            update_values.append(phone_val)
        
        if not update_fields: conn.close(); return jsonify({"error": "No fields for update"}), 400
        
        update_query = f"UPDATE accounts SET {', '.join(update_fields)} WHERE tg_id = %s;"
        update_values.append(tg_id)
        
        try:
            cur.execute(update_query, tuple(update_values))
            if cur.rowcount == 0: conn.close(); return jsonify({"error": "Account not found"}), 404
            conn.commit()
            return jsonify({"message": "Account updated"}), 200
        except psycopg2.IntegrityError as e:
            conn.rollback()
            if 'accounts_phone_number_key' in str(e):
                return jsonify({"error": "This phone number is already taken."}), 409
            if 'accounts_username_key' in str(e):
                return jsonify({"error": "This username is already taken."}), 409
            app.logger.error(f"Integrity error updating account {tg_id}: {e}", exc_info=True)
            return jsonify({"error": "A unique constraint was violated."}), 409
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error updating account {tg_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/account/settings', methods=['POST'])
def update_account_settings():
    data = request.get_json()
    tg_id = data.get('tg_id')
    custom_gifts_enabled = data.get('custom_gifts_enabled')

    if tg_id is None or not isinstance(custom_gifts_enabled, bool):
        return jsonify({"error": "tg_id and a boolean custom_gifts_enabled are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    with conn.cursor() as cur:
        try:
            if custom_gifts_enabled:
                cur.execute("INSERT INTO users_with_custom_gifts_enabled (tg_id) VALUES (%s) ON CONFLICT (tg_id) DO NOTHING;", (tg_id,))
            else:
                cur.execute("DELETE FROM users_with_custom_gifts_enabled WHERE tg_id = %s;", (tg_id,))
            conn.commit()
            return jsonify({"message": "Settings updated successfully"}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error updating settings for user {tg_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/gifts', methods=['POST'])
def add_gift():
    data = request.get_json()
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'instance_id']
    if not all(field in data for field in required_fields): return jsonify({"error": "Missing data"}), 400
    owner_id = data['owner_id']
    gift_name = data['gift_name']
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to acquire this item."}), 403

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER: return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
            
            cur.execute("""INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) VALUES (%s, %s, %s, %s, %s, %s);""", (data['instance_id'], owner_id, data['gift_type_id'], gift_name, data['original_image_url'], data.get('lottie_path')))
            conn.commit()
            return jsonify({"message": "Gift added"}), 201
        except Exception as e:
            conn.rollback(); app.logger.error(f"Error adding gift for {owner_id}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/create_collectible', methods=['POST'])
def create_collectible_gift():
    data = request.get_json()
    owner_id = data.get('owner_id')
    gift_name = data.get('gift_name')
    gift_type_id = data.get('gift_type_id')
    custom_model_data = data.get('custom_model')
    custom_backdrop_data = data.get('custom_backdrop')
    custom_pattern_data = data.get('custom_pattern')
    custom_pattern_base64 = data.get('custom_pattern_base64')

    if not all([owner_id, gift_name, gift_type_id]):
        return jsonify({"error": "owner_id, gift_name, and gift_type_id are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to create this item."}), 403

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            parts_data = fetch_collectible_parts(gift_name)
            selected_model = custom_model_data or select_weighted_random(parts_data.get('models', []))
            selected_backdrop = custom_backdrop_data or select_weighted_random(parts_data.get('backdrops', []))

            selected_pattern = None
            pattern_image_url = None
            if custom_pattern_base64:
                selected_pattern = {"name": "Custom Upload", "rarityPermille": 1}
                pattern_image_url = custom_pattern_base64
            else:
                selected_pattern = custom_pattern_data or select_weighted_random(parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500

            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            new_instance_id = str(uuid.uuid4())

            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"

            if not pattern_image_url:
                pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"

            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path, "patternImage": pattern_image_url,
                "backdropColors": selected_backdrop.get('hex'), "supply": random.randint(2000, 10000), "author": get_gift_author(gift_name)
            }

            cur.execute("""
                INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, is_collectible, collectible_data, collectible_number, original_image_url)
                VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s);
            """, (new_instance_id, owner_id, gift_type_id, gift_name, json.dumps(collectible_data), next_number, model_image_url))
            conn.commit()

            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (new_instance_id,))
            created_gift = dict(cur.fetchone())
            if isinstance(created_gift.get('collectible_data'), str):
                created_gift['collectible_data'] = json.loads(created_gift['collectible_data'])

            return jsonify(created_gift), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in create_collectible_gift for {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            if conn: conn.close()

@app.route('/api/gifts/upgrade', methods=['POST'])
def upgrade_gift():
    data = request.get_json()
    instance_id = data.get('instance_id')
    custom_model_data = data.get('custom_model')
    custom_backdrop_data = data.get('custom_backdrop')
    custom_pattern_data = data.get('custom_pattern')
    custom_pattern_base64 = data.get('custom_pattern_base64') # New field

    if not instance_id: return jsonify({"error": "instance_id is required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
            gift_row = cur.fetchone()
            if not gift_row: return jsonify({"error": "Gift not found or already collectible."}), 404
            
            owner_id, gift_type_id, gift_name = gift_row['owner_id'], gift_row['gift_type_id'], gift_row['gift_name']

            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to upgrade this item."}), 403

            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            
            parts_data = fetch_collectible_parts(gift_name)
            selected_model = custom_model_data or select_weighted_random(parts_data.get('models', []))
            selected_backdrop = custom_backdrop_data or select_weighted_random(parts_data.get('backdrops', []))
            
            selected_pattern = None
            pattern_image_url = None
            if custom_pattern_base64:
                selected_pattern = {"name": "Custom Upload", "rarityPermille": 1}
                pattern_image_url = custom_pattern_base64
            else:
                selected_pattern = custom_pattern_data or select_weighted_random(parts_data.get('patterns', []))
            
            if not all([selected_model, selected_backdrop, selected_pattern]):
                return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500

            supply = random.randint(2000, 10000)
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            
            if not pattern_image_url:
                pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"

            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path, "patternImage": pattern_image_url,
                "backdropColors": selected_backdrop.get('hex'), "supply": supply, "author": get_gift_author(gift_name)
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
    raw_input = data.get('url')
    owner_id = data.get('owner_id')

    if not raw_input or not owner_id:
        return jsonify({"error": "url and owner_id are required"}), 400
    
    normalized_url = normalize_and_build_clone_url(raw_input)
    if not normalized_url:
        return jsonify({"error": "Invalid gift format. Please use a valid t.me/nft/ link or 'Name #Number' format."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    
    try:
        response = requests.get(normalized_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        gift_name_element = soup.find('div', class_='tgme_gift_preview').find('text')
        gift_name = gift_name_element.text.strip() if gift_name_element else None

        scraped_parts = {}
        table = soup.find('table', class_='tgme_gift_table')
        if table:
            for row in table.find_all('tr'):
                header = row.find('th').text.strip().lower() if row.find('th') else None
                value = ' '.join(row.find('td').text.split()) if row.find('td') else None
                if header and value:
                    scraped_parts[header] = ' '.join(value.split(' ')[:-1]) if '%' in value else value
        
        model_name = scraped_parts.get('model')
        backdrop_name = scraped_parts.get('backdrop')
        pattern_name = scraped_parts.get('symbol')

        if not all([gift_name, model_name, backdrop_name, pattern_name]):
            app.logger.error(f"Scraping failed for URL {normalized_url}. Found: name={gift_name}, model={model_name}, backdrop={backdrop_name}, pattern={pattern_name}")
            return jsonify({"error": "Could not scrape all required gift parts from the provided link."}), 400

        with conn.cursor(cursor_factory=DictCursor) as cur:
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to clone this item."}), 403

            all_parts_data = fetch_collectible_parts(gift_name)
            custom_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
            custom_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
            custom_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None)

            if not all([custom_model, custom_backdrop, custom_pattern]):
                return jsonify({"error": "Could not match scraped part names to available data."}), 500

            new_instance_id = str(uuid.uuid4())
            base_gift_id_to_clone = "1" 

            cur.execute("INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name) VALUES (%s, %s, %s, %s);", (new_instance_id, owner_id, base_gift_id_to_clone, gift_name))
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (base_gift_id_to_clone,))
            next_number = cur.fetchone()[0]
            
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            
            model_image_url = custom_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(custom_model['name'])}.png"
            lottie_model_path = custom_model.get('lottie') if custom_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(custom_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(custom_pattern['name'])}.png"

            collectible_data = {
                "model": custom_model, "backdrop": custom_backdrop, "pattern": custom_pattern,
                "modelImage": model_image_url,
                "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url,
                "backdropColors": custom_backdrop.get('hex'), "supply": random.randint(2000, 10000),
                "author": get_gift_author(gift_name)
            }
            cur.execute("""UPDATE gifts SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s WHERE instance_id = %s;""", (json.dumps(collectible_data), next_number, new_instance_id))
            conn.commit()

            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (new_instance_id,))
            cloned_gift = dict(cur.fetchone())
            cloned_gift['collectible_data'] = json.loads(cloned_gift['collectible_data'])
        
        conn.close()
        return jsonify(cloned_gift), 201

    except Exception as e:
        if conn: conn.close()
        app.logger.error(f"Error cloning gift from {raw_input}: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred during cloning."}), 500

@app.route('/api/gifts/create_clone', methods=['POST'])
def create_clone_gift():
    data = request.get_json()
    raw_input = data.get('url')
    owner_id = data.get('owner_id')

    if not raw_input or not owner_id:
        return jsonify({"error": "url and owner_id are required"}), 400

    normalized_url = normalize_and_build_clone_url(raw_input)
    if not normalized_url:
        return jsonify({"error": "Invalid gift format. Please use a valid t.me/nft/ link or 'Name #Number' format."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500

    try:
        response = requests.get(normalized_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        gift_name_element = soup.find('div', class_='tgme_gift_preview').find('text')
        gift_name = gift_name_element.text.strip() if gift_name_element else None

        scraped_parts = {}
        table = soup.find('table', class_='tgme_gift_table')
        if table:
            for row in table.find_all('tr'):
                header = row.find('th').text.strip().lower() if row.find('th') else None
                value = ' '.join(row.find('td').text.split()) if row.find('td') else None
                if header and value:
                    scraped_parts[header] = ' '.join(value.split(' ')[:-1]) if '%' in value else value

        model_name = scraped_parts.get('model')
        backdrop_name = scraped_parts.get('backdrop')
        pattern_name = scraped_parts.get('symbol')

        if not all([gift_name, model_name, backdrop_name, pattern_name]):
            app.logger.error(f"Scraping failed for URL {normalized_url}. Found: name={gift_name}, model={model_name}, backdrop={backdrop_name}, pattern={pattern_name}")
            return jsonify({"error": "Could not scrape all required gift parts from the provided link."}), 400

        with conn.cursor(cursor_factory=DictCursor) as cur:
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to clone this item."}), 403

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            all_parts_data = fetch_collectible_parts(gift_name)
            custom_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
            custom_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
            custom_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None)

            if not all([custom_model, custom_backdrop, custom_pattern]):
                return jsonify({"error": "Could not match scraped part names to available data."}), 500

            gift_type_id = CUSTOM_GIFTS_DATA.get(gift_name, {}).get('id', 'cloned_gift')
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            new_instance_id = str(uuid.uuid4())

            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)

            model_image_url = custom_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(custom_model['name'])}.png"
            lottie_model_path = custom_model.get('lottie') if custom_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(custom_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(custom_pattern['name'])}.png"

            collectible_data = {
                "model": custom_model, "backdrop": custom_backdrop, "pattern": custom_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path, "patternImage": pattern_image_url,
                "backdropColors": custom_backdrop.get('hex'), "supply": random.randint(2000, 10000), "author": get_gift_author(gift_name)
            }

            cur.execute("""
                INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, is_collectible, collectible_data, collectible_number, original_image_url)
                VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s);
            """, (new_instance_id, owner_id, gift_type_id, gift_name, json.dumps(collectible_data), next_number, model_image_url))

            conn.commit()

            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (new_instance_id,))
            cloned_gift = dict(cur.fetchone())
            if isinstance(cloned_gift.get('collectible_data'), str):
                cloned_gift['collectible_data'] = json.loads(cloned_gift['collectible_data'])

            return jsonify(cloned_gift), 201

    except Exception as e:
        if conn and not conn.closed: conn.rollback()
        app.logger.error(f"Error cloning gift from {raw_input}: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred during cloning."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/gift/<string:gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("""SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE LOWER(g.gift_type_id) = LOWER(%s) AND g.collectible_number = %s AND g.is_collectible = TRUE;""", (gift_type_id, collectible_number))
            gift_data = cur.fetchone()
            if not gift_data: return jsonify({"error": "Collectible gift not found."}), 404

            if is_custom_gift(gift_data['gift_name']):
                viewer_id = request.args.get('viewer_id')
                if not has_custom_gifts_enabled(cur, viewer_id):
                    return jsonify({"error": "Sorry, you cannot see this gift.", "reason": "custom_content_disabled"}), 403

            result = dict(gift_data)
            if isinstance(result.get('collectible_data'), str): result['collectible_data'] = json.loads(result.get('collectible_data'))
            return jsonify(result), 200
        except Exception as e:
            app.logger.error(f"Error fetching deep-linked gift {gift_type_id}-{collectible_number}: {e}", exc_info=True); return jsonify({"error": "Internal server error"}), 500
        finally: conn.close()

@app.route('/api/gifts/<string:instance_id>', methods=['PUT'])
def update_gift_state(instance_id):
    data = request.get_json()
    action = data.get('action')
    value = data.get('value')

    if action not in ['pin', 'hide', 'wear'] or not isinstance(value, bool):
        return jsonify({"error": "Invalid action or value"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            if action == 'wear' and value is True:
                cur.execute("SELECT owner_id FROM gifts WHERE instance_id = %s;", (instance_id,))
                owner_id_result = cur.fetchone()
                if not owner_id_result: return jsonify({"error": "Gift not found for wear action."}), 404
                cur.execute("UPDATE gifts SET is_worn = FALSE WHERE owner_id = %s AND is_worn = TRUE;", (owner_id_result[0],))

            column_to_update = {'pin': 'is_pinned', 'hide': 'is_hidden', 'wear': 'is_worn'}[action]
            
            if action == 'hide' and value is True:
                cur.execute("UPDATE gifts SET is_hidden = TRUE, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s;", (instance_id,))
            elif action == 'pin' and value is False:
                cur.execute("UPDATE gifts SET is_pinned = FALSE, pin_order = NULL WHERE instance_id = %s;", (instance_id,))
            else:
                 cur.execute(f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;", (value, instance_id))

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Gift not found or state not changed."}), 404
                
            conn.commit()
            return jsonify({"message": f"Gift {action} state updated successfully."}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"DB error updating gift state for {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/gifts/<string:instance_id>', methods=['DELETE'])
def delete_gift(instance_id):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    with conn.cursor() as cur:
        try:
            # First delete from child tables to respect foreign key constraints
            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))
            cur.execute("DELETE FROM giveaway_gifts WHERE gift_instance_id = %s;", (instance_id,))
            # Then delete from the parent table
            cur.execute("DELETE FROM gifts WHERE instance_id = %s;", (instance_id,))
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Gift not found."}), 404
            conn.commit()
            return jsonify({"message": "Gift deleted"}), 204
        except Exception as e:
            conn.rollback()
            app.logger.error(f"DB error deleting gift {instance_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

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

            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))

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
                cur.execute("""
                    UPDATE gifts SET pin_order = %s 
                    WHERE instance_id = %s AND owner_id = %s AND is_pinned = TRUE;
                """, (index, instance_id, owner_id))
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

                cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = ANY(%s);", (instance_ids,))
                
                cur.execute("""
                    UPDATE gifts
                    SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL, acquired_date = CURRENT_TIMESTAMP
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

            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))

            cur.execute("""UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL, acquired_date = CURRENT_TIMESTAMP WHERE instance_id = %s AND is_collectible = TRUE;""", (receiver_id, instance_id))
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

# --- WALL (POSTS) API ---
@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    owner_id = data.get('owner_id')
    content = data.get('content')

    if not owner_id or not content:
        return jsonify({"error": "owner_id and content are required"}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("INSERT INTO posts (owner_id, content) VALUES (%s, %s) RETURNING *;", (owner_id, content))
            new_post_data = cur.fetchone()
            conn.commit()

            cur.execute("SELECT username FROM accounts WHERE tg_id = %s", (owner_id,))
            author_username = cur.fetchone()['username']

            threading.Thread(target=send_new_post_notifications, args=(dict(new_post_data), author_username)).start()
            threading.Thread(target=send_mention_notifications, args=(dict(new_post_data), author_username)).start()
            
            new_post_response = dict(new_post_data)
            new_post_response['reactions'] = None # No reactions on a new post
            return jsonify(new_post_response), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating post for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/posts/<int:post_id>', methods=['DELETE'])
def delete_post(post_id):
    data = request.get_json()
    owner_id = data.get('owner_id')
    if not owner_id:
        return jsonify({"error": "owner_id is required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            # First delete reactions associated with the post
            cur.execute("DELETE FROM post_reactions WHERE post_id = %s;", (post_id,))
            # Then delete the post, ensuring the owner is correct
            cur.execute("DELETE FROM posts WHERE id = %s AND owner_id = %s;", (post_id, owner_id))
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Post not found or you are not the owner."}), 404
            conn.commit()
            return jsonify({"message": "Post deleted successfully"}), 204
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error deleting post {post_id} for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/posts/<int:post_id>/view', methods=['POST'])
def increment_post_view(post_id):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("UPDATE posts SET views = views + 1 WHERE id = %s;", (post_id,))
            conn.commit()
            return jsonify({"message": "View count incremented"}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error incrementing view for post {post_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/posts/<int:post_id>/react', methods=['POST'])
def add_reaction(post_id):
    data = request.get_json()
    user_id = data.get('user_id')
    reaction = data.get('reaction')
    if not all([user_id, reaction]):
        return jsonify({"error": "user_id and reaction are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT count(*) FROM post_reactions WHERE post_id = %s AND user_id = %s;", (post_id, user_id))
            if cur.fetchone()[0] >= 3:
                return jsonify({"error": "Maximum of 3 reactions per user per post."}), 403

            cur.execute("INSERT INTO post_reactions (post_id, user_id, reaction) VALUES (%s, %s, %s);", (post_id, user_id, reaction))
            conn.commit()
            return jsonify({"message": "Reaction added"}), 201
        except psycopg2.IntegrityError: # Handles unique constraint violation
            conn.rollback()
            return jsonify({"error": "You have already added this reaction."}), 409
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error adding reaction for user {user_id} to post {post_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

@app.route('/api/posts/<int:post_id>/react', methods=['DELETE'])
def remove_reaction(post_id):
    data = request.get_json()
    user_id = data.get('user_id')
    reaction = data.get('reaction')
    if not all([user_id, reaction]):
        return jsonify({"error": "user_id and reaction are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM post_reactions WHERE post_id = %s AND user_id = %s AND reaction = %s;", (post_id, user_id, reaction))
            conn.commit()
            return jsonify({"message": "Reaction removed"}), 204
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error removing reaction for user {user_id} from post {post_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

@app.route('/api/posts/<int:post_id>/reactions/<string:emoji>', methods=['GET'])
def get_reaction_users(post_id, emoji):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("""
                SELECT a.tg_id, a.username, a.full_name, a.avatar_url 
                FROM accounts a 
                JOIN post_reactions pr ON a.tg_id = pr.user_id
                WHERE pr.post_id = %s AND pr.reaction = %s;
            """, (post_id, emoji))
            users = [dict(row) for row in cur.fetchall()]
            return jsonify(users), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error fetching users for reaction {emoji} on post {post_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

# --- WALL SUBSCRIPTION API ---
@app.route('/api/subscriptions/<int:target_user_id>', methods=['GET'])
def get_subscription_status(target_user_id):
    subscriber_id = request.args.get('subscriber_id')
    if not subscriber_id:
        return jsonify({"error": "subscriber_id is required"}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT on_mention, on_new_post FROM wall_subscriptions WHERE subscriber_id = %s AND target_user_id = %s;", (subscriber_id, target_user_id))
            sub = cur.fetchone()
            if not sub:
                return jsonify({"on_mention": False, "on_new_post": False}), 200
            return jsonify(dict(sub)), 200
        except Exception as e:
            app.logger.error(f"Error fetching subscription status for user {subscriber_id} to {target_user_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

@app.route('/api/subscriptions', methods=['POST'])
def update_subscription():
    data = request.get_json()
    subscriber_id = data.get('subscriber_id')
    target_user_id = data.get('target_user_id')
    on_mention = data.get('on_mention', False)
    on_new_post = data.get('on_new_post', False)

    if not all([subscriber_id, target_user_id]):
        return jsonify({"error": "subscriber_id and target_user_id are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO wall_subscriptions (subscriber_id, target_user_id, on_mention, on_new_post)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (subscriber_id, target_user_id)
                DO UPDATE SET on_mention = EXCLUDED.on_mention, on_new_post = EXCLUDED.on_new_post;
            """, (subscriber_id, target_user_id, on_mention, on_new_post))
            conn.commit()
            return jsonify({"message": "Subscription updated"}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error updating subscription for user {subscriber_id} to {target_user_id}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

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
    required_channels = data.get('required_channels')

    if not all([creator_id, gift_instance_ids, winner_rule]):
        return jsonify({"error": "creator_id, gift_instance_ids, and winner_rule are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("INSERT INTO giveaways (creator_id, winner_rule, required_channels) VALUES (%s, %s, %s) RETURNING id;", (creator_id, winner_rule, required_channels))
            giveaway_id = cur.fetchone()['id']
            for gift_id in gift_instance_ids:
                cur.execute("INSERT INTO giveaway_gifts (giveaway_id, gift_instance_id) VALUES (%s, %s);", (giveaway_id, gift_id))

            new_state = f"awaiting_giveaway_channel_{giveaway_id}"
            cur.execute("UPDATE accounts SET bot_state = %s WHERE tg_id = %s;", (new_state, creator_id))
            conn.commit()

            send_telegram_message(
                creator_id,
                ("🏆 <b>Giveaway Setup: Step 1 of 3</b>\n\n"
                 "Please send the <b>numerical ID</b> of the public channel for the giveaway post.\n\n"
                 "To get the ID, you can forward a message from your channel to a bot like @userinfobot.\n\n"
                 f"<i>Important: You must add @{BOT_USERNAME} as an administrator to this channel.</i>\n\n"
                 "To cancel, send /cancel.")
            )
            return jsonify({"message": "Giveaway initiated.", "giveaway_id": giveaway_id}), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

# --- COLLECTION ENDPOINTS ---
@app.route('/api/collections', methods=['POST'])
def create_collection():
    data = request.get_json()
    owner_id = data.get('owner_id')
    name = data.get('name')
    if not all([owner_id, name]):
        return jsonify({"error": "owner_id and name are required."}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM collections WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= MAX_COLLECTIONS_PER_USER:
                return jsonify({"error": f"Collection limit of {MAX_COLLECTIONS_PER_USER} reached."}), 403

            cur.execute("INSERT INTO collections (owner_id, name) VALUES (%s, %s) RETURNING id, name;", (owner_id, name))
            new_collection = cur.fetchone()
            conn.commit()
            return jsonify(dict(new_collection)), 201
        except psycopg2.IntegrityError:
            conn.rollback()
            return jsonify({"error": "A collection with this name already exists."}), 409
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error creating collection for user {owner_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/collections/<int:collection_id>/gifts', methods=['POST'])
def add_gifts_to_collection(collection_id):
    data = request.get_json()
    instance_ids = data.get('instance_ids')
    owner_id = data.get('owner_id') # For validation
    if not all([instance_ids, owner_id]) or not isinstance(instance_ids, list):
        return jsonify({"error": "owner_id and a list of instance_ids are required."}), 400
        
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1 FROM collections WHERE id = %s AND owner_id = %s;", (collection_id, owner_id))
            if not cur.fetchone():
                return jsonify({"error": "Collection not found or you are not the owner."}), 404
            
            cur.execute("SELECT COALESCE(MAX(order_in_collection), -1) FROM gift_collections WHERE collection_id = %s;", (collection_id,))
            max_order = cur.fetchone()[0]
            
            for i, instance_id in enumerate(instance_ids):
                cur.execute("""
                    INSERT INTO gift_collections (collection_id, gift_instance_id, order_in_collection)
                    VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
                """, (collection_id, instance_id, max_order + 1 + i))
            
            conn.commit()
            return jsonify({"message": "Gifts added to collection."}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error adding gifts to collection {collection_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/collections/reorder_in_collection', methods=['POST'])
def reorder_in_collection():
    data = request.get_json()
    collection_id = data.get('collection_id')
    ordered_ids = data.get('ordered_instance_ids')
    owner_id = data.get('owner_id')
    if not all([collection_id, owner_id]) or not isinstance(ordered_ids, list):
        return jsonify({"error": "collection_id, owner_id, and ordered_instance_ids list are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT 1 FROM collections WHERE id = %s AND owner_id = %s;", (collection_id, owner_id))
            if not cur.fetchone():
                return jsonify({"error": "Collection not found or not owned by you."}), 404
            
            for index, instance_id in enumerate(ordered_ids):
                cur.execute("""
                    UPDATE gift_collections SET order_in_collection = %s
                    WHERE collection_id = %s AND gift_instance_id = %s;
                """, (index, collection_id, instance_id))

            conn.commit()
            return jsonify({"message": "Gifts reordered in collection."}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error reordering in collection {collection_id}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/stats', methods=['GET'])
def get_stats_ultimate():
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    stats = {}
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT COUNT(*) FROM gifts;")
            total_gifts = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT owner_id) FROM gifts WHERE owner_id != %s;", (TEST_ACCOUNT_TG_ID,))
            unique_owners = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gifts WHERE is_collectible = TRUE;")
            collectible_items = cur.fetchone()[0]
            stats['general_metrics'] = { 'total_gifts': total_gifts, 'unique_owners': unique_owners, 'collectible_items': collectible_items }
            return jsonify(stats), 200
        except Exception as e:
            app.logger.error(f"Error gathering stats: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally: conn.close()
            
@app.route('/api/search', methods=['GET'])
def search_items():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({"users": [], "gifts": []})

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            users = []
            gift = None
            
            search_term = query.lstrip('@') + '%'
            cur.execute("SELECT tg_id, username, full_name, avatar_url FROM accounts WHERE username ILIKE %s LIMIT 5;", (search_term,))
            users = [dict(row) for row in cur.fetchall()]
            
            gift_match = re.match(r'^([\w\s\']{3,20})-([0-9]{1,6})$', query, re.UNICODE)
            if gift_match:
                gift_name, gift_number = gift_match.group(1).strip(), int(gift_match.group(2))
                cur.execute("SELECT gift_type_id FROM gifts WHERE gift_name = %s AND collectible_number = %s LIMIT 1;", (gift_name, gift_number))
                found_gift = cur.fetchone()
                if found_gift:
                    gift = { "gift_type_id": found_gift["gift_type_id"], "collectible_number": gift_number }

            return jsonify({"users": users, "gift": gift}), 200
        except Exception as e:
            app.logger.error(f"Error during search for query '{query}': {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
        finally:
            conn.close()

@app.route('/api/user-by-phone/<path:phone_number>', methods=['GET'])
def get_user_by_phone(phone_number):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT username FROM accounts WHERE phone_number = %s;", (phone_number,))
            user = cur.fetchone()
            if not user:
                return jsonify({"error": "User with this phone number not found."}), 404
            return jsonify(user), 200
        except Exception as e:
            app.logger.error(f"Error fetching user by phone {phone_number}: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500
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
            participants = [p['user_id'] for p in cur.fetchall()]
            cur.execute("SELECT g.* FROM gifts g JOIN giveaway_gifts gg ON g.instance_id = gg.gift_instance_id WHERE gg.giveaway_id = %s;", (giveaway_id,))
            gifts = cur.fetchall()

            if not participants:
                send_telegram_message(giveaway['creator_id'], f"😔 Your giveaway in channel ID {giveaway['channel_id']} has ended, but there were no participants.")
                cur.execute("UPDATE giveaways SET status = 'finished' WHERE id = %s;", (giveaway_id,))
                conn.commit()
                return

            rewards_text_list = []
            emojis = ["🥇", "🥈", "🥉"]
            
            if giveaway['winner_rule'] == 'single':
                winner_id = random.choice(participants)
                cur.execute("UPDATE gifts SET owner_id = %s, acquired_date = CURRENT_TIMESTAMP WHERE instance_id IN (SELECT gift_instance_id FROM giveaway_gifts WHERE giveaway_id = %s);", (winner_id, giveaway_id))
                cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                winner_username = cur.fetchone()['username']
                
                for i, gift in enumerate(gifts):
                    emoji = emojis[i] if i < len(emojis) else "🏅"
                    rewards_text_list.append(f'{emoji} {gift["gift_name"]} #{gift["collectible_number"]:,}')
                
                results_text = f"🏆 <b>Giveaway Results</b> 🏆\n\nCongratulations to our winner @{winner_username} who gets all the prizes!\n\n{' '.join(rewards_text_list)}"
            else: # multiple
                num_winners = min(len(gifts), len(participants))
                selected_winner_ids = random.sample(participants, k=num_winners)
                winner_lines = []
                for i, winner_id in enumerate(selected_winner_ids):
                    gift = gifts[i]
                    cur.execute("UPDATE gifts SET owner_id = %s, acquired_date = CURRENT_TIMESTAMP WHERE instance_id = %s;", (winner_id, gift['instance_id']))
                    cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (winner_id,))
                    winner_username = cur.fetchone()['username']
                    emoji = emojis[i] if i < len(emojis) else "🏅"
                    winner_lines.append(f'{emoji} {gift["gift_name"]} #{gift["collectible_number"]:,} ➔ @{winner_username}')
                
                results_text = "🏆 <b>Giveaway Results</b> 🏆\n\nCongratulations to our winners:\n\n" + "\n".join(winner_lines)

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

def process_all_finished_giveaways():
    app.logger.info("Running process_all_finished_giveaways...")
    conn = get_db_connection()
    if not conn: 
        app.logger.error("Could not get DB connection to process winners.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM giveaways WHERE status = 'active' AND end_date <= CURRENT_TIMESTAMP;")
            giveaway_ids = [row[0] for row in cur.fetchall()]
            
            if giveaway_ids:
                app.logger.info(f"Found finished giveaways: {giveaway_ids}. Setting status to 'processing'.")
                cur.execute("UPDATE giveaways SET status = 'processing' WHERE id = ANY(%s);", (giveaway_ids,))
                conn.commit()
                
                for gid in giveaway_ids:
                    processing_thread = threading.Thread(target=process_giveaway_winners, args=(gid,))
                    processing_thread.start()
            else:
                app.logger.info("No giveaways found that have ended.")
    except Exception as e:
        app.logger.error(f"Error during process_all_finished_giveaways: {e}", exc_info=True)
    finally:
        conn.close()


def check_finished_giveaways():
    NO_GIVEAWAYS_SLEEP_SECONDS = 3600

    while True:
        try:
            conn = get_db_connection()
            if not conn:
                app.logger.warning("DB connection failed in checker loop. Retrying in 5 minutes.")
                time.sleep(300)
                continue

            next_giveaway_end_date = None
            with conn.cursor() as cur:
                cur.execute("SELECT end_date FROM giveaways WHERE status = 'active' ORDER BY end_date ASC LIMIT 1;")
                result = cur.fetchone()
                if result:
                    next_giveaway_end_date = result[0]
            conn.close() 

            if next_giveaway_end_date:
                now_utc = datetime.now(pytz.utc)
                wait_seconds = (next_giveaway_end_date - now_utc).total_seconds()

                if wait_seconds > 0:
                    sleep_duration = wait_seconds + 1
                    app.logger.info(f"Next giveaway ends at {next_giveaway_end_date}. Sleeping for {sleep_duration:.0f} seconds.")
                    time.sleep(sleep_duration)
            else:
                app.logger.info(f"No active giveaways. Sleeping for {NO_GIVEAWAYS_SLEEP_SECONDS / 60} minutes.")
                time.sleep(NO_GIVEAWAYS_SLEEP_SECONDS)

            process_all_finished_giveaways()

        except Exception as e:
             app.logger.error(f"Critical error in giveaway checker loop: {e}", exc_info=True)
             time.sleep(300)

@app.route('/api/transfer_gift', methods=['POST'])
def api_transfer_gift():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    api_key = data.get('api_key')
    sender_username = data.get('sender_username')
    receiver_username = data.get('receiver_username')
    gift_name_and_number = data.get('giftnameandnumber')
    comment = data.get('comment')

    if not api_key or api_key != TRANSFER_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    if not all([sender_username, receiver_username, gift_name_and_number]):
        return jsonify({"error": "Missing required fields: sender_username, receiver_username, giftnameandnumber"}), 400

    match = re.match(r'^(.*?)-(\d+)$', gift_name_and_number)
    if not match:
        return jsonify({"error": "Invalid giftnameandnumber format. Expected 'Name-Number', e.g., 'PlushPepe-1'."}), 400
    
    gift_name = match.group(1).strip()
    collectible_number = int(match.group(2))

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (sender_username,))
            sender = cur.fetchone()
            if not sender: return jsonify({"error": f"Sender '{sender_username}' not found."}), 404
            sender_id = sender['tg_id']

            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver: return jsonify({"error": f"Receiver '{receiver_username}' not found."}), 404
            receiver_id = receiver['tg_id']

            cur.execute("""
                SELECT instance_id, gift_type_id FROM gifts 
                WHERE owner_id = %s AND gift_name = %s AND collectible_number = %s AND is_collectible = TRUE;
            """, (sender_id, gift_name, collectible_number))
            gift = cur.fetchone()
            if not gift:
                return jsonify({"error": f"Gift '{gift_name} #{collectible_number}' not found or not owned by '{sender_username}'."}), 404
            instance_id, gift_type_id = gift['instance_id'], gift['gift_type_id']

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))

            cur.execute("""
                UPDATE gifts SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL, acquired_date = CURRENT_TIMESTAMP
                WHERE instance_id = %s;
            """, (receiver_id, instance_id))

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Gift transfer failed unexpectedly."}), 500

            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{collectible_number}"
            link_text = f"{gift_name} #{collectible_number:,}"

            sender_text = f'You successfully transferred Gift <a href="{deep_link}">{link_text}</a> to @{receiver_username}.'
            if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)

            receiver_text = f'You have received Gift <a href="{deep_link}">{link_text}</a> from @{sender_username}.'
            if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check Out Gift", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)

            return jsonify({"message": "Gift transferred successfully"}), 200

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error during API gift transfer of {gift_name_and_number}: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/create_and_transfer_random_gift', methods=['POST'])
def create_and_transfer_random_gift():
    data = request.get_json()
    gift_name = data.get('giftname')
    receiver_username = data.get('receiverUsername')
    sender_username = data.get('senderUsername')
    comment = data.get('comment')

    if not all([gift_name, receiver_username, sender_username]):
        return jsonify({"error": "Missing required fields: giftname, receiverUsername, senderUsername"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (sender_username,))
            sender = cur.fetchone()
            if not sender: return jsonify({"error": f"Sender '{sender_username}' not found."}), 404
            sender_id = sender['tg_id']

            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver: return jsonify({"error": f"Receiver '{receiver_username}' not found."}), 404
            receiver_id = receiver['tg_id']

            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            all_parts_data = fetch_collectible_parts(gift_name)
            selected_model = select_weighted_random(all_parts_data.get('models', []))
            selected_backdrop = select_weighted_random(all_parts_data.get('backdrops', []))
            selected_pattern = select_weighted_random(all_parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                return jsonify({"error": f"Could not determine all random parts for '{gift_name}'."}), 500

            gift_type_id = CUSTOM_GIFTS_DATA.get(gift_name, {}).get('id', 'generated_gift')

            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            new_instance_id = str(uuid.uuid4())
            
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"

            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url, "backdropColors": selected_backdrop.get('hex'), 
                "supply": random.randint(2000, 10000)
            }
            
            cur.execute("""
                INSERT INTO gifts 
                (instance_id, owner_id, gift_type_id, gift_name, is_collectible, collectible_data, collectible_number) 
                VALUES (%s, %s, %s, %s, TRUE, %s, %s);
            """, (new_instance_id, receiver_id, gift_type_id, gift_name, json.dumps(collectible_data), next_number))

            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{next_number}"
            link_text = f"{gift_name} #{next_number:,}"
            sender_text = f'You successfully created and sent <a href="{deep_link}">{link_text}</a> to @{receiver_username}.'
            if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)
            
            receiver_text = f'You have received a new gift, <a href="{deep_link}">{link_text}</a>, from @{sender_username}!'
            if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check Out Gift", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)

            return jsonify({"message": "Random gift created and transferred successfully."}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in create_and_transfer_random_gift: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()


@app.route('/api/create_and_transfer_custom_gift', methods=['POST'])
def create_and_transfer_custom_gift():
    data = request.get_json()
    gift_name = data.get('giftname')
    receiver_username = data.get('receiverUsername')
    sender_username = data.get('senderUsername')
    comment = data.get('comment')
    model_name = data.get('model')
    backdrop_name = data.get('backdrop')
    pattern_name = data.get('pattern')

    if not all([gift_name, receiver_username, sender_username]):
        return jsonify({"error": "Missing required fields: giftname, receiverUsername, senderUsername"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    with conn.cursor(cursor_factory=DictCursor) as cur:
        try:
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (sender_username,))
            sender = cur.fetchone()
            if not sender: return jsonify({"error": f"Sender '{sender_username}' not found."}), 404
            sender_id = sender['tg_id']

            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver: return jsonify({"error": f"Receiver '{receiver_username}' not found."}), 404
            receiver_id = receiver['tg_id']
            
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            all_parts_data = fetch_collectible_parts(gift_name)
            
            if model_name:
                selected_model = next((m for m in all_parts_data.get('models', []) if m['name'] == model_name), None)
                if not selected_model: return jsonify({"error": f"Model '{model_name}' not found for this gift."}), 400
            else:
                selected_model = select_weighted_random(all_parts_data.get('models', []))

            if backdrop_name:
                selected_backdrop = next((b for b in all_parts_data.get('backdrops', []) if b['name'] == backdrop_name), None)
                if not selected_backdrop: return jsonify({"error": f"Backdrop '{backdrop_name}' not found for this gift."}), 400
            else:
                selected_backdrop = select_weighted_random(all_parts_data.get('backdrops', []))

            if pattern_name:
                selected_pattern = next((p for p in all_parts_data.get('patterns', []) if p['name'] == pattern_name), None)
                if not selected_pattern: return jsonify({"error": f"Pattern '{pattern_name}' not found for this gift."}), 400
            else:
                selected_pattern = select_weighted_random(all_parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500

            gift_type_id = CUSTOM_GIFTS_DATA.get(gift_name, {}).get('id', 'generated_gift')

            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            new_instance_id = str(uuid.uuid4())
            
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"

            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url, "backdropColors": selected_backdrop.get('hex'),
                "supply": random.randint(2000, 10000)
            }
            
            cur.execute("""
                INSERT INTO gifts 
                (instance_id, owner_id, gift_type_id, gift_name, is_collectible, collectible_data, collectible_number) 
                VALUES (%s, %s, %s, %s, TRUE, %s, %s);
            """, (new_instance_id, receiver_id, gift_type_id, gift_name, json.dumps(collectible_data), next_number))

            conn.commit()

            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=gift{gift_type_id}-{next_number}"
            link_text = f"{gift_name} #{next_number:,}"
            sender_text = f'You successfully created and sent <a href="{deep_link}">{link_text}</a> to @{receiver_username}.'
            if comment: sender_text += f'\n\n<i>With comment: "{comment}"</i>'
            send_telegram_message(sender_id, sender_text)
            
            receiver_text = f'You have received a new gift, <a href="{deep_link}">{link_text}</a>, from @{sender_username}!'
            if comment: receiver_text += f'\n\n<i>With comment: "{comment}"</i>'
            receiver_markup = {"inline_keyboard": [[{"text": "Check Out Gift", "url": deep_link}]]}
            send_telegram_message(receiver_id, receiver_text, receiver_markup)
            
            return jsonify({"message": "Custom gift created and transferred successfully."}), 201

        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error in create_and_transfer_custom_gift: {e}", exc_info=True)
            return jsonify({"error": "An internal server error occurred."}), 500
        finally:
            conn.close()

@app.route('/api/user_data/<string:username>', methods=['GET'])
def get_user_data_by_username(username):
    # This endpoint is for the admin "Login As" feature
    auth_header = request.headers.get('Authorization')
    requesting_user_id = int(request.args.get('requesting_user_id', 0))

    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization header is missing or invalid"}), 401
    
    token = auth_header.split(' ')[1]
    
    # Simple auth check for mock mode, would be more robust in production
    if not token or requesting_user_id != ADMIN_TG_ID:
        return jsonify({"error": "Unauthorized"}), 401

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed."}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number, created_at FROM accounts WHERE LOWER(username) = LOWER(%s);", (username,))
            user_profile = cur.fetchone()

            if not user_profile:
                return jsonify({"error": "User profile not found."}), 404
            
            user_id = user_profile['tg_id']
            # Fetch all data for this user to create a complete client-side state
            
            account_data = dict(user_profile)
            
            cur.execute("SELECT * FROM gifts WHERE owner_id = %s;", (user_id,))
            account_data['owned_gifts'] = [dict(row) for row in cur.fetchall()]
            
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (user_id,))
            account_data['collectible_usernames'] = [row['username'] for row in cur.fetchall()]
            
            cur.execute("SELECT id, name FROM collections WHERE owner_id = %s;", (user_id,))
            account_data['collections'] = [dict(row) for row in cur.fetchall()]
            
            cur.execute("SELECT * FROM posts WHERE owner_id = %s;", (user_id,))
            account_data['posts'] = [dict(row) for row in cur.fetchall()]

            cur.execute("SELECT 1 FROM users_with_custom_gifts_enabled WHERE tg_id = %s;", (user_id,))
            account_data['custom_gifts_enabled'] = cur.fetchone() is not None

            return jsonify(account_data), 200

    except Exception as e:
        app.logger.error(f"Error fetching admin user data for {username}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            conn.close()

# --- APP STARTUP ---
if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    set_webhook()
    init_db()
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()

if __name__ == '__main__':
    print("Starting Flask server for local development...")
    init_db()
    giveaway_thread = threading.Thread(target=check_finished_giveaways, daemon=True)
    giveaway_thread.start()
    app.run(debug=True, port=int(os.environ.get('PORT', 5001)))
