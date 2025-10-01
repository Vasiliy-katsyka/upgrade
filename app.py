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
from psycopg2 import pool
from portalsmp import giftsFloors, search, filterFloors

# --- CONFIGURATION ---

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
CORS(app, resources={r"/api/*": {"origins": ["https://vasiliy-katsyka.github.io", "https://kutair.github.io"]}})

# --- ENVIRONMENT VARIABLES & CONSTANTS ---
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
PORTALS_AUTH_TOKEN = os.environ.get('PORTALS_AUTH_TOKEN')
TRANSFER_API_KEY = os.environ.get('TRANSFER_API_KEY')
WEBHOOK_URL = "https://upgrade-a57g.onrender.com"

if not DATABASE_URL or not TELEGRAM_BOT_TOKEN:
    raise ValueError("Missing required environment variables: DATABASE_URL and/or TELEGRAM_BOT_TOKEN")

GIFT_LIMIT_PER_USER = 500000
MAX_COLLECTIONS_PER_USER = 9
MAX_COLLECTIBLE_USERNAMES = 10
MIN_SALE_PRICE = 125
MAX_SALE_PRICE = 24000000
CDN_BASE_URL = "https://cdn.changes.tg/gifts/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBAPP_URL = "https://vasiliy-katsyka.github.io/upgrade/"
WEBAPP_SHORT_NAME = "upgrade"
BOT_USERNAME = "upgradeDemoBot"
TEST_ACCOUNT_TG_ID = 9999999999 # Holds sold gifts
ADMIN_USER_ID = 5146625949 # Special user ID for admin features
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
GIVEAWAY_UPDATE_THROTTLE_SECONDS = 30
REQUIRED_GIVEAWAY_CHANNEL = "@CompactTelegram"

collectible_parts_cache = {}
CACHE_DURATION_SECONDS = 3600  # Cache for 1 hour

# --- DATABASE CONNECTION POOL ---
db_pool = None

# In app.py, replace the entire get_db_connection function with this one.

def get_db_connection():
    """Gets a connection from the pool, ensuring it's alive and ready."""
    global db_pool
    if db_pool is None:
        try:
            # SimpleConnectionPool is fine, we'll manage stale connections manually.
            db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
            app.logger.info("Database connection pool created.")
        except psycopg2.OperationalError as e:
            app.logger.error(f"Could not create database connection pool: {e}", exc_info=True)
            return None

    # --- NEW ROBUST CONNECTION LOGIC ---
    for attempt in range(3): # Try up to 3 times to get a working connection
        try:
            conn = db_pool.getconn()
            # A simple, fast query to check if the connection is alive.
            # If this fails, it will raise an exception.
            with conn.cursor() as cur:
                cur.execute('SELECT 1;')
            # If we reach here, the connection is good.
            return conn
        except psycopg2.OperationalError as e:
            app.logger.warning(f"Stale/dead database connection detected on attempt {attempt + 1}: {e}")
            if conn:
                # IMPORTANT: Close the bad connection so the pool can discard it.
                # The `close=True` parameter tells the pool to not reuse it.
                db_pool.putconn(conn, close=True) 
            if attempt == 2: # If it's the last attempt
                app.logger.error("Failed to get a valid database connection after 3 attempts.")
                return None
            time.sleep(0.1) # Small delay before retrying
    return None # Should not be reached, but for safety

def put_db_connection(conn):
    """Puts a connection back into the pool if it's not already closed."""
    if db_pool and conn and not conn.closed:
        db_pool.putconn(conn)


# --- INLINE BOT CACHE ---
inline_cache = {}


# --- CUSTOM GIFT DATA ---
CUSTOM_GIFTS_DATA = {
    "Skebob": {
        "id": "custom_skebob",
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
        "id": "custom_baggin_cat",
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
    },
    "Keychain Dog": {
        "id": "custom_keychain_dog",
        "defaultImage": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/IMG_20250814_001025_847.png?raw=true",
        "models": [
            {"name": "Eyes Closed", "rarityPermille": 1, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224650949.png?raw=true"},
            {"name": "Golden Dog", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224121131.png?raw=true"},
            {"name": "Sapphire", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224146402.png?raw=true"},
            {"name": "Pavel Du Rove", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224244337.png?raw=true"},
            {"name": "Dogugu", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224952768.png?raw=true"},
            {"name": "Fridge", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223539864.png?raw=true"},
            {"name": "Cabbage", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223714257.png?raw=true"},
            {"name": "Hot Peach", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223954489.png?raw=true"},
            {"name": "Emelard", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224215512.png?raw=true"},
            {"name": "Mathematics", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224336692.png?raw=true"},
            {"name": "Duck", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224759233.png?raw=true"},
            {"name": "Hop Nai-Ni-Nai", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_225034482.png?raw=true"},
            {"name": "Hippo", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223609273.png?raw=true"},
            {"name": "Pikachu", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223642208.png?raw=true"},
            {"name": "Bad Doggy", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223841940.png?raw=true"},
            {"name": "Demonic Dog", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224026427.png?raw=true"},
            {"name": "Angelic Dog", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224054677.png?raw=true"},
            {"name": "Frogie", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224626535.png?raw=true"},
            {"name": "Pick Me", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224726881.png?raw=true"},
            {"name": "Dying Light", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224830054.png?raw=true"},
            {"name": "Halloween", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224929413.png?raw=true"},
            {"name": "Invisible", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_225502804.png?raw=true"},
            {"name": "Kitten", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223514941.png?raw=true"},
            {"name": "Tiger", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223743205.png?raw=true"},
            {"name": "Spider", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_223907805.png?raw=true"},
            {"name": "Elephant", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224405798.png?raw=true"},
            {"name": "Ghosty", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224443074.png?raw=true"},
            {"name": "Banana", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_224857672.png?raw=true"},
            {"name": "Rainbow", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_225055413.png?raw=true"},
            {"name": "I Don't Care", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/KeychainDog/blob/main/BackgroundEraser_20250814_225130675.png?raw=true"}
        ],
        "backdrops_source": "Toy Bear",
        "patterns_source": "Toy Bear"
    },
    "Taped Eggplant": {
        "id": "custom_taped_eggplant",
        "models": [
            {"name": "Gold Sneaker", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200715791.png?raw=true"},
            {"name": "Paul The Eggplant", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201628865.png?raw=true"},
            {"name": "Adult Toy", "rarityPermille": 5, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201700100.png?raw=true"},
            {"name": "Golden", "rarityPermille": 8, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195914455.png?raw=true"},
            {"name": "Silver", "rarityPermille": 8, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200004319.png?raw=true"},
            {"name": "Sapphire", "rarityPermille": 8, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200031197.png?raw=true"},
            {"name": "Ducky", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200202226.png?raw=true"},
            {"name": "Rich", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200532079.png?raw=true"},
            {"name": "Cigars", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200822737.png?raw=true"},
            {"name": "Red Bull", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201008559.png?raw=true"},
            {"name": "iPhone", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201115777.png?raw=true"},
            {"name": "To The Moon", "rarityPermille": 10, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201544571.png?raw=true"},
            {"name": "Wooden", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195940841.png?raw=true"},
            {"name": "French Baguete", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200121211.png?raw=true"},
            {"name": "Wild Eggplant", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200229914.png?raw=true"},
            {"name": "Spray Can", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200601835.png?raw=true"},
            {"name": "Bowling", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200851078.png?raw=true"},
            {"name": "Brick", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200927659.png?raw=true"},
            {"name": "Minion", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201042405.png?raw=true"},
            {"name": "Pasta", "rarityPermille": 20, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201518384.png?raw=true"},
            {"name": "Banana", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195534071.png?raw=true"},
            {"name": "Musical", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195604308.png?raw=true"},
            {"name": "Bug", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195644493.png?raw=true"},
            {"name": "Mop", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195719096.png?raw=true"},
            {"name": "Mango", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195739251.png?raw=true"},
            {"name": "Tooth Brush", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195800972.png?raw=true"},
            {"name": "Newspaper", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_195826700.png?raw=true"},
            {"name": "Kebab", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_200259658.png?raw=true"},
            {"name": "Power Strip", "rarityPermille": 30, "image": "https://github.com/Vasiliy-katsyka/Taped-Eggplant/blob/main/BackgroundEraser_20250827_201451237.png?raw=true"}
        ],
        "backdrops_source": "Plush Pepe",
        "patterns_source": "Plush Pepe"
    },
    "Vintage Ferrari": {
        "id": "custom_vintage_ferrari",
        "models": [
            {"name": "Bored Ape", "rarityPermille": 1, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232837620.png"},
            {"name": "Diamond", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230807795.png"},
            {"name": "Ford Mustang", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230929829.png"},
            {"name": "The Swamp", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231349161.png"},
            {"name": "North Korea", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232859265.png"},
            {"name": "Gold Ferrari", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230022017.png"},
            {"name": "Leclerc F1", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230353227.png"},
            {"name": "Tsunoda F1", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230633177.png"},
            {"name": "Rothmans Porsche", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231207311.png"},
            {"name": "Cybertruck", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232446458.png"},
            {"name": "Shaiba", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232614195.png"},
            {"name": "American", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232648128.png"},
            {"name": "Hell Car", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232813077.png"},
            {"name": "Lamborghini", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_225824432.png"},
            {"name": "Motorcycle", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230108359.png"},
            {"name": "Helicopter", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230841030.png"},
            {"name": "Bugatti", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230952790.png"},
            {"name": "Aerostat", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231323581.png"},
            {"name": "Beetle", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231520676.png"},
            {"name": "Take Me Back To London", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231715347.png"},
            {"name": "Beautiful People", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231955163.png"},
            {"name": "Roller Coaster", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232330112.png"},
            {"name": "Future", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232356807.png"},
            {"name": "Upside Down", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232716367.png"},
            {"name": "Accident", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232746147.png"},
            {"name": "Golf Cart", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230709859.png"},
            {"name": "Taxi", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_230905043.png"},
            {"name": "Back To The Future", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231014793.png"},
            {"name": "Tractor", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231244911.png"},
            {"name": "Despicable Me", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231414903.png"},
            {"name": "Ghostbusters", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_231520676.png"},
            {"name": "Mr. Bean", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232118455.png"},
            {"name": "Ducky", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232300190.png"},
            {"name": "Transporter T1", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232513956.png"},
            {"name": "Watermelon", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/Vintage-Ferrari/main/BackgroundEraser_20250829_232543790.png"}
        ],
        "backdrops_source": "Snoop Dogg",
        "patterns_source": "Snoop Dogg"
    },
    "Rich Frog": {
        "id": "custom_rich_frog",
        "limit": 300,
        "models": [
            {"name": "Old Movie", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012742001.png"},
            {"name": "Diamond", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012429267.png"},
            {"name": "Red Diamond", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012455524.png"},
            {"name": "Business", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012520124.png"},
            {"name": "Telegram", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013004928.png"},
            {"name": "Pepe", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013048306.png"},
            {"name": "Galaxy", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013237557.png"},
            {"name": "Shaiba", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013427809.png"},
            {"name": "Silver", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012356530.png"},
            {"name": "From The Hell", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012409591.png"},
            {"name": "Old", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012443079.png"},
            {"name": "Soldier", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012558144.png"},
            {"name": "Poker", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012757225.png"},
            {"name": "Zombie", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013004928.png"},
            {"name": "Satoshi Natokama", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013017488.png"},
            {"name": "The Open Network", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013033825.png"},
            {"name": "BDSM", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013107410.png"},
            {"name": "Angelic", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013121916.png"},
            {"name": "Rapper", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013335968.png"},
            {"name": "Mermaid", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013354933.png"},
            {"name": "Freddy's Frog", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013459472.png"},
            {"name": "Umbrella", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012540549.png"},
            {"name": "Wooden", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012623191.png"},
            {"name": "Bad Quality", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012643696.png"},
            {"name": "Pink", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012815025.png"},
            {"name": "Butterfly", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012831763.png"},
            {"name": "Rainbow", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012857269.png"},
            {"name": "Hawaii", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013140547.png"},
            {"name": "Gamer", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013207148.png"},
            {"name": "Pickme", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013221620.png"},
            {"name": "Watermelon", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013250229.png"},
            {"name": "Cactus", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013304943.png"},
            {"name": "Red", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013441382.png"},
            {"name": "Snowy", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013518774.png"},
            {"name": "Tiger", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_013900673.png"}
        ],
        "backdrops_source": "Snoop Dogg",
        "patterns_source": "Snoop Dogg"
    },
    "Sheeran Guitar": {
        "id": "custom_sheeran_guitar",
        "limit": 500,
        "models": [
            {"name": "Minion", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010828869.png"},
            {"name": "Darkness", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010917671.png"},
            {"name": "Sponge Bob", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011122889.png"},
            {"name": "Emelard", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011206969.png"},
            {"name": "Golden", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_022612251.png"},
            {"name": "Sapphire", "rarityPermille": 8, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_022557588.png"},
            {"name": "Electric", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010415814.png"},
            {"name": "Watery", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010543207.png"},
            {"name": "Lava Stone", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010608356.png"},
            {"name": "Old", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010725595.png"},
            {"name": "Banjo", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010740321.png"},
            {"name": "Russian", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010843719.png"},
            {"name": "Slipknot", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010943972.png"},
            {"name": "Yellow Wood", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005818708.png"},
            {"name": "Virus", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005851841.png"},
            {"name": "Mathematics", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005949661.png"},
            {"name": "Divide", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010004362.png"},
            {"name": "Multiply", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010049483.png"},
            {"name": "Plus", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010142885.png"},
            {"name": "Equals", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010221112.png"},
            {"name": "Subtract", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010314441.png"},
            {"name": "Emo", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010440657.png"},
            {"name": "Poopie", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010630535.png"},
            {"name": "Stickers", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010654844.png"},
            {"name": "Mexican", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010756062.png"},
            {"name": "Chinese", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010810805.png"},
            {"name": "Fire On Fire", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011024645.png"},
            {"name": "Witch", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011107534.png"},
            {"name": "Hawaii", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011136893.png"},
            {"name": "Minecraft", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005837327.png"},
            {"name": "Ancient", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005913329.png"},
            {"name": "Greenwood", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_005933291.png"},
            {"name": "Fluffy", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010330086.png"},
            {"name": "Electric Purple", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_010519545.png"},
            {"name": "Lego House", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011041563.png"}
        ],
        "backdrops_source": "Snoop Dogg",
        "patterns_source": "Snoop Dogg"
    },
    "Dancing Cactus": {
        "id": "custom_dancing_cactus",
        "limit": 1000,
        "models": [
            {"name": "Golden", "rarityPermille": 5, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011316070.png"},
            {"name": "Diamonds", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011350480.png"},
            {"name": "Rainbow", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011445647.png"},
            {"name": "Drought", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011516035.png"},
            {"name": "Electric", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011559687.png"},
            {"name": "Silver", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011754280.png"},
            {"name": "Virus", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011906408.png"},
            {"name": "Milfa", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012304698.png"},
            {"name": "Arabic", "rarityPermille": 10, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012318660.png"},
            {"name": "Plushy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011410903.png"},
            {"name": "Sky", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011626338.png"},
            {"name": "Egypt", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011640892.png"},
            {"name": "Chinese", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011707770.png"},
            {"name": "Greek", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011723289.png"},
            {"name": "Owww", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011821198.png"},
            {"name": "Business", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011834166.png"},
            {"name": "Skeleton", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011925666.png"},
            {"name": "Cabels", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011942350.png"},
            {"name": "Creepy", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012014199.png"},
            {"name": "Budni Cowboya", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012028668.png"},
            {"name": "Marble", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012132211.png"},
            {"name": "Hawaii", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012157170.png"},
            {"name": "Ed Sheeran", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012210326.png"},
            {"name": "Pandemic", "rarityPermille": 20, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012223282.png"},
            {"name": "Play", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011302760.png"},
            {"name": "Ancient", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011335854.png"},
            {"name": "Wooden", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011500397.png"},
            {"name": "Pink", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011530016.png"},
            {"name": "In Pain", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011740171.png"},
            {"name": "Sandy", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_011959216.png"},
            {"name": "Kissed", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012044125.png"},
            {"name": "Blue", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012059061.png"},
            {"name": "Robotic", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012112591.png"},
            {"name": "Sapphire", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012236420.png"},
            {"name": "Gray", "rarityPermille": 30, "image": "https://raw.githubusercontent.com/Vasiliy-katsyka/sheeranGifts/main/BackgroundEraser_20250911_012250549.png"}
        ],
        "backdrops_source": "Snoop Dogg",
        "patterns_source": "Snoop Dogg"
    },
    "Babuka": {
        "id": "custom_babuka",
        "limit": 10, # New field for total stock
        "models": [
            {"name": "Kupik", "rarityPermille": 30}, # 3.0%
            {"name": "Upik", "rarityPermille": 10}   # 1.0%
        ],
        "backdrops_source": "Toy Bear", # Using existing assets for demo
        "patterns_source": "Toy Bear"
    },
}

MAX_BUY_PER_LEVEL_MAP = {
    1: 20, 2: 50, 3: 100, 4: 500, 5: 1000, 6: 5000, 7: 10000,
    8: 15000, 9: 20000, 10: 30000, 11: 40000, 12: 50000, 13: 100000
}
# Constants for extending the top-up limit beyond the explicitly defined map.
BASE_EXTENSION_LEVEL = 13
EXTENSION_INCREMENT = 50000  # Add 50,000 to the limit for each level after 13.

# Defines the number of gifts needed to reach each level.
LEVEL_THRESHOLDS = [
    {"level": 1, "min": 0, "max": 20}, {"level": 2, "min": 20, "max": 50},
    {"level": 3, "min": 50, "max": 100}, {"level": 4, "min": 100, "max": 200},
    {"level": 5, "min": 200, "max": 300}, {"level": 6, "min": 300, "max": 400},
    {"level": 7, "min": 400, "max": 500}, {"level": 8, "min": 500, "max": 600},
    {"level": 9, "min": 600, "max": 700}, {"level": 10, "min": 700, "max": 800},
    {"level": 11, "min": 800, "max": 900}, {"level": 12, "min": 900, "max": 1000},
]

# Programmatically extend the LEVEL_THRESHOLDS list up to level 1000.
# This makes the leveling system much more scalable.
last_level_data = LEVEL_THRESHOLDS[-1]
current_level = last_level_data['level'] + 1
current_min = last_level_data['max']
while current_level <= 1000:
    step = 1000  # Each new level requires another 1000 gifts.
    new_max = current_min + step
    LEVEL_THRESHOLDS.append({"level": current_level, "min": current_min, "max": new_max})
    current_min = new_max
    current_level += 1

def calculate_user_level(gift_count):
    """Calculates user level based on the number of gifts they own."""
    # This function works perfectly with the new extended LEVEL_THRESHOLDS list.
    for level_data in reversed(LEVEL_THRESHOLDS):
        if gift_count >= level_data["min"]:
            return level_data["level"]
    return 1

# --- DATABASE HELPERS ---
def init_db():
    conn = get_db_connection()
    if not conn:
        app.logger.warning("Database connection failed during initialization.")
        return

    try:
        with conn.cursor() as cur:
            # --- UPDATED: accounts table with stars_balance and music_status ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    tg_id BIGINT PRIMARY KEY,
                    username VARCHAR(255) UNIQUE,
                    full_name VARCHAR(255),
                    avatar_url TEXT,
                    bio TEXT,
                    phone_number VARCHAR(50),
                    bot_state VARCHAR(255),
                    music_status TEXT,
                    stars_balance NUMERIC(20, 2) DEFAULT 0.0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Add new columns to existing accounts table if they don't exist
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='accounts' AND column_name='stars_balance') THEN
                        ALTER TABLE accounts ADD COLUMN stars_balance NUMERIC(20, 2) DEFAULT 0.0;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='accounts' AND column_name='music_status') THEN
                        ALTER TABLE accounts ADD COLUMN music_status TEXT;
                    END IF;
                END $$;
            """)

            # --- UPDATED: gifts table with is_on_sale and sale_price ---
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gifts (
                    instance_id VARCHAR(50) PRIMARY KEY,
                    owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                    gift_type_id VARCHAR(255) NOT NULL, gift_name VARCHAR(255) NOT NULL,
                    original_image_url TEXT, lottie_path TEXT, is_collectible BOOLEAN DEFAULT FALSE,
                    collectible_data JSONB, collectible_number INT,
                    acquired_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_hidden BOOLEAN DEFAULT FALSE, is_pinned BOOLEAN DEFAULT FALSE, is_worn BOOLEAN DEFAULT FALSE,
                    pin_order INT,
                    is_on_sale BOOLEAN DEFAULT FALSE,
                    sale_price INT
                );
            """)
            # Add new columns to existing gifts table if they don't exist
            cur.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='gifts' AND column_name='is_on_sale') THEN
                        ALTER TABLE gifts ADD COLUMN is_on_sale BOOLEAN DEFAULT FALSE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='gifts' AND column_name='sale_price') THEN
                        ALTER TABLE gifts ADD COLUMN sale_price INT;
                    END IF;
                END $$;
            """)
            
            # --- Indexes for gifts table ---
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_owner_id ON gifts (owner_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_type_and_number ON gifts (gift_type_id, collectible_number);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_pin_order ON gifts (owner_id, pin_order);")

            # --- Other tables (unchanged from original) ---
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
                    reaction_emoji VARCHAR(10) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(post_id, user_id, reaction_emoji)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_post_reactions_post_id ON post_reactions (post_id);")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    id SERIAL PRIMARY KEY,
                    subscriber_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                    target_user_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                    notification_type VARCHAR(20) NOT NULL, -- 'mentions' or 'new_posts'
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(subscriber_id, target_user_id, notification_type)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_user_subscriptions_target ON user_subscriptions (target_user_id, notification_type);")

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
                DO $$ BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='giveaways' AND column_name='required_channels') THEN
                        ALTER TABLE giveaways ADD COLUMN required_channels TEXT;
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
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS limited_gifts_stock (
                    gift_type_id VARCHAR(255) PRIMARY KEY,
                    total_stock INT NOT NULL,
                    remaining_stock INT NOT NULL
                );
            """)

            # --- Data Initialization Logic (unchanged from original) ---
            for gift_name, gift_data in CUSTOM_GIFTS_DATA.items():
                if 'limit' in gift_data:
                    gift_type_id = gift_data['id']
                    limit = gift_data['limit']
                    cur.execute("""
                        INSERT INTO limited_gifts_stock (gift_type_id, total_stock, remaining_stock)
                        SELECT %s, %s, %s - (SELECT COUNT(*) FROM gifts WHERE gift_type_id = %s)
                        ON CONFLICT (gift_type_id) DO UPDATE
                        SET total_stock = EXCLUDED.total_stock;
                    """, (gift_type_id, limit, limit, gift_type_id))

            cur.execute("SELECT 1 FROM accounts WHERE tg_id = %s;", (TEST_ACCOUNT_TG_ID,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (tg_id) DO NOTHING;
                """, (TEST_ACCOUNT_TG_ID, 'system_test_account', 'Test Account', 'https://raw.githubusercontent.com/Vasiliy-katsyka/upgrade/main/DMJTGStarsEmoji_AgADUhMAAk9WoVI.png', 'This account holds sold gifts.'))
            
            # --- Final Commit and Logging ---
            conn.commit()
            app.logger.info("Database initialized successfully.")
            
    except Exception as e:
        app.logger.error(f"Error during DB initialization: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: put_db_connection(conn)

# --- UTILITY & HELPER FUNCTIONS ---
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
            app.logger.info(f"CACHE HIT for collectible parts: {gift_name}")
            return cached_data

    app.logger.info(f"CACHE MISS for collectible parts: {gift_name}")

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
    match = re.match(r'^([\w\s\']+)[\s#-]*(\d+)$', input_str, re.UNICODE)
    if not match:
        # Fallback for names without spaces or special chars
        match = re.match(r'^([a-zA-Z\d]+)-(\d+)$', input_str)
    if match:
        name_part = match.group(1).strip().replace(' ', '')
        number_part = match.group(2).strip()
        # Find the canonical gift name from the CDN data to build the URL
        # This is a simplification; a more robust solution would map aliases
        return f"https://t.me/nft/{name_part}-{number_part}"
    return None

# --- BOT & GIVEAWAY LOGIC ---
def update_giveaway_message(giveaway_id):
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT g.*, a.username as creator_username FROM giveaways g JOIN accounts a ON g.creator_id = a.tg_id WHERE g.id = %s", (giveaway_id,))
            giveaway = cur.fetchone()
            if not giveaway or not giveaway['message_id']:
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
            required_channels_text = giveaway.get('required_channels') or 'No channels required'
            
            giveaway_text = (
                f"<b>Started Gifts Giveaway!</b>\n\n"
                f"<b>Details:</b>\n"
                f"• Subscribe: {required_channels_text}\n"
                f"• Deadline: {end_date_str}\n"
                f"• Rewards:{rewards}\n\n"
                f"Participants can now join this giveaway. Good luck 🎁"
            )
            
            join_url = f"https://t.me/{BOT_USERNAME}?start=giveaway{giveaway_id}"
            reply_markup = {"inline_keyboard": [[{"text": f"➡️ Join ({participant_count} Participants)", "url": join_url}]]}

            edit_telegram_message_text(giveaway['channel_id'], giveaway['message_id'], giveaway_text, reply_markup, disable_web_page_preview=True)
    finally:
        if conn: put_db_connection(conn)

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
        if conn: put_db_connection(conn)
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
        if conn: put_db_connection(conn)
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
        if conn: put_db_connection(conn)
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
        if conn: put_db_connection(conn)

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
        if conn: put_db_connection(conn)

# --- NEW/MODIFIED API ENDPOINTS ---
@app.route('/api/users/subscribe', methods=['POST'])
def handle_user_subscription():
    data = request.get_json()
    subscriber_id = data.get('subscriber_id')
    target_user_id = data.get('target_user_id')
    notification_type = data.get('notification_type')
    is_subscribing = data.get('is_subscribing')

    if not all([subscriber_id, target_user_id, notification_type, isinstance(is_subscribing, bool)]):
        return jsonify({"error": "Missing or invalid parameters."}), 400
    if notification_type not in ['mentions', 'new_posts']:
        return jsonify({"error": "Invalid notification_type."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor() as cur:
            if is_subscribing:
                cur.execute("""
                    INSERT INTO user_subscriptions (subscriber_id, target_user_id, notification_type)
                    VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
                """, (subscriber_id, target_user_id, notification_type))
            else:
                cur.execute("""
                    DELETE FROM user_subscriptions
                    WHERE subscriber_id = %s AND target_user_id = %s AND notification_type = %s;
                """, (subscriber_id, target_user_id, notification_type))
            conn.commit()
            return jsonify({"message": "Subscription updated."}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error updating subscription for {subscriber_id} to {target_user_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/posts/<int:post_id>/react', methods=['POST'])
def react_to_post(post_id):
    data = request.get_json()
    user_id = data.get('user_id')
    reaction_emoji = data.get('reaction_emoji')

    if not all([user_id, reaction_emoji]):
        return jsonify({"error": "user_id and reaction_emoji are required."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Check if user has already reacted with this emoji
            cur.execute("SELECT id FROM post_reactions WHERE post_id = %s AND user_id = %s AND reaction_emoji = %s;", (post_id, user_id, reaction_emoji))
            existing_reaction = cur.fetchone()

            if existing_reaction:
                # User is un-reacting
                cur.execute("DELETE FROM post_reactions WHERE id = %s;", (existing_reaction['id'],))
            else:
                # User is adding a new reaction
                # Check if user has reached the 3-reaction limit for this post
                cur.execute("SELECT COUNT(DISTINCT reaction_emoji) FROM post_reactions WHERE post_id = %s AND user_id = %s;", (post_id, user_id))
                reaction_count = cur.fetchone()[0]
                if reaction_count >= 3:
                    return jsonify({"error": "You can only use up to 3 different reactions per post."}), 403
                
                cur.execute("INSERT INTO post_reactions (post_id, user_id, reaction_emoji) VALUES (%s, %s, %s);", (post_id, user_id, reaction_emoji))

            conn.commit()
            
            # Fetch updated reaction counts
            cur.execute("""
                SELECT reaction_emoji, COUNT(*) as count
                FROM post_reactions WHERE post_id = %s
                GROUP BY reaction_emoji;
            """, (post_id,))
            updated_reactions = {row['reaction_emoji']: row['count'] for row in cur.fetchall()}
            
            return jsonify({"message": "Reaction updated.", "reactions": updated_reactions}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error processing reaction for post {post_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/admin/impersonate', methods=['POST'])
def admin_impersonate():
    data = request.get_json()
    admin_id = data.get('admin_id')
    target_username = data.get('target_username')

    if not all([admin_id, target_username]):
        return jsonify({"error": "admin_id and target_username are required."}), 400

    if int(admin_id) != ADMIN_USER_ID:
        return jsonify({"error": "Unauthorized."}), 403

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT tg_id, username, full_name, avatar_url FROM accounts WHERE LOWER(username) = LOWER(%s);", (target_username,))
            target_account = cur.fetchone()
            if not target_account:
                return jsonify({"error": f"User @{target_username} not found."}), 404
            
            return jsonify(dict(target_account)), 200
    except Exception as e:
        app.logger.error(f"Error during impersonation by admin {admin_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/search', methods=['GET'])
def search_handler():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    results = []
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Search for users
            if query.startswith('@'):
                search_term = query[1:] + '%'
            else:
                search_term = query + '%'
            
            cur.execute("""
                SELECT tg_id, username, full_name, avatar_url FROM accounts 
                WHERE username ILIKE %s OR full_name ILIKE %s LIMIT 5;
            """, (search_term, search_term))
            
            for row in cur.fetchall():
                results.append({
                    "type": "user",
                    "id": row['tg_id'],
                    "username": row['username'],
                    "full_name": row['full_name'],
                    "avatar_url": row['avatar_url']
                })
            
            # Search for gifts
            gift_match = re.match(r'^(.+?)-(\d+)$', query)
            if gift_match:
                gift_name, gift_number = gift_match.group(1).strip(), int(gift_match.group(2))
                cur.execute("""
                    SELECT instance_id, gift_name, collectible_number, collectible_data 
                    FROM gifts WHERE gift_name ILIKE %s AND collectible_number = %s AND is_collectible = TRUE LIMIT 1;
                """, (gift_name, gift_number))
                
                gift_row = cur.fetchone()
                if gift_row:
                    cd = gift_row['collectible_data']
                    results.append({
                        "type": "gift",
                        "id": gift_row['instance_id'],
                        "name": f"{gift_row['gift_name']} #{gift_row['collectible_number']}",
                        "image_url": cd.get('modelImage') if isinstance(cd, dict) else ''
                    })
        
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"Error during search for '{query}': {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

# --- EXISTING API ENDPOINTS (AS PROMISED, FULLY WRITTEN) ---

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
            put_db_connection(conn)

    return jsonify({"status": "ok"}), 200

@app.route('/api/account/collection_price', methods=['GET'])
def get_collection_price():
    tg_id = request.args.get('tg_id')
    if not tg_id:
        return jsonify({"error": "tg_id is required"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed."}), 500
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Fetch all collectible gifts for the user
            cur.execute("""
                SELECT instance_id, gift_name, collectible_data
                FROM gifts 
                WHERE owner_id = %s AND is_collectible = TRUE;
            """, (tg_id,))
            user_gifts = cur.fetchall()

        if not user_gifts:
            return jsonify({"total_price": 0, "priced_gifts": []}), 200

        total_price = 0.0
        priced_gifts_details = []
        filter_floors_cache = {} # Cache filterFloors results for the duration of this request

        for gift in user_gifts:
            gift_name = gift['gift_name']
            cd = gift['collectible_data']
            
            # Ensure collectible data is valid
            if not isinstance(cd, dict) or not all(k in cd for k in ['model', 'backdrop', 'pattern']):
                continue

            model = cd['model']['name']
            backdrop = cd['backdrop']['name']
            symbol = cd['pattern']['name']
            gift_display_name = f"{gift_name} #{cd.get('collectible_number', '?')}"
            
            try:
                # 1. Primary Method: Search for the exact item's floor price
                search_result = search(
                    gift_name=gift_name, model=model, backdrop=backdrop, symbol=symbol,
                    sort="price_asc", limit=1, authData=PORTALS_AUTH_TOKEN
                )

                if search_result and isinstance(search_result, list) and len(search_result) > 0:
                    item_price = float(search_result[0]['price'])
                    total_price += item_price
                    priced_gifts_details.append({
                        "name": gift_display_name,
                        "price": item_price,
                        "source": "Direct Listing"
                    })
                else:
                    # 2. Fallback Method: Estimate price from attribute floors
                    if gift_name not in filter_floors_cache:
                        app.logger.info(f"Cache miss for {gift_name}, fetching filter floors...")
                        filter_floors_cache[gift_name] = filterFloors(gift_name=gift_name, authData=PORTALS_AUTH_TOKEN)
                    
                    floors_data = filter_floors_cache[gift_name]
                    estimated_price = 0.0
                    
                    if floors_data and 'models' in floors_data and model in floors_data['models']:
                        estimated_price += float(floors_data['models'][model]['floor'])
                    if floors_data and 'backdrops' in floors_data and backdrop in floors_data['backdrops']:
                        estimated_price += float(floors_data['backdrops'][backdrop]['floor'])
                    if floors_data and 'symbols' in floors_data and symbol in floors_data['symbols']:
                        estimated_price += float(floors_data['symbols'][symbol]['floor'])

                    if estimated_price > 0:
                        total_price += estimated_price
                        priced_gifts_details.append({
                            "name": gift_display_name,
                            "price": estimated_price,
                            "source": "Estimated Floor"
                        })
            
            except Exception as e:
                app.logger.error(f"Error pricing gift {gift_display_name}: {e}", exc_info=True)
                # Skip this gift if the external API fails for it, but continue with others.

        return jsonify({
            "total_price": round(total_price, 2),
            "priced_gifts": priced_gifts_details
        }), 200

    except Exception as e:
        app.logger.error(f"Error in get_collection_price for user {tg_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            put_db_connection(conn)

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

@app.route('/api/profile_by_collectible/<string:collectible>', methods=['GET'])
def get_profile_by_collectible(collectible):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            user_id = None
            if collectible.startswith('@'):
                username = collectible[1:]
                cur.execute("SELECT owner_id FROM collectible_usernames WHERE LOWER(username) = LOWER(%s);", (username,))
                result = cur.fetchone()
                if result:
                    user_id = result['owner_id']
            elif collectible.startswith('+888'):
                cur.execute("SELECT tg_id FROM accounts WHERE phone_number = %s;", (collectible,))
                result = cur.fetchone()
                if result:
                    user_id = result['tg_id']

            if not user_id:
                return jsonify({"error": "No user found for this collectible."}), 404

            cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (user_id,))
            owner_username = cur.fetchone()['username']

            return jsonify({"username": owner_username}), 200

    except Exception as e:
        app.logger.error(f"Error fetching profile for collectible {collectible}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/request_test_env', methods=['POST'])
def request_test_env():
    data = request.get_json()
    tg_id = data.get('tg_id')

    if not tg_id:
        return jsonify({"error": "tg_id is required"}), 400

    text = "Test environment request. Tap to open:"
    reply_markup = {
        "inline_keyboard": [
            [{"text": "Tap to open", "web_app": {"url": "https://Kutair.github.io/testUp"}}]
        ]
    }

    result = send_telegram_message(tg_id, text, reply_markup)

    if result and result.get('ok'):
        return jsonify({"message": "Test environment link sent."}), 200
    else:
        return jsonify({"error": "Failed to send message via Telegram API"}), 502

@app.route('/api/stars/topup', methods=['POST'])
def api_topup_stars():
    data = request.get_json()
    user_id = data.get('user_id')
    amount = data.get('amount')

    if not user_id or not isinstance(amount, (int, float)) or amount <= 0:
        return jsonify({"error": "user_id and a positive amount are required."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor() as cur:
            # Get user's gift count to determine their level
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (user_id,))
            gift_count = cur.fetchone()[0]
            
            # Use the helper function to calculate the correct level
            level = calculate_user_level(gift_count)
            
            # --- NEW LOGIC to determine the max top-up amount ---
            max_buy = 0
            if level in MAX_BUY_PER_LEVEL_MAP:
                # Use the specific value if the level is in our custom map
                max_buy = MAX_BUY_PER_LEVEL_MAP[level]
            elif level > BASE_EXTENSION_LEVEL:
                # For levels higher than our map, calculate it with a formula
                base_amount = MAX_BUY_PER_LEVEL_MAP[BASE_EXTENSION_LEVEL]
                extra_levels = level - BASE_EXTENSION_LEVEL
                max_buy = base_amount + (extra_levels * EXTENSION_INCREMENT)
            else:
                # A safe fallback in case of an unexpected level number
                max_buy = 50000 
            
            if amount > max_buy:
                return jsonify({"error": f"Amount exceeds your level {level} limit of {max_buy:,} Stars."}), 403

            cur.execute(
                "UPDATE accounts SET stars_balance = stars_balance + %s WHERE tg_id = %s RETURNING stars_balance;",
                (amount, user_id)
            )
            new_balance = cur.fetchone()[0]
            conn.commit()
            
            deep_link = f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp=Stars"
            message_text = f"✅ Successful top up of <b>{amount:,.0f} Stars</b>!\nCheck your <a href='{deep_link}'>balance</a>."
            send_telegram_message(user_id, message_text)
            
            return jsonify({"new_balance": float(new_balance)}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error in stars topup for user {user_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/market/summary', methods=['GET'])
def api_get_market_summary():
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT 
                    gift_type_id, 
                    gift_name, 
                    original_image_url,
                    MIN(sale_price) as lowest_price
                FROM gifts
                WHERE is_on_sale = TRUE AND is_collectible = TRUE
                GROUP BY gift_type_id, gift_name, original_image_url;
            """)
            summary = [dict(row) for row in cur.fetchall()]
            return jsonify(summary), 200
    except Exception as e:
        app.logger.error(f"Error fetching market summary: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/market/listings/<string:gift_type_id>', methods=['GET'])
def api_get_market_listings(gift_type_id):
    # Extract query params for filtering and sorting
    sort_by = request.args.get('sort_by', 'price_asc')
    model = request.args.get('model')
    backdrop = request.args.get('backdrop')
    symbol = request.args.get('symbol')

    # Base query
    query = "SELECT instance_id, collectible_data, sale_price FROM gifts WHERE is_on_sale = TRUE AND is_collectible = TRUE AND gift_type_id = %s"
    params = [gift_type_id]

    # Add filters
    if model:
        query += " AND collectible_data->'model'->>'name' = %s"
        params.append(model)
    if backdrop:
        query += " AND collectible_data->'backdrop'->>'name' = %s"
        params.append(backdrop)
    if symbol:
        query += " AND collectible_data->'pattern'->>'name' = %s"
        params.append(symbol)

    # Add sorting
    if sort_by == 'price_desc':
        query += " ORDER BY sale_price DESC"
    elif sort_by == 'number_asc':
        query += " ORDER BY collectible_number ASC"
    elif sort_by == 'number_desc':
        query += " ORDER BY collectible_number DESC"
    elif sort_by == 'rarity_asc':
        # Sorting by rarity requires casting the JSONB value to an integer
        query += " ORDER BY (collectible_data->'model'->>'rarityPermille')::int ASC"
    else: # Default to price_asc
        query += " ORDER BY sale_price ASC"
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, tuple(params))
            listings = [dict(row) for row in cur.fetchall()]
            # Process JSONB data before sending
            for item in listings:
                if 'collectible_data' in item and isinstance(item['collectible_data'], str):
                    item['collectible_data'] = json.loads(item['collectible_data'])
            return jsonify(listings), 200
    except Exception as e:
        app.logger.error(f"Error fetching market listings for {gift_type_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/market/buy/<string:instance_id>', methods=['POST'])
def api_buy_market_gift(instance_id):
    data = request.get_json()
    buyer_id = data.get('buyer_id')
    if not buyer_id:
        return jsonify({"error": "buyer_id is required."}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # --- START TRANSACTION ---
            # 1. Lock the gift row and get its details to prevent race conditions
            cur.execute(
                "SELECT owner_id, sale_price, gift_name, collectible_number FROM gifts WHERE instance_id = %s AND is_on_sale = TRUE FOR UPDATE;",
                (instance_id,)
            )
            gift_to_buy = cur.fetchone()
            if not gift_to_buy:
                conn.rollback()
                return jsonify({"error": "This gift is no longer for sale or does not exist."}), 404

            seller_id = gift_to_buy['owner_id']
            price = gift_to_buy['sale_price']
            gift_name = f"{gift_to_buy['gift_name']} #{gift_to_buy['collectible_number']}"

            if int(seller_id) == int(buyer_id):
                conn.rollback()
                return jsonify({"error": "You cannot buy your own gift."}), 400

            # 2. Check buyer's balance (lock the row)
            cur.execute("SELECT stars_balance FROM accounts WHERE tg_id = %s FOR UPDATE;", (buyer_id,))
            buyer_balance = cur.fetchone()['stars_balance']
            if buyer_balance < price:
                conn.rollback()
                return jsonify({"error": "Insufficient Stars balance."}), 402
            
            # 3. Deduct from buyer, add to seller
            cur.execute("UPDATE accounts SET stars_balance = stars_balance - %s WHERE tg_id = %s;", (price, buyer_id))
            cur.execute("UPDATE accounts SET stars_balance = stars_balance + %s WHERE tg_id = %s;", (price, seller_id))
            
            # 4. Transfer ownership and unlist the gift
            cur.execute(
                "UPDATE gifts SET owner_id = %s, is_on_sale = FALSE, sale_price = NULL, acquired_date = CURRENT_TIMESTAMP WHERE instance_id = %s;",
                (buyer_id, instance_id)
            )
            
            # --- COMMIT TRANSACTION ---
            conn.commit()

            # 5. Send notifications
            send_telegram_message(seller_id, f"🎉 Your {gift_name} has sold for ⭐ {price}!\nThe Stars have been added to your balance.")
            
            return jsonify({"message": "Purchase successful."}), 200

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error during market purchase of {instance_id} by {buyer_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred during the transaction."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/profile/<string:username>', methods=['GET'])
def get_user_profile(username):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    viewer_id = request.args.get('viewer_id')
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
            gifts = _update_gifts_with_live_supply(cur, gifts)
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
            
            # Fetch posts and their reactions for the Wall
            cur.execute("SELECT id, content, views, created_at FROM posts WHERE owner_id = %s ORDER BY created_at DESC;", (user_id,))
            posts = []
            for post_row in cur.fetchall():
                post = dict(post_row)
                cur.execute("""
                    SELECT reaction_emoji, COUNT(*) as count, ARRAY_AGG(a.username) as users
                    FROM post_reactions pr
                    JOIN accounts a ON pr.user_id = a.tg_id
                    WHERE pr.post_id = %s
                    GROUP BY pr.reaction_emoji;
                """, (post['id'],))
                post['reactions'] = {row['reaction_emoji']: {"count": row['count'], "users": row['users']} for row in cur.fetchall()}
                posts.append(post)

            profile_data['posts'] = posts

            # Fetch user's subscription status to this profile
            if viewer_id:
                cur.execute("SELECT notification_type FROM user_subscriptions WHERE subscriber_id = %s AND target_user_id = %s;", (viewer_id, user_id))
                profile_data['subscription_status'] = {row['notification_type']: True for row in cur.fetchall()}
            
            return jsonify(profile_data), 200
    except Exception as e:
        app.logger.error(f"Error fetching profile for {username}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally: 
        if conn: put_db_connection(conn)

@app.route('/api/account', methods=['POST'])
def get_or_create_account():
    data = request.get_json()
    if not data or 'tg_id' not in data: return jsonify({"error": "Missing tg_id"}), 400
    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT * FROM accounts WHERE tg_id = %s;", (tg_id,))
            account = cur.fetchone()
            if not account:
                # Use ON CONFLICT to handle race conditions gracefully
                cur.execute("""
                    INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio, phone_number) 
                    VALUES (%s, %s, %s, %s, %s, %s) 
                    ON CONFLICT(tg_id) DO NOTHING;
                """, (
                    tg_id, data.get('username'), data.get('full_name'), data.get('avatar_url'), 
                    'My first account!', 'Not specified'
                ))
                conn.commit()
            
            # Fetch the account data again to ensure consistency
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

            gifts = _update_gifts_with_live_supply(cur, gifts)

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
            
            # Fetch posts for Wall
            cur.execute("SELECT id, content, views, created_at FROM posts WHERE owner_id = %s ORDER BY created_at DESC;", (tg_id,))
            posts = []
            for post_row in cur.fetchall():
                post = dict(post_row)
                cur.execute("""
                    SELECT reaction_emoji, COUNT(*) as count
                    FROM post_reactions
                    WHERE post_id = %s
                    GROUP BY reaction_emoji;
                """, (post['id'],))
                post['reactions'] = {row['reaction_emoji']: row['count'] for row in cur.fetchall()}
                posts.append(post)
            account_data['posts'] = posts
            
            return jsonify(account_data), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error in get_or_create_account for {tg_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/account', methods=['PUT'])
def update_account():
    data = request.get_json()
    if not data or 'tg_id' not in data: 
        return jsonify({"error": "Missing tg_id"}), 400
    
    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn: 
        return jsonify({"error": "Database connection failed."}), 500
    
    try:
        with conn.cursor() as cur:
            update_fields, update_values = [], []
            
            # Build a dynamic query to update only the fields provided
            if 'username' in data: 
                update_fields.append("username = %s")
                update_values.append(data['username'])
            if 'full_name' in data: 
                update_fields.append("full_name = %s")
                update_values.append(data['full_name'])
            if 'avatar_url' in data: 
                update_fields.append("avatar_url = %s")
                update_values.append(data['avatar_url'])
            if 'bio' in data: 
                update_fields.append("bio = %s")
                update_values.append(data['bio'])
            
            # --- THIS IS THE FIX for Bug #4 ---
            if 'music_status' in data:
                update_fields.append("music_status = %s")
                update_values.append(data['music_status'])
            
            if 'phone_number' in data: 
                cur.execute("SELECT 1 FROM accounts WHERE phone_number = %s AND tg_id != %s;", (data['phone_number'], tg_id))
                if cur.fetchone():
                    return jsonify({"error": "This phone number is already in use."}), 409
                update_fields.append("phone_number = %s")
                update_values.append(data['phone_number'])

            if not update_fields: 
                return jsonify({"error": "No fields provided for update."}), 400
            
            update_query = f"UPDATE accounts SET {', '.join(update_fields)} WHERE tg_id = %s;"
            update_values.append(tg_id)
            
            cur.execute(update_query, tuple(update_values))
            
            if cur.rowcount == 0: 
                return jsonify({"error": "Account not found or no changes made."}), 404
            
            conn.commit()
            return jsonify({"message": "Account updated successfully."}), 200
            
    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        app.logger.warning(f"Integrity error updating account {tg_id}: {e}")
        if 'username' in str(e): 
            return jsonify({"error": "This username is already taken."}), 409
        return jsonify({"error": "A database conflict occurred."}), 409
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error updating account {tg_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/account/settings', methods=['POST'])
def update_account_settings():
    data = request.get_json()
    tg_id = data.get('tg_id')
    custom_gifts_enabled = data.get('custom_gifts_enabled')

    if tg_id is None or not isinstance(custom_gifts_enabled, bool):
        return jsonify({"error": "tg_id and a boolean custom_gifts_enabled are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed"}), 500
    try:
        with conn.cursor() as cur:
            if custom_gifts_enabled:
                cur.execute("INSERT INTO users_with_custom_gifts_enabled (tg_id) VALUES (%s) ON CONFLICT (tg_id) DO NOTHING;", (tg_id,))
            else:
                cur.execute("DELETE FROM users_with_custom_gifts_enabled WHERE tg_id = %s;", (tg_id,))
            conn.commit()
            return jsonify({"message": "Settings updated successfully"}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error updating settings for user {tg_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/gifts', methods=['POST'])
def add_gift():
    data = request.get_json()
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'instance_id']
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing data"}), 400
    
    owner_id = data['owner_id']
    gift_name = data['gift_name']
    gift_type_id = data['gift_type_id']

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database failed"}), 500
    
    try:
        with conn.cursor() as cur:
            # Check for custom gift permissions first
            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to acquire this item."}), 403

            # Check general gift limit
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403

            # --- UPDATED: Limited Gift Stock Logic ---
            is_limited = gift_type_id in [g['id'] for g in CUSTOM_GIFTS_DATA.values() if 'limit' in g]
            if is_limited:
                # Lock the row for update to prevent race conditions
                cur.execute("SELECT remaining_stock FROM limited_gifts_stock WHERE gift_type_id = %s FOR UPDATE;", (gift_type_id,))
                stock_row = cur.fetchone()
                if not stock_row or stock_row[0] <= 0:
                    conn.rollback() # Release the lock
                    return jsonify({"error": "This limited gift is sold out."}), 403
                
                # Decrement stock
                cur.execute("UPDATE limited_gifts_stock SET remaining_stock = remaining_stock - 1 WHERE gift_type_id = %s;", (gift_type_id,))
            # --- END OF UPDATE ---

            # Insert the gift
            cur.execute("""
                INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path) 
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (data['instance_id'], owner_id, gift_type_id, gift_name, data['original_image_url'], data.get('lottie_path')))
            
            conn.commit()
            return jsonify({"message": "Gift added"}), 201
    except Exception as e:
        if conn:
            conn.rollback()
        app.logger.error(f"Error adding gift for {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn:
            put_db_connection(conn)

@app.route('/api/gifts/upgrade', methods=['POST'])
def upgrade_gift():
    data = request.get_json()
    if 'instance_id' not in data: return jsonify({"error": "instance_id is required"}), 400
    instance_id = data['instance_id']
    custom_model_data = data.get('custom_model')
    custom_backdrop_data = data.get('custom_backdrop')
    custom_pattern_data = data.get('custom_pattern')
    custom_pattern_image = data.get('custom_pattern_image') # For uploads

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
            gift_row = cur.fetchone()
            if not gift_row: return jsonify({"error": "Gift not found or already collectible."}), 404
            
            owner_id, gift_type_id, gift_name = gift_row['owner_id'], gift_row['gift_type_id'], gift_row['gift_name']

            if is_custom_gift(gift_name) and not has_custom_gifts_enabled(cur, owner_id):
                return jsonify({"error": "You must enable Custom Gifts in settings to upgrade this item."}), 403

            # --- UPDATED SUPPLY LOGIC ---
            # Mark the gift as collectible first to include it in the count
            cur.execute("UPDATE gifts SET is_collectible = TRUE WHERE instance_id = %s;", (instance_id,))

            # Now, get the live total count for this gift type
            cur.execute("SELECT COUNT(*) FROM gifts WHERE gift_type_id = %s AND is_collectible = TRUE;", (gift_type_id,))
            live_supply_count = cur.fetchone()[0]
            # --- END OF UPDATE ---
            
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]
            
            # ... (logic to select random or custom parts) ...
            parts_data = fetch_collectible_parts(gift_name)
            selected_model = data.get('custom_model') or select_weighted_random(parts_data.get('models', []))
            selected_backdrop = data.get('custom_backdrop') or select_weighted_random(parts_data.get('backdrops', []))
            selected_pattern = data.get('custom_pattern') or select_weighted_random(parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                # Rollback the is_collectible change if parts are missing
                conn.rollback()
                app.logger.error(f"Could not determine all parts for '{gift_name}'.")
                return jsonify({"error": f"Could not determine all parts for '{gift_name}'."}), 500
            
            pattern_source_name = CUSTOM_GIFTS_DATA.get(gift_name, {}).get("patterns_source", gift_name)
            model_image_url = selected_model.get('image') or f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png"
            lottie_model_path = selected_model.get('lottie') if selected_model.get('lottie') is not None else f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json"
            pattern_image_url = f"{CDN_BASE_URL}patterns/{quote(pattern_source_name)}/png/{quote(selected_pattern['name'])}.png"
            
            collectible_data = {
                "model": selected_model, "backdrop": selected_backdrop, "pattern": selected_pattern,
                "modelImage": model_image_url, "lottieModelPath": lottie_model_path,
                "patternImage": pattern_image_url, "backdropColors": selected_backdrop.get('hex'), 
                "supply": live_supply_count, # Use the live count here
                "author": get_gift_author(gift_name)
            }
            
            cur.execute("""UPDATE gifts SET collectible_data = %s, collectible_number = %s, lottie_path = NULL WHERE instance_id = %s;""", (json.dumps(collectible_data), next_number, instance_id))
            
            conn.commit()
            
            # Finally, update the supply for all other collectibles of the same type
            cur.execute("""
                UPDATE gifts
                SET collectible_data = collectible_data || jsonb_build_object('supply', %s)
                WHERE gift_type_id = %s AND is_collectible = TRUE;
            """, (live_supply_count, gift_type_id))
            conn.commit()

            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (instance_id,))
            upgraded_gift = dict(cur.fetchone())
            if isinstance(upgraded_gift.get('collectible_data'), str): upgraded_gift['collectible_data'] = json.loads(upgraded_gift['collectible_data'])
            return jsonify(upgraded_gift), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error upgrading gift {instance_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

def _update_gifts_with_live_supply(cur, gifts_list):
    """Efficiently updates a list of gift dictionaries with the latest supply counts."""
    if not gifts_list:
        return []
    
    collectible_gift_types = list(set([g['gift_type_id'] for g in gifts_list if g['is_collectible']]))
    if not collectible_gift_types:
        return gifts_list

    cur.execute("""
        SELECT gift_type_id, COUNT(*) as live_count
        FROM gifts
        WHERE gift_type_id = ANY(%s) AND is_collectible = TRUE
        GROUP BY gift_type_id;
    """, (collectible_gift_types,))
    
    supply_map = {row['gift_type_id']: row['live_count'] for row in cur.fetchall()}

    for gift in gifts_list:
        if gift['is_collectible'] and gift['gift_type_id'] in supply_map:
            # Update the supply in the gift's collectible_data
            if isinstance(gift['collectible_data'], dict):
                gift['collectible_data']['supply'] = supply_map[gift['gift_type_id']]
    return gifts_list
# In app.py, add this new endpoint function.

@app.route('/api/public/gift_models', methods=['GET'])
def get_public_gift_models():
    """
    Provides a public list of models and their image URLs for a specific custom gift.
    The gift name is provided via a custom HTTP header.
    Requires API key authentication.
    """
    # 1. API Key Authentication (re-using the same secure pattern)
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization header is missing or invalid. Expected 'Bearer <API_KEY>'"}), 401
    
    token = auth_header.split(' ')[1]
    if not token or token != TRANSFER_API_KEY:
        return jsonify({"error": "Unauthorized: Invalid API Key"}), 401

    # 2. Get Gift Name from the Custom Header
    gift_name = request.headers.get('X-Gift-Name')
    if not gift_name:
        return jsonify({"error": "Required header 'X-Gift-Name' is missing."}), 400

    # 3. Look up the gift in the in-memory data (very fast)
    # The lookup is case-sensitive, matching the keys in CUSTOM_GIFTS_DATA
    gift_data = CUSTOM_GIFTS_DATA.get(gift_name)
    if not gift_data:
        return jsonify({"error": f"Gift with name '{gift_name}' not found."}), 404

    # 4. Check if the gift has models defined
    models_list = gift_data.get('models', [])
    if not models_list:
        # It's valid for a gift to have no models, so return an empty list.
        return jsonify([]), 200

    # 5. Format the response with the required fields
    response_data = []
    for model in models_list:
        # Ensure the model has both a name and an image before adding it
        if 'name' in model and 'image' in model:
            response_data.append({
                "name": model['name'],
                "image_url": model['image']
            })

    return jsonify(response_data), 200

@app.route('/api/public/available_custom_gifts', methods=['GET'])
def get_public_available_custom_gifts():
    """
    Provides a public list of available custom gifts and their stock status.
    Requires API key authentication.
    Excludes "Babuka" and sold-out items.
    """
    # 1. API Key Authentication
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization header is missing or invalid. Expected 'Bearer <API_KEY>'"}), 401
    
    token = auth_header.split(' ')[1]
    if not token or token != TRANSFER_API_KEY:
        return jsonify({"error": "Unauthorized: Invalid API Key"}), 401

    # 2. Database Connection
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed."}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # 3. Fetch all limited stock data in a single, efficient query
            cur.execute("SELECT gift_type_id, remaining_stock FROM limited_gifts_stock;")
            stock_map = {row['gift_type_id']: row['remaining_stock'] for row in cur.fetchall()}

        # 4. Process gift availability in memory (very fast)
        available_gifts = []
        for gift_name, gift_data in CUSTOM_GIFTS_DATA.items():
            # Exclude "Babuka" as requested
            #if gift_name == "Babuka":
            #    continue

            # Check if the gift is limited
            if 'limit' in gift_data:
                gift_type_id = gift_data['id']
                remaining = stock_map.get(gift_type_id, 0) # Use .get() for safety
                
                # Only include the gift if it's not sold out
                if remaining > 0:
                    available_gifts.append({
                        "name": gift_name,
                        "availability": f"{remaining} Left"
                    })
            else:
                # If 'limit' key is not present, it's unlimited
                available_gifts.append({
                    "name": gift_name,
                    "availability": "Unlimited"
                })
        
        return jsonify(available_gifts), 200

    except Exception as e:
        app.logger.error(f"Error in /api/public/available_custom_gifts: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            put_db_connection(conn)

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
            gift_type_id = next((g['id'] for g in CUSTOM_GIFTS_DATA.values() if g['name'] == gift_name), gift_name.replace(" ", ""))

            cur.execute("INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name) VALUES (%s, %s, %s, %s);", (new_instance_id, owner_id, gift_type_id, gift_name))
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
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
        
        return jsonify(cloned_gift), 201

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error cloning gift from {raw_input}: {e}", exc_info=True)
        return jsonify({"error": "An internal error occurred during cloning."}), 500
    finally:
        if conn: put_db_connection(conn)

# In app.py, replace the entire get_limited_gift_stock function

@app.route('/api/gifts/stock', methods=['GET'])
def get_limited_gift_stock():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # This single query fetches everything we need: total, remaining, and collectible counts.
            cur.execute("""
                SELECT 
                    s.gift_type_id, 
                    s.remaining_stock, 
                    s.total_stock,
                    COALESCE(c.collectible_count, 0) as collectible_count
                FROM limited_gifts_stock s
                LEFT JOIN (
                    SELECT gift_type_id, COUNT(*) as collectible_count
                    FROM gifts
                    WHERE is_collectible = TRUE
                    GROUP BY gift_type_id
                ) c ON s.gift_type_id = c.gift_type_id;
            """)
            stock_data = {
                row['gift_type_id']: {
                    'remaining': row['remaining_stock'], 
                    'total': row['total_stock'],
                    'collectible_count': row['collectible_count']
                } for row in cur.fetchall()
            }
            return jsonify(stock_data), 200
    except Exception as e:
        app.logger.error(f"Error fetching limited gift stock: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            put_db_connection(conn)

@app.route('/api/gift/<string:gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    viewer_id = request.args.get('viewer_id')
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar FROM gifts g JOIN accounts a ON g.owner_id = a.tg_id WHERE LOWER(g.gift_type_id) = LOWER(%s) AND g.collectible_number = %s AND g.is_collectible = TRUE;""", (gift_type_id, collectible_number))
            gift_data = cur.fetchone()
            if not gift_data: return jsonify({"error": "Collectible gift not found."}), 404

            if is_custom_gift(gift_data['gift_name']):
                if not has_custom_gifts_enabled(cur, viewer_id):
                    return jsonify({"error": "Sorry, you cannot see this gift.", "reason": "custom_content_disabled"}), 403

            result = dict(gift_data)
            if isinstance(result.get('collectible_data'), str): result['collectible_data'] = json.loads(result.get('collectible_data'))
            return jsonify(result), 200
    except Exception as e:
        app.logger.error(f"Error fetching deep-linked gift {gift_type_id}-{collectible_number}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/gifts/<string:instance_id>', methods=['PUT'])
def update_gift_state(instance_id):
    data = request.get_json()
    action = data.get('action')
    value = data.get('value')

    if action not in ['pin', 'hide', 'wear', 'sell'] or not isinstance(value, bool):
        return jsonify({"error": "Invalid action or value"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor() as cur:
            # --- ATOMIC WEAR LOGIC (BUG FIX #5) ---
            if action == 'wear' and value is True:
                # First, get the owner_id of the gift being worn
                cur.execute("SELECT owner_id FROM gifts WHERE instance_id = %s;", (instance_id,))
                owner_id_result = cur.fetchone()
                if not owner_id_result:
                    return jsonify({"error": "Gift not found for wear action."}), 404
                owner_id = owner_id_result[0]
                # Then, un-wear all other gifts for that owner
                cur.execute("UPDATE gifts SET is_worn = FALSE WHERE owner_id = %s AND is_worn = TRUE;", (owner_id,))
                # Finally, wear the new gift
                cur.execute("UPDATE gifts SET is_worn = TRUE WHERE instance_id = %s;", (instance_id,))

            # --- SELL/UNLIST LOGIC ---
            elif action == 'sell':
                if value is True: # Listing for sale
                    price = data.get('price')
                    if price is None or not isinstance(price, int) or not (MIN_SALE_PRICE <= price <= MAX_SALE_PRICE):
                        return jsonify({"error": f"Price must be a number between {MIN_SALE_PRICE} and {MAX_SALE_PRICE}."}), 400
                    # When listing, also unpin and unwear it
                    cur.execute("UPDATE gifts SET is_on_sale = TRUE, sale_price = %s, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s;", (price, instance_id))
                else: # Canceling a sale (unlisting)
                    cur.execute("UPDATE gifts SET is_on_sale = FALSE, sale_price = NULL WHERE instance_id = %s;", (instance_id,))
            
            # --- OTHER ACTIONS ---
            else:
                column_to_update = {'pin': 'is_pinned', 'hide': 'is_hidden', 'wear': 'is_worn'}[action]
                # If hiding a gift, also unpin and unwear it
                if action == 'hide' and value is True:
                    cur.execute("UPDATE gifts SET is_hidden = TRUE, is_pinned = FALSE, is_worn = FALSE, pin_order = NULL WHERE instance_id = %s;", (instance_id,))
                # If unpinning, clear the order
                elif action == 'pin' and value is False:
                     cur.execute("UPDATE gifts SET is_pinned = FALSE, pin_order = NULL WHERE instance_id = %s;", (instance_id,))
                else:
                    # Generic update for other simple toggles (un-hiding, pinning)
                    cur.execute(f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;", (value, instance_id))

            if cur.rowcount == 0:
                # This might happen if the state is already set, which is not an error.
                # We only return an error if the initial check for 'wear' fails.
                app.logger.warning(f"Update for gift {instance_id} action '{action}' resulted in 0 rows affected. State might have been unchanged.")

            conn.commit()
            return jsonify({"message": f"Gift {action} state updated"}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"DB error updating gift state for {instance_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/gifts/<string:instance_id>', methods=['DELETE'])
def delete_gift(instance_id):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM gift_collections WHERE gift_instance_id = %s;", (instance_id,))
            cur.execute("DELETE FROM post_reactions WHERE post_id IN (SELECT id FROM posts WHERE content LIKE %s);", (f'%{instance_id}%',))
            cur.execute("DELETE FROM gifts WHERE instance_id = %s;", (instance_id,))
            if cur.rowcount == 0: 
                conn.rollback()
                return jsonify({"error": "Gift not found."}), 404
            conn.commit()
            return jsonify({"message": "Gift deleted"}), 204
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"DB error deleting gift {instance_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

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

    try:
        with conn.cursor() as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error selling gift {instance_id} for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/gifts/reorder', methods=['POST'])
def reorder_pinned_gifts():
    data = request.get_json()
    owner_id = data.get('owner_id')
    ordered_ids = data.get('ordered_instance_ids')

    if not owner_id or not isinstance(ordered_ids, list):
        return jsonify({"error": "owner_id and ordered_instance_ids list are required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500

    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE gifts SET pin_order = NULL WHERE owner_id = %s AND is_pinned = TRUE;", (owner_id,))
            for index, instance_id in enumerate(ordered_ids):
                cur.execute("""
                    UPDATE gifts SET pin_order = %s 
                    WHERE instance_id = %s AND owner_id = %s AND is_pinned = TRUE;
                """, (index, instance_id, owner_id))
            conn.commit()
            return jsonify({"message": "Pinned gifts reordered successfully."}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error reordering pinned gifts for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error during batch action '{action}' for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)


@app.route('/api/posts/<int:post_id>', methods=['DELETE'])
def delete_post(post_id):
    # In a real app, you'd verify ownership via a session or JWT token.
    # Here we'll trust the owner_id sent from the frontend for simplicity.
    data = request.get_json()
    owner_id = data.get('owner_id')

    if not owner_id:
        return jsonify({"error": "owner_id is required"}), 400

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    try:
        with conn.cursor() as cur:
            # First, delete associated reactions to maintain data integrity
            cur.execute("DELETE FROM post_reactions WHERE post_id = %s;", (post_id,))
            # Then, delete the post, ensuring the user owns it
            cur.execute("DELETE FROM posts WHERE id = %s AND owner_id = %s;", (post_id, owner_id))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Post not found or you are not the owner."}), 404
            
            conn.commit()
            return jsonify({"message": "Post deleted successfully."}), 204
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error deleting post {post_id} for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
            if cur.rowcount == 0: 
                conn.rollback()
                return jsonify({"error": "Gift not found or could not be transferred."}), 404
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
        if conn: conn.rollback()
        app.logger.error(f"Error during gift transfer of {instance_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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

@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    owner_id = data.get('owner_id')
    content = data.get('content')

    if not owner_id or not content:
        return jsonify({"error": "owner_id and content are required"}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("INSERT INTO posts (owner_id, content) VALUES (%s, %s) RETURNING *;", (owner_id, content))
            new_post_data = cur.fetchone()
            conn.commit()
            new_post = dict(new_post_data)
            new_post['reactions'] = {} # New posts have no reactions yet

            # Handle notifications for mentions and new posts
            cur.execute("SELECT username FROM accounts WHERE tg_id = %s;", (owner_id,))
            poster_username = cur.fetchone()['username']
            
            # New post notifications
            cur.execute("SELECT subscriber_id FROM user_subscriptions WHERE target_user_id = %s AND notification_type = 'new_posts';", (owner_id,))
            for row in cur.fetchall():
                send_telegram_message(row['subscriber_id'], f"🔔 @{poster_username} has a new post on their wall! Come check it out.") # Add a button later

            # Mention notifications
            mentioned_users = set(re.findall(r'@([a-zA-Z0-9_]{5,32})', content))
            for username in mentioned_users:
                cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (username,))
                mentioned_user = cur.fetchone()
                if mentioned_user:
                     # Check if the mentioned user is subscribed to the poster for mentions
                    cur.execute("SELECT subscriber_id FROM user_subscriptions WHERE target_user_id = %s AND notification_type = 'mentions' AND subscriber_id = %s;", (owner_id, mentioned_user['tg_id']))
                    if cur.fetchone():
                        send_telegram_message(mentioned_user['tg_id'], f"🔔 You were mentioned on @{poster_username}'s wall!")

            return jsonify(new_post), 201
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error creating post for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/posts/<int:post_id>/view', methods=['POST'])
def increment_post_view(post_id):
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE posts SET views = views + 1 WHERE id = %s;", (post_id,))
            conn.commit()
            return jsonify({"message": "View count incremented"}), 200
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error incrementing view for post {post_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/collectible_usernames', methods=['POST'])
def add_collectible_username():
    data = request.get_json(); owner_id, username = data.get('owner_id'), data.get('username')
    if not owner_id or not username: return jsonify({"error": "owner_id and username are required"}), 400
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM collectible_usernames WHERE LOWER(username) = LOWER(%s);", (username,))
            if cur.fetchone(): return jsonify({"error": f"Username @{username} is already taken."}), 409
            cur.execute("SELECT COUNT(*) FROM collectible_usernames WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= MAX_COLLECTIBLE_USERNAMES: return jsonify({"error": f"Username limit of {MAX_COLLECTIBLE_USERNAMES} reached."}), 403
            cur.execute("""INSERT INTO collectible_usernames (owner_id, username) VALUES (%s, %s);""", (owner_id, username))
            conn.commit()
            return jsonify({"message": "Username added"}), 201
    except psycopg2.IntegrityError:
        if conn: conn.rollback()
        app.logger.warning(f"Integrity error adding username {username}.", exc_info=True)
        return jsonify({"error": f"Username @{username} is already taken."}), 409
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error adding username {username}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/collectible_usernames/<string:username>', methods=['DELETE'])
def delete_collectible_username(username):
    data = request.get_json(); owner_id = data.get('owner_id')
    if not owner_id: return jsonify({"error": "owner_id is required"}), 400
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor() as cur:
            cur.execute("""DELETE FROM collectible_usernames WHERE LOWER(username) = LOWER(%s) AND owner_id = %s;""", (username, owner_id))
            if cur.rowcount == 0: 
                conn.rollback()
                return jsonify({"error": "Username not found for this user."}), 404
            conn.commit()
            return jsonify({"message": "Username deleted"}), 204
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"DB error deleting username {username}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: put_db_connection(conn)

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
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error creating giveaway for user {creator_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/collections', methods=['POST'])
def create_collection():
    data = request.get_json()
    owner_id = data.get('owner_id')
    name = data.get('name')
    if not all([owner_id, name]):
        return jsonify({"error": "owner_id and name are required."}), 400
    
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM collections WHERE owner_id = %s;", (owner_id,))
            if cur.fetchone()[0] >= MAX_COLLECTIONS_PER_USER:
                return jsonify({"error": f"Collection limit of {MAX_COLLECTIONS_PER_USER} reached."}), 403

            cur.execute("INSERT INTO collections (owner_id, name) VALUES (%s, %s) RETURNING id, name;", (owner_id, name))
            new_collection = cur.fetchone()
            conn.commit()
            return jsonify(dict(new_collection)), 201
    except psycopg2.IntegrityError:
        if conn: conn.rollback()
        return jsonify({"error": "A collection with this name already exists."}), 409
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Error creating collection for user {owner_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/collections/<int:collection_id>/gifts', methods=['POST'])
def add_gifts_to_collection(collection_id):
    data = request.get_json()
    instance_ids = data.get('instance_ids')
    owner_id = data.get('owner_id')
    if not all([instance_ids, owner_id]) or not isinstance(instance_ids, list):
        return jsonify({"error": "owner_id and a list of instance_ids are required."}), 400
        
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database failed"}), 500
    try:
        with conn.cursor() as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error adding gifts to collection {collection_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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
    try:
        with conn.cursor() as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error reordering in collection {collection_id}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/stats', methods=['GET'])
def get_stats_ultimate():
    conn = get_db_connection()
    if not conn: return jsonify({"error": "Database connection failed."}), 500
    stats = {}
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
    finally:
        if conn: put_db_connection(conn)
            
@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    app.logger.warning(f"Unhandled API call: {request.method} /api/{path}")
    return jsonify({"error": f"The requested API endpoint '/api/{path}' was not found or the method is not allowed."}), 404

# --- GIVEAWAY WORKERS ---
def process_giveaway_winners(giveaway_id):
    app.logger.info(f"Processing winners for giveaway ID: {giveaway_id}")
    conn = get_db_connection()
    if not conn: return

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error processing giveaway {giveaway_id}: {e}", exc_info=True)
        if giveaway:
            send_telegram_message(giveaway['creator_id'], f"An error occurred while processing your giveaway. The bot might not have access to post in the provided channel ID.")
    finally:
        if conn: put_db_connection(conn)

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
        if conn: put_db_connection(conn)


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
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT end_date FROM giveaways WHERE status = 'active' ORDER BY end_date ASC LIMIT 1;")
                    result = cur.fetchone()
                    if result:
                        next_giveaway_end_date = result[0]
            finally:
                if conn: put_db_connection(conn)

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
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error during API gift transfer of {gift_name_and_number}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error in create_and_transfer_random_gift: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

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
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
        if conn: conn.rollback()
        app.logger.error(f"Error in create_and_transfer_custom_gift: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

@app.route('/api/user_data/<string:username>', methods=['GET'])
def get_user_data_by_username(username):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization header is missing or invalid"}), 401
    
    token = auth_header.split(' ')[1]
    if not token or token != TRANSFER_API_KEY:
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

            cur.execute("""
                SELECT * FROM gifts WHERE owner_id = %s
                ORDER BY is_pinned DESC, pin_order ASC NULLS LAST, acquired_date DESC;
            """, (user_id,))
            
            gifts = []
            for row in cur.fetchall():
                gift_dict = dict(row)
                if gift_dict.get('collectible_data') and isinstance(gift_dict.get('collectible_data'), str):
                    try:
                        gift_dict['collectible_data'] = json.loads(gift_dict['collectible_data'])
                    except json.JSONDecodeError:
                        app.logger.warning(f"Could not parse collectible_data for gift {gift_dict['instance_id']}")
                        gift_dict['collectible_data'] = None
                gifts.append(gift_dict)

            response_data = {
                "profile": dict(user_profile),
                "gifts": gifts
            }
            return jsonify(response_data), 200

    except Exception as e:
        app.logger.error(f"Error fetching user data for {username}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn: put_db_connection(conn)

# --- APP STARTUP & MAIN ---
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
