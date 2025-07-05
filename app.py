import os
import psycopg2
import json
import random
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib.parse import urlparse

# --- CONFIGURATION ---

app = Flask(__name__)

# Configure CORS to only allow requests from your frontend's origin
# This is crucial for security.
CORS(app, resources={r"/api/*": {"origins": "https://vasiliy-katsyka.github.io"}})

# Get the database URL from environment variables for security
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL set for Flask application")

# --- CONSTANTS ---
GIFT_LIMIT_PER_USER = 5000
CDN_BASE_URL = "https://cdn.changes.tg/gifts/"

# --- DATABASE HELPERS ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to database: {e}")
        return None

def init_db():
    """Initializes the database by creating necessary tables if they don't exist."""
    conn = get_db_connection()
    if not conn:
        print("Database connection failed, skipping initialization.")
        return
        
    with conn.cursor() as cur:
        # Accounts table to store user info
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
        
        # Gifts table for every single gift instance
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gifts (
                instance_id VARCHAR(50) PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                gift_type_id VARCHAR(255) NOT NULL, -- e.g., 'PlushPepe'
                gift_name VARCHAR(255) NOT NULL,
                original_image_url TEXT,
                lottie_path TEXT,
                is_collectible BOOLEAN DEFAULT FALSE,
                collectible_data JSONB, -- Stores model, backdrop, pattern, etc.
                collectible_number INT,
                acquired_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_hidden BOOLEAN DEFAULT FALSE,
                is_pinned BOOLEAN DEFAULT FALSE,
                is_worn BOOLEAN DEFAULT FALSE
            );
        """)
        # Index for faster lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_owner_id ON gifts (owner_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gifts_type_and_number ON gifts (gift_type_id, collectible_number);")

        # Collectible Usernames table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collectible_usernames (
                id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES accounts(tg_id) ON DELETE CASCADE,
                username VARCHAR(255) UNIQUE NOT NULL
            );
        """)
    
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

# --- UTILITY FUNCTIONS ---

def select_weighted_random(items):
    """Selects an item from a list based on 'rarityPermille' weight."""
    if not items: return None
    total_weight = sum(item.get('rarityPermille', 1) for item in items)
    if total_weight == 0: return random.choice(items)
    
    random_num = random.uniform(0, total_weight)
    for item in items:
        weight = item.get('rarityPermille', 1)
        if random_num < weight:
            return item
        random_num -= weight
    return items[-1] # Fallback

def fetch_collectible_parts(gift_name):
    """Fetches collectible parts (models, backdrops, patterns) from the CDN."""
    gift_name_encoded = requests.utils.quote(gift_name)
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
        except (requests.RequestException, json.JSONDecodeError):
            parts[part_type] = []
    return parts


# --- API ROUTES ---

@app.route('/api/account', methods=['POST'])
def get_or_create_account():
    """
    Handles user login. Creates an account if it doesn't exist,
    then returns the full account data including gifts.
    """
    data = request.get_json()
    if not data or 'tg_id' not in data:
        return jsonify({"error": "Missing tg_id"}), 400

    tg_id = data['tg_id']
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Check if user exists
        cur.execute("SELECT * FROM accounts WHERE tg_id = %s;", (tg_id,))
        account = cur.fetchone()

        # If not, create a new account
        if not account:
            cur.execute("""
                INSERT INTO accounts (tg_id, username, full_name, avatar_url, bio, phone_number)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                tg_id,
                data.get('username'),
                data.get('full_name'),
                data.get('avatar_url'),
                'My first account!',
                'Not specified'
            ))
            conn.commit()

        # Fetch full account data
        cur.execute("SELECT tg_id, username, full_name, avatar_url, bio, phone_number FROM accounts WHERE tg_id = %s;", (tg_id,))
        account_data = dict(zip([d[0] for d in cur.description], cur.fetchone()))

        # Fetch owned gifts
        cur.execute("SELECT * FROM gifts WHERE owner_id = %s ORDER BY acquired_date DESC;", (tg_id,))
        gifts = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        # Fetch collectible usernames
        cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
        usernames = [row[0] for row in cur.fetchall()]
        
        account_data['owned_gifts'] = gifts
        account_data['collectible_usernames'] = usernames

    conn.close()
    return jsonify(account_data), 200


@app.route('/api/gifts', methods=['POST'])
def buy_gift():
    """Buys one or more non-collectible gifts for a user."""
    data = request.get_json()
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'lottie_path', 'instance_id']
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required gift data"}), 400

    owner_id = data['owner_id']
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Enforce gift limit
        cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
        gift_count = cur.fetchone()[0]
        if gift_count >= GIFT_LIMIT_PER_USER:
            conn.close()
            return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached"}), 403

        # Insert new gift
        cur.execute("""
            INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            data['instance_id'], owner_id, data['gift_type_id'], data['gift_name'],
            data['original_image_url'], data['lottie_path']
        ))
        conn.commit()
    conn.close()
    return jsonify({"message": "Gift purchased successfully"}), 201


@app.route('/api/gifts/upgrade', methods=['POST'])
def upgrade_gift():
    """Upgrades a non-collectible gift to a collectible one."""
    data = request.get_json()
    if 'instance_id' not in data:
        return jsonify({"error": "instance_id is required"}), 400
    
    instance_id = data['instance_id']
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Fetch the gift to upgrade
        cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
        gift = cur.fetchone()
        if not gift:
            conn.close()
            return jsonify({"error": "Gift not found or already a collectible"}), 404
        
        owner_id, gift_type_id, gift_name = gift

        # Get next collectible number for this gift type
        cur.execute("SELECT MAX(collectible_number) FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
        max_number = cur.fetchone()[0]
        next_number = (max_number or 0) + 1

        # Fetch parts and generate random collectible data
        parts = fetch_collectible_parts(gift_name)
        selected_model = select_weighted_random(parts.get('models', []))
        selected_backdrop = select_weighted_random(parts.get('backdrops', []))
        selected_pattern = select_weighted_random(parts.get('patterns', []))

        if not all([selected_model, selected_backdrop, selected_pattern]):
            conn.close()
            return jsonify({"error": f"Could not fetch all collectible parts for {gift_name}"}), 500

        collectible_data = {
            "model": selected_model,
            "backdrop": selected_backdrop,
            "pattern": selected_pattern,
            "modelImage": f"{CDN_BASE_URL}models/{requests.utils.quote(gift_name)}/png/{requests.utils.quote(selected_model['name'])}.png",
            "lottieModelPath": f"{CDN_BASE_URL}models/{requests.utils.quote(gift_name)}/lottie/{requests.utils.quote(selected_model['name'])}.json",
            "patternImage": f"{CDN_BASE_URL}patterns/{requests.utils.quote(gift_name)}/png/{requests.utils.quote(selected_pattern['name'])}.png",
            "backdropColors": selected_backdrop.get('hex'),
        }

        # Update the gift record in the database
        cur.execute("""
            UPDATE gifts
            SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s, lottie_path = NULL
            WHERE instance_id = %s;
        """, (json.dumps(collectible_data), next_number, instance_id))
        conn.commit()

        # Fetch the newly upgraded gift to return to frontend
        cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (instance_id,))
        upgraded_gift = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        
    conn.close()
    return jsonify(upgraded_gift), 200


@app.route('/api/gifts/<instance_id>', methods=['PUT'])
def update_gift_state(instance_id):
    """Updates a gift's state (pin, hide, wear)."""
    data = request.get_json()
    action = data.get('action')
    value = data.get('value')

    if action not in ['pin', 'hide', 'wear'] or not isinstance(value, bool):
        return jsonify({"error": "Invalid action or value"}), 400

    column_map = {'pin': 'is_pinned', 'hide': 'is_hidden', 'wear': 'is_worn'}
    column_to_update = column_map[action]

    conn = get_db_connection()
    with conn.cursor() as cur:
        # If wearing a gift, un-wear all others for that user first
        if action == 'wear' and value is True:
            cur.execute("SELECT owner_id FROM gifts WHERE instance_id = %s;", (instance_id,))
            owner_id = cur.fetchone()[0]
            cur.execute("UPDATE gifts SET is_worn = FALSE WHERE owner_id = %s AND is_worn = TRUE;", (owner_id,))

        # Update the target gift
        cur.execute(f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;", (value, instance_id))
        conn.commit()
    conn.close()
    return jsonify({"message": f"Gift {action} state updated"}), 200


@app.route('/api/gifts/transfer', methods=['POST'])
def transfer_gift():
    """Transfers a collectible gift from one user to another by username."""
    data = request.get_json()
    if not data or 'instance_id' not in data or 'receiver_username' not in data:
        return jsonify({"error": "Missing instance_id or receiver_username"}), 400

    instance_id = data['instance_id']
    receiver_username = data['receiver_username'].lstrip('@')
    
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Find the receiver's account
        cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
        receiver = cur.fetchone()
        if not receiver:
            conn.close()
            return jsonify({"error": "Receiver username not found"}), 404
        
        receiver_id = receiver[0]

        # Check receiver's gift limit
        cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
        gift_count = cur.fetchone()[0]
        if gift_count >= GIFT_LIMIT_PER_USER:
            conn.close()
            return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached"}), 403
            
        # Update gift owner and reset its state
        cur.execute("""
            UPDATE gifts
            SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE
            WHERE instance_id = %s AND is_collectible = TRUE;
        """, (receiver_id, instance_id))
        
        if cur.rowcount == 0:
            conn.close()
            return jsonify({"error": "Gift not found, is not a collectible, or could not be transferred"}), 404

        conn.commit()
    conn.close()
    return jsonify({"message": "Gift transferred successfully"}), 200


@app.route('/api/gift/<gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    """Fetches a specific collectible gift by its type and number for deep linking."""
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.*, a.username as owner_username, a.full_name as owner_name
            FROM gifts g
            JOIN accounts a ON g.owner_id = a.tg_id
            WHERE g.gift_type_id = %s AND g.collectible_number = %s AND g.is_collectible = TRUE;
        """, (gift_type_id, collectible_number))
        
        gift_data = cur.fetchone()
        if not gift_data:
            conn.close()
            return jsonify({"error": "Collectible gift not found"}), 404
        
        result = dict(zip([d[0] for d in cur.description], gift_data))
    
    conn.close()
    return jsonify(result), 200

# Fallback route for undefined API calls
@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    return jsonify({"error": f"The requested API endpoint '/api/{path}' was not found."}), 404


if __name__ == '__main__':
    # This block will run when the script is executed directly.
    # On Render, the web server (like Gunicorn) will import the 'app' object,
    # so this block won't run. It's useful for local development.
    print("Starting Flask server for local development...")
    init_db() # Initialize DB on local startup
    app.run(debug=True, port=5001)