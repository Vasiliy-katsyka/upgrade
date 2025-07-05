import os
import psycopg2
import json
import random
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib.parse import quote # Changed from quote_plus to quote

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
GIFT_LIMIT_PER_USER = 5000 # This limit is now primarily enforced by the backend
CDN_BASE_URL = "https://cdn.changes.tg/gifts/"
MAX_COLLECTIBLE_USERNAMES = 10 # Backend enforces this limit too

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
    """Initializes the database by creating necessary tables if they don't exist."""
    conn = get_db_connection()
    if not conn:
        app.logger.warning("Database connection failed during initialization.")
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
    app.logger.info("Database initialized successfully.")

# --- UTILITY FUNCTIONS ---

def select_weighted_random(items):
    """Selects an item from a list based on 'rarityPermille' weight."""
    if not items: return None
    total_weight = sum(item.get('rarityPermille', 1) for item in items)
    if total_weight == 0: 
        # If all weights are 0, or list is non-empty but all weights 0
        return random.choice(items) if items else None 
    
    random_num = random.uniform(0, total_weight)
    for item in items:
        weight = item.get('rarityPermille', 1)
        if random_num < weight:
            return item
        random_num -= weight
    return items[-1] # Fallback to last item if precision issues (very rare)

def fetch_collectible_parts(gift_name):
    """Fetches collectible parts (models, backdrops, patterns) from the CDN."""
    gift_name_encoded = quote(gift_name) # Changed from quote_plus
    urls = {
        "models": f"{CDN_BASE_URL}models/{gift_name_encoded}/models.json",
        "backdrops": f"{CDN_BASE_URL}backdrops/{gift_name_encoded}/backdrops.json",
        "patterns": f"{CDN_BASE_URL}patterns/{gift_name_encoded}/patterns.json"
    }
    parts = {}
    for part_type, url in urls.items():
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            parts[part_type] = response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            app.logger.warning(f"Could not fetch {part_type} from {url}: {e}")
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
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
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
            # Convert fetched rows to dictionaries
            gifts = []
            for row in cur.fetchall():
                gift_dict = dict(zip([d[0] for d in cur.description], row))
                # Ensure collectible_data is treated as a JSON object, not string/None
                if gift_dict.get('collectible_data') is not None and isinstance(gift_dict.get('collectible_data'), str):
                    try:
                        gift_dict['collectible_data'] = json.loads(gift_dict['collectible_data'])
                    except json.JSONDecodeError:
                        app.logger.error(f"Failed to decode collectible_data for gift {gift_dict.get('instance_id')}: {gift_dict.get('collectible_data')}", exc_info=True)
                        gift_dict['collectible_data'] = None # Or handle as corrupted
                gifts.append(gift_dict)
            account_data['owned_gifts'] = gifts

            # Fetch collectible usernames
            cur.execute("SELECT username FROM collectible_usernames WHERE owner_id = %s;", (tg_id,))
            usernames = [row[0] for row in cur.fetchall()]
            
            account_data['collectible_usernames'] = usernames

            return jsonify(account_data), 200
        except Exception as e:
            conn.rollback() # Rollback any partial changes
            app.logger.error(f"Error in get_or_create_account for tg_id {tg_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred: {e}"}), 500
        finally:
            conn.close()

@app.route('/api/account', methods=['PUT'])
def update_account():
    """Updates an existing account's data."""
    data = request.get_json()
    if not data or 'tg_id' not in data:
        return jsonify({"error": "Missing tg_id"}), 400

    tg_id = data['tg_id']
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        update_fields = []
        update_values = []

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
        if 'phone_number' in data:
            update_fields.append("phone_number = %s")
            update_values.append(data['phone_number'])
        
        if not update_fields:
            conn.close()
            return jsonify({"error": "No fields provided for update"}), 400

        update_query = f"UPDATE accounts SET {', '.join(update_fields)} WHERE tg_id = %s;"
        update_values.append(tg_id)

        try:
            cur.execute(update_query, tuple(update_values))
            if cur.rowcount == 0:
                conn.close()
                return jsonify({"error": "Account not found"}), 404
            conn.commit()
            return jsonify({"message": "Account updated successfully"}), 200
        except psycopg2.Error as e:
            conn.rollback()
            app.logger.error(f"Database error updating account {tg_id}: {e}", exc_info=True)
            return jsonify({"error": f"Database error: {e}"}), 500
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Unexpected error updating account {tg_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gifts', methods=['POST'])
def add_gift():
    """Adds one or more gifts for a user. These are initially non-collectible."""
    data = request.get_json()
    required_fields = ['owner_id', 'gift_type_id', 'gift_name', 'original_image_url', 'lottie_path', 'instance_id']
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required gift data"}), 400

    owner_id = data['owner_id']
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            # Enforce gift limit
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (owner_id,))
            gift_count = cur.fetchone()[0]
            if gift_count >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Gift limit of {GIFT_LIMIT_PER_USER} reached for this account."}), 403

            # Insert new gift
            cur.execute("""
                INSERT INTO gifts (instance_id, owner_id, gift_type_id, gift_name, original_image_url, lottie_path)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                data['instance_id'], owner_id, data['gift_type_id'], data['gift_name'],
                data['original_image_url'], data['lottie_path']
            ))
            conn.commit()
            return jsonify({"message": "Gift added successfully"}), 201
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error adding gift for owner {owner_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gifts/upgrade', methods=['POST'])
def upgrade_gift():
    """Upgrades a non-collectible gift to a collectible one.
    Can optionally use custom parts provided by the frontend (for 'Create Gift' feature)."""
    data = request.get_json()
    if 'instance_id' not in data:
        return jsonify({"error": "instance_id is required"}), 400
    
    instance_id = data['instance_id']
    
    # Optional custom parts for custom gift creation
    custom_model_data = data.get('custom_model')
    custom_backdrop_data = data.get('custom_backdrop')
    custom_pattern_data = data.get('custom_pattern')

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            # Fetch the gift to upgrade
            cur.execute("SELECT owner_id, gift_type_id, gift_name FROM gifts WHERE instance_id = %s AND is_collectible = FALSE;", (instance_id,))
            gift_row = cur.fetchone()
            if not gift_row:
                return jsonify({"error": "Gift not found or already a collectible."}), 404
            
            owner_id, gift_type_id, gift_name = gift_row

            # Get next collectible number for this gift type
            # COALESCE handles case where no collectibles of this type exist yet (returns 0 instead of NULL)
            cur.execute("SELECT COALESCE(MAX(collectible_number), 0) + 1 FROM gifts WHERE gift_type_id = %s;", (gift_type_id,))
            next_number = cur.fetchone()[0]

            # Determine parts: use custom if provided, otherwise fetch and select randomly
            selected_model = None
            selected_backdrop = None
            selected_pattern = None
            
            # Always fetch parts data to get all options (even if custom is provided, to get rarity, etc.)
            parts_data = fetch_collectible_parts(gift_name)

            if custom_model_data:
                selected_model = custom_model_data
            else:
                selected_model = select_weighted_random(parts_data.get('models', []))

            if custom_backdrop_data:
                selected_backdrop = custom_backdrop_data
            else:
                selected_backdrop = select_weighted_random(parts_data.get('backdrops', []))

            if custom_pattern_data:
                selected_pattern = custom_pattern_data
            else:
                selected_pattern = select_weighted_random(parts_data.get('patterns', []))

            if not all([selected_model, selected_backdrop, selected_pattern]):
                return jsonify({"error": f"Could not determine all collectible parts for gift '{gift_name}'. Missing data on CDN or no valid parts found."}), 500
            
            # Assign a random supply for the collectible (can be made deterministic based on collection type if needed)
            supply = random.randint(2000, 10000) 

            collectible_data = {
                "model": selected_model,
                "backdrop": selected_backdrop,
                "pattern": selected_pattern,
                "modelImage": f"{CDN_BASE_URL}models/{quote(gift_name)}/png/{quote(selected_model['name'])}.png", # Changed from quote_plus
                "lottieModelPath": f"{CDN_BASE_URL}models/{quote(gift_name)}/lottie/{quote(selected_model['name'])}.json", # Changed from quote_plus
                "patternImage": f"{CDN_BASE_URL}patterns/{quote(gift_name)}/png/{quote(selected_pattern['name'])}.png", # Changed from quote_plus
                "backdropColors": selected_backdrop.get('hex'),
                "supply": supply # Add supply to collectible_data
            }

            # Update the gift record in the database
            cur.execute("""
                UPDATE gifts
                SET is_collectible = TRUE, collectible_data = %s, collectible_number = %s, lottie_path = NULL
                WHERE instance_id = %s;
            """, (json.dumps(collectible_data), next_number, instance_id))
            
            if cur.rowcount == 0:
                conn.rollback() # Important to rollback if no row was updated
                return jsonify({"error": "Failed to update gift. It might have been deleted or changed."}), 404

            conn.commit()

            # Fetch the newly upgraded gift to return to frontend
            cur.execute("SELECT * FROM gifts WHERE instance_id = %s;", (instance_id,))
            upgraded_gift = dict(zip([d[0] for d in cur.description], cur.fetchone()))
            # Ensure collectible_data is returned as a JSON object
            if upgraded_gift.get('collectible_data') is not None and isinstance(upgraded_gift.get('collectible_data'), str):
                try:
                    upgraded_gift['collectible_data'] = json.loads(upgraded_gift['collectible_data'])
                except json.JSONDecodeError:
                    app.logger.error(f"Failed to decode collectible_data for gift {upgraded_gift.get('instance_id')}: {upgraded_gift.get('collectible_data')}", exc_info=True)
                    upgraded_gift['collectible_data'] = None
            
            return jsonify(upgraded_gift), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error upgrading gift {instance_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred during upgrade: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gifts/<string:instance_id>', methods=['PUT'])
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
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            # If wearing a gift, un-wear all others for that user first
            if action == 'wear' and value is True:
                cur.execute("SELECT owner_id FROM gifts WHERE instance_id = %s;", (instance_id,))
                owner_id_result = cur.fetchone()
                if not owner_id_result: # Gift does not exist
                    return jsonify({"error": "Gift not found for wear action."}), 404
                owner_id = owner_id_result[0]
                cur.execute("UPDATE gifts SET is_worn = FALSE WHERE owner_id = %s AND is_worn = TRUE;", (owner_id,))

            # Update the target gift
            cur.execute(f"UPDATE gifts SET {column_to_update} = %s WHERE instance_id = %s;", (value, instance_id))
            
            if cur.rowcount == 0:
                conn.rollback() # Rollback if no row was updated
                return jsonify({"error": "Gift not found or state not changed."}), 404

            conn.commit()
            return jsonify({"message": f"Gift {action} state updated"}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Database error updating gift state for {instance_id}, action {action}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred while updating gift state: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gifts/<string:instance_id>', methods=['DELETE'])
def delete_gift(instance_id):
    """Deletes a gift instance."""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM gifts WHERE instance_id = %s;", (instance_id,))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Gift not found or could not be deleted."}), 404
            
            conn.commit()
            return jsonify({"message": "Gift deleted successfully"}), 204 # 204 No Content
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Database error deleting gift {instance_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred while deleting gift: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gifts/transfer', methods=['POST'])
def transfer_gift():
    """Transfers a collectible gift from one user to another by username."""
    data = request.get_json()
    if not data or 'instance_id' not in data or 'receiver_username' not in data:
        return jsonify({"error": "Missing instance_id or receiver_username"}), 400

    instance_id = data['instance_id']
    receiver_username = data['receiver_username'].lstrip('@')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            # Find the receiver's account
            cur.execute("SELECT tg_id FROM accounts WHERE username = %s;", (receiver_username,))
            receiver = cur.fetchone()
            if not receiver:
                return jsonify({"error": "Receiver username not found."}), 404
            
            receiver_id = receiver[0]

            # Check receiver's gift limit
            cur.execute("SELECT COUNT(*) FROM gifts WHERE owner_id = %s;", (receiver_id,))
            gift_count = cur.fetchone()[0]
            if gift_count >= GIFT_LIMIT_PER_USER:
                return jsonify({"error": f"Receiver's gift limit of {GIFT_LIMIT_PER_USER} reached."}), 403
                
            # Update gift owner and reset its state (pinned, worn)
            cur.execute("""
                UPDATE gifts
                SET owner_id = %s, is_pinned = FALSE, is_worn = FALSE
                WHERE instance_id = %s AND is_collectible = TRUE;
            """, (receiver_id, instance_id))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Gift not found, is not a collectible, or could not be transferred."}), 404

            conn.commit()
            return jsonify({"message": "Gift transferred successfully"}), 200
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Database error during gift transfer of {instance_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred during transfer: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/gift/<string:gift_type_id>/<int:collectible_number>', methods=['GET'])
def get_gift_by_details(gift_type_id, collectible_number):
    """Fetches a specific collectible gift by its type and number for deep linking."""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT g.*, a.username as owner_username, a.full_name as owner_name, a.avatar_url as owner_avatar
                FROM gifts g
                JOIN accounts a ON g.owner_id = a.tg_id
                WHERE g.gift_type_id = %s AND g.collectible_number = %s AND g.is_collectible = TRUE;
            """, (gift_type_id, collectible_number))
            
            gift_data = cur.fetchone()
            if not gift_data:
                return jsonify({"error": "Collectible gift not found."}), 404
            
            result = dict(zip([d[0] for d in cur.description], gift_data))
            # Ensure collectible_data is returned as a JSON object
            if result.get('collectible_data') is not None and isinstance(result.get('collectible_data'), str):
                try:
                    result['collectible_data'] = json.loads(result['collectible_data'])
                except json.JSONDecodeError:
                    app.logger.error(f"Failed to decode collectible_data for gift {result.get('instance_id')}: {result.get('collectible_data')}", exc_info=True)
                    result['collectible_data'] = None
            
            return jsonify(result), 200
        except Exception as e:
            app.logger.error(f"Error fetching deep-linked gift {gift_type_id}-{collectible_number}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred while fetching gift details: {e}"}), 500
        finally:
            conn.close()


@app.route('/api/collectible_usernames', methods=['POST'])
def add_collectible_username():
    """Adds a collectible username to an account."""
    data = request.get_json()
    owner_id = data.get('owner_id')
    username = data.get('username')

    if not owner_id or not username:
        return jsonify({"error": "owner_id and username are required"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            # Check if username already exists globally (unique constraint)
            cur.execute("SELECT 1 FROM collectible_usernames WHERE username = %s;", (username,))
            if cur.fetchone():
                return jsonify({"error": f"Username @{username} is already taken."}), 409 # Conflict

            # Check user's limit for collectible usernames
            cur.execute("SELECT COUNT(*) FROM collectible_usernames WHERE owner_id = %s;", (owner_id,))
            current_count = cur.fetchone()[0]
            if current_count >= MAX_COLLECTIBLE_USERNAMES:
                return jsonify({"error": f"You can only have a maximum of {MAX_COLLECTIBLE_USERNAMES} collectible usernames."}), 403

            cur.execute("""
                INSERT INTO collectible_usernames (owner_id, username)
                VALUES (%s, %s);
            """, (owner_id, username))
            conn.commit()
            return jsonify({"message": "Username added successfully"}), 201
        except psycopg2.IntegrityError: # Catch potential race conditions for unique constraint
            conn.rollback()
            app.logger.warning(f"Integrity error adding username {username} for owner {owner_id}.", exc_info=True)
            return jsonify({"error": f"Username @{username} is already taken (database constraint violation)."}), 409
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Error adding username {username} for owner {owner_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred while adding username: {e}"}), 500
        finally:
            conn.close()

@app.route('/api/collectible_usernames/<string:username>', methods=['DELETE'])
def delete_collectible_username(username):
    """Deletes a collectible username for a specific user."""
    data = request.get_json() # Frontend sends owner_id in body for verification
    owner_id = data.get('owner_id') 

    if not owner_id:
        return jsonify({"error": "owner_id is required"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed. Please try again later."}), 500

    with conn.cursor() as cur:
        try:
            cur.execute("""
                DELETE FROM collectible_usernames
                WHERE username = %s AND owner_id = %s;
            """, (username, owner_id))
            
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "Username not found for this user, or not owned by them."}), 404
            
            conn.commit()
            return jsonify({"message": "Username deleted successfully"}), 204 # 204 No Content
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Database error deleting username {username} for owner {owner_id}: {e}", exc_info=True)
            return jsonify({"error": f"An internal server error occurred while deleting username: {e}"}), 500
        finally:
            conn.close()

# Fallback route for undefined API calls
@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    app.logger.warning(f"Unhandled API call: {request.method} /api/{path}")
    return jsonify({"error": f"The requested API endpoint '/api/{path}' was not found or the method is not allowed."}), 404


if __name__ == '__main__':
    # When running locally, Flask's default development server is used.
    # On Render, Gunicorn will import 'app' and run it.
    print("Starting Flask server for local development...")
    init_db() # Initialize DB on local startup
    app.run(debug=True, port=5001)