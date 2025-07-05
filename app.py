import os
import hmac
import hashlib
import json
from urllib.parse import unquote, parse_qsl
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, and_

# --- App Initialization and Configuration ---
app = Flask(__name__)

# Configure CORS to only allow requests from your GitHub Pages domain
CORS(app, resources={r"/api/*": {"origins": "https://vasiliy-katsyka.github.io"}})

# Get secrets from environment variables
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_TELEGRAM_BOT_OKEN_HERE") # Replace with your actual bot token for local testing

db = SQLAlchemy(app)

# --- Constants ---
MAX_COLLECTIBLE_GIFTS = 5000
MAX_COLLECTIBLE_USERNAMES = 10

# --- Database Models ---

class Account(db.Model):
    __tablename__ = 'accounts'
    id = db.Column(db.Integer, primary_key=True)
    telegram_user_id = db.Column(db.BigInteger, unique=True, nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    username = db.Column(db.String(255), nullable=True, index=True)
    avatar = db.Column(db.Text, nullable=True)
    bio = db.Column(db.Text, nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    worn_gift_instance_id = db.Column(db.Integer, db.ForeignKey('gifts.instance_id', use_alter=True, name='fk_worn_gift'), nullable=True)
    
    gifts = db.relationship('Gift', back_populates='owner', lazy='dynamic', cascade="all, delete-orphan", foreign_keys='Gift.owner_telegram_id')
    usernames = db.relationship('CollectibleUsername', backref='owner', lazy=True, cascade="all, delete-orphan")
    
    def to_dict(self):
        return {
            "id": self.telegram_user_id,
            "name": self.name,
            "username": self.username,
            "avatar": self.avatar,
            "bio": self.bio,
            "phone": self.phone,
            "wornGiftInstanceId": self.worn_gift_instance_id,
            "collectibleUsernames": [u.username for u in self.usernames]
        }

class Gift(db.Model):
    __tablename__ = 'gifts'
    instance_id = db.Column(db.Integer, primary_key=True)
    owner_telegram_id = db.Column(db.BigInteger, db.ForeignKey('accounts.telegram_user_id'), nullable=False, index=True)
    
    base_gift_id = db.Column(db.String(100), nullable=False, index=True)
    base_gift_name = db.Column(db.String(255), nullable=False)
    
    is_collectible = db.Column(db.Boolean, default=False, nullable=False)
    collectible_number = db.Column(db.Integer, nullable=True)
    collectible_model_name = db.Column(db.String(255), nullable=True)
    collectible_backdrop_name = db.Column(db.String(255), nullable=True)
    collectible_pattern_name = db.Column(db.String(255), nullable=True)
    
    acquired_date = db.Column(db.DateTime, default=datetime.utcnow)
    is_hidden = db.Column(db.Boolean, default=False)
    is_pinned = db.Column(db.Boolean, default=False)

    owner = db.relationship('Account', back_populates='gifts', foreign_keys=[owner_telegram_id])

    __table_args__ = (db.UniqueConstraint('base_gift_id', 'collectible_number', name='_base_gift_uc'),)

    def to_dict(self):
        # This data structure must match the frontend's expectations
        return {
            "instanceId": self.instance_id,
            "id": self.base_gift_id,
            "name": self.base_gift_name,
            "isCollectible": self.is_collectible,
            "collectibleData": {
                "number": self.collectible_number,
                "model": { "name": self.collectible_model_name } if self.collectible_model_name else None,
                "backdrop": { "name": self.collectible_backdrop_name } if self.collectible_backdrop_name else None,
                "pattern": { "name": self.collectible_pattern_name } if self.collectible_pattern_name else None,
            } if self.is_collectible else None,
            "acquiredDate": self.acquired_date.isoformat(),
            "isHidden": self.is_hidden,
            "isPinned": self.is_pinned
        }

class CollectibleUsername(db.Model):
    __tablename__ = 'collectible_usernames'
    id = db.Column(db.Integer, primary_key=True)
    owner_telegram_id = db.Column(db.BigInteger, db.ForeignKey('accounts.telegram_user_id'), nullable=False)
    username = db.Column(db.String(255), unique=True, nullable=False)

# --- Authentication Decorator ---
def validate_telegram_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_data_str = request.headers.get('X-Telegram-Auth')
        if not auth_data_str:
            return jsonify({"error": "Not authorized: Missing auth header"}), 401

        try:
            params = dict(parse_qsl(unquote(auth_data_str)))
            hash_from_telegram = params.pop('hash', None)

            if not hash_from_telegram:
                 return jsonify({"error": "Not authorized: Missing hash"}), 401

            data_check_string = "\n".join(sorted([f"{k}={v}" for k, v in params.items()]))
            
            secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
            calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

            if calculated_hash != hash_from_telegram:
                return jsonify({"error": "Not authorized: Invalid hash"}), 403
            
            user_data = json.loads(params.get('user', '{}'))
            if not user_data:
                return jsonify({"error": "Not authorized: User data not found"}), 401

            # Pass the authenticated user data to the decorated function
            kwargs['user_data'] = user_data
        except Exception as e:
            return jsonify({"error": f"Authorization error: {str(e)}"}), 401

        return f(*args, **kwargs)
    return decorated_function

# --- API Endpoints ---

@app.route("/api/user/init", methods=['POST'])
@validate_telegram_auth
def init_user(user_data):
    """
    Initial endpoint called by the frontend.
    Checks if user exists, creates if not, and returns all user data.
    """
    try:
        tg_id = user_data['id']
        account = Account.query.filter_by(telegram_user_id=tg_id).first()

        if not account:
            account = Account(
                telegram_user_id=tg_id,
                name=f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip(),
                username=user_data.get('username'),
                bio="Hello, I'm new here!",
                avatar=user_data.get('photo_url')
            )
            db.session.add(account)
            db.session.commit()

        gifts = Gift.query.filter_by(owner_telegram_id=tg_id).order_by(Gift.acquired_date.desc()).all()
        
        # Manually construct the response to match frontend expectations
        # It's better to be explicit than to rely on a generic serializer
        response_data = {
            "account": account.to_dict(),
            "ownedGifts": [gift.to_dict() for gift in gifts],
            "pinnedGifts": [g.instance_id for g in gifts if g.is_pinned]
        }

        return jsonify(response_data), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database initialization error: {str(e)}"}), 500

@app.route("/api/gifts/upgrade", methods=['POST'])
@validate_telegram_auth
def upgrade_gift(user_data):
    """Upgrades a non-collectible gift to a collectible one."""
    data = request.get_json()
    instance_id = data.get('instanceId')
    collectible_parts = data.get('collectibleParts') # { model, backdrop, pattern }
    
    if not instance_id or not collectible_parts:
        return jsonify({"error": "Missing instanceId or collectibleParts"}), 400

    tg_id = user_data['id']
    gift = Gift.query.filter_by(instance_id=instance_id, owner_telegram_id=tg_id).first()

    if not gift:
        return jsonify({"error": "Gift not found or not owned by user"}), 404
    if gift.is_collectible:
        return jsonify({"error": "Gift is already a collectible"}), 400

    # Check collectible limit
    collectible_count = Gift.query.filter_by(owner_telegram_id=tg_id, is_collectible=True).count()
    if collectible_count >= MAX_COLLECTIBLE_GIFTS:
        return jsonify({"error": f"Collectible gift limit of {MAX_COLLECTIBLE_GIFTS} reached"}), 403

    try:
        # Find next collectible number for this base gift type
        max_number_result = db.session.query(func.max(Gift.collectible_number)).filter_by(base_gift_id=gift.base_gift_id).scalar()
        next_number = (max_number_result or 0) + 1
        
        gift.is_collectible = True
        gift.collectible_number = next_number
        gift.collectible_model_name = collectible_parts.get('model', {}).get('name')
        gift.collectible_backdrop_name = collectible_parts.get('backdrop', {}).get('name')
        gift.collectible_pattern_name = collectible_parts.get('pattern', {}).get('name')
        
        # Unpin if it was pinned (should not happen, but as a safeguard)
        gift.is_pinned = False

        db.session.commit()
        return jsonify(gift.to_dict()), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database upgrade error: {str(e)}"}), 500

@app.route("/api/gifts/transfer", methods=['POST'])
@validate_telegram_auth
def transfer_gift(user_data):
    """Transfers a gift from the current user to another."""
    data = request.get_json()
    instance_id = data.get('instanceId')
    receiver_username = data.get('receiverUsername')

    if not instance_id or not receiver_username:
        return jsonify({"error": "Missing instanceId or receiverUsername"}), 400

    tg_id = user_data['id']
    gift_to_transfer = Gift.query.filter_by(instance_id=instance_id, owner_telegram_id=tg_id).first()

    if not gift_to_transfer:
        return jsonify({"error": "Gift not found or not owned by user"}), 404

    receiver = Account.query.filter(func.lower(Account.username) == func.lower(receiver_username)).first()
    if not receiver:
        return jsonify({"error": f"User @{receiver_username} not found in the app"}), 404

    if receiver.telegram_user_id == tg_id:
        return jsonify({"error": "You cannot transfer a gift to yourself"}), 400

    # Check receiver's gift limit
    receiver_collectible_count = Gift.query.filter_by(owner_telegram_id=receiver.telegram_user_id, is_collectible=True).count()
    if receiver_collectible_count >= MAX_COLLECTIBLE_GIFTS:
        return jsonify({"error": f"Receiver has reached their collectible gift limit of {MAX_COLLECTIBLE_GIFTS}"}), 403
    
    try:
        # Transfer ownership
        gift_to_transfer.owner_telegram_id = receiver.telegram_user_id
        # Reset state for new owner
        gift_to_transfer.is_pinned = False
        gift_to_transfer.is_hidden = False
        # If the gift was worn by the sender, unworn it
        sender = Account.query.get(tg_id)
        if sender and sender.worn_gift_instance_id == gift_to_transfer.instance_id:
            sender.worn_gift_instance_id = None

        db.session.commit()
        return jsonify({"success": True, "message": f"Gift transferred to @{receiver.username}"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database transfer error: {str(e)}"}), 500

@app.route("/api/user/update", methods=['POST'])
@validate_telegram_auth
def update_user_profile(user_data):
    """Updates user profile details like bio, avatar, worn gift."""
    data = request.get_json()
    tg_id = user_data['id']
    account = Account.query.filter_by(telegram_user_id=tg_id).first()
    if not account:
        return jsonify({"error": "Account not found"}), 404
        
    try:
        if 'bio' in data:
            account.bio = data['bio']
        if 'avatar' in data:
            account.avatar = data['avatar']
        if 'phone' in data:
            account.phone = data['phone']
        if 'wornGiftInstanceId' in data:
            # Check if user owns the gift they are trying to wear
            gift_to_wear = Gift.query.filter_by(
                instance_id=data['wornGiftInstanceId'], 
                owner_telegram_id=tg_id
            ).first() if data['wornGiftInstanceId'] is not None else True
            
            if not gift_to_wear:
                return jsonify({"error": "Cannot wear a gift you do not own"}), 403
            
            account.worn_gift_instance_id = data['wornGiftInstanceId']

        db.session.commit()
        return jsonify(account.to_dict()), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database update error: {str(e)}"}), 500

@app.route("/api/gifts/update_state", methods=['POST'])
@validate_telegram_auth
def update_gift_state(user_data):
    """Updates a gift's state (pinned, hidden)."""
    data = request.get_json()
    instance_id = data.get('instanceId')
    tg_id = user_data['id']

    gift = Gift.query.filter_by(instance_id=instance_id, owner_telegram_id=tg_id).first()
    if not gift:
        return jsonify({"error": "Gift not found or not owned by user"}), 404

    try:
        if 'isPinned' in data:
            gift.is_pinned = data['isPinned']
        if 'isHidden' in data:
            gift.is_hidden = data['isHidden']
            # If hiding a gift, it must also be unpinned and unworn
            if gift.is_hidden:
                gift.is_pinned = False
                account = Account.query.filter_by(telegram_user_id=tg_id).first()
                if account and account.worn_gift_instance_id == gift.instance_id:
                    account.worn_gift_instance_id = None
        
        db.session.commit()
        return jsonify(gift.to_dict()), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database update error: {str(e)}"}), 500


@app.route("/api/data/view_gift", methods=['GET'])
def view_gift():
    """Public endpoint to view a specific gift by its name and number."""
    base_gift_name_raw = request.args.get('name')
    collectible_number = request.args.get('number')

    if not base_gift_name_raw or not collectible_number:
        return jsonify({"error": "Missing gift name or number"}), 400
        
    # The frontend might send "PlushPepe", so we need to add spaces back
    # This is a bit brittle; a better approach would be to use base_gift_id if possible.
    base_gift_name = ' '.join(re.findall('[A-Z][^A-Z]*', base_gift_name_raw))

    try:
        gift = Gift.query.filter_by(
            base_gift_name=base_gift_name,
            collectible_number=int(collectible_number),
            is_collectible=True
        ).first()

        if not gift:
            return jsonify({"error": "Collectible gift not found"}), 404

        owner = Account.query.filter_by(telegram_user_id=gift.owner_telegram_id).first()
        
        return jsonify({
            "gift": gift.to_dict(),
            "owner": owner.to_dict() if owner else None
        }), 200
    except Exception as e:
        return jsonify({"error": f"Database query error: {str(e)}"}), 500

@app.route("/api/data/clear_account", methods=['DELETE'])
@validate_telegram_auth
def clear_account_data(user_data):
    """Deletes all data associated with the current user."""
    tg_id = user_data['id']
    account = Account.query.filter_by(telegram_user_id=tg_id).first()
    if not account:
        return jsonify({"error": "Account not found"}), 404
    
    try:
        # Cascade should handle deleting gifts and usernames
        db.session.delete(account)
        db.session.commit()
        return jsonify({"success": True, "message": "Account data has been cleared."}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Database deletion error: {str(e)}"}), 500

@app.before_first_request
def create_tables():
    """Create all database tables if they don't exist."""
    db.create_all()

if __name__ == '__main__':
    # The app.before_first_request is deprecated in newer Flask versions.
    # For local development, it's fine. For production with Gunicorn,
    # you'd typically run a separate migration script.
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5001)