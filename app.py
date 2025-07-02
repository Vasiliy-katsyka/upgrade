import os
import logging
import asyncio

from flask import Flask, request, jsonify
from telegram import Update, WebAppInfo, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# --- Configuration ---
# Enable logging for better debugging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Get environment variables
# Your bot token from BotFather
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
# A secret key you create to secure the /notify-sale endpoint
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET") 
# The external URL of your Render service (provided automatically by Render)
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

# The URL of your Web App
WEB_APP_URL = "https://vasiliy-katsyka.github.io/upgrade/"

# --- Bot Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the /start command. Sends a welcome message with a button to open the Web App.
    """
    user = update.effective_user
    logger.info(f"User {user.id} ({user.username}) started the bot.")

    # Create the Web App button
    keyboard = [
        [
            InlineKeyboardButton(
                "🎁 Открыть Симулятор Подарков",
                web_app=WebAppInfo(url=WEB_APP_URL)
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Send the welcome message
    await update.message.reply_text(
        f"Добро пожаловать, {user.first_name}!\n\n"
        "Нажмите на кнопку ниже, чтобы открыть симулятор подарков и начать собирать свою коллекцию.",
        reply_markup=reply_markup,
    )

# --- Flask Application Setup ---

# Initialize Flask app
app = Flask(__name__)

# Initialize the Telegram Bot Application
# We use a context_based application for better state management
application = Application.builder().token(TELEGRAM_TOKEN).build()

# Add the /start command handler
application.add_handler(CommandHandler("start", start))


@app.route("/")
def index():
    """A simple health check endpoint."""
    return "Bot is running!"


@app.route("/telegram", methods=["POST"])
async def telegram_webhook():
    """
    This endpoint receives updates from Telegram.
    It's the entry point for all bot interactions.
    """
    try:
        update_data = request.get_json(force=True)
        update = Update.de_json(data=update_data, bot=application.bot)
        await application.process_update(update)
        return "OK", 200
    except Exception as e:
        logger.error(f"Error processing update: {e}", exc_info=True)
        return "Error", 500


@app.route("/notify-sale", methods=["POST"])
async def notify_sale():
    """
    An endpoint to notify a user about a successful gift sale.
    This should be called by your frontend application.
    
    Expected JSON payload:
    {
        "secret": "YOUR_WEBHOOK_SECRET",
        "user_id": 123456789,
        "gift_name": "Plush Pepe",
        "gift_number": "1,381",
        "sell_price": 550,
        "received_amount": 440
    }
    """
    # Security check: Ensure the request is coming from a trusted source
    data = request.get_json()
    if not data or data.get("secret") != WEBHOOK_SECRET:
        logger.warning("Unauthorized attempt to access /notify-sale endpoint.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    # Validate required fields
    required_fields = ["user_id", "gift_name", "gift_number", "sell_price", "received_amount"]
    if not all(field in data for field in required_fields):
        logger.error(f"Missing fields in /notify-sale request: {data}")
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    try:
        user_id = int(data["user_id"])
        gift_name = data["gift_name"]
        gift_number = data["gift_number"]
        sell_price = int(data["sell_price"])
        received_amount = int(data["received_amount"])

        # Format the notification message
        message = (
            f"Your Gift **{gift_name} #{gift_number}** was sold for **{sell_price} ⭐️**.\n\n"
            f"**{received_amount} ⭐️** successfully credited to your Stars balance."
        )

        # Send the message to the user
        await application.bot.send_message(
            chat_id=user_id,
            text=message,
            parse_mode='Markdown'
        )

        logger.info(f"Successfully sent sale notification to user {user_id}")
        return jsonify({"status": "success", "message": "Notification sent"}), 200

    except ValueError:
        logger.error(f"Invalid data types in /notify-sale request: {data}")
        return jsonify({"status": "error", "message": "Invalid data types for numeric fields"}), 400
    except Exception as e:
        logger.error(f"Error sending sale notification: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Internal server error"}), 500


async def setup_bot():
    """
    Sets up the bot and its webhook. This runs once on application startup.
    """
    if not TELEGRAM_TOKEN or not RENDER_EXTERNAL_URL:
        logger.error("TELEGRAM_TOKEN or RENDER_EXTERNAL_URL not set. Bot cannot start.")
        return

    webhook_url = f"https://{RENDER_EXTERNAL_URL}/telegram"
    try:
        await application.bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook successfully set to {webhook_url}")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}", exc_info=True)

# Run the setup function on startup
# Flask's startup mechanism is a bit tricky, but this will run when the module is imported by Gunicorn.
# Using asyncio.run() to execute the async function in a sync context.
try:
    loop = asyncio.get_running_loop()
except RuntimeError:  # 'RuntimeError: There is no current event loop...'
    loop = None

if loop and loop.is_running():
    loop.create_task(setup_bot())
else:
    asyncio.run(setup_bot())
