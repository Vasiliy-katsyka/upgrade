import os
import logging
import asyncio

from flask import Flask, request, jsonify
from telegram import Update, WebAppInfo, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

# --- Configuration ---
# Set up logging to see bot activity and errors
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Environment Variables ---
# Your bot token from BotFather. This is a secret.
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
# A secret key you create. Your frontend must send this key to use the /notify-sale endpoint.
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET") 
# The external URL of your Render service. Render provides this automatically.
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

# The public URL of your Web App (e.g., your GitHub Pages URL)
WEB_APP_URL = "https://vasiliy-katsyka.github.io/upgrade/"

# --- Telegram Bot Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    This function is called when a user sends the /start command.
    It sends a welcome message with a button that opens your Web App.
    """
    user = update.effective_user
    logger.info(f"User {user.id} ({user.username}) started the bot.")

    # This creates the button that opens your Web App.
    # The `web_app` parameter links the button to the specified URL.
    keyboard = [
        [
            InlineKeyboardButton(
                "🎁 Открыть Симулятор Подарков",
                web_app=WebAppInfo(url=WEB_APP_URL)
            )
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # The welcome message sent to the user.
    await update.message.reply_text(
        f"Добро пожаловать, {user.first_name}!\n\n"
        "Нажмите на кнопку ниже, чтобы открыть симулятор подарков и начать собирать свою коллекцию.",
        reply_markup=reply_markup,
    )

# --- Flask Web Application ---

# Initialize the Flask app, which will handle incoming HTTP requests.
app = Flask(__name__)

# Initialize the Telegram Bot Application using the token.
# The `python-telegram-bot` library manages all the low-level API communication.
application = Application.builder().token(TELEGRAM_TOKEN).build()

# Register the /start command handler. When the bot receives /start, it will call the `start` function.
application.add_handler(CommandHandler("start", start))


@app.route("/")
def index():
    """A simple 'health check' endpoint to confirm the web server is running."""
    return "Bot web server is running!"


@app.route("/telegram", methods=["POST"])
async def telegram_webhook():
    """
    This is the main webhook endpoint. Telegram sends all bot updates (messages, commands, etc.) here.
    The `python-telegram-bot` library then processes the update and passes it to the correct handler.
    """
    try:
        update_data = request.get_json(force=True)
        update = Update.de_json(data=update_data, bot=application.bot)
        await application.process_update(update)
        return "OK", 200
    except Exception as e:
        logger.error(f"Error processing Telegram update: {e}", exc_info=True)
        return "Error", 500


@app.route("/notify-sale", methods=["POST"])
async def notify_sale():
    """
    This is your custom endpoint for the frontend to report a gift sale.
    It is secured with a secret key.
    
    Example JSON payload your frontend should send:
    {
        "secret": "YOUR_SUPER_SECRET_STRING",
        "user_id": 123456789,
        "gift_name": "Plush Pepe",
        "gift_number": "1,381",
        "sell_price": 550,
        "received_amount": 440
    }
    """
    # 1. Security Check: Verify the secret key.
    data = request.get_json()
    if not data or data.get("secret") != WEBHOOK_SECRET:
        logger.warning("Unauthorized attempt to access /notify-sale endpoint.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    # 2. Validate that all required data fields are present.
    required_fields = ["user_id", "gift_name", "gift_number", "sell_price", "received_amount"]
    if not all(field in data for field in required_fields):
        logger.error(f"Missing fields in /notify-sale request: {data}")
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    try:
        # 3. Extract and parse data.
        user_id = int(data["user_id"])
        gift_name = data["gift_name"]
        gift_number = data["gift_number"]
        sell_price = int(data["sell_price"])
        received_amount = int(data["received_amount"])

        # 4. Format the notification message using Markdown for bold text.
        message = (
            f"Your Gift **{gift_name} #{gift_number}** was sold for **{sell_price} ⭐️**.\n\n"
            f"**{received_amount} ⭐️** successfully credited to your Stars balance."
        )

        # 5. Send the message to the user via the bot.
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
        logger.error(f"Failed to send sale notification: {e}", exc_info=True)
        # Handle cases where the bot might be blocked by the user, etc.
        return jsonify({"status": "error", "message": "Internal server error"}), 500


async def setup_bot():
    """
    This function runs once on application startup.
    It automatically discovers the public URL from the Render environment
    and sets the webhook with Telegram.
    """
    if not TELEGRAM_TOKEN or not RENDER_EXTERNAL_URL:
        logger.error("FATAL: TELEGRAM_TOKEN or RENDER_EXTERNAL_URL environment variables not set.")
        return

    # The full URL for our webhook endpoint.
    webhook_url = f"https://{RENDER_EXTERNAL_URL}/telegram"
    
    try:
        # Tell the Telegram API where to send updates for our bot.
        await application.bot.set_webhook(url=webhook_url)
        # You can check your bot's webhook status with: https://api.telegram.org/bot<TOKEN>/getWebhookInfo
        logger.info(f"Webhook successfully set to {webhook_url}")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}", exc_info=True)

# This logic ensures the async `setup_bot` function is run correctly
# when the application starts up in the Gunicorn/production environment.
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None

if loop and loop.is_running():
    loop.create_task(setup_bot())
else:
    asyncio.run(setup_bot())
