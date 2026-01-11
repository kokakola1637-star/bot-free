import asyncio
import logging
import aiosqlite
import aiohttp
import os
import json
from bs4 import BeautifulSoup
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_GROUP_ID = int(os.getenv("VIP_GROUP_ID"))
BULK_GROUP_ID = int(os.getenv("BULK_GROUP_ID"))
# These two are needed to save your database to GitHub
GIST_RAW_URL = os.getenv("GIST_RAW_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

AUTO_CHECK_INTERVAL = 30 * 60 

# ==========================================
# FIXED KEYWORDS SYSTEM
# ==========================================
# Maps a short ID (for callback_data) to the Full Keyword (for display/search)
KEYWORDS = {
    "k1": "desi homemade",
    "k2": "indian amateur sex",
    "k3": "real indian couple",
    "k4": "desi phone sex video",
    "k5": "homemade mms india",
    "k6": "indian raw fuck",
    "k7": "desi affair sex",
    "k8": "indian cheating wife",
    "k9": "office affair desi",
    "k10": "real cheating mms",
    "k11": "desi mms leak",
    "k12": "indian leaked sex",
    "k13": "viral mms india",
    "k14": "real mms scandal",
    "k15": "leaked desi couple",
    "k16": "indian wife homemade",
    "k17": "desi bhabhi amateur",
    "k18": "tamil homemade",
    "k19": "hidden cam desi",
    "k20": "real desi sex",
    "k21": "indian wife cheating",
    "k22": "desi bhabhi affair",
    "k23": "hidden affair",
    "k24": "indian mms",
    "k25": "desi scandal",
    "k26": "leaked aunty",
    "k27": "college girl leak",
    "k28": "village mms",
    "k29": "bangladeshi gf",
    "k30": "bangladeshi",
    "k31": "Curvy girl",
    "k32": "Beautiful gf",
    "k33": "Hot gf",
    "k34": "Cute girl",
    "k35": "Cute gf",
    "k36": "Masti"
}

TARGET_SITES = [
    "https://www.kamababa.desi/", "https://desixclip.me/tag/desitales2/",
    "https://kamababa.lol/tag/mmsbaba/", "https://www.hindichudaivideos.com/",
    "https://www.mydesi2.net/", "https://desixxx.love/",
    "https://www.eporner.com/cat/all/", "https://www.tamilsexzone.com/",
    "https://desii49.com/", "https://www.spicymms.com/",
    "https://area51.porn/", "https://xnxxporn.video/category/desi-sex/"
]

# ==========================================
# GIST DATABASE MANAGER
# ==========================================
class GistDB:
    def __init__(self):
        self.local_path = "bot_database.db"
        # Extract Gist ID from the Raw URL
        try:
            self.gist_id = GIST_RAW_URL.split('/')[4]
        except:
            self.gist_id = None

    async def sync_download(self):
        """Download DB from Gist on startup"""
        if not self.gist_id:
            logging.warning("No Gist URL found. Starting with empty DB.")
            return

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(GIST_RAW_URL) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        with open(self.local_path, "wb") as f:
                            f.write(content)
                        logging.info("✅ Downloaded DB from Gist (Previous history loaded).")
                    else:
                        logging.info("Gist is new or empty. Starting fresh.")
                        open(self.local_path, "a").close()
        except Exception as e:
            logging.error(f"Failed to download DB: {e}")
            # Start fresh if download fails
            open(self.local_path, "a").close()

    async def sync_upload(self):
        """Upload DB to Gist after save"""
        if not self.gist_id or not GITHUB_TOKEN:
            logging.warning("Skipping Gist upload (Token or URL missing).")
            return

        try:
            with open(self.local_path, "rb") as f:
                content = f.read()
            
            # GitHub Gist API
            url = f"https://api.github.com/gists/{self.gist_id}"
            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            
            # Decode bytes to string for JSON
            content_str = content.decode('utf-8', errors='ignore')
            
            data = {
                "files": {
                    "bot_database.db": {
                        "content": content_str
                    }
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=headers, json=data) as resp:
                    if resp.status == 200:
                        logging.info("💾 Saved DB to Gist.")
                    else:
                        text = await resp.text()
                        logging.error(f"Failed to save DB: {resp.status} - {text}")
        except Exception as e:
            logging.error(f"Sync upload error: {e}")

gist_db = GistDB()

# ==========================================
# DATABASE
# ==========================================
async def init_db():
    # 1. First, download from Gist
    await gist_db.sync_download()
    
    # 2. Then connect
    async with aiosqlite.connect(gist_db.local_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                url TEXT,
                site TEXT,
                category TEXT,
                keyword TEXT,
                sent_to TEXT,
                timestamp TEXT
            )
        """)
        await db.commit()
    logging.info("Database initialized.")

async def is_video_exists(url_id: str) -> bool:
    async with aiosqlite.connect(gist_db.local_path) as db:
        cursor = await db.execute("SELECT 1 FROM videos WHERE video_id = ?", (url_id,))
        return await cursor.fetchone() is not None

async def save_video(video_id, url, site, category, keyword, sent_to):
    async with aiosqlite.connect(gist_db.local_path) as db:
        timestamp = datetime.now().isoformat()
        try:
            await db.execute(
                "INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)",
                (video_id, url, site, category, keyword, sent_to, timestamp)
            )
            await db.commit()
            # 3. Upload to Gist after saving
            await gist_db.sync_upload()
        except aiosqlite.IntegrityError:
            pass

# ==========================================
# SCRAPER
# ==========================================
class Scraper:
    @staticmethod
    async def get_soup(session, url):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return BeautifulSoup(await response.text(), 'html.parser')
        except Exception:
            pass
        return None

    @staticmethod
    async def scrape_site(session, site_url, target_keyword=None):
        soup = await Scraper.get_soup(session, site_url)
        if not soup: return []

        videos = []
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            title = link.get_text(strip=True)
            if len(title) < 15: continue

            if target_keyword and target_keyword.lower() not in title.lower():
                continue

            if href.startswith('/'):
                from urllib.parse import urljoin
                href = urljoin(site_url, href)

            vid_id = href.split('?')[0].strip('/')
            if not vid_id: continue

            videos.append({
                "video_id": vid_id, "url": href, "site": site_url,
                "title": title, "category": target_keyword or "Auto-Scrape"
            })
            if len(videos) >= 50: break
        return videos

# ==========================================
# BOT LOGIC
# ==========================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class ManualMode(StatesGroup):
    selecting_category = State()
    entering_quantity = State()

auto_mode_enabled = True

def get_category_keyboard():
    # FIXED: Iterate over the dictionary items
    buttons = []
    for key_id, keyword_text in KEYWORDS.items():
        # Button text shows the full keyword
        # Callback data uses only the short key_id (e.g., cat_k1)
        buttons.append([InlineKeyboardButton(text=keyword_text, callback_data=f"cat_{key_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await message.answer("Select a Category:", reply_markup=get_category_keyboard())
    await state.set_state(ManualMode.selecting_category)

@dp.callback_query(ManualMode.selecting_category)
async def category_selected(callback: types.CallbackQuery, state: FSMContext):
    # FIXED: Extract the short ID (e.g., "k1") from the callback data
    callback_prefix = "cat_"
    key_id = callback.data[len(callback_prefix):]
    
    # Retrieve the full keyword text using the ID
    keyword = KEYWORDS.get(key_id)
    
    if keyword:
        await state.update_data(selected_keyword=keyword)
        await state.set_state(ManualMode.entering_quantity)
        await callback.message.edit_text(f"Selected: **{keyword}**\n\nHow many videos?", parse_mode="Markdown")
        await callback.answer()
    else:
        await callback.answer("Error: Invalid category selected.", show_alert=True)

@dp.message(ManualMode.entering_quantity)
async def quantity_entered(message: types.Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0: raise ValueError
    except ValueError:
        await message.answer("Enter a valid number.")
        return
    data = await state.get_data()
    keyword = data['selected_keyword']
    await message.answer(f"Collecting {quantity} videos for '{keyword}'...")
    await state.clear()
    asyncio.create_task(run_manual_scrape(message.chat.id, keyword, quantity))

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    status = "Running ✅" if auto_mode_enabled else "Stopped ⏸️"
    await message.answer(f"Auto Mode: {status}")

@dp.message(Command("auto"))
async def cmd_auto(message: types.Message):
    global auto_mode_enabled
    if message.text == "/auto on":
        auto_mode_enabled = True
        await message.answer("Auto Mode ON.")
    elif message.text == "/auto off":
        auto_mode_enabled = False
        await message.answer("Auto Mode OFF.")

async def send_video(video, chat_id):
    try:
        caption = f"🔥 <b>{video['title']}</b>\n🔗 {video['url']}"
        await bot.send_message(chat_id, caption, parse_mode="HTML")
        return True
    except Exception:
        return False

async def run_manual_scrape(user_id, keyword, quantity):
    found = 0
    async with aiohttp.ClientSession() as session:
        for site in TARGET_SITES:
            if found >= quantity: break
            videos = await Scraper.scrape_site(session, site, target_keyword=keyword)
            for video in videos:
                if found >= quantity: break
                if not await is_video_exists(video['video_id']):
                    if await send_video(video, BULK_GROUP_ID):
                        await save_video(video['video_id'], video['url'], video['site'], video['category'], keyword, "BULK")
                        found += 1
    await bot.send_message(user_id, f"✅ Done. Sent {found} videos.")

async def auto_scrape_loop():
    while True:
        if auto_mode_enabled:
            async with aiohttp.ClientSession() as session:
                for site in TARGET_SITES:
                    videos = await Scraper.scrape_site(session, site)
                    for video in videos:
                        if not await is_video_exists(video['video_id']):
                            if await send_video(video, VIP_GROUP_ID):
                                await save_video(video['video_id'], video['url'], site, "Latest", "N/A", "VIP")
                        await asyncio.sleep(2)
        await asyncio.sleep(AUTO_CHECK_INTERVAL)

async def main():
    await init_db()
    asyncio.create_task(auto_scrape_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
