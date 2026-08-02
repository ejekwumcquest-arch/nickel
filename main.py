import logging
import os
import datetime
import time
import json
import threading
import requests
import tls_client
import pickle
import random
import re
import websocket
from threading import Semaphore, Lock
from urllib.parse import urlparse
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Configuration ----------
def load_config():
    global token, webhook, proxy, blacklistedRoles, blacklistedUsers, scan_interval
    global BATCH_SIZE, INDIVIDUAL_THRESHOLD, guild_channel_pairs, friend_tokens
    global MAX_CONCURRENT_SCANS, FRIEND_REQUEST_WINDOW_MIN, FRIEND_REQUEST_WINDOW_MAX
    global FRIEND_REQUEST_RETRY_MAX, FRIEND_REQUEST_RETRY_WINDOW
    global GUILD_FAILURE_THRESHOLD, GUILD_FAILURE_SKIP_SECONDS, DRY_RUN

    token = None
    webhook = None
    proxy = ''
    blacklistedRoles = []
    blacklistedUsers = []
    scan_interval = 1800
    BATCH_SIZE = 20
    INDIVIDUAL_THRESHOLD = 5
    guild_channel_pairs = []
    friend_tokens = {}
    MAX_CONCURRENT_SCANS = 3
    FRIEND_REQUEST_WINDOW_MIN = 9000    # 2.5 hours
    FRIEND_REQUEST_WINDOW_MAX = 12600   # 3.5 hours
    FRIEND_REQUEST_RETRY_MAX = 3
    FRIEND_REQUEST_RETRY_WINDOW = 86400 # 24 hours
    GUILD_FAILURE_THRESHOLD = 3
    GUILD_FAILURE_SKIP_SECONDS = 3600   # 1 hour
    DRY_RUN = False

    # Environment variables first
    if 'DISCORD_TOKEN' in os.environ:
        token = os.environ['DISCORD_TOKEN']
        webhook = os.environ.get('DISCORD_WEBHOOK')
        proxy = os.environ.get('DISCORD_PROXY', '')
        # Safe JSON parsing with fallback
        try:
            blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
        except:
            blacklistedRoles = []
        try:
            blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
        except:
            blacklistedUsers = []
        try:
            friend_tokens = json.loads(os.environ.get('FRIEND_REQUEST_TOKENS', '{}'))
        except:
            friend_tokens = {}

        scan_interval = int(os.environ.get('SCAN_INTERVAL', '1800'))
        BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
        INDIVIDUAL_THRESHOLD = int(os.environ.get('INDIVIDUAL_THRESHOLD', '5'))
        MAX_CONCURRENT_SCANS = int(os.environ.get('MAX_CONCURRENT_SCANS', '3'))
        FRIEND_REQUEST_WINDOW_MIN = int(os.environ.get('FRIEND_REQUEST_WINDOW_MIN', '9000'))
        FRIEND_REQUEST_WINDOW_MAX = int(os.environ.get('FRIEND_REQUEST_WINDOW_MAX', '12600'))
        FRIEND_REQUEST_RETRY_MAX = int(os.environ.get('FRIEND_REQUEST_RETRY_MAX', '3'))
        FRIEND_REQUEST_RETRY_WINDOW = int(os.environ.get('FRIEND_REQUEST_RETRY_WINDOW', '86400'))
        GUILD_FAILURE_THRESHOLD = int(os.environ.get('GUILD_FAILURE_THRESHOLD', '3'))
        GUILD_FAILURE_SKIP_SECONDS = int(os.environ.get('GUILD_FAILURE_SKIP_SECONDS', '3600'))
        DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'

        # Parse guilds from DISCORD_GUILDS, DISCORD_GUILD_CONFIG, or parallel lists
        pairs_dict = {}
        if 'DISCORD_GUILDS' in os.environ:
            raw = os.environ['DISCORD_GUILDS']
            for entry in raw.split(';'):
                entry = entry.strip()
                if not entry or ':' not in entry:
                    continue
                guild_part, channels_part = entry.split(':', 1)
                guild = guild_part.strip()
                channels = [c.strip() for c in channels_part.split(',') if c.strip()]
                if guild and channels:
                    if guild not in pairs_dict:
                        pairs_dict[guild] = channels[0]
        elif 'DISCORD_GUILD_CONFIG' in os.environ:
            try:
                config_list = json.loads(os.environ['DISCORD_GUILD_CONFIG'])
                for entry in config_list:
                    g = entry.get('guildId') or entry.get('guild')
                    channels = entry.get('channels') or entry.get('channelIds') or []
                    if g and channels:
                        if g not in pairs_dict:
                            pairs_dict[g] = channels[0]
                        ft = entry.get('friendToken')
                        if ft:
                            friend_tokens[g] = ft
            except:
                pass
        elif 'DISCORD_GUILD_IDS' in os.environ and 'DISCORD_CHANNEL_IDS' in os.environ:
            raw_guilds = os.environ.get('DISCORD_GUILD_IDS', '')
            raw_channels = os.environ.get('DISCORD_CHANNEL_IDS', '')
            guilds = [g.strip() for g in raw_guilds.split(',') if g.strip()]
            channels = [c.strip() for c in raw_channels.split(',') if c.strip()]
            if len(guilds) != len(channels):
                raise ValueError("Number of guild IDs and channel IDs must match.")
            for g, c in zip(guilds, channels):
                if g not in pairs_dict:
                    pairs_dict[g] = c

        if pairs_dict:
            guild_channel_pairs = list(pairs_dict.items())
        else:
            raise ValueError("No guild configuration found.")
    else:
        # Fallback to config.json
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            token = config.get('token')
            webhook = config.get('webhook')
            proxy = config.get('proxy', '')
            blacklistedRoles = config.get('blacklistedRoles', [])
            blacklistedUsers = config.get('blacklistedUsers', [])
            scan_interval = config.get('scan_interval', 1800)
            BATCH_SIZE = config.get('batch_size', 20)
            INDIVIDUAL_THRESHOLD = config.get('individual_threshold', 5)
            friend_tokens = config.get('friendTokens', {})
            MAX_CONCURRENT_SCANS = config.get('max_concurrent_scans', 3)
            FRIEND_REQUEST_WINDOW_MIN = config.get('friend_request_window_min', 9000)
            FRIEND_REQUEST_WINDOW_MAX = config.get('friend_request_window_max', 12600)
            FRIEND_REQUEST_RETRY_MAX = config.get('friend_request_retry_max', 3)
            FRIEND_REQUEST_RETRY_WINDOW = config.get('friend_request_retry_window', 86400)
            GUILD_FAILURE_THRESHOLD = config.get('guild_failure_threshold', 3)
            GUILD_FAILURE_SKIP_SECONDS = config.get('guild_failure_skip_seconds', 3600)
            DRY_RUN = config.get('dry_run', False)

            if 'guilds' in config and isinstance(config['guilds'], list):
                pairs_dict = {}
                for item in config['guilds']:
                    g = item.get('guildId') or item.get('guild')
                    channels = item.get('channels') or item.get('channelIds') or []
                    if not g or not channels:
                        continue
                    if g not in pairs_dict:
                        pairs_dict[g] = channels[0]
                    ft = item.get('friendToken')
                    if ft:
                        friend_tokens[g] = ft
                guild_channel_pairs = list(pairs_dict.items())
            else:
                g = config.get('guildID') or config.get('guildId')
                c = config.get('channelId') or config.get('channelIDs')
                if isinstance(g, list): g = g[0]
                if isinstance(c, list): c = c[0]
                if g and c:
                    guild_channel_pairs = [(g, c)]
                    ft = config.get('friendToken')
                    if ft:
                        friend_tokens[g] = ft
        except FileNotFoundError:
            raise ValueError("No configuration found.")

    if not token:
        raise ValueError("DISCORD_TOKEN is not set.")
    if not webhook:
        raise ValueError("DISCORD_WEBHOOK is not set.")
    if not guild_channel_pairs:
        raise ValueError("No guild-channel pairs configured.")

    # Validate guilds and tokens on startup (function defined later)
    validate_configuration()

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="\x1b[38;5;9m[\x1b[0m%(asctime)s\x1b[38;5;9m]\x1b[0m %(message)s\x1b[0m",
    datefmt="%H:%M:%S"
)

# ---------- Constants ----------
JOIN_WINDOW_SECONDS = 2 * 24 * 60 * 60
NOTIFIED_CACHE_FILE = "notified_members.pkl"
NOTIFIED_CACHE_BACKUP = "notified_members_backup.pkl"

# ---------- Cache handling with fallback ----------
def load_notified_cache():
    try:
        if os.path.exists(NOTIFIED_CACHE_FILE):
            with open(NOTIFIED_CACHE_FILE, 'rb') as f:
                return pickle.load(f)
    except (EOFError, pickle.PickleError, FileNotFoundError):
        logging.warning("Main cache corrupted, attempting backup...")
        try:
            if os.path.exists(NOTIFIED_CACHE_BACKUP):
                with open(NOTIFIED_CACHE_BACKUP, 'rb') as f:
                    data = pickle.load(f)
                # Restore main from backup
                with open(NOTIFIED_CACHE_FILE, 'wb') as f:
                    pickle.dump(data, f)
                return data
        except:
            logging.warning("Backup cache also corrupted, starting fresh.")
    return {}

notified_members = load_notified_cache()

def save_notified_cache():
    # Write to main
    with open(NOTIFIED_CACHE_FILE, 'wb') as f:
        pickle.dump(notified_members, f)
    # Also write backup
    with open(NOTIFIED_CACHE_BACKUP, 'wb') as f:
        pickle.dump(notified_members, f)

# ---------- Configuration Validation ----------
def validate_configuration():
    """Check that all guilds exist and friend tokens are valid."""
    global guild_channel_pairs, friend_tokens  # <-- FIXED: moved to top
    logging.info("Validating configuration...")
    valid_guilds = []
    # Validate guilds
    for guild_id, channel_id in guild_channel_pairs:
        try:
            limiter = get_rest_limiter(guild_id)
            limiter.acquire()
            sess = get_session()
            resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}')
            if resp.status_code == 200:
                guild_name = resp.json().get('name', 'Unknown')
                logging.info(f"✅ Guild {guild_id} ('{guild_name}') validated.")
                valid_guilds.append((guild_id, channel_id))
            elif resp.status_code == 404:
                logging.warning(f"❌ Guild {guild_id} not found (404). Skipping.")
            else:
                logging.warning(f"❌ Guild {guild_id} validation failed ({resp.status_code}). Skipping.")
        except Exception as e:
            logging.warning(f"❌ Guild {guild_id} validation error: {e}. Skipping.")
    # Update guild_channel_pairs with only valid ones
    guild_channel_pairs = valid_guilds

    # Validate friend tokens
    for guild_id, token in list(friend_tokens.items()):
        try:
            sess = tls_client.Session(client_identifier='chrome_105')
            sess.headers.update({'Authorization': token})
            resp = sess.get('https://discord.com/api/v9/users/@me')
            if resp.status_code == 200:
                username = resp.json().get('username', 'Unknown')
                logging.info(f"✅ Friend token for guild {guild_id} validated (user: {username}).")
            else:
                logging.warning(f"❌ Friend token for guild {guild_id} invalid (status {resp.status_code}). Removing.")
                del friend_tokens[guild_id]
        except Exception as e:
            logging.warning(f"❌ Friend token for guild {guild_id} validation error: {e}. Removing.")
            del friend_tokens[guild_id]

    if not guild_channel_pairs:
        raise ValueError("No valid guilds left after validation.")

# ---------- RateLimiter ----------
class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = 0
        self.lock = Semaphore()
        self.start = time.time()

    def acquire(self):
        with self.lock:
            now = time.time()
            if now - self.start > self.period:
                self.start = now
                self.calls = 0
            if self.calls >= self.max_calls:
                sleep_time = self.period - (now - self.start) + 0.1
                time.sleep(max(0, sleep_time))
                self.start = time.time()
                self.calls = 0
            self.calls += 1

# ---------- Friend Request Rate Limiter (sliding window with random window) ----------
class FriendRequestLimiter:
    def __init__(self, max_requests, window_min, window_max):
        self.max_requests = max_requests
        self.window_min = window_min
        self.window_max = window_max
        self.window = random.uniform(window_min, window_max)  # randomised on creation
        self.timestamps = deque()
        self.lock = Lock()

    def can_send(self):
        with self.lock:
            now = time.time()
            while self.timestamps and now - self.timestamps[0] > self.window:
                self.timestamps.popleft()
            return len(self.timestamps) < self.max_requests

    def record_send(self):
        with self.lock:
            self.timestamps.append(time.time())

    def wait_until_capacity(self):
        while True:
            if self.can_send():
                return
            with self.lock:
                if self.timestamps:
                    oldest = self.timestamps[0]
                    sleep_until = oldest + self.window
                    now = time.time()
                    if sleep_until > now:
                        time.sleep(sleep_until - now + random.uniform(1, 5))
                else:
                    time.sleep(random.uniform(1, 5))

# ---------- Rate limiters for REST/WS ----------
rest_limiters = {}
ws_limiters = {}

def get_rest_limiter(guild_id):
    if guild_id not in rest_limiters:
        rest_limiters[guild_id] = RateLimiter(1, 1)
    return rest_limiters[guild_id]

def get_ws_limiter(guild_id):
    if guild_id not in ws_limiters:
        ws_limiters[guild_id] = RateLimiter(1, 1)
    return ws_limiters[guild_id]

webhook_limiter = RateLimiter(2, 1)

# ---------- Friend Request Limiters per token ----------
friend_limiters = {}
friend_limiters_lock = Lock()

def get_friend_limiter(token):
    with friend_limiters_lock:
        if token not in friend_limiters:
            friend_limiters[token] = FriendRequestLimiter(
                max_requests=4,
                window_min=FRIEND_REQUEST_WINDOW_MIN,
                window_max=FRIEND_REQUEST_WINDOW_MAX
            )
        return friend_limiters[token]

# ---------- Proxy validation ----------
def is_valid_proxy_host(hostname):
    ipv4_re = r'^(\d{1,3}\.){3}\d{1,3}$'
    if re.match(ipv4_re, hostname):
        parts = hostname.split('.')
        return all(0 <= int(p) <= 255 for p in parts)
    domain_re = r'^(?=.{1,253}$)(?!-)(?:[a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,63}$'
    return bool(re.match(domain_re, hostname))

# ---------- Global Session (scraper) ----------
shared_session = None

def get_session():
    global shared_session
    if shared_session is None:
        shared_session = tls_client.Session(client_identifier='chrome_105')
        shared_session.headers.update({
            'accept': '*/*',
            'accept-encoding': 'application/json',
            'accept-language': 'en-US,en;q=0.8',
            'Content-Type': 'application/json',
            'Authorization': token,
            'referer': 'https://discord.com/channels/@me',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
            'x-context-properties': 'eyJsb2NhdGlvbiI6IlVzZXIgUHJvZmlsZSJ9',
            'x-debug-options': 'bugReporterEnabled',
            'x-discord-locale': 'en-US',
            'x-super-properties': 'eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiRGlzY29yZCBDbGllbnQiLCJyZWxlYXNlX2NoYW5uZWwiOiJjYW5hcnkiLCJjbGllbnRfdmVyc2lvbiI6IjEuMC41OSIsIm9zX3ZlcnNpb24iOiIxMC4wLjIyNjIxIiwib3NfYXJjaCI6Ing2NCIsInN5c3RlbV9sb2NhbGUiOiJlbi1VUyIsImNsaWVudF9idWlsZF9udW1iZXIiOjE4MTk2NywibmF0aXZlX2J1aWxkX251bWJlciI6MzA4NTIsImNsaWVudF9ldmVudF9zb3VyY2UiOm51bGwsImRlc2lnbl9pZCI6MH0='
        })
        if proxy:
            proxy_url = proxy
            if '://' not in proxy_url:
                proxy_url = 'http://' + proxy_url
            try:
                parsed = urlparse(proxy_url)
                host = parsed.hostname
                if host and is_valid_proxy_host(host):
                    shared_session.proxies = {'http': proxy_url, 'https': proxy_url}
                    logging.info(f"✅ Proxy set: {host}:{parsed.port or 'default'}")
                else:
                    logging.warning(f"❌ Invalid proxy host '{host}' – ignoring proxy.")
            except Exception as e:
                logging.warning(f"❌ Invalid proxy format '{proxy}': {e} – ignoring.")
    return shared_session

# ---------- Friend Request Sender (with retry queue) ----------
class FriendRequestQueue:
    def __init__(self):
        self.queue = defaultdict(list)  # token -> list of (user_id, tag, guild_id, attempt, first_try)
        self.lock = Lock()
        self.sent_cache = defaultdict(set)  # token -> set of user_id

    def enqueue(self, token, user_id, tag, guild_id, attempt=0):
        with self.lock:
            if user_id not in self.sent_cache[token]:
                self.queue[token].append((user_id, tag, guild_id, attempt, time.time()))
                self.sent_cache[token].add(user_id)

    def pop(self, token):
        with self.lock:
            if token in self.queue and self.queue[token]:
                return self.queue[token].pop(0)
            return None

    def size(self, token):
        with self.lock:
            return len(self.queue.get(token, []))

    def total_size(self):
        with self.lock:
            return sum(len(q) for q in self.queue.values())

friend_queue = FriendRequestQueue()

def friend_request_worker(token):
    """Background worker for a single token."""
    limiter = get_friend_limiter(token)
    logging.info(f"Friend worker started for token {token[:8]}...")
    while True:
        try:
            # Get next request from queue
            item = friend_queue.pop(token)
            if not item:
                time.sleep(5)
                continue
            user_id, tag, guild_id, attempt, first_try = item

            # Check if retry window expired
            if time.time() - first_try > FRIEND_REQUEST_RETRY_WINDOW:
                logging.warning(f"[Guild {guild_id}] Dropping request for {user_id} – retry window expired.")
                continue

            # Wait for rate limit capacity
            limiter.wait_until_capacity()

            # Send request (dry run?)
            if DRY_RUN:
                logging.info(f"[DRY RUN] Would send FR to {tag} ({user_id})")
                limiter.record_send()
                time.sleep(random.uniform(30, 120))
                continue

            success = send_friend_request(user_id, token, guild_id)
            if success:
                limiter.record_send()
                logging.info(f"✅ Friend request sent to {tag} ({user_id}) in guild {guild_id}")
                # Random delay between 30 and 120 seconds
                time.sleep(random.uniform(30, 120))
            else:
                logging.warning(f"❌ Failed to send friend request to {tag} ({user_id})")
                # Re‑queue with incremented attempt
                if attempt < FRIEND_REQUEST_RETRY_MAX:
                    friend_queue.enqueue(token, user_id, tag, guild_id, attempt + 1)
                else:
                    logging.error(f"Gave up on {tag} ({user_id}) after {FRIEND_REQUEST_RETRY_MAX} attempts.")
        except Exception as e:
            logging.error(f"Friend worker error: {e}")
            time.sleep(10)

def send_friend_request(user_id, friend_token, guild_id, max_retries=2):
    """Send a single friend request, returns True on success."""
    attempt = 0
    while attempt <= max_retries:
        try:
            session = tls_client.Session(client_identifier='chrome_105')
            session.headers.update({
                'Authorization': friend_token,
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
            })
            if proxy:
                session.proxies = shared_session.proxies if shared_session else {}
            resp = session.put(f'https://discord.com/api/v9/users/@me/relationships/{user_id}')
            if resp.status_code == 204:
                return True
            elif resp.status_code == 429:
                retry_after = resp.json().get('retry_after', 5)
                logging.warning(f"[Guild {guild_id}] Friend request rate limited, waiting {retry_after}s")
                time.sleep(retry_after + random.uniform(0, 2))
                attempt += 1
                continue
            else:
                logging.error(f"[Guild {guild_id}] Friend request failed ({resp.status_code}): {resp.text[:200]}")
                return False
        except Exception as e:
            logging.error(f"[Guild {guild_id}] Friend request exception: {e}")
            attempt += 1
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return False

def enqueue_friend_requests_for_guild(guild_id, pending_members):
    friend_token = friend_tokens.get(guild_id)
    if not friend_token:
        logging.info(f"[Guild {guild_id}] No friend token – skipping friend requests.")
        return
    for item in pending_members:
        friend_queue.enqueue(friend_token, item['member_id'], item['tag'], guild_id)
    logging.info(f"[Guild {guild_id}] Enqueued {len(pending_members)} friend requests.")

# ---------- REST member fetch (fixed) ----------
def fetch_all_members_rest(guild_id, max_retries=3):
    members = {}
    after = '0'
    retry_count = 0
    success = False
    limiter = get_rest_limiter(guild_id)
    while True:
        try:
            limiter.acquire()
            sess = get_session()
            resp = sess.get(
                f'https://discord.com/api/v9/guilds/{guild_id}/members',
                params={'limit': 1000, 'after': after}
            )
            if resp.status_code == 429:
                retry_after = resp.json().get('retry_after', 2)
                logging.warning(f"[Guild {guild_id}] REST rate limited, waiting {retry_after}s...")
                time.sleep(retry_after + random.uniform(0, 0.5))
                continue
            if resp.status_code == 403:
                logging.warning(f"[Guild {guild_id}] REST endpoint returned 403 (Missing Access) – falling back to WebSocket.")
                return None
            if resp.status_code == 401:
                logging.error("Token invalid or logged out. Stopping.")
                raise SystemExit("Token invalid – exiting.")
            if resp.status_code != 200:
                logging.error(f"[Guild {guild_id}] REST fetch failed: {resp.status_code} - {resp.text[:200]}")
                retry_count += 1
                if retry_count > max_retries:
                    break
                sleep_time = (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(sleep_time)
                continue
            data = resp.json()
            if not data:
                break
            for mem in data:
                user = mem.get('user', {})
                user_id = user.get('id')
                if not user_id:
                    continue
                if user.get('bot'):
                    continue
                if user_id in blacklistedUsers:
                    continue
                if set(blacklistedRoles).intersection(mem.get('roles', [])):
                    continue
                username = user.get('username', 'Unknown')
                discrim = user.get('discriminator', '0')
                tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
                joined_at = mem.get('joined_at')
                members[user_id] = (tag, joined_at)
            if len(data) < 1000:
                break
            after = data[-1]['user']['id']
            retry_count = 0
            success = True
        except Exception as e:
            logging.error(f"[Guild {guild_id}] REST fetch error: {e}")
            retry_count += 1
            if retry_count > max_retries:
                break
            time.sleep((2 ** retry_count) + random.uniform(0, 1))
    return members if success else None

# ---------- WebSocket fallback with dynamic timeout ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id, timeout=30):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.timeout = timeout
        self.blacklisted_roles = [str(r) for r in blacklistedRoles]
        self.blacklisted_users = [str(u) for u in blacklistedUsers]

        self.socket_headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
        }
        super().__init__(
            "wss://gateway.discord.gg/?encoding=json&v=9",
            header=self.socket_headers,
            on_open=lambda ws: self.sock_open(ws),
            on_message=lambda ws, msg: self.sock_message(ws, msg),
            on_close=lambda ws, close_code, close_msg: self.sock_close(ws, close_code, close_msg)
        )
        self.endScraping = False
        self.guilds = {}
        self.members = {}
        self.ranges = [[0, 99]]
        self.lastRange = 0
        self.packets_recv = 0
        self.rate_limited = False
        self.heartbeat_interval = None
        self.heartbeat_thread = None
        self.member_count = 0

    def run(self):
        timer = threading.Timer(self.timeout, self.close)
        timer.daemon = True
        timer.start()
        self.run_forever()
        timer.cancel()
        return self.members

    def scrapeUsers(self):
        if self.endScraping:
            return
        limiter = get_ws_limiter(self.guild_id)
        limiter.acquire()
        payload = {
            "op": 14,
            "d": {
                "guild_id": self.guild_id,
                "typing": True,
                "activities": True,
                "threads": True,
                "channels": {self.channel_id: self.ranges}
            }
        }
        self.send(json.dumps(payload))

    def sock_open(self, ws):
        identify = {
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": 125,
                "properties": {
                    "os": "Windows",
                    "browser": "Firefox",
                    "device": "",
                    "system_locale": "it-IT",
                    "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
                    "browser_version": "94.0",
                    "os_version": "10",
                    "referrer": "",
                    "referring_domain": "",
                    "referrer_current": "",
                    "referring_domain_current": "",
                    "release_channel": "stable",
                    "client_build_number": 103981,
                    "client_event_source": None
                },
                "presence": {"status": "online", "since": 0, "activities": [], "afk": False},
                "compress": False,
                "client_state": {
                    "guild_hashes": {},
                    "highest_last_message_id": "0",
                    "read_state_version": 0,
                    "user_guild_settings_version": -1,
                    "user_settings_version": -1
                }
            }
        }
        self.send(json.dumps(identify))

    def heartbeatThread(self, interval):
        try:
            while True:
                time.sleep(interval)
                if not self.sock:
                    break
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
        except Exception:
            return

    def sock_message(self, ws, message):
        try:
            decoded = json.loads(message)
            if not isinstance(decoded, dict):
                return
            op = decoded.get("op")
            t = decoded.get("t")
            if op != 11:
                self.packets_recv += 1
            if op == 10:
                interval = decoded["d"]["heartbeat_interval"] / 1000
                self.heartbeat_thread = threading.Thread(target=self.heartbeatThread, args=(interval,), daemon=True)
                self.heartbeat_thread.start()
            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}
            if t == "READY_SUPPLEMENTAL":
                self.member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                if self.member_count == 0:
                    logging.warning(f"[Guild {self.guild_id}] Member count is 0. Closing socket.")
                    self.close()
                    return
                # Dynamic timeout: adjust based on member count
                self.timeout = max(30, self.member_count / 50)
                self.ranges = [[0, 99]]
                self.lastRange = 0
                self.scrapeUsers()
            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = self.parseGuildMemberListUpdate(decoded)
                if parsed['guild_id'] != self.guild_id:
                    return
                for elem, index in enumerate(parsed["types"]):
                    updates = parsed["updates"][elem]
                    if isinstance(updates, dict):
                        updates = [updates]
                    elif not isinstance(updates, list):
                        updates = []
                    if index == "SYNC":
                        if len(updates) == 0:
                            self.endScraping = True
                            break
                        for item in updates:
                            if "member" in item:
                                mem = item["member"]
                                user = mem.get("user", {})
                                if not user:
                                    continue
                                user_id = user.get("id")
                                if not user_id:
                                    continue
                                if set(self.blacklisted_roles).intersection(mem.get("roles", [])):
                                    continue
                                if user.get("bot"):
                                    continue
                                if user_id in self.blacklisted_users:
                                    continue
                                username = user.get('username', 'Unknown')
                                discrim = user.get('discriminator', '0')
                                tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
                                joined_at = mem.get('joined_at')
                                self.members[user_id] = (tag, joined_at)
                    elif index == "UPDATE":
                        for item in updates:
                            if "member" in item:
                                mem = item["member"]
                                user = mem.get("user", {})
                                if not user:
                                    continue
                                user_id = user.get("id")
                                if not user_id:
                                    continue
                                if set(self.blacklisted_roles).intersection(mem.get("roles", [])):
                                    continue
                                if user.get("bot"):
                                    continue
                                if user_id in self.blacklisted_users:
                                    continue
                                username = user.get('username', 'Unknown')
                                discrim = user.get('discriminator', '0')
                                tag = f"{username}#{discrim}" if discrim != "0" else f"@{username}"
                                joined_at = mem.get('joined_at')
                                self.members[user_id] = (tag, joined_at)
                    if not self.endScraping:
                        self.lastRange += 1
                        next_start = self.lastRange * 100
                        if self.member_count > 0 and next_start >= self.member_count:
                            self.endScraping = True
                            break
                        self.ranges = [[next_start, next_start + 99]]
                        self.scrapeUsers()
                if self.endScraping:
                    self.close()
        except Exception as e:
            logging.error(f"[Guild {self.guild_id}] WS error: {e}")

    def parseGuildMemberListUpdate(self, response):
        memberdata = {
            "online_count": response["d"]["online_count"],
            "member_count": response["d"]["member_count"],
            "id": response["d"]["id"],
            "guild_id": response["d"]["guild_id"],
            "hoisted_roles": response["d"]["groups"],
            "types": [],
            "locations": [],
            "updates": []
        }
        for chunk in response['d']['ops']:
            memberdata['types'].append(chunk['op'])
            if chunk['op'] in ('SYNC', 'INVALIDATE'):
                memberdata['locations'].append(chunk['range'])
                if chunk['op'] == 'SYNC':
                    memberdata['updates'].append(chunk['items'])
                else:
                    memberdata['updates'].append([])
            elif chunk['op'] in ('INSERT', 'UPDATE', 'DELETE'):
                memberdata['locations'].append(chunk['index'])
                if chunk['op'] == 'DELETE':
                    memberdata['updates'].append([])
                else:
                    memberdata['updates'].append(chunk['item'])
        return memberdata

    def sock_close(self, ws, close_code, close_msg):
        if close_msg and "Rate limited" in close_msg:
            self.rate_limited = True
            logging.warning(f"[Guild {self.guild_id}] Rate limit detected on channel {self.channel_id}.")

def fetch_all_members_via_websocket(guild_id, channel_id):
    all_members = {}
    max_retries = 2
    for attempt in range(max_retries):
        try:
            # Get approximate member count first to set timeout
            timeout = 30
            try:
                limiter = get_rest_limiter(guild_id)
                limiter.acquire()
                sess = get_session()
                resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}')
                if resp.status_code == 200:
                    member_count = resp.json().get('approximate_member_count', 0)
                    timeout = max(30, member_count / 50)
            except:
                pass
            logging.info(f"[Guild {guild_id}] WS scanning channel {channel_id} (attempt {attempt+1}/{max_retries}, timeout {timeout:.0f}s) ...")
            sb = DiscordSocket(token, guild_id, channel_id, timeout=timeout)
            result = sb.run()
            if result:
                logging.info(f"[Guild {guild_id}] Channel {channel_id} returned {len(result)} members via WS.")
                all_members.update(result)
                break
            else:
                if sb.rate_limited:
                    logging.warning(f"[Guild {guild_id}] Rate limited on WS for channel {channel_id}. Waiting 60s.")
                    time.sleep(60 + random.uniform(0, 10))
                else:
                    logging.warning(f"[Guild {guild_id}] Channel {channel_id} returned 0 members. Retrying...")
                    time.sleep((2 ** (attempt + 1)) + random.uniform(0, 2))
        except Exception as e:
            logging.error(f"[Guild {guild_id}] WS error: {e}")
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return all_members

# ---------- Unified member fetcher ----------
def fetch_all_members(guild_id, channel_id):
    rest_members = fetch_all_members_rest(guild_id)
    if rest_members is not None:
        logging.info(f"[Guild {guild_id}] REST fetch successful.")
        return rest_members
    logging.info(f"[Guild {guild_id}] Falling back to WebSocket scraping (user token).")
    return fetch_all_members_via_websocket(guild_id, channel_id)

# ---------- Webhook sending (unchanged) ----------
def send_single_webhook(guild_id, member_id, tag, join_time, max_retries=3):
    if DRY_RUN:
        logging.info(f"[DRY RUN] Would send webhook for {member_id}")
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            rest_limiter = get_rest_limiter(guild_id)
            rest_limiter.acquire()
            guild_resp = get_session().get(f'https://discord.com/api/v9/guilds/{guild_id}')
            guild_name = guild_resp.json().get('name', 'Unknown') if guild_resp.status_code == 200 else 'Unknown'
            if tag.startswith('@'):
                clean_username = tag[1:]
            elif '#' in tag:
                clean_username = tag.split('#')[0]
            else:
                clean_username = tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"@here New User Joined {guild_id}",
                "embeds": [{
                    "color": 161791,
                    "author": {"name": "Snitched Successful"},
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "fields": [
                        {"name": "Username", "value": f"[{clean_username}](https://discord.com/users/{member_id})", "inline": True},
                        {"name": "Full Tag (copy)", "value": f"`{tag}`", "inline": True},
                        {"name": "User ID", "value": member_id, "inline": True},
                        {"name": "Joined Server", "value": join_str, "inline": False},
                        {"name": "Mention", "value": f"<@{member_id}>", "inline": True},
                        {"name": "Guild", "value": guild_name, "inline": True}
                    ]
                }]
            }
            webhook_limiter.acquire()
            response = requests.post(webhook, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Webhook sent for {member_id} in guild {guild_id}")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Webhook rate limited for {member_id}, waiting {wait_time}s...")
                time.sleep(wait_time + random.uniform(0, 0.5))
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Webhook exception: {e}")
            attempt += 1
            time.sleep((2 ** attempt) + random.uniform(0, 1))

def send_batch_webhook(guild_id, batch, max_retries=3):
    if DRY_RUN:
        logging.info(f"[DRY RUN] Would send batch webhook for {len(batch)} members")
        return
    if not batch:
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            rest_limiter = get_rest_limiter(guild_id)
            rest_limiter.acquire()
            guild_resp = get_session().get(f'https://discord.com/api/v9/guilds/{guild_id}')
            guild_name = guild_resp.json().get('name', 'Unknown') if guild_resp.status_code == 200 else 'Unknown'
            fields = []
            for item in batch:
                member_id = item['member_id']
                tag = item['tag']
                join_time = item['join_time']
                clean_username = tag[1:] if tag.startswith('@') else tag.split('#')[0] if '#' in tag else tag
                join_str = join_time.strftime("%m-%d-%Y %I:%M %p")
                fields.append({
                    "name": "New Member",
                    "value": (
                        f"**Full Tag (copy):** `{tag}`\n"
                        f"**Profile:** [{clean_username}](https://discord.com/users/{member_id})\n"
                        f"**ID:** `{member_id}`\n"
                        f"**Joined:** {join_str}"
                    ),
                    "inline": False
                })
            embed = {
                "color": 161791,
                "author": {"name": f"Snitched Successful ({len(batch)} new members)"},
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "fields": fields,
                "footer": {"text": f"Guild: {guild_name}"}
            }
            payload = {"embeds": [embed]}
            webhook_limiter.acquire()
            response = requests.post(webhook, json=payload)
            if response.status_code == 204:
                logging.info(f"✅ Batch webhook sent for {len(batch)} members in guild {guild_id}.")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Batch rate limited, waiting {wait_time}s...")
                time.sleep(wait_time + random.uniform(0, 0.5))
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Batch webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Batch webhook exception: {e}")
            attempt += 1
            time.sleep((2 ** attempt) + random.uniform(0, 1))

# ---------- Guild failure tracking ----------
guild_failure_counts = defaultdict(int)
guild_skip_until = {}

def should_skip_guild(guild_id):
    if guild_id in guild_skip_until and time.time() < guild_skip_until[guild_id]:
        return True
    return False

def record_guild_failure(guild_id):
    guild_failure_counts[guild_id] += 1
    if guild_failure_counts[guild_id] >= GUILD_FAILURE_THRESHOLD:
        guild_skip_until[guild_id] = time.time() + GUILD_FAILURE_SKIP_SECONDS
        logging.warning(f"[Guild {guild_id}] Skipped for {GUILD_FAILURE_SKIP_SECONDS/60:.0f} minutes due to {GUILD_FAILURE_THRESHOLD} consecutive failures.")

def record_guild_success(guild_id):
    guild_failure_counts[guild_id] = 0
    if guild_id in guild_skip_until:
        del guild_skip_until[guild_id]

# ---------- Processing ----------
def process_new_members(guild_id, new_members_dict):
    if not new_members_dict:
        return
    now = datetime.datetime.now(datetime.timezone.utc)
    pending = []
    if guild_id not in notified_members:
        notified_members[guild_id] = set()
    guild_notified = notified_members[guild_id]

    for member_id, (tag, joined_at) in new_members_dict.items():
        if not joined_at:
            logging.info(f"[Guild {guild_id}] Missing joined_at for {member_id}, fetching via API...")
            joined_at = fetch_member_joined_at(guild_id, member_id)
            if not joined_at:
                logging.warning(f"[Guild {guild_id}] Could not fetch joined_at for {member_id}, skipping.")
                continue
        if not isinstance(joined_at, str):
            continue
        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= JOIN_WINDOW_SECONDS:
                if member_id in guild_notified:
                    continue
                pending.append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time
                })
                guild_notified.add(member_id)
            else:
                logging.debug(f"[Guild {guild_id}] Member {member_id} joined {age/3600:.1f} hours ago, skipping.")
        except Exception as e:
            logging.warning(f"[Guild {guild_id}] Error processing {member_id}: {e}")

    if not pending:
        logging.info(f"[Guild {guild_id}] No new members within 2‑day window.")
        return

    # Send webhooks
    if len(pending) <= INDIVIDUAL_THRESHOLD:
        logging.info(f"[Guild {guild_id}] 📨 Sending {len(pending)} members individually.")
        for item in pending:
            send_single_webhook(guild_id, item['member_id'], item['tag'], item['join_time'])
            time.sleep(random.uniform(1.0, 3.0))
    else:
        logging.info(f"[Guild {guild_id}] 📦 Sending {len(pending)} members in batches of {BATCH_SIZE}.")
        for i in range(0, len(pending), BATCH_SIZE):
            batch = pending[i:i+BATCH_SIZE]
            send_batch_webhook(guild_id, batch)
            time.sleep(random.uniform(1.0, 3.0))

    # Enqueue friend requests (background sender will handle them)
    enqueue_friend_requests_for_guild(guild_id, pending)

    save_notified_cache()
    logging.info(f"[Guild {guild_id}] ✅ Finished processing new members.")

def fetch_member_joined_at(guild_id, user_id):
    try:
        limiter = get_rest_limiter(guild_id)
        limiter.acquire()
        sess = get_session()
        resp = sess.get(f'https://discord.com/api/v9/guilds/{guild_id}/members/{user_id}')
        if resp.status_code == 200:
            return resp.json().get('joined_at')
        else:
            logging.warning(f"[Guild {guild_id}] API fetch for {user_id} returned {resp.status_code}")
            return None
    except Exception as e:
        logging.error(f"[Guild {guild_id}] Error fetching member {user_id}: {e}")
        return None

# ---------- Stats logging ----------
def log_stats():
    total_enqueued = friend_queue.total_size()
    logging.info(f"📊 Stats: Friend requests enqueued: {total_enqueued}")
    for guild_id, _ in guild_channel_pairs:
        failures = guild_failure_counts.get(guild_id, 0)
        skipped = should_skip_guild(guild_id)
        logging.info(f"  Guild {guild_id}: failures={failures}, skipped={skipped}")

# ---------- Health Check Server ----------
def run_health_server():
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not guild_channel_pairs:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b"Service Unavailable: No guilds configured")
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            def do_HEAD(self):
                self.send_response(200)
                self.end_headers()
        port = int(os.environ.get('PORT', 10000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        logging.warning(f"Health server error: {e}")

# ---------- Scan a single guild ----------
previous_members = {}  # defined in main scope

def scan_guild(guild_id, channel_id):
    global previous_members
    if should_skip_guild(guild_id):
        logging.info(f"[Guild {guild_id}] Skipped due to previous failures.")
        return
    logging.info(f"[Guild {guild_id}] Scanning (channel {channel_id})...")
    current_members = fetch_all_members(guild_id, channel_id)
    if current_members is None:
        record_guild_failure(guild_id)
        logging.error(f"[Guild {guild_id}] Failed to fetch members.")
        return
    record_guild_success(guild_id)
    # Process differences
    prev = previous_members.get(guild_id, {})
    prev_ids = set(prev.keys())
    curr_ids = set(current_members.keys())
    diff_ids = curr_ids - prev_ids
    if diff_ids:
        diff_dict = {uid: current_members[uid] for uid in diff_ids}
        logging.info(f"[Guild {guild_id}] Found {len(diff_dict)} new IDs not in previous scan.")
        process_new_members(guild_id, diff_dict)
    else:
        logging.info(f"[Guild {guild_id}] No new members detected.")
    previous_members[guild_id] = current_members

# ---------- Webhook readiness check ----------
def wait_for_webhook_ready():
    logging.info("Checking webhook availability...")
    attempt = 0
    wait_time = 2
    while True:
        try:
            payload = {"content": "Startup check"}
            response = requests.post(webhook, json=payload, timeout=10)
            if response.status_code == 204:
                logging.info("✅ Webhook is ready.")
                return True
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Webhook rate-limited on startup, waiting {wait_time}s...")
                time.sleep(wait_time + random.uniform(0, 0.5))
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.warning(f"Webhook check returned {response.status_code}. Proceeding anyway.")
                return True
        except Exception as e:
            logging.warning(f"Webhook check exception: {e}. Proceeding anyway.")
            return True

# ---------- Main ----------
if __name__ == '__main__':
    # Load config first (this also calls validate_configuration)
    load_config()

    logging.info("Starting multi‑guild snitch (swap interval %ds)...", scan_interval)
    threading.Thread(target=run_health_server, daemon=True).start()
    logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))

    # Start friend request workers for each unique token
    for token in set(friend_tokens.values()):
        threading.Thread(target=friend_request_worker, args=(token,), daemon=True).start()
    logging.info("Friend request background workers started.")

    webhook_mask = webhook[:40] + "..." if len(webhook) > 40 else webhook
    logging.info("Configuration: %d guild(s), webhook: %s", len(guild_channel_pairs), webhook_mask)
    for g, c in guild_channel_pairs:
        ft = friend_tokens.get(g, "None")
        logging.info(f"  Guild {g} → channel {c} | friend token: {'set' if ft != 'None' else 'not set'}")

    wait_for_webhook_ready()

    previous_members = {}

    # Initial baseline – scan all guilds sequentially (to avoid overwhelming)
    for guild_id, channel_id in guild_channel_pairs:
        logging.info(f"Building initial baseline for guild {guild_id}...")
        scan_guild(guild_id, channel_id)
        if guild_id != guild_channel_pairs[-1][0]:
            logging.info(f"Waiting {scan_interval}s before next guild initial scan...")
            time.sleep(scan_interval + random.uniform(0, 10))

    # Main loop – scan in parallel
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCANS) as executor:
        while True:
            futures = []
            for guild_id, channel_id in guild_channel_pairs:
                if not should_skip_guild(guild_id):
                    futures.append(executor.submit(scan_guild, guild_id, channel_id))
                else:
                    logging.info(f"[Guild {guild_id}] Skipped this cycle (failure cooldown).")
            # Wait for all scans to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Scan thread error: {e}")
            log_stats()
            logging.info("Completed a full cycle. Waiting before next cycle...")
            time.sleep(scan_interval + random.uniform(0, 300))  # add jitter up to 5 min
