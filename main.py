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
from threading import Semaphore
from urllib.parse import urlparse

# ---------- RateLimiter class (MUST be defined before any usage) ----------
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

# ---------- Read configuration (multi-format) ----------
def load_config():
    global token, webhook, proxy, blacklistedRoles, blacklistedUsers, scan_interval, BATCH_SIZE, INDIVIDUAL_THRESHOLD
    global guild_channel_pairs

    token = None
    webhook = None
    proxy = ''
    blacklistedRoles = []
    blacklistedUsers = []
    scan_interval = 1800
    BATCH_SIZE = 20
    INDIVIDUAL_THRESHOLD = 5
    guild_channel_pairs = []

    # Try environment variables first
    if 'DISCORD_TOKEN' in os.environ:
        token = os.environ.get('DISCORD_TOKEN')
        webhook = os.environ.get('DISCORD_WEBHOOK')
        proxy = os.environ.get('DISCORD_PROXY', '')
        blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
        blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
        scan_interval = int(os.environ.get('SCAN_INTERVAL', '1800'))
        BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
        INDIVIDUAL_THRESHOLD = int(os.environ.get('INDIVIDUAL_THRESHOLD', '5'))

        pairs_dict = {}

        # Format 1: DISCORD_GUILDS = "guild1:ch1,ch2;guild2:ch3"
        if 'DISCORD_GUILDS' in os.environ:
            raw = os.environ['DISCORD_GUILDS']
            for entry in raw.split(';'):
                entry = entry.strip()
                if not entry or ':' not in entry:
                    continue
                guild_part, channels_part = entry.split(':', 1)
                guild = guild_part.strip()
                channels = [c.strip() for c in channels_part.split(',') if c.strip()]
                if not guild or not channels:
                    continue
                if guild not in pairs_dict:
                    pairs_dict[guild] = channels[0]  # use first channel
                else:
                    logging.warning(f"Duplicate guild ID {guild} – using first channel only.")

        # Format 2: DISCORD_GUILD_CONFIG = JSON array
        elif 'DISCORD_GUILD_CONFIG' in os.environ:
            config_list = json.loads(os.environ['DISCORD_GUILD_CONFIG'])
            for entry in config_list:
                g = entry.get('guildId') or entry.get('guild')
                channels = entry.get('channels') or entry.get('channelIds') or []
                if not g or not channels:
                    continue
                if g not in pairs_dict:
                    pairs_dict[g] = channels[0]
                else:
                    logging.warning(f"Duplicate guild ID {g} – using first channel only.")

        # Format 3: parallel lists
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
                else:
                    logging.warning(f"Duplicate guild ID {g} – using first channel only.")

        if pairs_dict:
            guild_channel_pairs = list(pairs_dict.items())
        else:
            raise ValueError("No guild configuration found. Provide DISCORD_GUILDS, DISCORD_GUILD_CONFIG, or DISCORD_GUILD_IDS+CHANNEL_IDS.")

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

            if 'guilds' in config and isinstance(config['guilds'], list):
                pairs_dict = {}
                for item in config['guilds']:
                    g = item.get('guildId') or item.get('guild')
                    channels = item.get('channels') or item.get('channelIds') or []
                    if not g or not channels:
                        continue
                    if g not in pairs_dict:
                        pairs_dict[g] = channels[0]
                guild_channel_pairs = list(pairs_dict.items())
            else:
                # old single-guild style
                g = config.get('guildID') or config.get('guildId')
                c = config.get('channelId') or config.get('channelIDs')
                if isinstance(g, list): g = g[0]
                if isinstance(c, list): c = c[0]
                if g and c:
                    guild_channel_pairs = [(g, c)]
        except FileNotFoundError:
            raise ValueError("No configuration found. Set environment variables or provide config.json.")

    if not token:
        raise ValueError("DISCORD_TOKEN is not set.")
    if not webhook:
        raise ValueError("DISCORD_WEBHOOK is not set.")
    if not guild_channel_pairs:
        raise ValueError("No guild-channel pairs configured.")

load_config()

logging.basicConfig(
    level=logging.INFO,
    format="\x1b[38;5;9m[\x1b[0m%(asctime)s\x1b[38;5;9m]\x1b[0m %(message)s\x1b[0m",
    datefmt="%H:%M:%S"
)

JOIN_WINDOW_SECONDS = 2 * 24 * 60 * 60
NOTIFIED_CACHE_FILE = "notified_members.pkl"

if os.path.exists(NOTIFIED_CACHE_FILE):
    with open(NOTIFIED_CACHE_FILE, 'rb') as f:
        notified_members = pickle.load(f)
else:
    notified_members = {}

def save_notified_cache():
    with open(NOTIFIED_CACHE_FILE, 'wb') as f:
        pickle.dump(notified_members, f)

# ---------- Per‑guild rate limiters ----------
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

# ---------- Proxy validation ----------
def is_valid_proxy_host(hostname):
    ipv4_re = r'^(\d{1,3}\.){3}\d{1,3}$'
    if re.match(ipv4_re, hostname):
        parts = hostname.split('.')
        return all(0 <= int(p) <= 255 for p in parts)
    domain_re = r'^(?=.{1,253}$)(?!-)(?:[a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,63}$'
    return bool(re.match(domain_re, hostname))

# ---------- Global Session ----------
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

# ---------- REST member fetch ----------
def fetch_all_members_rest(guild_id, max_retries=3):
    members = {}
    after = '0'
    retry_count = 0
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
        except Exception as e:
            logging.error(f"[Guild {guild_id}] REST fetch error: {e}")
            retry_count += 1
            if retry_count > max_retries:
                break
            time.sleep((2 ** retry_count) + random.uniform(0, 1))
    return members

# ---------- WebSocket fallback ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_id):
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
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

    def run(self, timeout=30):
        timer = threading.Timer(timeout, self.close)
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
            logging.info(f"[Guild {guild_id}] WS scanning channel {channel_id} (attempt {attempt+1}/{max_retries}) ...")
            sb = DiscordSocket(token, guild_id, channel_id)
            result = sb.run(timeout=30)
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

# ---------- Webhook sending ----------
def send_single_webhook(guild_id, member_id, tag, join_time, max_retries=3):
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

# ---------- Startup webhook check ----------
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

# ---------- Health Check Server ----------
def run_health_server():
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
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

# ---------- Main ----------
if __name__ == '__main__':
    logging.info("Starting multi‑guild snitch (swap interval %ds)...", scan_interval)
    threading.Thread(target=run_health_server, daemon=True).start()
    logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))

    webhook_mask = webhook[:40] + "..." if len(webhook) > 40 else webhook
    logging.info("Configuration: %d guild(s), webhook: %s", len(guild_channel_pairs), webhook_mask)
    for g, c in guild_channel_pairs:
        logging.info(f"  Guild {g} → channel {c}")

    wait_for_webhook_ready()

    previous_members = {}  # guild_id -> {user_id: (tag, joined_at)}

    # Initial baseline: scan all guilds once
    for guild_id, channel_id in guild_channel_pairs:
        logging.info(f"Building initial baseline for guild {guild_id}...")
        members = fetch_all_members(guild_id, channel_id)
        if members:
            previous_members[guild_id] = members
            logging.info(f"Baseline for guild {guild_id}: {len(members)} members.")
            process_new_members(guild_id, members)
        else:
            logging.warning(f"Failed to fetch initial members for guild {guild_id}. Skipping.")
            previous_members[guild_id] = {}
        # Wait between initial scans too
        if guild_id != guild_channel_pairs[-1][0]:
            logging.info(f"Waiting {scan_interval}s before next guild initial scan...")
            time.sleep(scan_interval + random.uniform(0, 10))

    # Main rotation loop
    while True:
        for guild_id, channel_id in guild_channel_pairs:
            logging.info(f"Scanning guild {guild_id} (channel {channel_id})...")
            current_members = fetch_all_members(guild_id, channel_id)
            if current_members is None:
                logging.error(f"Failed to fetch members for guild {guild_id}. Skipping this cycle.")
                time.sleep(scan_interval + random.uniform(0, 10))
                continue

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

            if guild_id != guild_channel_pairs[-1][0]:
                logging.info(f"Waiting {scan_interval}s before moving to next guild...")
                time.sleep(scan_interval + random.uniform(0, 10))

        logging.info("Completed a full cycle. Starting over after a short jitter...")
        time.sleep(random.uniform(5, 30))
