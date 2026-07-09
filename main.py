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

# ---------- Read configuration ----------
if 'DISCORD_TOKEN' in os.environ:
    token = os.environ.get('DISCORD_TOKEN')
    raw_guild = os.environ.get('DISCORD_GUILD_ID', '')
    if ',' in raw_guild:
        guildId = raw_guild.split(',')[0].strip()
        logging.warning(f"Multiple guild IDs detected, using the first one: {guildId}")
    else:
        guildId = raw_guild
    channel_id_env = os.environ.get('DISCORD_CHANNEL_ID', '')
    if ',' in channel_id_env:
        channelIds = [ch.strip() for ch in channel_id_env.split(',') if ch.strip()]
    else:
        channelIds = [channel_id_env] if channel_id_env else []
    channelIds = list(dict.fromkeys(channelIds))
    webhook = os.environ.get('DISCORD_WEBHOOK')
    proxy = os.environ.get('DISCORD_PROXY', '')
    blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
    blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
    scan_interval = int(os.environ.get('SCAN_INTERVAL', '300'))  # 5 min default
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
    INDIVIDUAL_THRESHOLD = int(os.environ.get('INDIVIDUAL_THRESHOLD', '5'))
else:
    from json import load
    config = load(open('config.json'))
    guildId = config.get('guildID')
    if isinstance(guildId, list):
        guildId = guildId[0]
        logging.warning(f"Multiple guild IDs in config, using first: {guildId}")
    if 'channelIds' in config:
        channelIds = config['channelIds']
    elif 'channelId' in config:
        channelIds = [config['channelId']]
    else:
        channelIds = []
    channelIds = list(dict.fromkeys(channelIds))
    token = config.get('token')
    webhook = config.get('webhook')
    proxy = config.get('proxy', '')
    blacklistedRoles = config.get('blacklistedRoles', [])
    blacklistedUsers = config.get('blacklistedUsers', [])
    scan_interval = config.get('scan_interval', 300)
    BATCH_SIZE = config.get('batch_size', 20)
    INDIVIDUAL_THRESHOLD = config.get('individual_threshold', 5)

if not token:
    raise ValueError("DISCORD_TOKEN is not set.")
if not guildId:
    raise ValueError("DISCORD_GUILD_ID is not set.")
if not channelIds:
    raise ValueError("No channel(s) provided (DISCORD_CHANNEL_ID or channelIds).")
if not webhook:
    raise ValueError("DISCORD_WEBHOOK is not set.")

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
    notified_members = set()

def save_notified_cache():
    with open(NOTIFIED_CACHE_FILE, 'wb') as f:
        pickle.dump(notified_members, f)

# ---------- Rate Limiter ----------
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
                sleep_time = self.period - (now - self.start) + 0.05
                time.sleep(max(0, sleep_time))
                self.start = time.time()
                self.calls = 0
            self.calls += 1

rest_limiter = RateLimiter(30, 1)
webhook_limiter = RateLimiter(5, 1)
ws_limiter = RateLimiter(10, 1)   # WebSocket requests (op 14) limited to 10/s

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

# ---------- REST member fetch (try first) ----------
def fetch_all_members_rest(guild_id, max_retries=2):
    members = {}
    after = '0'
    retry_count = 0
    while True:
        try:
            rest_limiter.acquire()
            sess = get_session()
            resp = sess.get(
                f'https://discord.com/api/v9/guilds/{guild_id}/members',
                params={'limit': 1000, 'after': after}
            )
            if resp.status_code == 429:
                retry_after = resp.json().get('retry_after', 2)
                logging.warning(f"REST rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            if resp.status_code == 403:
                # Missing Access – REST not allowed, return None to trigger fallback
                logging.warning("REST endpoint returned 403 (Missing Access) – falling back to WebSocket.")
                return None
            if resp.status_code != 200:
                logging.error(f"REST fetch failed: {resp.status_code} - {resp.text[:200]}")
                retry_count += 1
                if retry_count > max_retries:
                    break
                time.sleep(2 ** retry_count)
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
            logging.error(f"REST fetch error: {e}")
            retry_count += 1
            if retry_count > max_retries:
                break
            time.sleep(2 ** retry_count)
    return members

# ---------- WebSocket member fetch (fallback for user tokens) ----------
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

    def run(self, timeout=45):
        timer = threading.Timer(timeout, self.close)
        timer.daemon = True
        timer.start()
        self.run_forever()
        timer.cancel()
        return self.members

    def scrapeUsers(self):
        if self.endScraping:
            return
        ws_limiter.acquire()
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
                member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                if member_count == 0:
                    logging.warning(f"⚠️ Member count is 0 for channel {self.channel_id}. Closing socket.")
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
                        self.ranges = [[self.lastRange * 100, self.lastRange * 100 + 99]]
                        self.scrapeUsers()
                if self.endScraping:
                    self.close()
        except Exception as e:
            logging.error(f"WS error: {e}")

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
            logging.warning(f"Rate limit detected on channel {self.channel_id}.")

def fetch_all_members_via_websocket(guild_id, channel_ids):
    all_members = {}
    for ch_id in channel_ids:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logging.info(f"WS scanning channel {ch_id} (attempt {attempt+1}/{max_retries}) ...")
                sb = DiscordSocket(token, guild_id, ch_id)
                result = sb.run(timeout=45)
                if result:
                    logging.info(f"Channel {ch_id} returned {len(result)} members via WS.")
                    all_members.update(result)
                    break
                else:
                    if sb.rate_limited:
                        logging.warning(f"Rate limited on WS for channel {ch_id}. Waiting 60s.")
                        time.sleep(60)
                    else:
                        logging.warning(f"Channel {ch_id} returned 0 members. Retrying...")
                        time.sleep(2 * (attempt + 1))
            except Exception as e:
                logging.error(f"WS error for channel {ch_id}: {e}")
                time.sleep(2 ** attempt)
        # Delay between channels
        time.sleep(5)
    return all_members

# ---------- Unified member fetcher ----------
def fetch_all_members(guild_id, channel_ids):
    # Try REST first
    rest_members = fetch_all_members_rest(guild_id)
    if rest_members is not None:
        logging.info("REST fetch successful.")
        return rest_members
    # Fallback to WebSocket
    logging.info("Falling back to WebSocket scraping (user token).")
    return fetch_all_members_via_websocket(guild_id, channel_ids)

# ---------- Webhook Sending (unchanged) ----------
def send_single_webhook(member_id, tag, join_time, max_retries=3):
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            rest_limiter.acquire()
            guild_resp = get_session().get(f'https://discord.com/api/v9/guilds/{guildId}')
            guild_name = guild_resp.json().get('name', 'Unknown') if guild_resp.status_code == 200 else 'Unknown'
            if tag.startswith('@'):
                clean_username = tag[1:]
            elif '#' in tag:
                clean_username = tag.split('#')[0]
            else:
                clean_username = tag
            join_str = join_time.strftime("%m-%d-%Y on %I:%M %p")
            payload = {
                "content": f"@here New User Joined {guildId}",
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
                logging.info(f"✅ Webhook sent for {member_id}")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Webhook rate limited for {member_id}, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Webhook exception: {e}")
            attempt += 1
            time.sleep(2 ** attempt)

def send_batch_webhook(batch, max_retries=3):
    if not batch:
        return
    attempt = 0
    wait_time = 2
    while attempt <= max_retries:
        try:
            rest_limiter.acquire()
            guild_resp = get_session().get(f'https://discord.com/api/v9/guilds/{guildId}')
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
                logging.info(f"✅ Batch webhook sent for {len(batch)} members.")
                return
            elif response.status_code == 429:
                try:
                    data = response.json()
                    retry_after = data.get('retry_after', wait_time)
                except:
                    retry_after = wait_time
                wait_time = max(wait_time, retry_after)
                logging.warning(f"Batch rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
                wait_time = wait_time * 2
                continue
            else:
                logging.error(f"Batch webhook failed with status {response.status_code}: {response.text[:200]}")
                return
        except Exception as e:
            logging.error(f"Batch webhook exception: {e}")
            attempt += 1
            time.sleep(2 ** attempt)

# ---------- Processing ----------
def process_new_members(new_members_dict):
    if not new_members_dict:
        return
    now = datetime.datetime.now(datetime.timezone.utc)
    pending = []
    for member_id, (tag, joined_at) in new_members_dict.items():
        if not joined_at:
            logging.info(f"Missing joined_at for {member_id}, fetching via API...")
            joined_at = fetch_member_joined_at(member_id)
            if not joined_at:
                logging.warning(f"Could not fetch joined_at for {member_id}, skipping.")
                continue
        if not isinstance(joined_at, str):
            continue
        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= JOIN_WINDOW_SECONDS:
                if member_id in notified_members:
                    continue
                pending.append({
                    'member_id': member_id,
                    'tag': tag,
                    'join_time': join_time
                })
                notified_members.add(member_id)
            else:
                logging.debug(f"Member {member_id} joined {age/3600:.1f} hours ago, skipping.")
        except Exception as e:
            logging.warning(f"Error processing {member_id}: {e}")

    if not pending:
        logging.info("No new members within 2‑day window.")
        return

    if len(pending) <= INDIVIDUAL_THRESHOLD:
        logging.info(f"📨 Sending {len(pending)} members individually.")
        for item in pending:
            send_single_webhook(item['member_id'], item['tag'], item['join_time'])
            time.sleep(random.uniform(1.0, 3.0))
    else:
        logging.info(f"📦 Sending {len(pending)} members in batches of {BATCH_SIZE}.")
        for i in range(0, len(pending), BATCH_SIZE):
            batch = pending[i:i+BATCH_SIZE]
            send_batch_webhook(batch)
            time.sleep(random.uniform(1.0, 3.0))

    save_notified_cache()
    logging.info("✅ Finished processing new members.")

def fetch_member_joined_at(user_id):
    try:
        rest_limiter.acquire()
        sess = get_session()
        resp = sess.get(f'https://discord.com/api/v9/guilds/{guildId}/members/{user_id}')
        if resp.status_code == 200:
            return resp.json().get('joined_at')
        else:
            logging.warning(f"API fetch for {user_id} returned {resp.status_code}")
            return None
    except Exception as e:
        logging.error(f"Error fetching member {user_id}: {e}")
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
                time.sleep(wait_time)
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
    logging.info("Starting snitch (%ds interval, 2-day join window)...", scan_interval)
    threading.Thread(target=run_health_server, daemon=True).start()
    logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))

    webhook_mask = webhook[:40] + "..." if len(webhook) > 40 else webhook
    logging.info("Configuration: guildId=%s, channels=%s, token starts with %s..., webhook: %s",
                 guildId, channelIds, token[:8], webhook_mask)

    wait_for_webhook_ready()

    logging.info("Building initial baseline (fetching all members)...")
    current_members = fetch_all_members(guildId, channelIds)
    logging.info("Baseline built: %s members visible.", len(current_members))

    logging.info("Checking baseline members for recent joins...")
    process_new_members(current_members)

    while True:
        time.sleep(random.uniform(0, 5))
        logging.info("Fetching member list...")
        new_members = fetch_all_members(guildId, channelIds)
        logging.info("Fetched: %s members visible.", len(new_members))

        current_ids = set(current_members.keys())
        new_ids = set(new_members.keys())
        diff_ids = new_ids - current_ids
        if diff_ids:
            diff_dict = {uid: new_members[uid] for uid in diff_ids}
            logging.info("Found %s new IDs not in previous scan.", len(diff_dict))
            process_new_members(diff_dict)

        current_members = new_members
        logging.info("Sleeping %s seconds...", scan_interval)
        time.sleep(scan_interval)
