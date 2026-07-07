import logging, os, datetime, time, json, threading, requests, httpx, tls_client, pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Read configuration with robust parsing ----------
def safe_json_parse(value, fallback=None):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except:
        if ',' in value:
            return [x.strip().strip('"').strip("'") for x in value.split(',') if x.strip()]
        if value.startswith('[') and value.endswith(']'):
            cleaned = value.replace("'", '"')
            try:
                return json.loads(cleaned)
            except:
                pass
        return fallback

if 'DISCORD_TOKEN' in os.environ:
    token = os.environ.get('DISCORD_TOKEN')
    guildId = os.environ.get('DISCORD_GUILD_ID')
    channel_ids = safe_json_parse(os.environ.get('DISCORD_CHANNEL_IDS'), [])
    if not channel_ids and os.environ.get('DISCORD_CHANNEL_ID'):
        channel_ids = [os.environ.get('DISCORD_CHANNEL_ID')]
    webhook = os.environ.get('DISCORD_WEBHOOK')
    proxy = os.environ.get('DISCORD_PROXY', '')
    blacklistedRoles = safe_json_parse(os.environ.get('DISCORD_BLACKLISTED_ROLES'), [])
    blacklistedUsers = safe_json_parse(os.environ.get('DISCORD_BLACKLISTED_USERS'), [])
    scan_interval = int(os.environ.get('SCAN_INTERVAL', '60'))
    tokens = safe_json_parse(os.environ.get('DISCORD_TOKENS'), [])
    if not tokens and token:
        tokens = [token]
    baseline_timeout = int(os.environ.get('BASELINE_TIMEOUT', '120'))
else:
    from json import load
    config = load(open('config.json'))
    guildId = config['guildID']
    channel_ids = config.get('channelIds', [config.get('channelId')])
    tokens = config.get('tokens', [config.get('token')])
    webhook = config['webhook']
    proxy = config.get('proxy', '')
    blacklistedRoles = config.get('blacklistedRoles', [])
    blacklistedUsers = config.get('blacklistedUsers', [])
    scan_interval = 60
    baseline_timeout = 120

if not isinstance(channel_ids, list):
    channel_ids = [channel_ids] if channel_ids else []
if not isinstance(tokens, list):
    tokens = [tokens] if tokens else []
channel_ids = [c for c in channel_ids if c]
tokens = [t for t in tokens if t]

if not channel_ids:
    raise ValueError("No channel IDs provided. Set DISCORD_CHANNEL_IDS or DISCORD_CHANNEL_ID.")
if not tokens:
    raise ValueError("No tokens provided. Set DISCORD_TOKENS or DISCORD_TOKEN.")

try:
    import websocket
except:
    os.system("pip install websocket-client")
    import websocket

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

# ---------- Utils ----------
class Utils:
    def rangeCorrector(ranges):
        if [0, 99] not in ranges:
            ranges.insert(0, [0, 99])
        return ranges

    def getRanges(index, multiplier, memberCount):
        initialNum = int(index*multiplier)
        rangesList = [[initialNum, initialNum+99]]
        if memberCount > initialNum+99:
            rangesList.append([initialNum+100, initialNum+199])
        return Utils.rangeCorrector(rangesList)

    def parseGuildMemberListUpdate(response):
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

# ---------- DiscordSocket with timeout thread ----------
class DiscordSocket(websocket.WebSocketApp):
    def __init__(self, token, guild_id, channel_ids, timeout_seconds=baseline_timeout):
        self.token = token
        self.guild_id = guild_id
        self.channel_ids = channel_ids if isinstance(channel_ids, list) else [channel_ids]
        self.blacklisted_roles = [str(r) for r in blacklistedRoles]
        self.blacklisted_users = [str(u) for u in blacklistedUsers]
        self.timeout_seconds = timeout_seconds

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
        self.ranges = [[0, 0]]
        self.lastRange = 0
        self.packets_recv = 0
        self.total_member_count = 0
        self.channels_completed = 0
        self.start_time = time.time()
        self.received_events = []
        self.timeout_triggered = False

        # Start a background thread to force timeout
        self.timeout_thread = threading.Thread(target=self._timeout_monitor, daemon=True)
        self.timeout_thread.start()

    def _timeout_monitor(self):
        """Force close the socket after timeout_seconds regardless of events."""
        time.sleep(self.timeout_seconds)
        if not self.endScraping:
            logging.warning(f"⏱️ Baseline timeout ({self.timeout_seconds}s) reached – forcing completion.")
            self.timeout_triggered = True
            self.endScraping = True
            if hasattr(self, 'ws') and self.ws:
                self.ws.close()
            else:
                self.close()

    def run(self):
        self.run_forever()
        return self.members

    def scrapeUsers(self):
        if self.endScraping:
            return
        if not self.ranges or not self.ranges[0]:
            logging.warning("Ranges empty, not scraping.")
            return
        channels_payload = {cid: self.ranges for cid in self.channel_ids}
        payload = {
            "op": 14,
            "d": {
                "guild_id": self.guild_id,
                "typing": True,
                "activities": True,
                "threads": True,
                "channels": channels_payload
            }
        }
        self.send(json.dumps(payload))

    def sock_open(self, ws):
        self.ws = ws
        self.send('{"op":2,"d":{"token":"' + self.token + '","capabilities":125,"properties":{"os":"Windows","browser":"Firefox","device":"","system_locale":"it-IT","browser_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0","browser_version":"94.0","os_version":"10","referrer":"","referring_domain":"","referrer_current":"","referring_domain_current":"","release_channel":"stable","client_build_number":103981,"client_event_source":null},"presence":{"status":"online","since":0,"activities":[],"afk":false},"compress":false,"client_state":{"guild_hashes":{},"highest_last_message_id":"0","read_state_version":0,"user_guild_settings_version":-1,"user_settings_version":-1}}}')

    def heartbeatThread(self, interval):
        try:
            while True:
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
                time.sleep(interval)
        except Exception:
            return

    def sock_message(self, ws, message):
        if self.endScraping:
            return
        try:
            decoded = json.loads(message)
            if not isinstance(decoded, dict):
                return

            op = decoded.get("op")
            t = decoded.get("t")

            if t:
                logging.debug(f"Received event: {t}")
                self.received_events.append(t)

            if op != 11:
                self.packets_recv += 1

            if op == 10:
                threading.Thread(target=self.heartbeatThread, args=(decoded["d"]["heartbeat_interval"] / 1000,), daemon=True).start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}
                self.total_member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                logging.info(f"READY: Guild {self.guild_id} has {self.total_member_count} members")

            if t == "READY_SUPPLEMENTAL":
                logging.info("READY_SUPPLEMENTAL received")
                if self.total_member_count:
                    self.ranges = Utils.getRanges(0, 100, self.total_member_count)
                    self.scrapeUsers()
                else:
                    logging.warning("No member count, using default ranges.")
                    self.ranges = Utils.getRanges(0, 100, 1000)
                    self.scrapeUsers()

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                logging.info("GUILD_MEMBER_LIST_UPDATE received")
                parsed = Utils.parseGuildMemberListUpdate(decoded)
                if parsed['guild_id'] != self.guild_id:
                    logging.debug("Guild mismatch, ignoring")
                    return

                channel_id = parsed.get('id')
                if channel_id not in self.channel_ids:
                    logging.debug(f"Update for channel {channel_id} not in our list, ignoring")
                    return

                # Log the types we see
                logging.debug(f"Types in update: {parsed['types']}")

                if 'SYNC' in parsed['types'] or 'UPDATE' in parsed['types']:
                    for elem, index in enumerate(parsed["types"]):
                        updates = parsed["updates"][elem]
                        # Convert to list if needed
                        if isinstance(updates, dict):
                            updates = [updates]
                        elif not isinstance(updates, list):
                            updates = []

                        logging.debug(f"Processing {len(updates)} items for {index}")

                        if index == "SYNC":
                            if len(updates) == 0:
                                self.channels_completed += 1
                                logging.info(f"Channel {channel_id} completed ({self.channels_completed}/{len(self.channel_ids)})")
                                if self.channels_completed >= len(self.channel_ids):
                                    self.endScraping = True
                                    self.close()
                                break
                            for item in updates:
                                # Try to extract member
                                mem = item.get('member')
                                if not mem:
                                    # Some updates might have a different structure
                                    logging.warning("No 'member' key in item, skipping")
                                    continue
                                user = mem.get('user', {})
                                if not user:
                                    continue
                                user_id = user.get('id')
                                if not user_id:
                                    continue
                                if set(self.blacklisted_roles).intersection(mem.get('roles', [])):
                                    continue
                                if user.get('bot'):
                                    continue
                                if user_id in self.blacklisted_users:
                                    continue
                                username = user.get('username', 'Unknown')
                                discrim = user.get('discriminator', '0')
                                if discrim != "0":
                                    tag = f"{username}#{discrim}"
                                else:
                                    tag = f"@{username}"
                                joined_at = mem.get('joined_at')
                                if user_id not in self.members:
                                    self.members[user_id] = (tag, joined_at)
                                    logging.debug(f"Added member {user_id} ({tag})")

                        elif index == "UPDATE":
                            for item in updates:
                                mem = item.get('member')
                                if not mem:
                                    logging.warning("No 'member' key in UPDATE item, skipping")
                                    continue
                                user = mem.get('user', {})
                                if not user:
                                    continue
                                user_id = user.get('id')
                                if not user_id:
                                    continue
                                if set(self.blacklisted_roles).intersection(mem.get('roles', [])):
                                    continue
                                if user.get('bot'):
                                    continue
                                if user_id in self.blacklisted_users:
                                    continue
                                username = user.get('username', 'Unknown')
                                discrim = user.get('discriminator', '0')
                                if discrim != "0":
                                    tag = f"{username}#{discrim}"
                                else:
                                    tag = f"@{username}"
                                joined_at = mem.get('joined_at')
                                if user_id not in self.members:
                                    self.members[user_id] = (tag, joined_at)
                                    logging.debug(f"Added member {user_id} ({tag})")

                    if not self.endScraping:
                        self.lastRange += 1
                        self.ranges = Utils.getRanges(self.lastRange, 100, self.total_member_count)
                        self.scrapeUsers()

            # If timeout was triggered by background thread, close immediately
            if self.timeout_triggered:
                self.endScraping = True
                self.close()

        except Exception as e:
            logging.error(f"Error in sock_message: {e}")

    def sock_close(self, ws, close_code, close_msg):
        logging.info(f"WebSocket closed: {close_code} - {close_msg}")
        if self.timeout_triggered or self.endScraping:
            return

def autoSnitch(token, guild_id, channel_ids):
    sb = DiscordSocket(token, guild_id, channel_ids)
    return sb.run()

def rotateProxy():
    if proxy:
        return {'http': 'http://%s' % proxy, 'https': 'http://%s' % proxy}
    return None

def session(token):
    sess = tls_client.Session(client_identifier='chrome_105')
    sess.headers.update({
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
    return sess

def send_webhook(member_id, join_time, tag):
    try:
        sess = session(tokens[0])
        guild_resp = sess.get(f'https://discord.com/api/v9/guilds/{guildId}')
        guild_name = guild_resp.json().get('name', 'Unknown')
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
                    {"name": "Username", "value": clean_username, "inline": True},
                    {"name": "User ID", "value": member_id, "inline": True},
                    {"name": "Joined Server", "value": join_str, "inline": False},
                    {"name": "Mention", "value": f"<@{member_id}>", "inline": True},
                    {"name": "Guild", "value": guild_name, "inline": True}
                ]
            }]
        }
        requests.post(webhook, json=payload)
        logging.info("Webhook sent for %s", member_id)
    except Exception as e:
        logging.error("Webhook failed for %s: %s", member_id, e)

def process_new_members(new_members_dict):
    if not new_members_dict:
        return
    total = len(new_members_dict)
    logging.info("Processing %s new members...", total)
    now = datetime.datetime.now(datetime.timezone.utc)
    for member_id, (tag, joined_at) in new_members_dict.items():
        if not joined_at:
            logging.debug("Member %s has no joined_at, skipping", member_id)
            continue
        try:
            join_time = datetime.datetime.fromisoformat(joined_at.replace('Z', '+00:00'))
            age = (now - join_time).total_seconds()
            if age <= JOIN_WINDOW_SECONDS:
                if member_id in notified_members:
                    logging.debug("Member %s already notified, skipping.", member_id)
                    continue
                logging.info("✅ New member (within 2 days): %s (%s)", member_id, tag)
                send_webhook(member_id, join_time, tag)
                notified_members.add(member_id)
                save_notified_cache()
            else:
                logging.debug("Member %s joined %.1f days ago – skipped", member_id, age/86400)
        except Exception as e:
            logging.warning("Error processing %s: %s", member_id, e)
    logging.info("Finished processing %s new members.", total)

# ---------- Main ----------
if __name__ == '__main__':
    logging.info("Starting improved snitch (%ds interval, %d channel(s), %d token(s))",
                 scan_interval, len(channel_ids), len(tokens))
    logging.info("Channels: %s", channel_ids)
    logging.info("Tokens: %s", [t[:8]+"..." for t in tokens])

    # HTTP keep‑alive
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
        def run_http_server():
            server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
            server.serve_forever()
        threading.Thread(target=run_http_server, daemon=True).start()
        logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))
    except Exception as e:
        logging.warning("Could not start HTTP server: %s", e)

    logging.info("Building initial baseline (timeout %ds)...", baseline_timeout)
    combined_members = {}

    # Try with all channels
    for i, t in enumerate(tokens):
        try:
            logging.info("Token %s scanning baseline...", t[:8])
            members = autoSnitch(t, guildId, channel_ids)
            logging.info(f"Token {t[:8]} returned {len(members)} members")
            for uid, data in members.items():
                if uid not in combined_members:
                    combined_members[uid] = data
        except Exception as e:
            logging.error("Token %s baseline failed: %s", t[:8], e)
        time.sleep(1)

    # If no members were collected, fall back to first channel only
    if len(combined_members) == 0:
        logging.warning("⚠️ Baseline with all channels returned 0 members. Retrying with first channel only...")
        fallback_channel = channel_ids[0]
        for i, t in enumerate(tokens):
            try:
                logging.info("Token %s scanning fallback (single channel)...", t[:8])
                members = autoSnitch(t, guildId, [fallback_channel])
                logging.info(f"Token {t[:8]} returned {len(members)} members (fallback)")
                for uid, data in members.items():
                    if uid not in combined_members:
                        combined_members[uid] = data
            except Exception as e:
                logging.error("Token %s fallback failed: %s", t[:8], e)
            time.sleep(1)

    current_ids = set(combined_members.keys())
    logging.info("Combined baseline: %s unique members visible.", len(current_ids))

    logging.info("Checking baseline members for recent joins...")
    process_new_members(combined_members)

    while True:
        all_new_members = {}
        for i, t in enumerate(tokens):
            try:
                members = autoSnitch(t, guildId, channel_ids)
                for uid, data in members.items():
                    if uid not in all_new_members:
                        all_new_members[uid] = data
            except Exception as e:
                logging.error("Token %s scan failed: %s", t[:8], e)
            time.sleep(0.5)

        new_ids = set(all_new_members.keys()) - current_ids
        if new_ids:
            diff_dict = {uid: all_new_members[uid] for uid in new_ids}
            logging.info("Found %s new IDs across all tokens.", len(diff_dict))
            process_new_members(diff_dict)
            current_ids.update(new_ids)
        else:
            logging.debug("No new IDs found.")

        save_notified_cache()
        logging.info("Sleeping %s seconds...", scan_interval)
        time.sleep(scan_interval)
