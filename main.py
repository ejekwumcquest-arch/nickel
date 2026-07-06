import logging, os, datetime, time, json, threading, requests, httpx, tls_client
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Read configuration ----------
if 'DISCORD_TOKEN' in os.environ:
    token = os.environ.get('DISCORD_TOKEN')
    guildId = os.environ.get('DISCORD_GUILD_ID')
    channelId = os.environ.get('DISCORD_CHANNEL_ID')
    webhook = os.environ.get('DISCORD_WEBHOOK')
    proxy = os.environ.get('DISCORD_PROXY', '')
    blacklistedRoles = json.loads(os.environ.get('DISCORD_BLACKLISTED_ROLES', '[]'))
    blacklistedUsers = json.loads(os.environ.get('DISCORD_BLACKLISTED_USERS', '[]'))
else:
    from json import load
    config = load(open('config.json'))
    guildId, channelId, proxy, token, webhook, blacklistedRoles, blacklistedUsers = (
        config['guildID'], config['channelId'], config['proxy'],
        config['token'], config['webhook'],
        config['blacklistedRoles'], config['blacklistedUsers']
    )

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

JOIN_WINDOW_SECONDS = 2 * 24 * 60 * 60    # 2 days

class Utils:
    # ... (keep all Utils methods same as before) ...
    # I'll keep them for brevity – copy from your previous script.

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
        self.members = {}  # user_id -> (tag, joined_at or None)
        self.ranges = [[0, 0]]
        self.lastRange = 0
        self.packets_recv = 0

    def run(self):
        self.run_forever()
        return self.members

    def scrapeUsers(self):
        if not self.endScraping:
            self.send('{"op":14,"d":{"guild_id":"' + self.guild_id + '","typing":true,"activities":true,"threads":true,"channels":{"' + self.channel_id + '":' + json.dumps(self.ranges) + '}}}')

    def sock_open(self, ws):
        self.send('{"op":2,"d":{"token":"' + self.token + '","capabilities":125,"properties":{"os":"Windows","browser":"Firefox","device":"","system_locale":"it-IT","browser_user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0","browser_version":"94.0","os_version":"10","referrer":"","referring_domain":"","referrer_current":"","referring_domain_current":"","release_channel":"stable","client_build_number":103981,"client_event_source":null},"presence":{"status":"online","since":0,"activities":[],"afk":false},"compress":false,"client_state":{"guild_hashes":{},"highest_last_message_id":"0","read_state_version":0,"user_guild_settings_version":-1,"user_settings_version":-1}}}')

    def heartbeatThread(self, interval):
        try:
            while True:
                self.send('{"op":1,"d":' + str(self.packets_recv) + '}')
                time.sleep(interval)
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
                threading.Thread(target=self.heartbeatThread, args=(decoded["d"]["heartbeat_interval"] / 1000,), daemon=True).start()

            if t == "READY":
                for guild in decoded.get("d", {}).get("guilds", []):
                    self.guilds[guild["id"]] = {"member_count": guild.get("member_count", 0)}

            if t == "READY_SUPPLEMENTAL":
                member_count = self.guilds.get(self.guild_id, {}).get("member_count", 0)
                if member_count:
                    self.ranges = Utils.getRanges(0, 100, member_count)
                    self.scrapeUsers()

            elif t == "GUILD_MEMBER_LIST_UPDATE":
                parsed = Utils.parseGuildMemberListUpdate(decoded)
                if parsed['guild_id'] == self.guild_id and ('SYNC' in parsed['types'] or 'UPDATE' in parsed['types']):
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
                                    if discrim != "0":
                                        tag = f"{username}#{discrim}"
                                    else:
                                        tag = f"@{username}"
                                    # Extract joined_at if available
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
                                    if discrim != "0":
                                        tag = f"{username}#{discrim}"
                                    else:
                                        tag = f"@{username}"
                                    joined_at = mem.get('joined_at')
                                    self.members[user_id] = (tag, joined_at)

                        self.lastRange += 1
                        self.ranges = Utils.getRanges(self.lastRange, 100, self.guilds.get(self.guild_id, {}).get("member_count", 0))
                        self.scrapeUsers()

                if self.endScraping:
                    self.close()

        except Exception as e:
            logging.error("Error in sock_message: %s", e)

    def sock_close(self, ws, close_code, close_msg):
        pass


def autoSnitch(token, guild_id, channel_id):
    sb = DiscordSocket(token, guild_id, channel_id)
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
        sess = session(token)
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

def process_new_members(new_members_dict, token):
    """new_members_dict: user_id -> (tag, joined_at or None)"""
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
                logging.info("✅ New member (within %.0f days): %s (%s)", JOIN_WINDOW_SECONDS/86400, member_id, tag)
                send_webhook(member_id, join_time, tag)
            else:
                logging.debug("Member %s joined %.1f days ago – skipped", member_id, age/86400)
        except Exception as e:
            logging.warning("Error processing %s: %s", member_id, e)

    logging.info("Finished processing %s new members.", total)


if __name__ == '__main__':
    logging.info("Starting scraper (10s interval, %.0f-day join window)...", JOIN_WINDOW_SECONDS/86400)

    # HTTP server for keep-alive
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
        def run_http_server():
            server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
            server.serve_forever()
        threading.Thread(target=run_http_server, daemon=True).start()
        logging.info("HTTP health check server started on port %s", os.environ.get('PORT', 10000))
    except Exception as e:
        logging.warning("Could not start HTTP server: %s", e)

    logging.info("Building initial baseline...")
    current_members_raw = autoSnitch(token, guildId, channelId)
    current_ids = set(current_members_raw.keys())
    logging.info("Baseline built: %s members visible.", len(current_ids))

    logging.info("Checking baseline members for recent joins...")
    process_new_members(current_members_raw, token)

    while True:
        new_members_raw = autoSnitch(token, guildId, channelId)
        new_ids = set(new_members_raw.keys())
        logging.info("Scanned: %s members visible.", len(new_ids))

        diff_ids = new_ids - current_ids
        if diff_ids:
            diff_dict = {uid: new_members_raw[uid] for uid in diff_ids}
            logging.info("Found %s new IDs not in previous scan.", len(diff_dict))
            process_new_members(diff_dict, token)

        current_ids = new_ids
        time.sleep(60)
