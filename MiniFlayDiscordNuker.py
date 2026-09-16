import asyncio
import getpass
import importlib.util
import itertools
import os
import random
import signal
import sys
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import httpx

RESET = "\033[0m"
BOLD = "\033[1m"
GRAY = "\033[90m"
WHITE = "\033[37m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
ORANGE = "\033[38;5;208m"
PURPLE = "\033[38;5;135m"
PINK = "\033[38;5;213m"
TEAL = "\033[38;5;51m"
LIME = "\033[38;5;154m"

API_BASE = "https://discord.com/api/v10"

MAX_RETRIES = 5
INITIAL_WORKERS = 24
MIN_WORKERS = 12
MAX_WORKERS = 40

JITTER_MIN = 0.003
JITTER_MAX = 0.020
BACKOFF_BASE = 0.10
BACKOFF_MAX = 2.5

CONNECT_TIMEOUT = 3.5
READ_TIMEOUT = 10.0
WRITE_TIMEOUT = 10.0
POOL_TIMEOUT = 3.5
MAX_CONNECTIONS = 120
MAX_KEEPALIVE = 60
KEEPALIVE_EXPIRY = 30.0

PROGRESS_INTERVAL = 0.35
AUTOTUNE_INTERVAL = 1.2
AUTOTUNE_LOG_COOLDOWN = 8.0

GLOBAL_RL_CAP = 90.0
QUEUE_TIMEOUT = 1.0
STAGGER_MAX = 0.04
SLEEP_SLICE = 0.25

TUNE_UP_STEP = 3
TUNE_DOWN_FACTOR = 0.75
TUNE_RL_THRESHOLD = 0.25
TUNE_SUCCESS_THRESHOLD = 15

PERM_ADMIN = 1 << 3
PERM_CHANNELS = 1 << 4
PERM_GUILD = 1 << 5
PERM_ROLES = 1 << 28
PERM_WEBHOOKS = 1 << 29
PERM_EXPRESSIONS = 1 << 30

stats: Dict[str, Any] = {
    "done": 0,
    "already": 0,
    "failed": 0,
    "retries": 0,
    "rate_limits": 0,
    "abandoned": 0,
    "start_time": 0.0,
}

stats_lock: Optional[asyncio.Lock] = None
stop_event: Optional[asyncio.Event] = None


def safe_print(text: str, end: str = "\n") -> None:
    try:
        print(text, end=end, flush=True)
    except UnicodeEncodeError:
        try:
            print(text.encode("ascii", "ignore").decode("ascii"), end=end, flush=True)
        except Exception:
            pass
    except Exception:
        pass


def clear_screen() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def draw_box(title: str, message: str, color: str = CYAN) -> None:
    safe_print("\r\033[K", end="")
    safe_print("")
    safe_print(
        f"{PURPLE}┌─ {color}{BOLD}{title}{RESET} "
        f"{PURPLE}───────────────────────────────────────────────────┐{RESET}"
    )
    for line in str(message).split("\n"):
        if line:
            safe_print(f"{PURPLE}│{RESET} {color}{line}{RESET}")
    safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
    safe_print("")


def error_box(message: str) -> None:
    draw_box("Error", message, RED)


def info_box(message: str) -> None:
    draw_box("Info", message, CYAN)


def warn_box(message: str) -> None:
    draw_box("Warning", message, YELLOW)


def progress_bar(done: int, total: int, width: int = 30) -> str:
    width = max(0, min(200, width))
    if total <= 0:
        return f"{PURPLE}[{' ' * width}]{RESET}"
    done = max(0, min(done, total))
    pct = safe_div(done, total, 0)
    filled = max(0, min(width, int(width * pct)))
    if pct < 0.33:
        color = RED
    elif pct < 0.66:
        color = ORANGE
    else:
        color = GREEN
    bar = f"{color}{'█' * filled}{PURPLE}{'░' * (width - filled)}{RESET}"
    return f"{PURPLE}[{RESET}{bar}{PURPLE}]{RESET}"


async def safe_sleep(seconds: float) -> None:
    await asyncio.sleep(max(0.0, min(120.0, seconds)))


async def interruptible_sleep(seconds: float) -> bool:
    remaining = max(0.0, min(120.0, seconds))
    while remaining > 0.0:
        if stop_event is not None and stop_event.is_set():
            return False
        chunk = min(SLEEP_SLICE, remaining)
        try:
            await asyncio.sleep(chunk)
        except asyncio.CancelledError:
            raise
        remaining -= chunk
    return True


def jitter(low: float, high: float) -> float:
    try:
        if low > high:
            low, high = high, low
        return random.uniform(low, high)
    except Exception:
        return JITTER_MIN


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return default


def parse_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except Exception:
        return None


def dig(mapping: Any, key: str, default: Any = None) -> Any:
    if not isinstance(mapping, dict):
        return default
    return mapping.get(key, default)


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    if den == 0:
        return default
    return num / den


def prompt(text: str) -> str:
    try:
        return input(text).strip()
    except (EOFError, UnicodeDecodeError):
        return ""


def secret_prompt(text: str) -> str:
    try:
        import pwinput
        return pwinput.pwinput(prompt=text, mask="*").strip()
    except ImportError:
        pass
    except Exception:
        pass
    try:
        return getpass.getpass(prompt=text).strip()
    except Exception:
        return ""


def backoff(attempt: int) -> float:
    exp = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_MAX)
    return exp + jitter(JITTER_MIN, JITTER_MAX)


def is_write_op(method: str) -> bool:
    return method.upper() in ("DELETE", "PUT", "POST", "PATCH")


def require_http2() -> bool:
    if importlib.util.find_spec("h2") is None:
        error_box("Missing dependency: h2\nInstall it with: pip install httpx[http2]")
        return False
    return True


class ServerNuker:
    def __init__(self, token: str, guild_id: str) -> None:
        self.token = token
        self.guild_id = guild_id

        self.headers: Dict[str, str] = {
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

        self.client: Optional[httpx.AsyncClient] = None
        self.queue: Optional[asyncio.Queue[Tuple[str, str, Optional[Dict[str, Any]]]]] = None

        self.cache: Dict[str, Optional[List[Any]]] = {
            "channels": None,
            "roles": None,
        }

        self.user: Optional[Dict[str, Any]] = None
        self.guild: Optional[Dict[str, Any]] = None
        self.perms: int = 0
        self.is_owner: bool = False
        self.is_admin: bool = False

        self.owner_id: Optional[str] = None
        self.top_role_pos: int = -1
        self.my_role_ids: Set[str] = set()
        self.role_pos: Dict[str, int] = {}
        self.role_cache: Dict[str, Dict[str, Any]] = {}
        self.raw_roles: List[Any] = []

    async def __aenter__(self) -> "ServerNuker":
        self.queue = asyncio.Queue()
        limits = httpx.Limits(
            max_connections=MAX_CONNECTIONS,
            max_keepalive_connections=MAX_KEEPALIVE,
            keepalive_expiry=KEEPALIVE_EXPIRY,
        )
        timeout = httpx.Timeout(
            connect=CONNECT_TIMEOUT,
            read=READ_TIMEOUT,
            write=WRITE_TIMEOUT,
            pool=POOL_TIMEOUT,
        )
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=self.headers,
            http2=True,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        try:
            if self.client is not None:
                await self.client.aclose()
        except Exception:
            pass

    async def validate_token(self) -> bool:
        try:
            resp = await self.request("GET", f"{API_BASE}/users/@me")
            if resp is not None and resp.status_code == 200:
                data = parse_json(resp)
                if isinstance(data, dict):
                    self.user = data
                    return True
            return False
        except Exception:
            return False

    async def validate_guild(self) -> bool:
        try:
            resp = await self.request("GET", f"{API_BASE}/guilds/{self.guild_id}")
            if resp is not None and resp.status_code == 200:
                data = parse_json(resp)
                if isinstance(data, dict):
                    self.guild = data
                    return True
            return False
        except Exception:
            return False

    async def load_context(self) -> bool:
        try:
            if not self.guild or not self.user:
                return False
            my_id = dig(self.user, "id")
            if not my_id:
                return False

            self.owner_id = dig(self.guild, "owner_id")

            roles_resp = await self.request("GET", f"{API_BASE}/guilds/{self.guild_id}/roles")
            if roles_resp is None or roles_resp.status_code != 200:
                return False
            all_roles = parse_json(roles_resp)
            if not isinstance(all_roles, list):
                return False

            self.raw_roles = all_roles
            self.role_pos.clear()
            self.role_cache.clear()
            for role in all_roles:
                if not isinstance(role, dict):
                    continue
                rid = dig(role, "id")
                if not rid:
                    continue
                self.role_pos[rid] = to_int(dig(role, "position", 0), 0)
                self.role_cache[rid] = role

            if self.owner_id == my_id:
                self.is_owner = True
                self.is_admin = True
                self.perms = 0xFFFFFFFFFFFFFFFF
                self.top_role_pos = 1 << 30
                self.my_role_ids = set()
                self.cache["roles"] = all_roles
                return True

            member_resp = await self.request(
                "GET", f"{API_BASE}/guilds/{self.guild_id}/members/{my_id}"
            )
            if member_resp is None:
                return False

            if member_resp.status_code != 200:
                self.perms = 0
                self.is_admin = False
                self.my_role_ids = set()
                self.top_role_pos = 0
                self.cache["roles"] = all_roles
                return True

            member = parse_json(member_resp)
            if not isinstance(member, dict):
                self.perms = 0
                self.is_admin = False
                self.my_role_ids = set()
                self.top_role_pos = 0
                self.cache["roles"] = all_roles
                return True

            my_roles = set(dig(member, "roles", []) or [])
            self.my_role_ids = {str(r) for r in my_roles}

            accumulated = 0
            for role in all_roles:
                if not isinstance(role, dict):
                    continue
                rid = dig(role, "id")
                if not rid:
                    continue
                if rid == self.guild_id or rid in my_roles:
                    accumulated |= to_int(dig(role, "permissions", 0), 0)

            self.perms = accumulated
            self.is_admin = bool(accumulated & PERM_ADMIN)

            highest = max(
                (self.role_pos.get(rid, 0) for rid in self.my_role_ids),
                default=0,
            )
            self.top_role_pos = highest
            self.cache["roles"] = all_roles
            return True
        except Exception:
            return False

    async def setup(self) -> bool:
        safe_print("")
        safe_print(
            f"{PURPLE}┌─ {MAGENTA}{BOLD}Validation{RESET} "
            f"{PURPLE}───────────────────────────────────────────────┐{RESET}"
        )

        if not self.token:
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Token: required{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not await self.validate_token():
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Token: invalid{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        username = dig(self.user, "username", "Unknown")
        disc = dig(self.user, "discriminator", "0")
        display = f"{username}#{disc}" if disc != "0" else username
        safe_print(f"{PURPLE}│{RESET} {GREEN}✔ Token: {TEAL}{BOLD}{display}{RESET}")

        if not self.guild_id:
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Guild: required{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not self.guild_id.isdigit():
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Guild: invalid ID format{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if not await self.validate_guild():
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Guild: not found{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        safe_print(
            f"{PURPLE}│{RESET} {GREEN}✔ Guild: {TEAL}{BOLD}"
            f"{dig(self.guild, 'name', 'Unknown')}{RESET}"
        )

        if not await self.load_context():
            safe_print(f"{PURPLE}│{RESET} {RED}✘ Permissions / Hierarchy: could not load{RESET}")
            safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
            return False

        if self.is_owner:
            safe_print(f"{PURPLE}│{RESET} {GREEN}✔ Permissions: {TEAL}{BOLD}Owner{RESET}")
        elif self.is_admin:
            safe_print(f"{PURPLE}│{RESET} {GREEN}✔ Permissions: {TEAL}{BOLD}Administrator{RESET}")
        else:
            safe_print(f"{PURPLE}│{RESET} {YELLOW}⚠ Permissions: {TEAL}{BOLD}Limited{RESET}")

        perm_checks = [
            ("Manage Channels", PERM_CHANNELS),
            ("Manage Roles", PERM_ROLES),
            ("Manage Guild", PERM_GUILD),
            ("Manage Webhooks", PERM_WEBHOOKS),
            ("Manage Expressions", PERM_EXPRESSIONS),
        ]
        granted = [name for name, flag in perm_checks if self.has_perm(flag)]
        missing = [name for name, flag in perm_checks if not self.has_perm(flag)]

        safe_print(
            f"{PURPLE}│{RESET} {GREEN}✔ Granted:{RESET} "
            f"{TEAL}{', '.join(granted) or 'None'}{RESET}"
        )
        if missing:
            safe_print(
                f"{PURPLE}│{RESET} {YELLOW}⚠ Missing:{RESET} "
                f"{GRAY}{', '.join(missing)}{RESET}"
            )

        hierarchy = "Owner" if self.is_owner else f"Position {self.top_role_pos}"
        safe_print(
            f"{PURPLE}│{RESET} {BLUE}⌂ Hierarchy:{RESET} "
            f"{TEAL}{hierarchy}{RESET}"
        )
        safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
        return True

    async def request(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[httpx.Response]:
        try:
            if stop_event is None or stop_event.is_set():
                return None
            if stats_lock is None or self.client is None:
                return None

            mutating = is_write_op(method)

            for attempt in range(MAX_RETRIES):
                if stop_event is None or stop_event.is_set():
                    return None

                try:
                    resp = await self.client.request(method, url, json=payload)
                except asyncio.CancelledError:
                    raise
                except (httpx.RequestError, asyncio.TimeoutError):
                    async with stats_lock:
                        stats["retries"] += 1
                    ok = await interruptible_sleep(backoff(attempt))
                    if not ok:
                        return None
                    continue
                except Exception:
                    return None

                status = resp.status_code

                if status in (200, 201, 204):
                    if mutating:
                        async with stats_lock:
                            stats["done"] += 1
                    return resp

                if status == 404:
                    if mutating:
                        async with stats_lock:
                            stats["already"] += 1
                    return resp

                if status == 429:
                    resp_body = parse_json(resp) or {}
                    retry_after = to_float(resp_body.get("retry_after", 1.0), 1.0)
                    scope = resp.headers.get("X-RateLimit-Scope", "")
                    try:
                        header_retry = resp.headers.get("Retry-After")
                        if header_retry is not None:
                            retry_after = max(retry_after, to_float(header_retry, retry_after))
                    except Exception:
                        pass
                    is_global = bool(resp_body.get("global", False)) or scope == "global"
                    async with stats_lock:
                        stats["rate_limits"] += 1
                        stats["retries"] += 1
                    sleep_for = retry_after + jitter(JITTER_MIN, JITTER_MAX)
                    if is_global:
                        warn_box(f"Global rate limit — sleeping {sleep_for:.2f}s")
                    ok = await interruptible_sleep(min(sleep_for, GLOBAL_RL_CAP))
                    if not ok:
                        return None
                    continue

                if status in (401, 403):
                    if mutating:
                        async with stats_lock:
                            stats["failed"] += 1
                    return resp

                if status in (500, 502, 503, 504):
                    async with stats_lock:
                        stats["retries"] += 1
                    ok = await interruptible_sleep(backoff(attempt))
                    if not ok:
                        return None
                    continue

                if mutating:
                    async with stats_lock:
                        stats["failed"] += 1
                return resp

            if mutating:
                async with stats_lock:
                    stats["failed"] += 1
            return None
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    async def get_channels(self) -> List[Any]:
        if self.cache["channels"] is not None:
            return self.cache["channels"]
        resp = await self.request("GET", f"{API_BASE}/guilds/{self.guild_id}/channels")
        if resp is not None and resp.status_code == 200:
            data = parse_json(resp)
            if isinstance(data, list):
                self.cache["channels"] = data
                return data
        return []

    async def get_roles(self) -> List[Any]:
        if self.cache["roles"] is not None:
            return self.cache["roles"]
        resp = await self.request("GET", f"{API_BASE}/guilds/{self.guild_id}/roles")
        if resp is not None and resp.status_code == 200:
            data = parse_json(resp)
            if isinstance(data, list):
                self.cache["roles"] = data
                return data
        return []

    def has_perm(self, flag: int) -> bool:
        if self.perms & PERM_ADMIN:
            return True
        return bool(self.perms & flag)

    def guild_owner(self) -> bool:
        return bool(
            self.user
            and self.guild
            and dig(self.user, "id") == dig(self.guild, "owner_id")
        )

    def can_manage_role(self, role_id: str) -> Tuple[bool, str]:
        if not self.has_perm(PERM_ROLES):
            return False, "Missing Manage Roles"
        role = self.role_cache.get(role_id)
        if not role:
            return False, "Role not in cache"
        if dig(role, "managed", False):
            return False, "Bot-managed role"
        if role_id == self.guild_id:
            return False, "Cannot modify @everyone"
        if self.guild_owner():
            return True, ""
        target_pos = self.role_pos.get(role_id, 0)
        if target_pos >= self.top_role_pos:
            return False, (
                f"Role position {target_pos} >= user position {self.top_role_pos}"
            )
        return True, ""


class TaskPool:
    def __init__(self, mgr: ServerNuker) -> None:
        self.mgr = mgr
        self.count = INITIAL_WORKERS
        self.tasks: Deque[asyncio.Task[None]] = deque()
        self.lock = asyncio.Lock()
        self.last_log: float = 0.0

    async def work(self, stagger: float = 0.0) -> None:
        try:
            if stagger > 0:
                try:
                    ok = await interruptible_sleep(stagger)
                    if not ok:
                        return
                except asyncio.CancelledError:
                    raise

            while True:
                if stop_event is None or stop_event.is_set():
                    return

                item: Optional[Tuple[str, str, Optional[Dict[str, Any]]]] = None
                try:
                    item = await asyncio.wait_for(
                        self.mgr.queue.get(),
                        timeout=QUEUE_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    raise

                try:
                    await self.mgr.request(item[0], item[1], item[2])
                except asyncio.CancelledError:
                    async with stats_lock:
                        stats["abandoned"] += 1
                    try:
                        self.mgr.queue.task_done()
                    except Exception:
                        pass
                    raise
                except Exception:
                    try:
                        self.mgr.queue.task_done()
                    except Exception:
                        pass
                else:
                    try:
                        self.mgr.queue.task_done()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def start(self) -> None:
        for _ in range(self.count):
            s = jitter(0.0, STAGGER_MAX)
            self.tasks.append(asyncio.create_task(self.work(stagger=s)))

    async def autotune(self) -> None:
        last_done = 0
        last_rl = 0
        while True:
            if stop_event is None or stop_event.is_set():
                return
            try:
                await safe_sleep(AUTOTUNE_INTERVAL)
            except asyncio.CancelledError:
                return

            now = time.time()
            async with stats_lock:
                done_delta = stats["done"] - last_done
                rl_delta = stats["rate_limits"] - last_rl
                last_done = stats["done"]
                last_rl = stats["rate_limits"]

            log_msg: Optional[str] = None

            async with self.lock:
                self.tasks = deque(t for t in self.tasks if not t.done())

                rl_pressure = rl_delta > done_delta * TUNE_RL_THRESHOLD
                if rl_pressure and self.count > MIN_WORKERS:
                    new_count = max(MIN_WORKERS, int(self.count * TUNE_DOWN_FACTOR))
                    if new_count < self.count:
                        excess = len(self.tasks) - new_count
                        for _ in range(max(0, excess)):
                            try:
                                t = self.tasks.pop()
                                t.cancel()
                            except Exception:
                                pass
                        old = self.count
                        self.count = new_count
                        if now - self.last_log >= AUTOTUNE_LOG_COOLDOWN:
                            self.last_log = now
                            log_msg = f"Autotune down — {old} → {new_count} workers"

                elif (
                    rl_delta == 0
                    and done_delta > TUNE_SUCCESS_THRESHOLD
                    and self.count < MAX_WORKERS
                ):
                    new_count = min(MAX_WORKERS, self.count + TUNE_UP_STEP)
                    to_spawn = new_count - len(self.tasks)
                    for _ in range(max(0, to_spawn)):
                        self.tasks.append(
                            asyncio.create_task(
                                self.work(
                                    stagger=jitter(0.0, STAGGER_MAX)
                                )
                            )
                        )
                    old = self.count
                    self.count = new_count
                    if now - self.last_log >= AUTOTUNE_LOG_COOLDOWN:
                        self.last_log = now
                        log_msg = f"Autotune up — {old} → {new_count} workers"

            if log_msg:
                info_box(log_msg)

    async def stop(self) -> None:
        async with self.lock:
            snapshot = list(self.tasks)
            self.tasks.clear()
        for t in snapshot:
            try:
                t.cancel()
            except Exception:
                pass
        if snapshot:
            await asyncio.gather(*snapshot, return_exceptions=True)
        q = self.mgr.queue
        if q is not None:
            while True:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    async with stats_lock:
                        stats["abandoned"] += 1
                    try:
                        q.task_done()
                    except Exception:
                        pass


async def report_progress(total: int) -> None:
    last_settled = 0
    last_time = time.time()
    while True:
        if stop_event is None or stop_event.is_set():
            return
        try:
            await safe_sleep(PROGRESS_INTERVAL)
        except asyncio.CancelledError:
            return

        now = time.time()
        async with stats_lock:
            done = stats["done"]
            already = stats["already"]
            failed = stats["failed"]
            retries = stats["retries"]
            rate_limits = stats["rate_limits"]
            abandoned = stats["abandoned"]

        settled = done + already + failed + abandoned
        delta = max(0, settled - last_settled)
        dt = max(0.0, now - last_time)
        rate = safe_div(delta, dt, 0)
        last_settled = settled
        last_time = now

        bar = progress_bar(settled, total)
        pct = max(0.0, min(100.0, safe_div(settled, total, 0) * 100))
        remaining = max(0, total - settled)
        eta = safe_div(remaining, rate, 0)

        line = (
            f"{bar} {BOLD}{TEAL}{pct:5.1f}%{RESET} "
            f"{PURPLE}│{RESET} {GREEN}OK {done}{RESET} "
            f"{PURPLE}│{RESET} {GRAY}SKIP {already}{RESET} "
            f"{PURPLE}│{RESET} {RED}FAIL {failed}{RESET} "
            f"{PURPLE}│{RESET} {YELLOW}RL {rate_limits}{RESET} "
            f"{PURPLE}│{RESET} {LIME}RETRY {retries}{RESET} "
            f"{PURPLE}│{RESET} {ORANGE}ABANDON {abandoned}{RESET} "
            f"{PURPLE}│{RESET} {PINK}{rate:5.1f}/s{RESET} "
            f"{PURPLE}│{RESET} {BLUE}ETA {eta:5.0f}s{RESET}"
        )
        try:
            print(f"\r{line}", end="", flush=True)
        except UnicodeEncodeError:
            pass


async def channel_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_CHANNELS):
        error_box("Missing Manage Channels permission")
        return []
    channels = await mgr.get_channels()
    if not channels:
        error_box("No channels found")
        return []

    cats: List[Any] = []
    rest: List[Any] = []
    for ch in channels:
        (cats if dig(ch, "type") == 4 else rest).append(ch)

    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    for ch in itertools.chain(rest, cats):
        cid = dig(ch, "id")
        if not cid:
            continue
        ops.append(("DELETE", f"{API_BASE}/channels/{cid}", None))
    return ops


async def role_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_ROLES):
        error_box("Missing Manage Roles permission")
        return []
    all_roles = await mgr.get_roles()
    if not all_roles:
        error_box("No roles found")
        return []

    eligible: List[str] = []
    for role in all_roles:
        rid = dig(role, "id")
        if not rid or rid == mgr.guild_id:
            continue
        if dig(role, "managed", False):
            continue
        eligible.append(rid)

    if not eligible:
        error_box("No deletable roles found")
        return []

    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    skipped = 0
    for rid in eligible:
        ok, _ = mgr.can_manage_role(rid)
        if not ok:
            skipped += 1
            continue
        ops.append(
            ("DELETE", f"{API_BASE}/guilds/{mgr.guild_id}/roles/{rid}", None)
        )

    if skipped:
        warn_box(f"Skipped {skipped} role(s) — hierarchy restriction")
    if not ops:
        error_box("No eligible roles found")
    return ops


async def emoji_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_EXPRESSIONS):
        error_box("Missing Manage Expressions permission")
        return []
    resp = await mgr.request("GET", f"{API_BASE}/guilds/{mgr.guild_id}/emojis")
    if not resp or resp.status_code != 200:
        return []
    emojis = parse_json(resp)
    if not isinstance(emojis, list) or not emojis:
        error_box("No emojis found")
        return []
    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    for emoji in emojis:
        eid = dig(emoji, "id")
        if not eid:
            continue
        ops.append(
            ("DELETE", f"{API_BASE}/guilds/{mgr.guild_id}/emojis/{eid}", None)
        )
    return ops


async def sticker_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_EXPRESSIONS):
        error_box("Missing Manage Expressions permission")
        return []
    resp = await mgr.request("GET", f"{API_BASE}/guilds/{mgr.guild_id}/stickers")
    if not resp or resp.status_code != 200:
        return []
    stickers = parse_json(resp)
    if not isinstance(stickers, list) or not stickers:
        error_box("No stickers found")
        return []
    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    for sticker in stickers:
        sid = dig(sticker, "id")
        if not sid:
            continue
        ops.append(
            ("DELETE", f"{API_BASE}/guilds/{mgr.guild_id}/stickers/{sid}", None)
        )
    return ops


async def invite_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_GUILD):
        error_box("Missing Manage Guild permission")
        return []
    resp = await mgr.request("GET", f"{API_BASE}/guilds/{mgr.guild_id}/invites")
    if not resp or resp.status_code != 200:
        return []
    invites = parse_json(resp)
    if not isinstance(invites, list) or not invites:
        error_box("No invites found")
        return []
    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    for invite in invites:
        code = dig(invite, "code")
        if not code:
            continue
        ops.append(("DELETE", f"{API_BASE}/invites/{code}", None))
    return ops


async def webhook_ops(mgr: ServerNuker) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if not mgr.has_perm(PERM_WEBHOOKS):
        error_box("Missing Manage Webhooks permission")
        return []
    resp = await mgr.request("GET", f"{API_BASE}/guilds/{mgr.guild_id}/webhooks")
    if not resp or resp.status_code != 200:
        return []
    webhooks = parse_json(resp)
    if not isinstance(webhooks, list) or not webhooks:
        error_box("No webhooks found")
        return []
    ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
    for wh in webhooks:
        wid = dig(wh, "id")
        if not wid:
            continue
        ops.append(("DELETE", f"{API_BASE}/webhooks/{wid}", None))
    return ops


ACTIONS: Dict[str, Tuple[str, Optional[Callable]]] = {
    "1": ("Delete Channels & Categories", channel_ops),
    "2": ("Delete Roles", role_ops),
    "3": ("Delete Emojis", emoji_ops),
    "4": ("Delete Stickers", sticker_ops),
    "5": ("Delete Invites", invite_ops),
    "6": ("Delete Webhooks", webhook_ops),
    "7": ("Run Everything", None),
}


async def run_action(mgr: ServerNuker, choice: str) -> List[Tuple[str, str, Optional[Dict[str, Any]]]]:
    if choice == "7":
        ops: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
        for key in ("1", "2", "3", "4", "5", "6"):
            _, fn = ACTIONS[key]
            ops.extend(await fn(mgr))
        return ops
    _, fn = ACTIONS[choice]
    return await fn(mgr)


def print_menu() -> None:
    safe_print("")
    safe_print(
        f"{PURPLE}┌─ {MAGENTA}{BOLD}Guild Cleaner{RESET} "
        f"{PURPLE}──────────────────────────────────────────────┐{RESET}"
    )
    safe_print(f"{PURPLE}│{RESET} {TEAL}Available actions{RESET}")
    safe_print(f"{PURPLE}├──────────────────────────────────────────────────────────┤{RESET}")
    for key in sorted(ACTIONS.keys(), key=int):
        name, _ = ACTIONS[key]
        if key == "7":
            safe_print(f"{PURPLE}│{RESET} {RED}{BOLD}[{key}] {name}{RESET}")
        else:
            safe_print(f"{PURPLE}│{RESET} {LIME}[{key}]{RESET} {WHITE}{name}{RESET}")
    safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
    safe_print("")


def confirm(choice: str, total: int) -> bool:
    if total <= 0:
        return False
    if choice == "7":
        msg = (
            f"Confirm Run Everything ({total} operations)?\n"
            f"Type y to proceed, N to cancel."
        )
        draw_box("Confirm", msg, RED)
    else:
        msg = (
            f"Confirm {total} operations?\n"
            f"Type y to proceed, N to cancel."
        )
        draw_box("Confirm", msg, YELLOW)
    answer = prompt(
        f"{PURPLE}│{RESET} {TEAL}Confirm{RESET}  {PURPLE}➜{RESET} "
    ).lower()
    return answer in ("y", "yes")


async def main() -> None:
    global stats_lock, stop_event

    if not require_http2():
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
            "abandoned": 0,
            "start_time": 0.0,
        }
    )

    clear_screen()
    safe_print("")
    safe_print(
        f"{PURPLE}┌─ {MAGENTA}{BOLD}Configuration{RESET} "
        f"{PURPLE}────────────────────────────────────────────┐{RESET}"
    )
    token = secret_prompt(f"{PURPLE}│{RESET} {TEAL}Token{RESET}  {PURPLE}➜{RESET} ")
    guild_id = prompt(f"{PURPLE}│{RESET} {TEAL}Guild{RESET}  {PURPLE}➜{RESET} ")
    safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")

    async with ServerNuker(token, guild_id) as mgr:
        if not await mgr.setup():
            return

        print_menu()
        safe_print(
            f"{PURPLE}┌─ {MAGENTA}{BOLD}Select{RESET} "
            f"{PURPLE}──────────────────────────────────────────────────┐{RESET}"
        )
        choice = prompt(f"{PURPLE}│{RESET} {TEAL}Option{RESET}  {PURPLE}➜{RESET} ")
        safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")
        if choice not in ACTIONS:
            error_box("Invalid option")
            return

        stats["start_time"] = time.time()
        ops = await run_action(mgr, choice)
        total = len(ops)
        if total == 0:
            return

        if not confirm(choice, total):
            warn_box("Operation cancelled")
            return

        for method, url, body in ops:
            mgr.queue.put_nowait((method, url, body))

        safe_print("")
        safe_print(
            f"{PURPLE}┌─ {MAGENTA}{BOLD}Queue{RESET} "
            f"{PURPLE}─────────────────────────────────────────────────┐{RESET}"
        )
        safe_print(f"{PURPLE}│{RESET} Total operations queued: {LIME}{BOLD}{total}{RESET}")
        safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")

        pool = TaskPool(mgr)
        await pool.start()
        reporter = asyncio.create_task(report_progress(total))
        tuner = asyncio.create_task(pool.autotune())

        try:
            await mgr.queue.join()
        finally:
            stop_event.set()
            tuner.cancel()
            reporter.cancel()
            await pool.stop()
            await asyncio.gather(tuner, reporter, return_exceptions=True)
            safe_print("")

        safe_print("")
        elapsed = time.time() - stats["start_time"]
        done = stats["done"]
        already = stats["already"]
        failed = stats["failed"]
        retries = stats["retries"]
        rate_limits = stats["rate_limits"]
        abandoned = stats["abandoned"]

        safe_print(
            f"{PURPLE}┌─ {MAGENTA}{BOLD}Final Results{RESET} "
            f"{PURPLE}────────────────────────────────────────┐{RESET}"
        )
        safe_print(f"{PURPLE}│{RESET} {GREEN}✔  Success   {RESET}: {BOLD}{done}/{total}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {GRAY}↷  Skipped   {RESET}: {BOLD}{already}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {RED}✘  Failed    {RESET}: {BOLD}{failed}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {ORANGE}⊘  Abandoned {RESET}: {BOLD}{abandoned}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {YELLOW}↻  Retries   {RESET}: {BOLD}{retries}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {LIME}⏱  Rate Lmt  {RESET}: {BOLD}{rate_limits}{RESET}")
        safe_print(f"{PURPLE}│{RESET} {LIME}⏲  Time      {RESET}: {BOLD}{elapsed:.2f}s{RESET}")
        safe_print(
            f"{PURPLE}│{RESET} {PINK}⚡  Avg Rate  {RESET}: "
            f"{BOLD}{safe_div(done, max(elapsed, 0.01), 0):.1f}/s{RESET}"
        )
        safe_print(f"{PURPLE}└──────────────────────────────────────────────────────────┘{RESET}")


def on_signal(sig: int, frame: Any) -> None:
    warn_box("Interrupted — shutting down...")
    if stop_event is not None:
        stop_event.set()
    os._exit(1)


def run() -> int:
    try:
        asyncio.get_running_loop()
        error_box("Cannot run inside an existing event loop")
        return 1
    except RuntimeError:
        pass

    try:
        asyncio.run(main())
        return 0
    except KeyboardInterrupt:
        return 130
    except (RuntimeError, Exception):
        return 1


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, on_signal)
    except Exception:
        pass
    try:
        sys.exit(run())
    except SystemExit:
        raise
    except Exception:
        pass
