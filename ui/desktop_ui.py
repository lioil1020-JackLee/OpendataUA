import json
import os
import queue
from copy import deepcopy
import signal
import socket
import subprocess
import sys
import threading
import time
import tkinter as tk
import urllib.request as request
from tkinter import font as tkfont
from tkinter import ttk
from urllib.parse import urlparse

APP_TITLE = "OpenData Weather OPC UA"
WINDOW_ICON_PATH: str | None = None

VALUE_TAGS = [
    "24R",
    "D_TN",
    "D_TS",
    "D_TX",
    "ELEV",
    "H_10D",
    "H_F10",
    "H_FX",
    "H_UVI",
    "H_XD",
    "HUMD",
    "PRES",
    "TEMP",
    "WDIR",
    "WDSD",
    "CITY",
    "D_TNT",
    "D_TXT",
    "H_F10T",
    "H_FXT",
    "VIS",
    "Weather",
]

DISPLAY_FIELDS: list[tuple[str, str, str]] = [
    ("ELEV", "高度", "m"),
    ("H_FX", "小時最大陣風風速", "m/s"),
    ("D_TX", "本日最高溫", "°C"),
    ("WDIR", "風向", "deg"),
    ("H_XD", "小時最大陣風風向", "deg"),
    ("D_TXT", "本日最高溫時間", ""),
    ("WDSD", "風速", "m/s"),
    ("H_FXT", "小時最大陣風時間", ""),
    ("D_TN", "本日最低溫", "°C"),
    ("TEMP", "溫度", "°C"),
    ("H_F10", "10分鐘平均風速", "m/s"),
    ("D_TNT", "本日最低溫時間", ""),
    ("HUMD", "相對濕度", "%RH"),
    ("H_10D", "10分鐘平均風向", "deg"),
    ("D_TS", "本日總日照時數", "hr"),
    ("PRES", "測站氣壓", "hpa"),
    ("H_F10T", "10分鐘平均風速時間", ""),
    ("VIS", "10分鐘盛行能見度", ""),
    ("24R", "日累積雨量", "mm"),
    ("H_UVI", "小時紫外線指數", ""),
    ("Weather", "10分鐘天氣現象描述", ""),
]

DEFAULT_CONFIG: dict = {
    "openData": {
        "address": "https://opendata.cwa.gov.tw/api/v1/rest/datastore/",
        "api": "O-A0003-001",
        "auth_key": "CWB-448F9C5A-3C92-44BF-8FD0-D57CE12F7FA5",
        "target": ["466900", "466920", "467050", "467571", "467441"],
        "stations": [
            {"id": "466900", "name": ""},
            {"id": "466920", "name": ""},
            {"id": "467050", "name": ""},
            {"id": "467571", "name": ""},
            {"id": "467441", "name": ""},
        ],
    },
    "opcUA": {"url": "opc.tcp://0.0.0.0:48480"},
}


def _default_config() -> dict:
    return deepcopy(DEFAULT_CONFIG)


def _config_path(repo_root: str) -> str:
    return os.path.join(repo_root, "config.json")


def _main_path(repo_root: str) -> str:
    return os.path.join(repo_root, "main.py")


def _load_config(repo_root: str) -> dict:
    p = _config_path(repo_root)
    if not os.path.exists(p):
        cfg = _default_config()
        _save_config(repo_root, cfg)
        return cfg
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_config(repo_root: str, cfg: dict) -> None:
    with open(_config_path(repo_root), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=4)


def _pick_python(repo_root: str) -> str:
    if getattr(sys, "frozen", False):
        return sys.executable
    if os.name == "nt":
        py = os.path.join(repo_root, ".venv", "Scripts", "python.exe")
    else:
        py = os.path.join(repo_root, ".venv", "bin", "python")
    if os.path.exists(py):
        return py
    return sys.executable or "python"


def _apply_window_icon(win: tk.Misc) -> None:
    if not WINDOW_ICON_PATH or not os.path.exists(WINDOW_ICON_PATH):
        return
    try:
        win.iconbitmap(WINDOW_ICON_PATH)
    except Exception:
        pass


def _center_over_master(win: tk.Misc, master: tk.Misc) -> None:
    master.winfo_toplevel().update_idletasks()
    win.update_idletasks()
    m = master.winfo_toplevel()
    width = win.winfo_reqwidth()
    height = win.winfo_reqheight()
    x = m.winfo_rootx() + (m.winfo_width() - width) // 2
    y = m.winfo_rooty() + (m.winfo_height() - height) // 2
    win.geometry(f"{width}x{height}+{x}+{y}")


def _parse_opcua_url(url: str) -> tuple[str, int] | None:
    try:
        ep = urlparse(str(url).strip())
        if not ep.hostname or not ep.port:
            return None
        return ep.hostname, int(ep.port)
    except Exception:
        return None


def _is_port_open(host: str, port: int, timeout_sec: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_sec):
            return True
    except Exception:
        return False


def _find_all_project_service_pids(repo_root: str) -> list[int]:
    try:
        ps = (
            "$procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine }; "
            "$procs | ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }"
        )
        cp = subprocess.run(["powershell", "-NoProfile", "-Command", ps], check=False, capture_output=True, text=True)
        repo = os.path.normcase(os.path.abspath(repo_root)).replace("/", "\\")
        me = os.getpid()
        pids: list[int] = []
        for line in (cp.stdout or "").splitlines():
            line = line.strip()
            if not line or "\t" not in line:
                continue
            pid_s, cmd = line.split("\t", 1)
            cmd_norm = os.path.normcase(cmd).replace("/", "\\")
            if (
                (repo in cmd_norm)
                and (" server" in cmd_norm)
                and (("main.py" in cmd_norm) or ("opendataua" in cmd_norm))
            ):
                try:
                    pid = int(pid_s.strip())
                    if pid != me:
                        pids.append(pid)
                except Exception:
                    pass
        return sorted(set([p for p in pids if p > 0]))
    except Exception:
        return []


def _kill_pid(pid: int) -> None:
    if pid <= 0:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True)
    else:
        os.kill(pid, signal.SIGTERM)


def _cleanup_stale_services(repo_root: str) -> None:
    for pid in _find_all_project_service_pids(repo_root):
        _kill_pid(pid)


def _start_server_process(repo_root: str, wait_sec: float = 5.0) -> tuple[bool, str, subprocess.Popen | None]:
    py = _pick_python(repo_root)
    if getattr(sys, "frozen", False):
        args = [py, "server"]
    else:
        args = [py, "-u", _main_path(repo_root), "server"]

    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)

    try:
        proc = subprocess.Popen(
            args,
            cwd=repo_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    except Exception as e:
        return False, f"Failed to start process: {e}", None

    deadline = time.time() + wait_sec
    while time.time() < deadline:
        rc = proc.poll()
        if rc is not None:
            return False, f"Process exited immediately (code={rc})", None
        time.sleep(0.15)

    return True, f"Started (pid={proc.pid})", proc


def _fetch_values(cfg: dict, station_ids: list[str]) -> dict[str, dict[str, str]]:
    od = cfg.get("openData") or {}
    addr = str(od.get("address") or "").strip()
    api = str(od.get("api") or "").strip()
    auth = str(od.get("auth_key") or "").strip()
    ids = [x.strip() for x in station_ids if x.strip()]
    if not (addr and api and auth and ids):
        return {}

    qid = ",".join(ids)
    url = f"{addr}{api}?Authorization={auth}&format=JSON&StationId={qid}&WeatherElement=&GeoInfo=StationAltitude,CountyName"
    try:
        with request.urlopen(url, timeout=6) as resp:
            payload = json.load(resp)
        rows = payload.get("records", {}).get("Station", [])
    except Exception:
        return {}

    out: dict[str, dict[str, str]] = {}
    for s in rows:
        sid = str(s.get("StationId") or "").strip()
        if not sid:
            continue
        w = s.get("WeatherElement") or {}
        g = s.get("GeoInfo") or {}
        parts = [
            str(w.get("Now", {}).get("Precipitation", "")),
            str(w.get("DailyExtreme", {}).get("DailyLow", {}).get("TemperatureInfo", {}).get("AirTemperature", "")),
            str(w.get("SunshineDuration", "")),
            str(w.get("DailyExtreme", {}).get("DailyHigh", {}).get("TemperatureInfo", {}).get("AirTemperature", "")),
            str(g.get("StationAltitude", "")),
            str(w.get("Max10MinAverage", {}).get("Occurred_at", {}).get("WindDirection", "")),
            str(w.get("Max10MinAverage", {}).get("WindSpeed", "")),
            str(w.get("GustInfo", {}).get("PeakGustSpeed", "")),
            str(w.get("UVIndex", "")),
            str(w.get("GustInfo", {}).get("Occurred_at", {}).get("WindDirection", "")),
            str(w.get("RelativeHumidity", "")),
            str(w.get("AirPressure", "")),
            str(w.get("AirTemperature", "")),
            str(w.get("WindDirection", "")),
            str(w.get("WindSpeed", "")),
            str(g.get("CountyName", "")),
            str(w.get("DailyExtreme", {}).get("DailyLow", {}).get("TemperatureInfo", {}).get("Occurred_at", {}).get("DateTime", "")),
            str(w.get("DailyExtreme", {}).get("DailyHigh", {}).get("TemperatureInfo", {}).get("Occurred_at", {}).get("DateTime", "")),
            str(w.get("Max10MinAverage", {}).get("Occurred_at", {}).get("DateTime", "")),
            str(w.get("GustInfo", {}).get("Occurred_at", {}).get("DateTime", "")),
            str(w.get("VisibilityDescription", "")),
            str(w.get("Weather", "")),
        ]
        out[sid] = {k: v for k, v in zip(VALUE_TAGS, parts, strict=False)}
    return out


class _StationDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, title: str, station_id: str = "", station_name: str = "") -> None:
        super().__init__(master)
        self.withdraw()
        _apply_window_icon(self)
        self.title(title)
        self.resizable(False, False)
        self.result: dict[str, str] | None = None

        self.columnconfigure(1, weight=1)
        pad = {"padx": 10, "pady": 8}

        ttk.Label(self, text="Station ID:").grid(row=0, column=0, sticky="w", **pad)
        self.id_var = tk.StringVar(value=station_id)
        self.id_ent = ttk.Entry(self, textvariable=self.id_var, width=26)
        self.id_ent.grid(row=0, column=1, sticky="ew", **pad)

        ttk.Label(self, text="Station Name (optional):").grid(row=1, column=0, sticky="w", **pad)
        self.name_var = tk.StringVar(value=station_name)
        ttk.Entry(self, textvariable=self.name_var, width=40).grid(row=1, column=1, sticky="ew", **pad)

        btns = ttk.Frame(self)
        btns.grid(row=2, column=0, columnspan=2, sticky="e", padx=10, pady=10)
        ttk.Button(btns, text="Cancel", command=self._cancel, style="NoFocus.TButton", takefocus=False).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok, style="NoFocus.TButton", takefocus=False).pack(side="right")

        self.bind("<Escape>", lambda _e: self._cancel())
        self.bind("<Return>", lambda _e: self._ok())
        self.grab_set()
        _center_over_master(self, master)
        self.deiconify()
        self.id_ent.focus_set()

    def _ok(self) -> None:
        sid = self.id_var.get().strip()
        if not sid:
            _SimpleMessageDialog(self, "Validation", "Station ID is required.").show()
            return
        self.result = {"id": sid, "name": self.name_var.get().strip()}
        self.destroy()

    def _cancel(self) -> None:
        self.result = None
        self.destroy()


class _CenteredConfirmDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, title: str, message: str, ok_text: str = "確定", cancel_text: str = "取消") -> None:
        super().__init__(master)
        self.withdraw()
        _apply_window_icon(self)
        self.title(title)
        self.resizable(False, False)
        self.transient(master)
        self.result = False

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)
        ttk.Label(frm, text=message, anchor="w", justify="left").pack(fill="x")

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=(12, 0))
        ttk.Button(btns, text=cancel_text, command=self._cancel, style="NoFocus.TButton", takefocus=False).pack(side="right", padx=(8, 0))
        ttk.Button(btns, text=ok_text, command=self._ok, style="NoFocus.TButton", takefocus=False).pack(side="right")

        self.bind("<Escape>", lambda _e: self._cancel())
        self.bind("<Return>", lambda _e: self._ok())
        self.grab_set()
        _center_over_master(self, master)
        self.deiconify()

    def _ok(self) -> None:
        self.result = True
        self.destroy()

    def _cancel(self) -> None:
        self.result = False
        self.destroy()


class _SimpleMessageDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, title: str, message: str) -> None:
        super().__init__(master)
        self.withdraw()
        _apply_window_icon(self)
        self.title(title)
        self.resizable(False, False)
        self.transient(master)

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)
        ttk.Label(frm, text=message, anchor="w", justify="left").pack(fill="x")
        ttk.Button(frm, text="OK", command=self.destroy, style="NoFocus.TButton", takefocus=False).pack(anchor="e", pady=(12, 0))

        self.bind("<Escape>", lambda _e: self.destroy())
        self.bind("<Return>", lambda _e: self.destroy())
        self.grab_set()
        _center_over_master(self, master)
        self.deiconify()

    def show(self) -> None:
        self.wait_window(self)


class DesktopApp:
    def __init__(self, repo_root: str) -> None:
        global WINDOW_ICON_PATH
        self.repo_root = repo_root
        self.root = tk.Tk()
        self.root.withdraw()
        self.root.title(APP_TITLE)
        self.root.minsize(900, 560)
        self._is_closing = False
        self._close_authorized = False
        self._handling_unmap = False

        self._server_proc: subprocess.Popen | None = None
        self._tray_icon = None
        self._tray_thread: threading.Thread | None = None
        self._in_tray = False

        icon_path = os.path.join(repo_root, "lioil.ico")
        WINDOW_ICON_PATH = icon_path if os.path.exists(icon_path) else None
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except Exception:
                pass

        self.cfg = _load_config(repo_root)
        endpoint = str(((self.cfg.get("opcUA") or {}).get("url")) or "opc.tcp://127.0.0.1:48480")
        parsed = _parse_opcua_url(endpoint) or ("127.0.0.1", 48480)

        self.auth_var = tk.StringVar(value=str(((self.cfg.get("openData") or {}).get("auth_key")) or ""))
        self.endpoint_host_var = tk.StringVar(value=parsed[0])
        self.endpoint_port_var = tk.StringVar(value=str(parsed[1]))
        self.bind_ip_var = tk.StringVar(value=str(((self.cfg.get("opcUA") or {}).get("bind_ip")) or ""))
        self.server_status_var = tk.StringVar(value="Server: checking...")

        self._value_map: dict[str, dict[str, str]] = {}
        self._stations: list[dict[str, str]] = []
        self._startup_queue: queue.Queue[tuple[bool, str, subprocess.Popen | None]] = queue.Queue()
        self._startup_thread: threading.Thread | None = None

        self._style = ttk.Style(self.root)
        self._apply_theme()
        self._remove_button_focus_outline()
        self._build_ui()
        self._load_station_cards()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close_request)
        self.root.bind("<Unmap>", self._on_root_unmap)
        self.root.after(200, self._start_services_async)
        self.root.after(100, self._poll_startup)
        self.root.after(1500, self._refresh_server_status)

        self.root.deiconify()
        self.root.lift()

    def _apply_theme(self) -> None:
        names = set(self._style.theme_names())
        preferred = ["vista", "xpnative", "winnative", "default", "clam"] if os.name == "nt" else ["clam", "default"]
        for name in preferred:
            if name in names:
                try:
                    self._style.theme_use(name)
                    break
                except Exception:
                    pass

    def _remove_button_focus_outline(self) -> None:
        def _strip_focus(nodes: list) -> list:
            cleaned = []
            for node in nodes:
                if not isinstance(node, tuple) or len(node) < 2:
                    continue
                element, opts = node
                if "focus" in str(element).lower():
                    continue
                new_opts = dict(opts)
                children = opts.get("children")
                if isinstance(children, list):
                    new_opts["children"] = _strip_focus(children)
                cleaned.append((element, new_opts))
            return cleaned

        try:
            layout = self._style.layout("TButton")
            self._style.layout("NoFocus.TButton", _strip_focus(layout))
        except Exception:
            self._style.configure("NoFocus.TButton")

    def _btn(self, parent: tk.Misc, text: str, command) -> ttk.Button:
        return ttk.Button(parent, text=text, command=command, style="NoFocus.TButton", takefocus=False)

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        top = ttk.Frame(outer)
        top.pack(fill="x")
        ttk.Label(top, text=APP_TITLE, font=("Segoe UI", 14, "bold")).pack(side="left")
        self._btn(top, "Config", self._open_config_popup).pack(side="right")
        ttk.Label(top, textvariable=self.server_status_var, font=("Segoe UI", 10, "bold")).pack(side="right", padx=(0, 12))

        frm = ttk.LabelFrame(outer, text="Stations", padding=10)
        frm.pack(fill="both", expand=True, pady=(10, 0))
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(1, weight=1)

        btns = ttk.Frame(frm)
        btns.grid(row=0, column=0, sticky="e", pady=(0, 8))
        self._btn(btns, "Add", self._on_add_station).pack(side="right", padx=6)
        self._btn(btns, "Edit", self._on_edit_station).pack(side="right", padx=6)
        self._btn(btns, "Remove", self._on_remove_station).pack(side="right")

        self.station_nb = ttk.Notebook(frm)
        self.station_nb.grid(row=1, column=0, sticky="nsew")

    def _stations_from_widgets(self) -> list[dict[str, str]]:
        return [{"id": str(s.get("id") or "").strip(), "name": str(s.get("name") or "").strip()} for s in self._stations if str(s.get("id") or "").strip()]

    def _current_station_index(self) -> int | None:
        if not self._stations:
            return None
        try:
            idx = int(self.station_nb.index("current"))
        except Exception:
            return None
        return idx if 0 <= idx < len(self._stations) else None

    def _render_station_tabs(self, stations: list[dict]) -> None:
        current_sid = ""
        cur = self._current_station_index()
        if cur is not None and cur < len(self._stations):
            current_sid = str(self._stations[cur].get("id") or "").strip()

        self._stations = [
            {"id": str((s or {}).get("id") or "").strip(), "name": str((s or {}).get("name") or "").strip()}
            for s in stations
            if isinstance(s, dict) and str((s or {}).get("id") or "").strip()
        ]

        for tab_id in self.station_nb.tabs():
            self.station_nb.forget(tab_id)

        if not self._stations:
            empty = ttk.Frame(self.station_nb, padding=16)
            ttk.Label(empty, text="No stations configured.", font=("Segoe UI", 10)).pack(anchor="w")
            self.station_nb.add(empty, text="No Station")
            return

        active_tab = 0
        for i, s in enumerate(self._stations):
            sid = s["id"]
            if sid == current_sid:
                active_tab = i

            vals = self._value_map.get(sid, {})
            city = str(vals.get("CITY", "")).strip()
            tab = ttk.Frame(self.station_nb, padding=8)
            for c in range(3):
                tab.columnconfigure(c, weight=1)

            for n, (tag, title, unit) in enumerate(DISPLAY_FIELDS):
                r = n // 3
                c = n % 3
                card = ttk.Frame(tab, padding=(8, 6))
                card.grid(row=r, column=c, sticky="nsew", padx=6, pady=5)

                text = str(vals.get(tag, "")).strip()
                if text and unit:
                    text = f"{text} {unit}"

                ttk.Label(card, text=title, anchor="center", justify="center", font=("Segoe UI", 10, "bold")).pack(fill="x")
                tk.Label(card, text=text, relief="solid", bd=1, padx=8, pady=6, font=("Segoe UI", 10)).pack(fill="x", pady=(4, 0))

            tab_text = sid if not city else f"{sid}\n{city}"
            self.station_nb.add(tab, text=tab_text)

        try:
            self.station_nb.select(active_tab)
        except Exception:
            pass

    def _load_station_cards(self) -> None:
        od = self.cfg.get("openData") or {}
        stations = od.get("stations") or []
        if not (isinstance(stations, list) and stations):
            stations = [{"id": str(x), "name": ""} for x in (od.get("target") or [])]

        ids = [str((s or {}).get("id") or "").strip() for s in stations if isinstance(s, dict)]
        self._value_map = _fetch_values(self.cfg, ids)
        self._render_station_tabs(stations)

    def _write_cfg_from_widgets(self) -> None:
        self.cfg.setdefault("openData", {})
        self.cfg.setdefault("opcUA", {})
        self.cfg["openData"]["auth_key"] = self.auth_var.get().strip()
        stations = self._stations_from_widgets()
        self.cfg["openData"]["stations"] = stations
        self.cfg["openData"]["target"] = [s["id"] for s in stations]

        host = self.endpoint_host_var.get().strip()
        port_text = self.endpoint_port_var.get().strip()
        if not host:
            raise ValueError("Endpoint host is required.")
        port = int(port_text)
        if port <= 0 or port > 65535:
            raise ValueError("Endpoint port must be in range 1-65535.")
        self.cfg["opcUA"]["url"] = f"opc.tcp://{host}:{port}"

        bind_ip = self.bind_ip_var.get().strip()
        if bind_ip:
            self.cfg["opcUA"]["bind_ip"] = bind_ip
        else:
            self.cfg["opcUA"].pop("bind_ip", None)

    def _open_config_popup(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.withdraw()
        _apply_window_icon(dlg)
        dlg.title("Config")
        dlg.resizable(False, False)
        dlg.transient(self.root)
        dlg.grab_set()

        frm = ttk.Frame(dlg, padding=12)
        frm.pack(fill="both", expand=True)
        frm.columnconfigure(1, weight=1)

        ttk.Label(frm, text="Authorization Key").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.auth_var).grid(row=0, column=1, columnspan=3, sticky="ew", padx=(10, 0))

        ttk.Label(frm, text="Endpoint URL").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ep = ttk.Frame(frm)
        ep.grid(row=1, column=1, columnspan=3, sticky="ew", padx=(10, 0), pady=(10, 0))
        ttk.Label(ep, text="opc.tcp://").pack(side="left")
        ttk.Entry(ep, textvariable=self.endpoint_host_var, width=26).pack(side="left", padx=(0, 4))
        ttk.Label(ep, text=":").pack(side="left")
        ttk.Entry(ep, textvariable=self.endpoint_port_var, width=7).pack(side="left", padx=(4, 0))

        ttk.Label(frm, text="Bind IP (optional)").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.bind_ip_var).grid(row=2, column=1, columnspan=3, sticky="ew", padx=(10, 0), pady=(10, 0))

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=4, sticky="e", pady=(14, 0))

        def _save_close() -> None:
            try:
                self._write_cfg_from_widgets()
                _save_config(self.repo_root, self.cfg)
                self._load_station_cards()
            except Exception as e:
                _SimpleMessageDialog(dlg, "Save", str(e)).show()
                return
            dlg.destroy()

        self._btn(btns, "Save", _save_close).pack(side="right")
        self._btn(btns, "Cancel", dlg.destroy).pack(side="right", padx=8)

        fnt = tkfont.nametofont("TkDefaultFont")
        auth_width_px = max(420, fnt.measure(self.auth_var.get().strip() or "Authorization Key") + 240)
        self.root.update_idletasks()
        dlg.update_idletasks()
        width = max(auth_width_px, dlg.winfo_reqwidth())
        height = dlg.winfo_reqheight()
        x = self.root.winfo_rootx() + (self.root.winfo_width() - width) // 2
        y = self.root.winfo_rooty() + (self.root.winfo_height() - height) // 2
        dlg.geometry(f"{width}x{height}+{x}+{y}")
        dlg.deiconify()
        dlg.focus_force()

    def _on_add_station(self) -> None:
        dlg = _StationDialog(self.root, "Add Station")
        self.root.wait_window(dlg)
        if not dlg.result:
            return
        sid = dlg.result["id"]
        for s in self._stations:
            if str(s.get("id") or "").strip() == sid:
                _SimpleMessageDialog(self.root, "Stations", f"Station ID already exists: {sid}").show()
                return
        self._stations.append({"id": sid, "name": dlg.result.get("name", "")})
        self._render_station_tabs(self._stations)

    def _on_edit_station(self) -> None:
        idx = self._current_station_index()
        if idx is None:
            _SimpleMessageDialog(self.root, "Stations", "Select one station tab to edit.").show()
            return
        sid = str(self._stations[idx].get("id") or "")
        name = str(self._stations[idx].get("name") or "")
        dlg = _StationDialog(self.root, "Edit Station", sid, name)
        self.root.wait_window(dlg)
        if not dlg.result:
            return

        new_sid = dlg.result["id"]
        if new_sid != sid.strip():
            for i, s in enumerate(self._stations):
                if i != idx and str(s.get("id") or "").strip() == new_sid:
                    _SimpleMessageDialog(self.root, "Stations", f"Station ID already exists: {new_sid}").show()
                    return

        self._stations[idx] = {"id": new_sid, "name": dlg.result.get("name", "")}
        self._render_station_tabs(self._stations)

    def _on_remove_station(self) -> None:
        idx = self._current_station_index()
        if idx is None:
            return
        sid = str(self._stations[idx].get("id") or "").strip()
        ask = _CenteredConfirmDialog(self.root, "Stations", f"Remove station {sid}?", ok_text="Remove", cancel_text="Cancel")
        self.root.wait_window(ask)
        if not ask.result:
            return
        del self._stations[idx]
        self._render_station_tabs(self._stations)

    def _on_root_unmap(self, _event=None) -> None:
        if self._handling_unmap or self._is_closing:
            return
        try:
            if self.root.state() == "iconic":
                self._minimize_to_tray()
        except Exception:
            pass

    def _create_tray_icon(self):
        try:
            import pystray
            from PIL import Image, ImageDraw
        except Exception:
            return None

        img = None
        icon_path = os.path.join(self.repo_root, "lioil.ico")
        if os.path.exists(icon_path):
            try:
                with Image.open(icon_path) as ico:
                    img = ico.convert("RGBA").resize((64, 64), Image.LANCZOS)
            except Exception:
                img = None

        if img is None:
            img = Image.new("RGBA", (64, 64), (255, 255, 255, 0))
            draw = ImageDraw.Draw(img)
            draw.rounded_rectangle((8, 8, 56, 56), radius=10, fill=(24, 120, 196, 255))
            draw.rectangle((16, 18, 48, 46), fill=(255, 255, 255, 255))

        def _restore(_icon=None, _item=None) -> None:
            self.root.after(0, self._restore_from_tray)

        def _exit_app(_icon=None, _item=None) -> None:
            self.root.after(0, self._exit_from_tray)

        menu = pystray.Menu(pystray.MenuItem("Restore", _restore, default=True), pystray.MenuItem("Exit", _exit_app))
        return pystray.Icon("opendata_weather_ua", img, APP_TITLE, menu)

    def _minimize_to_tray(self) -> None:
        if self._in_tray or self._is_closing:
            return
        icon = self._create_tray_icon()
        if icon is None:
            # If tray dependencies are missing, fallback to normal minimize.
            return

        self._in_tray = True
        self._tray_icon = icon
        self._handling_unmap = True
        try:
            self.root.withdraw()
        finally:
            self._handling_unmap = False

        def _run_icon() -> None:
            try:
                icon.run()
            except Exception:
                pass

        self._tray_thread = threading.Thread(target=_run_icon, daemon=True)
        self._tray_thread.start()

    def _restore_from_tray(self) -> None:
        if self._tray_icon is not None:
            try:
                self._tray_icon.stop()
            except Exception:
                pass
            self._tray_icon = None
        self._in_tray = False

        if self.root.winfo_exists():
            self._handling_unmap = True
            try:
                self.root.deiconify()
                self.root.state("normal")
                self.root.lift()
                self.root.focus_force()
            finally:
                self._handling_unmap = False

    def _exit_from_tray(self) -> None:
        self._close_authorized = True
        self._on_close()

    def _start_services_async(self) -> None:
        if self._startup_thread and self._startup_thread.is_alive():
            return
        self.server_status_var.set("Server: starting...")

        def _worker() -> None:
            _cleanup_stale_services(self.repo_root)
            ok, msg, proc = _start_server_process(self.repo_root)
            self._startup_queue.put((ok, msg, proc))

        self._startup_thread = threading.Thread(target=_worker, daemon=True)
        self._startup_thread.start()

    def _poll_startup(self) -> None:
        try:
            while True:
                srv_ok, srv_msg, srv_proc = self._startup_queue.get_nowait()
                if srv_ok:
                    self._server_proc = srv_proc
                    self.server_status_var.set(srv_msg)
                else:
                    self.server_status_var.set(f"Server not running ({srv_msg})")
                    _SimpleMessageDialog(self.root, "OPC UA", f"OPC UA server failed to start.\n{srv_msg}").show()
        except queue.Empty:
            pass
        finally:
            if self.root.winfo_exists() and not self._is_closing:
                self.root.after(120, self._poll_startup)

    def _refresh_server_status(self) -> None:
        if self._is_closing or not self.root.winfo_exists():
            return

        endpoint = str(((self.cfg.get("opcUA") or {}).get("url")) or "opc.tcp://127.0.0.1:48480")
        parsed = _parse_opcua_url(endpoint) or ("127.0.0.1", 48480)
        proc_running = bool(self._server_proc and self._server_proc.poll() is None)
        port_open = _is_port_open(parsed[0], parsed[1])

        if proc_running:
            self.server_status_var.set(f"Server running (pid={self._server_proc.pid})")
        elif port_open:
            self.server_status_var.set("Server running")
        else:
            self.server_status_var.set("Server not running")

        self.root.after(2000, self._refresh_server_status)

    def _on_close_request(self) -> None:
        ask = _CenteredConfirmDialog(self.root, "Exit", "確定要關閉程式？", ok_text="Exit", cancel_text="Cancel")
        self.root.wait_window(ask)
        if ask.result:
            self._close_authorized = True
            self._on_close()

    def _stop_server_proc(self) -> None:
        proc = self._server_proc
        self._server_proc = None
        if proc is None:
            return
        if proc.poll() is not None:
            return

        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def _on_close(self) -> None:
        if not self._close_authorized:
            return
        if self._is_closing:
            return
        self._is_closing = True

        try:
            if self._tray_icon is not None:
                self._tray_icon.stop()
                self._tray_icon = None
            self._in_tray = False
        except Exception:
            pass

        try:
            self._stop_server_proc()
            _cleanup_stale_services(self.repo_root)
        except Exception:
            pass

        self.root.destroy()

    def _force_close(self) -> None:
        if self._is_closing:
            return
        self._is_closing = True
        try:
            self._stop_server_proc()
            _cleanup_stale_services(self.repo_root)
        except Exception:
            pass
        try:
            if self.root.winfo_exists():
                self.root.destroy()
        except Exception:
            pass

    def run(self) -> None:
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self._force_close()


def main(*, repo_root: str) -> None:
    if os.name == "nt":
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        except Exception:
            pass
    DesktopApp(repo_root=repo_root).run()


if __name__ == "__main__":
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    main(repo_root=root)
