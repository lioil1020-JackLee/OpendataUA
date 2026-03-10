import asyncio
import json
import os
import signal
import sys
import urllib.request as request
from copy import deepcopy
from datetime import datetime
from urllib.parse import urlparse

from asyncua import Client, Server, ua

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

MIRROR_ENDPOINT = "opc.tcp://lioil.ddnsfree.com:48484"
MIRROR_STATION_MAP = {
    "466900": "W466900",
    "466920": "W466920",
    "467050": "W467050",
    "467571": "W467571",
    "467441": "W467441",
}
MIRROR_TAG_NAME_MAP = {"24R": "R24"}


def _default_config() -> dict:
    return deepcopy(DEFAULT_CONFIG)


def _format_datetime_str(s: str) -> str:
    """Format ISO-like datetime strings to 'yyyy-MM-dd HH:mm:ss'."""
    if not s or not isinstance(s, str):
        return s
    s = s.strip()
    if not ("-" in s and ":" in s):
        return s
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            if s.endswith("Z"):
                s2 = s[:-1]
            else:
                s2 = s
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s2, fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue
        except Exception:
            pass
    return s


def _save_config(config_path: str, cfg: dict) -> None:
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=4)


def _parse_url(url: str) -> tuple[str, int]:
    parsed = urlparse(str(url or "").strip() or "opc.tcp://127.0.0.1:48480")
    host = parsed.hostname or "127.0.0.1"
    port = int(parsed.port or 48480)
    return host, port


def _load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        cfg = _default_config()
        _save_config(config_path, cfg)
        return cfg
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


class _RemoteMirrorWriter:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self._client: Client | None = None
        self._nodes: dict[str, dict[str, object]] = {}
        self._last_error: str = ""

    def _report_error(self, message: str) -> None:
        msg = str(message).strip()
        if msg and msg != self._last_error:
            self._last_error = msg
            # intentionally silent: do not emit prints or logs in packaged exe
            return

    async def _disconnect(self) -> None:
        c = self._client
        self._client = None
        self._nodes = {}
        if c is not None:
            try:
                await c.disconnect()
            except Exception:
                pass

    async def _connect_if_needed(self) -> bool:
        if self._client is not None:
            return True
        try:
            self._client = Client(url=self.endpoint)
            await self._client.connect()
            await self._resolve_nodes()
            self._last_error = ""
            return True
        except Exception as e:
            self._report_error(f"connect failed: {e}")
            await self._disconnect()
            return False

    async def _resolve_nodes(self) -> None:
        if self._client is None:
            return
        resolved: dict[str, dict[str, object]] = {}

        for sid, remote_sid in MIRROR_STATION_MAP.items():
            row_nodes: dict[str, object] = {}
            for tag in VALUE_TAGS:
                remote_tag = MIRROR_TAG_NAME_MAP.get(tag, tag)
                nodeid = f"ns=4;s=Root.OpenData.{remote_sid}.{remote_tag}"
                row_nodes[tag] = self._client.get_node(nodeid)
            resolved[sid] = row_nodes

        self._nodes = resolved

    async def _coerce_value(self, node, text: str):
        try:
            vt = await node.read_data_type_as_variant_type()
        except Exception:
            return text

        if vt in (ua.VariantType.String, ua.VariantType.ByteString, ua.VariantType.XmlElement):
            return text
        if vt == ua.VariantType.Boolean:
            return text.strip().lower() in ("1", "true", "yes", "on")

        if text == "":
            return None

        try:
            if vt in (ua.VariantType.Float, ua.VariantType.Double):
                return float(text)
            if vt in (
                ua.VariantType.SByte,
                ua.VariantType.Byte,
                ua.VariantType.Int16,
                ua.VariantType.UInt16,
                ua.VariantType.Int32,
                ua.VariantType.UInt32,
                ua.VariantType.Int64,
                ua.VariantType.UInt64,
            ):
                return int(float(text))
        except Exception:
            return None

        return text

    async def write_values(self, values: dict[str, dict[str, str]]) -> None:
        if not await self._connect_if_needed():
            return
        try:
            for sid, row in values.items():
                if sid not in MIRROR_STATION_MAP:
                    continue
                row_nodes = self._nodes.get(sid) or {}
                if not row_nodes:
                    continue
                for tag, node in row_nodes.items():
                    try:
                        raw = _format_datetime_str(str(row.get(tag, "")))
                        casted = await self._coerce_value(node, raw)
                        if casted is None:
                            continue
                        vt = await node.read_data_type_as_variant_type()
                        dv = ua.DataValue(
                            Value=ua.Variant(casted, vt),
                            SourceTimestamp=None,
                            ServerTimestamp=None,
                            SourcePicoseconds=None,
                            ServerPicoseconds=None,
                        )
                        await node.write_attribute(ua.AttributeIds.Value, dv)
                    except Exception:
                        continue
        except Exception:
            self._report_error("write loop failed; reconnecting")
            await self._disconnect()

    async def close(self) -> None:
        await self._disconnect()


def _station_ids(cfg: dict) -> list[str]:
    od = cfg.get("openData") or {}
    stations = od.get("stations") or []
    if isinstance(stations, list) and stations:
        return [str((s or {}).get("id") or "").strip() for s in stations if isinstance(s, dict)]
    return [str(x).strip() for x in (od.get("target") or []) if str(x).strip()]


def _fetch_values(cfg: dict, station_ids: list[str]) -> dict[str, dict[str, str]]:
    od = cfg.get("openData") or {}
    addr = str(od.get("address") or "").strip()
    api = str(od.get("api") or "").strip()
    auth = str(od.get("auth_key") or "").strip()
    ids = [x.strip() for x in station_ids if x.strip()]
    if not (addr and api and auth and ids):
        return {}

    url = f"{addr}{api}?Authorization={auth}&format=JSON&StationId={','.join(ids)}&WeatherElement=&GeoInfo=StationAltitude,CountyName"
    try:
        with request.urlopen(url, timeout=8) as resp:
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


async def run_server(config_path: str) -> int:
    cfg = _load_config(config_path)
    host, port = _parse_url(str((cfg.get("opcUA") or {}).get("url") or "opc.tcp://127.0.0.1:48480"))
    bind_ip = str((cfg.get("opcUA") or {}).get("bind_ip") or "").strip() or host

    server = Server()
    await server.init()
    endpoint = f"opc.tcp://{host}:{port}"
    server.set_endpoint(endpoint)
    server.set_server_name("OpenData Weather UA")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    idx = await server.register_namespace("urn:opendata:weather")
    weather_obj = await server.nodes.objects.add_object(idx, "Weather")

    station_nodes: dict[str, dict[str, ua.NodeId]] = {}
    for sid in _station_ids(cfg):
        sid = sid.strip()
        if not sid:
            continue
        station_obj = await weather_obj.add_object(idx, sid)
        nodes: dict[str, ua.NodeId] = {}
        for tag in VALUE_TAGS:
            v = await station_obj.add_variable(idx, tag, "")
            await v.set_writable(False)
            nodes[tag] = v
        station_nodes[sid] = nodes

    stop_event = asyncio.Event()
    mirror = _RemoteMirrorWriter(MIRROR_ENDPOINT)

    def _request_stop(*_args):
        stop_event.set()

    loop = asyncio.get_running_loop()
    if os.name != "nt":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _request_stop)

    async with server:
        while not stop_event.is_set():
            all_ids = sorted(set(list(station_nodes.keys()) + list(MIRROR_STATION_MAP.keys())))
            values = _fetch_values(cfg, all_ids)
            for sid, tag_nodes in station_nodes.items():
                row = values.get(sid, {})
                for tag, node in tag_nodes.items():
                    try:
                        val = _format_datetime_str(str(row.get(tag, "")))
                        await node.write_value(val)
                    except Exception:
                        pass
            await mirror.write_values(values)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass

    await mirror.close()
    return 0


def main(config_path: str) -> int:
    try:
        return int(asyncio.run(run_server(config_path=config_path)))
    except KeyboardInterrupt:
        return 0
    except Exception:
        # Silent failure: do not write logs or print to stderr in packaged exe
        return 1


if __name__ == "__main__":
    if getattr(sys, "frozen", False):
        cfg = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "config.json")
    else:
        cfg = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    raise SystemExit(main(config_path=cfg))
