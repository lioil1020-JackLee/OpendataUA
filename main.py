import argparse
import os
import sys


def repo_root() -> str:
    # When packaged as a frozen executable (PyInstaller), prefer the
    # directory of the executable so config files are created next to
    # the exe instead of the temporary extraction folder.
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def normalize_argv(argv: list[str] | None) -> list[str]:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        return ["ui"]
    if args[0] in ("ui", "server"):
        return args
    if args[0].startswith("-"):
        if args[0] in ("-h", "--help"):
            return args
        return ["ui", *args]
    return ["ui", *args]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="opendata-weather-ua")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("ui", help="Run desktop UI")

    p_srv = sub.add_parser("server", help="Run OPC UA server")
    p_srv.add_argument("--config", default=os.path.join(repo_root(), "config.json"))

    args = parser.parse_args(normalize_argv(argv))
    cmd = args.cmd or "ui"

    if cmd == "ui":
        from ui.desktop_ui import main as desktop_main

        try:
            desktop_main(repo_root=repo_root())
        except KeyboardInterrupt:
            # GUI mode should exit quietly even if console sends an interrupt event.
            return 0
        return 0

    if cmd == "server":
        from server.opcua_server import main as server_main

        return int(server_main(config_path=args.config) or 0)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
