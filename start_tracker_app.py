"""Start the tracker and channel-management REST application."""

import argparse

from apps.tracker_app import create_tracker_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the P2P tracker application")
    parser.add_argument("--server-ip", default="0.0.0.0")
    parser.add_argument("--server-port", type=int, default=2027)
    args = parser.parse_args()
    create_tracker_app(args.server_ip, args.server_port)


if __name__ == "__main__":
    main()
