"""Chay thu peer P2P tren Linux; thoat bang Ctrl+C.

Chuoi --connect co the lap lai, vi du: --connect peer-b --connect peer-c.
Module apps.peer cung cap API de UI va messaging cua nhom goi truc tiep.
"""

import argparse
import asyncio
import logging

from apps.peer import PeerNode, TrackerClient


async def run(args):
    node = PeerNode(args.peer_id, args.user_id, TrackerClient(args.tracker),
                    args.listen_ip, args.listen_port)
    await node.start()
    print(f"Peer {node.peer_id} dang lang nghe cong {node.listen_port}", flush=True)
    try:
        for peer_id in args.connect:
            await node.connect_peer(peer_id)
            print(f"Da ket noi {peer_id}", flush=True)
        while True:
            peer_id, frame = await node.incoming.get()
            print(f"Nhan tu {peer_id}: {frame}", flush=True)
    finally:
        await node.close()


def main():
    parser = argparse.ArgumentParser(description="Peer discovery va P2P connection")
    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--tracker", required=True, help="Vi du http://192.168.56.10:2027")
    parser.add_argument("--listen-ip", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--connect", action="append", default=[])
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run(parser.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
