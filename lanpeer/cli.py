"""Terminal interface for LAN peer chat and file sharing."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import queue
import socket
import threading

from .network import Peer, PeerNode


def format_size(count: int) -> str:
    value = float(count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{count} B"
        value /= 1024
    return f"{count} B"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for number in range(1, 10_000):
        candidate = path.with_name(f"{path.stem} ({number}){path.suffix}")
        if not candidate.exists():
            return candidate
    raise OSError("Could not find a free filename")


class TerminalApp:
    def __init__(self, name: str, port: int, discovery_port: int, discovery: bool) -> None:
        self.events: queue.Queue[tuple[str, object]] = queue.Queue()
        self.node = PeerNode(name, port, discovery_port, self._on_network_event, discovery)
        self.selected_id: str | None = None
        self.pending_files: dict[int, dict] = {}
        self.next_offer = 1
        self.last_progress: dict[tuple[str, str], int] = {}
        self.running = True

    def _on_network_event(self, kind: str, data: dict) -> None:
        self.events.put(("network", (kind, data)))

    def _read_input(self) -> None:
        while self.running:
            try:
                line = input("> ")
            except EOFError:
                line = "/quit"
            self.events.put(("input", line))
            if line.strip().lower() in {"/quit", "/exit"}:
                return

    def run(self) -> None:
        self.node.start()
        print(f"LAN Peer Chat | You: {self.node.name} | TCP port: {self.node.port}")
        print("Connect both computers to the same LAN. Type /help for commands.")
        threading.Thread(target=self._read_input, daemon=True).start()
        try:
            while self.running:
                source, item = self.events.get()
                if source == "input":
                    self._handle_input(str(item).strip())
                else:
                    kind, data = item
                    self._handle_network_event(kind, data)
        except KeyboardInterrupt:
            print("\nExiting...")
        finally:
            self.running = False
            for offer in self.pending_files.values():
                offer["answer"].put(None)
            self.node.stop()

    def _selected_peer(self) -> Peer | None:
        return next((peer for peer in self.node.list_peers() if peer.peer_id == self.selected_id), None)

    def _show_peers(self) -> None:
        peers = self.node.list_peers()
        if not peers:
            print("No peers found. Try /connect IP[:port].")
            return
        for number, peer in enumerate(peers, 1):
            marker = "*" if peer.peer_id == self.selected_id else " "
            print(f"{marker} {number}. {peer.name} ({peer.host}:{peer.port})")

    def _handle_input(self, line: str) -> None:
        if not line:
            return
        if not line.startswith("/"):
            peer = self._selected_peer()
            if peer is None:
                print("Select a peer first with /select NUMBER (or wait for one to appear).")
                return
            self.node.send_chat(peer.peer_id, line)
            return

        command, _, argument = line.partition(" ")
        command = command.lower()
        argument = argument.strip()
        if command == "/help":
            print("/peers                 List peers (* is selected)")
            print("/connect IP[:port]     Connect manually (default port 5001)")
            print("/select NUMBER         Select the peer to chat with")
            print("/file PATH             Offer a file to the selected peer")
            print("/accept [N] [PATH]     Accept an offered file (saves in received_files by default)")
            print("/reject [N]           Reject an offered file")
            print("/quit                 Exit")
            print("Type any other line to send it as a chat message.")
        elif command == "/peers":
            self._show_peers()
        elif command == "/connect":
            if not argument:
                print("Usage: /connect IP[:port]")
                return
            host, separator, port_text = argument.partition(":")
            try:
                port = int(port_text) if separator else 5001
                if not host or not (1 <= port <= 65535):
                    raise ValueError
            except ValueError:
                print("Enter an IPv4 address or hostname, optionally followed by :port.")
                return
            self.node.add_by_ip(host, port)
            print(f"Connecting to {host}:{port}...")
        elif command == "/select":
            peers = self.node.list_peers()
            try:
                number = int(argument)
                if not 1 <= number <= len(peers):
                    raise ValueError
            except ValueError:
                print("Usage: /select NUMBER (see /peers)")
                return
            peer = peers[number - 1]
            self.selected_id = peer.peer_id
            print(f"Chatting with {peer.name}.")
        elif command == "/file":
            peer = self._selected_peer()
            if peer is None:
                print("Select a peer first with /select NUMBER.")
                return
            source = Path(argument.strip('"')).expanduser()
            if not argument or not source.is_file():
                print("Usage: /file PATH (path must name an existing file)")
                return
            self.node.send_file(peer.peer_id, str(source))
            print(f"Offering {source.name} to {peer.name}...")
        elif command in {"/accept", "/reject"}:
            self._answer_file(command == "/accept", argument)
        elif command in {"/quit", "/exit"}:
            self.running = False
            print("Goodbye.")
        else:
            print("Unknown command. Type /help.")

    def _answer_file(self, accept: bool, argument: str) -> None:
        if not self.pending_files:
            print("No incoming file offers.")
            return
        first, separator, rest = argument.partition(" ")
        if first.isdigit():
            offer_number = int(first)
            target_text = rest.strip() if separator else ""
        elif len(self.pending_files) == 1:
            offer_number = next(iter(self.pending_files))
            target_text = argument
        else:
            print("Several files are waiting. Use /accept NUMBER or /reject NUMBER.")
            return
        offer = self.pending_files.pop(offer_number, None)
        if offer is None:
            print("No file offer has that number.")
            return
        if not accept:
            offer["answer"].put(None)
            print(f"Rejected {offer['name']}.")
            return
        try:
            if target_text:
                destination = Path(target_text.strip('"')).expanduser()
                if destination.is_dir():
                    destination /= str(offer["name"])
            else:
                destination = Path.cwd() / "received_files" / str(offer["name"])
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination = unique_path(destination)
            offer["answer"].put(str(destination))
            print(f"Receiving {offer['name']} into {destination}...")
        except OSError as exc:
            offer["answer"].put(None)
            print(f"Could not save file: {exc}")

    def _handle_network_event(self, kind: str, data: dict) -> None:
        if kind == "peers_changed":
            peers = self.node.list_peers()
            if self._selected_peer() is None:
                self.selected_id = peers[0].peer_id if len(peers) == 1 else None
                if self.selected_id:
                    print(f"\nConnected to {peers[0].name}. You can type a message now.")
            else:
                print("\nPeer list changed. Type /peers to see it.")
        elif kind == "chat_received":
            print(f"\n{data['name']}: {data['text']}")
        elif kind == "chat_sent":
            print(f"You → {data['name']}: {data['text']}")
        elif kind == "file_offer":
            number = self.next_offer
            self.next_offer += 1
            self.pending_files[number] = data
            print(f"\nFile offer {number}: {data['sender']} wants to send {data['name']} ({format_size(int(data['size']))}).")
            print(f"Type /accept {number} or /reject {number}.")
        elif kind == "file_progress":
            total = int(data["total"])
            percentage = int(100 * int(data["done"]) / total) if total else 100
            key = (str(data["transfer_id"]), str(data["direction"]))
            previous = self.last_progress.get(key, -10)
            if percentage // 10 > previous // 10 or percentage == 100:
                self.last_progress[key] = percentage
                print(f"{data['direction']} {data['name']}: {percentage}%")
        elif kind == "file_sent":
            print(f"Sent {data['name']} to {data['peer']}.")
        elif kind == "file_received":
            print(f"Received {data['name']} from {data['sender']} → {data['path']}.")
        elif kind == "file_rejected":
            print(f"{data['peer']} rejected {data['name']}.")
        elif kind == "error":
            print(f"Error: {data['message']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal peer-to-peer chat and file sharing on a LAN")
    parser.add_argument("--name", default=os.environ.get("USER") or socket.gethostname(), help="Your displayed name")
    parser.add_argument("--port", type=int, default=5001, help="TCP listening port (default: 5001)")
    parser.add_argument("--discovery-port", type=int, default=5000, help="UDP discovery port (default: 5000)")
    parser.add_argument("--no-discovery", action="store_true", help="Use /connect instead of automatic discovery")
    args = parser.parse_args()
    try:
        TerminalApp(args.name, args.port, args.discovery_port, not args.no_discovery).run()
    except OSError as exc:
        print(f"Could not start on TCP port {args.port}: {exc}")
        raise SystemExit(1) from exc
