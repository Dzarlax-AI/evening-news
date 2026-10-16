"""Fixed, peer-restricted CDP TCP relay for the isolated browser namespace."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 9222
TARGET_HOST = "127.0.0.1"
TARGET_PORT = 5100
MAX_CONNECTIONS = 4
BUFFER_BYTES = 65536
IDLE_SECONDS = 120
CONNECT_SECONDS = 2
LIFETIME_SECONDS = 7200


def private_controller_ip(value: str) -> str:
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        raise ValueError("Controller address must be an exact private IPv4 literal") from None
    if not address.is_private or address.is_loopback or address.is_link_local or address.is_multicast:
        raise ValueError("Controller address must be an exact private IPv4 literal")
    return str(address)


async def copy_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await asyncio.wait_for(reader.read(BUFFER_BYTES), timeout=IDLE_SECONDS)
        if not data:
            if writer.can_write_eof():
                writer.write_eof()
                await writer.drain()
            return
        writer.write(data)
        await asyncio.wait_for(writer.drain(), timeout=IDLE_SECONDS)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, ConnectionError, RuntimeError):
        pass


class Relay:
    def __init__(self, controller_ip: str, target_host: str = TARGET_HOST,
                 target_port: int = TARGET_PORT):
        self.controller_ip = controller_ip
        self.target_host = target_host
        self.target_port = target_port
        self.active = 0
        self.tasks: set[asyncio.Task] = set()

    async def handle(self, client_reader: asyncio.StreamReader,
                     client_writer: asyncio.StreamWriter) -> None:
        peer = client_writer.get_extra_info("peername")
        if not peer or peer[0] != self.controller_ip or self.active >= MAX_CONNECTIONS:
            client_writer.close()
            await client_writer.wait_closed()
            return
        self.active += 1
        target_writer = None
        try:
            target_reader, target_writer = await asyncio.wait_for(
                asyncio.open_connection(self.target_host, self.target_port), timeout=CONNECT_SECONDS)
            forward = asyncio.create_task(copy_stream(client_reader, target_writer))
            reverse = asyncio.create_task(copy_stream(target_reader, client_writer))
            self.tasks.update((forward, reverse))
            done, pending = await asyncio.wait((forward, reverse), return_when=asyncio.FIRST_COMPLETED)
            for completed in done:
                completed.result()
            if pending:
                await asyncio.wait_for(asyncio.gather(*pending), timeout=IDLE_SECONDS)
        except (OSError, asyncio.TimeoutError, ConnectionError, RuntimeError):
            pass  # Never log CDP payloads, endpoint URLs, or connection details.
        finally:
            for task in (locals().get("forward"), locals().get("reverse")):
                if task is not None:
                    task.cancel()
                    self.tasks.discard(task)
            try:
                if target_writer is not None:
                    await close_writer(target_writer)
            finally:
                try:
                    await close_writer(client_writer)
                finally:
                    self.active -= 1


async def serve(controller_ip: str) -> None:
    relay = Relay(private_controller_ip(controller_ip))
    server = await asyncio.start_server(relay.handle, LISTEN_HOST, LISTEN_PORT,
                                        limit=BUFFER_BYTES, backlog=MAX_CONNECTIONS)
    async with server:
        try:
            await asyncio.wait_for(server.serve_forever(), timeout=LIFETIME_SECONDS)
        except asyncio.TimeoutError:
            pass
    for task in tuple(relay.tasks):
        task.cancel()
    if relay.tasks:
        await asyncio.gather(*relay.tasks, return_exceptions=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--controller-ip", required=True)
    args = parser.parse_args(argv)
    asyncio.run(serve(args.controller_ip))


if __name__ == "__main__":
    main()
