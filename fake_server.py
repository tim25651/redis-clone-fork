"""A TCP redis clone from the fakeredis package.

Example:
    To start the server on port `7000` with `10` databases,
    a `120` second dump interval and `mydata.json` as dump file, run:

        python fake_server.py -p 7000 -n 10 -i 120 -f mydata.json

Note:
    *   The server listens on `127.0.0.1` and the specified `PORT`.
    *   Data is loaded from and periodically dumped to the specified file.

Usage:
    The `fake_server.py` script can be executed from the command line
    with the following options:

        python fake_server.py [-h] [-n N_DBS] [-i INTERVAL] [-f FILE] [-p PORT]

    *   `-h, --help`: Show the help message and exit.
    *   `-n N_DBS, --n-dbs N_DBS`: Initialize the server with this many databases
        (default: 8).
    *   `-i INTERVAL, --interval INTERVAL`: Dump the databases to disk every this many
        seconds. Setting the interval to 0 disables dumping. (default: 300).
    *   `-f FILE, --file FILE`: File path for storing database data. If it doesnt exist,
        empty databases will be initialized. (default: databases.json).
    *   `-p PORT, --port PORT`: Port number for the server to listen on (default: 6379).
    *   `-v, --verbose`: Verbose logging (default: False).
"""

from __future__ import annotations

import argparse
import logging
import time
from os import PathLike
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, TypeAlias

import msgspec
from fakeredis._commands import Item
from fakeredis._helpers import Database
from fakeredis._tcp_server import TcpFakeServer

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fakeredis import FakeServer
logger = logging.getLogger("fakeserver")

StrPath: TypeAlias = str | PathLike[str]

# Constants
DEFAULT_PORT = 6379
DEFAULT_N_DBS = 8
DEFAULT_DUMP_INTERVAL = 300
DEFAULT_DUMP_FILE = Path("databases.json")

_DECODER = msgspec.json.Decoder(dict[str, dict[str, str]])
_ENCODER = msgspec.json.Encoder()


def load_databases_from_file(
    path: StrPath, server: FakeServer, n_dbs: int = 8, dump: bool = True
) -> None:
    """Load databases from a JSON file.

    Args:
        path: Path to the JSON file.
        server: Redis server
        n_dbs: Number of databases to initialize if file is not available.
        dump: Dump newly created databases.
    """
    if server.dbs:
        logger.error("Databases already populated, skipping load")
        return

    databases = server.dbs
    path = Path(path)

    if path.exists():
        data = _DECODER.decode(path.read_bytes())
        # Convert keys back to integers and load the data
        for k, v in data.items():
            databases[int(k)] = Database(server.lock)
            for k2, v2 in v.items():
                databases[int(k)][k2.encode()] = Item(v2.encode())
        size = sum(len(v) for v in databases.values())
        logger.info("Databases loaded successfully: %d keys.", size)
    else:
        for ix in range(n_dbs):
            if ix not in databases:
                databases[ix] = Database(server.lock)
        logger.info("Initialized %d empty databases.", n_dbs)
        if dump:
            dump_databases_to_file(path, server)


def dump_databases_to_file(path: StrPath, server: FakeServer) -> None:
    """Dump databases to a JSON file.

    Args:
        path: Path to the JSON file.
        server: Redis server
    """
    databases = server.dbs
    try:
        size = sum(len(v) for v in databases.values())
        dump_databases = {
            str(k): {
                k2.decode(): v2.value.decode() for k2, v2 in v.items()
            }  # TODO: dont dump expired keys
            for k, v in databases.items()
        }

        path = Path(path)
        path.write_bytes(_ENCODER.encode(dump_databases))
        logger.info("Databases dumped successfully: %d keys.", size)
    except Exception:
        logger.exception("Failed to dump databases")


def periodic_dump(path: StrPath, server: FakeServer, interval: float = 300) -> None:
    """Periodically dump databases to a JSON file.

    Args:
        path: Path to the JSON file.
        server: Redis server
        interval: Time in seconds between the dumps.
    """
    while True:
        time.sleep(interval)
        dump_databases_to_file(path, server)


def cli(argv: Sequence[str] | None = None) -> None:
    """Command line interface for redis-clone.

    Args:
      argv: List of command line arguments. Defaults to sys.argv[1:].
    """
    parser = argparse.ArgumentParser(
        description=__doc__.split("Usage:")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-n",
        "--n-dbs",
        type=int,
        default=DEFAULT_N_DBS,
        help="Initialize the server with this many databases (default: %(default)s)",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=DEFAULT_DUMP_INTERVAL,
        help="Dump the databases to disk every this many seconds. Setting the interval to 0 disables dumping. (default: %(default)s)",  # noqa: E501
    )
    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        default=DEFAULT_DUMP_FILE,
        help="File path for storing database data. If it doesnt exist, empty databases will be initialized. (default: %(default)s)",  # noqa: E501
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port number for the server to listen on (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Verbose logging (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    if args.n_dbs <= 0:
        parser.error("Number of databases must be positive.")
    if args.interval < 0:
        parser.error("Dump interval must be positive or zero.")
    if args.port <= 0:
        parser.error("Port must be positive.")

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level)

    start_server(args.file, args.interval, args.port, args.n_dbs)


def start_server(
    dump_file: StrPath,
    dump_interval: int = DEFAULT_DUMP_INTERVAL,
    port: int = DEFAULT_PORT,
    n_dbs: int = DEFAULT_N_DBS,
) -> None:
    """Start a redis clone server.

    Args:
        dump_file: File to initially load the storage from and periodically dump it to.
        dump_interval: Time in seconds between the dumps. Defaults to 300.
        port: Port of the server. Defaults to 6379.
        n_dbs: Number of initialized databases. Defaults to 8.
    """
    dump = dump_interval > 0
    if not dump:
        logger.info("Dumping disabled")

    server_address = ("127.0.0.1", port)
    server = TcpFakeServer(server_address, server_type="redis")

    # Add this function call at startup to load the database
    load_databases_from_file(dump_file, server.fake_server, n_dbs, dump)

    # Start the periodic dump thread
    if dump:
        dump_thread = Thread(
            target=periodic_dump,
            args=(dump_file, server.fake_server, dump_interval),
            daemon=True,
        )
        dump_thread.start()

    server_thread = Thread(target=server.serve_forever)
    server_thread.start()
    logger.info("Server listening on %s:%s", "localhost", port)

    try:
        while True:
            time.sleep(0.1)
    finally:
        server.shutdown()
        # on exit
        if dump:
            dump_databases_to_file(dump_file, server.fake_server)


if __name__ == "__main__":
    cli()
