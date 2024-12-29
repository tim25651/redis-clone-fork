"""A small Redis clone built with Python.

This module provides a basic implementation of a Redis server, supporting
core functionalities like:

-   String storage (SET, GET, MSET, MGET, DEL)
-   Key patterns (KEYS)
-   Database selection (SELECT)
-   Basic commands (ECHO, PING)
-   Data persistence to a JSON file

It uses a custom RESP (Redis Serialization Protocol) decoder and encoder.

Usage:
    The `redis_clone.py` script can be executed from the command line with the following
    options:

        redis-clone [-h] [-n N_DBS] [-i INTERVAL] [-f FILE] [-p PORT]

    *   `-h, --help`: Show the help message and exit.
    *   `-n N_DBS, --n-dbs N_DBS`: Initialize the server with this many databases
        (default: 8).
    *   `-i INTERVAL, --interval INTERVAL`: Dump the databases to disk every this many
        seconds (default: 300).
    *   `-f FILE, --file FILE`: File path for storing database data
        (default: databases.json).
    *   `-p PORT, --port PORT`: Port number for the server to listen on (default: 6379).

Example:
    To start the server on port `7000` with `10` databases, a `120` second dump
    interval and `mydata.json` as dump file, run:

        redis-clone -p 7000 -n 10 -i 120 -f mydata.json

Note:
    *   The server listens on `localhost` and the specified `PORT`.
    *   Data is loaded from and periodically dumped to the specified file.
    *   The server does not handle concurrent connections.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import shlex
import socket
import threading
import time
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__package__)

StrPath: TypeAlias = str | PathLike[str]


# Constants
DEFAULT_PORT = 6379
DEFAULT_N_DBS = 8
DEFAULT_DUMP_INTERVAL = 300
DEFAULT_DUMP_FILE = Path("databases.json")
BUFFER_SIZE = 1024
SEP: bytes = b"\r\n"
NIL: bytes = b"$-1" + SEP
OK: bytes = b"+OK" + SEP

databases: dict[int, dict[bytes, tuple[bytes, int | None]]] = {}
"""Global storage of the databases.

The outer dictionary uses integers as keys for different databases.
The inner dictionaries use bytes as keys and store tuples containing:
the value and the expiry timestamp (None for no expiry).
"""


class SocketProto(Protocol):
    def send(self, data: bytes) -> None:
        """Send `data`."""

    def recv(self, n: int) -> bytes:
        """Receive `n` bytes."""

    def close(self) -> None:
        """Close the socket."""


class ConnectionBuffer:
    """Wraps SocketProto and adds support for reading until a delimiter."""

    def __init__(self, conn: SocketProto) -> None:
        """Initialize the connection buffer.

        Args:
            conn: Connection socket.
        """
        self.conn: SocketProto = conn
        """Connection socket."""
        self.buffer: bytes = b""
        """Buffer of unparsed data."""

    def read_until_delimiter(self, delimiter: bytes = SEP) -> bytes:
        """Read `self.buffer` and `self.conn` until a delimiter is found.

        Args:
            delimiter: Delimiter to search for. Defaults to `SEP`.

        Returns:
            Data before the delimiter, omitting the delimiter itself.

        Raises:
            ValueError: If data stream ends before the delimiter is found.
        """
        while delimiter not in self.buffer:
            try:
                data = self.conn.recv(BUFFER_SIZE)
                logger.debug("Received %s", data)
            except OSError as e:
                raise ValueError("Socket error during receive") from e

            if not data:
                raise ValueError("Socket closed.")

            self.buffer += data

        data_before_delimiter, _, self.buffer = self.buffer.partition(delimiter)
        return data_before_delimiter

    def read(self, bufsize: int) -> bytes:
        """Read exactly `bufsize` bytes from `self.buffer` and `self.conn`.

        Args:
            bufsize: Number of bytes to read.

        Returns:
            `bufsize` bytes from the stream.

        Raises:
            ValueError: If no or not enough data is received.
        """
        if len(self.buffer) < bufsize:
            try:
                data = self.conn.recv(BUFFER_SIZE)
                logger.debug("Received %s", data)
            except OSError as e:
                raise ValueError("Socket error during receive") from e

            if not data:
                raise ValueError("Socket closed")

            self.buffer += data

        data, self.buffer = self.buffer[:bufsize], self.buffer[bufsize:]
        if len(data) < bufsize:
            raise ValueError("Not enough data sent")
        return data


class RESPDecoder:
    """Decode a RESP response."""

    def __init__(self, conn: SocketProto) -> None:
        """Intialize the RESP decoder.

        Args:
            conn: Connection socket.
        """
        self.conn = ConnectionBuffer(conn)
        """Connection socket."""

    @overload
    def decode(  # pragma: no cover
        self, allow_list: None = None
    ) -> None | int | bytes | list[int | bytes | None]: ...

    @overload
    def decode(
        self, allow_list: Literal[True] = ...
    ) -> list[int | bytes | None]: ...  # pragma: no cover

    @overload
    def decode(
        self, allow_list: Literal[False] = ...
    ) -> None | int | bytes: ...  # pragma: no cover

    def decode(
        self, allow_list: bool | None = None
    ) -> None | int | bytes | list[int | bytes | None]:
        r"""Decode an incoming request.

        Reads the first byte to determine the data type. If "+",
        forwards the request to `decode_simple_string`, if "$" to `decode_bulk_string`,
        or if "*" to `decode_array`.

        Args:
            allow_list: If True only "\*" is supported, if False "\*" is
                not supported. Defaults to None.

        Returns:
            The decoded string or array.

        Raises:
            ValueError: If the first byte is neither "+", "$", or "*" or
                `allow_list` doesn't suit the marker.
        """
        type_byte = self.read_type_byte()

        if allow_list is not True and type_byte == b":":
            return self.decode_int()

        if allow_list is not True and type_byte == b"-":
            return self.decode_err_msg()

        if allow_list is not True and type_byte == b"+":
            return self.decode_simple_string()

        if allow_list is not True and type_byte == b"$":
            return self.decode_bulk_string()

        if type_byte == b"*":
            if allow_list is False:
                raise ValueError("Arrays disabled or nested arrays not supported")
            return self.decode_array()

        raise ValueError(f"Unknown data type byte: {type_byte!r}")

    def read_type_byte(self) -> bytes:
        """Read the first byte from the connection.

        Raises:
            ValueError: If reading from socket fails.
        """
        try:
            return self.conn.read(1)
        except ValueError as e:
            raise ValueError("Error reading from socket") from e

    def decode_int(self) -> int:
        """Decode a integer including the type byte.

        Raises:
            ValueError: If no integer is sent.
        """
        try:
            return int(self.decode_simple_string())
        except ValueError as e:
            raise ValueError("No integer was sent") from e

    def decode_err_msg(self) -> bytes:
        """Decode an error message.

        Raises:
            ValueError: If message does not start with "ERR ".
        """
        msg = self.conn.read_until_delimiter()
        if not msg.startswith(b"ERR "):
            raise ValueError("No error message")
        return b"-" + msg

    def decode_simple_string(self) -> bytes:
        """Decode a simple string until the delimiter."""
        return self.conn.read_until_delimiter()

    def decode_bulk_string(self) -> bytes | None:
        """Decode a bulk string with a defined size.

        Raises:
            ValueError: If more data than the specified size was sent.
        """
        bulk_string_length = int(self.conn.read_until_delimiter())
        if bulk_string_length == -1:
            return None
        data = self.conn.read(bulk_string_length)
        if self.conn.read_until_delimiter() != b"":
            raise ValueError("delimiter should be immediately after string")
        return data

    def decode_array(self) -> list[int | bytes | None]:
        """Decode multiple parts into a list."""
        result: list[int | bytes | None] = []
        array_length = int(self.conn.read_until_delimiter())
        result.extend(self.decode(allow_list=False) for _ in range(array_length))

        return result


class RESPEncoder:
    """Encodes data into RESP format."""

    @staticmethod
    def encode_simple_string(value: bytes) -> bytes:
        """Encode a simple string.

        Args:
            value: The string to encode.

        Returns:
           Encoded string.
        """
        return b"+" + value + SEP

    @staticmethod
    def encode_bulk_string(value: bytes) -> bytes:
        """Encode a bulk string.

        Args:
            value: The string to encode.

        Returns:
            Encoded bulk string.
        """
        return b"$%d" % len(value) + SEP + value + SEP

    @staticmethod
    def encode_array(values: list[bytes]) -> bytes:
        """Encode an array of already encoded bulk strings.

        Args:
            values: The list of encoded bulk strings.

        Returns:
            Encoded array string.
        """
        return b"*" + str(len(values)).encode() + SEP + b"".join(values)

    @staticmethod
    def encode_int(value: int) -> bytes:
        """Encode an integer to a RESP integer.

        Args:
            value: The integer to encode.

        Returns:
             Encoded integer string.
        """
        return b":" + str(value).encode() + SEP

    @staticmethod
    def encode_error(message: str) -> bytes:
        """Encode an error message into RESP error string.

        Args:
            message: The error message.

        Returns:
            Encoded error message string.
        """
        return b"-ERR " + message.encode() + SEP

    @staticmethod
    def encode_nil() -> bytes:
        """Encode a nil value.

        Returns:
             Encoded nil value string.
        """
        return NIL


def encode_cmd(cmd: str) -> bytes:
    """Encodes a command using RESPEncoder.

    Args:
        cmd: Shell-like quoted command with arguments.

    Returns:
        The encoded array for the command and each of its arguments.
    """
    args = shlex.split(cmd)
    bulk = [RESPEncoder.encode_bulk_string(arg.encode()) for arg in args]
    return RESPEncoder.encode_array(bulk)


def send_err(conn: SocketProto, msg: str) -> None:
    """Send a RESP error message to the client."""
    conn.send(RESPEncoder.encode_error(msg))


def get_single_key(current_db: int, key: bytes) -> bytes:
    """Get the entry in `current_db` for `key` if available.

    Args:
        current_db: The index of the current database.
        key: The key to look up.

    Returns:
        A RESP encoded bulk string if available or a RESP nil string if entry
        is not available or expired.
    """
    entry = databases[current_db].get(key)
    if entry is None:
        return RESPEncoder.encode_nil()
    value, expiry = entry
    if expiry is not None and expiry <= int(time.time() * 1000):
        del databases[current_db][key]
        return RESPEncoder.encode_nil()

    return RESPEncoder.encode_bulk_string(value)


class Handler:
    """Defines handlers for processing commands using the scheme "handle_cmd"."""

    def __init__(self, conn: SocketProto, current_db: int) -> None:
        """Initialize the handler with the connection and current database.

        Args:
            conn: Connection socket.
            current_db: The index of the current database.
        """
        self.conn: SocketProto = conn
        """Connection socket."""
        raw_args = RESPDecoder(conn).decode(allow_list=True)
        if not all(isinstance(x, bytes) for x in raw_args):
            raise ValueError("Invalid command or args")
        all_args: tuple[bytes, ...] = raw_args  # type: ignore[assignment]
        raw_command, *other_args = all_args
        self.cmd = raw_command.decode("ascii").lower()
        """Command to run"""
        self.args = tuple(other_args)
        """Remaining arguments as bytes."""
        self.current_db = current_db
        """The index of the current database."""

    def handle_ping(self) -> None:
        """Send a RESP PONG to the client."""
        self.conn.send(RESPEncoder.encode_simple_string(b"PONG"))

    def handle_echo(self) -> None:
        """Echo a RESP bulk string to the client."""
        if len(self.args) != 1:
            send_err(self.conn, "wrong number of arguments for 'echo' command")
            return
        self.conn.send(RESPEncoder.encode_bulk_string(self.args[0]))

    def handle_set(self) -> None:
        """Sets `args[0]` to `args[1]` in `current_db`, supports px and ex."""
        if len(self.args) < 2:  # noqa: PLR2004
            send_err(self.conn, "wrong number of arguments for 'set' command")
            return

        key, value, *raw_options = self.args
        expiry = None

        # Check for "px" or "ex" argument and extract expiry value
        options = tuple(x.lower() for x in raw_options)
        if b"px" in options or b"ex" in options:
            if len(options) < 2:  # noqa: PLR2004
                send_err(self.conn, "invalid 'set' command arguments")
                return
            ix_val, factor = (b"px", 1) if b"px" in options else (b"ex", 1000)
            try:
                expiry_index = options.index(ix_val) + 1
                expiry = int(options[expiry_index])
                expiry = int(time.time() * 1000) + expiry * factor
            except (ValueError, IndexError):
                send_err(self.conn, "invalid 'set' command expiry")
                return

        databases[self.current_db][key] = (value, expiry)
        self.conn.send(OK)

    def handle_get(self) -> None:
        """Gets value for `args[0]` from `current_db`. Sends the response.

        Sends a RESP bulk string or a RESP nil string.
        """
        if len(self.args) != 1:
            send_err(self.conn, "wrong number of arguments for 'get' command")
            return
        self.conn.send(get_single_key(self.current_db, self.args[0]))

    def handle_mget(self) -> None:
        """Get multiple keys from the current database. Sends a single response.

        Sends a RESP array of bulk or nil strings.
        """
        if not self.args:
            send_err(self.conn, "wrong number of arguments for 'mget' command")
            return

        response = [get_single_key(self.current_db, key) for key in self.args]
        self.conn.send(RESPEncoder.encode_array(response))

    def handle_mset(self) -> None:
        """Set each key:value pair in `current_db` with no expiry."""
        if len(self.args) % 2 != 0 or not self.args:
            # MSET requires an even number of arguments (key-value pairs)
            send_err(self.conn, "wrong number of arguments for 'mset' command")
            return

        # Process key-value pairs
        for i in range(0, len(self.args), 2):
            key = self.args[i]
            value = self.args[i + 1]
            databases[self.current_db][key] = (value, None)

        # Send a simple string response indicating success
        self.conn.send(OK)

    def handle_del(self) -> None:
        """Deletes keys from `current_db`. Sends a RESP integer with deleted count."""
        if not self.args:
            send_err(self.conn, "wrong number of arguments for 'del' command")
            return

        deleted = 0
        for key in self.args:
            if databases[self.current_db].pop(key, None) is not None:
                deleted += 1

        self.conn.send(RESPEncoder.encode_int(deleted))

    def handle_select(self) -> int:
        """Tries to switch to the requested db index. Sends "OK" on success.

        Returns:
            The requested db index on success, else `current_db`.
        """
        if len(self.args) != 1:
            # SELECT requires exactly one argument
            send_err(self.conn, "wrong number of arguments for 'select' command")
            return self.current_db

        try:
            db_index = int(self.args[0])
        except ValueError:
            # If the argument is not a valid integer
            send_err(self.conn, "invalid DB index")
            return self.current_db

        if db_index < 0 or db_index >= len(databases):
            # If the index is out of range
            send_err(self.conn, "DB index out of range")
            return self.current_db

        self.conn.send(OK)
        return db_index

    def handle_keys(self) -> None:
        """Finds matching keys in `current_db`. Sends a single response.

        Sends RESP array of bulk strings.
        """
        if len(self.args) != 1:
            # KEYS requires exactly one argument
            send_err(self.conn, "wrong number of arguments for 'keys' command")
            return

        # Find matching keys in the current database
        matching_keys = [
            key
            for key in databases[self.current_db]
            if fnmatch.fnmatch(key, self.args[0])
        ]
        # Construct the RESP array response
        encoded_keys = [RESPEncoder.encode_bulk_string(key) for key in matching_keys]

        self.conn.send(RESPEncoder.encode_array(encoded_keys))


def handle_connection(conn: SocketProto) -> None:
    """Handle incoming client requests.

    Args:
        conn: Connection socket.
    """
    current_db = 0
    try:
        while True:
            try:
                handler = Handler(conn, current_db)
                if handler.cmd == "select":
                    current_db = handler.handle_select()
                    continue

                handle_cmd: Callable[[], None] | None = getattr(
                    handler, f"handle_{handler.cmd}", None
                )
                if handle_cmd is None:
                    send_err(conn, "unknown command")
                else:
                    handle_cmd()
            except ValueError as e:
                logger.exception("Error processing command")
                send_err(conn, str(e))
                return  # close connection on error
    finally:
        conn.close()


def load_databases_from_file(path: StrPath, n_dbs: int = 8) -> None:
    """Load databases from a JSON file.

    Args:
        path: Path to the JSON file.
        n_dbs: Number of databases to initialize.
    """
    if databases:
        logger.error("Databases already populated, skipping load")
        return
    try:
        path = Path(path)
        if path.exists():
            with path.open() as file:
                data: dict[str, dict[str, tuple[str, int | None]]] = json.load(file)
                # Convert keys back to integers and load the data
                for k, v in data.items():
                    databases[int(k)] = {}
                    for k2, (v2, ex) in v.items():
                        # TODO: dont load expired keys
                        databases[int(k)][k2.encode()] = (v2.encode(), ex)
                size = sum(len(v) for v in databases.values())
                logger.info("Databases loaded successfully: %d keys.", size)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning(
            "Database file not found or invalid. Starting with empty databases."
        )
    except Exception:
        logger.exception("Failed to load databases")
    finally:
        for ix in range(n_dbs):
            if ix not in databases:
                databases[ix] = {}


def dump_databases_to_file(path: StrPath) -> None:
    """Dump databases to a JSON file.

    Args:
        path: Path to the JSON file.
    """
    try:
        size = sum(len(v) for v in databases.values())
        dump_databases = {
            str(k): {
                k2.decode(): (v2.decode(), ex) for k2, (v2, ex) in v.items()
            }  # TODO: dont dump expired keys
            for k, v in databases.items()
        }

        path = Path(path)
        with path.open("w") as file:
            json.dump(dump_databases, file)
            logger.info("Databases dumped successfully: %d keys.", size)
    except Exception:
        logger.exception("Failed to dump databases")


def periodic_dump(path: StrPath, interval: float = 300) -> None:
    """Periodically dump databases to a JSON file.

    Args:
        path: Path to the JSON file.
        interval: Time in seconds between the dumps.
    """
    while True:
        time.sleep(interval)
        dump_databases_to_file(path)


def cli(argv: Sequence[str] | None = None) -> None:
    """Command line interface for redis-clone.

    Args:
      argv: List of command line arguments. Defaults to sys.argv[1:].
    """
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="A simple Redis clone")
    parser.add_argument(
        "-n",
        "--n-dbs",
        type=int,
        default=DEFAULT_N_DBS,
        help="Number of databases (default: %(default)s)",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=DEFAULT_DUMP_INTERVAL,
        help="Dump interval (seconds, default: %(default)s)",
    )
    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        default=DEFAULT_DUMP_FILE,
        help="Dump file (default: %(default)s)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Port to listen on (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    if args.n_dbs <= 0:
        parser.error("Number of databases must be positive.")
    if args.interval <= 0:
        parser.error("Dump interval must be positive.")
    if args.port <= 0:
        parser.error("Port must be positive.")

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
    try:
        server_socket = socket.create_server(("localhost", port), reuse_port=True)
        logger.info("Server listening on %s:%s", "localhost", port)
        # Add this function call at startup to load the database
        load_databases_from_file(dump_file, n_dbs)

        # Start the periodic dump thread
        dump_thread = threading.Thread(
            target=periodic_dump, args=(dump_file, dump_interval), daemon=True
        )
        dump_thread.start()

        while True:
            conn, _ = server_socket.accept()  # wait for client
            logger.debug("Accepted connection from %s:%s", *conn.getpeername())
            threading.Thread(target=handle_connection, args=(conn,)).start()
    except OSError as e:
        logger.critical("Could not create socket: %s", e)
    except KeyboardInterrupt:
        logger.info("Shutting down server")
    finally:
        if "server_socket" in locals():
            server_socket.close()  # type: ignore[possibly-undefined]


__all__ = [
    "Handler",
    "RESPDecoder",
    "RESPEncoder",
    "cli",
    "databases",
    "encode_cmd",
    "start_server",
]

if __name__ == "__main__":
    cli()
