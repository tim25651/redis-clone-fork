"""Test the handling of different RESP commands."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from redis_clone import NIL, OK, Handler, databases, encode_cmd

if TYPE_CHECKING:
    from collections.abc import Callable
    from unittest.mock import MagicMock


def test_handler_init_invalid_args(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"*2\r\n$3\r\nset\r\n:1\r\n")
    with pytest.raises(ValueError, match="Invalid command or args"):
        Handler(mock_socket, 0)


def test_handler_init(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(encode_cmd("SET key value"))
    handler = Handler(mock_socket, 0)
    assert handler.cmd == "set"
    assert handler.args == (b"key", b"value")
    assert handler.current_db == 0


def test_handler_ping(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(encode_cmd("PING"))
    handler = Handler(mock_socket, 0)
    handler.handle_ping()
    mock_socket.send.assert_called_once_with(b"+PONG\r\n")


def test_handler_echo(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(encode_cmd("ECHO hello"))
    handler = Handler(mock_socket, 0)
    handler.handle_echo()
    mock_socket.send.assert_called_once_with(b"$5\r\nhello\r\n")


def test_handler_set(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("SET key value"))
    handler = Handler(mock_socket, 0)
    handler.handle_set()
    mock_socket.send.assert_called_once_with(OK)
    assert databases[0] == {b"key": (b"value", None)}


def test_handler_set_with_ex(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("SET key value EX 1"))
    handler = Handler(mock_socket, 0)
    handler.handle_set()
    mock_socket.send.assert_called_once_with(OK)
    expiry = databases[0][b"key"][1]
    assert expiry is not None
    assert expiry > int(time.time() * 1000)
    assert databases[0] == {b"key": (b"value", expiry)}


@pytest.mark.parametrize(
    ("cmd", "wrong"),
    [
        ('SET mykey "myvalue" PX', "arguments"),
        ('SET mykey "myvalue" PX string', "expiry"),
    ],
)
def test_handler_invalid_set(
    cmd: str,
    wrong: str,
    get_mock_socket: Callable[..., MagicMock],
    unused_clear_databases: None,
) -> None:
    """Test command with invalid arguments."""
    mock_socket = get_mock_socket(encode_cmd(cmd))
    handler = Handler(mock_socket, 0)
    handler.handle_set()
    mock_socket.send.assert_called_once_with(
        b"-ERR invalid 'set' command %b\r\n" % wrong.encode()
    )


def test_handler_get(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("GET key"))
    databases[0] = {b"key": (b"value", None)}
    handler = Handler(mock_socket, 0)
    handler.handle_get()
    mock_socket.send.assert_called_once_with(b"$5\r\nvalue\r\n")


def test_handler_get_not_found(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("GET key"))
    handler = Handler(mock_socket, 0)
    handler.handle_get()
    mock_socket.send.assert_called_once_with(NIL)


def test_handler_mget(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("MGET key1 key2"))
    databases[0] = {b"key1": (b"value1", None), b"key2": (b"value2", None)}
    handler = Handler(mock_socket, 0)
    handler.handle_mget()
    mock_socket.send.assert_called_once_with(b"*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n")
    databases[0] = {}


def test_handler_mget_some_not_found(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("MGET key1 key2"))
    databases[0] = {b"key1": (b"value1", None)}
    handler = Handler(mock_socket, 0)
    handler.handle_mget()
    mock_socket.send.assert_called_once_with(b"*2\r\n$6\r\nvalue1\r\n$-1\r\n")


def test_handler_mset(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("MSET key1 value1 key2 value2"))
    handler = Handler(mock_socket, 0)
    handler.handle_mset()
    mock_socket.send.assert_called_once_with(OK)
    assert databases[0] == {b"key1": (b"value1", None), b"key2": (b"value2", None)}


def test_handler_del(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("DEL key1 key2"))
    databases[0] = {
        b"key1": (b"value1", None),
        b"key2": (b"value2", None),
        b"key3": (b"value3", None),
    }
    handler = Handler(mock_socket, 0)
    handler.handle_del()
    mock_socket.send.assert_called_once_with(b":2\r\n")
    assert databases[0] == {b"key3": (b"value3", None)}


def test_handler_del_not_existing(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("DEL key1 key2"))
    handler = Handler(mock_socket, 0)
    handler.handle_del()
    mock_socket.send.assert_called_once_with(b":0\r\n")


def test_handler_select(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("SELECT 1"))
    handler = Handler(mock_socket, 0)
    new_db = handler.handle_select()
    mock_socket.send.assert_called_once_with(OK)
    assert new_db == 1


def test_handler_select_invalid_index(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("SELECT 10"))
    handler = Handler(mock_socket, 0)
    new_db = handler.handle_select()
    mock_socket.send.assert_called_once_with(b"-ERR DB index out of range\r\n")
    assert new_db == 0


def test_handler_select_wrong_type(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"*2\r\n$6\r\nselect\r\n$1\r\na\r\n")
    handler = Handler(mock_socket, 0)
    new_db = handler.handle_select()
    mock_socket.send.assert_called_once_with(b"-ERR invalid DB index\r\n")
    assert new_db == 0


def test_handler_keys(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("KEYS a*"))
    databases[0] = {
        b"abc": (b"value1", None),
        b"acd": (b"value2", None),
        b"bcd": (b"value3", None),
    }
    handler = Handler(mock_socket, 0)
    handler.handle_keys()
    mock_socket.send.assert_called_once_with(b"*2\r\n$3\r\nabc\r\n$3\r\nacd\r\n")


def test_handler_keys_not_matching(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("KEYS x"))
    databases[0] = {
        b"abc": (b"value1", None),
        b"acd": (b"value2", None),
        b"bcd": (b"value3", None),
    }
    handler = Handler(mock_socket, 0)
    handler.handle_keys()
    mock_socket.send.assert_called_once_with(b"*0\r\n")


@pytest.mark.parametrize(
    "cmd", ["GET", "ECHO", "SET key", "SELECT", "MGET", "MSET key", "DEL", "KEYS"]
)
def test_handler_wrong_args(
    cmd: str, get_mock_socket: Callable[..., MagicMock]
) -> None:
    short = cmd.split(maxsplit=1)[0].lower()
    mock_socket = get_mock_socket(encode_cmd(cmd))
    handler = Handler(mock_socket, 0)
    func: Callable[[], None | int] = getattr(handler, f"handle_{short}")
    func()
    mock_socket.send.assert_called_once_with(
        b"-ERR wrong number of arguments for '%b' command\r\n" % short.encode()
    )
