"""Test encoding, decoding, and helper functions."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from redis_clone import (
    NIL,
    OK,
    ConnectionBuffer,
    RESPDecoder,
    RESPEncoder,
    databases,
    encode_cmd,
    get_single_key,
    send_err,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from unittest.mock import MagicMock


def test_read_until_delimiter_success(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test successful read until delimiter."""
    mock_socket = get_mock_socket(b"hello\r\nworld", b"")
    conn_buffer = ConnectionBuffer(mock_socket)
    data = conn_buffer.read_until_delimiter()
    assert data == b"hello"
    assert conn_buffer.buffer == b"world"


def test_read_until_delimiter_no_delimiter_in_first_chunk(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test read with no delimiter in the first chunk but it's in second."""
    mock_socket = get_mock_socket(b"hello", b"world\r\n")
    conn_buffer = ConnectionBuffer(mock_socket)
    data = conn_buffer.read_until_delimiter()
    assert data == b"helloworld"
    assert conn_buffer.buffer == b""


def test_read_until_delimiter_empty_buffer(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test read when socket closes."""
    mock_socket = get_mock_socket(b"")
    conn_buffer = ConnectionBuffer(mock_socket)

    with pytest.raises(ValueError, match="Socket closed."):
        conn_buffer.read_until_delimiter()


def test_read_until_delimiter_socket_error(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test read when socket receive raises an exception."""
    mock_socket = get_mock_socket()
    mock_socket.recv.side_effect = OSError("Socket error")
    conn_buffer = ConnectionBuffer(mock_socket)

    with pytest.raises(ValueError, match="Socket error during receive"):
        conn_buffer.read_until_delimiter()


def test_read_success(get_mock_socket: Callable[..., MagicMock]) -> None:
    """Test successful read of a specific number of bytes."""
    mock_socket = get_mock_socket(b"abcdefgh")
    conn_buffer = ConnectionBuffer(mock_socket)

    data = conn_buffer.read(4)
    assert data == b"abcd"
    assert conn_buffer.buffer == b"efgh"


def test_read_from_buffer(get_mock_socket: Callable[..., MagicMock]) -> None:
    """Test read from buffer without using recv."""
    mock_socket = get_mock_socket()
    conn_buffer = ConnectionBuffer(mock_socket)
    conn_buffer.buffer = b"abcdefgh"
    data = conn_buffer.read(4)
    assert data == b"abcd"
    assert conn_buffer.buffer == b"efgh"


def test_read_not_enough_data_on_socket_closed(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test read when socket closed and not enough data."""
    mock_socket = get_mock_socket(b"abc", b"")
    conn_buffer = ConnectionBuffer(mock_socket)

    with pytest.raises(ValueError, match="Not enough data sent"):
        conn_buffer.read(5)


def test_read_socket_closed_empty_buffer(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    """Test read when socket closes when buffer is empty."""
    mock_socket = get_mock_socket(b"")
    conn_buffer = ConnectionBuffer(mock_socket)
    with pytest.raises(ValueError, match="Socket closed"):
        conn_buffer.read(5)


def test_read_socket_error(get_mock_socket: Callable[..., MagicMock]) -> None:
    """Test read when socket receive raises an exception."""
    mock_socket = get_mock_socket()
    mock_socket.recv.side_effect = OSError("Socket error")
    conn_buffer = ConnectionBuffer(mock_socket)

    with pytest.raises(ValueError, match="Socket error during receive"):
        conn_buffer.read(5)


def test_decode_simple_string(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(OK)
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode() == b"OK"


def test_decode_int(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(123)
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode() == 123


def test_decode_error_message(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"-ERR something is wrong\r\n")
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode() == b"-ERR something is wrong"


def test_decode_bulk_string(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket("hello")
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode() == b"hello"


def test_decode_bulk_string_none(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(NIL)
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode() is None


def test_decode_bulk_string_invalid_delimiter(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(b"$5\r\nhello1\r\n")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(
        ValueError, match="delimiter should be immediately after string"
    ):
        decoder.decode()


@pytest.mark.parametrize("allow_list", [True, False, None])
def test_decode_array(
    allow_list: bool | None, get_mock_socket: Callable[..., MagicMock]
) -> None:
    mock_socket = get_mock_socket(["hello", "world"])
    decoder = RESPDecoder(mock_socket)
    if allow_list is not False:
        assert decoder.decode(allow_list=allow_list) == [b"hello", b"world"]
    else:
        with pytest.raises(
            ValueError, match="Arrays disabled or nested arrays not supported"
        ):
            decoder.decode(allow_list=allow_list)


def test_decode_array_nested(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(
        ValueError, match="Arrays disabled or nested arrays not supported"
    ):
        decoder.decode()


def test_decode_with_allow_list_false_no_error(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(OK)
    decoder = RESPDecoder(mock_socket)
    assert decoder.decode(allow_list=False) == b"OK"


def test_decode_invalid_type(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"%something\r\n")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(ValueError, match="Unknown data type byte"):
        decoder.decode()


def test_decode_socket_error(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket()
    mock_socket.recv.side_effect = OSError("Socket error")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(ValueError, match="Error reading from socket"):
        decoder.decode()


def test_decode_int_error(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b":a\r\n")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(ValueError, match="No integer was sent"):
        decoder.decode()


def test_decode_err_msg_error(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket(b"-something\r\n")
    decoder = RESPDecoder(mock_socket)
    with pytest.raises(ValueError, match="No error message"):
        decoder.decode()


def test_encode_simple_string() -> None:
    assert RESPEncoder.encode_simple_string(b"OK") == b"+OK\r\n"


def test_encode_bulk_string() -> None:
    assert RESPEncoder.encode_bulk_string(b"hello") == b"$5\r\nhello\r\n"


def test_encode_array() -> None:
    encoded_strings = [
        RESPEncoder.encode_bulk_string(b"hello"),
        RESPEncoder.encode_bulk_string(b"world"),
    ]
    assert (
        RESPEncoder.encode_array(encoded_strings)
        == b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    )


def test_encode_int() -> None:
    assert RESPEncoder.encode_int(123) == b":123\r\n"


def test_encode_error() -> None:
    assert (
        RESPEncoder.encode_error("something is wrong") == b"-ERR something is wrong\r\n"
    )


def test_encode_nil() -> None:
    assert RESPEncoder.encode_nil() == b"$-1\r\n"


def test_encode_cmd_simple() -> None:
    cmd = "SET key value"
    expected = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
    assert encode_cmd(cmd) == expected


def test_encode_cmd_with_quotes() -> None:
    cmd = 'SET key "long value"'
    expected = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$10\r\nlong value\r\n"
    assert encode_cmd(cmd) == expected


def test_send_err(get_mock_socket: Callable[..., MagicMock]) -> None:
    mock_socket = get_mock_socket()
    send_err(mock_socket, "Test error")
    mock_socket.send.assert_called_once_with(b"-ERR Test error\r\n")


def test_get_single_key_existing(unused_clear_databases: None) -> None:
    databases[0] = {b"key": (b"value", None)}
    assert get_single_key(0, b"key") == b"$5\r\nvalue\r\n"


def test_get_single_key_not_existing(unused_clear_databases: None) -> None:
    assert get_single_key(0, b"key") == b"$-1\r\n"


def test_get_single_key_expired(unused_clear_databases: None) -> None:
    databases[0] = {b"key": (b"value", int(time.time() * 1000) - 1000)}
    assert get_single_key(0, b"key") == b"$-1\r\n"
    assert b"key" not in databases[0]
