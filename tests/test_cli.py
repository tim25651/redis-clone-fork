"""Test cli, start_server and handle_connection."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from redis_clone import (
    DEFAULT_DUMP_FILE,
    DEFAULT_DUMP_INTERVAL,
    DEFAULT_N_DBS,
    DEFAULT_PORT,
    OK,
    cli,
    databases,
    dump_databases_to_file,
    encode_cmd,
    handle_connection,
    load_databases_from_file,
    periodic_dump,
    start_server,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from unittest.mock import MagicMock

    from pytest_mock import MockerFixture


def test_handle_connection_simple_command(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(
        encode_cmd("PING"), b""
    )  # ping command then socket close
    handle_connection(mock_socket)
    mock_socket.send.assert_any_call(b"+PONG\r\n")
    mock_socket.close.assert_called_once()


def test_handle_connection_select_db(
    get_mock_socket: Callable[..., MagicMock], unused_clear_databases: None
) -> None:
    mock_socket = get_mock_socket(encode_cmd("SELECT 1"), encode_cmd("PING"), b"")
    handle_connection(mock_socket)
    mock_socket.send.assert_any_call(OK)
    mock_socket.send.assert_any_call(b"+PONG\r\n")
    mock_socket.close.assert_called_once()


def test_handle_connection_unknown_command(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(encode_cmd("NOPE"), b"")
    handle_connection(mock_socket)
    mock_socket.send.assert_any_call(b"-ERR unknown command\r\n")
    mock_socket.close.assert_called_once()


def test_handle_connection_command_error(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(b"*2\r\n$4\r\nECHO\r\n:1\r\n", b"")
    handle_connection(mock_socket)
    mock_socket.send.assert_any_call(b"-ERR Invalid command or args\r\n")
    mock_socket.close.assert_called_once()


def test_handle_connection_value_error(
    get_mock_socket: Callable[..., MagicMock],
) -> None:
    mock_socket = get_mock_socket(encode_cmd("GET"), b"")
    handle_connection(mock_socket)
    mock_socket.send.assert_any_call(
        b"-ERR wrong number of arguments for 'get' command\r\n"
    )
    mock_socket.close.assert_called_once()


def test_load_databases_from_file_success(
    tmp_path: Path, unused_reset_databases: None
) -> None:
    file_path = tmp_path / "test_db.json"
    data = {
        "0": {"key1": ("value1", 1234), "key2": ("value2", None)},
        "1": {"key3": ("value3", None)},
    }
    with file_path.open("w") as f:
        json.dump(data, f)

    load_databases_from_file(file_path, n_dbs=2)

    assert databases == {
        0: {b"key1": (b"value1", 1234), b"key2": (b"value2", None)},
        1: {b"key3": (b"value3", None)},
    }


def test_load_databases_from_file_no_file(
    tmp_path: Path, unused_reset_databases: None
) -> None:
    file_path = tmp_path / "test_db.json"
    load_databases_from_file(file_path, n_dbs=2)
    assert databases == {ix: {} for ix in range(2)}


def test_load_databases_from_file_invalid_json(
    mocker: MockerFixture, tmp_path: Path, unused_reset_databases: None
) -> None:
    mocked_logger = mocker.patch("redis_clone.logger")
    file_path = tmp_path / "test_db.json"
    with file_path.open("w") as f:
        f.write("invalid json")

    load_databases_from_file(file_path, n_dbs=2)
    assert databases == {ix: {} for ix in range(2)}
    mocked_logger.warning.assert_called_once_with(
        "Database file not found or invalid. Starting with empty databases."
    )


def test_load_databases_already_loaded(
    mocker: MockerFixture, tmp_path: Path, unused_clear_databases: None
) -> None:
    mocked_logger = mocker.patch("redis_clone.logger")
    file_path = tmp_path / "test_db.json"
    databases[0] = {b"key": (b"value", None)}
    load_databases_from_file(file_path, n_dbs=2)
    assert databases == {
        0: {b"key": (b"value", None)},
        **{ix: {} for ix in range(1, DEFAULT_N_DBS)},
    }
    mocked_logger.error.assert_called_once_with(
        "Databases already populated, skipping load"
    )


def test_load_databases_exception(
    mocker: MockerFixture, tmp_path: Path, unused_reset_databases: None
) -> None:
    mocked_logger = mocker.patch("redis_clone.logger")
    file_path = tmp_path / "test_db.json"
    mocker.patch("redis_clone.Path", side_effect=Exception("Something"))

    load_databases_from_file(file_path, n_dbs=2)
    assert databases == {ix: {} for ix in range(2)}

    mocked_logger.exception.assert_called_once_with("Failed to load databases")


def test_dump_databases_to_file_success(
    tmp_path: Path, unused_clear_databases: None
) -> None:
    file_path = tmp_path / "dump_db.json"
    databases[0] = {b"key1": (b"value1", 1234), b"key2": (b"value2", None)}
    databases[1] = {b"key3": (b"value3", None)}

    dump_databases_to_file(file_path)

    with file_path.open() as f:
        loaded_data = json.load(f)

    assert loaded_data == {
        "0": {"key1": ["value1", 1234], "key2": ["value2", None]},
        "1": {"key3": ["value3", None]},
        **{str(ix): {} for ix in range(2, DEFAULT_N_DBS)},
    }


def test_dump_databases_to_file_empty(tmp_path: Path) -> None:
    file_path = tmp_path / "dump_db.json"
    dump_databases_to_file(file_path)

    with file_path.open() as f:
        loaded_data = json.load(f)

    assert loaded_data == {}


def test_dump_databases_to_file_exception(
    mocker: MockerFixture, tmp_path: Path
) -> None:
    mocked_logger = mocker.patch("redis_clone.logger")
    file_path = tmp_path / "dump_db.json"
    mocker.patch("redis_clone.Path", side_effect=Exception("Something"))
    dump_databases_to_file(file_path)
    mocked_logger.exception.assert_called_once_with("Failed to dump databases")


def test_periodic_dump(mocker: MockerFixture, tmp_path: Path) -> None:
    mock_time = mocker.patch("redis_clone.time.sleep")
    mock_dump = mocker.patch("redis_clone.dump_databases_to_file")
    file_path = tmp_path / "dump.json"

    thread = threading.Thread(target=periodic_dump, args=(file_path, 0.01), daemon=True)
    thread.start()
    time.sleep(0.05)

    mock_time.assert_called()
    mock_dump.assert_called()


def test_cli_defaults(mocker: MockerFixture) -> None:
    mock_start_server = mocker.patch("redis_clone.start_server")
    cli([])
    mock_start_server.assert_called_once_with(
        DEFAULT_DUMP_FILE, DEFAULT_DUMP_INTERVAL, DEFAULT_PORT, DEFAULT_N_DBS
    )


def test_cli_custom_values(mocker: MockerFixture) -> None:
    mock_start_server = mocker.patch("redis_clone.start_server")
    cli(["-n", "4", "-i", "60", "-f", "test.json", "-p", "1234"])
    mock_start_server.assert_called_once_with(Path("test.json"), 60, 1234, 4)


def test_cli_help_message() -> None:
    """Test that the CLI displays the help message when requested."""
    with pytest.raises(SystemExit) as cm:
        cli(["-h"])
    assert cm.value.code == 0


def test_cli_invalid_n_dbs() -> None:
    """Test that the CLI errors on invalid number of databases."""
    with pytest.raises(SystemExit) as cm:
        cli(["-n", "0"])
    assert cm.value.code == 2


def test_cli_invalid_interval() -> None:
    """Test that the CLI errors on invalid dump interval."""
    with pytest.raises(SystemExit) as cm:
        cli(["-i", "0"])
    assert cm.value.code == 2


def test_cli_invalid_port() -> None:
    """Test that the CLI errors on invalid port number."""
    with pytest.raises(SystemExit) as cm:
        cli(["-p", "0"])
    assert cm.value.code == 2


def test_start_server_success(mocker: MockerFixture) -> None:
    mock_socket = mocker.patch("redis_clone.socket.create_server")
    mock_load_databases = mocker.patch("redis_clone.load_databases_from_file")
    mock_handle_connection = mocker.patch("redis_clone.handle_connection")

    mock_conn1 = mocker.mock_module.MagicMock()
    mock_conn2 = mocker.mock_module.MagicMock()
    mock_socket.return_value.accept.side_effect = [
        (mock_conn1, None),
        (mock_conn2, None),
        KeyboardInterrupt(),
    ]
    mock_conn1.getpeername.return_value = ["127.0.0.1", 1234]
    mock_conn2.getpeername.return_value = ["127.0.0.1", 5678]

    start_server(Path("test.json"), 1, 1234, 2)

    mock_socket.assert_called_once_with(("localhost", 1234), reuse_port=True)
    mock_load_databases.assert_called_once_with(Path("test.json"), 2)

    assert mock_handle_connection.call_count == 2


def test_start_server_socket_error(mocker: MockerFixture) -> None:
    mock_socket = mocker.patch(
        "redis_clone.socket.create_server", side_effect=OSError("Socket error")
    )
    mocked_logger = mocker.patch("redis_clone.logger")
    start_server(Path("test.json"), 1, 1234, 2)
    mock_socket.assert_called_once_with(("localhost", 1234), reuse_port=True)
    mocked_logger.critical.assert_called_once()


def test_start_server_keyboard_interrupt(mocker: MockerFixture) -> None:
    mock_socket = mocker.patch("redis_clone.socket.create_server")
    mocked_logger = mocker.patch("redis_clone.logger")
    mock_socket.side_effect = KeyboardInterrupt()
    start_server(Path("test.json"), 1, 1234, 2)
    mocked_logger.info.assert_called_once_with("Shutting down server")
