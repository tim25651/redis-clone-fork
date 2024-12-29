"""Fixtures for the redis-clone tests."""

from __future__ import annotations

from copy import copy
from typing import TYPE_CHECKING

import pytest

from redis_clone import DEFAULT_N_DBS, RESPEncoder, SocketProto, databases

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from unittest.mock import MagicMock

    from pytest_mock import MockerFixture


@pytest.fixture
def get_mock_socket(mocker: MockerFixture) -> Callable[..., MagicMock]:
    def _get_mock_socket(*args: str | int | None | bytes | list[str]) -> MagicMock:
        mock_socket = mocker.mock_module.MagicMock(spec=SocketProto)
        side_effects: list[bytes] = []
        for arg in args:
            if isinstance(arg, bytes):
                side_effects.append(arg)
            elif isinstance(arg, list):
                bulk = [RESPEncoder.encode_bulk_string(x.encode()) for x in arg]
                side_effects.append(RESPEncoder.encode_array(bulk))
            elif isinstance(arg, str):
                side_effects.append(RESPEncoder.encode_bulk_string(arg.encode()))
            elif isinstance(arg, int):
                side_effects.append(RESPEncoder.encode_int(arg))
            elif arg is None:
                side_effects.append(RESPEncoder.encode_nil())
            else:
                raise ValueError("unsupported")
        mock_socket.recv.side_effect = side_effects
        return mock_socket  # type: ignore[no-any-return]

    return _get_mock_socket


@pytest.fixture
def unused_reset_databases() -> Generator[None]:
    """Fixture to reset databases to previous state."""
    prev_databases = copy(databases)
    yield
    for ix in list(databases):
        if ix in prev_databases:
            databases[ix] = prev_databases[ix]
        else:
            databases.pop(ix)


@pytest.fixture
def unused_clear_databases(unused_reset_databases: None) -> None:
    """Fixture to get empty databases."""
    for ix in range(DEFAULT_N_DBS):
        databases[ix] = {}
