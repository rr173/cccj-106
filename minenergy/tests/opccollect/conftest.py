"""共享 pytest 夹具：真实 OPC UA 服务端 + 临时历史库。

辅助构造函数在 :mod:`minenergy.opccollect.testing`，夹具只保留 pytest
fixture 与 tmp_path 绑定。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio  # type: ignore

from minenergy.opccollect.server_fixture import FixtureServer
from minenergy.opccollect.testing import free_port


@pytest_asyncio.fixture
async def fixture_server() -> AsyncIterator[FixtureServer]:
    server = FixtureServer(port=free_port())
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "opc_history.sqlite"
