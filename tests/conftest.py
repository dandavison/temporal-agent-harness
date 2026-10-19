from __future__ import annotations

import socket
import subprocess
import time

import pytest

from tests.utils import stream_env


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--stream-backend",
        choices=stream_env.BACKENDS,
        default="native",
        help="The temporalio.streams backend that agents' event streams use.",
    )


_redis: subprocess.Popen[bytes] | None = None


def pytest_configure(config: pytest.Config) -> None:
    global _redis
    stream_env.backend = config.getoption("--stream-backend")
    if stream_env.backend == "redis":
        port = _free_port()
        _redis = subprocess.Popen(
            ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL,
        )
        _wait_for_port(port)
        stream_env.redis_url = f"redis://127.0.0.1:{port}"


def pytest_unconfigure(config: pytest.Config) -> None:
    if _redis is not None:
        _redis.terminate()
        _redis.wait()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int) -> None:
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"redis-server did not start on port {port}")
