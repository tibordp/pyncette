import datetime
import os
import signal
import subprocess
import time

import pytest


@pytest.mark.integration
def test_signal_handling():
    with subprocess.Popen(
        ["coverage", "run", "-m", "tests.test_pyncette_process"],  # noqa: S607
        env={**os.environ, "LOG_LEVEL": "DEBUG"},
    ) as proc:
        time.sleep(2)
        proc.send_signal(signal.SIGINT)
        ret_code = proc.wait()

    assert ret_code == 0


@pytest.mark.integration
def test_signal_handling_uvloop():
    with subprocess.Popen(
        ["coverage", "run", "-m", "tests.test_pyncette_process"],  # noqa: S607
        env={**os.environ, "LOG_LEVEL": "DEBUG", "USE_UVLOOP": "1"},
    ) as proc:
        time.sleep(2)
        proc.send_signal(signal.SIGINT)
        ret_code = proc.wait()

    assert ret_code == 0


@pytest.mark.integration
def test_signal_handling_force():
    with subprocess.Popen(
        ["coverage", "run", "-m", "tests.test_pyncette_process"],  # noqa: S607
        env={**os.environ, "LOG_LEVEL": "DEBUG"},
    ) as proc:
        time.sleep(2)
        proc.send_signal(signal.SIGINT)
        time.sleep(1)
        proc.send_signal(signal.SIGINT)
        ret_code = proc.wait()

    assert ret_code != 0


if __name__ == "__main__":
    import asyncio

    from pyncette import Context
    from pyncette import Pyncette

    app = Pyncette()

    @app.task(interval=datetime.timedelta(seconds=1))
    async def foo(context: Context):
        await asyncio.sleep(4)

    app.main()
