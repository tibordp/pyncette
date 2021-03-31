import datetime
import os
import signal
import subprocess
import time


def test_signal_handling():
    os.environ["LOG_LEVEL"] = "DEBUG"

    with subprocess.Popen(
        ["coverage", "run", "-m", "tests.test_pyncette_process"]
    ) as proc:
        time.sleep(2)
        proc.send_signal(signal.SIGTERM)
        ret_code = proc.wait()

    assert ret_code == 0


def test_signal_handling_force():
    os.environ["LOG_LEVEL"] = "DEBUG"

    with subprocess.Popen(
        ["coverage", "run", "-m", "tests.test_pyncette_process"]
    ) as proc:
        time.sleep(2)
        proc.send_signal(signal.SIGTERM)
        time.sleep(1)
        proc.send_signal(signal.SIGTERM)
        ret_code = proc.wait()

    assert ret_code != 0


if __name__ == "__main__":
    import asyncio

    from pyncette import Context
    from pyncette import Pyncette

    app = Pyncette()

    @app.task(interval=datetime.timedelta(seconds=1))
    async def foo(context: Context):
        await asyncio.sleep(3)

    app.main()
