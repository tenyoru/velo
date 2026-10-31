import asyncio
import contextlib
import io
import sys
import unittest
from unittest import mock

import run


class EchoTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_large_payload_half_close_and_empty_peer(self):
        for name in ("velo-epoll", "libuv", "tokio"):
            with self.subTest(implementation=name):
                command = [arg.format(connections=2) for arg in run.COMMANDS[name]]
                process, ready = await run.start_server(command, 5)
                writers = []
                try:
                    async with asyncio.timeout(5):
                        reader, writer = await asyncio.open_connection("127.0.0.1", ready["port"])
                        writers.append(writer)
                        _, empty = await asyncio.open_connection("127.0.0.1", ready["port"])
                        writers.append(empty)
                        empty.close()
                        await empty.wait_closed()
                        payload = bytes(range(256)) * 257
                        writer.write(payload[:3])
                        await writer.drain()
                        self.assertEqual(await reader.readexactly(3), payload[:3])
                        writer.write(payload[3:])
                        await writer.drain()
                        writer.write_eof()
                        self.assertEqual(await reader.readexactly(len(payload) - 3), payload[3:])
                        self.assertEqual(await reader.read(), b"")
                        stdout, stderr = await process.communicate()
                        self.assertEqual((process.returncode, stdout, stderr), (0, b"", b""))
                finally:
                    for writer in writers:
                        writer.close()
                        await writer.wait_closed()
                    await run.stop_server(process)

    async def test_measured_counts_and_drain(self):
        config = run.options(["--connections", "3", "--iterations", "11", "--warmup", "2"])
        for name in ("velo-epoll", "libuv", "tokio"):
            with self.subTest(implementation=name):
                result = await run.measure(name, config)
                self.assertEqual(result["requests"], 33)
                self.assertEqual(result["server"]["backend"], "epoll")
                self.assertGreater(result["p50_ns"], 0)
                self.assertGreaterEqual(result["p99_ns"], result["p50_ns"])
                self.assertGreater(result["server_peak_rss_bytes"], 0)
                self.assertTrue(result["validated"])

    async def test_startup_timeout_kills_and_reaps_child(self):
        children = []
        spawn = asyncio.create_subprocess_exec

        async def record(*args, **kwargs):
            child = await spawn(*args, **kwargs)
            children.append(child)
            return child

        with mock.patch.object(asyncio, "create_subprocess_exec", record):
            with self.assertRaises(TimeoutError):
                await run.start_server([sys.executable, "-c", "import time; time.sleep(30)"], 0.1)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)

    async def test_corruption_fails_and_reaps_child(self):
        script = """
import json, socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
s.listen()
print(json.dumps({'port': s.getsockname()[1], 'backend': 'fake', 'version': 'test'}), flush=True)
c, _ = s.accept()
while data := c.recv(4096):
    c.sendall(bytes(value ^ 255 for value in data))
"""
        config = run.options(["--connections", "1", "--iterations", "1", "--warmup", "1"])
        children = []
        start = run.start_server

        async def record(*args, **kwargs):
            child, ready = await start(*args, **kwargs)
            children.append(child)
            return child, ready

        with mock.patch.dict(run.COMMANDS, fake=[sys.executable, "-c", script]):
            with mock.patch.object(run, "start_server", record):
                with self.assertRaises(ExceptionGroup) as failure:
                    await run.measure("fake", config)
        self.assertIn("incorrect echo payload", str(failure.exception.exceptions))
        self.assertIsNotNone(children[0].returncode)

    async def test_workload_timeout_kills_and_reaps_child(self):
        script = """
import json, socket, time
s = socket.socket()
s.bind(('127.0.0.1', 0))
s.listen()
print(json.dumps({'port': s.getsockname()[1], 'backend': 'fake', 'version': 'test'}), flush=True)
c, _ = s.accept()
data = c.recv(4096)
c.sendall(data)
time.sleep(30)
"""
        config = run.options(["--connections", "1", "--iterations", "1", "--warmup", "1"])
        children = []
        start = run.start_server

        async def record(*args, **kwargs):
            child, ready = await start(*args, **kwargs)
            children.append(child)
            config.timeout = 0.1
            return child, ready

        with mock.patch.dict(run.COMMANDS, fake=[sys.executable, "-c", script]):
            with mock.patch.object(run, "start_server", record):
                with self.assertRaises(TimeoutError):
                    await run.measure("fake", config)
        self.assertIsNotNone(children[0].returncode)

    def test_invalid_workload_is_rejected(self):
        for args in (["--connections", "0"], ["--size", "0"], ["--timeout", "nan"],
                     ["--connections", "10000", "--iterations", "10000000"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    run.options(args)


if __name__ == "__main__":
    run.build()
    unittest.main()
