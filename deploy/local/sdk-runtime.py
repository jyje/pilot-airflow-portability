#!/usr/bin/env python
"""Run the E02 SDK workloads with their original Airflow coordinator protocol."""

import asyncio
import json
import os
import signal
import socket
import sys
from contextlib import closing
from functools import partial
from pathlib import Path

NODE = "/usr/local/bin/node"
JAVA = "/opt/java/openjdk/bin/java"


def configure() -> None:
    import yaml

    coordinators = json.loads(os.environ.get("AIRFLOW__SDK__COORDINATORS", "{}"))
    if "ts" in coordinators:
        coordinators["ts"]["classpath"] = "e02_coordinator.ContainerNodeCoordinator"
        kwargs = coordinators["ts"].setdefault("kwargs", {})
        kwargs.update(node_executable="/usr/local/bin/pilot-sdk-node", task_startup_timeout=45)
    if "java" in coordinators:
        kwargs = coordinators["java"].setdefault("kwargs", {})
        kwargs.update(java_executable="/usr/local/bin/pilot-sdk-java", task_startup_timeout=45)
    if "go" in coordinators:
        coordinators["go"]["classpath"] = "e02_coordinator.ContainerExecutableCoordinator"
        coordinators["go"].setdefault("kwargs", {})["task_startup_timeout"] = 45

    chain = Path(os.environ["AIRFLOW__CORE__DAGS_FOLDER"]) / "chain.yaml"
    if chain.exists():
        definition = yaml.safe_load(chain.read_text())
        for task in definition["tasks"]:
            if task.get("uses") != "airflow.providers.standard.operators.bash.BashOperator":
                continue
            task["uses"] = "airflow.providers.docker.operators.docker.DockerOperator"
            bash = task["with"]
            task["with"] = {
                "image": os.environ["PILOT_DOCKER_TASK_IMAGE"],
                "command": ["bash", "-ec", bash["bash_command"]],
                "environment": bash.get("env", {}),
                "docker_url": "unix:///var/run/docker.sock",
                "mount_tmp_dir": False,
                "auto_remove": "force",
                "labels": {"org.jyje.airflow.chain-task": task["id"]},
            }
        chain.write_text(yaml.safe_dump(definition, sort_keys=False))
    print(json.dumps(coordinators))


async def copy_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while data := await reader.read(65536):
        writer.write(data)
        await writer.drain()
    writer.write_eof()


async def relay(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, host: str, port: int
) -> None:
    upstream_writer = None
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
        async with asyncio.TaskGroup() as streams:
            streams.create_task(copy_stream(reader, upstream_writer))
            streams.create_task(copy_stream(upstream_reader, writer))
    finally:
        if upstream_writer is not None:
            upstream_writer.close()
        writer.close()


async def run_workload(client, executable: str, arguments: list[str], runtime: str) -> int:
    # The coordinator checks which process owns each connection. Keep the supervisor
    # connections in this launcher and forward them over separate loopback sockets.
    parent = client.containers.get(socket.gethostname())
    servers = []
    try:
        for index, argument in enumerate(arguments):
            name, separator, address = argument.partition("=")
            if separator and name in {"--comm", "--logs"}:
                host, _, port = address.rpartition(":")
                server = await asyncio.start_server(
                    partial(relay, host=host, port=int(port)), "127.0.0.1", 0
                )
                servers.append(server)
                arguments[index] = f"{name}=127.0.0.1:{server.sockets[0].getsockname()[1]}"
        workload = client.containers.run(
            parent.image.id,
            arguments,
            detach=True,
            entrypoint=[executable],
            network_mode=f"container:{parent.id}",
            labels={
                "org.jyje.airflow.sdk-runtime": runtime,
                "org.jyje.airflow.sdk-parent": parent.id,
            },
        )
        try:
            status = await asyncio.to_thread(workload.wait)
            if status["StatusCode"]:
                sys.stderr.buffer.write(workload.logs(tail=20))
            return int(status["StatusCode"])
        finally:
            workload.remove(force=True)
    finally:
        for server in servers:
            server.close()
            await server.wait_closed()


def launch(executable: str, arguments: list[str], runtime: str) -> int:
    if not any(arg.startswith("--comm=") for arg in arguments):
        os.execv(executable, [executable, *arguments])

    import docker

    signal.signal(signal.SIGTERM, lambda _number, _frame: sys.exit(143))
    with closing(docker.from_env()) as client:
        return asyncio.run(run_workload(client, executable, arguments, runtime))


if __name__ == "__main__":
    match Path(sys.argv[0]).name, sys.argv[1:]:
        case "pilot-sdk-node", arguments:
            sys.exit(launch(NODE, arguments, "node"))
        case "pilot-sdk-java", arguments:
            sys.exit(launch(JAVA, arguments, "java"))
        case _, ["--configure"]:
            configure()
        case _, [executable, *arguments]:
            sys.exit(launch(executable, arguments, "go"))
        case _:
            sys.exit("Missing SDK executable")
