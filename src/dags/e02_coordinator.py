from airflow.sdk.coordinators.executable import ExecutableCoordinator
from airflow.sdk.coordinators.node import NodeCoordinator


class ContainerNodeCoordinator(NodeCoordinator):
    """Keep DAG discovery local and containerize Node task execution."""

    def _build_parse_dag_command(self, *, path):
        command, version = super()._build_parse_dag_command(path=path)
        return ["/usr/local/bin/node", *command[1:]], version


class ContainerExecutableCoordinator(ExecutableCoordinator):
    """Run the selected Go handler in the local pilot's workload image."""

    def _build_execute_task_command(self, *, what):
        command, version = super()._build_execute_task_command(what=what)
        return ["/usr/local/bin/pilot-sdk-runtime", *command], version
