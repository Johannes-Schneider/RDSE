import logging
from multiprocessing import Process
from pathlib import Path

from workers.shared_worker_config import SharedWorkerConfig
from workers.ssh_command_executor import SSHCommandExecutor


class ExecutionWorker(Process):
    COMMAND_IP_TEMPLATE_PATTERN = "{NODE-IP}"
    COMMAND_MASTER_IP_TEMPLATE_PATTERN = "{MASTER-NODE-IP}"
    COMMAND_NUMBER_OF_SLAVES_TEMPLATE_PATTERN = "{NUMBER-OF-SLAVE-NODES}"

    def __init__(self, config: SharedWorkerConfig, run_command: Path, master_ip: str, number_of_slaves: int):
        super(ExecutionWorker, self).__init__()
        self._configuration: SharedWorkerConfig = config
        self._run_command: str = self._prepare_run_command(run_command, master_ip, number_of_slaves)

    def _prepare_run_command(self, run_command_template_file: Path, master_ip: str, number_of_slaves: int) -> str:
        with run_command_template_file.open("r") as input_stream:
            run_command_template = " ".join(input_stream.readlines()).replace("\n", "")
        return run_command_template \
            .replace(ExecutionWorker.COMMAND_IP_TEMPLATE_PATTERN, self._configuration.node_ip) \
            .replace(ExecutionWorker.COMMAND_MASTER_IP_TEMPLATE_PATTERN, master_ip) \
            .replace(ExecutionWorker.COMMAND_NUMBER_OF_SLAVES_TEMPLATE_PATTERN, str(number_of_slaves))

    def __call__(self, *args, **kwargs) -> None:
        self.run()

    def run(self) -> None:
        ssh_executor: SSHCommandExecutor = SSHCommandExecutor(self._configuration.node_ip,
                                                              self._configuration.username,
                                                              self._configuration.password)
        self._execute(ssh_executor)
        ssh_executor.close()

        logging.info(f"[{self._configuration.node_ip}] SUCCESS")

    def _execute(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] EXECUTE RUN COMMAND")
        ssh_session.execute(f"cd {self._configuration.home_dir}/{self._configuration.project_root_dir}")
        ssh_session.execute(f"screen -d -m -S {self._configuration.project_title} bash -c '{self._run_command}'")
