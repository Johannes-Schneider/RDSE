import logging
import os
from multiprocessing import Process
from pathlib import Path
from typing import Optional

from workers.shared_worker_config import SharedWorkerConfig
from workers.ssh_command_executor import SSHCommandExecutor


class DeploymentWorker(Process):

    def __init__(self, config: SharedWorkerConfig, github_source: str, build_command: str,
                 corpus: Optional[Path] = None):
        super(DeploymentWorker, self).__init__()
        self._configuration: SharedWorkerConfig = config
        self._github_source: str = github_source
        self._build_command: str = build_command
        self._corpus: Path = corpus

    def __call__(self, *args, **kwargs) -> None:
        self.run()

    def run(self) -> None:
        ssh_executor: SSHCommandExecutor = SSHCommandExecutor(self._configuration.node_ip,
                                                              self._configuration.username,
                                                              self._configuration.password)
        self._clean_up(ssh_executor)
        self._create_home_dir(ssh_executor)
        self._checkout_project(ssh_executor)
        self._build_project(ssh_executor)
        self._transfer_corpus()

        ssh_executor.close()

        logging.info(f"[{self._configuration.node_ip}] SUCCESS")

    def _clean_up(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] CLEAN UP")
        ssh_session.execute(f"rm -rf {self._configuration.home_dir}")

    def _create_home_dir(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] CREATE HOME DIR")
        ssh_session.execute(f"mkdir {self._configuration.home_dir}")
        ssh_session.execute(f"cd {self._configuration.home_dir}")

    def _checkout_project(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] CHECKOUT")
        ssh_session.execute(f"git clone {self._github_source}")
        ssh_session.execute(f"cd {self._configuration.project_root_dir}")

    def _build_project(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] BUILD")
        ssh_session.execute(self._build_command)

    def _transfer_corpus(self):
        if not self._corpus:
            return

        logging.info(f"[{self._configuration.node_ip}] TRANSFER CORPUS")
        os.system(
            f"sshpass -p '{self._configuration.password}' scp '{self._corpus.absolute()}' {self._configuration.username}@{self._configuration.node_ip}:{self._configuration.home_dir}/{self._configuration.project_root_dir}/corpus.txt")
