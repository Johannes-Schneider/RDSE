import logging
from multiprocessing import Process

from workers.shared_worker_config import SharedWorkerConfig
from workers.ssh_command_executor import SSHCommandExecutor


class ShutdownWorker(Process):

    def __init__(self, config: SharedWorkerConfig):
        super(ShutdownWorker, self).__init__()
        self._configuration: SharedWorkerConfig = config

    def __call__(self, *args, **kwargs) -> None:
        self.run()

    def run(self) -> None:
        ssh_executor: SSHCommandExecutor = SSHCommandExecutor(self._configuration.node_ip,
                                                              self._configuration.username,
                                                              self._configuration.password)

        self._kill_screen(ssh_executor)
        ssh_executor.close()

        logging.info(f"[{self._configuration.node_ip}] SUCCESS")

    def _kill_screen(self, ssh_session: SSHCommandExecutor) -> None:
        logging.info(f"[{self._configuration.node_ip}] KILL SCREEN")
        ssh_session.execute(f"screen -X -S {self._configuration.project_title} quit")
