import logging

from pexpect import pxssh


class SSHCommandExecutor:

    def __init__(self, hostname: str, username: str, password: str):
        self._hostname: str = hostname
        self._ssh_session: pxssh.pxssh = pxssh.pxssh()
        if not self._ssh_session.login(hostname, username, password):
            raise Exception(f"[{hostname}] FAILED SSH CONNECTION")

    def execute(self, line: str):
        self._ssh_session.sendline(line)
        self._ssh_session.prompt()
        logging.debug(f"[{self._hostname}] Executing: {line}\n{str(self._ssh_session.before)}")

    def close(self):
        self._ssh_session.close()
