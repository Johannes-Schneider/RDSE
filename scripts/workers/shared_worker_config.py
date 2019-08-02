from typing import Iterable


class SharedWorkerConfig:

    @staticmethod
    def create_from_args(args, password: str) -> Iterable["SharedWorkerConfig"]:
        with args.nodes.open("r") as input_stream:
            for ip_address in input_stream:
                if not ip_address:
                    continue
                ip_address = ip_address.replace("\n", "")

                yield SharedWorkerConfig(ip_address,
                                         args.username,
                                         password,
                                         args.home_dir,
                                         args.project_root_dir,
                                         args.project_title)

    def __init__(self,
                 node_ip: str,
                 username: str,
                 password: str,
                 home_dir: str,
                 project_root_dir: str,
                 project_title: str):
        self._node_ip: str = node_ip
        self._username: str = username
        self._password: str = password
        self._home_dir: str = home_dir
        self._project_root_dir: str = project_root_dir
        self._project_title: str = project_title

    @property
    def node_ip(self) -> str:
        return self._node_ip

    @property
    def username(self) -> str:
        return self._username

    @property
    def password(self) -> str:
        return self._password

    @property
    def home_dir(self) -> str:
        return self._home_dir

    @property
    def project_root_dir(self) -> str:
        return self._project_root_dir

    @property
    def project_title(self) -> str:
        return self._project_title
