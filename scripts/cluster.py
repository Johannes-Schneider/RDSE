import argparse
import getpass
import logging
import time
from pathlib import Path
from typing import List

from validators.remote_path_validator import RemotePathValidator
from workers.deployment_worker import DeploymentWorker
from workers.execution_worker import ExecutionWorker
from workers.shared_worker_config import SharedWorkerConfig
from workers.shutdown_worker import ShutdownWorker


def main():
    logging.basicConfig(format="%(asctime)s : [%(process)s] %(levelname)s : %(message)s", level=logging.INFO)

    parser = _initialize_parser()
    args = parser.parse_args()

    if "action" not in args or not args.action:
        parser.print_usage()
        return

    password: str = getpass.getpass()

    if args.action == "deploy":
        _deploy(args, password)

    if args.action == "run":
        _run(args, password)

    if args.action == "shutdown":
        _shutdown(args, password)


def _initialize_parser():
    general_parser = argparse.ArgumentParser(description="Clustering trained entity embeddings")
    subparsers = general_parser.add_subparsers()

    _initialize_deploy_parser(subparsers)
    _initialize_run_parser(subparsers)
    _initialize_shutdown_parser(subparsers)

    return general_parser


def _initialize_deploy_parser(subparsers):
    deploy_parser = subparsers.add_parser("deploy")
    deploy_parser.set_defaults(action="deploy")

    _initialize_shared_arguments(deploy_parser)

    deploy_parser.add_argument(
        "--repository-url",
        type=str,
        help=f"Url of the repository to clone",
        required=True
    )
    deploy_parser.add_argument(
        "--build-command",
        type=str,
        help=f"Command, which is executed to build the project",
        required=False,
        default="mvn clean package"
    )
    deploy_parser.add_argument(
        "--corpus-file",
        type=Path,
        help="Path to corpus file",
        required=False
    )


def _initialize_run_parser(subparsers):
    run_parser = subparsers.add_parser("run")
    run_parser.set_defaults(action="run")

    _initialize_shared_arguments(run_parser)

    run_parser.add_argument(
        "--master-command",
        type=Path,
        help="Path to file containing master command",
        required=True
    )
    run_parser.add_argument(
        "--slave-command",
        type=Path,
        help="Path to file containing slave command",
        required=True
    )


def _initialize_shutdown_parser(subparsers):
    shutdown_parser = subparsers.add_parser("shutdown")
    shutdown_parser.set_defaults(action="shutdown")

    _initialize_shared_arguments(shutdown_parser)


def _initialize_shared_arguments(parser):
    parser.add_argument(
        "--username",
        type=str,
        help="User to use for auth on each machine",
        required=True
    )
    parser.add_argument(
        "--nodes",
        type=Path,
        help=f"Path to file containing IPs to all nodes",
        required=True
    )
    parser.add_argument(
        "--project-title",
        type=str,
        help="Project title",
        required=False,
        default="RDSE-johannes_julian"
    )
    parser.add_argument(
        "--home-dir",
        action=RemotePathValidator,
        type=str,
        help=f"Desired name of home dir",
        required=True)
    parser.add_argument(
        "--project-root-dir",
        type=str,
        help=f"Path of the project root dir, relative to the home dir",
        required=True)


def _deploy(args, password: str):
    is_master: bool = True
    for config in SharedWorkerConfig.create_from_args(args, password):
        worker: DeploymentWorker = DeploymentWorker(config=config, github_source=args.repository_url,
                                                    build_command=args.build_command,
                                                    corpus=args.corpus_file if is_master else None)
        is_master = False
        worker.start()


def _run(args, password: str):
    is_master: bool = True
    configs: List[SharedWorkerConfig] = list(SharedWorkerConfig.create_from_args(args, password))
    for config in configs:
        worker: DeploymentWorker = ExecutionWorker(config=config,
                                                   run_command=args.master_command if is_master else args.slave_command,
                                                   number_of_slaves=(len(configs) - 1),
                                                   master_ip=configs[0].node_ip)
        worker.start()
        if is_master:
            time.sleep(5)
        is_master = False


def _shutdown(args, password: str):
    for config in SharedWorkerConfig.create_from_args(args, password):
        worker: ShutdownWorker = ShutdownWorker(config)
        worker.start()


if __name__ == "__main__":
    main()
