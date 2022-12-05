import os
import pickle
from functools import partial
from typing import Optional

import typer
from rich.console import Console

from redisero import __app_name__, __version__, cluster, schemas

app = typer.Typer()
console = Console()

REDIS_BINARY = os.environ.get("REDIS_BINARY", "redis-server")
RUN_STATE = "/remstate"
ROOT_DIR = os.path.abspath(os.getcwd()) + RUN_STATE
INIT_FOLDERS = ("bin", "cfg", "mod", "log", "rdb", "run")


@app.command()
def init():
    concat_root_path = partial(os.path.join, ROOT_DIR)
    make_directory = partial(os.makedirs, exist_ok=True)

    for path_items in map(concat_root_path, INIT_FOLDERS):
        make_directory(path_items)


@app.command()
def start(
    shards: int = typer.Option(1, help="Number of shards"),
    with_replicas: bool = typer.Option(0, help="Use slaves"),
):

    modules_dir = ROOT_DIR + "/mod/"
    default_args = schemas.Defaults().getKwargs()
    default_args["useSlaves"] = with_replicas
    if redis_modules := os.listdir(modules_dir):
        default_args["modulePath"] = [
            modules_dir + redis_module for redis_module in redis_modules
        ]

    cluster_env = cluster.ClusterEnv(
        remstate=ROOT_DIR,
        shardsCount=shards,
        redisBinaryPath=REDIS_BINARY,
        outputFilesFormat="%s-test",
        randomizePorts=schemas.Defaults.randomize_ports,
        **default_args,
    )
    cluster_env.startEnv()

    with open(f"{ROOT_DIR}/run/cluster_env.pickle", "wb") as pfile:
        pickle.dump(cluster_env, pfile, protocol=pickle.HIGHEST_PROTOCOL)


@app.command()
def stop():
    root_directory = os.path.abspath(os.getcwd()) + RUN_STATE
    with open(f"{root_directory}/run/cluster_env.pickle", "rb") as handle:
        cluster_env = pickle.load(handle)
    cluster_env.stopEnv()
    os.remove(f"{root_directory}/run/cluster_env.pickle")


@app.command()
def info():
    root_directory = os.path.abspath(os.getcwd()) + RUN_STATE
    with open(f"{root_directory}/run/cluster_env.pickle", "rb") as handle:
        cluster_env = pickle.load(handle)
    cluster_env.printEnvData()


@app.command()
def cli(sh, cmd):
    import subprocess

    root_directory = os.path.abspath(os.getcwd()) + RUN_STATE
    with open(f"{root_directory}/run/cluster_env.pickle", "rb") as handle:
        cluster_env = pickle.load(handle)
    for shard in cluster_env.shards:
        if str(shard.masterServerId) == str(sh):
            command = ["redis-cli", "-c", "-p", str(shard.port), cmd]
            command_output = subprocess.Popen(
                command, stdout=subprocess.PIPE
            ).communicate()[0]
            print(command_output)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return
