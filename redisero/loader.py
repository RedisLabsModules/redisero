import os
import zipfile

import requests
import yaml
from rich.console import Console

from . import utils
from .schemas import Module, StateDir

console = Console()
MODULE_PACKAGE_DEFAULT_NAME = "module.zip"
NPM_METADATA_FILE = "modules.json"


class ModuleLoader:
    def __init__(self, cfg_path: str, state_dir_path: str) -> None:
        # todo check if files exists
        self.cfg_path = cfg_path
        self.state_dir_path = state_dir_path
        self.modules = []

    def load_config(self) -> None:
        """Load Redis modules config file"""
        with open(self.cfg_path, "r") as stream:
            try:
                for module in yaml.safe_load(stream):
                    module = Module(**module)
                    console.print(
                        f"Module: <[cyan]{module}[/cyan]> loaded from config file"
                    )
                    self.modules.append(module)

            except yaml.YAMLError as exc:
                print(exc)

    def download_module_packages(self) -> None:
        """Download Redis modules npm packages"""
        for module in self.modules:
            console.print(f"Downloading npm package: <[cyan]{module.name}[/cyan]>")
            utils.run_npm(
                self.state_dir_path,
                "install",
                f"--prefix {self.state_dir_path}",
                module.name,
            )

    def extract_modules(self) -> None:
        package_path = (
            f"{self.state_dir_path}/{StateDir.MOD.value}/{MODULE_PACKAGE_DEFAULT_NAME}"
        )
        """Download Redis modules based on npm package metadata"""
        for module_data in utils.find_module_json(
            f"{self.state_dir_path}/node_modules", NPM_METADATA_FILE
        ):
            # todo network error handling
            # download archive from s3
            console.print(
                f"Downloading [cyan]{module_data['name']}[/cyan] module from blob storate"
            )
            response = requests.get(module_data["module_path"])
            with open(
                package_path,
                "wb",
            ) as f:
                f.write(response.content)

            # extract module .so file from archive
            console.print(f"Extracting [cyan]{module_data['name']}[/cyan] module")
            with zipfile.ZipFile(
                package_path,
                "r",
            ) as zip_ref:
                zip_ref.extract(
                    module_data["name"],
                    path=f"{self.state_dir_path}/{StateDir.MOD.value}/",
                )

            # remove archive
            os.remove(package_path)
