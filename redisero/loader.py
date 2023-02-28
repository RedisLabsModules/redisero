import json
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
        if not os.path.exists(self.cfg_path):
            return

        with open(self.cfg_path, "r") as stream:
            try:
                for module in yaml.safe_load(stream):
                    module = Module(**module)
                    console.print(
                        f"Module: <[cyan]{module}[/cyan]> loaded from config file"
                    )
                    self.modules.append(module)

            except Exception as e:
                print("Redis modules config file not loaded")

    def download_module_packages(self) -> None:
        """Download Redis modules npm packages"""
        for module in self.modules:
            console.print(f"Downloading npm package: <[cyan]{module.name}[/cyan]>")
            utils.run_npm(
                self.state_dir_path,
                "install",
                f"--prefix {self.state_dir_path}",
                f"@{module.name}",
            )

    def extract_modules(self) -> None:
        """Download Redis modules based on npm package metadata"""

        package_path = (
            f"{self.state_dir_path}/{StateDir.MOD.value}/{MODULE_PACKAGE_DEFAULT_NAME}"
        )
        for module in self.modules:
            if "/" in module.name:
                package_name = module.name.split("/")[-1]

            # locate npm package folder based on config file
            package_folder = utils.find_folder(
                package_name, f"{self.state_dir_path}/node_modules"
            )

            with open(f"{package_folder}/{NPM_METADATA_FILE}") as f:
                module_data = json.load(f)

                target_platform = None
                module_platforms = module_data["platform"]
                console.print("Available versions:")
                for platform in module_platforms:
                    print(platform)
                    if module.platform.lower() == platform.lower():
                        target_platform = platform

                if not target_platform:
                    continue

                target_module = module_data["platform"][target_platform]
                # download module by s3 link
                console.print(f"Downloading [cyan]{target_platform}[/cyan] version.")
                response = requests.get(target_module["path"])
                with open(
                    package_path,
                    "wb",
                ) as f:
                    f.write(response.content)

                # extract module .so file from archive
                console.print(f"Extracting [cyan]{target_module['name']}[/cyan] file")
                with zipfile.ZipFile(
                    package_path,
                    "r",
                ) as zip_ref:
                    zip_ref.extract(
                        target_module["name"], 
                        path=f"{self.state_dir_path}/{StateDir.MOD.value}/{module.platform.lower()}",
                    )

                # remove tmp archive
                os.remove(package_path)

                # make modules file executable
                os.chmod(
                    f"{self.state_dir_path}/{StateDir.MOD.value}/{module.platform.lower()}/{target_module['name']}",
                    0o777,
                )
