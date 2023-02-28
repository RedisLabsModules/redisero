import typing
from enum import Enum

import pydantic

import redisero.os_platform


class Defaults:
    module = None
    module_args = None

    env = "oss"
    binary = "redis-server"
    proxy_binary = None
    re_binary = None
    re_libdir = None
    decode_responses = False
    use_aof = False
    use_rdb_preamble = True
    debugger = None
    sanitizer = None
    debug_print = False
    debug_pause = False
    no_capture_output = False
    no_log = False
    exit_on_failure = False
    logdir = None
    use_slaves = False
    num_shards = 1
    external_addr = "localhost:6379"
    use_unix = False
    randomize_ports = False
    oss_password = None
    cluster_node_timeout = None
    curr_test_name = None
    port = 6379
    enable_debug_command = False

    def getKwargs(self):
        kwargs = {
            "modulePath": self.module,
            "moduleArgs": self.module_args,
            "useSlaves": self.use_slaves,
            "useAof": self.use_aof,
            "useRdbPreamble": self.use_rdb_preamble,
            "dbDirPath": self.logdir,
            "debugger": self.debugger,
            "sanitizer": self.sanitizer,
            "noCatch": self.no_capture_output,
            "noLog": self.no_log,
            "password": self.oss_password,
        }
        return kwargs


# State dir structure
class StateDir(Enum):
    BIN = "bin"
    CFG = "cfg"
    MOD = "mod"
    LOG = "log"
    RDB = "rdb"
    RUN = "run"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


# Module model
class Module(pydantic.BaseModel):
    name: str
    version: typing.Optional[str] = None
    platform: typing.Optional[str] = None

    @pydantic.validator("platform", pre=True, always=True)
    def default_platform(cls, v):
        if not v:
            platform = redisero.os_platform.Platform()
            return f"{platform.os}-{platform.dist}-{platform.os_ver}-{platform.arch}"
        return v
