import os
import subprocess
import sys
import time
import uuid

import psutil
import redis
from rich.console import Console

from redisero.utils import (fix_modules, fix_modulesArgs, get_random_port,
                            wait_for_conn)

MASTER = "master"
SLAVE = "slave"
console = Console()


class StandardEnv(object):
    def __init__(
        self,
        redisBinaryPath,
        port=6379,
        remstate=None,
        modulePath=None,
        moduleArgs=None,
        outputFilesFormat=None,
        dbDirPath=None,
        useSlaves=False,
        serverId=1,
        password=None,
        libPath=None,
        clusterEnabled=False,
        decodeResponses=False,
        useAof=False,
        useRdbPreamble=True,
        debugger=None,
        sanitizer=None,
        noCatch=False,
        noLog=False,
        unix=False,
        verbose=False,
        clusterNodeTimeout=None,
        tlsPassphrase=None,
        enableDebugCommand=False,
    ):
        self.uuid = uuid.uuid4().hex
        self.redisBinaryPath = (
            os.path.expanduser(redisBinaryPath)
            if redisBinaryPath.startswith("~/")
            else redisBinaryPath
        )
        self.remstate = remstate
        self.modulePath = fix_modules(modulePath)
        self.moduleArgs = fix_modulesArgs(self.modulePath, moduleArgs, haveSeqs=False)
        self.outputFilesFormat = self.uuid + "." + outputFilesFormat
        self.useSlaves = useSlaves
        self.masterServerId = serverId
        self.password = password
        self.clusterEnabled = clusterEnabled
        self.decodeResponses = decodeResponses
        self.useAof = useAof
        self.useRdbPreamble = useRdbPreamble
        self.envIsUp = False
        self.debugger = debugger
        self.sanitizer = sanitizer
        self.noCatch = noCatch
        self.noLog = noLog
        self.environ = os.environ.copy()
        self.useUnix = unix
        self.dbDirPath = dbDirPath or self.remstate + "/rdb"
        self.masterProcess = None
        self.masterExitCode = None
        self.slaveProcess = None
        self.slaveExitCode = None
        self.verbose = verbose
        self.role = MASTER
        self.clusterNodeTimeout = clusterNodeTimeout
        self.tlsPassphrase = tlsPassphrase
        self.enableDebugCommand = enableDebugCommand
        self.terminateRetries = None
        self.terminateRetrySecs = None

        if port > 0:
            self.port = port
            self.slavePort = port + 1 if self.useSlaves else 0
        elif port == 0:
            self.port = get_random_port()
            self.slavePort = get_random_port() if self.useSlaves else 0
        else:
            self.port = -1
            self.slavePort = -1

        if self.useUnix:
            if self.clusterEnabled:
                raise ValueError("Unix sockets cannot be used with cluster mode")
            self.port = -1

        if self.has_interactive_debugger and serverId > 1:
            assert self.noCatch and not self.useSlaves and not self.clusterEnabled

        if libPath:
            self.libPath = (
                os.path.expanduser(libPath) if libPath.startswith("~/") else libPath
            )
        else:
            self.libPath = None
        if self.libPath:
            if "LD_LIBRARY_PATH" in self.environ.keys():
                self.environ["LD_LIBRARY_PATH"] = (
                    self.libPath + ":" + self.environ["LD_LIBRARY_PATH"]
                )
            else:
                self.environ["LD_LIBRARY_PATH"] = self.libPath

        self.masterCmdArgs = self.createCmdArgs(MASTER)
        self.masterOSEnv = self.createCmdOSEnv(MASTER)
        if self.useSlaves:
            self.slaveServerId = serverId + 1
            self.slaveCmdArgs = self.createCmdArgs(SLAVE)
            self.slaveOSEnv = self.createCmdOSEnv(SLAVE)

        self.envIsHealthy = True

    def _getFileName(self, role, suffix):
        return (self.outputFilesFormat + suffix) % (
            "master-%d" % self.masterServerId
            if role == MASTER
            else "slave-%d" % self.slaveServerId
        )

    def _getValgrindFilePath(self, role):
        return os.path.join(self.dbDirPath, self._getFileName(role, ".valgrind.log"))

    def getMasterPort(self):
        return self.port

    def getPassword(self):
        return self.password

    def getUnixPath(self, role):
        basename = "{}-{}.sock".format(self.uuid, role)
        return os.path.abspath(os.path.join(self.dbDirPath, basename))

    @property
    def has_interactive_debugger(self):
        return self.debugger and self.debugger.is_interactive

    def _getRedisVersion(self):
        options = {
            "stderr": subprocess.PIPE,
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
        }
        p = subprocess.Popen(args=[self.redisBinaryPath, "--version"], **options)
        while p.poll() is None:
            time.sleep(0.1)
        exit_code = p.poll()
        if exit_code != 0:
            raise Exception("Could not extract Redis version")
        out, err = p.communicate()
        out = out.decode("utf-8")
        v = out[out.find("v=") + 2 : out.find("sha=") - 1].split(".")
        return int(v[0]) * 10000 + int(v[1]) * 100 + int(v[2])

    def createCmdArgs(self, role):
        cmdArgs = []
        if self.debugger:
            cmdArgs += self.debugger.generate_command(
                self._getValgrindFilePath(role) if not self.noCatch else None
            )

        cmdArgs += [self.redisBinaryPath]

        if self.port > -1:
            cmdArgs += ["--port", str(self.getPort(role))]
        else:
            cmdArgs += ["--port", str(0), "--unixsocket", self.getUnixPath(role)]

        if self.modulePath:
            if self.moduleArgs and len(self.modulePath) != len(self.moduleArgs):
                console.print(
                    "[red]Number of module args sets in Env does not match number of modules[/red]"
                )
                sys.exit(1)
            for pos, module in enumerate(self.modulePath):
                cmdArgs += ["--loadmodule", module]
                if self.moduleArgs:
                    module_args = self.moduleArgs[pos]
                    if module_args:
                        # make sure there are no spaces within args
                        args = []
                        for arg in module_args:
                            if arg.strip() != "":
                                args += arg.split(" ")
                        cmdArgs += args

        if self.dbDirPath is not None:
            cmdArgs += ["--dir", self.dbDirPath]
        if self.noLog:
            cmdArgs += ["--logfile", "/dev/null"]
        elif self.outputFilesFormat is not None and not self.noCatch:
            cmdArgs += [
                "--logfile",
                self.remstate + "/log/" + self._getFileName(role, ".log"),
            ]
        if self.outputFilesFormat is not None:
            cmdArgs += [
                "--dbfilename",
                self._getFileName(role, ".rdb"),
            ]
        if role == SLAVE:
            cmdArgs += ["--slaveof", "localhost", str(self.port)]
            if self.password:
                cmdArgs += ["--masterauth", self.password]
        if self.password:
            cmdArgs += ["--requirepass", self.password]
        if self.clusterEnabled and role is not SLAVE:
            # creating .cluster.conf in /tmp as lock fails on NFS
            cmdArgs += [
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                self.remstate + "/cfg/" + self._getFileName(role, ".cluster.conf"),
                "--cluster-node-timeout",
                "5000"
                if self.clusterNodeTimeout is None
                else str(self.clusterNodeTimeout),
            ]
        if self.useAof:
            cmdArgs += ["--appendonly", "yes"]
            cmdArgs += ["--appendfilename", self._getFileName(role, ".aof")]
            if not self.useRdbPreamble:
                cmdArgs += ["--aof-use-rdb-preamble", "no"]

        if self.enableDebugCommand:
            if self._getRedisVersion() > 70000:
                cmdArgs += ["--enable-debug-command", "yes"]

        return cmdArgs

    def createCmdOSEnv(self, role):
        if self.sanitizer != "addr" and self.sanitizer != "address":
            return self.environ
        osenv = self.environ.copy()
        san_log = self._getFileName(role, ".asan.log")
        asan_options = osenv.get("ASAN_OPTIONS")
        osenv["ASAN_OPTIONS"] = "{OPT}:log_path={DIR}".format(
            OPT=asan_options, DIR=san_log
        )
        return osenv

    def waitForRedisToStart(self, con):
        wait_for_conn(con, retries=1000 if self.debugger else 200)
        self._waitForAOFChild(con)

    def getPid(self, role):
        return self.masterProcess if role == MASTER else self.slaveProcess

    def getPort(self, role):
        return self.port if role == MASTER else self.slavePort

    def getServerId(self, role):
        return self.masterServerId if role == MASTER else self.slaveServerId

    def _printEnvData(self, prefix="", role=MASTER):
        console.print(prefix + "pid: %d" % (self.getPid(role)))
        if self.useUnix:
            console.print(prefix + "unix_socket_path: %s" % (self.getUnixPath(role)))

        else:
            console.print(prefix + "port: %d" % (self.getPort(role)))
        console.print(prefix + "binary path: %s" % (self.redisBinaryPath))
        console.print(prefix + "server id: %d" % (self.getServerId(role)))
        console.print(prefix + "using debugger: {}".format(bool(self.debugger)))
        if self.modulePath:
            console.print(prefix + "module: %s" % (self.modulePath))
            if self.moduleArgs:
                console.print(prefix + "module args: %s" % (self.moduleArgs))
        if self.outputFilesFormat:
            console.print(prefix + "log file: %s" % (self._getFileName(role, ".log")))

            console.print(prefix + "db file name: %s" % self._getFileName(role, ".rdb"))

        if self.dbDirPath:
            console.print(prefix + "db dir path: %s" % (self.dbDirPath))
        if self.libPath:
            console.print(prefix + "library path: %s" % (self.libPath))

    def printEnvData(self, prefix=""):
        console.print(prefix + "master:")
        self._printEnvData(prefix + "\t", MASTER)
        if self.useSlaves:
            console.print(prefix + "slave:")
            self._printEnvData(prefix + "\t", SLAVE)

    def startEnv(self, masters=True, slaves=True):
        if self.envIsUp and self.envIsHealthy:
            return  # env is already up
        stdoutPipe = subprocess.PIPE
        stderrPipe = subprocess.STDOUT
        stdinPipe = subprocess.PIPE
        if self.noCatch:
            stdoutPipe = sys.stdout
            stderrPipe = sys.stderr

        if self.has_interactive_debugger:
            stdinPipe = sys.stdin

        options = {
            "stderr": stderrPipe,
            "stdin": stdinPipe,
            "stdout": stdoutPipe,
        }

        if self.verbose:
            console.print("[cyan]Redis master command:[/cyan] " + " ".join(self.masterCmdArgs))
        if masters and self.masterProcess is None:
            self.masterProcess = subprocess.Popen(
                args=self.masterCmdArgs, env=self.masterOSEnv, **options
            ).pid
            con = self.getConnection()
            self.waitForRedisToStart(con)
        if self.useSlaves and slaves and self.slaveProcess is None:
            if self.verbose:
                console.print("Redis slave command: " + " ".join(self.slaveCmdArgs))
            self.slaveProcess = subprocess.Popen(
                args=self.slaveCmdArgs, env=self.slaveOSEnv, **options
            ).pid
            con = self.getSlaveConnection()
            self.waitForRedisToStart(con)
        self.envIsUp = True
        self.envIsHealthy = self.masterProcess is not None and (
            self.slaveProcess is not None if self.useSlaves else True
        )

    def _isAlive(self, pid):
        return psutil.pid_exists(pid)

    def _stopProcess(self, role):
        pid = self.masterProcess if role == MASTER else self.slaveProcess
        if not self._isAlive(pid):
            if not self.has_interactive_debugger:
                if self.outputFilesFormat is not None and not self.noCatch:
                    self.verbose_analyse_server_log(role)
            return

        p0 = psutil.Process(pid=pid)
        pchi = p0.children(recursive=True)
        for p in pchi:
            try:
                p.terminate()
                exit_code = p.wait(timeout=3)
            except Exception as e:
                print(e)
                pass

        p0.terminate()
        exit_code = p0.wait(timeout=3)

        if role == MASTER:
            self.masterExitCode = exit_code
        else:
            self.slaveExitCode = exit_code

    def verbose_analyse_server_log(self, role):
        path = "{0}".format(self._getFileName(role, ".log"))
        if self.dbDirPath is not None:
            path = "{0}/{1}".format(self.dbDirPath, self._getFileName(role, ".log"))
        console.print("\t" + "check the redis log at: {0}".format(path))
        console.print("\t" + "Printing only REDIS BUG REPORT START and STACK TRACE")

        with open(path) as file:
            bug_report_found = False
            for line in file:
                if "REDIS BUG REPORT START" in line:
                    bug_report_found = True
                if "------ INFO OUTPUT ------" in line:
                    break
                if bug_report_found is True:
                    console.print("\t\t" + line.rstrip())

    def stopEnv(self, masters=True, slaves=True):
        if self.masterProcess is not None and masters is True:
            self._stopProcess(MASTER)
            self.masterProcess = None
        if self.useSlaves and self.slaveProcess is not None and slaves is True:
            self._stopProcess(SLAVE)
            self.slaveProcess = None
        self.envIsUp = self.masterProcess is not None or self.slaveProcess is not None
        self.envIsHealthy = self.masterProcess is not None and (
            self.slaveProcess is not None if self.useSlaves else True
        )

    def _getConnection(self, role):
        if self.useUnix:
            return redis.Redis(
                unix_socket_path=self.getUnixPath(role),
                password=self.password,
                decode_responses=self.decodeResponses,
            )

        return redis.Redis(
            "localhost",
            self.getPort(role),
            password=self.password,
            decode_responses=self.decodeResponses,
        )

    def getConnection(self, shardId=1):
        return self._getConnection(MASTER)

    def getSlaveConnection(self):
        if self.useSlaves:
            return self._getConnection(SLAVE)
        raise Exception("asked for slave connection but no slave exists")

    def _waitForAOFChild(self, con):
        import time

        # Wait until file is available
        while True:
            info = con.info("persistence")
            if info["aof_rewrite_scheduled"] or info["aof_rewrite_in_progress"]:
                time.sleep(0.1)
            else:
                break


class ClusterEnv(object):
    def __init__(self, **kwargs):
        self.shards = []
        self.envIsUp = False
        self.envIsHealthy = False
        self.modulePath = kwargs["modulePath"]
        self.moduleArgs = kwargs["moduleArgs"]
        self.password = kwargs["password"]
        self.shardsCount = kwargs.pop("shardsCount")
        useSlaves = kwargs.get("useSlaves", False)
        self.decodeResponses = kwargs.get("decodeResponses", False)
        startPort = kwargs.pop("port", 10000)
        totalRedises = self.shardsCount * (2 if useSlaves else 1)
        randomizePorts = kwargs.pop("randomizePorts", False)
        for i in range(0, totalRedises, (2 if useSlaves else 1)):
            port = 0 if randomizePorts else startPort
            shard = StandardEnv(
                port=port,
                serverId=(i + 1),
                clusterEnabled=True,
                **kwargs,
            )
            self.shards.append(shard)
            startPort += 2

    def printEnvData(self, prefix=""):
        console.print(prefix + "Info:")
        console.print(prefix + "\tshards count:%d" % len(self.shards))
        if self.modulePath:
            console.print(prefix + "\tzip module path:%s" % self.modulePath)
        if self.moduleArgs:
            console.print(prefix + "\tmodule args:%s" % self.moduleArgs)
        for i, shard in enumerate(self.shards):
            console.print(prefix + "Shard: %d" % (i + 1))
            shard.printEnvData(prefix + "\t")

    def waitCluster(self, timeout_sec=40):

        st = time.time()
        ok = 0

        while st + timeout_sec > time.time():
            ok = 0
            for shard in self.shards:
                con = shard.getConnection()
                try:
                    status = con.execute_command("CLUSTER", "INFO")
                except Exception as e:
                    print("got error on cluster info, will try again, %s" % str(e))
                    continue
                if "cluster_state:ok" in str(status):
                    ok += 1
            if ok == len(self.shards):
                for shard in self.shards:
                    try:
                        shard.getConnection().execute_command("FT.CLUSTERREFRESH")
                    except Exception:
                        pass
                    try:
                        shard.getConnection().execute_command("SEARCH.CLUSTERREFRESH")
                    except Exception:
                        pass
                return

            time.sleep(0.1)
        raise RuntimeError(
            "Cluster OK wait loop timed out after %s seconds" % timeout_sec
        )

    def startEnv(self, masters=True, slaves=True):
        if self.envIsUp == True:
            print("Env already running")
            return  # env is already up
        try:
            for shard in self.shards:
                shard.startEnv(masters, slaves)
        except Exception:
            for shard in self.shards:
                shard.stopEnv()
            raise

        slots_per_node = int(16384 / len(self.shards)) + 1
        for i, shard in enumerate(self.shards):
            con = shard.getConnection()
            for s in self.shards:
                con.execute_command("CLUSTER", "MEET", "127.0.0.1", s.getMasterPort())

            start_slot = i * slots_per_node
            end_slot = start_slot + slots_per_node
            if end_slot > 16384:
                end_slot = 16384

            try:
                con.execute_command(
                    "CLUSTER",
                    "ADDSLOTS",
                    *(str(x) for x in range(start_slot, end_slot)),
                )
            except Exception:
                pass

        self.waitCluster()
        self.envIsUp = True
        self.envIsHealthy = True

    def stopEnv(self, masters=True, slaves=True):
        self.envIsUp = False
        self.envIsHealthy = False
        for shard in self.shards:
            shard.stopEnv(masters, slaves)
            self.envIsUp = self.envIsUp or shard.envIsUp
            self.envIsHealthy = self.envIsHealthy and shard.envIsUp
