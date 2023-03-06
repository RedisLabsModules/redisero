import copy
import errno
import fcntl
import itertools
import json
import os
import random
import re
import socket
import struct
import subprocess
import sys
import time

import redis


def wait_for_conn(conn, retries=20, command="PING", shouldBe=True):
    """Wait until a given Redis connection is ready"""
    err1 = ""
    while retries > 0:
        try:
            if conn.execute_command(command) == shouldBe:
                return conn
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.1)  # give extra 100msec in case of RDB loading
        except redis.ConnectionError as err:
            err1 = str(err)
        except redis.ResponseError as err:
            err1 = str(err)
            if not err1.startswith("DENIED"):
                raise
        time.sleep(0.1)
        retries -= 1
    raise Exception("Cannot establish connection %s: %s" % (conn, err1))


def fix_modules(modules, defaultModules=None):
    # modules is one of the following:
    # None
    # ['path',...]
    if modules:
        if not isinstance(modules, list):
            modules = [modules]
        modules = list(map(lambda p: os.path.abspath(p), modules))
    else:
        modules = defaultModules
    return modules


def split_by_semicolon(s):
    return list(
        filter(
            lambda s: s != "",
            map(lambda s: re.sub(r"\\(.)", r"\1", s.strip()), re.split(r"(?<!\\);", s)),
        )
    )


def args_list_to_dict(args_list):
    def dicty(args):
        return dict((seq.split(" ")[0], seq) for seq in args)

    return list(map(lambda args: dicty(args), args_list))


def join_lists(lists):
    return list(itertools.chain.from_iterable(lists))


def fix_modulesArgs(modules, modulesArgs, defaultArgs=None, haveSeqs=True):
    # modulesArgs is one of the following:
    # None
    # 'args ...': arg string for a single module
    # ['args ...', ...]: arg list for a single module
    # [['arg', ...', ...], ...]: arg strings for multiple modules

    # arg string is a string of words seperated by whitespace
    # arg string can be seperated by semicolons into (logical) arg lists.
    # semicolons can be escaped with a backslash.
    # arg list is a list of arg strings.
    # arg list starts with an arg name that can later be used for argument overriding.
    # arg strings are transformed into arg lists (haveSeqs parameter controls this behavior):
    # thus, 'num 1; names a b' becomes ['num 1', 'names a b']

    if type(modulesArgs) == str:
        # case # 'args ...': arg string for a single module
        # transformed into [['arg', ...]]
        modulesArgs = [split_by_semicolon(modulesArgs)]
    elif type(modulesArgs) == list:
        args = []
        is_list = False
        is_str = False
        for argx in modulesArgs:
            if type(argx) == list:
                # case [['arg', ...], ...]: arg strings for multiple modules
                # already transformed into [['arg', ...], ...]
                if is_str:
                    print("Error in args: %s" % str(modulesArgs))
                    sys.exit(1)
                is_list = True
                if haveSeqs:
                    lists = map(lambda x: split_by_semicolon(x), argx)
                    args += [join_lists(lists)]
                else:
                    args += [argx]
            else:
                # case ['args ...', ...]: arg list for a single module
                # transformed into [['arg', ...], ...]
                if is_list:
                    print("Error in args: %s" % str(modulesArgs))
                    sys.exit(1)
                is_str = True
                args += split_by_semicolon(argx)
        if is_str:
            args = [args]
        modulesArgs = args
    # modulesArgs is now [['arg', ...], ...]

    is_copy = not modulesArgs and defaultArgs
    if is_copy:
        modulesArgs = copy.deepcopy(defaultArgs)

    n = 0
    num_mods = len(modulesArgs) if modulesArgs else 0
    if defaultArgs:
        n = len(defaultArgs) - num_mods
        num_mods += n

    if isinstance(modules, list) and len(modules) > 1:
        n = len(modules) - num_mods

    if n > 0:
        if not modulesArgs:
            modulesArgs = []
        modulesArgs.extend([[]] * n)

    if is_copy or not defaultArgs:
        return modulesArgs

    # if there are fewer defaultArgs than modulesArgs, we should bail out
    # as we cannot pad the defaults with emply arg lists
    if defaultArgs and len(modulesArgs) > len(defaultArgs):
        print("Number of module args sets in Env does not match number of modules")
        print(defaultArgs)
        print(modulesArgs)
        sys.exit(1)

    # for each module, sync defaultArgs to modulesARgs
    modules_args_dict = args_list_to_dict(modulesArgs)
    for imod, args_list in enumerate(defaultArgs):
        for arg in args_list:
            name = arg.split(" ")[0]
            if name not in modules_args_dict[imod]:
                modulesArgs[imod] += [arg]

    return modulesArgs


def _check_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError as e:
        if e.errno == errno.EPERM:
            return True
        return False


def register_port(port):
    fp = open("/tmp/redisero_portfile.lock", "a+")
    fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
    fp.seek(0, 2)  # seek from end
    if fp.tell() == 0:
        entries = {}
    else:
        fp.seek(0, 0)
        entries = json.load(fp)
    # remove not responsive processes
    entries = {p: pid for p, pid in entries.items() if _check_alive(pid)}

    if str(port) in entries:
        ret = False
    else:
        entries[str(port)] = os.getpid()
        ret = True

    fp.seek(0, 0)
    fp.truncate()
    json.dump(entries, fp)
    fp.close()
    return ret


def get_random_port():
    for _ in range(10000):
        p = random.randint(10000, 20000)
        # Try to open and bind the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        try:
            s.bind(("", p))
            s.close()
            if not register_port(p):
                continue
            return p
        except Exception as e:
            if hasattr(e, "errno") and e.errno in (
                errno.EADDRINUSE,
                errno.EADDRNOTAVAIL,
            ):
                pass
            else:
                raise e

    raise Exception("Could not find open port to listen on!")


def find_folder(name, path):
    for root, dirs, files in os.walk(path):
        if name in dirs:
            return os.path.join(root, name)
    return None


def list_files(path):
    for root, directories, files in os.walk(path):
        for filename in files:
            yield os.path.join(root, filename)


def run_npm(
    pkgdir: str,
    cmd: str,
    prefix: str,
    args: str,
    npm_bin: str = "npm",
):
    """Run npm command"""
    command = [npm_bin, cmd, prefix, args]

    return subprocess.call(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=pkgdir,
    )
