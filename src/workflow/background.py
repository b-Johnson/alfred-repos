#!/usr/bin/env python3
# encoding: utf-8
#
# Copyright (c) 2022 Thomas Harr <xDevThomas@gmail.com>
# Copyright (c) 2019 Dean Jackson <deanishe@deanishe.net>
#
# MIT Licence. See http://opensource.org/licenses/MIT
#
# Created on 2014-04-06
#

"""This module provides an API to run commands in background processes.

Combine with the :ref:`caching API <caching-data>` to work from cached data
while you fetch fresh data in the background.

See :ref:`the User Manual <background-processes>` for more information
and examples.
"""

import os
import pickle
import signal
import subprocess

from workflow import Workflow

__all__ = ['is_running', 'run_in_background']

_wf = None


def wf():
    global _wf
    if _wf is None:
        _wf = Workflow()
    return _wf


def _log():
    return wf().logger


def _arg_cache(name):
    """Return path to pickle cache file for arguments.

    :param name: name of task
    :type name: ``str``
    :returns: Path to cache file
    :rtype: ``str`` filepath

    """
    return wf().cachefile(name + '.argcache')


def _pid_file(name):
    """Return path to PID file for ``name``.

    :param name: name of task
    :type name: ``str``
    :returns: Path to PID file for task
    :rtype: ``str`` filepath

    """
    return wf().cachefile(name + '.pid')


def _process_exists(pid):
    """Check if a process with PID ``pid`` exists.

    :param pid: PID to check
    :type pid: ``int``
    :returns: ``True`` if process exists, else ``False``
    :rtype: ``Boolean``

    """
    try:
        os.kill(pid, 0)
    except OSError:  # not running
        return False
    return True


def _job_pid(name):
    """Get PID of job or `None` if job does not exist.

    Args:
        name (str): Name of job.

    Returns:
        int: PID of job process (or `None` if job doesn't exist).
    """
    pidfile = _pid_file(name)
    if not os.path.exists(pidfile):
        return

    with open(pidfile, 'r') as fp:
        pid = int(fp.read())

        if _process_exists(pid):
            return pid

    os.unlink(pidfile)


def is_running(name):
    """Test whether task ``name`` is currently running.

    :param name: name of task
    :type name: str
    :returns: ``True`` if task with name ``name`` is running, else ``False``
    :rtype: bool

    """
    if _job_pid(name) is not None:
        return True

    return False


def _start_background(pidfile, args, **kwargs):  # pragma: no cover
    """Start a detached background process and write its PID to ``pidfile``.

    Uses ``start_new_session=True`` instead of the double-fork pattern to
    avoid the ``os.fork()`` deprecation warning introduced in Python 3.12.

    :param pidfile: file to write PID of the detached process to.
    :type pidfile: filepath
    :param args: command and arguments passed to :func:`subprocess.Popen`
    :param kwargs: keyword arguments passed to :func:`subprocess.Popen`

    """
    popen_kwargs = {k: v for k, v in kwargs.items() if k != 'timeout'}
    with open(os.devnull, 'rb') as devnull:
        proc = subprocess.Popen(
            args,
            stdin=devnull,
            stdout=devnull,
            stderr=devnull,
            start_new_session=True,
            **popen_kwargs,
        )

    tmp = pidfile + '.tmp'
    with open(tmp, 'w') as fp:
        fp.write(str(proc.pid))
    os.rename(tmp, pidfile)


def kill(name, sig=signal.SIGTERM):
    """Send a signal to job ``name`` via :func:`os.kill`.

    .. versionadded:: 1.29

    Args:
        name (str): Name of the job
        sig (int, optional): Signal to send (default: SIGTERM)

    Returns:
        bool: `False` if job isn't running, `True` if signal was sent.
    """
    pid = _job_pid(name)
    if pid is None:
        return False

    os.kill(pid, sig)
    return True


def run_in_background(name, args, **kwargs):
    r"""Cache arguments then call this script again via :func:`subprocess.call`.

    :param name: name of job
    :type name: str
    :param args: arguments passed as first argument to :func:`subprocess.call`
    :param \**kwargs: keyword arguments to :func:`subprocess.call`
    :returns: exit code of sub-process
    :rtype: int

    When you call this function, it caches its arguments and then calls
    ``background.py`` in a subprocess. The Python subprocess will load the
    cached arguments, fork into the background, and then run the command you
    specified.

    This function will return as soon as the ``background.py`` subprocess has
    forked, returning the exit code of *that* process (i.e. not of the command
    you're trying to run).

    If that process fails, an error will be written to the log file.

    If a process is already running under the same name, this function will
    return immediately and will not run the specified command.

    """
    if is_running(name):
        _log().info('[%s] job already running', name)
        return

    argcache = _arg_cache(name)

    # Cache arguments
    with open(argcache, 'wb') as fp:
        pickle.dump({'args': args, 'kwargs': kwargs}, fp)
        _log().debug('[%s] command cached: %s', name, argcache)

    # Call this script in module mode because of relativ import
    cmd = ['/usr/bin/env', 'python3', '-m', 'workflow.background', name]
    _log().debug('[%s] passing job to background runner: %r', name, cmd)
    retcode = subprocess.call(cmd)

    if retcode:  # pragma: no cover
        _log().error('[%s] background runner (%r) failed with %d', name, cmd, retcode)
    else:
        _log().debug('[%s] background job started', name)

    return retcode


def main(wf):  # pragma: no cover
    """Run command in a background process.

    Load cached arguments, launch as a detached subprocess, then return.

    """
    log = wf.logger
    name = wf.args[0]
    argcache = _arg_cache(name)
    if not os.path.exists(argcache):
        msg = '[{0}] command cache not found: {1}'.format(name, argcache)
        log.critical(msg)
        raise IOError(msg)

    # Load cached arguments
    with open(argcache, 'rb') as fp:
        data = pickle.load(fp)

    args = data['args']
    kwargs = data['kwargs']

    # Delete argument cache file
    os.unlink(argcache)

    pidfile = _pid_file(name)
    log.debug('[%s] running command: %r', name, args)
    _start_background(pidfile, args, **kwargs)
    log.debug('[%s] background job started', name)


if __name__ == '__main__':  # pragma: no cover
    wf().run(main)
