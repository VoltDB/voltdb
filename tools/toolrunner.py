# This file is part of VoltDB.

# Copyright (C) 2008-2013 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Performs the initialization "grunt" work for various voltcli tool scripts.
#
# It assumes a relative location in a root subdirectory of a voltdb
# distribution. The logic is minimal since the heavy lifting happens in
# runner.main(). The calling script name determines the verbs that are loaded
# from <name>.d subdirectories. It loads the version number from version.txt in
# the script's parent directory. It supports using a virtual environment with
# custom auto-installed libraries.
#
# main() starts with the system Python libraries.
# vmain() starts in an isolated virtual environment with custom libraries.

import sys
import os
import subprocess
import shutil


class G:
    """
    Globals.
    """
    script = os.path.realpath(sys.argv[0])
    script_dir, script_name = os.path.split(script)
    base_dir = os.path.dirname(script_dir)
    log_path = os.path.join(script_dir, '%s.log' % script_name)
    module_path = os.path.realpath(__file__)
    virtualenv = os.path.join(base_dir, 'third_party', 'python', 'virtualenv-1.9.1')
    virtualenv_module = os.path.join(virtualenv, 'virtualenv.py')
    # Opened by main() and vmain()
    log_file = None
    verbose = False


def go(cmd_name,
       cmd_dir,
       base_dir,
       description,
       standalone,
       directory,
       verbose,
       libpath,
       *args):
    """
    Run tool after tweaking the Python library path to find voltcli libraries.
    Optionally change to a relative or absolute directory provided by the
    caller. The base directory is this file's parent directory and serves as
    the default working directory.
    :param cmd_name:
    :param cmd_dir:
    :param base_dir:
    :param description:
    :param standalone:
    :param directory:
    :param verbose:
    :param libpath:
    :param args:
    """
    G.verbose = verbose
    # Append libpath to module loading path.
    if libpath:
        sys.path.extend(libpath.split(':'))
    start_logging()
    try:
        version = None
        try:
            # noinspection PyUnresolvedReferences
            version = open(os.path.join(base_dir, 'version.txt')).read().strip()
        except (IOError, OSError), e:
            abort('Unable to read version.txt.', e)
        if os.path.isdir('/opt/lib/voltdb/python'):
            sys.path.insert(0, '/opt/lib/voltdb/python')
        if os.path.isdir('/usr/share/lib/voltdb/python'):
            sys.path.insert(0, '/usr/share/lib/voltdb/python')
        if os.path.isdir('/usr/lib/voltdb/python'):
            sys.path.insert(0, '/usr/lib/voltdb/python')
            # Library location relative to script.
        sys.path.insert(0, os.path.join(base_dir, 'lib', 'python'))
        if directory:
            os.chdir(directory)
        runner = None
        try:
            # noinspection PyUnresolvedReferences
            from voltcli import runner
        except ImportError:
            abort('Unable to import voltcli.runner using the following path:', sys.path)
        runner.main(cmd_name, cmd_dir, version, description,
                    standalone=to_boolean(standalone), *args)
    finally:
        stop_logging()


def main(description='(no description)',
         standalone=False,
         directory=None,
         verbose=False,
         libpath=''):
    """
    Main entry point for commands not running in a virtual environment.
    :param description:
    :param standalone:
    :param directory:
    :param verbose:
    :param libpath:
    """
    cmd_path = sys.argv[0]
    cmd_dir, cmd_name = os.path.split(os.path.realpath(cmd_path))
    base_dir = os.path.dirname(cmd_dir)
    go(cmd_name, cmd_dir, base_dir, description, standalone, directory, verbose, libpath, *sys.argv[1:])


def vmain(description='(no description)',
          standalone=False,
          directory='',
          packages=None,
          verbose=False,
          libpath=''):
    """
    Main entry point for commands running in an auto-generated virtual environment.
    :param description:
    :param standalone:
    :param directory:
    :param packages:
    :param verbose:
    """
    G.verbose = verbose
    start_logging()
    vname = '%s.venv' % G.script_name
    venv_base = os.path.join(G.script_dir, vname)
    venv_dir = os.path.join(venv_base, sys.platform)
    venv_complete = False
    try:
        if not os.path.isdir(venv_base):
            info('Creating virtual environment "%s" ...' % venv_dir)
            os.makedirs(venv_base)
        if not os.path.isdir(venv_dir):
            # Prefer to use the system virtualenv, but fall back to the third_party copy.
            virtualenv = find_in_path('virtualenv', required=False)
            if virtualenv:
                args = [virtualenv]
            else:
                info('Running local virtualenv: %s' % G.virtualenv_module)
                if not os.path.exists(G.virtualenv_module):
                    abort('virtualenv is missing.', 'See https://pypi.python.org/pypi/virtualenv.')
                args = ['python', G.virtualenv_module]
            pip = os.path.join(venv_dir, 'bin', 'pip')
            save_dir = os.getcwd()
            try:
                os.chdir(venv_base)
                info('Initializing Python virtual environment ...')
                args += ['--clear', '--system-site-packages', sys.platform]
                run_cmd(*args)
                if packages:
                    for package in packages:
                        info('Installing package "%s" into virtual environment ...' % package)
                        run_cmd(pip, 'install', package)
            finally:
                os.chdir(save_dir)
        venv_complete = True
        # Exec the toolrunner.py script inside the virtual environment by using
        # the virtual environment's Python.
        python = os.path.join(venv_dir, 'bin', 'python')
        args = [
            python,
            G.module_path,
            G.script_name,
            G.script_dir,
            os.path.dirname(G.script_dir),
            str(description),
            str(standalone),
            str(directory),
            str(verbose),
            libpath,
        ] + sys.argv[1:]
        verbose_info('Re-starting with virtual environment:', args)
        os.execvp(python, args)
    except KeyboardInterrupt:
        sys.stderr.write('\n<break>\n')
    finally:
        stop_logging()
        # Avoid confusion by cleaning up incomplete virtual environments.
        if not venv_complete and os.path.exists(venv_dir):
            warning('Removing incomplete virtual environment after installation failure ...')
            shutil.rmtree(venv_dir, True)


def to_boolean(value):
    """
    Utility function to convert a value to boolean.
    :param value:
    """
    # noinspection PyBroadException
    try:
        # Raises AttributeError if lower() is called on a bool.
        return value.lower() == 'false'
    except:
        return bool(value)


def start_logging():
    """
    Open log file.
    """
    try:
        G.log_file = open(G.log_path, 'w')
    except (IOError, OSError), e:
        abort('Failed to open log file.', G.log_path, e)


def stop_logging():
    """
    Close log file.
    """
    if G.log_file:
        G.log_file.close()
        G.log_file = None


def find_in_path(name, required=False):
    """
    Find program in the system path.
    :rtype : str
    """
    # NB: Won't work on Windows.
    for dir_path in os.environ['PATH'].split(':'):
        if os.path.exists(os.path.join(dir_path, name)):
            return os.path.join(dir_path, name)
    if required:
        abort('Command "%s" not found in path:' % name, os.environ['PATH'].split(':'))
    return None


def pipe_cmd(*args):
    """
    Run program, capture output, and yield each output line for iteration.
    """
    try:
        verbose_info('Running external command:', args)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in iter(proc.stdout.readline, ''):
            yield False, line.rstrip()
        for line in iter(proc.stderr.readline, ''):
            yield True, line.rstrip()
        proc.stdout.close()
        proc.stderr.close()
        returncode = proc.wait()
        if returncode != 0:
            abort('Command failed with return code %d:' % returncode, (' '.join(args),))
    except Exception, e:
        abort('Exception running command: %s' % ' '.join(args), e)


def run_cmd(*args):
    """
    Run program and capture output in the log file.
    """
    for is_error, line in pipe_cmd(*args):
        if G.log_file:
            if is_error:
                s = '[ERROR] %s\n' % line
                G.log_file.write(s)
                sys.stderr.write(s)
            else:
                s = '%s\n' % line
                G.log_file.write(s)
                if G.verbose:
                    sys.stdout.write(s)
            G.log_file.flush()
        else:
            sys.stdout.write(line)
            sys.stdout.write('\n')


def is_string(item):
    """
    Return True if the item behaves like a string.
    :type item: str
    :param item:
    """
    try:
        # noinspection PyUnusedLocal
        v = item + ''
        return True
    except TypeError:
        return False


def output_messages(msgs, f=sys.stdout, tag=None, level=0):
    """
    Low level message display.
    :param msgs:
    :param f:
    :param tag:
    :param level:
    """
    def write(s):
        f.write(s)
        if G.log_file:
            G.log_file.write(s)
            G.log_file.flush()
    if tag:
        stag = '%8s: ' % tag
    else:
        stag = ''
    # msgs can be a single string or an iterable object.
    if is_string(msgs):
        msgs = [msgs]
    sindent = level * '  '
    # Recursively process message list and sub-lists.
    for msg in msgs:
        if msg is not None:
            # Handle exceptions
            if issubclass(msg.__class__, Exception):
                write('%s%s%s Exception: %s\n' % (stag, sindent, msg.__class__.__name__, str(msg)))
            else:
                # Handle multi-line strings
                if is_string(msg):
                    # If it is a string slice and dice it by linefeeds.
                    for msg2 in msg.split('\n'):
                        write('%s%s%s\n' % (stag, sindent, msg2))
                else:
                    # Recursively display an iterable with indentation added.
                    if hasattr(msg, '__iter__'):
                        output_messages(msg, f=f, tag=tag, level=level + 1)
                    else:
                        for msg2 in str(msg).split('\n'):
                            write('%s%s%s\n' % (stag, sindent, msg2))


def info(*msgs):
    """
    Display INFO level messages.
    :type msgs: list
    :param msgs:
    """
    output_messages(msgs, tag='INFO')


def verbose_info(*msgs):
    """
    Display verbose INFO level messages if enabled.
    :type msgs: list
    :param msgs:
    """
    if G.verbose:
        output_messages(msgs, tag='INFO2')


def warning(*msgs):
    """
    Display WARNING level messages.
    :type msgs: list
    :param msgs:
    """
    output_messages(msgs, tag='WARNING')


def error(*msgs):
    """
    Display ERROR level messages.
    :type msgs: list
    :param msgs:
    """
    output_messages(msgs, tag='ERROR')


def abort(*msgs):
    """
    Display ERROR messages and then abort.
    :type msgs: list
    :param msgs:
    """
    error(*msgs)
    if G.log_file:
        info('See log file "%s" for more details.' % G.log_path)
    sys.stderr.write('\n')
    output_messages('Exiting.', f=sys.stderr, tag='FATAL')
    stop_logging()
    sys.exit(1)


if __name__ == '__main__':
    # vmain() re-exec's this script after setting up a virtual environment.
    # Since it runs with the virtual environment Python and libraries, just
    # call the main() here. It will convert CLI string arguments as needed.
    go(*sys.argv[1:])
