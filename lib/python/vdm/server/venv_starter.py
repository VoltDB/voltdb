#!/usr/bin/env python
# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
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

# This script assumes a relative location in a root subdirectory of a voltdb
# distribution. The logic is intentionally minimal since almost all of the
# heavy lifting happens in runner.main(). The script name determines the verbs
# that are loaded from <name>.d subdirectories. It loads the version number
# from version.txt in the script's parent directory. It can be copied to other
# names, and also to other locations if the path-building is adjusted. The
# description should also be changed if re-used for another base command.

import sys
import os
import subprocess
import shutil
import re
import glob
import copy


class G:
    """
    Globals.
    """
    cmd_dir, cmd_name = os.path.split(os.path.realpath(sys.argv[0]))
    base_dir = os.path.dirname(cmd_dir)
    script_dir = os.path.join(base_dir, 'bin')
    script_name = 'voltdeploy';
    # Use ~/.<script> as the output directory for logging and virtual environments.
    user_dir = os.path.expanduser(os.path.join('~', '.voltdeploy'))
    log_path = os.path.join(user_dir, 'logs', '%s.log' % script_name)
    module_path = os.path.realpath(__file__)
    # Opened by main() and vmain()
    log_file = None
    verbose = False
    # Full path glob of virtualenv packages. The latest is chosen based on the parsed version number.
    virtualenv_package_glob = os.path.join(base_dir, 'third_party', 'python', 'packages', 'virtualenv-*.tar.gz')
    virtualenv_parse_re = re.compile('^.*/(virtualenv-([0-9.]+))[.]tar[.]gz$')


def get_version(base_dir, error_abort=True):
    try:
        # noinspection PyUnresolvedReferences
        return open(os.path.join(base_dir, 'version.txt')).read().strip()
    except (IOError, OSError), e:
        if error_abort:
            abort('Unable to read version.txt.', e)
        return None


def get_virtualenv():
    """
    Find the virtualenv runtime. Fall back to using one from
    third_party/python/packages. Untar it under the working folder. Return a
    command line argument list.
    """
    virtualenv = find_in_path('virtualenv', required=False)
    if virtualenv:
        return [virtualenv]
    # Find the latest package.
    latest_version = []
    latest_package_path = None
    latest_package_name = None
    for package_path in glob.glob(G.virtualenv_package_glob):
        m = G.virtualenv_parse_re.match(package_path)
        if m:
            version = [int(i) for i in m.group(2).split('.')]
            # Zero-pad the versions for comparison.
            if len(version) > len(latest_version):
                latest_version.extend([0] * (len(version) - len(latest_version)))
            elif len(latest_version) > len(version):
                version.extend([0] * (len(latest_version) - len(version)))
            for iv in range(len(version)):
                if version[iv] > latest_version[iv]:
                    latest_version = copy.copy(version)
                    latest_package_path = package_path
                    latest_package_name = m.group(1)
                    break
                elif version[iv] < latest_version[iv]:
                    break
        else:
            warning('Failed to parse virtualenv package path: %s' % package_path)
    if not latest_package_path:
        abort('virtualenv is missing.', 'See https://pypi.python.org/pypi/virtualenv.')
    info('Unpacking the distribution copy of virtualenv:',  [latest_package_path])
    returncode = os.system('tar xf "%s"' % latest_package_path)
    if returncode != 0:
        abort('Failed to extract the virtualenv package.')
    return ['python', os.path.join(os.getcwd(), latest_package_name, 'virtualenv.py')]


# Internal function to build/rebuild the virtual environment
def _build_virtual_environment(venv_dir, version, packages):
    # Wipe any existing virtual environment directory.
    if os.path.exists(venv_dir):
        try:
            shutil.rmtree(venv_dir)
        except (IOError, OSError), e:
            abort('Failed to remove existing virtual environment.', (venv_dir, e))
    # Create the directory as needed.
    if not os.path.isdir(venv_dir):
        os.makedirs(venv_dir)
    # Save version.txt in the venv root directory.
    try:
        version_path = os.path.join(venv_dir, 'version.txt')
        f = open(version_path, 'w')
        try:
            try:
                f.write(version)
            except (IOError, OSError), e:
                abort('Failed to write version file.', (version_path, e))
        finally:
            f.close()
    except (IOError, OSError), e:
        abort('Failed to open version file for writing.', (version_path, e))
    # Prefer to use the system virtualenv, but fall back to the third_party copy.
    save_dir = os.getcwd()
    save_lc_all = os.environ.get('LC_ALL', None)
    # Makes sure a pip failure provides clean output instead of a Unicode error.
    os.environ['LC_ALL'] = 'C'
    try:
        os.chdir(os.path.dirname(venv_dir))
        args = get_virtualenv()
        pip = os.path.join(venv_dir, 'bin', 'pip')
        info('Preparing the %s Python virtual environment:' % G.script_name, [
                    '(an Internet connection is required)',
                    'Folder: %s' % venv_dir])
        args += ['--clear', '--system-site-packages', sys.platform]
        run_cmd(*args)
        if packages:
            for package in packages:
                # info('Installing virtual environment package: %s' % package)
                run_cmd(pip, '--quiet', 'install', package)
    finally:
        os.chdir(save_dir)
        if save_lc_all is None:
            del os.environ['LC_ALL']
        else:
            os.environ['LC_ALL'] = save_lc_all


def main(arr):
    python = 'python'
    args = [python, os.path.join(G.base_dir, 'lib/python/vdm/vdmrunner.py')]
    if arr[0]['filepath'] is not None:
        args.append('-p' + str(arr[0]['filepath']))
    if arr[0]['server'] is not None:
        args.append('-s' + str(arr[0]['server']))
    os.execvp(python, args)


def start_virtual_environment(arr, packages=None, verbose=False):
    G.verbose = verbose;
    start_logging()
    # Set up virtual environment under home since it should be write-able.
    output_dir = os.path.join(G.user_dir, 'voltdeploy')
    # Make sure the output directory is available.
    if not os.path.isdir(output_dir):
        if os.path.exists(output_dir):
            abort('Output path "%s" exists, but is not a directory.' % output_dir,
                  'Please move or delete it before running this command again.')
        try:
            os.makedirs(output_dir)
        except (IOError, OSError), e:
            abort('Output path "%s" exists, but is not a directory.' % output_dir,
                  'Please move or delete it before running this command again.', e)
    venv_base = os.path.join(output_dir, 'venv')
    venv_dir = os.path.join(venv_base, sys.platform)
    venv_complete = False
    version = get_version(os.path.dirname(G.script_dir))
    try:
        build_venv = not os.path.isdir(venv_dir)
        if not build_venv:
            # If the virtual environment is present check that it's current.
            # If version.txt is not present leave it alone so that we don't
            # get in the situation where the virtual environment gets
            # recreated every time.
            venv_version = get_version(venv_dir, error_abort=False)
            if venv_version is None:
                warning('Unable to read the version file:',
                        [os.path.join(venv_dir, 'version.txt')],
                        'Assuming that the virtual environment is current.',
                        'To force a rebuild delete the virtual environment base directory:',
                        [venv_base])
            else:
                build_venv = venv_version != version
        if build_venv:
            _build_virtual_environment(venv_dir, version, packages)
        venv_complete = True
        # the virtual environment's Python.
        python = os.path.join(venv_dir, 'bin', 'python')
        args = [python, os.path.join(G.base_dir, 'lib/python/vdm/vdmrunner.py')]
        if arr[0]['filepath'] is not None:
            args.append('-p' + str(arr[0]['filepath']))
        if arr[0]['server'] is not None:
            args.append('-s' + str(arr[0]['server']))
        os.execvp(python, args)
    except KeyboardInterrupt:
        sys.stderr.write('\n<break>\n')
    finally:
        stop_logging()
        # Avoid confusion by cleaning up incomplete virtual environments.
        if not venv_complete and os.path.exists(venv_dir):
            warning('Removing incomplete virtual environment after installation failure ...')
            shutil.rmtree(venv_dir, True)
            return {'status': 'error', 'path_venv_python': ''}
        else:
            return {'status': 'success', 'path_venv_python': python}


def start_logging():
    """
    Open log file.
    """
    base_dir = os.path.dirname(G.log_path)
    if not os.path.exists(base_dir):
        try:
            os.makedirs(base_dir)
        except (IOError, OSError), e:
            abort('Failed to create log directory.', (base_dir, e))
    try:
        G.log_file = open(G.log_path, 'w')
    except (IOError, OSError), e:
        abort('Failed to open log file.', G.log_path, e)


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


def verbose_info(*msgs):
    """
    Display verbose INFO level messages if enabled.
    :type msgs: list
    :param msgs:
    """
    if G.verbose:
        output_messages(msgs, tag='INFO2')


def run_cmd(*args):
    """
    Run program and capture output in the log file.
    """
    for is_error, line in pipe_cmd(*args):
        if G.log_file:
            if is_error:
                if 'You are using pip version 6.1.1, however version 8.0.2 is available.' in line or \
                                'You should consider upgrading via the \'pip install --upgrade pip\' command.' in line:
                    s = '[WARNING] %s\n' % line
                else:
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


def stop_logging():
    """
    Close log file.
    """
    if G.log_file:
        G.log_file.close()
        G.log_file = None


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

