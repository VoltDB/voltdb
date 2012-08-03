#!/usr/bin/env python
# This file is part of VoltDB.

# Copyright (C) 2008-2011 VoltDB Inc.
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

import sys
import os
from glob import glob
from optparse import OptionParser
import tempfile
import shutil
import subprocess
import pwd
import grp
from copy import copy

#### Utility functions

def _message(f, tag, *msgs):
    '''Low level message display.'''
    if tag:
        stag = '%8s: ' % tag
    else:
        stag = ''
    for msg in msgs:
        if msg is not None:
            # Handle exceptions
            if issubclass(msg.__class__, Exception):
                f.write('%s%s Exception: %s\n' % (stag, msg.__class__.__name__, str(msg)))
            else:
                # Handle multi-line strings
                try:
                    t = msg + ' '   # Throws exception if not string
                    for msg2 in msg.split('\n'):
                        f.write('%s%s\n' % (stag, msg2))
                except TypeError:
                    if hasattr(msg, '__iter__'):
                        for msg2 in msg:
                            f.write('%s   %s\n' % (stag, msg2))
                    else:
                        f.write('%s%s\n' % (stag, str(msg)))

def info(*msgs):
    _message(sys.stdout, 'INFO', *msgs)

def debug(*msgs):
    if Global.options.debug:
        _message(sys.stdout, 'DEBUG', *msgs)

def warning(*msgs):
    _message(sys.stdout, 'WARNING', *msgs)

def error(*msgs):
    _message(sys.stderr, 'ERROR', *msgs)

def abort(*msgs):
    error(*msgs)
    _message(sys.stderr, 'FATAL', 'Errors are fatal, aborting execution')
    sys.exit(1)

def get_dst_path(*dirs):
    return os.path.join(Global.options.prefix, *dirs)

def run_cmd(cmd, *args):
    fullcmd = cmd
    for arg in args:
        if len(arg.split()) > 1:
            fullcmd += ' "%s"' % arg
        else:
            fullcmd += ' %s' % arg
    if Global.options.dryrun:
        print fullcmd
    else:
        retcode = os.system(fullcmd)
        if retcode != 0:
            abort('return code %d: %s' % (retcode, fullcmd))

def pipe_cmd(*args):
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, ''):
            yield line.rstrip()
        proc.stdout.close()
    except Exception, e:
        warning('Exception running command: %s' % ' '.join(args), e)

class ChDir(object):
    '''Object that can be used in a with statement to temporarily change the
    working directory. It creates the directory if missing.'''
    def __init__(self, newdir):
        self.newdir = newdir
        self.olddir = None
    def __enter__(self):
        if self.newdir:
            if not os.path.exists(self.newdir):
                info('Creating directory "%s"...' % self.newdir)
                os.makedirs(self.newdir)
            self.olddir = os.getcwd()
            debug('Changing to directory "%s"...' % self.newdir)
            os.chdir(self.newdir)
    def __exit__(self, type, value, traceback):
        if self.olddir:
            debug('Changing back to directory "%s"...' % self.olddir)
            os.chdir(self.olddir)

# http://code.activestate.com/recipes/208993-compute-relative-path-from-one-directory-to-anothe
def relpath(p1, p2):
    '''Calculate the relative path between two directories.'''
    (common, l1, l2) = commonpath(os.path.realpath(p1).split(os.path.sep),
                                  os.path.realpath(p2).split(os.path.sep))
    p = []
    if len(l1) > 0:
        p = [ '../' * len(l1) ]
    p = p + l2
    return os.path.join(*p)

def commonpath(l1, l2, common=[]):
    '''Used by relpath() to determine the common path components.'''
    if len(l1) < 1:
        return (common, l1, l2)
    if len(l2) < 1:
        return (common, l1, l2)
    if l1[0] != l2[0]:
        return (common, l1, l2)
    return commonpath(l1[1:], l2[1:], common+[l1[0]])

def find_source_root(path):
    '''Find the root directory of the source tree containing a specified path.'''
    if os.path.isdir(path):
        dir = path
    else:
        dir = os.path.dirname(path)
    while dir != '/':
        if (    os.path.exists(os.path.join(dir, 'build.xml'))
            and os.path.exists(os.path.join(dir, 'third_party'))):
            return dir
        dir = os.path.dirname(dir)
    abort('Source root directory not found for "%s".\n' % path)

def format_debian_description(description):
    '''Reformat the control file description for Debian compatibility.'''
    # Strip blank lines and add one space on lines 2-n.
    desc_lines  = [s.strip() for s in description.split('\n') if s.strip()]
    return '\n'.join(desc_lines[:1] + [' %s' % s for s in desc_lines[1:]])

def fix_ownership(path):
    '''Recursively change ownership to match the source root owner:group.
    Allows non-root access to directories and files created as root using sudo.'''
    ### Change output directory and package ownership to match root directory ownership.
    diruid = os.stat(Global.source_root).st_uid
    dirgid = os.stat(Global.source_root).st_gid
    if diruid != os.getuid() or dirgid != os.getgid():
        owner_group = ':'.join((pwd.getpwuid(diruid).pw_name, grp.getgrgid(dirgid).gr_name))
        info('Setting ownership of "%s" to %s...' % (path, owner_group))
        run_cmd('chown', '-R', owner_group, path)

#### Globals

class Global:
    options     = None
    args        = None
    mydir       = os.path.dirname(os.path.realpath(__file__))
    source_root = find_source_root(os.getcwd())
    script_root = find_source_root(os.path.realpath(__file__))
    # Get the version number.
    version = None
    with open(os.path.join(script_root, 'version.txt')) as f:
        version = f.readline().strip()
    assert version is not None
    # Configure for community vs. pro editions.
    if os.path.exists(os.path.join(source_root, 'mmt.xml')):
        edition     = 'voltdb-ent'
        dist_subdir = os.path.join('obj', 'pro', '%s-%s' % (edition, version))
    else:
        edition     = 'voltdb'
        dist_subdir = os.path.join('obj', 'release', 'dist')
    # Output directory for Debian package building
    debian_output_root = os.path.join(source_root, 'obj', 'debian')
    # Property dictionary used for generating Debian control file.
    control = dict(
        pkgname     = edition,
        pkgrelease  = 1,
        arch        = 'amd64',
        provides    = 'voltdb',
        conflicts   = 'voltdb',
        replaces    = 'voltdb',
        depends     = 'default-jdk,libc6',
        priority    = 'extra',
        section     = 'database',
        maintainer  = 'VoltDB',
        pkgversion  = version,
        description = format_debian_description('''
VoltDB is a blazingly fast NewSQL database system.

It is specifically designed to run on modern scale-out architectures - fast,
inexpensive servers connected via high-speed data networks.

VoltDB is aimed at a new generation of database applications - real-time feeds,
sensor-driven data streams, micro-transactions, low-latency trading systems -
requiring database throughput that can reach millions of operations per second.
What's more, the applications that use this data must scale on demand, provide
flawless fault tolerance and enable real-time visibility into the data that
drives business value.''')
    )
    # Template for generating Debian control file.
    control_template = '''\
Package: %(pkgname)s
Priority: %(priority)s
Section: %(section)s
Installed-Size: %(installed_size)d
Maintainer: %(maintainer)s
Architecture: %(arch)s
Version: %(pkgversion)s-%(pkgrelease)d
Depends: %(depends)s
Provides: %(provides)s
Conflicts: %(conflicts)s
Replaces: %(replaces)s
Description: %(description)s
'''

#### Command class

class Command(object):
    def __init__(self, working_dir, *args):
        self.working_dir = working_dir
        self.args = args

#### Installation action class

class Action(object):

    def __init__(self, dist_dir_or_glob, dst_dir, recursive = True, link_dir = None):
        self.src_dir_or_glob = os.path.join(Global.dist_subdir, dist_dir_or_glob)
        self.dst_dir         = dst_dir
        self.recursive       = recursive
        self.link_dir        = link_dir
        # Prevent overlapping globs from installing the same thing.
        # Prioritize the first one encountered.
        self.installed = set()

    def getcmds(self):
        if os.path.isdir(self.src_dir_or_glob):
            src_dir_or_glob = os.path.join(self.src_dir_or_glob, '*')
        else:
            src_dir_or_glob = self.src_dir_or_glob
        for cmd in self.getcmds_glob(src_dir_or_glob, get_dst_path(self.dst_dir)):
            yield cmd

    def getcmds_glob(self, src_dir_or_glob, dst_dir):
        for path in glob(src_dir_or_glob):
            name = os.path.basename(path)
            dst_path = os.path.join(dst_dir, name)
            if os.path.isdir(path):
                if self.recursive:
                    for cmd in self.getcmds_glob(os.path.join(path, '*'), dst_path):
                        yield cmd
            elif path not in self.installed:
                if path.endswith('.so'):
                    opts = '-Dps'
                else:
                    opts = '-Dp'
                if Global.options.verbose:
                    opts += 'v'
                yield Command(None, 'install', opts, path, dst_path)
                if self.link_dir is not None:
                    # Create a symlink with a relative path.
                    link_dir = get_dst_path(self.link_dir)
                    link_path = os.path.join(link_dir, name)
                    if not os.path.exists(link_path):
                        opts = '-s'
                        if Global.options.verbose:
                            opts += 'v'
                        yield Command(link_dir, 'ln', opts, relpath(link_dir, dst_path), link_path)
                self.installed.add(path)

    def __str__(self):
        return ('Action: src_dir_or_glob=%(src_dir_or_glob)s '
                'dst_dir=%(dst_dir)s '
                'recursive=%(recursive)s '
                'link_dir=%(link_dir)s') % self.__dict__

#### Installation actions

# Files are only processed once, so order matters here.
actions = (
    Action('doc',           'usr/share/voltdb/doc'),
    Action('examples',      'usr/share/voltdb/examples'),
    Action('tools',         'usr/share/voltdb/tools'),
    Action('bin/*',         'usr/share/voltdb/bin', link_dir = 'usr/bin'),
    Action('lib/*',         'usr/lib/voltdb'),
    Action('voltdb/log4j*', 'usr/share/voltdb/voltdb'),
    Action('voltdb/*',      'usr/lib/voltdb'),
    Action('*',             'usr/share/voltdb', recursive = False),
)

#### Install

def install():
    ncommands = 0
    with ChDir(Global.source_root):
        info('Installing files to prefix "%s"...' % Global.options.prefix)
        for action in actions:
            debug(str(action))
            for cmd in action.getcmds():
                ncommands += 1
                if cmd.working_dir:
                    with ChDir(cmd.working_dir):
                        run_cmd(*cmd.args)
                else:
                    run_cmd(*cmd.args)
        if ncommands == 0:
            abort('Nothing was installed - you may need to do a build.')
        else:
            info('Installation is complete.')

#### Uninstall

def uninstall():
    # Remove symlinks in usr/bin from usr/share/voltdb/bin
    bin = get_dst_path('usr/share/voltdb/bin')
    opts = '-rf'
    if Global.options.verbose:
        opts += 'v'
    # Delete usr/bin symlinks.
    if os.path.isdir(bin):
        bin = get_dst_path('usr/bin')
        info('Deleting "%s" symlinks...' % bin)
        for path in glob(os.path.join(bin, '*')):
            link_path = os.path.join(bin, os.path.basename(path))
            if os.path.exists(link_path):
                run_cmd('rm', opts, link_path)
    # Recursively delete usr/share/voltdb.
    share = get_dst_path('usr/share/voltdb')
    if os.path.isdir(share):
        info('Deleting "%s"...' % share)
        run_cmd('rm', opts, share)
    # Recursively delete usr/lib/voltdb.
    lib = get_dst_path('usr/lib/voltdb')
    if os.path.isdir(lib):
        info('Deleting "%s"...' % lib)
        run_cmd('rm', opts, lib)

#### Check installation

def check():
    count = 0
    lib = get_dst_path('usr/lib/voltdb')
    if os.path.isdir(lib):
        print '  Library directory: %s' % lib
        count += 1
    else:
        print '  Library directory: missing'
    share = get_dst_path('usr/share/voltdb')
    expected_symlinks = []
    if os.path.isdir(share):
        print '   Shared directory: %s' % share
        for path in glob(os.path.join(share, 'bin', '*')):
            expected_symlinks.append(os.path.basename(path))
        missing_symlinks = []
        found_symlinks = []
        symlink_dir = get_dst_path('usr/bin')
        for name in expected_symlinks:
            symlink = os.path.join(symlink_dir, name)
            if not os.path.exists(symlink):
                missing_symlinks.append(name)
            else:
                found_symlinks.append(name)
        count += 1
        print '     Symbolic links: %s' % ' '.join(found_symlinks)
        if missing_symlinks:
            print '   Missing symlinks: (in %s) %s' % (symlink_dir, ' '.join(missing_symlinks))
        else:
            count += 1
    else:
        print '   Shared directory: missing'
        print '           Symlinks: not checked'
    if count == 0:
        print 'Installation status: uninstalled'
    elif count == 3:
        print 'Installation status: complete'
    else:
        print 'Installation status: partial', count

#### Debian package

def debian():

    # Change the working directory to the source root directory.
    with ChDir(Global.source_root):

        ### Preparation
        blddir  = os.path.join(Global.debian_output_root, 'build')
        debdir  = os.path.join(blddir, 'DEBIAN')
        Global.options.prefix = blddir
        if os.path.exists(Global.debian_output_root):
            info('Removing existing output directory "%s"...' % Global.debian_output_root)
            if not Global.options.dryrun:
                fix_ownership(Global.debian_output_root)
                shutil.rmtree(Global.debian_output_root)

        ### Installation
        install()
        # End a dry run here after displaying the installation actions.
        if Global.options.dryrun:
            return

        ### DEBIAN control file generation
        # Calculate the installation size.
        for line in pipe_cmd('du', '-s', blddir):
            installed_size = int(line.split()[0])
        if not os.path.exists(debdir):
            os.makedirs(debdir)
        # Works with an empty DEBIAN/conffiles file.
        with open(os.path.join(debdir, 'conffiles'), 'w'):
            pass
        # Perform substitutions with control_template to generate DEBIAN/control.
        # Merge local and Global symbols.
        syms = copy(locals())
        syms.update(Global.control)
        with open(os.path.join(debdir, 'control'), 'w') as fout:
            fout.write(Global.control_template % syms)

        ### Package creation
        pkgfile = os.path.join(Global.debian_output_root,
                               '%(pkgname)s_%(pkgversion)s-%(pkgrelease)d_%(arch)s.deb' % syms)
        info('Creating Debian package "%s"...' % pkgfile)
        run_cmd('dpkg-deb', '-b', blddir, pkgfile)

        ### Cleanup
        # Change to non-root ownership of package build output.
        fix_ownership(Global.debian_output_root)
        if not Global.options.keep:
            info('Wiping build directory "%s"...' % blddir)
            shutil.rmtree(blddir)

        info('Done creating Debian package: %s' % pkgfile)

#### Clean package building output

def clean():
    info('Cleaning package building output in "%s"...' % Global.debian_output_root)
    if not Global.options.dryrun:
        shutil.rmtree(Global.debian_output_root)

#### Command line main

if __name__ == '__main__':
    parser = OptionParser(description = '''\
This script can install, uninstall, validate an installation, and create
packages for VoltDB. It uses the working directory to determine the active
source tree. When creating a package it detects whether the source tree is for
the pro or the community edition and adjusts the package accordingly.''')
    parser.set_usage('%prog [OPTIONS]')
    parser.add_option('-C', '--clean', action = 'store_true', dest = 'clean',
                      help = 'clean package building output')
    parser.add_option('-c', '--check', action = 'store_true', dest = 'check',
                      help = 'check VoltDB installation')
    parser.add_option('-D', '--debian', action = 'store_true', dest = 'debian',
                      help = 'create debian package (in %s)'
                                % relpath(Global.source_root, Global.debian_output_root))
    parser.add_option('-d', '--debug', action = 'store_true', dest = 'debug',
                      help = 'display debug messages')
    parser.add_option('-i', '--install', action = 'store_true', dest = 'install',
                      help = 'install VoltDB directly (without creating package)')
    parser.add_option('-k', '--keep', action = 'store_true', dest = 'keep',
                      help = "keep (don't delete) build directory")
    parser.add_option('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                      help = 'perform dry run without executing actions')
    parser.add_option('-p', '--prefix', type = 'string', dest = 'prefix', default = '/',
                      help = 'specify prefix directory for installation target (default=/)')
    parser.add_option('-u', '--uninstall', action = 'store_true', dest = 'uninstall',
                      help = 'uninstall VoltDB')
    parser.add_option('-v', '--verbose', action = 'store_true', dest = 'verbose',
                      help = 'display verbose messages')
    (Global.options, Global.args) = parser.parse_args()
    if len(Global.args) > 0:
        error('Bad arguments to command line.')
        parser.print_help()
        abort()
    # Perform the selected actions.
    acted = False
    if Global.options.check:
        check()
        acted = True
    if Global.options.uninstall:
        uninstall()
        acted = True
    if Global.options.clean:
        clean()
        acted = True
    if Global.options.debian:
        debian()
        acted = True
    if Global.options.install:
        install()
        acted = True
    # Warn if no action was selected.
    if not acted:
        error('No action was selected')
        parser.print_help()
