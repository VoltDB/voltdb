#!/usr/bin/env python
# This file is part of VoltDB.

# Copyright (C) 2008-2015 VoltDB Inc.
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
from zipfile import ZipFile
import tempfile
import shutil
import subprocess
import pwd
import grp
import re
from copy import copy
import string

myname = os.path.splitext(os.path.basename(__file__))[0]
mydir  = os.path.dirname(os.path.realpath(__file__))

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
                    t = msg + ' '   # Raises TypeError if not a string
                    for msg2 in msg.split('\n'):
                        f.write('%s%s\n' % (stag, msg2))
                except TypeError:
                    # Handle mult-string iterators
                    if hasattr(msg, '__iter__'):
                        for msg2 in msg:
                            f.write('%s   %s\n' % (stag, msg2))
                    else:
                        f.write('%s%s\n' % (stag, str(msg)))

def info(*msgs):
    _message(sys.stdout, 'INFO', *msgs)

def debug(*msgs):
    if meta is not None and meta.options.debug:
        _message(sys.stdout, 'DEBUG', *msgs)

def warning(*msgs):
    _message(sys.stdout, 'WARNING', *msgs)

def error(*msgs):
    _message(sys.stderr, 'ERROR', *msgs)

def abort(*msgs):
    error(*msgs)
    _message(sys.stderr, 'FATAL', 'Giving up.')
    sys.exit(1)

def get_dst_path(*dirs):
    return os.path.join(meta.options.prefix, *dirs)

def run_cmd(cmd, *args):
    fullcmd = cmd
    for arg in args:
        if len(arg.split()) > 1:
            fullcmd += ' "%s"' % arg
        else:
            fullcmd += ' %s' % arg
    if meta.options.dryrun:
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

def find_volt_root(path):
    '''Find the Volt root directory based on specified path.'''
    if os.path.isdir(path):
        dir = path
    else:
        dir = os.path.dirname(path)
    while dir != '/':
        if (glob(os.path.join(dir, 'README.thirdparty*'))):
            return dir
        dir = os.path.dirname(dir)
    abort('Volt root directory not found starting from "%s".' % path)

def format_debian_description(description):
    '''Reformat the control file description for Debian compatibility.'''
    # Strip blank lines and add one space on lines 2-n.
    desc_lines  = [s.strip() for s in description.split('\n') if s.strip()]
    return '\n'.join(desc_lines[:1] + [' %s' % s for s in desc_lines[1:]])

def fix_ownership(path):
    '''Recursively change ownership to match the source tree owner:group to
    allow non-root access to directories/files created as root using sudo.'''
    ### Change output directory and package ownership to match root directory ownership.
    diruid = os.stat(meta.volt_root).st_uid
    dirgid = os.stat(meta.volt_root).st_gid
    if diruid != os.getuid() or dirgid != os.getgid():
        owner_group = ':'.join((pwd.getpwuid(diruid).pw_name, grp.getgrgid(dirgid).gr_name))
        info('Setting ownership of "%s" to %s...' % (path, owner_group))
        run_cmd('chown', '-R', owner_group, path)

#### Metadata

class Metadata:
    def __init__(self, options, args):
        self.volt_root   = None
        self.options     = options
        self.args        = args
        self.re_volt_jar = re.compile('^voltdb(client)?-[.0-9]+[.]([\w]+\.)*jar$')
    def initialize(self, version, volt_root, build_root, output_root, clean_up_items):
        self.version         = version
        self.volt_root       = volt_root
        self.build_root  = build_root
        self.output_root = output_root
        self.clean_up_items  = clean_up_items
        # Detect and configure for community vs. pro edition.
        if os.path.exists(os.path.join(self.volt_root, 'README.thirdparty.ent')):
            self.edition = 'voltdb-ent'
            self.summary = 'VoltDB In-Memory Database Enterprise Edition'
            self.license = 'Proprietary'
            src_dist_subdir = os.path.join('obj', 'pro', '%s-%s' % (self.edition, self.version))
        else:
            self.edition = 'voltdb'
            self.summary = 'VoltDB In-Memory Database Community Edition'
            self.license = 'AGPL'
            src_dist_subdir = os.path.join('obj', 'release', 'dist')
        # Detect and configure for distribution vs. source tree.
        if os.path.isdir(os.path.join(self.volt_root, 'obj')):
            self.dist_subdir = src_dist_subdir
        else:
            self.dist_subdir = ''
        # Property dictionary used for generating Debian control file.
        self.deb_control = dict(
            pkgname     = self.edition,
            pkgrelease  = 1,
            arch        = 'amd64',
            provides    = 'voltdb',
            conflicts   = 'voltdb',
            replaces    = 'voltdb',
            depends     = 'openjdk-7-jdk,libc6',
            priority    = 'extra',
            section     = 'database',
            maintainer  = 'VoltDB',
            pkgversion  = self.version,
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
        self.deb_control_template = '''\
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
        """
        Developer Notes: so much of voltdb_install.py is hardwired to install voltdb into a tree like {prefix}/voltdb/...
        In the future it seems desireable to change this to something like {prefix}/{edition}/...
        Doing so would eliminate the split between pkgname and voltdbdir
        Also, link /usr/lib/voltdb... to directory /usr/share/voltdb/lib
        """
        self.rpm_control = dict(
            pkgname     = self.edition,
            pkgversion  = self.version,
            pkgrelease  = 1,
            arch        = 'x86_64',
            provides    = self.edition,
            conflicts   = self.edition,
            summary     = self.summary,
            license     = self.license,
            maintainer  = 'VoltDB',
            prefix      = '/usr',
            voltdbdir = 'voltdb'
            )

        self.rpm_control_template = '''\
%%define        __spec_install_post %%{nil}
%%define          debug_package %%{nil}
%%define        voltdbdir %(voltdbdir)s

Summary: %(summary)s
Name: %(pkgname)s
Version: %(pkgversion)s
Release: %(pkgrelease)d
License: %(license)s
Group: Applications/Databases
Distribution: .el6
SOURCE0 : %%{name}-%%{version}.tar.gz
Vendor: %(maintainer)s
URL: http://www.voltdb.com
Provides: %(provides)s
Conflicts: %(conflicts)s
Requires: libgcc >= 4.1.2, libstdc++ >= 4.1.2, python >= 2.6
Requires: java >= 1:1.7.0
Requires: java-devel >= 1:1.7.0
Summary: VoltDB is a blazingly fast in memory (IMDB) NewSQL database system.
Prefix: %(prefix)s

# this var is ignored by el6 rpmbuild (but not el5 rpmbuild)
# el6 rpm uses _buildroot in .rpmmacro to do the same thing
BuildRoot: %%{_buildrootdir}/%%{name}-%%{version}-%%{release}.%%{_arch}

%%description
VoltDB is a blazingly fast in memory (IMDB) NewSQL database system.

It is specifically designed to run on modern scale-out architectures - fast,
inexpensive servers connected via high-speed data networks.

VoltDB is aimed at a new generation of database applications - real-time feeds,
sensor-driven data streams, micro-transactions, low-latency trading systems -
requiring database throughput that can reach millions of operations per second.
What's more, the applications that use this data must scale on demand, provide
flawless fault tolerance and enable real-time visibility into the data that
drives business value.

%%prep
%%setup -q

cat << \EOF > %%{name}-req
#!/bin/sh
%%{__perl_requires} $* |\
sed -e '/perl(.*)/d'
EOF

%%global __perl_requires %%{_builddir}/%%{name}-%%{version}/%%{name}-req
chmod +x %%{__perl_requires}

%%build

%%install

%%clean

%%files
%%defattr(-,root,root,-)
%%include myfiles

%%post
echo "To make a copy of the VoltDB sample programs run the command: cp -r %%{prefix}/share/%%{voltdbdir}/examples <your-new-examples-directory>"
echo "Thanks for installing VoltDB!"

%%postun
# remove voltdb directory tree
if [ -n "%%{name}" ]; then
    rm -rf %%{prefix}/lib/%%{voltdbdir}
    rm -rf %%{prefix}/share/%%{voltdbdir}
fi

%%changelog
* Mon Nov 11 2013  Phil Rosegay <support@voltdb.com> 4.0-1
- GA-4.0
* Fri Jan 14 2013  Phil Rosegay <support@voltdb.com> 3.0-1
- GA-3.0
'''

# Set in main
meta = None

#### Command class

class Command(object):
    def __init__(self, working_dir, *args):
        self.working_dir = working_dir
        self.args = args

#### Installation action class

class Action(object):

    def __init__(self, dist_dir_or_glob, dst_dir, recursive = True, link_dir = None):
        self.dist_dir_or_glob = dist_dir_or_glob
        self.dst_dir          = dst_dir
        self.recursive        = recursive
        self.link_dir         = link_dir
        # Prevent overlapping globs from installing the same thing.
        # Prioritize the first one encountered.
        self.installed = set()

    def getcmds(self):
        full_dist_dir_or_glob = os.path.join(meta.dist_subdir, self.dist_dir_or_glob)
        if os.path.isdir(full_dist_dir_or_glob):
            dist_dir_or_glob = os.path.join(full_dist_dir_or_glob, '*')
        else:
            dist_dir_or_glob = full_dist_dir_or_glob
        for cmd in self.getcmds_glob(dist_dir_or_glob, get_dst_path(self.dst_dir)):
            yield cmd

    def getcmds_glob(self, dist_dir_or_glob, dst_dir):
        for path in glob(dist_dir_or_glob):
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
                if meta.options.verbose:
                    opts += 'v'
                yield Command(None, 'install', opts, path, dst_path)
                if self.link_dir is not None:
                    # Create a symlink with a relative path.
                    link_dir = get_dst_path(self.link_dir)
                    link_path = os.path.join(link_dir, name)
                    if not os.path.exists(link_path):
                        opts = '-s'
                        if meta.options.verbose:
                            opts += 'v'
                        yield Command(link_dir, 'ln', opts, relpath(link_dir, dst_path), link_path)
                self.installed.add(path)

    def __str__(self):
        return ('Action: dist_dir_or_glob=%(dist_dir_or_glob)s '
                'dst_dir=%(dst_dir)s '
                'recursive=%(recursive)s '
                'link_dir=%(link_dir)s') % self.__dict__

#### Installation actions

# Files are only processed once. Action order matters.
actions = (
    Action('doc',                     'usr/share/voltdb/doc'),
    Action('examples',                'usr/share/voltdb/examples'),
    Action('tools',                   'usr/share/voltdb/tools'),
    Action('bin/*',                   'usr/share/voltdb/bin', link_dir = 'usr/bin'),
    Action('lib/*',                   'usr/lib/voltdb'),
    Action('management/*.jar',        'usr/lib/voltdb'),
    Action('management/*.sh',         'usr/share/voltdb/management'),
    Action('management/*.xml',        'usr/share/voltdb/management'),
    Action('management/*.properties', 'usr/share/voltdb/management'),
    Action('third_party/python',      'usr/share/voltdb/third_party/python'),
    Action('voltdb/log4j*',           'usr/share/voltdb/voltdb'),
    Action('voltdb/*',                'usr/lib/voltdb'),
    Action('*',                       'usr/share/voltdb', recursive = False),
)

#### Install

def install():
    ncommands = 0
    with ChDir(meta.volt_root):
        info('Installing files from "%s" to prefix "%s"...' % (meta.volt_root, meta.options.prefix))
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
    if meta.options.verbose:
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
    with ChDir(meta.volt_root):

        ### Preparation
        blddir = os.path.join(meta.build_root, 'build')
        debdir = os.path.join(blddir, 'DEBIAN')
        meta.options.prefix = blddir
        if os.path.exists(meta.build_root):
            info('Removing existing output directory "%s"...' % meta.build_root)
            if not meta.options.dryrun:
                fix_ownership(meta.build_root)
                shutil.rmtree(meta.build_root)

        ### Installation
        install()
        # End a dry run here after displaying the installation actions.
        if meta.options.dryrun:
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
        # Merge local and global symbols.
        syms = copy(locals())
        syms.update(meta.deb_control)
        with open(os.path.join(debdir, 'control'), 'w') as fout:
            fout.write(meta.deb_control_template % syms)

        ### Package creation
        pkgfile = os.path.join(meta.output_root,
                               '%(pkgname)s_%(pkgversion)s-%(pkgrelease)d_%(arch)s.deb' % syms)
        info('Creating Debian package "%s"...' % pkgfile)
        run_cmd('dpkg-deb', '-b', blddir, pkgfile)

        ### Cleanup
        # Change to non-root ownership of package build output.
        fix_ownership(meta.build_root)
        if not meta.options.keep:
            info('Wiping build directory "%s"...' % blddir)
            shutil.rmtree(blddir)

        info('Done creating Debian package: %s' % pkgfile)

def rpm():

    blddir = os.path.join(meta.build_root, 'rpmbuild')

    if os.path.exists(meta.build_root):
        info('Removing existing output directory "%s"...' % meta.build_root)
        if not meta.options.dryrun:
            fix_ownership(meta.build_root)
            shutil.rmtree(meta.build_root)

    # setup the rpmbuild working tree
    for D in ["BUILD","SOURCES","RPMS","SPECS","SRPMS", "BUILDROOT"]:
        p = os.path.join(blddir, D)
        if not os.path.exists(p):
            os.makedirs(os.path.join(blddir, D))

    with open(os.path.join(os.environ["HOME"], ".rpmmacros"), 'w') as fout:
        fout.write("%%_topdir\t%s\n" % blddir)
        fout.write("%_buildrootdir\t%{_topdir}/BUILDROOT\n")
        fout.write("%_buildroot\t%{_buildrootdir}/%{name}-%{version}-%{release}.%{_arch}\n")

    # assemble the SPEC file
    syms = copy(locals())
    syms.update(meta.rpm_control)
    with open(os.path.join(blddir, 'SPECS', 'voltdb.spec'), 'w') as fout:
        fout.write(meta.rpm_control_template % syms)

    voltdb_dist = "%(pkgname)s-%(pkgversion)s" % syms
    voltdb_build = voltdb_dist + "-%(pkgrelease)d.%(arch)s" % syms

    # stage the voltdb distribution files where rpmbuild needs them
    buildroot = os.path.join(blddir, "BUILDROOT", voltdb_build)
    meta.options.prefix = buildroot
    install()

    # make a list of the files that rpmbuild will package
    # only FILES that go into the rpm should be listed
    # watch out for spaces and special characters in the filenames, hence the quotes
    # also spit out command lists for the %post and %preun sections
    # that will link/unlink the binaries from /usr/bin
    with ChDir(buildroot):
        files = []
        for l in pipe_cmd("find", ".", "-not", "-type", "d"):
            files.append(string.lstrip(l, '.'))

        links = []
        for l in pipe_cmd("find", ".", "-type", "l"):
            print l
            links.append(string.lstrip(l, '.'))

    with open(os.path.join(blddir, "SPECS", "myfiles"), 'w') as fout:
        for f in files:
            fout.write("\"%s\"\n" % f)

    with open(os.path.join(blddir, "SPECS", "preuncmd"), 'w') as uout:
        for l in links:
            uout.write("unlink %s\n" % l)

    # make an empty SOURCE tarball to satisfy rpmbuild's need to build something from source
    rpm_sources = os.path.join(blddir, "SOURCES")
    os.mkdir(os.path.join(rpm_sources, voltdb_dist))
    with ChDir(rpm_sources):
        run_cmd("tar", "-cf", voltdb_dist+".tar.gz", voltdb_dist)
        os.rmdir(voltdb_dist)

    # build the rpm
    with ChDir(os.path.join(blddir, "SPECS")):
        run_cmd("rpmbuild", "-bb", "voltdb.spec")

    # snag our new package
    shutil.copy(os.path.join(blddir, "RPMS", "x86_64",
            '%(pkgname)s-%(pkgversion)s-%(pkgrelease)d.%(arch)s.rpm' % syms),
                 meta.output_root)

#### Clean package building output

def clean():
    info('Cleaning package building output in "%s"...' % meta.build_root)
    if not meta.options.dryrun:
        shutil.rmtree(meta.build_root)

#### Extract distribution tarball

def extract_distribution(tarball):
    if not os.path.isfile(tarball):
        abort('Distribution file "%s" does not exist.' % tarball)
    if not tarball.endswith('.tar.gz'):
        abort('Distribution file "%s" does not have a "tar.gz" extension.' % tarball)
    full_path = os.path.realpath(tarball)
    tmpdir = tempfile.mkdtemp(prefix = '%s_' % myname, suffix = '_tmp')
    with ChDir(tmpdir):
        info('Extracting distribution tarball to "%s"...' % tmpdir)
        retcode = os.system('tar xfz "%s"' % full_path)
        if retcode != 0:
            abort('Failed to extract distribution tarball "%s" with return code %d.' %
                        (full_path, retcode))
        # Expect it to have a single subdirectory with the distribution.
        subdirs = [d for d in glob('voltdb*') if os.path.isdir(d)]
        if len(subdirs) == 0:
            abort('Did not find a voltdb* subdirectory in the distribution tarball.')
        if len(subdirs) > 1:
            abort('Found %d voltdb* subdirectories in the distribution tarball.' % len(subdirs))
    return tmpdir, os.path.realpath(os.path.join(tmpdir, subdirs[0]))

#### Get version number from a distribution directory

def get_distribution_version(dist_dir):
    glob_pat = os.path.join(dist_dir, 'voltdb', 'voltdb-*.jar')
    jars = [jar for jar in glob(glob_pat)
                if meta.re_volt_jar.match(os.path.basename(jar))]
    if len(jars) == 0:
        abort('Could not find "%s" matching pattern "%s".'
                    % (glob_pat, re_volt_jar.pattern))
    if len(jars) > 1:
        abort('Found more than one "%s" matching pattern "%s".'
                    % (glob_pat, re_volt_jar.pattern))
    # Read buildstring.txt from the jar file.
    version = None
    try:
        info('Reading buildstring.txt from "%s"...' % jars[0])
        # see http://bugs.python.org/issue5511 Zipfile() with "with" fixed in python 2.7
        # changed so it will work with python2.6 only for convenience
        zip = ZipFile(jars[0])
        f = zip.open('buildstring.txt')
        version = f.readline().strip().split()[0]
        assert version is not None
    except (IOError, OSError, KeyError), e:
        abort('Error reading buildstring.txt from "%s".' % jars[0], e)
    finally:
        f.close()
    return version

#### Get the version number from a source tree

def get_source_version(source_root):
    try:
        with open(os.path.join(source_root, 'version.txt')) as f:
            version = f.readline().strip()
    except (IOError, OSError), e:
        abort('Error reading version.txt from "%s".' % source_root, e)
    assert version is not None
    return version

#### Get the output root directory, e.g. for produced packages

def get_output_root():
    if not meta.options.output:
        return os.getcwd()
    output_root = meta.options.output
    if not os.path.exists(output_root):
        try:
            os.makedirs(output_root)
        except (IOError, OSError), e:
            abort('Error creating output directory "%s".' % output_root, e)
    return output_root

#### Command line main

if __name__ == '__main__':

    # Set up command line interface.
    parser = OptionParser(description = '''\
This script can install, uninstall, validate an installation, and create
packages for VoltDB. It uses the working directory to determine the active
source tree. When creating a package it detects whether the source tree is for
the pro or the community edition and adjusts the package accordingly. If a
distribution tarball is provided this script uses the distribution tree from
that tarball instead of the working directory.''')
    parser.set_usage('%prog [OPTIONS] [DISTRIBUTION_TARBALL]')
    parser.add_option('-C', '--clean', action = 'store_true', dest = 'clean',
                      help = 'clean package building output')
    parser.add_option('-c', '--check', action = 'store_true', dest = 'check',
                      help = 'check VoltDB installation')
    parser.add_option('-D', '--debian', action = 'store_true', dest = 'debian',
                      help = 'create debian package')
    parser.add_option('-d', '--debug', action = 'store_true', dest = 'debug',
                      help = 'display debug messages')
    parser.add_option('-i', '--install', action = 'store_true', dest = 'install',
                      help = 'install VoltDB directly (without creating package)')
    parser.add_option('-k', '--keep', action = 'store_true', dest = 'keep',
                      help = "keep (don't delete) temporary directories")
    parser.add_option('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                      help = 'perform dry run without executing actions')
    parser.add_option('-o', '--output', type = 'string', dest = 'output',
                      help = 'create package in the specified output directory')
    parser.add_option('-p', '--prefix', type = 'string', dest = 'prefix', default = '/',
                      help = 'specify prefix directory for installation target (default=/)')
    parser.add_option('-R', '--rpm', action = 'store_true', dest = 'rpm',
                      help = 'create rpm package')
    parser.add_option('-u', '--uninstall', action = 'store_true', dest = 'uninstall',
                      help = 'uninstall VoltDB')
    parser.add_option('-v', '--verbose', action = 'store_true', dest = 'verbose',
                      help = 'display verbose messages')
    (options, args) = parser.parse_args()
    if len(args) > 1:
        error('Bad arguments to command line.')
        parser.print_help()
        abort()

    # Metadata initialization is in two phases so that options can be in effect
    # before running commands to initialize for the distribution.
    # A single argument optionally specifies a distribution tarball. If not
    # present the script assumes it's running from inside of a source or
    # distribution directory tree.
    # Configure debian package building as needed.
    meta = Metadata(options, args)
    clean_up_items = []
    version = None
    if len(args) == 1:
        tmpdir, volt_root = extract_distribution(args[0])
        output_root = get_output_root()
        version = get_distribution_version(volt_root)
        clean_up_items.append(tmpdir)
        if meta.options.debian:
            build_root  = tempfile.mkdtemp(prefix = '%s_' % myname, suffix = '_deb')
            clean_up_items.append(build_root)
        elif meta.options.rpm:
            build_root  = tempfile.mkdtemp(prefix = '%s_' % myname, suffix = '_rpmbuild')
            clean_up_items.append(build_root)
        else:
            build_root = None
    else:
        source_root  = find_volt_root(os.path.realpath(__file__))
        version      = get_source_version(source_root)
        volt_root    = find_volt_root(os.getcwd())
        if meta.options.debian:
            build_root   = os.path.join(volt_root, 'obj', 'debian')
        elif meta.options.rpm:
            build_root   = os.path.join(volt_root, 'obj', 'rpm')
        output_root  = build_root
    meta.initialize(version, volt_root, build_root, output_root, clean_up_items)

    # Perform selected actions.
    try:
        acted = False
        if meta.options.check:
            check()
            acted = True
        if meta.options.uninstall:
            uninstall()
            acted = True
        if meta.options.clean:
            clean()
            acted = True
        if meta.options.debian:
            debian()
            acted = True
        if meta.options.rpm:
            rpm()
            acted = True
        if meta.options.install:
            install()
            acted = True
        # Warn if no action was selected.
        if not acted:
            error('No action was selected')
            parser.print_help()

    # Clean up temporary files and directories.
    finally:
        if not meta.options.keep:
            for clean_up_item in meta.clean_up_items:
                info('Delete temporary: %s' % clean_up_item)
                if os.path.exists(clean_up_item):
                    shutil.rmtree(clean_up_item)
