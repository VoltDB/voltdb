#!/usr/bin/env python
import os, sys, subprocess
import argparse

parser = argparse.ArgumentParser(description='Build VoltDB EE Engine.')
parser.add_argument('--eetestsuite',
                    help='Which ee test suite to execute when testing.')
parser.add_argument('--build-type',
                    dest='buildtype',
                    required=True,
                    help='VoltDB build type.  One of debug, release, memcheck, memcheck_nofreelist.')
parser.add_argument('--clean-build',
                    dest='cleanbuild',
                    action='store_true',
                    help='Do a completely clean build by deleting the obj directory first.')
parser.add_argument('--source-directory',
                    dest='sourcedir',
                    required=True,
                    help='Root of VoltDB source tree.')
parser.add_argument('--build-directory',
                    dest='buildDirRoot',
                    help='''
                    Root of the VoltDB build tree.  The default is $SRCDIR/obj,
                    where $SRCDIR is the source directory.  All build artifacts will
                    be in the directory $BUILDDIR/$BUILDTYPE, where $BUILDDIR is
                    this value and $BUILDTYPE is the build type, debug, release or
                    memcheck''')
parser.add_argument('--runalltests',
                    action='store_true',
                    help='Build and run the EE unit tests.  Use valgrind for memcheck or memcheck_nofreelist.')
parser.add_argument('--build-tests',
                    dest='buildtests',
                    action='store_true',
                    help='Just build the EE unit tests.  Do not run them.')
parser.add_argument('--build',
                    action='store_true',
                    help='Just build the EE jni library.')
parser.add_argument('--build-ipc',
                    dest='buildipc',
                    action='store_true',
                    help='Just build the EE IPC jni library (used for debugging).')
parser.add_argument('--profile',
                    action='store_true',
                    help='Do profiling.')
parser.add_argument('--coverage',
                    action='store_true',
                    help='Do coverage testing.')
parser.add_argument('--debug',
                    action='store_true',
                    help='Print commands for debugging.')
parser.add_argument('--configure',
                    action='store_true',
                    help='Run CMake.')
parser.add_argument('--install',
                    action='store_true',
                    help='Install the binaries')
parser.add_argument('--generator',
                    default='makefiles',
                    help='Tool used to do builds.')
parser.add_argument('--eclipse',
                    action='store_true',
                    help='Generate an Eclipse project in the build area.')

config=parser.parse_args()
if not config.buildDirRoot:
    config.buildDirRoot = 'obj'
config.builddir = os.path.join(config.buildDirRoot, config.buildtype)
def deleteDiretory(prefix, config):
    if config.debug:
        print("Deleting directory %s" % prefix)
    else:
        subprocess.call('rm -rf %s' % prefix, shell=True)

def makeDirectory(prefix, config):
    if config.debug:
        print("Making directory %s" % prefix)
    else:
        subprocess.call('mkdir -p %s' % prefix, shell = True)

def generatorString(config):
    if config.generator == 'makefiles':
        generator='Unix Makefiles'
    elif config.generator == 'ninja':
        generator='Ninja'
    else:
        print('Unknown generator \'%s\'' % config.generator)
        sys.exit(100)
    if config.eclipse:
        generator='Eclipse CDT4 - ' + generator
    return '-G \'%s\'' % generator

def cmakeCommandString(config):
    profile = "OFF"
    coverage = "OFF"
    if config.coverage:
        coverage = "ON"
    if config.profile:
        profile = 'ON'
    return 'cmake -DVOLTDB_BUILD_TYPE=%s %s -DVOLTDB_USE_COVERAGE=%s -DVOLTDB_USE_PROFILING=%s %s' \
            % (config.buildtype, generatorString(config), coverage, profile, config.sourcedir)
def makeCommandString(config):
    target=''
    if config.buildtests:
        target += ' build-tests'
    if config.buildipc:
        target += ' voltdbipc'
    if config.runalltests:
        target += ' runalltests'
    if config.install:
        target += ' install'
    if len(target) > 0:
        if config.generator == 'makefiles':
            make_cmd = 'make -j -k'
        elif config.generator == 'ninja':
            make_cmd = 'ninja'
            return ("%s %s" % (make_cmd, target))
    else:
        return ""

def runCommand(commandStr, config):
    if config.debug:
        print(commandStr)
    else:
        subprocess.call(commandStr, shell = True)
#
# The prefix must either not exist or else
# be a directory.  If this is a clean build we
# delete the existing directory.
#
if os.path.exists(config.builddir):
    if not os.path.isdir(config.builddir):
        print('build.py: \'%s\' exists but is not a directory.' % config.builddir)
        sys.exit(100)
    if config.cleanbuild:
        deleteDiretory(config.builddir, config)

if not os.path.exists(config.builddir):
    makeDirectory(config.builddir, config)

if config.debug:
    print('Changing to directory %s' % config.builddir)
else:
    os.chdir(config.builddir)
#
# If we have not already configured, we want to reconfigure.
#
if not os.path.exists('CMakeCache.txt'):
    runCommand(cmakeCommandString(config), config)
makeCommand=makeCommandString(config)
if makeCommand:
    runCommand(makeCommand, config)

