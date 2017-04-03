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
parser.add_argument('--source-dir',
                    dest='sourcedir',
                    required=True,
                    help='Root of Voltdb source tree.')
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
parser.add_argument('--configure',
                    action='store_true',
                    help='Configure the build but do not do any building.')
parser.add_argument('--coverage',
                    action='store_true',
                    help='Do coverage testing.')
parser.add_argument('--debug',
                    action='store_true',
                    help='Print commands for debugging.')
parser.add_argument('--install',
                    action='store_true',
                    help='Install the binaries')
parser.add_argument('--generator',
                    default='Unix Makefiles',
                    help='Tool used to do builds.')

config=parser.parse_args()
prefix=os.path.join('obj', config.buildtype)

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

def cmakeCommandString(config):
    profile = "OFF"
    coverage = "OFF"
    if config.coverage:
        coverage = "ON"
    if config.profile:
        profile = 'ON'
    return 'cmake -DVOLTDB_BUILD_TYPE=%s -G \'%s\' -DVOLTDB_USE_COVERAGE=%s -DVOLTDB_USE_PROFILING=%s %s' \
            % (config.buildtype, config.generator, coverage, profile, config.sourcedir)
def makeCommandString(config):
    target='build'
    if config.buildtests:
        target += ' build-tests'
    if config.buildipc:
        target += ' voltdbipc'
    if config.runalltests:
        target += ' runalltests'
    return ("make -j %s" % target)

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
if os.path.exists(prefix):
    if not os.path.isdir(prefix):
        print('build.py: \'%s\' exists but is not a directory.' % prefix)
        sys.exit(100)
    if config.cleanbuild:
        deleteDiretory(prefix, config)

if not os.path.exists(prefix):
    makeDirectory(prefix, config)

if config.debug:
    print('Changing to directory %s' % prefix)
else:
    os.chdir(prefix)
#
# If we have not already configured, we want to reconfigure.
#
if not os.path.exists('CMakeCache.txt'):
    runCommand(cmakeCommandString(config), config)
runCommand(makeCommandString(config), config)
