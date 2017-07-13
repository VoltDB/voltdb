#!/usr/bin/env python
import os, sys, subprocess
import argparse
import re
import multiprocessing

parser = argparse.ArgumentParser(description='Build VoltDB EE Engine.')
#
# Build configuration.
#
parser.add_argument('--debug',
                    action='store_true',
                    help='''
                    Print commands for debugging.  Don't execute anything.
                    ''')
parser.add_argument('--build-type',
                    dest='buildtype',
                    default='release',
                    help='''
                    VoltDB build type.  One of debug, release, memcheck, memcheck_nofreelist.
                    The default is release.''')
parser.add_argument('--profile',
                    action='store_true',
                    help='''
                    Configure for profiling.''')
parser.add_argument('--coverage',
                    action='store_true',
                    help='''
                    Configure for coverage testing.''')
parser.add_argument('--generator',
                    default='Unix Makefiles',
                    help='''
                    Name the tool used to do builds.  Currently only 'Unix Makefiles' is supported,
                    and this is the default.  The other choices are 'Ninja', 'Eclipse CDT4 - Unix Makefiles'
                    and 'Eclipse CDT4 - Ninja'.  These choose the ninja build system, and will also
                    create an Eclipse CDT4 project in the build area.''')
#
# Build parameters.
#
parser.add_argument('--source-directory',
                    dest='sourcedir',
                    default=os.getcwd(),
                    help='''
                    Root of Voltdb source tree.''')
parser.add_argument('--max-processors',
                    dest='max_processors',
                    type=int,
                    default=8,
                    help='''
                    Maximum number of processors used.  Default is 8.  In any case,
                    we use at most the number of hardware processors.
                    ''')
#
# Build Steps.
#
parser.add_argument('--clean-build',
                    dest='cleanbuild',
                    action='store_true',
                    help='''
                    Do a completely clean build by deleting the obj directory first.''')
parser.add_argument('--configure',
                    action='store_true',
                    help='''
                    Configure the build but do not do any building.''')
parser.add_argument('--build',
                    action='store_true',
                    help='''
                    Just build the EE jni library.''')
parser.add_argument('--build-ipc',
                    dest='buildipc',
                    action='store_true',
                    help='''
                    Just build the EE IPC jni library (used for debugging).''')
parser.add_argument('--build-tests',
                    dest='buildtests',
                    action='store_true',
                    help='''
                    Just build the EE unit tests.  Do not run them.  This implies --configure and --build-ipc.''')
parser.add_argument('--install',
                    action='store_true',
                    help='''
                    Install the binaries''')
#
# Testing.
#
parser.add_argument('--run-all-tests',
                    dest='runalltests',
                    action='store_true',
                    help='''
                    Build and run the EE unit tests.  Use valgrind for memcheck or memcheck_nofreelist.
                    This implies --build-tests.  See --eetestsuite as well.''')
parser.add_argument('--eetestsuite',
                    default=None,
                    help='''
                    Which ee test suite to execute when testing.  The default is to run all the tests.
                    ''')

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

def makeGeneratorCommand(config):
    np = getNumberProcessors(config)
    if config.generator.endswith('Unix Makefiles'):
        return "make -j%d " % np
    elif config.generator.endswith('Ninja'):
        return "ninja -j %d" % np
    else:
        print('Unknown generator \'%s\'' % config.generator)

def cmakeCommandString(config):
    profile = "OFF"
    coverage = "OFF"
    if config.coverage:
        coverage = "ON"
    if config.profile:
        profile = 'ON'
    return 'cmake -DVOLTDB_BUILD_TYPE=%s -G \'%s\' -DVOLTDB_USE_COVERAGE=%s -DVOLTDB_USE_PROFILING=%s %s' \
             % (config.buildtype, config.generator, coverage, profile, config.sourcedir)

def getNumberProcessors(config):
    np = multiprocessing.cpu_count()
    if np < 1:
        np = 1
    if config.max_processors < np:
        np = config.max_processors
    return np

def makeCommandString(config):
    target=''
    cmdstr = None
    if config.build:
        target += ' build'
    if config.install:
        target += ' install'
    if config.buildtests:
        target += ' build-tests'
    if config.buildipc:
        target += ' voltdbipc'
    if config.runalltests:
        target += ' build-tests'
    if len(target) > 0:
        cmdstr = "%s %s" % (makeGeneratorCommand(config), target)
    return cmdstr

def runCommand(commandStr, config):
    if config.debug:
        print(commandStr)
        return True
    else:
        retcode = subprocess.call(commandStr, shell = True)
        return (retcode == 0)
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
    configCmd = cmakeCommandString(config)
    if not runCommand(configCmd, config):
        print("Cmake command \"%s\" failed." % configCmd)
        sys.exit(100)

#
# Do the actual build.
#
buildCmd = makeCommandString(config)
if buildCmd:
    if not runCommand(buildCmd, config):
        print("Build command \"%s\" failed." % buildCmd)
        sys.exit(100)

#
# Do testing if requested.
#
if config.runalltests:
    testCmd = "ctest --output-on-failure -j %d" % getNumberProcessors(config)
    if config.eetestsuite:
        testCmd += " -R %s" % config.eetestsuite
    if not runCommand(testCmd, config):
        print("Test command \"%s\" failed." % testCmd)
        sys.exit(100)

print("Build success.")
sys.exit(0)

