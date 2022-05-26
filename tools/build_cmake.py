#!/usr/bin/env python3
# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
import os, sys, subprocess
import argparse
import re
import multiprocessing

########################################################################
#
# Make the command line parser.
#
########################################################################
def makeParser():
    class OnOffAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, 'ON' if values == 'true' else 'OFF')

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
    parser.add_argument('--show-test-output',
                        action='store_true',
                        dest='showtestoutput',
                        default=False,
                        help='''
                        By default tests are run with test output shown only for failing tests.
                        If this option is included then all test output will be shown, even if
                        the tests pass.
                        ''')
    parser.add_argument('--verbose-config',
                        default='no',
                        help='''
                        Use verbose builds when building.
                        ''')
    parser.add_argument('--verbose-build',
                        default='no',
                        help='''
                        Use verbose builds when building.
                        ''')
    parser.add_argument('--log-level',
                        default='500',
                        help='''
                        Set the log level.  The default is 500.
                        ''')
    parser.add_argument('--pool-checking',
                        action=OnOffAction,
                        default='OFF',
                        help='''
                        Turns on conditionally compiled code to verify usage of memory
                        pools in the EE.''')
    parser.add_argument('--trace-pools',
                        action=OnOffAction,
                        default='OFF',
                        help='''
                        Turns on conditionally compiled code to save stack traces for memory
                        pool allocations in the EE.''')
    parser.add_argument('--timer-enabled',
                        action=OnOffAction,
                        default='OFF',
                        help='''
                        Turns on conditionally compiled code to enable timers in the EE.''')

    #
    # Build parameters.
    #
    parser.add_argument('--source-directory',
                        dest='srcdir',
                        metavar='SOURCE_DIR',
                        help='''
                        Root of VoltDB EE source tree.
                        ''')
    parser.add_argument('--object-directory',
                        dest='objdir',
                        default=None,
                        metavar='OBJECT_DIR',
                        help='''
                        Root of the object directory.  This is typically S/obj/BT,
                        where S is the source directory for all of VoltDB and BT
                        is the build type.<
                        ''')
    parser.add_argument('--max-processors',
                        dest='max_processors',
                        type=int,
                        default=-1,
                        help='''
                        Specify the maximum number of processors.  By default we use
                        all cores, and we will never use more than the number of cores.
                        But if this number is less than the number of cores we will
                        use this number.  If this number is not specified and we cannot
                        determine the number of cores we will use 1.
                        ''')
    ####################################################################
    #
    # Build Steps.
    #
    # The steps are:
    #   1.) --clean
    #       Do a clean before doing anything else.  This deletes the
    #       entire object directory.
    #   2.) --build
    #           Build the VoltDB shared object.  This builds all the dependences,
    #           compiles all the files and links them into a shared library.
    #   3.) Building Tests
    #       3a.) --build-one-test=test or --build-one-testdir=testdir
    #           Build one EE unit test or else build all the tests in a given
    #           test directory.
    #       3b.) --build-tests
    #           This builds all the tests.
    #   4.) Running Tests
    #       4a.) --run-one-test=test or --run-one-testdir=testdir
    #            Run one test or all the tests in the given test directory.
    #       4b.) --run-all-tests
    #           This runs all the tests.  The tests are run concurrently.  The
    #           only output shown is failing output unless --show-test-output has
    #           been specified.  Note that this will run valgrind as well if
    #           the build type is memcheck.
    #
    ####################################################################
    parser.add_argument('--clean',
                        dest='cleanbuild',
                        action='store_true',
                        help='''
                        Do a completely clean build by deleting the obj directory first.''')
    parser.add_argument('--build-all-tests',
                        dest='buildalltests',
                        action='store_true',
                        help='''
                        Just build the EE unit tests.  Do not run them.  This implies --build.
                        This is incompatible with --build-one-test and --build-one-testdir.
                        ''')
    parser.add_argument('--build-one-test',
                        dest='buildonetest',
                        metavar='TEST',
                        help='''
                        Build only one EE unit test.  This is incompatible with
                        --build-all-tests and build-one-testdir.  This implies
                        --build.
                        ''')
    parser.add_argument('--build-one-testdir',
                        dest='buildonetestdir',
                        metavar='TESTDIR',
                        help='''
                        Build all tests in TESTDIR.  This is incompatible with
                        --build-all-tests and --build-one-test.  This implies
                        --build.
                        ''')
    #
    # Installation
    #
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
                        Build and run the EE unit tests.  Use valgrind for
                        memcheck or memcheck_nofreelist.
                        This implies --build-all-tests. This is mutually
                        incompatible with --run-one-test and --run-one-testdir.  This
                        implies --build-all-tests.
                        ''')
    parser.add_argument('--run-one-test',
                        dest='runonetest',
                        metavar='TEST',
                        help='''
                        Run one test.  This is mutually incompatible with --run-all-tests
                        and --run-one-testdir.  This implies --build-one-test=TEST.
                        ''')
    parser.add_argument('--run-one-testdir',
                        dest='runonetestdir',
                        metavar='TESTDIR',
                        help='''
                        Run all tests in TESTDIR.  This is
                        mutually incompatible with --run-all-tests and --run-one-test.
                        This implies --build-one-testdir=TESTDIR.
                        ''')
    return parser

########################################################################
#
# Delete a directory.  On failure exit with a non-zero status.
#
########################################################################
def deleteDirectory(dirname, config):
    if config.debug:
        print("Deleting directory %s" % dirname)
    else:
        subprocess.call('rm -rf %s' % dirname, shell=True)

########################################################################
#
# Get the number of cores we will use.  This is the min of
# the number of cores actually available and the number of
# cores specified in the parameters.  If none are specified
# in the parameters we use all available. If we cannot
# determine how many are available we use 1.
#
########################################################################
def getNumberProcessors(config):
    # np is the number of cores to use.
    np = multiprocessing.cpu_count()
    if np < 1:
        np = 1
        if 0 < config.max_processors:
            # We can't find out (np < 1) but the user has
            # given us a number (0 < config.max_processors).
            # Use the user's number.
            np = config.max_processors
    elif 1 <= config.max_processors and config.max_processors < np:
        # If we have a core count but the user gave us one
        # which is smaller then use the user's number.
        np = config.max_processors
    return np

########################################################################
#
# Make a string we can use to call the builder, which would
# be make or ninja.
#
########################################################################
def makeBuilderCall(config):
    np = getNumberProcessors(config)
    if config.generator.endswith('Unix Makefiles'):
        return "make -j%d %s" % (
            np,
            "VERBOSE=1" if config.verbose_build == 'yes' else ''
        )
    elif config.generator.endswith('Ninja'):
        return "ninja -j %d " % (
            np,
            "-v" if config.verbose_build == 'yes' else ""
        )
    else:
        print('Unknown generator \'%s\'' % config.generator)

########################################################################
#
# Get the cmake command string.
#
########################################################################
def configureCommandString(config):
    profile = "OFF"
    coverage = "OFF"
    if config.coverage:
        coverage = "ON"
    if config.profile:
        profile = 'ON'
    if config.buildtype == 'debug' or config.buildtype == 'memcheck':
        cmakeBuildType="Debug"
    else:
        cmakeBuildType="Release"
    verbose = "--debug" if config.verbose_config == 'yes' else ''

    return (('cmake '
            '%s '                         # verbose
            '-DCMAKE_BUILD_TYPE=%s '      # cmakeBuildType
            '-DVOLTDB_BUILD_TYPE=%s '     # config.buildtype
            '-G \'%s\' '                  # config.generator
            '-DVOLTDB_USE_COVERAGE=%s '   # coverage
            '-DVOLTDB_USE_PROFILING=%s '  # profile
            '-DVOLT_LOG_LEVEL=%s '        # config.log_level
            '-DVOLT_POOL_CHECKING=%s '    # pool_checking
            '-DVOLT_TRACE_ALLOCATIONS=%s '# trace_pools
            '-DVOLT_TIMER_ENABLED=%s '    # timer_enabled
            '-DVOLTDB_CORE_COUNT=%d '     # number of processors
            '%s')                         # config.srcdir
              % (verbose,
                cmakeBuildType,
                config.buildtype,
                config.generator,
                coverage,
                profile,
                config.log_level,
                config.pool_checking,
                config.trace_pools,
                config.timer_enabled,
                getNumberProcessors(config),
                config.srcdir))

########################################################################
#
# Build the builder strings.  These would the call to make or
# ninja to build the tool.  Since both are so similar we use
# the same target specification.  Note that we return a list
# of commands.  Apparently some make versions don't handle
# dependencies properly, and running the tests can be interleaved
# with building the tests if we make these all the one make
# command.
#
# Note that we will have called validateConfig, which set
# some target implications.  For example, run-all-tests implies
# build-all-tests.
#
########################################################################
def buildCommandSet(config):
    targets=[]
    if config.install:
        targets += ['install']
    if config.buildonetest:
        targets += ['build-test-%s' % config.buildonetest]
    elif config.buildonetestdir:
        targets += ['build-testdir-%s' % config.buildonetestdir]
    elif config.buildalltests:
        targets += ['build-all-tests']
    if config.runonetest:
        targets += ['run-test-%s' % config.runonetest]
    elif config.runonetestdir:
        targets += ['run-dir-%s' % config.runonetestdir]
    elif config.runalltests:
        targets += ['run-all-tests']
    # If we got no targets here then
    # don't return a string.  Return None.
    if len(targets) > 0:
        return ["%s %s" % (makeBuilderCall(config), target) for target in targets ]
    return []

def runCommand(commandStr, config):
    print("########################################################################")
    print("#")
    print("# Running command '%s'" % commandStr)
    print("#")
    print("########################################################################")
    sys.stdout.flush()
    if config.debug:
        print(commandStr)
        return (True, 0)
    else:
        retcode = subprocess.call(commandStr, shell = True)
        return (retcode == 0, retcode)

def morethanoneof(a, b, c):
    if a:
        return b or c
    elif b:
        return c
    else:
        return False

def validateConfig(config):
    # If we don't have an explicit objdir,
    # then it's obj/${BUILDTYPE}.
    if not config.objdir:
        config.objdir = os.path.join('obj', config.buildtype)
    # If we don't have an explicity srcdir
    # then it's the current working directory.
    if not config.srcdir:
        config.srcdir = os.path.join(os.getcwd())
    # Some of the build and run parameters are incompatible.
    if morethanoneof(config.runalltests, config.runonetest, config.runonetestdir):
        print("--run-all-tests, --run-one-testdir and --run-one-test are incompatible.")
        os.exit(1)
    if morethanoneof(config.buildalltests, config.buildonetest, config.buildonetestdir):
        print("--build-all-tests, --build-one-testdir and --build-one-test are incompatible")
        os.exit(1)
    # If we have specifed running something then we need
    # to build it.
    if config.runalltests or config.runonetest or config.runonetestdir:
        config.install = True
        config.buildalltests = config.runalltests
        config.buildonetest = config.runonetest
        config.buildonetestdir = config.runonetestdir
    # If we have specified building one or more tests
    # then we need to build the shared library and install
    # it.
    if config.buildalltests or config.buildonetest or config.buildonetestdir:
        config.install = True

def doCleanBuild(config):
    #
    # The config.objdir must either not exist or else
    # be a directory.  If this is a clean build we
    # delete the existing directory.
    #
    if os.path.exists(config.objdir) and not os.path.isdir(config.objdir):
        print('build.py: \'%s\' exists but is not a directory.' % config.objdir)
        sys.exit(100)
    deleteDirectory(config.objdir, config)

def ensureInObjDir(config):
    if not os.path.exists(config.objdir):
        if (config.debug):
            print("Making directory \"%s\"" % config.objdir)
        else:
            try:
                os.makedirs(config.objdir)
            except OSError as ex:
                print("Cannot make directory \"%s\": %s" % (config.objdir, ex))
                os.exit(1)
    if config.debug:
        print('Changing to directory %s' % config.objdir)
    else:
        try:
            os.chdir(config.objdir)
        except OSError as ex:
            print("Cannot change directory to \"%s\"", config.objdir)
            os.exit(1)

def doConfigure(config):
    #
    # If we have not already configured, we want to reconfigure.
    # We always want to do this.
    #
    configCmd = configureCommandString(config)
    (success, retval) = runCommand(configCmd, config);
    if not success:
        print("Cmake command \"%s\" failed, return status %d."
              % (configCmd, retval))
        sys.exit(100)

def doBuild(config):
    buildCmds = buildCommandSet(config)
    for buildCmd in buildCmds:
        (success, retval) = runCommand(buildCmd, config)
        if not success:
            print("Build command \"%s\" failed, return status %d."
                  % (buildCmd, retval))
            sys.exit(100)

def main():
    parser=makeParser()
    config=parser.parse_args()

    # Not all configs are valid.  Check here.
    validateConfig(config)

    # Are we doing a clean build?
    if config.cleanbuild:
        doCleanBuild(config)

    # Make sure we are in the obj directory.
    ensureInObjDir(config)

    # Configure the build if necessary.
    doConfigure(config)
    #
    # Do the actual build.  This will build the
    # shared library, the ipc executable and all tests
    # that are asked for.  This will also run all
    # the tests that are asked for.
    #
    doBuild(config)
    print("Build success.")

if __name__ == '__main__':
    main()
    sys.exit(0)
