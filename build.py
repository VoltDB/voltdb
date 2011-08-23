#!/usr/bin/env python

import os, sys, commands, string
from buildtools import *

# usage:
# The following all work as you might expect:
# python build.py
# ./build.py debug
# python build.py release
# python build.py test
# ./build.py clean
# ./build.py release clean
# python build.py release test

# The command line args can include a build level: release or debug
#  the default is debug
# The command line args can include an action: build, clean or test
#  the default is build
# The order doesn't matter
# Including multiple levels or actions is a bad idea

###############################################################################
# INITIALIZE BUILD CONTEXT
#  - Detect Platform
#  - Parse Target and Level from Command Line
###############################################################################

CTX = BuildContext(sys.argv)

# CTX is an instance of BuildContext, which is declared in buildtools.py
# BuildContext contains vars that determine how the makefile will be built
#  and how the build will go down. It also checks the platform and parses
#  command line args to determine target and build level.

###############################################################################
# SET GLOBAL CONTEXT VARIABLES FOR BUILDING
###############################################################################

# these are the base compile options that get added to every compile step
# this does not include header/lib search paths or specific flags for
#  specific targets
CTX.CPPFLAGS = """-Wall -Wextra -Werror -Woverloaded-virtual -Wconversion
            -Wpointer-arith -Wcast-qual -Wcast-align -Wwrite-strings
            -Winit-self -Wno-sign-compare -Wno-unused-parameter
            -pthread
            -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
            -fno-omit-frame-pointer
            -fvisibility=hidden -DBOOST_SP_DISABLE_THREADS"""

if gcc_major == 4 and gcc_minor >= 3:
    CTX.CPPFLAGS += " -Wno-ignored-qualifiers -fno-strict-aliasing"

if CTX.PROFILE:
    CTX.CPPFLAGS += " -fvisibility=default -DPROFILE_ENABLED"

# linker flags
CTX.LDFLAGS = """ -g3 -rdynamic -ldl"""

if CTX.COVERAGE:
    CTX.LDFLAGS += " -ftest-coverage -fprofile-arcs"

# for the google perftools profiler and the recommended stack unwinder
# which you must have separately built and installed. Take some guesses
# at the library location (/usr/local/lib).
if CTX.PROFILE:
    CTX.LDFLAGS = """  -L/usr/local/lib -g3 -rdynamic -lprofiler -lunwind"""

# this is where the build will look for header files
# - the test source will also automatically look in the test root dir
CTX.INCLUDE_DIRS = ['src/ee']
CTX.SYSTEM_DIRS = ['third_party/cpp']

# extra flags that will get added to building test source
if CTX.LEVEL == "MEMCHECK":
    CTX.TEST_EXTRAFLAGS = """ -g3 -DDEBUG -DMEMCHECK"""
else:
    CTX.TEST_EXTRAFLAGS = """ -g3 -DDEBUG """

# don't worry about checking for changes in header files in the following
#  directories
CTX.IGNORE_SYS_PREFIXES = ['/usr/include', '/usr/lib', 'third_party']

# where to find the source
CTX.INPUT_PREFIX = "src/ee/"

# where to find the source
CTX.THIRD_PARTY_INPUT_PREFIX = "third_party/cpp/"

# where to find the tests
CTX.TEST_PREFIX = "tests/ee/"

###############################################################################
# SET RELEASE LEVEL CONTEXT
###############################################################################

if CTX.LEVEL == "MEMCHECK":
    CTX.EXTRAFLAGS += " -g3 -rdynamic -DDEBUG -DMEMCHECK -DVOLT_LOG_LEVEL=500"
    CTX.OUTPUT_PREFIX = "obj/memcheck"

if CTX.LEVEL == "DEBUG":
    CTX.EXTRAFLAGS += " -g3 -rdynamic -DDEBUG -DVOLT_LOG_LEVEL=500"
    CTX.OUTPUT_PREFIX = "obj/debug"

if CTX.LEVEL == "RELEASE":
    CTX.EXTRAFLAGS += " -g3 -O3 -mmmx -msse -msse2 -msse3 -DNDEBUG -DVOLT_LOG_LEVEL=500"
    CTX.OUTPUT_PREFIX = "obj/release"

# build in parallel directory instead of subdir so that relative paths work
if CTX.COVERAGE:
    CTX.EXTRAFLAGS += " -ftest-coverage -fprofile-arcs"
    CTX.OUTPUT_PREFIX += "-coverage"

CTX.OUTPUT_PREFIX += "/"

###############################################################################
# HANDLE PLATFORM SPECIFIC STUFF
###############################################################################

# Defaults Section
CTX.JNIEXT = "so"
CTX.JNILIBFLAGS += " -shared"
CTX.SOFLAGS += " -shared"
CTX.SOEXT = "so"
out = Popen('java -cp tools/ SystemPropertyPrinter java.library.path'.split(),
            stdout = PIPE).communicate()[0]
libpaths = ' '.join( '-L' + path for path in out.strip().split(':') if path != '' and path != '/usr/lib' )
CTX.JNIBINFLAGS += " " + libpaths
CTX.JNIBINFLAGS += " -ljava -ljvm -lverify"

if CTX.PLATFORM == "Darwin":
    CTX.CPPFLAGS += " -DMACOSX -arch x86_64"
    CTX.JNIEXT = "jnilib"
    CTX.JNILIBFLAGS = " -bundle"
    CTX.JNIBINFLAGS = " -framework JavaVM,1.6"
    CTX.SOFLAGS += "-dynamiclib -undefined dynamic_lookup -single_module"
    CTX.SOEXT = "dylib"
    CTX.JNIFLAGS = "-framework JavaVM,1.6"

if CTX.PLATFORM == "Linux":
    CTX.CPPFLAGS += " -Wno-attributes -DLINUX -fpic"
    CTX.NMFLAGS += " --demangle"

###############################################################################
# SPECIFY SOURCE FILE INPUT
###############################################################################

# the input is a map from directory name to a list of whitespace
# separated source files (cpp only for now).  Preferred ordering is
# one file per line, indented one space, in alphabetical order.

CTX.INPUT[''] = """
 voltdbjni.cpp
"""

CTX.INPUT['catalog'] = """
 authprogram.cpp
 catalog.cpp
 cataloginteger.cpp
 catalogtype.cpp
 cluster.cpp
 column.cpp
 columnref.cpp
 connector.cpp
 connectortableinfo.cpp
 constraint.cpp
 constraintref.cpp
 database.cpp
 deployment.cpp
 group.cpp
 groupref.cpp
 host.cpp
 index.cpp
 materializedviewinfo.cpp
 partition.cpp
 planfragment.cpp
 procedure.cpp
 procparameter.cpp
 program.cpp
 site.cpp
 statement.cpp
 stmtparameter.cpp
 systemsettings.cpp
 table.cpp
 user.cpp
 userref.cpp
 snapshotschedule.cpp
 commandlog.cpp
"""

CTX.INPUT['structures'] = """
 CompactingPool.cpp
 ContiguousAllocator.cpp
"""

CTX.INPUT['common'] = """
 CompactingStringPool.cpp
 CompactingStringStorage.cpp
 ThreadLocalPool.cpp
 SegvException.cpp
 SerializableEEException.cpp
 SQLException.cpp
 StringRef.cpp
 tabletuple.cpp
 TupleSchema.cpp
 types.cpp
 UndoLog.cpp
 NValue.cpp
 RecoveryProtoMessage.cpp
 RecoveryProtoMessageBuilder.cpp
 DefaultTupleSerializer.cpp
 executorcontext.cpp
 serializeio.cpp
"""

CTX.INPUT['execution'] = """
 JNITopend.cpp
 VoltDBEngine.cpp
"""

CTX.INPUT['executors'] = """
 abstractexecutor.cpp
 deleteexecutor.cpp
 distinctexecutor.cpp
 executorutil.cpp
 indexscanexecutor.cpp
 insertexecutor.cpp
 limitexecutor.cpp
 materializeexecutor.cpp
 nestloopexecutor.cpp
 nestloopindexexecutor.cpp
 orderbyexecutor.cpp
 projectionexecutor.cpp
 receiveexecutor.cpp
 sendexecutor.cpp
 seqscanexecutor.cpp
 unionexecutor.cpp
 updateexecutor.cpp
"""

CTX.INPUT['expressions'] = """
 abstractexpression.cpp
 expressionutil.cpp
 tupleaddressexpression.cpp
"""

CTX.INPUT['plannodes'] = """
 abstractjoinnode.cpp
 abstractoperationnode.cpp
 abstractplannode.cpp
 abstractscannode.cpp
 aggregatenode.cpp
 deletenode.cpp
 distinctnode.cpp
 indexscannode.cpp
 insertnode.cpp
 limitnode.cpp
 materializenode.cpp
 nestloopindexnode.cpp
 nestloopnode.cpp
 orderbynode.cpp
 plannodefragment.cpp
 plannodeutil.cpp
 projectionnode.cpp
 receivenode.cpp
 SchemaColumn.cpp
 sendnode.cpp
 seqscannode.cpp
 unionnode.cpp
 updatenode.cpp
"""

CTX.INPUT['indexes'] = """
 arrayuniqueindex.cpp
 tableindex.cpp
 tableindexfactory.cpp
 IndexStats.cpp
"""

CTX.INPUT['storage'] = """
 constraintutil.cpp
 CopyOnWriteContext.cpp
 CopyOnWriteIterator.cpp
 ConstraintFailureException.cpp
 MaterializedViewMetadata.cpp
 persistenttable.cpp
 PersistentTableStats.cpp
 PersistentTableUndoDeleteAction.cpp
 PersistentTableUndoInsertAction.cpp
 PersistentTableUndoUpdateAction.cpp
 StreamedTableStats.cpp
 streamedtable.cpp
 table.cpp
 TableCatalogDelegate.cpp
 tablefactory.cpp
 TableStats.cpp
 tableutil.cpp
 temptable.cpp
 TempTableLimits.cpp
 TupleStreamWrapper.cpp
 RecoveryContext.cpp
 TupleBlock.cpp
"""

CTX.INPUT['stats'] = """
 StatsAgent.cpp
 StatsSource.cpp
"""

CTX.INPUT['logging'] = """
 JNILogProxy.cpp
 LogManager.cpp
"""

# specify the third party input

CTX.THIRD_PARTY_INPUT['json_spirit'] = """
 json_spirit_reader.cpp
 json_spirit_value.cpp
"""

###############################################################################
# SPECIFY THE TESTS
###############################################################################

whichtests = os.getenv("EETESTSUITE")
# input format similar to source, but the executable name is listed
if whichtests ==  "${eetestsuite}":
    CTX.TESTS['.'] = """
     harness_test
    """

if whichtests in ("${eetestsuite}", "catalog"):
    CTX.TESTS['catalog'] = """
    catalog_test
    """

if whichtests in ("${eetestsuite}", "logging"):
    CTX.TESTS['logging'] = """
    logging_test
    """

if whichtests in ("${eetestsuite}", "common"):
    CTX.TESTS['common'] = """
     debuglog_test
     serializeio_test
     undolog_test
     valuearray_test
     nvalue_test
     tabletuple_test
    """

if whichtests in ("${eetestsuite}", "execution"):
    CTX.TESTS['execution'] = """
     add_drop_table
     engine_test
    """

if whichtests in ("${eetestsuite}", "expressions"):
    CTX.TESTS['expressions'] = """
     expression_test
    """

if whichtests in ("${eetestsuite}", "indexes"):
    CTX.TESTS['indexes'] = """
     index_key_test
     index_scripted_test
     index_test
     compacting_hash_index
    """

if whichtests in ("${eetestsuite}", "storage"):
    CTX.TESTS['storage'] = """
     CompactionTest
     CopyOnWriteTest
     constraint_test
     filter_test
     persistent_table_log_test
     PersistentTableMemStatsTest
     serialize_test
     StreamedTable_test
     table_and_indexes_test
     table_test
     tabletuple_export_test
     TempTableLimitsTest
     TupleStreamWrapper_test
    """

if whichtests in ("${eetestsuite}", "structures"):
    CTX.TESTS['structures'] = """
     CompactingMapTest
     CompactingHashTest
     CompactingPoolTest
    """

if whichtests in ("${eetestsuite}", "plannodes"):
    CTX.TESTS['plannodes'] = """
     PlanNodeFragmentTest
    """

###############################################################################
# BUILD THE MAKEFILE
###############################################################################

# this function (in buildtools.py) generates the makefile
# it's currently a bit ugly but it'll get cleaned up soon
buildMakefile(CTX)

###############################################################################
# RUN THE MAKEFILE
###############################################################################
numHardwareThreads = 4
if CTX.PLATFORM == "Darwin":
    numHardwareThreads = 0
    output = commands.getstatusoutput("sysctl hw.ncpu")
    numHardwareThreads = int(string.strip(string.split(output[1])[1]))
elif CTX.PLATFORM == "Linux":
    numHardwareThreads = 0
    for line in open('/proc/cpuinfo').readlines():
        name_value = map(string.strip, string.split(line, ':', 1))
        if len(name_value) != 2:
            continue
        name,value = name_value
        if name == "processor":
            numHardwareThreads = numHardwareThreads + 1
print "Detected %d hardware threads to use during the build" % (numHardwareThreads)

retval = os.system("make --directory=%s -j%d" % (CTX.OUTPUT_PREFIX, numHardwareThreads))
print "Make returned: ", retval
if retval != 0:
    sys.exit(-1)

###############################################################################
# RUN THE TESTS IF ASKED TO
###############################################################################

retval = 0
if CTX.TARGET == "TEST":
    retval = runTests(CTX)
elif CTX.TARGET == "VOLTDBIPC":
    retval = buildIPC(CTX)

if retval != 0:
    sys.exit(-1)
