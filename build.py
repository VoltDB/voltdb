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
CTX.CPPFLAGS += """-Wall -Wextra -Werror -Woverloaded-virtual
            -Wpointer-arith -Wcast-qual -Wwrite-strings
            -Winit-self -Wno-sign-compare -Wno-unused-parameter
            -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
            -fno-omit-frame-pointer
            -fvisibility=default
            -DBOOST_SP_DISABLE_THREADS -DBOOST_DISABLE_THREADS -DBOOST_ALL_NO_LIB"""

# clang doesn't seem to want this
if compiler_name == 'gcc':
    CTX.CPPFLAGS += " -pthread"
    CTX.LDFLAGS += " -rdynamic"

if (compiler_name == 'clang') and (compiler_major == 3 and compiler_minor >= 4):
    CTX.CPPFLAGS += " -Wno-varargs"

if (compiler_name != 'gcc') or (compiler_major == 4 and compiler_minor >= 3):
    CTX.CPPFLAGS += " -Wno-ignored-qualifiers -fno-strict-aliasing"

if CTX.PROFILE:
    CTX.CPPFLAGS += " -fvisibility=default -DPROFILE_ENABLED"

# linker flags
CTX.LDFLAGS += """ -g3"""
CTX.LASTLDFLAGS = """ -ldl"""

if CTX.COVERAGE:
    CTX.LDFLAGS += " -ftest-coverage -fprofile-arcs"

# for the google perftools profiler and the recommended stack unwinder
# which you must have separately built and installed. Take some guesses
# at the library location (/usr/local/lib).
if CTX.PROFILE:
    CTX.LDFLAGS = """  -L/usr/local/lib -g3 -lprofiler -lunwind"""
    # consider setting CTX.LASTLDFLAGS to " " rather than -ldl if that option really is unwanted.

# this is where the build will look for header files
# - the test source will also automatically look in the test root dir
CTX.INCLUDE_DIRS = ['src/ee']
CTX.SYSTEM_DIRS = ['third_party/cpp']

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
if "VOLT_LOG_LEVEL" in os.environ:
    LOG_LEVEL = os.environ["VOLT_LOG_LEVEL"]
else:
    LOG_LEVEL = "500"

if CTX.LEVEL == "MEMCHECK":
    CTX.CPPFLAGS += " -g3 -DDEBUG -DMEMCHECK -DVOLT_LOG_LEVEL=%s" % LOG_LEVEL
    CTX.OUTPUT_PREFIX = "obj/memcheck"

if CTX.LEVEL == "DEBUG":
    CTX.CPPFLAGS += " -g3 -DDEBUG -DVOLT_LOG_LEVEL=%s" % LOG_LEVEL
    CTX.OUTPUT_PREFIX = "obj/debug"

if CTX.LEVEL == "RELEASE":
    CTX.CPPFLAGS += " -g3 -O3 -mmmx -msse -msse2 -msse3 -DNDEBUG -DVOLT_LOG_LEVEL=%s" % LOG_LEVEL
    CTX.OUTPUT_PREFIX = "obj/release"

# build in parallel directory instead of subdir so that relative paths work
if CTX.COVERAGE:
    CTX.CPPFLAGS += " -ftest-coverage -fprofile-arcs"
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
    CTX.JNIBINFLAGS = " -framework JavaVM,1.7"
    CTX.SOFLAGS += "-dynamiclib -undefined dynamic_lookup -single_module"
    CTX.SOEXT = "dylib"
    CTX.JNIFLAGS = "-framework JavaVM,1.7"

if CTX.PLATFORM == "Linux":
    CTX.CPPFLAGS += " -Wno-attributes -Wcast-align -Wconversion -DLINUX -fpic"
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
 catalog.cpp
 catalogtype.cpp
 cluster.cpp
 column.cpp
 columnref.cpp
 connector.cpp
 connectortableinfo.cpp
 connectorproperty.cpp
 constraint.cpp
 constraintref.cpp
 database.cpp
 index.cpp
 materializedviewinfo.cpp
 planfragment.cpp
 statement.cpp
 table.cpp
"""

CTX.INPUT['structures'] = """
 CompactingPool.cpp
 ContiguousAllocator.cpp
"""

CTX.INPUT['common'] = """
 CompactingStringPool.cpp
 CompactingStringStorage.cpp
 FatalException.cpp
 ThreadLocalPool.cpp
 SegvException.cpp
 SerializableEEException.cpp
 SQLException.cpp
 InterruptException.cpp
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
 StreamPredicateList.cpp
 Topend.cpp
 TupleOutputStream.cpp
 TupleOutputStreamProcessor.cpp
 MiscUtil.cpp
 debuglog.cpp
"""

CTX.INPUT['execution'] = """
 FragmentManager.cpp
 JNITopend.cpp
 VoltDBEngine.cpp
"""

CTX.INPUT['executors'] = """
 abstractexecutor.cpp
 aggregateexecutor.cpp
 deleteexecutor.cpp
 executorutil.cpp
 indexscanexecutor.cpp
 indexcountexecutor.cpp
 tablecountexecutor.cpp
 insertexecutor.cpp
 limitexecutor.cpp
 materializeexecutor.cpp
 materializedscanexecutor.cpp
 nestloopexecutor.cpp
 nestloopindexexecutor.cpp
 orderbyexecutor.cpp
 projectionexecutor.cpp
 receiveexecutor.cpp
 sendexecutor.cpp
 seqscanexecutor.cpp
 tuplescanexecutor.cpp
 unionexecutor.cpp
 updateexecutor.cpp
"""

CTX.INPUT['expressions'] = """
 abstractexpression.cpp
 expressionutil.cpp
 vectorexpression.cpp
 functionexpression.cpp
 tupleaddressexpression.cpp
 operatorexpression.cpp
 parametervalueexpression.cpp
 subqueryexpression.cpp
 scalarvalueexpression.cpp
 vectorcomparisonexpression.cpp
"""

CTX.INPUT['plannodes'] = """
 abstractjoinnode.cpp
 abstractoperationnode.cpp
 abstractplannode.cpp
 abstractscannode.cpp
 aggregatenode.cpp
 deletenode.cpp
 indexscannode.cpp
 indexcountnode.cpp
 tablecountnode.cpp
 insertnode.cpp
 limitnode.cpp
 materializenode.cpp
 materializedscanplannode.cpp
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
 tuplescannode.cpp
 unionnode.cpp
 updatenode.cpp
"""

CTX.INPUT['indexes'] = """
 tableindex.cpp
 tableindexfactory.cpp
 IndexStats.cpp
"""

CTX.INPUT['storage'] = """
 constraintutil.cpp
 CopyOnWriteContext.cpp
 ElasticContext.cpp
 CopyOnWriteIterator.cpp
 ConstraintFailureException.cpp
 TableStreamer.cpp
 ElasticScanner.cpp
 MaterializedViewMetadata.cpp
 persistenttable.cpp
 PersistentTableStats.cpp
 StreamedTableStats.cpp
 streamedtable.cpp
 table.cpp
 TableCatalogDelegate.cpp
 tablefactory.cpp
 TableStats.cpp
 tableutil.cpp
 temptable.cpp
 TempTableLimits.cpp
 TupleStreamBase.cpp
 ExportTupleStream.cpp
 DRTupleStream.cpp
 BinaryLogSink.cpp
 RecoveryContext.cpp
 TupleBlock.cpp
 TableStreamerContext.cpp
 ElasticIndex.cpp
 ElasticIndexReadContext.cpp
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

CTX.THIRD_PARTY_INPUT['jsoncpp'] = """
 jsoncpp.cpp
"""

CTX.THIRD_PARTY_INPUT['crc'] = """
 crc32c.cc
 crc32ctables.cc
"""

CTX.THIRD_PARTY_INPUT['murmur3'] = """
 MurmurHash3.cpp
"""

CTX.THIRD_PARTY_INPUT['sha1'] = """
 sha1.cpp
"""


###############################################################################
# SPECIFY THE TESTS
###############################################################################

whichtests = os.getenv("EETESTSUITE")
if whichtests == None:
    whichtests = "${eetestsuite}"

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
     pool_test
     tabletuple_test
     elastic_hashinator_test
    """

if whichtests in ("${eetestsuite}", "execution"):
    CTX.TESTS['execution'] = """
     add_drop_table
     engine_test
     FragmentManagerTest
    """

if whichtests in ("${eetestsuite}", "expressions"):
    CTX.TESTS['expressions'] = """
     expression_test
     function_test
    """

if whichtests in ("${eetestsuite}", "indexes"):
    CTX.TESTS['indexes'] = """
     index_key_test
     index_scripted_test
     index_test
     compacting_hash_index
     CompactingTreeMultiIndexTest
    """

if whichtests in ("${eetestsuite}", "storage"):
    CTX.TESTS['storage'] = """
     CompactionTest
     constraint_test
     CopyOnWriteTest
     filter_test
     persistent_table_log_test
     PersistentTableMemStatsTest
     serialize_test
     StreamedTable_test
     table_and_indexes_test
     table_test
     tabletuple_export_test
     TempTableLimitsTest
     ExportTupleStream_test
     DRTupleStream_test
     DRBinaryLog_test
    """

if whichtests in ("${eetestsuite}", "structures"):
    CTX.TESTS['structures'] = """
     CompactingMapTest
     CompactingMapIndexCountTest
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
if not os.environ.get('EESKIPBUILDMAKEFILE'):
    print "build.py: Making the makefile"
    buildMakefile(CTX)

if os.environ.get('EEONLYBUILDMAKEFILE'):
    sys.exit()

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

retval = os.system("make --directory=%s -j%d" % (CTX.OUTPUT_PREFIX, numHardwareThreads))
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
