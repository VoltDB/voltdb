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

###############################################################################
# CTX is an instance of BuildContext, which is declared in buildtools.py
# BuildContext contains vars that determine how the makefile will be built
#  and how the build will go down. It also checks the platform and parses
#  command line args to determine target and build level.
###############################################################################
CTX = BuildContext(sys.argv)

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
if CTX.compilerName() == 'gcc':
    CTX.CPPFLAGS += " -pthread"
    CTX.LDFLAGS += " -rdynamic"
    if (CTX.compilerMajorVersion() >= 4):
        CTX.CPPFLAGS += " -Wno-deprecated-declarations  -Wno-unknown-pragmas"
    # GCC 4 warning disablement options
    if (CTX.compilerMajorVersion() == 4):
        # Do we want -Wno-unused-but-set-variable?
        if (CTX.compilerMinorVersion() == 6) \
          or (CTX.compilerMinorVersion() == 8) \
          or (CTX.compilerMinorVersion() == 9):
            CTX.CPPFLAGS += " -Wno-unused-but-set-variable"
        # Do we want -Wno-unused-local-typedefs?
        if (CTX.compilerMinorVersion() == 8) \
          or (CTX.compilerMinorVersion() == 9):
            CTX.CPPFLAGS += " -Wno-unused-local-typedefs"
        # Do we want -Wno-float-conversion?
        if (CTX.compilerMinorVersion() == 8):
            CTX.CPPFLAGS += " -Wno-float-conversion"
        # Do we want -Wno-conversion?
        if (CTX.compilerMinorVersion() == 8):
            CTX.CPPFLAGS += " -Wno-conversion"
    # GCC 5 warning disablement options
    if (CTX.compilerMajorVersion() == 5):
        CTX.CPPFLAGS += " -Wno-unused-local-typedefs"

if (CTX.compilerName() == 'clang') and (CTX.compilerMajorVersion() == 3 and CTX.compilerMinorVersion() >= 4):
    CTX.CPPFLAGS += " -Wno-varargs"

if (CTX.compilerName() == 'clang') and (CTX.compilerMajorVersion() >= 7):
    CTX.CPPFLAGS += " -Wno-unused-local-typedefs -Wno-absolute-value"

if (CTX.compilerName() != 'gcc') or (CTX.compilerMajorVersion() == 4 and CTX.compilerMinorVersion() >= 3) or (CTX.compilerMajorVersion() == 5):
    CTX.CPPFLAGS += " -Wno-ignored-qualifiers -fno-strict-aliasing"


if CTX.PROFILE:
    CTX.CPPFLAGS += " -fvisibility=default -DPROFILE_ENABLED"

# Set the compiler version and C++ standard flag.
# GCC before 4.3 is too old.
# GCC 4.4 up to but not including 4.7 use -std=c++0x
# GCC 4.7 and later use -std=c++11
# Clang uses -std=c++11
# This should match the calculation in CMakeLists.txt
if CTX.compilerName() == 'gcc':
    if (CTX.compilerMajorVersion() < 4) \
        or ((CTX.compilerMajorVersion() == 4) \
             and (CTX.compilerMinorVersion() < 4)):
        print("GCC Version %d.%d.%d is too old\n"
              % (CTX.compilerMajorVersion(), CTX.compilerMinorVersion(), CTX.compilerPatchLevel()));
        sys.exit(-1)
    if (CTX.compilerMajorVersion() == 4):
        if 4 <= CTX.compilerMinorVersion() <= 6:
            CTX.CXX_VERSION_FLAG = "c++0x"
        else:
            CTX.CXX_VERSION_FLAG ="c++11"
    elif (CTX.compilerMajorVersion() == 5):
        CTX.CXX_VERSION_FLAG = "c++11"
    else:
        print("WARNING: GCC Version %d.%d.%d is newer than the VoltDB Validated compilers.\n"
               % (CTX.compilerMajorVersion(),
                  CTX.compilerMinorVersion(),
                  CTX.compilerPatchLevel()))
elif CTX.compilerName() == 'clang':
    CTX.CXX_VERSION_FLAG="c++11"
else:
    print("WARNING: Unknown compiler %s" % CTX.compilerName())
print("Building with %s" % CTX.CXX_VERSION_FLAG)
CTX.CPPFLAGS += " -std=" + CTX.CXX_VERSION_FLAG

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
CTX.SRC_INCLUDE_DIRS += ['src/ee' ]
CTX.SYSTEM_DIRS = ['third_party/cpp']

# don't worry about checking for changes in header files in the following
#  directories
CTX.IGNORE_SYS_PREFIXES = ['/usr/include', '/usr/lib', 'third_party']

# where to find the source
CTX.INPUT_PREFIX = "src/ee/"

# where to find the source
CTX.THIRD_PARTY_INPUT_PREFIX = "third_party/cpp"

# where to find the tests
CTX.TEST_PREFIX = "tests/ee/"

# linker flags
CTX.LDFLAGS += """ -g3"""
CTX.LASTLDFLAGS += """ -lpcre2-8 """
CTX.LASTIPCLDFLAGS = """ -ldl """

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
    CTX.JNIBINFLAGS = " -framework JavaVM,1.8"
    CTX.SOFLAGS += "-dynamiclib -undefined dynamic_lookup -single_module"
    CTX.SOEXT = "dylib"
    CTX.JNIFLAGS = "-framework JavaVM,1.8"

if CTX.PLATFORM == "Linux":
    CTX.CPPFLAGS += " -Wno-attributes -Wcast-align -DLINUX -fpic"
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
 indexref.cpp
 materializedviewhandlerinfo.cpp
 materializedviewinfo.cpp
 planfragment.cpp
 statement.cpp
 table.cpp
 tableref.cpp
"""

CTX.INPUT['structures'] = """
 ContiguousAllocator.cpp
"""

CTX.INPUT['common'] = """
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
 ExecutorVector.cpp
"""

CTX.INPUT['executors'] = """
 OptimizedProjector.cpp
 abstractexecutor.cpp
 abstractjoinexecutor.cpp
 aggregateexecutor.cpp
 deleteexecutor.cpp
 executorfactory.cpp
 executorutil.cpp
 indexcountexecutor.cpp
 indexscanexecutor.cpp
 insertexecutor.cpp
 limitexecutor.cpp
 materializedscanexecutor.cpp
 materializeexecutor.cpp
 mergereceiveexecutor.cpp
 nestloopexecutor.cpp
 nestloopindexexecutor.cpp
 orderbyexecutor.cpp
 windowfunctionexecutor.cpp
 projectionexecutor.cpp
 receiveexecutor.cpp
 sendexecutor.cpp
 seqscanexecutor.cpp
 tablecountexecutor.cpp
 tuplescanexecutor.cpp
 unionexecutor.cpp
 updateexecutor.cpp
"""

CTX.INPUT['expressions'] = """
 abstractexpression.cpp
 expressionutil.cpp
 functionexpression.cpp
 geofunctions.cpp
 operatorexpression.cpp
 parametervalueexpression.cpp
 scalarvalueexpression.cpp
 subqueryexpression.cpp
 tupleaddressexpression.cpp
 vectorexpression.cpp
"""

CTX.INPUT['plannodes'] = """
 abstractjoinnode.cpp
 abstractoperationnode.cpp
 abstractplannode.cpp
 abstractreceivenode.cpp
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
 mergereceivenode.cpp
 nestloopindexnode.cpp
 nestloopnode.cpp
 orderbynode.cpp
 plannodefragment.cpp
 plannodeutil.cpp
 windowfunctionnode.cpp
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
 CoveringCellIndex.cpp
 IndexStats.cpp
 tableindex.cpp
 tableindexfactory.cpp
"""

CTX.INPUT['storage'] = """
 AbstractDRTupleStream.cpp
 BinaryLogSink.cpp
 BinaryLogSinkWrapper.cpp
 CompatibleBinaryLogSink.cpp
 CompatibleDRTupleStream.cpp
 ConstraintFailureException.cpp
 constraintutil.cpp
 CopyOnWriteContext.cpp
 CopyOnWriteIterator.cpp
 DRTupleStream.cpp
 ElasticContext.cpp
 ElasticIndex.cpp
 ElasticIndexReadContext.cpp
 ElasticScanner.cpp
 ExportTupleStream.cpp
 MaterializedViewHandler.cpp
 MaterializedViewTriggerForInsert.cpp
 MaterializedViewTriggerForWrite.cpp
 persistenttable.cpp
 PersistentTableStats.cpp
 RecoveryContext.cpp
 streamedtable.cpp
 StreamedTableStats.cpp
 table.cpp
 TableCatalogDelegate.cpp
 tablefactory.cpp
 TableStats.cpp
 TableStreamer.cpp
 TableStreamerContext.cpp
 tableutil.cpp
 tabletuplefilter.cpp
 temptable.cpp
 TempTableLimits.cpp
 TupleBlock.cpp
 TupleStreamBase.cpp
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
# Some special handling for S2.
###############################################################################
CTX.S2GEO_LIBS += "-ls2geo -lcrypto"
CTX.LASTLDFLAGS += CTX.S2GEO_LIBS

###############################################################################
# Some special handling for OpenSSL
###############################################################################
CTX.OPENSSL_VERSION="1.0.2d"

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

if whichtests in ("${eetestsuite}", "memleaktests"):
   CTX.TESTS['memleaktests'] = """
     definite_losses
     indirect_losses
     no_losses
     still_reachable_losses
     possible_losses
     rw_deleted
   """
if whichtests in ("${eetestsuite}", "common"):
    CTX.TESTS['common'] = """
     debuglog_test
     elastic_hashinator_test
     nvalue_test
     pool_test
     serializeio_test
     tabletuple_test
     ThreadLocalPoolTest
     tupleschema_test
     undolog_test
     valuearray_test
     uniqueid_test
    """

if whichtests in ("${eetestsuite}", "execution"):
    CTX.TESTS['execution'] = """
     add_drop_table
     engine_test
     FragmentManagerTest
    """
if whichtests in ("${eetestsuite}", "executors"):
    CTX.TESTS['executors'] = """
    OptimizedProjectorTest
    MergeReceiveExecutorTest
    TestGeneratedPlans
    TestRank
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
     CompactingHashIndexTest
     CompactingTreeMultiIndexTest
     CoveringCellIndexTest
    """

if whichtests in ("${eetestsuite}", "storage"):
    CTX.TESTS['storage'] = """
     CompactionTest
     CopyOnWriteTest
     DRBinaryLog_test
     DRTupleStream_test
     ExportTupleStream_test
     PersistentTableMemStatsTest
     StreamedTable_test
     TempTableLimitsTest
     constraint_test
     filter_test
     persistent_table_log_test
     persistenttable_test
     serialize_test
     table_and_indexes_test
     table_test
     tabletuple_export_test
     tabletuplefilter_test
    """

if whichtests in ("${eetestsuite}", "structures"):
    CTX.TESTS['structures'] = """
     CompactingMapTest
     CompactingMapIndexCountTest
     CompactingHashTest
     CompactingPoolTest
     CompactingMapBenchmark
    """

if whichtests in ("${eetestsuite}", "plannodes"):
    CTX.TESTS['plannodes'] = """
     WindowFunctionPlanNodeTest
     PlanNodeFragmentTest
    """

###############################################################################
#
# Print some configuration information.  This is useful for debugging.
#
###############################################################################
print("Compiler: %s %d.%d.%d" % (CTX.compilerName(), CTX.compilerMajorVersion(), CTX.compilerMinorVersion(), CTX.compilerPatchLevel()))
print("OpenSSL: version %s, config %s\n" % (CTX.getOpenSSLVersion(), CTX.getOpenSSLToken()))

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

print("Making in directory \"%s\" with %d threads"
        % (CTX.OUTPUT_PREFIX, numHardwareThreads))
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
