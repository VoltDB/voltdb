import os, sys, threading, shutil
from subprocess import Popen, PIPE, STDOUT

class BuildContext:
    def __init__(self, args):
        self.CPPFLAGS = ""
        self.EXTRAFLAGS = ""
        self.LDFLAGS = ""
        self.JNIEXT = ""
        self.JNILIBFLAGS = ""
        self.JNIBINFLAGS = ""
        self.SOFLAGS = ""
        self.SOEXT = ""
        self.IGNORE_SYS_PREFIXES = ()
        self.INPUT_PREFIX = ""
        self.THIRD_PARTY_INPUT_PREFIX = ""
        self.OUTPUT_PREFIX = ""
        self.TEST_PREFIX = ""
        # We define input files in sets, where a
        # set is all included source files in a
        # single directory.  The directory name is
        # the key to the following three dictionaries.
        # the set of all libraries needed to link
        # is in the single, string variable INPUT_LIBS.
        # These are initialized in build.py. 
        self.INPUT = {}
        self.INPUT_INCLUDES = {}
        self.INPUT_CPPFLAGS = {}
        self.INPUT_LIBS = ""
        # We treat sources from third party libraries the same
        # as VDB sources with respect to folders, includes,
        # compilation flags and libraries.
        self.THIRD_PARTY_INPUT = {}
        self.THIRD_PARTY_INCLUDES = {}
        self.THIRD_PARTY_CPPFLAGS = {}
        self.THIRD_PARTY_LIBS = ""
        # We do almost the same with tests as with the ones
        # above.  But libraries are slightly different.
        self.TESTS = {}
        self.TESTS_INCLUDES = {}
        self.TESTS_CPPFLAGS = {}
        self.TESTS_LIBS = {}
        self.PLATFORM = os.uname()[0]
        self.LEVEL = "DEBUG"
        self.TARGET = "BUILD"
        self.NM = "/usr/bin/nm"
        self.NMFLAGS = "-n"    # specialized by platform in build.py
        self.COVERAGE = False
        self.PROFILE = False
        self.CC = "gcc"
        self.CXX = "g++"
        for arg in [x.strip().upper() for x in args]:
            if arg in ["DEBUG", "RELEASE", "MEMCHECK", "MEMCHECK_NOFREELIST"]:
                self.LEVEL = arg
            if arg in ["BUILD", "CLEAN", "TEST", "VOLTRUN", "VOLTDBIPC"]:
                self.TARGET = arg
            if arg in ["COVERAGE"]:
                self.COVERAGE = True
            if arg in ["PROFILE"]:
                self.PROFILE = True
        # Exec build.local if available, a python script provided with this
        # BuildContext object as the update-able symbol BUILD.  These example
        # build.local lines force the use of clang instead of gcc/g++.
        #   BUILD.CC = "clang"
        #   BUILD.CXX = "clang"
        buildLocal = os.path.join(os.path.dirname(__file__), "build.local")
        if os.path.exists(buildLocal):
            execfile(buildLocal, dict(BUILD = self))

def readFile(filename):
    "read a file into a string"
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

# get the volt version by reading buildstring.txt
version = "0.0.0"
try:
    version = readFile("version.txt")
    version = version.strip()
except:
    print "ERROR: Unable to read version number from version.txt."
    sys.exit(-1)

# Replace the extension of filename with the
# new extension.  If the filename does not
# have an extension, the new extension is just
# appended.
def replaceExtension(filename, newExtension):
    pos = filename.rfind(".")
    if pos == -1:
        return filename + newExtension
    else:
        return filename[0:pos] + newExtension
    
def outputNamesForSource(filename):
    relativepath = "/".join(filename.split("/")[2:])
    jni_objname = replaceExtension("objects/" + relativepath, ".o")
    static_objname = replaceExtension("static_objects/" + relativepath, ".o")
    return jni_objname, static_objname

def namesForTestCode(filename):
    relativepath = "/".join(filename.split("/")[2:])
    binname = "cpptests/" + relativepath
    sourcename = filename + ".cpp"
    objectname = replaceExtension("static_objects/" + filename.split("/")[-1], ".o")
    return binname, objectname, sourcename

def buildMakefile(CTX):
    global version

    CPPFLAGS = " ".join(CTX.CPPFLAGS.split())
    MAKECPPFLAGS = CPPFLAGS
    for dir in CTX.SYSTEM_DIRS:
        MAKECPPFLAGS += " -isystem ../../%s" % (dir)
    for dir in CTX.INCLUDE_DIRS:
        MAKECPPFLAGS += " -I../../%s" % (dir)
    LOCALCPPFLAGS = CPPFLAGS
    for dir in CTX.SYSTEM_DIRS:
        LOCALCPPFLAGS += " -isystem %s" % (dir)
    for dir in CTX.INCLUDE_DIRS:
        LOCALCPPFLAGS += " -I%s" % (dir)
    JNILIBFLAGS = " ".join(CTX.JNILIBFLAGS.split())
    JNIBINFLAGS = " ".join(CTX.JNIBINFLAGS.split())
    INPUT_PREFIX = CTX.INPUT_PREFIX.rstrip("/")
    THIRD_PARTY_INPUT_PREFIX = CTX.THIRD_PARTY_INPUT_PREFIX.rstrip("/")
    OUTPUT_PREFIX = CTX.OUTPUT_PREFIX.rstrip("/")
    TEST_PREFIX = CTX.TEST_PREFIX.rstrip("/")
    IGNORE_SYS_PREFIXES = CTX.IGNORE_SYS_PREFIXES
    JNIEXT = CTX.JNIEXT.strip()
    NM = CTX.NM
    NMFLAGS = CTX.NMFLAGS
    LIBS = " " + CTX.INPUT_LIBS + " " + CTX.THIRD_PARTY_LIBS

    # create directories for output if they don't exist
    os.system("mkdir -p %s" % (OUTPUT_PREFIX))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/nativelibs"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/objects"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/static_objects"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/cpptests"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/prod"))

    # Calculate the input_paths and third_party_input_paths.
    # For each source directory, we have a set of file names,
    # perhaps a set of source directory specific CPPFLAGS and
    # perhaps a set of source directory specific include
    # directories.  We calculate the actual file name,
    # the final CPPFLAGS value and remember the list of
    # include directories.
    input_paths = []
    for dir in CTX.INPUT.keys():
        input = CTX.INPUT[dir].split()
        cppflags = ""
        includes = []
        if dir in CTX.INPUT_CPPFLAGS:
            cppflags += (" " + CTX.INPUT_CPPFLAGS[dir])
        if dir in CTX.INPUT_INCLUDES:
            includes += CTX.INPUT_INCLUDES
        input_paths += [{'filename': INPUT_PREFIX + "/" + dir + "/" + x, 'cppflags':cppflags, 'includes': includes} for x in input]
    third_party_input_paths = []
    for dir in CTX.THIRD_PARTY_INPUT.keys():
        # Calculate the per-third-party cpp flags
        # and include files.
        flags = ""
        includes = []
        if dir in CTX.THIRD_PARTY_CPPFLAGS:
            flags += (" " + CTX.THIRD_PARTY_CPPFLAGS[dir])
        if dir in CTX.THIRD_PARTY_INCLUDES:
            includes += CTX.THIRD_PARTY_INCLUDES[dir]
        input = CTX.THIRD_PARTY_INPUT[dir].split()
        third_party_input_paths += [{'filename': THIRD_PARTY_INPUT_PREFIX + "/" + dir + "/" + x, 'cppflags': flags, 'includes': includes} for x in input]
    tests = []
    for dir in CTX.TESTS.keys():
        flags = ""
        includes = ""
        libs = ""
        if dir in CTX.TESTS_CPPFLAGS:
            flags += (" " + CTX.TESTS_CPPFLAGS[dir])
        if dir in CTX.TESTS_INCLUDES:
            includes += " " + CTX.TESTS_INCLUDES[dir]
        if dir in CTX.TESTS_LIBS:
            libs = CTX.TESTS_LIBS[dir]
        input = CTX.TESTS[dir].split()
        tests += [{'filename': TEST_PREFIX + "/" + dir + "/" + x, 'cppflags': flags, 'includes': includes, 'libs': libs} for x in input]
    makefile = file(OUTPUT_PREFIX + "/makefile", 'w')
    makefile.write("CC = %s\n" % CTX.CC)
    makefile.write("CXX = %s\n" % CTX.CXX)
    makefile.write("CPPFLAGS += %s\n" % (MAKECPPFLAGS))
    makefile.write("LDFLAGS += %s\n" % (CTX.LDFLAGS))
    makefile.write("JNILIBFLAGS += %s\n" % (JNILIBFLAGS))
    makefile.write("JNIBINFLAGS += %s\n" % (JNIBINFLAGS))
    makefile.write("JNIEXT = %s\n" % (JNIEXT))
    makefile.write("SRC = ../../%s\n" % (INPUT_PREFIX))
    makefile.write("THIRD_PARTY_SRC = ../../%s\n" % (THIRD_PARTY_INPUT_PREFIX))
    makefile.write("NM = %s\n" % (NM))
    makefile.write("NMFLAGS = %s\n" % (NMFLAGS))
    makefile.write("LIBS = %s\n" % (LIBS))
    makefile.write("\n")

    if CTX.TARGET == "CLEAN":
        makefile.write(".PHONY: clean\n")
        makefile.write("clean: \n")
        makefile.write("\trm -rf *\n")
        makefile.close()
        return

    makefile.write(".PHONY: main\n")
    if CTX.TARGET == "VOLTRUN":
        makefile.write("main: prod/voltrun\n")
    elif CTX.TARGET == "TEST":
        makefile.write("main: ")
    else:
        makefile.write("main: nativelibs/libvoltdb-%s.$(JNIEXT)\n" % version)
    makefile.write("\n")

    jni_objects = []
    static_objects = []
    for file_descr in input_paths:
        filename = file_descr['filename']
        jni, static = outputNamesForSource(filename)
        jni_objects.append(jni)
        static_objects.append(static)
    for file_descr in third_party_input_paths:
        filename = file_descr['filename']
        jni, static = outputNamesForSource(filename)
        jni_objects.append(jni)
        static_objects.append(static)

    makefile.write("# create symbols by running nm against libvoltdb-%s\n" % version)
    makefile.write("nativelibs/libvoltdb-%s.sym: nativelibs/libvoltdb-%s.$(JNIEXT)\n" % (version, version))
    makefile.write("\t$(NM) $(NMFLAGS) nativelibs/libvoltdb-%s.$(JNIEXT) > $@\n" % version)
    makefile.write("\n")

    makefile.write("# main jnilib target\n")
    makefile.write("nativelibs/libvoltdb-%s.$(JNIEXT): " % version + " ".join(jni_objects) + "\n")
    makefile.write("\t$(LINK.cpp) $(JNILIBFLAGS) -o $@ $^ $(LIBS)\n")
    makefile.write("\n")

    makefile.write("# voltdb instance that loads the jvm from C++\n")
    makefile.write("prod/voltrun: $(SRC)/voltrun.cpp " + " ".join(static_objects) + "\n")
    makefile.write("\t$(LINK.cpp) $(JNIBINFLAGS) -o $@ $^ $(LIBS)\n")
    makefile.write("\n")

    makefile.write("# voltdb execution engine that accepts work on a tcp socket (vs. jni)\n")
    makefile.write("prod/voltdbipc: $(SRC)/voltdbipc.cpp " + " objects/volt.a\n")
    makefile.write("\t$(LINK.cpp) -o $@ $^ %s\n" % (CTX.LASTLDFLAGS))
    makefile.write("\n")


    makefile.write(".PHONY: test\n")
    makefile.write("test: ")
    for test in tests:
        binname, objectname, sourcename = namesForTestCode(test['filename'])
        makefile.write(binname + " ")
    if CTX.LEVEL == "MEMCHECK":
        makefile.write("prod/voltdbipc")
    if CTX.LEVEL == "MEMCHECK_NOFREELIST":
        makefile.write("prod/voltdbipc")
    makefile.write("\n\n")
    makefile.write("objects/volt.a: " + " ".join(jni_objects) + " objects/harness.o\n")
    makefile.write("\t$(AR) $(ARFLAGS) $@ $?\n")
    makefile.write("objects/harness.o: ../../" + TEST_PREFIX + "/harness.cpp\n")
    makefile.write("\t$(CCACHE) $(COMPILE.cpp) -o $@ $^\n")
    makefile.write("\t$(CCACHE) $(COMPILE.cpp) -o $@ $^\n")
    makefile.write("\n")

    LOCALTESTCPPFLAGS = LOCALCPPFLAGS + " -I%s" % (TEST_PREFIX)
    for file_descr in input_paths:
        filename = file_descr['filename']
        cppflags = file_descr['cppflags']
        includes = " ".join([ "-I ../../%s" % include_name for include_name in file_descr['includes']])
        jni_objname, static_objname = outputNamesForSource(filename)
        filename = filename.replace(INPUT_PREFIX, "$(SRC)")
        jni_targetpath = OUTPUT_PREFIX + "/" + "/".join(jni_objname.split("/")[:-1])
        static_targetpath = OUTPUT_PREFIX + "/" + "/".join(static_objname.split("/")[:-1])
        os.system("mkdir -p %s" % (jni_targetpath))
        os.system("mkdir -p %s" % (static_targetpath))
        makefile.write("\n-include %s\n" % replaceExtension(jni_objname, ".d"))
        makefile.write(jni_objname + ":\n")
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s %s %s -MMD -MP -o %s %s\n" % (CTX.EXTRAFLAGS, cppflags, includes, jni_objname, filename))
        makefile.write("\n-include %s\n" % replaceExtension(static_objname, ".d"))
        makefile.write(static_objname + ":\n")
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s %s %s -MMD -MP -o %s %s\n" % (CTX.EXTRAFLAGS, cppflags, includes, static_objname, filename))
    makefile.write("\n")

    for file_descr in third_party_input_paths:
        filename = file_descr['filename']
        cppflags = file_descr['cppflags']
        includes = " ".join([ "-I ../../%s" % include_name for include_name in file_descr['includes']])
        jni_objname, static_objname = outputNamesForSource(filename)
        filename = filename.replace(THIRD_PARTY_INPUT_PREFIX, "$(THIRD_PARTY_SRC)")
        jni_targetpath = OUTPUT_PREFIX + "/" + "/".join(jni_objname.split("/")[:-1])
        static_targetpath = OUTPUT_PREFIX + "/" + "/".join(static_objname.split("/")[:-1])
        os.system("mkdir -p %s" % (jni_targetpath))
        os.system("mkdir -p %s" % (static_targetpath))
        makefile.write("\n-include %s\n" % replaceExtension(jni_objname, ".d"))
        makefile.write(jni_objname + ":\n")
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s %s %s -o %s %s\n" % (CTX.EXTRAFLAGS, cppflags, includes, jni_objname, filename))
        makefile.write("\n-include %s\n" % replaceExtension(static_objname, ".d"))
        makefile.write(static_objname + ":\n")
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s %s %s -o %s %s\n" % (CTX.EXTRAFLAGS, cppflags, includes, static_objname, filename))
    makefile.write("\n")

    for test_descr in tests:
        test = test_descr['filename']
        cppflags = test_descr['cppflags']
        includes = test_descr['includes']
        libs     = test_descr['libs']
        binname, objectname, sourcename = namesForTestCode(test)

        # build the object file
        makefile.write("%s: ../../%s" % (objectname, sourcename))
        makefile.write("\n")
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s -I../../%s %s -o %s ../../%s\n" % (cppflags, TEST_PREFIX, includes, objectname, sourcename))

        # link the test
        makefile.write("%s: %s objects/volt.a\n" % (binname, objectname))
        makefile.write("\t$(LINK.cpp) -o %s %s objects/volt.a %s\n" % (binname, objectname, libs))
        targetpath = OUTPUT_PREFIX + "/" + "/".join(binname.split("/")[:-1])
        os.system("mkdir -p %s" % (targetpath))

        pysourcename = sourcename[:-3] + "py"
        if os.path.exists(pysourcename):
            shutil.copy(pysourcename, targetpath)

    makefile.write("\n")
    makefile.close()
    return True

def buildIPC(CTX):
    retval = os.system("make --directory=%s prod/voltdbipc -j4" % (CTX.OUTPUT_PREFIX))
    return retval

def runTests(CTX):
    failedTests = []

    retval = os.system("make --directory=%s test -j4" % (CTX.OUTPUT_PREFIX))
    if retval != 0:
        return -1

    successes = 0
    failures = 0
    noValgrindTests = [ "CompactionTest", "CopyOnWriteTest", "harness_test", "serializeio_test" ]
    for test in tests:
        binname, objectname, sourcename = namesForTestCode(test)
        targetpath = OUTPUT_PREFIX + "/" + binname
        retval = 0
        if test.endswith("CopyOnWriteTest") and CTX.LEVEL == "MEMCHECK_NOFREELIST":
            continue
        if os.path.exists(targetpath + ".py"):
            retval = os.system("/usr/bin/env python " + targetpath + ".py")
        else:
            isValgrindTest = True;
            for test in noValgrindTests:
                if targetpath.find(test) != -1:
                    isValgrindTest = False;
            if CTX.PLATFORM == "Linux" and isValgrindTest:
                process = Popen(executable="valgrind", args=["valgrind", "--leak-check=full", "--show-reachable=yes", "--error-exitcode=-1", targetpath], stderr=PIPE, bufsize=-1)
                #out = process.stdout.readlines()
                allHeapBlocksFreed = False
                out_err = process.stderr.readlines()
                retval = process.wait()
                for str in out_err:
                    if str.find("All heap blocks were freed") != -1:
                        allHeapBlocksFreed = True
                if not allHeapBlocksFreed:
                    print "Not all heap blocks were freed"
                    retval = -1
                if retval == -1:
                    for str in out_err:
                        print str
                sys.stdout.flush()
            else:
                retval = os.system(targetpath)
        if retval == 0:
            successes += 1
        else:
            failedTests += [binname]
            failures += 1
    print "==============================================================================="
    print "TESTING COMPLETE (PASSED: %d, FAILED: %d)" % (successes, failures)
    for test in failedTests:
        print "TEST: " + test + " in DIRECTORY: " + CTX.OUTPUT_PREFIX + " FAILED"
    if failures == 0:
        print "*** SUCCESS ***"
    else:
        print "!!! FAILURE !!!"
    print "==============================================================================="

    return failures

def getCompilerVersion():
    vinfo = output = Popen(["gcc", "-v"], stderr=PIPE).communicate()[1]
    # Apple now uses clang and has its own versioning system.
    # Not sure we do anything that cares which version of clang yet.
    # This is pretty dumb code that could be improved as we support more
    #  compilers and versions.
    if output.find('clang') != -1:
        # this is a hacky way to find the real clang version from the output of
        # gcc -v on the mac. The version is right after the string "based on LLVM ".
        token = "based on LLVM "
        pos = vinfo.find(token)
        if pos != -1:
            pos += len(token)
            vinfo = vinfo[pos:pos+3]
            vinfo = vinfo.split(".")
            return "clang", vinfo, vinfo, 0
        # if not the expected apple clang format
        # this probably needs to be adjusted for clang on linux or from source
        return "clang", 0, 0, 0
    else:
        vinfo = vinfo.strip().split("\n")
        vinfo = vinfo[-1]
        vinfo = vinfo.split()[2]
        vinfo = vinfo.split(".")
        return "gcc", int(vinfo[0]), int(vinfo[1]), int(vinfo[2])

# get the version of gcc and make it avaliable
compiler_name, compiler_major, compiler_minor, compiler_point = getCompilerVersion()

