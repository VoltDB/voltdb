import os, sys, threading, shutil, re
from subprocess import Popen, PIPE, STDOUT


class BuildContext:
    def __init__(self, args):
        self.CPPFLAGS = ""
        self.EXTRAFLAGS = ""
        self.LDFLAGS = "-L${INSTALL_DIR}/lib"
        self.LASTLDFLAGS = ""
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
        self.INPUT = {}
        self.SRC_INCLUDE_DIRS = []
        self.OBJ_INCLUDE_DIRS = ["3pty-install/include"]
        self.THIRD_PARTY_INPUT = {}
        self.TESTS = {}
        self.PLATFORM = os.uname()[0]
        self.LEVEL = "DEBUG"
        self.TARGET = "BUILD"
        self.NM = "/usr/bin/nm"
        self.NMFLAGS = "-n"    # specialized by platform in build.py
        self.COVERAGE = False
        self.PROFILE = False
        # Compiler Configuration
        self.CC = "gcc"
        self.CXX = "g++"
        self.COMPILER_NAME = "Unknown"
        self.COMPILER_CONFIGURED = False
        self.MAJORVERSION = 0
        self.MINORVERSION = 0
        self.PATCHLEVEL   = 0
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
        
    def compilerName(self):
        self.getCompilerVersion()
        if self.COMPILER_NAME:
            return self.COMPILER_NAME
        else:
            return "UnknownCompiler"
    def compilerMajorVersion(self):
        self.getCompilerVersion()
        return self.MAJORVERSION
    def compilerMinorVersion(self):
        self.getCompilerVersion()
        return self.MINORVERSION
    def compilerPatchLevel(self):
        self.getCompilerVersion()
        return self.PATCHLEVEL
    def getCompilerVersion(self):
        if self.COMPILER_CONFIGURED:
            return
        self.COMPILER_CONFIGURED = True
        vinfo = output = Popen([self.CXX, "-v"], stderr=PIPE).communicate()[1]
        self.MAJORVERSION = 0
        self.MINORVERSION = 0
        self.PATCHLEVEL   = 0
        vvector = [0, 0, 0]
        # Apple now uses clang and has its own versioning system.
        # The version 7 compiler needs special compilation options.
        # This is pretty dumb code that could be improved as we support more
        # compilers and versions.
        #   Xcode versions before 10.10
        #   Xcode version 10.11
        #   Clang built not from apple.
        #   gcc
        patterns = [
                    [ r"based on LLVM ([.0-9]*)[^.0-9]",      "clang"],
                    [ r"Apple LLVM version ([.0-9]*)[^.0-9]", "clang"],
                    [ r"clang version ([.0-9]*)[^.0-9]",      "clang"],
                    [ r"gcc version ([.0-9]*)[^.0-9]",        "gcc"  ],
                   ]
        for pattern in patterns:
            m = re.search(pattern[0], vinfo)
            if m:
                self.COMPILER_NAME = pattern[1]
                vinfo = m.group(1)
                vvector = vinfo.split(".")
                break
        if self.COMPILER_NAME == "Unknown":
            print("Cannot find compiler version by running \"%s\"" % self.CXX)
        else:
            vlen = len(vvector)
            if vlen > 0:
                self.MAJORVERSION = int(vvector[0])
                if vlen > 1:
                    self.MINORVERSION = int(vvector[1])
                    if vlen > 2:
                        self.PATCHLEVEL = int(vvector[2])

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
def replaceSuffix(name, suffix):
    pos = name.rindex(".")
    if pos < 0:
        return name + suffix;
    else:
        return name[:pos] + suffix;
    
def outputNamesForSource(filename):
    relativepath = "/".join(filename.split("/")[2:])
    jni_objname = "objects/" + replaceSuffix(relativepath, ".o")
    static_objname = "static_objects/" + replaceSuffix(relativepath, ".o")
    return os.path.normpath(jni_objname), os.path.normpath(static_objname)

def namesForTestCode(filename):
    relativepath = "/".join(filename.split("/")[2:])
    binname = "cpptests/" + relativepath
    sourcename = filename + ".cpp"
    objectname = "static_objects/" + filename.split("/")[-1] + ".o"
    return os.path.normpath(binname), os.path.normpath(objectname), os.path.normpath(sourcename)

def formatList(list):
    str = ""
    indent = 16
    for name in list:
        if indent + len(name) + 1 > 120:
            str += " \\\n\t\t"
            indent = 16
        str += (" " + name)
        indent += len(name) + 1
    return str

def buildMakefile(CTX):
    global version

    CPPFLAGS = " ".join(CTX.CPPFLAGS.split())
    MAKECPPFLAGS = CPPFLAGS
    for dir in CTX.SYSTEM_DIRS:
        MAKECPPFLAGS += " -isystem $(ROOTDIR)/%s" % (dir)
    for dir in CTX.SRC_INCLUDE_DIRS:
        MAKECPPFLAGS += " -I$(ROOTDIR)/%s" % (dir)
    for dir in CTX.OBJ_INCLUDE_DIRS:
        MAKECPPFLAGS += " -I${OBJDIR}/%s" % (dir)
    MAKECPPFLAGS += " -I${OBJDIR}"
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
    THIRD_PARTY_INSTALL_DIR=(OUTPUT_PREFIX + "/" + "3pty-install")

    # create directories for output if they don't exist
    os.system("mkdir -p %s" % (OUTPUT_PREFIX))
    os.system("mkdir -p %s" % THIRD_PARTY_INSTALL_DIR)
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/nativelibs"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/objects"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/static_objects"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/cpptests"))
    os.system("mkdir -p %s" % (OUTPUT_PREFIX + "/prod"))

    input_paths = []
    for dir in CTX.INPUT.keys():
        input = CTX.INPUT[dir].split()
        input_paths += [INPUT_PREFIX + "/" + dir + "/" + x for x in input]

    third_party_input_paths = []
    for dir in CTX.THIRD_PARTY_INPUT.keys():
        input = CTX.THIRD_PARTY_INPUT[dir].split()
        third_party_input_paths += [THIRD_PARTY_INPUT_PREFIX + "/" + dir + "/" + x for x in input]

    tests = []
    for dir in CTX.TESTS.keys():
        input = CTX.TESTS[dir].split()
        tests += [TEST_PREFIX + "/" + dir + "/" + x for x in input]

    makefile = file(OUTPUT_PREFIX + "/makefile", 'w')
    makefile.write("BUILD=%s\n" % CTX.LEVEL.lower())
    makefile.write("CC = %s\n" % CTX.CC)
    makefile.write("CXX = %s\n" % CTX.CXX)
    makefile.write("CPPFLAGS += %s\n" % formatList(MAKECPPFLAGS.split(" ")))
    makefile.write("LDFLAGS += %s\n" % (CTX.LDFLAGS))
    makefile.write("JNILIBFLAGS += %s\n" % (JNILIBFLAGS))
    makefile.write("JNIBINFLAGS += %s\n" % (JNIBINFLAGS))
    makefile.write("JNIEXT = %s\n" % (JNIEXT))
    makefile.write("NM = %s\n" % (NM))
    makefile.write("NMFLAGS = %s\n" % (NMFLAGS))
    makefile.write('RM = /bin/rm -rf\n')
    makefile.write("#\n")
    makefile.write("# Capture the (relative) name of the root directory.\n")
    makefile.write("# Also, remember the obj directory,\n")
    makefile.write("# which should be the directory we are currently in.\n")
    makefile.write("#\n")
    makefile.write('ROOTDIR=$(shell (cd ../..; /bin/pwd))\n')
    makefile.write('OBJDIR=$(ROOTDIR)/obj/${BUILD}\n')
    makefile.write('INSTALL_DIR=$(ROOTDIR)/%s\n' % THIRD_PARTY_INSTALL_DIR)
    makefile.write('#\n')
    makefile.write('# This is the root of the cpp sources.\n')
    makefile.write('#\n')
    makefile.write("SRCDIR = $(ROOTDIR)/src/ee\n")
    makefile.write('#\n')
    makefile.write('# This is the root of the third party sources.\n')
    makefile.write('#\n')
    makefile.write("THIRD_PARTY_SRC = $(ROOTDIR)/third_party/cpp\n")
    makefile.write("\n")
    makefile.write('#\n')
    makefile.write('# We keep tarballs of third party sources here.\n')
    makefile.write('#\n')
    makefile.write('TARBALLS_DIR=${THIRD_PARTY_SRC}/tarballs\n')
    makefile.write('\n')
    makefile.write('# These are the PCRE2 library\'s source, build and install directories,\n')
    makefile.write('# and the location of the source tarball\n')
    makefile.write('PCRE2_VERSION=10.10\n')
    makefile.write('PCRE2_NAME=pcre2-${PCRE2_VERSION}\n')
    makefile.write('PCRE2_SRC=${THIRD_PARTY_SRC}/${PCRE2_NAME}\n')
    makefile.write('PCRE2_OBJ=${OBJDIR}/${PCRE2_NAME}\n')
    makefile.write('PCRE2_INSTALL=${INSTALL_DIR}\n')
    makefile.write('PCRE2_TARBALL=${TARBALLS_DIR}/${PCRE2_NAME}.tar.bz2\n')

    if CTX.TARGET == "CLEAN":
        makefile.write(".PHONY: clean\n")
        makefile.write("clean: \n")
	makefile.write('\t${RM} "${PCRE2_SRC}"\n')
        makefile.write('\trm -rf "${OBJDIR}"\n')
        makefile.close()
        return

    makefile.write(".PHONY: main\n\n")
    if CTX.TARGET == "VOLTRUN":
        makefile.write("main: prod/voltrun\n")
    elif CTX.TARGET == "TEST":
        makefile.write("main:\n")
    else:
        makefile.write("main: nativelibs/libvoltdb-%s.$(JNIEXT)\n" % version)
    makefile.write("\n")

    jni_objects = []
    static_objects = []
    for filename in input_paths:
        jni, static = outputNamesForSource(filename)
        jni_objects.append(jni)
        static_objects.append(static)
    for filename in third_party_input_paths:
        jni, static = outputNamesForSource(filename)
        jni_objects.append(jni)
        static_objects.append(static)

    cleanobjs = []
    makefile.write("# create symbols by running nm against libvoltdb-%s\n" % version)
    nmfilename = ("nativelibs/libvoltdb-%s.sym" % version)
    jnilibname = ("nativelibs/libvoltdb-%s.$(JNIEXT)" % version)
    cleanobjs += [nmfilename]
    makefile.write("%s: %s\n" % (nmfilename, jnilibname))
    makefile.write("\t$(NM) $(NMFLAGS) %s > $@\n" % jnilibname)
    makefile.write("\n")

    makefile.write("# main jnilib target\n")
    makefile.write("%s: %s\n" % (jnilibname, formatList(jni_objects)))
    makefile.write("\t$(LINK.cpp) $(JNILIBFLAGS) -o $@ $^ %s \n" % ( CTX.LASTLDFLAGS ) )
    makefile.write("\n")
    cleanobjs += [ jnilibname ]

    makefile.write("# voltdb instance that loads the jvm from C++\n")
    makefile.write("prod/voltrun: $(SRCDIR)/voltrun.cpp " + formatList(static_objects) + "\n")
    makefile.write("\t$(LINK.cpp) $(JNIBINFLAGS) -o $@ $^ %s\n" % ( CTX.LASTLDFLAGS ))
    makefile.write("\n")
    cleanobjs += ["prod/voltrun"]

    makefile.write("# voltdb execution engine that accepts work on a tcp socket (vs. jni)\n")
    makefile.write("prod/voltdbipc: $(SRCDIR)/voltdbipc.cpp " + " objects/volt.a\n")
    makefile.write("\t$(LINK.cpp) -o $@ $^ %s %s\n" % (CTX.LASTLDFLAGS, CTX.LASTIPCLDFLAGS))
    makefile.write("\n")
    cleanobjs += ["prod/voltdbipc"]

    # If this is a memcheck or memcheck_nofreelist build, then
    # building test requires building the ipc executable as well.
    IPC_EXENAME = ""
    if CTX.LEVEL == "MEMCHECK" or CTX.LEVEL == "MEMCHECK_NOFREELIST":
        IPC_EXENAME = "prod/voltdbipc "
    makefile.write(".PHONY: test\n")
    makefile.write("test: %s%s\n" % (IPC_EXENAME, formatList((namesForTestCode(test)[0] for test in tests))))
        
    makefile.write("objects/volt.a: objects/harness.o %s\n" % formatList(jni_objects))
    makefile.write("\t$(AR) $(ARFLAGS) $@ $?\n")
    harness_source = TEST_PREFIX + "/harness.cpp"
    makefile.write("objects/harness.o: $(ROOTDIR)/" + harness_source + "\n")
    makefile.write("\t$(CCACHE) $(COMPILE.cpp) -MMD -MP -o $@ $^\n")
    makefile.write("-include %s\n" % "objects/harness.d")
    makefile.write("\n")
    cleanobjs += ["objects/volt.a", "objects/harness.o", "objects/harness.d"]
    
    makefile.write("########################################################################\n")
    makefile.write("#\n# %s\n#\n" % "Volt Files")
    makefile.write("########################################################################\n")
    for filename in input_paths:
        jni_objname, static_objname = outputNamesForSource(filename)
        filename = os.path.normpath(filename.replace(INPUT_PREFIX, "$(SRCDIR)"))
        jni_targetpath = OUTPUT_PREFIX + "/" + "/".join(jni_objname.split("/")[:-1])
        static_targetpath = OUTPUT_PREFIX + "/" + "/".join(static_objname.split("/")[:-1])
        os.system("mkdir -p %s" % (jni_targetpath))
        os.system("mkdir -p %s" % (static_targetpath))
        makefile.write("########################################################################\n")
        makefile.write("#\n# %s\n#\n" % filename)
        makefile.write("########################################################################\n")
        makefile.write("%s: %s | build-third-party-tools \n" % (jni_objname, filename))
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s -MMD -MP -o $@ %s\n" % (CTX.EXTRAFLAGS, filename))
        makefile.write("%s: %s | build-third-party-tools \n" % (static_objname, filename))
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s -MMD -MP -o $@ %s\n" % (CTX.EXTRAFLAGS, filename))
        makefile.write("-include %s\n" % replaceSuffix(jni_objname, ".d"))
        makefile.write("-include %s\n" % replaceSuffix(static_objname, ".d"))
        cleanobjs += [jni_objname,
                      static_objname,
                      replaceSuffix(jni_objname, ".d"),
                      replaceSuffix(jni_objname, ".d")]
        makefile.write("\n")
    makefile.write("\n")

    makefile.write("########################################################################\n")
    makefile.write("#\n# %s\n#\n" % "Third Party Files")
    makefile.write("########################################################################\n")
    for filename in third_party_input_paths:
        jni_objname, static_objname = outputNamesForSource(filename)
        filename = filename.replace(THIRD_PARTY_INPUT_PREFIX, "$(THIRD_PARTY_SRC)")
        jni_targetpath = OUTPUT_PREFIX + "/" + "/".join(jni_objname.split("/")[:-1])
        static_targetpath = OUTPUT_PREFIX + "/" + "/".join(static_objname.split("/")[:-1])
        os.system("mkdir -p %s" % (jni_targetpath))
        os.system("mkdir -p %s" % (static_targetpath))
        makefile.write("########################################################################\n")
        makefile.write("#\n# %s\n#\n" % filename)
        makefile.write("########################################################################\n")
        makefile.write("%s: %s | build-third-party-tools \n" % (jni_objname, filename))
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s -MMD -MP -o $@ %s\n" % (CTX.EXTRAFLAGS, filename))
        makefile.write("%s: %s | build-third-party-tools \n" % (static_objname, filename))
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) %s -MMD -MP -o $@ %s\n" % (CTX.EXTRAFLAGS, filename))
        makefile.write("-include %s\n" % replaceSuffix(jni_objname, ".d"))
        makefile.write("-include %s\n" % replaceSuffix(static_objname, ".d"))
        cleanobjs += [jni_objname,
                      static_objname,
                      replaceSuffix(jni_objname, ".d"),
                      replaceSuffix(jni_objname, ".d")]
        makefile.write("\n")
    makefile.write("\n")

    makefile.write("########################################################################\n")
    makefile.write("#\n# This target lets us print makefile variables, for debugging\n")
    makefile.write("########################################################################\n")
    makefile.write("echo_makefile_config:\n")
    makefile.write('\t@echo "ROOTDIR = $(ROOTDIR)"\n')
    makefile.write('\t@echo "OBJDIR = $(OBJDIR)"\n')
    makefile.write('\t@echo "SRCDIR = $(SRCDIR)"\n')
    makefile.write('\t@echo "THIRD_PARTY_SRC = $(THIRD_PARTY_SRC)"\n\n')

    makefile.write("########################################################################\n")
    makefile.write("#\n")
    makefile.write("# Third Party Tools.\n")
    makefile.write("#\n")
    makefile.write("########################################################################\n")
    makefile.write('.PHONY: build-third-party-tools\n')
    makefile.write('build-third-party-tools: build-pcre2\n')
    makefile.write('\n')
    makefile.write('########################################################################\n')
    makefile.write('# Testing of third party tools.  We do not run this usually, but\n')
    makefile.write('# it is convenient to put it in the makefile.\n')
    makefile.write('# Note that these will not be run by Jenkins.\n')
    makefile.write('########################################################################\n')
    makefile.write('test-third-party-tools: test-pcre2\n')
    makefile.write('.PHONY: test-third-party-tools')
    makefile.write('\n')
    makefile.write('test-pcre2: build-pcre2\n')
    makefile.write('\t@echo Testing PCRE2 in ${PCRE2_OBJ}\n')
    makefile.write('\t(cd "${PCRE2_OBJ}"; ${MAKE} check)\n')
    makefile.write('\n')
    makefile.write('########################################################################\n')
    makefile.write('# pcre2 - regular expressions.\n')
    makefile.write('########################################################################\n')
    makefile.write('build-pcre2: configure-pcre2\n')
    makefile.write('\tif [ ! -f "${PCRE2_INSTALL}/lib/libpcre2-8.a" ] ; then \\\n')
    makefile.write('\t  @echo Building PCRE2 ; \\\n')
    makefile.write('\t  (cd "$(PCRE2_OBJ)"; ${MAKE} install); \\\n')
    makefile.write('\tfi\n')
    makefile.write('.PHONY: build-pcre2\n')
    makefile.write('\n')
    makefile.write('configure-pcre2: unpack-pcre2\n')
    makefile.write('\tif [ ! -d "${PCRE2_OBJ}" ] ; then \\\n')
    makefile.write('\t  @echo Configuring PCRE2; \\\n')
    makefile.write('\t  /bin/rm -rf "${PCRE2_OBJ}"; \\\n')
    makefile.write('\t  mkdir -p "${PCRE2_OBJ}"; \\\n')
    makefile.write('\t  cd "${PCRE2_OBJ}"; \\\n')
    makefile.write('\t  "${PCRE2_SRC}/configure" --disable-shared --prefix="${PCRE2_INSTALL}" ; \\\n')
    makefile.write('\tfi\n')
    makefile.write('.PHONY: configure-pcre2\n')
    makefile.write('\n')
    makefile.write('unpack-pcre2:\n')
    makefile.write('\tif [ ! -d "$PCRE2_SRC" ] ; then \\\n')
    makefile.write('\t  tar -x -j -f "${PCRE2_TARBALL}" -C "${THIRD_PARTY_SRC}" ; \\\n')
    makefile.write('fi\n')
    makefile.write('.PHONY: unpack-pcre2\n')
    makefile.write('\n')
    makefile.write('########################################################################\n')
    makefile.write('#\n')
    makefile.write('# Tests\n')
    makefile.write('#\n')
    makefile.write('########################################################################\n')
    for test in tests:
        binname, objectname, sourcename = namesForTestCode(test)

        # build the object file
        makefile.write("########################################################################\n")
        makefile.write("#\n")
        makefile.write("# %s\n" % sourcename)
        makefile.write("#\n")
        makefile.write("########################################################################\n")
        makefile.write("%s: $(ROOTDIR)/%s | build-third-party-tools \n" % (objectname, sourcename))
        makefile.write("\t$(CCACHE) $(COMPILE.cpp) -I$(ROOTDIR)/%s -MMD -MP -o $@ $(ROOTDIR)/%s\n" % (TEST_PREFIX, sourcename))
        makefile.write("-include %s\n" % replaceSuffix(objectname, ".d"))
        # link the test
        makefile.write("%s: %s objects/volt.a  | build-third-party-tools \n" % (binname, objectname))
        makefile.write("\t$(LINK.cpp) -o %s %s objects/volt.a %s\n" % (binname, objectname, CTX.LASTLDFLAGS))
        makefile.write("\n")
        targetpath = OUTPUT_PREFIX + "/" + "/".join(binname.split("/")[:-1])
        os.system("mkdir -p %s" % (targetpath))
        pysourcename = sourcename[:-3] + "py"
        cleanobjs += [objectname, replaceSuffix(objectname, ".d"), binname]
        if os.path.exists(pysourcename):
            shutil.copy(pysourcename, targetpath)
            cleanobjs += [targetpath]

    makefile.write("########################################################################\n")
    makefile.write("#\n")
    makefile.write("# Cleaning\n")
    makefile.write("#\n")
    makefile.write("########################################################################\n")
    makefile.write("\n")
    makefile.write("clean: \n")
    makefile.write("\t@echo Deleting all derived files.\n")
    makefile.write('\t@${RM} %s\n' % formatList(cleanobjs))
    makefile.write('\t@${RM} "${PCRE2_OBJ}"\n')
    makefile.write('\t@${RM} "${INSTALL_DIR}/*"\n')
    makefile.write('\t@${RM} "${PCRE2_SRC}"\n')
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
    TESTOBJECTS_DIR = os.environ['TEST_DIR']
    TEST_PREFIX = CTX.TEST_PREFIX.rstrip("/")
    OUTPUT_PREFIX = CTX.OUTPUT_PREFIX.rstrip("/")

    tests = []
    for dir in CTX.TESTS.keys():
        input = CTX.TESTS[dir].split()
        tests += [TEST_PREFIX + "/" + dir + "/" + x for x in input]
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

