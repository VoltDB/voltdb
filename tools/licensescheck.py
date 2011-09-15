#!/usr/bin/python

import os, sys, re

# Path to eng checkout root directory. To use this as a git pre-commit hook,
# create a symlink to this file in .git/hooks with the name pre-commit
basepath = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + os.sep
ascommithook = False

prunelist = ('hsqldb19b3',
             'hsqldb',
             'jetty716',
             'proj_gen',
             'jni_md.h',
             'jni.h',
             'org_voltdb_jni_ExecutionEngine.h',
             'org_voltdb_utils_DBBPool.h',
             'org_voltdb_utils_DBBPool_DBBContainer.h',
             'xml2',
             'simplejson',
             'projectfile',
             'deploymentfile',
             'xml',
             'helloworld',
             'jaxb')

def verifyLicense(f, content, approvedLicensesJavaC, approvedLicensesPython):
    if f.endswith('.py'):
        if not content.startswith("#"):
            if content.lstrip().startswith("#"):
                print "ERROR: \"%s\" contains whitespace before initial comment." % f
                return 1
            else:
                print "ERROR: \"%s\" does not begin with a comment." % f
                return 1

        # skip hashbang
        if content.startswith("#!"):
            (ignore, content) = content.split("\n", 1)
            content = content.lstrip()

        # skip python coding magic
        if content.startswith("# -*-"):
            (ignore, content) = content.split("\n", 1)
            content = content.lstrip()

        # verify license
        for license in approvedLicensesPython:
            if content.startswith(license):
                return 0
        print "ERROR: \"%s\" does not start with an approved license." % f
    else:
        if not content.startswith("/*"):
            if content.lstrip().startswith("/*"):
                print "ERROR: \"%s\" contains whitespace before initial comment." % f
            else:
                print "ERROR: \"%s\" does not begin with a comment." % f
            return 1
        for license in approvedLicensesJavaC:
            if content.startswith(license):
                return 0
        print "ERROR: \"%s\" does not start with an approved license." % f
    return 1

def verifyTrailingWhitespace(f, content):
    if re.search(r'[\t\f\v ]\n', content):
        print("ERROR: \"%s\" contains trailing whitespace." % (f))
        return 1
    return 0

def verifyTabs(f, content):
    num = content.count('\t')
    if num  > 0:
        print("ERROR: \"%s\" contains %d tabs." % (f, num))
        return 1
    return 0

def verifySprintf(f, content):
    num = content.count('sprintf')
    if num > 0:
        print("ERROR: \"%s\" contains %d calls to sprintf(). Use snprintf()." % (f, num))
        return 1
    return 0

def readFile(filename):
    "read a file into a string"
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

def processFile(f, approvedLicensesJavaC, approvedLicensesPython):
    for suffix in ('.java', '.cpp', '.cc', '.h', '.hpp', '.py'):
        if f.endswith(suffix):
            break
    else:
        return 0
    content = readFile(f)
    retval = verifyLicense(f, content,  approvedLicensesJavaC, approvedLicensesPython)
    if retval != 0:
        return retval
    retval = verifyTabs(f, content)
    if retval != 0:
        return retval
    retval = verifyTrailingWhitespace(f, content)
    if (retval != 0):
        return retval
    retval = verifySprintf(f, content)
    if (retval != 0):
        return retval
    return 0

def processAllFiles(d, approvedLicensesJavaC, approvedLicensesPython):
    files = os.listdir(d)
    errcount = 0
    for f in [f for f in files if not f.startswith('.') and f not in prunelist]:
        fullpath = os.path.join(d,f)
        if os.path.isdir(fullpath):
            errcount += processAllFiles(fullpath, approvedLicensesJavaC, approvedLicensesPython)
        else:
            errcount += processFile(fullpath, approvedLicensesJavaC, approvedLicensesPython)
    return errcount



testLicenses =   [basepath + 'tools/approved_licenses/mit_x11_hstore_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/mit_x11_evanjones_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/mit_x11_michaelmccanna_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/mit_x11_voltdb.txt']

srcLicenses =    [basepath + 'tools/approved_licenses/gpl3_hstore_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/gpl3_evanjones_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/gpl3_base64_and_voltdb.txt',
                  basepath + 'tools/approved_licenses/gpl3_voltdb.txt']

testLicensesPy = [basepath + 'tools/approved_licenses/mit_x11_voltdb_python.txt']

srcLicensesPy =  [basepath + 'tools/approved_licenses/gpl3_voltdb_python.txt']


errcount = 0
errcount += processAllFiles(basepath + "src",
                            tuple([readFile(f) for f in srcLicenses]),
                            tuple([readFile(f) for f in srcLicensesPy]))

errcount += processAllFiles(basepath + "tests",
                            tuple([readFile(f) for f in testLicenses]),
                            tuple([readFile(f) for f in testLicensesPy]))

errcount += processAllFiles(basepath + "examples",
                            tuple([readFile(f) for f in testLicenses]),
                            tuple([readFile(f) for f in testLicensesPy]))

if errcount == 0:
    print "SUCCESS. Found 0 license text errors, 0 files containing tabs or trailing whitespace."
else:
    print "FAILURE. Found %d license text or whitespace errors." % errcount

# run through any other source the caller wants checked
# assumes a single valid license in $repo/tools/approved_licenses/license.txt
# "${voltpro}" is the build.xml property - can be seen as a literal if the
# property is not set.
if not ascommithook:
    for arg in sys.argv[1:]:
        if arg != "${voltpro}":
            print "Checking additional repository: " + arg;
            proLicenses = ["../" + arg + '/tools/approved_licenses/license.txt']
            proLicensesPy = ["../" + arg + '/tools/approved_licenses/license_python.txt']
            errcount = 0
            errcount += processAllFiles("../" + arg + "/src/",
                                        tuple([readFile(f) for f in proLicenses]),
                                        tuple([readFile(f) for f in proLicensesPy]))

            errcount += processAllFiles("../" + arg + "/tests/",
                                        tuple([readFile(f) for f in proLicenses]),
                                        tuple([readFile(f) for f in proLicensesPy]))

            if errcount == 0:
                print "SUCCESS. Found 0 license text errors, 0 files containing tabs or trailing whitespace."
            else:
                print "FAILURE (%s). Found %d license text or whitespace errors." % (arg, errcount)



sys.exit(errcount)
