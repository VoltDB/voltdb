#!/usr/bin/env python

import os, sys, shutil
from fabric.api import run, cd, local, get, settings, lcd
from fabric_ssh_config import getSSHInfoForHost

builddir = "/tmp/" + os.getenv('USER') + "/buildtemp"
version = "UNKNOWN"

################################################
# CHECKOUT CODE INTO A TEMP DIR
################################################

def checkoutCode(engSvnUrl, proSvnUrl):
    global buildir
    # clean out the existing dir
    run("rm -rf " + builddir)
    # make the build dir again
    run("mkdir -p " + builddir)
    # change to it
    with cd(builddir):
        # do the checkouts
        run("git clone git@github.com:VoltDB/voltdb.git")
        run("git clone git@github.com:VoltDB/pro.git")
        return run("cat voltdb/version.txt").strip()

################################################
# MAKE A RELEASE DIR
################################################

def makeReleaseDir(releaseDir):
    # handle the case where a release dir exists for this version
    if os.path.exists(releaseDir):
        if (len(os.listdir(releaseDir)) > 0):
            # make a backup before we clear an existing release dir
            if os.path.exists(releaseDir + ".tgz"):
                os.remove(releaseDir + ".tgz")
            local("tar -czf " +  releaseDir + ".tgz " + releaseDir)
        shutil.rmtree(releaseDir)
    # create a release dir
    os.makedirs(releaseDir)

################################################
# BUILD THE COMMUNITY VERSION
################################################

def buildCommunity():
    with cd(builddir + "/voltdb"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("ant clean default dist")

################################################
# BUILD THE ENTERPRISE VERSION
################################################

def buildPro():
    with cd(builddir + "/pro"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("VOLTCORE=../voltdb ant -f mmt.xml clean dist.pro")

################################################
# COPY FILES
################################################

def copyCommunityFilesToReleaseDir(releaseDir, version, operatingsys):
    get("%s/voltdb/obj/release/voltdb-%s.tar.gz" % (builddir, version),
        "%s/%s-voltdb-%s.tar.gz" % (releaseDir, operatingsys, version))
    get("%s/voltdb/obj/release/voltdb-client-java-%s.tar.gz" % (builddir, version),
        "%s/voltdb-client-java-%s.tar.gz" % (releaseDir, version))
    get("%s/voltdb/obj/release/voltdb-studio.web-%s.zip" % (builddir, version),
        "%s/voltdb-studio.web-%s.zip" % (releaseDir, version))
    get("%s/voltdb/obj/release/voltdb-voltcache-%s.tar.gz" % (builddir, version),
        "%s/%s-voltdb-voltcache-%s.tar.gz" % (releaseDir, operatingsys, version))
    get("%s/voltdb/obj/release/voltdb-voltkv-%s.tar.gz" % (builddir, version),
        "%s/%s-voltdb-voltkv-%s.tar.gz" % (releaseDir, operatingsys, version))

    # add stripped symbols
    if operatingsys == "LINUX":
        os.makedirs(releaseDir + "/other")
        get("%s/voltdb/obj/release/voltdb-%s.sym" % (builddir, version),
            "%s/other/%s-voltdb-voltkv-%s.sym" % (releaseDir, operatingsys, version))

def copyEnterpriseFilesToReleaseDir(releaseDir, version, operatingsys):
    get("%s/pro/obj/pro/voltdb-ent-%s.tar.gz" % (builddir, version),
        "%s/%s-voltdb-ent-%s.tar.gz" % (releaseDir, operatingsys, version))

################################################
# COMPUTE CHECKSUMS
################################################

def computeChecksums(releaseDir):
    md5cmd = "md5sum"
    sha1cmd = "sha1sum"
    if os.uname()[0] == "Darwin":
        md5cmd = "md5 -r"
        sha1cmd = "shasum -a 1"

    with lcd(releaseDir):
        local('echo "CRC checksums:" > checksums.txt')
        local('echo "" >> checksums.txt')
        local('cksum *.*z* >> checksums.txt')
        local('echo "MD5 checksums:" >> checksums.txt')
        local('echo "" >> checksums.txt')
        local('%s *.*z* >> checksums.txt' % md5cmd)
        local('echo "SHA1 checksums:" >> checksums.txt')
        local('echo "" >> checksums.txt')
        local('%s *.*z* >> checksums.txt' % sha1cmd)

################################################
# CREATE CANDIDATE SYMLINKS
################################################

def createCandidateSysmlink(releaseDir):
    candidateDir =  os.getenv('HOME') + "/releases/candidate";
    local("rm -rf " + candidateDir)
    local("ln -s %s %s" % (releaseDir, candidateDir))

################################################
# GET THE SVN URLS TO BUILD THE KIT FROM
################################################

if len(sys.argv) > 3:
    print "usage"

def getSVNURL(defaultPrefix, input):
    input = input.strip()
    if input.startswith("http"):
        return input
    if input[0] == '/':
        input = input[1:]
    return defaultPrefix + input

argv = sys.argv
if len(argv) == 1: argv = ["build-kit.py", "trunk", "branches/rest"]
if len(argv) == 2: argv = ["build-kit.py", argv[0], argv[0]]
eng_svn_url = getSVNURL("https://svn.voltdb.com/eng/", argv[1])
pro_svn_url = getSVNURL("https://svn.voltdb.com/pro/", argv[2])

version = "unknown"
releaseDir = "unknown"

# get ssh config
volt5f = getSSHInfoForHost("volt5f")
voltmini = getSSHInfoForHost("voltmini")

# build kits on 5f
with settings(user='test',host_string=volt5f[1],disable_known_hosts=True,key_filename=volt5f[0]):
    version = checkoutCode(eng_svn_url, pro_svn_url)
    releaseDir = os.getenv('HOME') + "/releases/" + version
    makeReleaseDir(releaseDir)
    print "VERSION: " + version
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, version, "LINUX")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, version, "LINUX")

# build kits on the mini
with settings(user='test',host_string=voltmini[1],disable_known_hosts=True,key_filename=voltmini[0]):
    version2 = checkoutCode(eng_svn_url, pro_svn_url)
    assert version == version2
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, version, "MAC")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, version, "MAC")

computeChecksums(releaseDir)
createCandidateSysmlink(releaseDir)
