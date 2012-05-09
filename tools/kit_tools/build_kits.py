#!/usr/bin/env python

import os, sys, shutil, datetime
from fabric.api import run, cd, local, get, settings, lcd
from fabric_ssh_config import getSSHInfoForHost

username='test'
builddir = "/tmp/" + username + "/buildtemp"
version = "UNKNOWN"

################################################
# CHECKOUT CODE INTO A TEMP DIR
################################################

def checkoutCode(voltdbGit, proGit):
    global buildir
    # clean out the existing dir
    run("rm -rf " + builddir)
    # make the build dir again
    run("mkdir -p " + builddir)
    # change to it
    with cd(builddir):
        # do the checkouts
        run("git clone git@github.com:VoltDB/voltdb.git")
        run("cd voltdb; git checkout %s" % voltdbGit)
        run("git clone git@github.com:VoltDB/pro.git")
        run("cd pro; git checkout %s" % proGit)
        return run("cat voltdb/version.txt").strip()

################################################
# MAKE A RELEASE DIR
################################################

def makeReleaseDir(releaseDir):
    # handle the case where a release dir exists for this version
    if os.path.exists(releaseDir):
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
        run("VOLTCORE=../voltdb ant -f mmt.xml -Dallowreplication=true clean dist.pro")

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
# BACKUP RELEASE DIR
################################################

def backupReleaseDir(releaseDir,archiveDir,version):
    if not os.path.exists(archiveDir):
        os.makedirs(archiveDir)
    # make a backup with the timstamp of the  build
    timestamp = datetime.datetime.now().strftime("%y%m%d-%H%M%S")
    local("tar -czf %s/%s-%s.tgz %s" \
          % (archiveDir, version, timestamp, releaseDir))

################################################
# GET THE GIT TAGS OR SHAS TO BUILD FROM
################################################

if (len(sys.argv) > 3 or (len(sys.argv) == 2 and sys.argv[1] == "-h")):
    print "usage:"
    print "   build-kit.py"
    print "   build-kit.py git-tag"
    print "   build-kit.py voltdb-git-SHA pro-git-SHA"

proTreeish = "master"
voltdbTreeish = "master"

#Candidate is only for newest trunk build
createCandidate = True

if len(sys.argv) == 2:
    createCandidate = False
    proTreeish = sys.argv[1]
    voltdbTreeish = sys.argv[1]
if len(sys.argv) == 3:
    createCandidate = False
    voltdbTreeish = sys.argv[1]
    proTreeish = sys.argv[2]

print "Building with pro: %s and voltdb: %s" % (proTreeish, voltdbTreeish)
print "Create link for releases/candidate = %s" % createCandidate 

version = "unknown"
releaseDir = "unknown"

# get ssh config
volt5f = getSSHInfoForHost("volt5f")
voltmini = getSSHInfoForHost("voltmini")

# build kits on 5f
with settings(user=username,host_string=volt5f[1],disable_known_hosts=True,key_filename=volt5f[0]):
    version = checkoutCode(voltdbTreeish, proTreeish)
    if voltdbTreeish == "master":
        releaseDir = os.getenv('HOME') + "/releases/" + version
    else:
        releaseDir = "%s/releases/one-offs/%s-%s-%s" % \
            (os.getenv('HOME'), version, voltdbTreeish, proTreeish)
    makeReleaseDir(releaseDir)
    print "VERSION: " + version
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, version, "LINUX")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, version, "LINUX")

# build kits on the mini
with settings(user=username,host_string=voltmini[1],disable_known_hosts=True,key_filename=voltmini[0]):
    version2 = checkoutCode(voltdbTreeish, proTreeish)
    assert version == version2
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, version, "MAC")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, version, "MAC")


computeChecksums(releaseDir)
if createCandidate:
    createCandidateSysmlink(releaseDir)
archiveDir = os.getenv('HOME') + "/releases/archive/" + version
backupReleaseDir(releaseDir, archiveDir, version)
