#!/usr/bin/env python

import os, sys, shutil, datetime
from fabric.api import run, cd, local, get, settings, lcd, put
from fabric_ssh_config import getSSHInfoForHost

username='test'
builddir = "/tmp/" + username + "Kits/buildtemp"
version = "UNKNOWN"
defaultlicensedays = 37 #default trial license in kit = 30 days + 1 week

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
    print "Created dir: " + releaseDir

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
        run("VOLTCORE=../voltdb ant -f mmt.xml -Dallowreplication=true -Dlicensedays=%d clean dist.pro" % defaultlicensedays)

################################################
# MAKE AN ENTERPRISE TRIAL LICENSE
################################################

# Must be called after buildPro has been done
def makeTrialLicense(days=30):
    with cd(builddir + "/pro/tools"):
        run("./make_trial_licenses.pl -t %d -W" % (days))

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
    get("%s/voltdb/obj/release/voltdb-tools-%s.tar.gz" % (builddir, version),
        "%s/voltdb-tools-%s.tar.gz" % (releaseDir, version))

    # add stripped symbols
    if operatingsys == "LINUX":
        os.makedirs(releaseDir + "/other")
        get("%s/voltdb/obj/release/voltdb-%s.sym" % (builddir, version),
            "%s/other/%s-voltdb-voltkv-%s.sym" % (releaseDir, operatingsys, version))

def copyEnterpriseFilesToReleaseDir(releaseDir, version, operatingsys):
    get("%s/pro/obj/pro/voltdb-ent-%s.tar.gz" % (builddir, version),
        "%s/%s-voltdb-ent-%s.tar.gz" % (releaseDir, operatingsys, version))

def copyTrialLicenseToReleaseDir(releaseDir):
    get("%s/pro/trial_*.xml" % (builddir),
        "%s/license.xml" % (releaseDir))


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

# pass -o if you want the build put in the one-offs directory
# passing different voltdb and pro trees also forces one-off
if '-o' in sys.argv:
    oneOff = True
    sys.argv.remove('-o')
else:
    oneOff = False

if len(sys.argv) == 2:
    createCandidate = False
    proTreeish = sys.argv[1]
    voltdbTreeish = sys.argv[1]
if len(sys.argv) == 3:
    createCandidate = False
    voltdbTreeish = sys.argv[1]
    proTreeish = sys.argv[2]
    if voltdbTreeish != proTreeish:
        oneOff = True     #force oneoff when not same tag/branch

print "Building with pro: %s and voltdb: %s" % (proTreeish, voltdbTreeish)

versionCentos = "unknown"
versionMac = "unknown"
releaseDir = "unknown"

# get ssh config [key_filename, hostname]
CentosSSHInfo = getSSHInfoForHost("volt5f")
MacSSHInfo = getSSHInfoForHost("voltmini")
UbuntuSSHInfo = getSSHInfoForHost("volt12d")

# build kits on 5f
with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
    versionCentos = checkoutCode(voltdbTreeish, proTreeish)
    if oneOff:
        releaseDir = "%s/releases/one-offs/%s-%s-%s" % \
            (os.getenv('HOME'), versionCentos, voltdbTreeish, proTreeish)
    else:
        releaseDir = os.getenv('HOME') + "/releases/" + voltdbTreeish
    makeReleaseDir(releaseDir)
    print "VERSION: " + versionCentos
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, versionCentos, "LINUX")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, versionCentos, "LINUX")
    makeTrialLicense()
    copyTrialLicenseToReleaseDir(releaseDir)

# build kits on the mini
with settings(user=username,host_string=MacSSHInfo[1],disable_known_hosts=True,key_filename=MacSSHInfo[0]):
    versionMac = checkoutCode(voltdbTreeish, proTreeish)
    assert versionCentos == versionMac
    buildCommunity()
    copyCommunityFilesToReleaseDir(releaseDir, versionMac, "MAC")
    buildPro()
    copyEnterpriseFilesToReleaseDir(releaseDir, versionMac, "MAC")

# build debian kit
with settings(user=username,host_string=UbuntuSSHInfo[1],disable_known_hosts=True,key_filename=UbuntuSSHInfo[0]):
    debbuilddir = "%s/deb_build/" % builddir
    run("rm -rf " + debbuilddir)
    run("mkdir -p " + debbuilddir)

    with cd(debbuilddir):
        put ("tools/voltdb-install.py",".")

        commbld = "%s-voltdb-%s.tar.gz" % ('LINUX', versionCentos)
        put("%s/%s" % (releaseDir, commbld),".")
        run ("sudo python voltdb-install.py -D " + commbld)
        get("voltdb_%s-1_amd64.deb" % (versionCentos), releaseDir)

        entbld = "%s-voltdb-ent-%s.tar.gz" % ('LINUX', versionCentos)
        put("%s/%s" % (releaseDir, entbld),".")
        run ("sudo python voltdb-install.py -D " + entbld)
        get("voltdb-ent_%s-1_amd64.deb" % (versionCentos), releaseDir)

# build rpm kit
with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
    rpmbuilddir = "%s/rpm_build/" % builddir
    run("rm -rf " + rpmbuilddir)
    run("mkdir -p " + rpmbuilddir)

    with cd(rpmbuilddir):
        put ("tools/voltdb-install.py",".")

        commbld = "%s-voltdb-%s.tar.gz" % ('LINUX', versionCentos)
        put("%s/%s" % (releaseDir, commbld),".")
        run ("python2.6 voltdb-install.py -R " + commbld)
        get("voltdb-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

        entbld = "%s-voltdb-ent-%s.tar.gz" % ('LINUX', versionCentos)
        put("%s/%s" % (releaseDir, entbld),".")
        run ("python2.6 voltdb-install.py -R " + entbld)
        get("voltdb-ent-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

computeChecksums(releaseDir)
#archiveDir = os.path.join(os.getenv('HOME'), "releases", "archive", voltdbTreeish, versionCentos)
#backupReleaseDir(releaseDir, archiveDir, versionCentos)
