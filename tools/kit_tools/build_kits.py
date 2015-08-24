#!/usr/bin/env python

import os, sys, shutil, datetime
from fabric.api import run, cd, local, get, settings, lcd, put
from fabric_ssh_config import getSSHInfoForHost
from fabric.context_managers import shell_env
from fabric.utils import abort

username='test'
builddir = "/tmp/" + username + "Kits/buildtemp"
version = "UNKNOWN"
nativelibdir = "/nativelibs/obj"  #  ~test/libs/... usually
defaultlicensedays = 45 #default trial license length

################################################
# CHECKOUT CODE INTO A TEMP DIR
################################################

def checkoutCode(voltdbGit, proGit, rbmqExportGit):
    global buildir
    # clean out the existing dir
    run("rm -rf " + builddir)
    # make the build dir again
    run("mkdir -p " + builddir)
    # change to it
    with cd(builddir):
        # do the checkouts, collect checkout errors on both community &
        # pro repos so user gets status on both checkouts
        message = ""
        run("git clone git@github.com:VoltDB/voltdb.git")
        result = run("cd voltdb; git checkout %s" % voltdbGit, warn_only=True)
        if result.failed:
            message = "VoltDB checkout failed. Missing branch %s." % rbmqExportGit

        run("git clone git@github.com:VoltDB/pro.git")
        result = run("cd pro; git checkout %s" % proGit, warn_only=True)
        if result.failed:
            message += "\nPro checkout failed. Missing branch %s." % rbmqExportGit

        run("git clone git@github.com:VoltDB/export-rabbitmq.git")
        result = run("cd export-rabbitmq; git checkout %s" % rbmqExportGit, warn_only=True)
        # Probably ok to use master for export-rabbitmq.
        if result.failed:
            print "\nExport-rabbitmg branch %s checkout failed. Defaulting to master." % rbmqExportGit

        if len(message) > 0:
            abort(message)

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
# SEE IF HAS ZIP TARGET
###############################################
def versionHasZipTarget():
    with settings(warn_only=True):
        with cd(os.path.join(builddir,'pro')):
                return run("ant -p -f mmt.xml | grep dist.pro.zip")

################################################
# BUILD THE COMMUNITY VERSION
################################################

def buildCommunity():
    with cd(builddir + "/voltdb"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("ant -Djmemcheck=NO_MEMCHECK -Dkitbuild=true %s clean default dist" % build_args)

################################################
# BUILD THE ENTERPRISE VERSION
################################################

def buildPro():
    with cd(builddir + "/pro"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("VOLTCORE=../voltdb ant -f mmt.xml -Djmemcheck=NO_MEMCHECK -Dallowreplication=true -Dlicensedays=%d -Dkitbuild=true %s clean dist.pro" % (defaultlicensedays, build_args))

################################################
# BUILD THE RABBITMQ EXPORT CONNECTOR
################################################

def buildRabbitMQExport(version):
    with cd(builddir + "/export-rabbitmq"):
        run("pwd")
        run("git status")
        run("git describe --dirty", warn_only=True)
        run("VOLTDIST=../pro/obj/pro/voltdb-ent-%s ant" % version)
    # Repackage the pro tarball and zip file with the RabbitMQ connector Jar
    with cd("%s/pro/obj/pro" % builddir):
        run("pwd")
        run("gunzip voltdb-ent-%s.tar.gz" % version)
        run("tar uvf voltdb-ent-%s.tar voltdb-ent-%s/lib/extension/voltdb-rabbitmq.jar" % (version, version))
        if versionHasZipTarget():
            run("gzip voltdb-ent-%s.tar" % version)
            run("zip -r voltdb-ent-%s.zip voltdb-ent-%s" % (version, version))

################################################
# MAKE AN ENTERPRISE TRIAL LICENSE
################################################

# Must be called after buildPro has been done
def makeTrialLicense(days=30):
    with cd(builddir + "/pro/tools"):
        run("./make_trial_licenses.pl -t %d -W" % (days))

################################################
# MAKE AN ENTERPRISE ZIP FILE FOR SOME PARTNER UPLOAD SITES
################################################

def makeEnterpriseZip():
    with cd(builddir + "/pro"):
        run("VOLTCORE=../voltdb ant -f mmt.xml dist.pro.zip")

################################################
# MAKE AN JAR FILES NEEDED TO PUSH TO MAVEN
################################################

def makeMavenJars():
    with cd(builddir + "/voltdb"):
        run("VOLTCORE=../voltdb ant -f build-client.xml maven-jars")

################################################
# COPY FILES
################################################

def copyCommunityFilesToReleaseDir(releaseDir, version, operatingsys):
    get("%s/voltdb/obj/release/voltdb-%s.tar.gz" % (builddir, version),
        "%s/voltdb-%s.tar.gz" % (releaseDir, version))
    get("%s/voltdb/obj/release/voltdb-client-java-%s.tar.gz" % (builddir, version),
        "%s/voltdb-client-java-%s.tar.gz" % (releaseDir, version))
    get("%s/voltdb/obj/release/voltdb-tools-%s.tar.gz" % (builddir, version),
        "%s/voltdb-tools-%s.tar.gz" % (releaseDir, version))

    # add stripped symbols
    if operatingsys == "LINUX":
        os.makedirs(releaseDir + "/other")
        get("%s/voltdb/obj/release/voltdb-%s.sym" % (builddir, version),
            "%s/other/%s-voltdb-voltkv-%s.sym" % (releaseDir, operatingsys, version))

def copyEnterpriseFilesToReleaseDir(releaseDir, version, operatingsys):
    get("%s/pro/obj/pro/voltdb-ent-%s.tar.gz" % (builddir, version),
        "%s/voltdb-ent-%s.tar.gz" % (releaseDir, version))

def copyTrialLicenseToReleaseDir(releaseDir):
    get("%s/pro/trial_*.xml" % (builddir),
        "%s/license.xml" % (releaseDir))

def copyEnterpriseZipToReleaseDir(releaseDir, version, operatingsys):
    get("%s/pro/obj/pro/voltdb-ent-%s.zip" % (builddir, version),
        "%s/voltdb-ent-%s.zip" % (releaseDir, version))

def copyMavenJarsToReleaseDir(releaseDir, version):
    #The .jars and upload file must be in a directory called voltdb - it is the projectname
    mavenProjectDir = releaseDir + "/mavenjars/voltdb"
    if not os.path.exists(mavenProjectDir):
        os.makedirs(mavenProjectDir)

    #Get the voltdbclient-n.n.jar from the recently built community build
    get("%s/voltdb/obj/release/dist-client-java/voltdb/voltdbclient-%s.jar" % (builddir, version),
        "%s/voltdbclient-%s.jar" % (mavenProjectDir, version))
    #Get the upload.gradle file
    get("%s/voltdb/tools/kit_tools/upload.gradle" % (builddir),
        "%s/upload.gradle" % (mavenProjectDir))
    #Get the src and javadoc .jar files
    get("%s/voltdb/obj/release/voltdbclient-%s-javadoc.jar" % (builddir, version),
        "%s/voltdbclient-%s-javadoc.jar" % (mavenProjectDir, version))
    get("%s/voltdb/obj/release/voltdbclient-%s-sources.jar" % (builddir, version),
        "%s/voltdbclient-%s-sources.jar" % (mavenProjectDir, version))

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
# REMOVE NATIVE LIBS FROM SHARED DIRECTORY
################################################

def rmNativeLibs():
    # local("ls -l ~" + username + nativelibdir)
    local("rm -rf ~" + username + nativelibdir)

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
rbmqExportTreeish = "master"

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
    rbmqExportTreeish = sys.argv[1]
if len(sys.argv) == 3:
    createCandidate = False
    voltdbTreeish = sys.argv[1]
    proTreeish = sys.argv[2]
    rbmqExportTreeish = sys.argv[2]
    if voltdbTreeish != proTreeish:
        oneOff = True     #force oneoff when not same tag/branch

rmNativeLibs()

try:
    build_args = os.environ['VOLTDB_BUILD_ARGS']
except:
    build_args=""

print "Building with pro: %s and voltdb: %s" % (proTreeish, voltdbTreeish)

build_errors=False

versionCentos = "unknown"
versionMac = "unknown"
releaseDir = "unknown"

# get ssh config [key_filename, hostname]
CentosSSHInfo = getSSHInfoForHost("volt5f")
MacSSHInfo = getSSHInfoForHost("voltmini")
UbuntuSSHInfo = getSSHInfoForHost("volt12d")

# build kits on the mini
try:
    with settings(user=username,host_string=MacSSHInfo[1],disable_known_hosts=True,key_filename=MacSSHInfo[0]):
        versionMac = checkoutCode(voltdbTreeish, proTreeish, rbmqExportTreeish)
        buildCommunity()
except Exception as e:
    print "Could not build MAC kit. Exception: " + str(e) + ", Type: " + str(type(e))
    build_errors=True

# build kits on 5f
try:
    with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
        versionCentos = checkoutCode(voltdbTreeish, proTreeish, rbmqExportTreeish)
        assert versionCentos == versionMac
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
        buildRabbitMQExport(versionCentos)
        copyEnterpriseFilesToReleaseDir(releaseDir, versionCentos, "LINUX")
        makeTrialLicense()
        copyTrialLicenseToReleaseDir(releaseDir)
        if versionHasZipTarget():
            makeEnterpriseZip()
            copyEnterpriseZipToReleaseDir(releaseDir, versionCentos, "LINUX")
        makeMavenJars()
        copyMavenJarsToReleaseDir(releaseDir, versionCentos)

except Exception as e:
    print "Could not build LINUX kit. Exception: " + str(e) + ", Type: " + str(type(e))
    build_errors=True

# build debian kit
try:
    with settings(user=username,host_string=UbuntuSSHInfo[1],disable_known_hosts=True,key_filename=UbuntuSSHInfo[0]):
        debbuilddir = "%s/deb_build/" % builddir
        run("rm -rf " + debbuilddir)
        run("mkdir -p " + debbuilddir)

        with cd(debbuilddir):
            put ("tools/voltdb-install.py",".")

            commbld = "voltdb-%s.tar.gz" % (versionCentos)
            put("%s/%s" % (releaseDir, commbld),".")
            run ("sudo python voltdb-install.py -D " + commbld)
            get("voltdb_%s-1_amd64.deb" % (versionCentos), releaseDir)

            entbld = "voltdb-ent-%s.tar.gz" % (versionCentos)
            put("%s/%s" % (releaseDir, entbld),".")
            run ("sudo python voltdb-install.py -D " + entbld)
            get("voltdb-ent_%s-1_amd64.deb" % (versionCentos), releaseDir)
except Exception as e:
    print "Could not build debian kit. Exception: " + str(e) + ", Type: " + str(type(e))
    build_errors=True

try:
    # build rpm kit
    with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
        rpmbuilddir = "%s/rpm_build/" % builddir
        run("rm -rf " + rpmbuilddir)
        run("mkdir -p " + rpmbuilddir)

        with cd(rpmbuilddir):
            put ("tools/voltdb-install.py",".")

            commbld = "voltdb-%s.tar.gz" % (versionCentos)
            put("%s/%s" % (releaseDir, commbld),".")
            run ("python2.6 voltdb-install.py -R " + commbld)
            get("voltdb-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

            entbld = "voltdb-ent-%s.tar.gz" % (versionCentos)
            put("%s/%s" % (releaseDir, entbld),".")
            run ("python2.6 voltdb-install.py -R " + entbld)
            get("voltdb-ent-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

except Exception as e:
    print "Could not build rpm kit. Exception: " + str(e) + ", Type: " + str(type(e))
    build_errors=True

computeChecksums(releaseDir)

rmNativeLibs()      # cleanup imported native libs so not picked up unexpectedly by other builds

exit (build_errors)
#archiveDir = os.path.join(os.getenv('HOME'), "releases", "archive", voltdbTreeish, versionCentos)
#backupReleaseDir(releaseDir, archiveDir, versionCentos)
