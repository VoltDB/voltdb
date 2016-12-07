#!/usr/bin/env python

import argparse, datetime, os, sys, shutil, traceback
from fabric.api import run, cd, local, get, settings, lcd, put
from fabric_ssh_config import getSSHInfoForHost
from fabric.context_managers import shell_env
from fabric.utils import abort

username='test'
builddir = "/tmp/" + username + "Kits/buildtemp"
version = "UNKNOWN"
nativelibdir = "/nativelibs/obj"  #  ~test/libs/... usually
defaultlicensedays = 70 #default trial license length


################################################
# CHECKOUT CODE INTO A TEMP DIR
################################################

def checkoutCode(voltdbGit, proGit, rbmqExportGit, gitloc):
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
        run("git clone -q %s/voltdb.git" % gitloc)
        result = run("cd voltdb; git checkout %s" % voltdbGit, warn_only=True)
        if result.failed:
            message = "VoltDB checkout failed. Missing branch %s." % rbmqExportGit

        run("git clone -q %s/pro.git" % gitloc)
        result = run("cd pro; git checkout %s" % proGit, warn_only=True)
        if result.failed:
            message += "\nPro checkout failed. Missing branch %s." % rbmqExportGit

        #rabbitmq isn't mirrored internally, so don't use gitloc
        run("git clone -q git@github.com:VoltDB/export-rabbitmq.git")
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
    if build_mac:
        packageMacLib="true"
    else:
        packageMacLib="false"
    with cd(builddir + "/voltdb"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("ant -Djmemcheck=NO_MEMCHECK -Dkitbuild=%s %s clean default dist" % (packageMacLib,  build_args))

################################################
# BUILD THE ENTERPRISE VERSION
################################################

def buildEnterprise():
    if build_mac:
        packageMacLib="true"
    else:
        packageMacLib="false"
    with cd(builddir + "/pro"):
        run("pwd")
        run("git status")
        run("git describe --dirty")
        run("VOLTCORE=../voltdb ant -f mmt.xml -Djmemcheck=NO_MEMCHECK -Dallowreplication=true -DallowDrActiveActive=true -Dlicensedays=%d -Dkitbuild=%s %s clean dist.pro" % (defaultlicensedays, packageMacLib, build_args))

################################################
# BUILD THE PRO VERSION
################################################

#
def packagePro(version):
    print "Making license"
    makeTrialLicense(days=defaultlicensedays, dr_and_xdcr=False, nodes=3)
    print "Repacking pro kit"
    with cd(builddir + "/pro/obj/pro"):
        run("mkdir pro_kit_staging")
    with cd(builddir + "/pro/obj/pro/pro_kit_staging"):
        run("tar xf ../voltdb-ent-%s.tar.gz" % version)
        run("mv voltdb-ent-%s voltdb-pro-%s" % (version, version))
        run("cp %s/pro/trial_*.xml voltdb-pro-%s/voltdb/license.xml" % (builddir, version))
        run("tar cvfz ../voltdb-pro-%s.tar.gz voltdb-pro-%s" % (version, version))

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

# Must be called after buildEnterprise has been done
def makeTrialLicense(days=30, dr_and_xdcr="true", nodes=12):
    with cd(builddir + "/pro/tools"):
        run("./make_trial_licenses.pl -t %d -H %d -W %s" % (days, nodes, dr_and_xdcr ))

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

def copyFilesToReleaseDir(releaseDir, version, type=None):
    print "Copying files to releaseDir"
    if type:
        typeString="-" + type
    else:
        typeString=""
    get("%s/pro/obj/pro/voltdb%s-%s.tar.gz" % (builddir, typeString, version),
        "%s/voltdb%s-%s.tar.gz" % (releaseDir, typeString, version))

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

parser = argparse.ArgumentParser(description = "Create a full kit. With no args, will do build of master")
parser.add_argument('voltdb_sha', nargs="?", default="master", help="voltdb repository commit, tag or branch" )
parser.add_argument('pro_sha', nargs="?", default="master", help="pro repository commit, tag or branch" )
parser.add_argument('rabbitmq_sha', nargs="?", default="master", help="rabbitmq repository commit, tag or branch" )
parser.add_argument('-g','--gitloc', default="git@github.com:VoltDB", help="Repository location. For example: /home/github-mirror")
parser.add_argument('--nomac', action='store_true', help="Don't build Mac OSX")
parser.add_argument('--nopackages', action='store_true', help="Don't build .rpm and .deb packages")
parser.add_argument('--nocommunity', action='store_true', help="Don't build community")
args = parser.parse_args()

proTreeish = args.voltdb_sha
voltdbTreeish = args.pro_sha
rbmqExportTreeish = args.rabbitmq_sha

print args

build_community = not args.nocommunity
build_mac = not args.nomac
build_packages = not args.nopackages

#If anything is missing we're going to dump this in oneoffs dir.
build_all = build_community and build_mac and build_packages
if voltdbTreeish != proTreeish or not build_all:
    oneOff = True
else:
    oneOff  = False

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
CentosSSHInfo = getSSHInfoForHost("volt15a")
MacSSHInfo = getSSHInfoForHost("voltmini")
UbuntuSSHInfo = getSSHInfoForHost("volt12d")

# build kits on the mini
if build_mac and build_community:
    try:
        with settings(user=username,host_string=MacSSHInfo[1],disable_known_hosts=True,key_filename=MacSSHInfo[0]):
            versionMac = checkoutCode(voltdbTreeish, proTreeish, rbmqExportTreeish, args.gitloc)
            buildCommunity()
    except Exception as e:
        print traceback.format_exc()
        print "Could not build MAC kit. Exception: " + str(e) + ", Type: " + str(type(e))
        build_errors=True

# build kits on 15f
try:
    with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
        versionCentos = checkoutCode(voltdbTreeish, proTreeish, rbmqExportTreeish, args.gitloc)
        if build_mac:
            assert versionCentos == versionMac

        if oneOff:
            releaseDir = "%s/releases/one-offs/%s-%s-%s" % \
                (os.getenv('HOME'), versionCentos, voltdbTreeish, proTreeish)
        else:
            releaseDir = os.getenv('HOME') + "/releases/" + voltdbTreeish
        makeReleaseDir(releaseDir)
        print "VERSION: " + versionCentos
        if build_community:
            buildCommunity()
            copyCommunityFilesToReleaseDir(releaseDir, versionCentos, "LINUX")
        buildEnterprise()
        buildRabbitMQExport(versionCentos)
        copyFilesToReleaseDir(releaseDir, versionCentos, "ent")

        packagePro(versionCentos)
        copyFilesToReleaseDir(releaseDir, versionCentos, "pro")
        makeTrialLicense()
        copyTrialLicenseToReleaseDir(releaseDir)
        if versionHasZipTarget():
            makeEnterpriseZip()
            copyEnterpriseZipToReleaseDir(releaseDir, versionCentos, "LINUX")
        makeMavenJars()
        copyMavenJarsToReleaseDir(releaseDir, versionCentos)

except Exception as e:
    print traceback.format_exc()
    print "Could not build LINUX kit. Exception: " + str(e) + ", Type: " + str(type(e))
    build_errors=True

if build_packages:
    # build debian kit
    try:
        with settings(user=username,host_string=UbuntuSSHInfo[1],disable_known_hosts=True,key_filename=UbuntuSSHInfo[0]):
            debbuilddir = "%s/deb_build/" % builddir
            run("rm -rf " + debbuilddir)
            run("mkdir -p " + debbuilddir)

            with cd(debbuilddir):
                put ("tools/voltdb-install.py",".")

                if build_community:
                    commbld = "voltdb-%s.tar.gz" % (versionCentos)
                    put("%s/%s" % (releaseDir, commbld),".")
                    run ("sudo python voltdb-install.py -D " + commbld)
                    get("voltdb_%s-1_amd64.deb" % (versionCentos), releaseDir)

                entbld = "voltdb-ent-%s.tar.gz" % (versionCentos)
                put("%s/%s" % (releaseDir, entbld),".")
                run ("sudo python voltdb-install.py -D " + entbld)
                get("voltdb-ent_%s-1_amd64.deb" % (versionCentos), releaseDir)
    except Exception as e:
        print traceback.format_exc()
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

                if build_community:
                    commbld = "voltdb-%s.tar.gz" % (versionCentos)
                    put("%s/%s" % (releaseDir, commbld),".")
                    run ("python2.6 voltdb-install.py -R " + commbld)
                    get("voltdb-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

                entbld = "voltdb-ent-%s.tar.gz" % (versionCentos)
                put("%s/%s" % (releaseDir, entbld),".")
                run ("python2.6 voltdb-install.py -R " + entbld)
                get("voltdb-ent-%s-1.x86_64.rpm" % (versionCentos), releaseDir)

    except Exception as e:
        print traceback.format_exc()
        print "Could not build rpm kit. Exception: " + str(e) + ", Type: " + str(type(e))
        build_errors=True

computeChecksums(releaseDir)

rmNativeLibs()      # cleanup imported native libs so not picked up unexpectedly by other builds

exit (build_errors)
#archiveDir = os.path.join(os.getenv('HOME'), "releases", "archive", voltdbTreeish, versionCentos)
#backupReleaseDir(releaseDir, archiveDir, versionCentos)
