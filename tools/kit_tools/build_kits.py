#!/usr/bin/env python

import argparse, datetime, getpass, os, sys, shutil, traceback
from fabric.api import run, cd, local, get, settings, lcd, put
from fabric_ssh_config import getSSHInfoForHost
from fabric.context_managers import shell_env
from fabric.utils import abort

#TODO: These should not be globals

#Login as user test, but build in a directory by real username
username = 'test'
builddir = "/tmp/" + getpass.getuser() + "Kits/buildtemp"
version = "UNKNOWN"
nativelibdir = "/nativelibs/obj"  #  ~test/libs/... usually
defaultlicensedays = 70 #default trial license length


################################################
# Single repo checkout
################################################
# Try to checkout shallow
# If it's a sha, this won't work, so try that
# Returns checkout Success (boolean)

def repoCheckout(repo, treeish):
    #Try a shallow clone (only works with branch names)
    print "Trying a shallow single branch clone"
    result = run("git clone -q %s --depth=1 --branch %s --single-branch "% (repo, treeish), warn_only=True)
    if result.failed:
        #Maybe it was a sha and needs a full clone
        try:
            #Check if it is hex
            int(treeish, 16)
        except ValueError:
            return False
        #Okay, it's hex, so lets try a real clone + checkout
        print "Trying a full repo clone"
        run("git clone -q %s" % repo)
        directory = repo.split("/")[-1].split(".")[0]
        result = run("cd %s; git checkout %s" % (directory, treeish), warn_only=True)
        if result.failed:
            return False
    return True



################################################
# CHECKOUT CODE INTO A TEMP DIR
################################################

def checkoutCode(voltdbGit, proGit, gitloc):
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
        if voltdbGit:
            repo = gitloc + "/internal.git"
            checkout_succeeded = repoCheckout(repo, voltdbGit)
            if not checkout_succeeded:
                message += "\nCheckout of '%s' from %s repository failed." % (voltdbGit, repo)
        if proGit:
            repo = gitloc + "/pro.git"
            checkout_succeeded = repoCheckout(repo, proGit)
            if not checkout_succeeded:
                message += "\nCheckout of '%s' from %s repository failed." % (proGit, repo)

        if len(message) > 0:
            abort(message)

        return run("cat internal/version.txt").strip()

################################################
# MAKE A RELEASE DIR
################################################

def makeReleaseDir(releaseDir):
    # handle the case where a release dir exists for this version
    if os.path.exists(releaseDir):
        shutil.rmtree(releaseDir)
    # create a release dir
    os.makedirs(releaseDir)
    local("chmod 755 %s" % releaseDir)
    print "Created dir: " + releaseDir


################################################
# CHANGES FOR A WHITELABEL BUILD
################################################

def changesWhitelabel(builddir, package_mac):
    uifile = (builddir + "/pro/tools/whitelabel-logo.png")
    uifinalfile = (builddir + "/internal/src/frontend/org/voltdb/dbmonitor/images/whitelabel-logo.png")
    indexfile = (builddir + "/internal/src/frontend/org/voltdb/dbmonitor/index.htm")
    asciifile = (builddir + "/internal/src/frontend/org/voltdb/utils/voltdb_logstrings.properties")
    buildpro = (builddir + "/pro/tools/jenkins-pipelines/buildupdate.sh")
    antcommunity = (builddir + "/internal/build.xml")
    antpro = (builddir + "/pro/mmt.xml")
    run("cp -rf %s %s" % (uifile, uifinalfile))
    #Replace values in index.htm for UI changes
    run("chmod 775 %s" %(buildpro))
    run("sh %s %s %s %s" % (buildpro, indexfile, antcommunity, antpro))
    #Replace ascii start logo
    if package_mac:
        f="-i ''"
    else:
        f="-i"
    run("sed %s '/^\host_VoltDB_StartupString = /s/=.*$/= REPLACE HERE/g' %s" % (f, asciifile)) 
    run(r"sed -E %s 's#REPLACE HERE# Initializing VoltDB...\\\\n\\\\n   _____ __                                  __              \\\\n  / ___// /_  __  ______ _____  ____ _____  / /_  ____ _____ \\\\n  \\\\\\\\\\__ \\\\\\\\\\/ __ \\\\\\\\\\/ / / / __ `/ __ \\\\\\\\\\/ __ `/_  / / __ \\\\\\\\\\/ __ `/ __ \\\\\\\\\\ \\\\n ___/ / / / / /_/ / /_/ / / / / /_/ / / /_/ / / / /_/ / /_/ /\\\\n/____/_/ /_/\\\\\\\\\\__,_/\\\\\\\\\\__,_/_/ /_/\\\\\\\\\\__, / /___/_/ /_/\\\\\\\\\\__,_/\\\\\\\\\\____/ \\\\n                             /____/                          \\\\n\\\\n--------------------------------------------------------------#g'  %s" % (f, asciifile))


################################################
# BUILD THE COMMUNITY VERSION
################################################

def buildCommunity(edition_type, ee_only=False):
    if build_mac:
        packageMacLib="true"
    else:
        packageMacLib="false"
    if build_whitelabel:
        whitelabel_type="true"
    else:
        whitelabel_type="false"
    with cd(builddir + "/internal"):
        run("pwd")
        run("git status")
        if ee_only:
            run("ant -Dvoltdb_editiontype=%s -Djmemcheck=NO_MEMCHECK -Dkitbuild=%s -Dvoltdb_whitelabeltype=%s %s clean ee" % (edition_type, packageMacLib, whitelabel_type, build_args))
        else:
            run("ant -Dvoltdb_editiontype=%s -Djmemcheck=NO_MEMCHECK -Dkitbuild=%s -Dvoltdb_whitelabeltype=%s %s clean default dist" % (edition_type, packageMacLib, whitelabel_type, build_args))


################################################
# BUILD THE ENTERPRISE VERSION
################################################

def buildEnterprise(version):
    licensee="VoltDB Enterprise Trial User " + version
    if build_mac:
        packageMacLib="true"
    else:
        packageMacLib="false"
    if build_whitelabel:
        whitelabel_type="true"
    else:
        whitelabel_type="false"
    with cd(builddir + "/pro"):
        run("pwd")
        run("git status")
        run("VOLTCORE=../internal ant -f mmt.xml \
        -Djmemcheck=NO_MEMCHECK \
        -DallowDrReplication=true -DallowDrActiveActive=true \
        -Dlicensedays=%d -Dlicensee='%s' \
        -Dkitbuild=%s -Dvoltdb_whitelabeltype=%s %s \
        clean dist.pro" \
            % (defaultlicensedays, licensee, packageMacLib, whitelabel_type, build_args))


################################################
# MAKE AN ENTERPRISE TRIAL LICENSE
################################################
#This creates a trial license to put in the
#downloads for internal use only

# Must be called after buildEnterprise has been done
# Modified for V3 license format
def makeTrialLicense(licensee, days=30, dr_and_xdcr="true", nodes=12):
    timestring = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    filename = 'trial_' + timestring + '.xml'
    drtype = 'XDCR' if dr_and_xdcr else 'none'
    with cd(builddir + "/pro"):
        run("ant -f licensetool.xml createlicense \
        -Dfilename=%s -Dlicensetype='Trial' -DisTrial=true \
        -DdrType=%s -Dlicensedays=%d -Dlicensee='%s' \
        -Dnote='VoltDB Internal Use Only'" % (filename, drtype, days, licensee))
        return filename

################################################
# MAKE A SHA256 checksum
################################################

def makeSHA256SUM(version, type):
    if build_whitelabel:
        dir_base="shinetech"
    else:
        dir_base="voltdb"
    with cd(builddir + "/pro/obj/pro"):
        kitname= dir_base + "-" +  type + "-" + version
        run("sha256sum -b %s.tar.gz > %s.SHA256SUM" % (kitname, kitname))

################################################
# MAKE AN JAR FILES NEEDED TO PUSH TO MAVEN
################################################

def makeMavenJars():
    with cd(builddir + "/internal"):
        run("VOLTCORE=../internal ant -f build.xml maven-jars")
        run("VOLTCORE=../internal ant -f build-client.xml maven-jars")

################################################
# COPY FILES
################################################

def copyFilesToReleaseDir(releaseDir, version, type=None):
    print "Copying files to releaseDir"
    if type:
        typeString="-" + type
    else:
        typeString=""

    if build_whitelabel:
        dir_base="shinetech"
    else:
        dir_base="voltdb"

    get("%s/pro/obj/pro/%s%s-%s.tar.gz" % (builddir, dir_base, typeString, version),
        "%s/%s%s-%s.tar.gz" % (releaseDir, dir_base, typeString, version))
    get("%s/pro/obj/pro/%s%s-%s.SHA256SUM" % (builddir, dir_base, typeString, version),
        "%s/%s%s-%s.SHA256SUM" % (releaseDir, dir_base, typeString, version))
    # make don't allow group memebers to delete the directory, the default
    # permissions are 664
    local("chmod 755 %s" % releaseDir)

def copyCommunityFilesToReleaseDir(releaseDir, version, edition_type, operatingsys):
    if build_whitelabel:
        dir_base="shinetech"
    else:
        dir_base="voltdb"
    get("%s/internal/obj/release/%s-%s-%s.tar.gz" % (builddir, dir_base, edition_type, version),
        "%s/%s-%s-%s.tar.gz" % (releaseDir, dir_base, edition_type, version))

    # add stripped symbols
    if operatingsys == "LINUX":
        os.makedirs(releaseDir + "/other")
        get("%s/internal/obj/release/voltdb-%s.sym" % (builddir, version),
            "%s/other/%s-voltdb-voltkv-%s.sym" % (releaseDir, operatingsys, version))

def copyTrialLicenseToReleaseDir(licensefile, releaseDir):
    get(licensefile,
        "%s/license.xml" % (releaseDir))

def copyMavenJarsToReleaseDir(releaseDir, version):
    #The .jars and upload file must be in a directory called voltdb - it is the projectname
    mavenProjectDir = releaseDir + "/mavenjars/voltdb"
    if not os.path.exists(mavenProjectDir):
        os.makedirs(mavenProjectDir)

    #Get the upload.gradle file
    get("%s/internal/tools/kit_tools/upload.gradle" % (builddir),
        "%s/upload.gradle" % (mavenProjectDir))

    #Get the voltdbclient-n.n.jar from the recently built community build
    get("%s/internal/obj/release/dist-client-java/voltdb/voltdbclient-%s.jar" % (builddir, version),
        "%s/voltdbclient-%s.jar" % (mavenProjectDir, version))
    #Get the client's src and javadoc .jar files
    get("%s/internal/obj/release/voltdbclient-%s-javadoc.jar" % (builddir, version),
        "%s/voltdbclient-%s-javadoc.jar" % (mavenProjectDir, version))
    get("%s/internal/obj/release/voltdbclient-%s-sources.jar" % (builddir, version),
        "%s/voltdbclient-%s-sources.jar" % (mavenProjectDir, version))

    #Get the voltdb-n.n.jar from the recently built community build
    get("%s/internal/voltdb/voltdb-%s.jar" % (builddir, version),
        "%s/voltdb-%s.jar" % (mavenProjectDir, version))
    #Get the server's src and javadoc .jar files
    get("%s/internal/obj/release/voltdb-%s-javadoc.jar" % (builddir, version),
        "%s/voltdb-%s-javadoc.jar" % (mavenProjectDir, version))
    get("%s/internal/obj/release/voltdb-%s-sources.jar" % (builddir, version),
        "%s/voltdb-%s-sources.jar" % (mavenProjectDir, version))

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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = "Create a full kit. With no args, will do build of master")
    parser.add_argument('voltdb_sha', nargs="?", default="master", help="voltdb repository commit, tag or branch" )
    parser.add_argument('pro_sha', nargs="?", default="master", help="pro repository commit, tag or branch" )
    parser.add_argument('-g','--gitloc', default="git@github.com:VoltDB", help="Repository location. For example: /home/github-mirror")
    parser.add_argument('--nomac', action='store_true', help="Don't build Mac OSX")
    parser.add_argument('--nocommunity', action='store_true', help="Don't build community")
    parser.add_argument('--whitelabel', action='store_true', help="Do a whitelabel build")
    parser.add_argument('-l','--includelicense', action='store_true', help="Include a trial license in the Enterprise kit")
    args = parser.parse_args()


    proTreeish = args.pro_sha
    voltdbTreeish = args.voltdb_sha

    print args

    build_community = not args.nocommunity
    build_mac = not args.nomac
    build_whitelabel = args.whitelabel

    #If anything is missing we're going to dump this in oneoffs dir.
    build_all = build_community and build_mac
    if voltdbTreeish != proTreeish or not build_all:
        oneOff = True
    else:
        oneOff  = False

    rmNativeLibs()

    #Set a bunch of build_args
    build_args=""
    try:
        build_args = os.environ['VOLTDB_BUILD_ARGS']
    except:
        pass

    if not args.includelicense:
        build_args += " -Dtriallicense=false"

    print "Building with pro: %s and voltdb: %s" % (proTreeish, voltdbTreeish)

    build_errors=False

    editionType = "developer"
    versionCentos = "unknown"
    versionMac = "unknown"
    releaseDir = "unknown"

    # get ssh config [key_filename, hostname]
    CentosSSHInfo = getSSHInfoForHost("buildkits_C7")
    MacSSHInfo = getSSHInfoForHost("voltmini")
    UbuntuSSHInfo = getSSHInfoForHost("volt12d")

    # build community kit on the mini so that .so can be picked up for unified kit
    if build_mac or build_community:
        try:
            with settings(user=username,host_string=MacSSHInfo[1],disable_known_hosts=True,key_filename=MacSSHInfo[0]):
                if build_whitelabel:
                    try:
                       package_mac = True
                       versionMac = checkoutCode(voltdbTreeish, proTreeish, args.gitloc)
                       changesWhitelabel(builddir, package_mac)
                    except Exception as e:
                        print traceback.format_exc()
                        print "Could not do changes for a whitelabel build"
                        build_errors=True
                else:
                    versionMac = checkoutCode(voltdbTreeish, None, args.gitloc)
                buildCommunity(editionType, ee_only=True)
        except Exception as e:
            print traceback.format_exc()
            print "Could not build MAC kit. Exception: " + str(e) + ", Type: " + str(type(e))
            build_errors=True

    # build kits on 15f
    try:
        with settings(user=username,host_string=CentosSSHInfo[1],disable_known_hosts=True,key_filename=CentosSSHInfo[0]):
            versionCentos = checkoutCode(voltdbTreeish, proTreeish, args.gitloc)
            if build_mac:
                assert versionCentos == versionMac

            if oneOff:
                releaseDir = "%s/releases/one-offs/%s-%s-%s" % \
                    (os.getenv('HOME'), versionCentos, voltdbTreeish, proTreeish)
            else:
                releaseDir = os.getenv('HOME') + "/releases/" + voltdbTreeish
            makeReleaseDir(releaseDir)
            #print "VERSION: " + versionCentos
            if build_whitelabel:
                try:
                    package_mac = False
                    changesWhitelabel(builddir, package_mac)
                except Exception as e:
                    print traceback.format_exc()
                    print "Could not do changes for a whitelabel build"
                    build_errors=True
            if build_community:
                buildCommunity(editionType)
                copyCommunityFilesToReleaseDir(releaseDir, versionCentos, editionType, "LINUX")                
                makeMavenJars()
                copyMavenJarsToReleaseDir(releaseDir, versionCentos)
            buildEnterprise(versionCentos)
            makeSHA256SUM(versionCentos,"ent")
            copyFilesToReleaseDir(releaseDir, versionCentos, "ent")
            licensefile = makeTrialLicense(licensee="VoltDB Internal Use Only " + versionCentos)
            copyTrialLicenseToReleaseDir(builddir + "/pro/" + licensefile, releaseDir)

    except Exception as e:
        print traceback.format_exc()
        print "Could not build LINUX kit. Exception: " + str(e) + ", Type: " + str(type(e))
        build_errors=True

    rmNativeLibs()      # cleanup imported native libs so not picked up unexpectedly by other builds

    exit (build_errors)
    #archiveDir = os.path.join(os.getenv('HOME'), "releases", "archive", voltdbTreeish, versionCentos)
    #backupReleaseDir(releaseDir, archiveDir, versionCentos)
