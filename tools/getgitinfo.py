#!/usr/bin/python
import os
from subprocess import Popen, PIPE
import sys
import re

def getSvnInfo():
    """Make a build string for svn
    
    Returns a string or None, of not in an svn repository"""

    (urlbase, gitHash, dirty) = ("","","")    

    (svnStatus,stderr) = Popen("svn status", shell=True,stdout=PIPE, stderr=PIPE).communicate()

    # svn status returns an error if you're not in a repository
    if stderr:
        print "This not an svn working copy"
        return
    if svnStatus:
        print "This is a dirty svn working copy"
        dirty = "-dirty"

    (svnInfo,stderr) = Popen("svn info 2>/dev/null",shell=True, stdout=PIPE).communicate()
    
    
    for line in str.splitlines(svnInfo):
        if not len(line):
            continue
        (k, v) = line.split(": ")
        if k == "URL": urlbase = v
        if k == "Revision": revision = v

    return "%s?revision=%s%s" % (urlbase, revision, dirty)

def getGitInfo():
    """Make a build string for svn
    
    Returns a string or None if not in a git repository"""

    (urlbase, gitLocalVersion, local) = ("","","")    
    
    # git describe --dirty adds '-dirty' to the version string if uncommitted code is found
    (gitLocalVersion,stderr) = Popen("git describe --dirty", shell=True, stdout=PIPE, stderr=PIPE).communicate()
    if stderr:
        print "This is not a git working tree\n"
        return
    gitLocalVersion = gitLocalVersion.strip()

    #check if local repository == remote repository
        
    (gitLocalBranch, stderr) = Popen("git name-rev --name-only HEAD", 
                                         shell=True, stdout=PIPE, stderr=PIPE).communicate()
    gitLocalBranch = gitLocalBranch.strip()
    #print "git config --get branch.%s.remote" % (gitLocalBranch)

    (gitRemote, stderr) = Popen("git config --get branch.%s.remote" % (gitLocalBranch), 
                                    shell=True, stdout=PIPE, stderr=PIPE).communicate()
    gitRemote = gitRemote.strip()

    if not gitRemote:  #if there is no remote, then this is a local-only branch
        local = "-local"
    else:       # if there is a remote branch, see if it has the same hash
        (gitRemoteVersion, stderr) = Popen("git describe %s" % gitRemote , 
                                            shell=True, stdout=PIPE, stderr=PIPE).communicate()
        gitRemoteVersion = gitRemoteVersion.strip()
        if gitRemoteVersion != gitLocalVersion[:len(gitRemoteVersion)]:
            local = "-local"
            
    return "%s%s" % (gitLocalVersion, local)


def saveBuildInfo(version, build):
    """ Save the information to a buildstring.txt file

    """



if __name__ == "__main__":
    buildstring = None
    version = "0.0.0"

    if (len(sys.argv) > 1):
        version = sys.argv[1]

    buildstring = getGitInfo()
    if  not buildstring:
        buildstring = getSvnInfo()
        if not buildstring:
            buildstring = "This is not from a known repository"

    vfile = open("version.txt", "r")
    version = vfile.readline().strip()
    vfile.close()

    bfile = open("buildstring.txt", "w")
    bfile.write ("%s %s\n"% (version,buildstring))
    bfile.close()


    print "Version: ",version,buildstring

