#!/usr/bin/env python

import os, sys, shutil
from fabric.api import run, cd, local, get, settings, lcd
from fabric_ssh_config import getSSHInfoForHost

verifydir = "/tmp/" + os.getenv('USER') + "/verifytemp"
version = "5.1"

################################################
# SETUP A DIST & TOOLS IN A TEMP DIR
################################################

def setupVerifyDir(operatingsys, kitname):
    global verifydir
    global version
    # clean out the existing dir
    run("rm -rf " + verifydir)
    # make the build dir again
    run("mkdir -p " + verifydir)
    with cd(verifydir):
        run("svn co https://svn.voltdb.com/eng/trunk/tools/kit_tools")
        run("curl -C - -O http://volt0/kits/candidate/%s-%s.tar.gz" % (operatingsys, kitname))
        run("tar -xzf %s-%s.tar.gz" % (operatingsys, kitname))
        run("rm %s-%s.tar.gz" % (operatingsys, kitname))
        run("mv %s dist" % (kitname))

################################################
# RUN TESTS
################################################

def runTests():
    global verifydir
    global version
    buildString = "Build: " + version
    with cd(verifydir + "/dist/doc/tutorials/auction"):
        run("%s/kit_tools/auction.exp \"%s\" || exit 1" % (verifydir, buildString))
        run("%s/kit_tools/auction.sh || exit 1" % (verifydir))
    with cd(verifydir + "/dist/tools"):
        run("%s/kit_tools/generate.exp \"%s\" || exit 1" % (verifydir, buildString))
    with cd(verifydir + "/dist/examples/voter"):
        run("%s/kit_tools/voter.exp \"%s\" || exit 1" % (verifydir, buildString))
    with cd(verifydir + "/dist/examples/voltkv"):
        run("%s/kit_tools/voltkv.exp \"%s\" || exit 1" % (verifydir, buildString))

# get ssh config
volt5f = getSSHInfoForHost("volt5f")
voltmini = getSSHInfoForHost("voltmini")

# test kits on 5f
with settings(host_string=volt5f[1],disable_known_hosts=True,key_filename=volt5f[0]):
    setupVerifyDir("LINUX", "voltdb-%s" % version)
    runTests()
    setupVerifyDir("LINUX", "voltdb-ent-%s" % version)
    runTests()

# test kits on mini
with settings(host_string=voltmini[1],disable_known_hosts=True,key_filename=voltmini[0]):
    setupVerifyDir("MAC", "voltdb-%s" % version)
    runTests()
    setupVerifyDir("MAC", "voltdb-ent-%s" % version)
    runTests()
