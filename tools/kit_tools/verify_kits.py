#!/usr/bin/env python

import os, sys, shutil
from fabric.api import run, cd, local, get, settings, lcd
from fabric_ssh_config import getSSHInfoForHost

verifydir = "/tmp/" + os.getenv('USER') + "/verifytemp"
version = "2.0"

################################################
# SETUP A DIST & TOOLS IN A TEMP DIR
################################################

def setupVerifyDir(kitname):
    global verifydir
    global version
    # clean out the existing dir
    run("rm -rf " + verifydir)
    # make the build dir again
    run("mkdir -p " + verifydir)
    with cd(verifydir):
        run("svn co https://svn.voltdb.com/eng/trunk/tools/kit_tools")
        run("curl -C - -O http://volt0/kits/%s.tar.gz" % (kitname))
        run("tar -xzf %s.tar.gz" % (kitname))
        rum("rm %s.tar.gz" % (kitname))
        run("mv %s dist" % (kitname))

# get ssh config
volt5f = getSSHInfoForHost("volt5f")
voltmini = getSSHInfoForHost("voltmini")

# build kits on 5f
with settings(host_string=volt5f[1],disable_known_hosts=True,key_filename=volt5f[0]):
    setupVerifyDir("voltdb-%s" % version);

