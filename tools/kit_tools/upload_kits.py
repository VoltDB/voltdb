#!/usr/bin/env python


from fabric.api import run, local, get, put, env, cd, settings, quiet
from fabric_ssh_config import getSSHInfoForHost
import os


if not env.hosts:
    exit("FATAL: Missing -H server. You must specifiy the host to upload the kits to")

sshinfo = getSSHInfoForHost(env.hosts[0])

def str_option_to_bool(opt):
    if opt in ('True', 'true', '1', 1, 'on', True):
        return True
    elif opt in ('False', 'false', '0', 0, 'off', False):
        return False
    else:
        raise "invalid option setting"



def upload_kits(version,
                editionType="community",
                upload_type="ent",
                remote_dir="/var/www/downloads.voltdb.com/technologies/server",
                kit_dir="/home/test/releases/released",
                dry_run='False'):

    """ Upload kits to the voltdb download site.
        Args:
            version - version number such as 7.1.2
            upload_type - upload types are "ent" (only ent kit) and "all" for all kits
            remote_dir - directory to upload to
            kits_dir - directory to pull kits from
            dry_run - if true, the upload will not be done


    """
    if not version:
        exit ("FATAL: You must specify a version number that exists in ~tests/releases/released")

    #Any upload_type starting with ent will work
    #This allows the Jenkins selector to be "ent_only"
    upload_type = upload_type[:3]
    if upload_type not in ("ent", "all"):
        exit("FATAL: Upload upload_type must be enterprise or all")

    communityEditionType = os.path.join("voltdb-" + editionType + "-%s.tar.gz")

    dry_run = str_option_to_bool(dry_run)
    ent_kits = ["voltdb-ent-%s.tar.gz","voltdb-ent-%s.SHA256SUM","LINUX-voltdb-ent-%s.tar.gz","MAC-voltdb-ent-%s.tar.gz"]
    all_kits = ent_kits + ["voltdb-pro-%s.tar.gz","voltdb-pro-%s.SHA256SUM", "voltdb-%s.tar.gz", communityEditionType]
    kits_home = kit_dir + "/voltdb-" + version
    count=0

    with quiet():
        dircheck = local("ls -d " + kits_home, capture=True)
    if dircheck.failed:
        exit ("FATAL: " + dircheck.stderr)


    #For all the files in ent_kits or all_kits upload the files
    for k in eval(upload_type + "_kits"):
        f = os.path.join(kits_home, k) % version
        #print "====testing" + f
        with quiet():
            filecheck = local("ls " + f)

        if filecheck.succeeded:
            count += 1
            if dry_run:
                print "DRYRUN:" + f + "  -->  " + remote_dir
            else:
                with cd(remote_dir):
                    put(f, k % version)


    if not count:
        print "Maybe the kits in kits_home do not match version " + version
        print "This can happen if your tag does not match your version.txt"
        print
        print "Maybe these files need to be renamed:"
        local("ls " + kits_home)
        exit ("FATAL: No files uploaded")
