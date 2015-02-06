#!/usr/bin/env python
#
# This script contains fabric tasks for making a new VMWare image for VoltDB.
# The main task is make_new_vmimage, but many of the tasks are separately callable.
#
# fab -f build_new_vmware.py --list #to list all tasks
#
# To make a new vmware image from an existing image:
# fab -f build_new_vmware.py make_new_vmimage:4.8,~/vmware/downloads/VoltDB-Ubuntu-x64-14.04-LTS-v4.7-Demo
#
# Note: vmware workstation must be installed and DISPLAY must be set for this to work
#
from fabric.api import hide, local, task
import fnmatch
import os
import pwd
import re
import tempfile

user='voltdb'
password='voltdb'
vmx=None

def _print_line():
    print "==============================================================="

def _get_vmx():
    vmxerror="""FATAL: You must set VMX in your environment
> export VMX=<path-to-.vmx-file>)
"""
    try:
        global vmx
        vmx = os.environ['VMX']
    except KeyError:
        exit(vmxerror)


def _find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(os.path.expanduser(path)):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

def _delete_files(files):
    for f in files:
        print f
        try:
            local('rm %s' % f)
        except Exception as e:
            print e
            pass

@task
def vm_start():
    """
    Start vm $VMX. Will start VMware if not already running
    """
    _get_vmx()
    local('export DISPLAY=%s;vmrun -T ws start %s' % (os.environ['DISPLAY'],vmx))

@task
def vm_clone(newvmx, clonename):
    """
    Clone vm $VMX. Usage: vm_clone:newvmx,clonename
    """
    _get_vmx()
    if not newvmx or not clonename:
        exit('FATAL: You must specify a new vmx (newvmx) and a new name (clonename)')
    newvmx=os.path.abspath(os.path.expanduser(newvmx))
    local('vmrun -T ws clone %s %s full --cloneName="%s"' % (vmx, newvmx, clonename))

@task
def vm_stop():
    """
    Stop vm $VMX.
    """
    if not vmx:
        exit('FATAL: You must set VMX in your environment')
    local("vmrun -T ws stop %s" % (vmx))

@task
def vm_cmd(cmd, quiet=False):
    """
    Run a bash command or script on $VMX. Usage: vm_cmd:cmd,[quiet=True]
    """
    _get_vmx()
    tf=tempfile.NamedTemporaryFile(prefix='vmrun-')
    tf.close()
    with hide('stderr'):
        local('vmrun -T ws -gu %s -gp %s runScriptInGuest %s /bin/bash "%s > %s 2>&1"' % (
                user, password, vmx, cmd, tf.name))
        if not quiet:
            vm_getfile(tf.name, tf.name)
        #ls local('vmrun -T ws -gu %s -gp %s copyFileFromGuestToHost %s %s %s' % (
        #        user, password, vmx, tf.name, tf.name ))
            local('cat %s' % tf.name)

@task
def vm_getfile(source, dest="."):
    """
    Copy a file from guest to host. Usage: vm_getfile:source,destfile|destdir
    """
    _get_vmx()
    dest = os.path.expanduser(dest)
    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(source))
    else:
        dest = os.path.abspath(dest)

    local('vmrun -T ws -gu %s -gp %s copyFileFromGuestToHost %s %s %s' % (
                user, password, vmx, source, dest ))

@task
def vm_putfile(source, dest="."):
    """
    Copy a file from host to guest. Usage: vm_getfile:source,destfile|destdir
    """
    _get_vmx()
    if os.path.isdir(dest):
        dest = os.path.join(dest,os.path.basename(source))
    else:
        dest = os.path.abspath(os.path.expanduser(dest))

    source=os.path.abspath(source)

    local('vmrun -T ws -gu %s -gp %s copyFileFromHostToGuest %s %s %s' % (
                user, password, vmx, source, dest ))

@task
def vm_update(version):
    """
    Do all the updates needed on the VM. Usage vm_update:version
    """

    _get_vmx()
    #Update
    #TODO: Make this work from anywhere. Right now it expects you to be in kit_tools
    login_script='demo-vmware-login.sh'
    vm_putfile(login_script, '/home/voltdb/.voltdbprofile')

    #Copy voltupdate.sh to the guest OS and run it. This script does all the heavy lifting
    #for updating packages, setting up system and voltdb and cleaning up after itself

    update_script='demo-vmware-update.sh'
    dest_script = '/tmp/demo-vmware-update.sh'
    vm_putfile(update_script, dest_script)
    vm_cmd('%s %s' % (dest_script, version))
    vm_cmd('rm -vf ~/.bash_history', quiet=True)
    vm_cmd('sudo rm -Rf /tmp/*', quiet=True)

@task
def vm_zerofilldisk(vmdk):
    """
    Mount, zerofill, unmount a vmdk: Usage: vm_zerofill:vmxdir
    """

    #TODO: Check if disk is already mounted
    _get_vmx()
    vmdir = os.path.dirname(vmx)
    local ('du -hs ' + vmdir)

    try:
        #Mount device locally
        mount = vmdk_mountlocal(vmdk, 1)

        #Clean up and zero out disk
        _print_line()
        print "Cleaning up disk"
        print "  Ignore the failure 'cat: write error: No space left on device'"
        local('sudo cat /dev/zero > %s/zero.fill;sync;sleep 1;sync;rm -f %s/zero.fill' % (mount,mount))
    finally:
        vmdk_unmountlocal(mount)

@task
def vmdk_compressdisk(vmdk):
    """
    Compress a virtual disk. Usage: vm_compressdisk:vmdk
    """
#shrink the disk
    local('vmware-vdiskmanager -k %s' % (vmdk))

@task
def vmdk_mountlocal(vmdk, partition, mountpoint='/mnt/vmdk'):
    """
    Mount vm disk partition on localhost. Usage: vmdk_mountlocal:vmdk,partition#,mountpoint,owner
    """

    me =  pwd.getpwuid(os.getuid())[0]
    print 'sudo chown %s %s' % (me, mountpoint)
    #owner = os.getlogin()
    local('sudo mkdir -p %s' % mountpoint)
    local('sudo vmware-mount %s %s %s' % (vmdk, partition, mountpoint))
    local('sudo chown %s %s' % (me, mountpoint))
    return mountpoint

@task
def vmdk_unmountlocal(mountpoint='/mnt/vmdk'):
    """
    Unmount the local vm disk. Usage: vmdk_unmountlocal:mountpoint
    """

    local('sudo vmware-mount -d %s' % mountpoint)

@task
def zip_write(path, outfile, chdir=''):
    cdcmd=''
    if chdir:
        cdcmd = 'cd %s;' % chdir
    local('%s zip -r %s %s' % (cdcmd, outfile, path))
    return

@task
def zip_read(path, chdir=''):
    cdcmd=''
    if chdir:
        cdcmd = 'cd %s;' % chdir
    local('%s unzip -o %s' % (cdcmd, path))
    return


@task
def clean_and_pack_vmimage():

    _get_vmx()
    #Stop
    _print_line()
    print "Stopping VM"
    try:
        vm_stop()
    except:
        print "This VM was already stopped"

    newvmdir = os.path.dirname(vmx)
    try:
        vmdkfiles = _find_files('*LTS*.vmdk', newvmdir)
        vmdk = [v for v in vmdkfiles if 's0' not in v][0]
    except:
        exit("FATAL: Cannot find vmdk file in %s" % newvmdir)

    _print_line()
    print "Mounting VM disk locally to zerofill before compressing"
    print "  Ignore the failure about 'Invalid configuration file parameter'"
    vm_zerofilldisk(vmdk)

    #Shrink disk
    _print_line()
    print "Compressing disk"
    local ('du -hs ' + os.path.dirname(vmx))
    vmdk_compressdisk(vmdk)
    local ('du -hs ' + os.path.dirname(vmx))

    #Clean up files in vmdir
    _delete_files( _find_files('*.log*', newvmdir))
    _delete_files( _find_files('*.lck*', newvmdir))

    #zip it
    _print_line()
    print "Zipping to %s.zip" % newvmdir
    zip_write(os.path.basename(newvmdir),
              newvmdir + '.zip',
              chdir = os.path.join(newvmdir,".."))

@task
def start_from_update(version):
    """
    Run all the steps to update $VMX.  Usage: restart_from_update:version
    """

    _print_line()
    print "Updating new VM with V" + version
    vm_update(version)

    clean_and_pack_vmimage()

    #Start
    _print_line()
    print "Starting cloned VM for your inspection."
    vm_start()

    #Stop
    #_print_line()
    #print "Stopping VM"
    #vm_stop()
    #upload

@task(default=True)
def make_new_vmimage(version,oldvmdir=None):
    """
    Run all the steps to make a new vmware image from an old one.  Usage: make_new_vmimage:version,oldvmdirectory
    """


    #get and unpack a vm from voltdb website, if necessary
    if not oldvmdir:
        exit("Usage: make_new_vmimage:version,oldvmdir")
        #TODO: Make this work some day.
        import urllib
        import zipfile
        tmpdir = tempfile.mkdtemp();
        try:
            oldvmzip=os.path.join(tmpdir,"oldvm.zip")
            _print_line()
            print "Downloading current VoltDB VM to " + oldvmzip + " This may take a while."
            urllib.urlretrieve("http://voltdb.com/downloads/loader.php?kit=VmwareUrl&j=", oldvmzip)
            if not zipfile.is_zipfile(oldvmzip):
                exit ("FATAL: Downloaded kit is not .zip format. You can inspect the file at " + oldvmzip)
            _print_line()
            print "Extracting zipfile in tmpdir"
            zip_read(oldvmzip, chdir=tmpdir)
            root, dirs, files = os.walk(tmpdir).next()
            #If I just unpacked this in a tmpdir, I only have 1 dir, right?
            assert(len(dirs) == 1)
            oldvmdir = dirs[0]
        except Exception as e:
            exit("FATAL: Error downloading and unzipping vmware image: " + str(e))

    # Set up VMX in envirnoment
    oldvmdir=os.path.abspath(os.path.expanduser(oldvmdir))
    try:
        oldvmx = _find_files('*.vmx', oldvmdir)[0]
    except:
        print "FATAL: Cannot find vmx file in %s" % oldvmdir
    os.environ['VMX'] = oldvmx
    _get_vmx()

    #Clone
    newvmx = re.sub(r"LTS-v\S+-Demo","LTS-v%s-Demo" % version, oldvmx)
    _print_line()
    print "Cloning\n %s ---> %s" % (oldvmx, newvmx)
    vm_clone(newvmx, "VoltDB %s Demo on Ubuntu 14.04" % version)

    #Start
    _print_line()
    print "Starting cloned VM"

    #reset the environment first
    os.environ['VMX'] = newvmx
    _get_vmx()
    vm_start()

    start_from_update(version)


