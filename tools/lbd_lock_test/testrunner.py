#!/usr/bin/env python

from subprocess import Popen, PIPE
import os, sys, datetime, fcntl, time

DURATION_IN_SECONDS = 240

def cmd_readlines(cmd):
    "Run a shell command and get the output as a list of lines"
    fd = os.popen(cmd)
    retval = fd.readlines()
    fd.close()
    return retval
    
def killProcess(p):
    "Kill all processes for this user named 'LBDLockPatternTest'"
    # get all the java processes for this user
    javaprocs = cmd_readlines("jps")
    # split them into (pid, name) tuples
    javaprocs = [line.split() for line in javaprocs]
    # throw out any with no name
    javaprocs = [t for t in javaprocs if len(t) > 1]
    # find pids for processes with the right name
    javaprocs = [int(t[0]) for t in javaprocs if t[1].startswith("LBDLockPatternTest")]
    # kill all the running procs with the right name explicitly
    # (only for this user usally)
    for pid in javaprocs:
         killcmd = "kill -9 " + str(pid)
         os.system(killcmd)
    # this seems to do nothing at all on many platforms :(
    p.wait()
    
def blockUntilInput(f):
    "Assuming f is non blocking, block until you can read a line from it"
    while True:
        try: f.readline(); return
        except: time.sleep(0.1)

# make stdin non-blocking
fd = sys.stdin.fileno()
fl = fcntl.fcntl(fd, fcntl.F_GETFL)
fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

# compile the java code we need (assuming it's in the same folder)
print "Compiling Java Reprodcuer..."
output = os.system("javac LBDLockPatternTest.java")
if output == 0:
    print "Success"
else:
    print "Failed to compile reproducer."
    print "Check the output of \"javac LBDLockPatternTest.java\" from your shell."
    sys.exit(-1)

def runTest(i):
    """Start a subprocess that runs the java reproducer. If it hangs, let the user know and
       leave the subprocess process running until the user presses a key. If it runs for
       DURATION_IN_SECONDS seconds without hanging, kill the subprocess and repeat."""

    print "\nBeginning run %d for %d seconds. Press ENTER or RETURN to end the test.\n" % (i, DURATION_IN_SECONDS)

    p = Popen("java LBDLockPatternTest", shell=True, bufsize=0, stdout=PIPE)

    # make the process's output non-blocking
    fd = p.stdout.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    # get the current time and init some variables
    start = datetime.datetime.utcnow()
    prevnow = start        # the last time a progress time was printed
    lastdotprinted = start # the last time a dot was successfully read
    
    # true if there was a break in the dots
    possiblyFailed = False

    # while the java process isn't dead
    while p.poll() == None:
        now = datetime.datetime.utcnow()

        # print a progress time out every 10 seconds
        if possiblyFailed == False:
            if (now - prevnow).seconds == 10:
                prevnow = now
                sys.stdout.write(" %d seconds " % ((now - start).seconds))

            # if no dots read in 10 seconds, then we assume the java proc has hung
            if (now - lastdotprinted).seconds > 20:
                print("\nSorry, this platfrom may have reproduced the issue. If you do not see more dots, it's sadness time.")
                possiblyFailed = True

            # if all's gone well for DURATION_IN_SECONDS, we kill the proc and return true
            if (now - start).seconds > DURATION_IN_SECONDS:
                print("\nThis run (%d) did not reproduce the issue." % (i))
                killProcess(p)
                return True

        # do a non-blocking input read to see if the user wants to stop
        try:
            sys.stdin.readline()
            print("\nThis run (%d) interrupted by user." % (i))
            killProcess(p)
            sys.exit(-1)
        except:
            pass

        # do a non-blocking java-output read to see if a dot has been printed
        try:
            c = p.stdout.read(1)
            sys.stdout.write(c)
            lastdotprinted = now
            possiblyFailed = False
        except:
            time.sleep(0.1)
            
    # before the function exits, make sure the process is gone
    p.wait()

# repeat until failure or the user presses ENTER or RETURN
i = 1
while runTest(i):
    i += 1
