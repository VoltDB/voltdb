/**
cleanup_jenkins.gvy
A script for removing workspaces for deleted jobs from all the Jenkins slaves.

This job is meant ot be run from Jenkins. It takes 2 parameters:
dryRun - no directories are deleted.
slavelist -
**/

import hudson.FilePath;
import hudson.model.*

// get current thread / Executor
def thr = Thread.currentThread()
// get current build
def build = thr.executable


def resolver = build.buildVariableResolver;
def param_slavelist = resolver.resolve("slavelist")
def slavelist = param_slavelist.tokenize(',')

def param_dryRun = resolver.resolve("dryRun")
def dryRun = false

if (slavelist) {
  println "-" * 80
  println "Files will be deleted for following slaves: "
  println param_slavelist
  println "-" * 80
}

// Initialize dryRun parameter to TRUE if not given as script parameter
if (param_dryRun == 'true' ){
  dryRun = true;
  println "** Execute a dryRun - no files will ever be deleted **";
}

// shortcut to Jenkins instance
def jenkins = jenkins.model.Jenkins.instance;

// Search for Projects without custom workspace and collect their name
//
def jobNames = jenkins.items.findAll { it instanceof hudson.model.Job && it.customWorkspace==null }.collect { it.name };

//println("Existing Jobs: ");
//jobNames.each {println "  $it"}


// Slaves create a workspace for each job under their 'workspaceRoot'.
// The subdirectory is named after the job name, possibly with a @ followed by a
// number in case of matrix jobs.
// We simply list the workspace content and try to find a matching job. If none
// is found, the directory is scheduled for deletion.
//
// This process is done only for slaves that are online.
// There is no need to inspect Master since job workspaces will be automatically
// deleted when the job definition is deleted.
//
for (slave in jenkins.slaves)
{
  if (slavelist != []  && !(slave.nodeName in slavelist)) {
    continue
  }


  println ("=====================================================================")
  // Make sure slave is online
  if( ! slave.computer.online ) {
    println("Slave '$slave.nodeName' is currently offline - cleaning up anyways");
  }
  else {
    println("Slave '$slave.nodeName' is online - perform workspace cleanup:");
  }


  println ("=====================================================================")
  // Retrieve the a FilePath to the workspace root
  def wsRoot = slave.workspaceRoot;
  if( wsRoot == null ) {
    println("Slave '$slave.nodeName' has a <null> workspaceRoot - skip workspace cleanup");
    continue;
  }

  // List workspace content and perform cleanup
  def subdirs = wsRoot.list();

  if( subdirs && subdirs.size() == 0 ) {
    println("  (workspace is empty)");
    continue;
  }

  for(d in subdirs) {

    // Remove any suffixes from the dir name
    def dirName = d.name.split("@")[0];

    // Find matching job
    def jobMatch = jobNames.find { it==dirName };

    if ( jobMatch != null ) {
      println("  KEEP: $d --> job:$jobMatch");
    }
    else {
      if( dryRun == true ) {
          println(" DELETE: $d (dryRun)");
      }
      else {
        println("  DELETE: $d");
          d.deleteRecursive();
      }
    }
  }
}
