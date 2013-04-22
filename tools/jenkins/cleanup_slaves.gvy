import hudson.FilePath;

/*
 Set DRY RUN
 SET slavelist
 Note: offline hosts WILL be processed.
 */

slavelist = [""];
 
// Initialize dryRun parameter to TRUE if not given as script parameter
if( !binding.variables.containsKey("dryRun") ) {
  dryRun = true;
}
if( dryRun == true ) {
  println "** Execute a dryRun - no files will ever be deleted **";
}
 
// shortcut to Jenkins instance
def jenkins = jenkins.model.Jenkins.instance;
 
// Search for Projects without custom workspace and collect their name
//
def jobNames = jenkins.items.findAll { it instanceof hudson.model.Job && it.customWorkspace==null }.collect { it.name };
 
//println("Existing Jobs: ");
//jobNames.each {println "  $it"}
 
 
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
  if (!(slave.nodeName in slavelist)) {
    continue
  }

 
  // Make sure slave is online
  if( ! slave.computer.online ) {
    println("Slave '$slave.nodeName' is currently offline - cleaning up anyways");
    //continue;
  }
 
  // Retrieve the a FilePath to the workspace root
  def wsRoot = slave.workspaceRoot;
  if( wsRoot == null ) {
    printlnt("Slave '$slave.nodeName' has a <null> workspaceRoot - skip workspace cleanup");
    continue;
  }
 
  // List workspace content and perform cleanup
  println("Slave '$slave.nodeName' is online - perform workspace cleanup:");
 
  def subdirs = wsRoot.list();
 
  if( subdirs.size() == 0 ) {
    println("  (workspace is empty)");
    continue;
  }
 
  for(d in subdirs) {
 
    // Remove any suffixes from the dir name
    def dirName = d.name.split("@")[0];
 
    // Find matching job
    def jobMatch = jobNames.find { it==dirName };
 
    if ( jobMatch != null ) {
      println("  KEEP: $d --> job:$jobMatch");
    }
    else {
      if( dryRun == true ) {
          println(" DELETE: $d (dryRun)");
      }
      else {
        println("  DELETE: $d");
          d.deleteRecursive();
      }
    }
  }
}
