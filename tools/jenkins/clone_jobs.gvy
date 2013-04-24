/*
A jenkins groovy script written as a first exercise with
jenkins and goovy that clones all the system-test jobs 
to new jobs which build a different branch and set the
email notification to a different set or recipients
*/

import hudson.model.*
import hudson.plugins.cloneworkspace.*
import hudson.tasks.Mailer

def str_search = "system-test-nextrelease"
def str_oldbranch = "master"
def str_branch = "release-3.2.0.1"
def workspace_name = str_search.replace("nextrelease", str_branch)
//whitespace separated list of email addresses
def recipientlist = "qa@voltdb.com"

/*
import hudson.plugins.build_timeout
BuildTimeoutWrapper.ABSOLUTE;
*/

for(item in Hudson.instance.items)
{
  if (item.getName().contains(str_search) && !item.disabled) {

      println("processing JOB : "+item.name)

      //create the new project name
      newName = item.getName().replace("nextrelease", str_branch)
      
      // delete existing job with new name  
      nj = Hudson.instance.getJob(newName)
      if (nj) 
            nj.delete()

      // copy the job, disable and save it
      def job = Hudson.instance.copy(item, newName)
      job.disabled = true
      //job.save()

      AbstractProject project = job
/*
      // set the branch to build
      branch = project.parametersDefinitionProperty.getParameterDefinition("BRANCH")
      if (branch)
            branch.setDefaultValue(str_branch)
*/
      project.scm = new hudson.plugins.cloneworkspace.CloneWorkspaceSCM("kit-"+workspace_name+"-build", "any")

    // update recipient list
    project.publishersList.each() { p ->
        if (p instanceof Mailer) {
          p.recipients = recipientlist
        }
    }

      project.save()

      println(" $item.name copied as $newName")
  }
}
