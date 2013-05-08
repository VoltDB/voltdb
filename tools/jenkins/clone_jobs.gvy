/*
A jenkins groovy script written as a first exercise with
jenkins and goovy that clones all the system-test jobs
to new jobs which build a different branch and set the
email notification to a different set or recipients
*/

import hudson.model.*
import hudson.plugins.cloneworkspace.*
import hudson.tasks.Mailer

def str_search_1 = "system-test-nextrelease"
def str_search_2 = "performance-nextrelease"
def str_oldbranch = "master"
def str_branch = "release-3.2.0.1"
def workspace_name = str_search_1.replace("nextrelease", str_branch)
//whitespace separated list of email addresses
def recipientlist = "qa@voltdb.com"

/*
import hudson.plugins.build_timeout
BuildTimeoutWrapper.ABSOLUTE;
*/

for(item in Hudson.instance.items)
{
  if ( ! item.disabled && (item.getName().contains(str_search_1) || item.getName().contains(str_search_2))) {

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


      // set the any parameter named BRANCH to the branch to build
      for ( ParametersDefinitionProperty pd: project.getActions(ParametersDefinitionProperty))
      {
        pd.parameterDefinitions.each
        {
          if (it.name == "BRANCH")
            it.defaultValue = str_branch
        }
      }

      // if the job has a cloned workspace, replace it with the new kitbuild workspace
      if (project.scm instanceof hudson.plugins.cloneworkspace.CloneWorkspaceSCM) {
              project.scm = new hudson.plugins.cloneworkspace.CloneWorkspaceSCM("kit-"+workspace_name+"-build", "any")
      }

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
