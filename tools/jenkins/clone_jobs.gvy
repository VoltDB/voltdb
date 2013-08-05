import jenkins.*
import jenkins.model.*
import hudson.*
import hudson.model.*
import hudson.plugins.cloneworkspace.*
import hudson.tasks.Mailer

def str_nextrelease = "nextrelease"
def str_search_1 = "system-test-" + str_nextrelease
def str_search_2 = "performance-" + str_nextrelease
def str_search_3 = "endurance-" + str_nextrelease
def str_oldbranch = "master"

def str_branch = "stream-refactor"
boolean enable_performance = false
boolean enable_systemtest = false

def workspace_name = str_search_1.replace("nextrelease", str_branch)
//whitespace separated list of email addresses
def recipientlist = "prosegay@voltdb.com"

AbstractProject kit = null
downstream = ""

alljobs = []

for (item in Hudson.instance.items) {
   if (! item.disabled && (item.getName().contains(str_search_1) ||
                item.getName().contains(str_search_2) ||
                           item.getName().contains(str_search_3))) {
                  
    if (item.getName().contains("kit-"))
      alljobs.add(0, item)
    else
      alljobs.add(item)
   }
}

for(item in alljobs)
{
      println("\n\nprocessing JOB : "+item.name)

      //create the new project name
      newName = item.getName().replace(str_nextrelease, str_branch)

      // delete existing job with new name
      if (Hudson.instance.getJob(newName))
            Hudson.instance.getJob(newName).delete()

      // copy the job, disable and save it
      def job = Hudson.instance.copy(item, newName)
      job.disabled = true

      AbstractProject project = job

      // save the kit-build project ref
      if (job.getName().startsWith("kit-")) kit = project
      else if (kit == null) {
        // kit-build job must be processed first
        assert(false)
      }

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
              project.scm = new hudson.plugins.cloneworkspace.CloneWorkspaceSCM(kit.getName(), "any") //"kit-"+workspace_name+"-build", "any")
      }

    // option to remove cron triggers and trigger everthing from the kit build
    if (true) {
        try {
            t = job.getTrigger(triggers.TimerTrigger)
            //println t.getSpec() // crontab specification as string ie. "0 22 * * *"
            // to create a new trigger use addTrigger(new Trigger("0 22 * * *"))
            if (t != null)
              job.removeTrigger(t.getDescriptor())
            if (project != kit) {
              // make a list of all jobs for a BuildTrigger for the kit build job
              downstream = downstream + "," + project.getName() // make a list of downstream projects
            }
        catch(e) { println "no Timer Trigger found" }
    }

    // option to modify build timeout
    if (false) {
        for (o in project.getBuildWrappersList()) {
            o.timeoutMinutes=3
            o.failBuild=false
            o.timeoutPercentage=0
            o.timeoutType=absolute
            o.timeoutMinutesElasticDefault=3
        }
    }

    // update email notification recipient list
    project.publishersList.each() { p ->
        if (p instanceof Mailer) {
          p.recipients = recipientlist
        }
    }

      if (project.getName().startsWith("performance-"))
          project.disabled = !enable_performance
      if (project.getName().startsWith("system-test-"))
          project.disabled = !enable_systemtest

      project.save()

      println(" $item.name copied as $newName")
}

if (downstream.length() > 0) {
// finally, set all projects to be downstream of/triggered-by the kit build job
  kit.getPublishersList().add(new tasks.BuildTrigger(downstream.substring(1), false))
  kit.save()
}
