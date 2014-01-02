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
def str_viewname="system tests-elastic"
//def str_viewname="system tests-noelastic"

def str_branch = "ENG-5154"
boolean enable_performance = false
boolean enable_systemtest = true
boolean enable_cl_truncation = false
boolean enable_supers = false
boolean makenew = false // true=delete existing jobs, false=keep existing jobs but change job enable/disable settings

def workspace_name = str_search_1.replace("nextrelease", str_branch)
//whitespace separated list of email addresses
def recipientlist = "qa@voltdb.com"

AbstractProject kit = null
downstream = ""

alljobs = []

def view = Hudson.instance.getView(str_viewname)

/*
for (item in Hudson.instance.items) {
   if (! item.disabled && (item.getName().contains(str_search_1) ||
                item.getName().contains(str_search_2) ||
                           item.getName().contains(str_search_3))) {
*/
for(item in view.getItems()) {
    if (item.disabled)
         continue
    if (item.getName().startsWith("kit-"))
      alljobs.add(0, item)
    else
      alljobs.add(item)
   }

for(item in alljobs)
{
      println("\n\nprocessing JOB : "+item.name)

      //create the new project name
      newName = item.getName().replace(str_nextrelease, str_branch)

      // delete existing job with new name
      if (Hudson.instance.getJob(newName))
            if (makenew)
                Hudson.instance.getJob(newName).delete()

      if ( ! Hudson.instance.getJob(newName))
            // copy the job, save it
            def job = Hudson.instance.copy(item, newName)
      else
            job = Hudson.instance.getJob(newName)

      AbstractProject project = job

      if (project.getName().startsWith("performance-"))
          project.disabled = !enable_performance
      else if (project.getName().endsWith("-cl-truncation"))
          project.disabled = !enable_cl_truncation
      else if (project.getName().startsWith("system-test-"))
          project.disabled = !enable_systemtest
      else if (project.getName().startsWith("test-"))
          project.disabled = !enable_supers
     else
          project.disabled = false

     if ( ! makenew)
          continue

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
        println "getTrigger"
        try {
        t = job.getTrigger(triggers.TimerTrigger)  
        println t.getSpec() // crontab specification as string ie. "0 22 * * *"
        // to create a new trigger use addTrigger(new Trigger("0 22 * * *"))
        if (t != null)
          job.removeTrigger(t.getDescriptor())
        if (project != kit) {
          // make a list of all jobs for a BuildTrigger for the kit build job
          downstream = downstream + "," + project.getName() // make a list of downstream projects
        }
      } catch(e) { println "no timer trigger found" }
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

      project.save()

      println(" $item.name copied as $newName")
}

if (downstream.length() > 0) {
// finally, set all projects to be downstream of/triggered-by the kit build job
  kit.getPublishersList().add(new tasks.BuildTrigger(downstream.substring(1), false))
  kit.save()
}
