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
def str_viewname="All system all perf tests on master"
//def str_viewname="system tests-noelastic"

// DONT CHANGE IT HERE, USE BUILD PARAMETERS

def str_branch = build.buildVariableResolver.resolve("branch").replaceAll("\\s","")
boolean enable_performance =  build.buildVariableResolver.resolve("enable_performance").toBoolean()
def trigger_performance = "kit" //build.buildVariableResolver.resolve("schedue_performance")
boolean enable_systemtest = build.buildVariableResolver.resolve("enable_systemtest").toBoolean()
boolean enable_endurance = build.buildVariableResolver.resolve("enable_endurance").toBoolean()
boolean enable_cl_truncation = build.buildVariableResolver.resolve("enable_cl_truncation").toBoolean()
boolean enable_supers = false //build.buildVariableResolver.resolve("enable_supers").toBoolean()
boolean makenew = build.buildVariableResolver.resolve("makeNew").toBoolean() // true=delete existing jobs, false=keep existing jobs but change job enable/disable settings
boolean deletejob = build.buildVariableResolver.resolve("deletejobs").toBoolean()
boolean cloneDisabled = build.buildVariableResolver.resolve("cloneDisabled").toBoolean()

def workspace_name = str_search_1.replace("nextrelease", str_branch)
def recipientlist = build.buildVariableResolver.resolve("email_list")

AbstractProject kit = null
AbstractProject kitperf = null
downstream = ""
downstream_perf = ""

alljobs = []

def view = Hudson.instance.getView(str_viewname)
/*
for (item in Hudson.instance.items) {
   if (! item.disabled && (item.getName().contains(str_search_1) ||
                item.getName().contains(str_search_2) ||
                           item.getName().contains(str_search_3))) {
*/
for(item in view.getItems()) {
    if (item.getName().startsWith("kit-system-test-"))
      alljobs.add(0, item)
    else if (item.getName().startsWith("kit-performance-"))
      alljobs.add(0, item)
    else if ( ! item.getName().startsWith("batch-"))
      alljobs.add(item)
   }


for(item in alljobs)
{
      println("\n\nprocessing JOB : "+item.name)

      //create the new project name
      newName = "branch-" + item.getName().replace(str_nextrelease+"-", "") + "-" + str_branch

      // delete existing job with new name
      if (Hudson.instance.getJob(newName))
            if (makenew || deletejob)
                Hudson.instance.getJob(newName).delete()
      if (deletejob)
           continue

      if ( ! Hudson.instance.getJob(newName)) {

         if (item.disabled)
             if (!cloneDisabled)
                 continue

            // copy the job, save it
            job = Hudson.instance.copy(item, newName)
            newjob = true
      }
      else {
            job = Hudson.instance.getJob(newName)
            newjob = false
      }

      AbstractProject project = job

      if (item.disabled)
         if (cloneDisabled)
             project.disabled = true
         else
             continue
      else if (project.getName().startsWith("branch-performance-"))
          project.disabled = !enable_performance
      else if (project.getName().startsWith("branch-system-test-cl-truncation"))
          project.disabled = !enable_cl_truncation
      else if (project.getName().startsWith("branch-system-test-"))
          project.disabled = !enable_systemtest
      else if (project.getName().startsWith("branch-test-"))
          project.disabled = !enable_supers
      else if (project.getName().startsWith("branch-endurance-"))
          project.disabled = !enable_endurance
     else
          project.disabled = true


      // save the kit-build project ref
      if (job.getName().startsWith("branch-kit-system-test-build-")) {
        kit = project
      } else if (job.getName().startsWith("branch-kit-performance-build-")) {
        kitperf = project
      } else if (kit == null || kitperf == null) {
        // kit-build job must be processed first
        assert(false)
      }

     //if ( ! newjob)
     //     continue

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
          if (project.getName().startsWith("branch-system-test") || project.getName().startsWith("branch-endurance"))
              project.scm = new hudson.plugins.cloneworkspace.CloneWorkspaceSCM(kit.getName(), "any")
          else if (project.getName().startsWith("branch-performance"))
              project.scm = new hudson.plugins.cloneworkspace.CloneWorkspaceSCM(kitperf.getName(), "any")
      }

    // option to remove cron triggers and trigger everything from the kit build
    if (true) {
        println "getTrigger"
        try {
            t = job.getTrigger(triggers.TimerTrigger)  
            println t.getSpec() // crontab specification as string ie. "0 22 * * *"
            // to create a new trigger use addTrigger(new Trigger("0 22 * * *"))
            if (t != null)
                job.removeTrigger(t.getDescriptor())
        } catch(e) { println "no timer trigger found" }

        if (project != kit && project != kitperf) {
            if (project.getName().startsWith("branch-performance-") && trigger_performance != "kit")
                project.addTrigger(new triggers.TimerTrigger(trigger_performance))
            else
                // make a list of all jobs for a BuildTrigger for the kit build job
                if (project.getName().startsWith("branch-system-test"))
                    downstream = downstream + "," + project.getName() // make a list of downstream projects
                if (project.getName().startsWith("branch-performance"))
                    downstream_perf = downstream_perf + "," + project.getName()
        }
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

    if (!project.getName().startsWith("kit-")) {
        project.save()
        project.doReload()
    }

     println(" $item.name copied as $newName")
}

if (!deletejob) {
   if (downstream.length() > 0) {
   // finally, set all projects to be downstream of/triggered-by the kit build job
     bt = new tasks.BuildTrigger(downstream.substring(1), false)
     try {
        kit.getPublishersList().replace(bt)
     } catch(e) {
        println e.getMessage();
        kit.getPublishersList().add(bt)
     }
   }
   kit.save()
   kit.doReload()

 if (downstream_perf.length() > 0) {
   // finally, set all projects to be downstream of/triggered-by the kit performance build job
     btp = new tasks.BuildTrigger(downstream_perf.substring(1), false)
     try {
        kitperf.getPublishersList().replace(btp)
     } catch(e) {
        println e.getMessage()
        kitperf.getPublishersList().add(btp)
     }
   }
   kitperf.save()
   kitperf.doReload()
}
