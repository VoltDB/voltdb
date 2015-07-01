import hudson.model.*

def str_branch = "-ENG-4686-"

for(item in Hudson.instance.items)
{
  if ( item.getName().contains(str_branch) ) {
      println("deleting JOB : "+item.name)
      //item.delete()
      // or disable jobs
      //item.disabled = true
      //item.save()
  }
}
