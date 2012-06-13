To add a new ad hoc test type:
- Implement a new class that extends QueryTestBase with a corresponding factory class
  that implements the QueryTestBase.Factory interface's "make" method.
  Most of the work for the QueryTestBase class goes into its getQuery() method.
- Edit BenchmarkConfiguration.java to add the new factory by name (the "test name")
  to the testFactory Map with a static call to installFactory.
- Edit config.xml to add your test name and any generated table(s) it may need.
- Edit run.sh to pass your test name to the command line of Benchmark.java
  in new and/or existing benchmark command functions.

To add minor variations of existing tests, you can simply add new Factory classes 
to their existing QueryTestBase classes. Each Factory would configure different 
instances (initialize different properties) of the class. The getQuery() method could
then generate slightly different queries based on the object's properties.

The files mentioned above should be the only ones you need to touch. Keep others
ignorant of specific test types.
