To add a new ad hoc test type:

- Edit BenchmarkConfiguration.java and add the new name wherever specific test type
  names are currently used. There are currently 3 methods you will have to touch.
- Implement a new subclass of QueryTestBase. Most of the code goes into getQuery().
- Add a new table (if needed) and test(s) to config.xml using the new test name.

The files mentioned above should be the only ones you need to touch. Keep others
ignorant of specific test types.