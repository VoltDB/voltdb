[![Build Status](https://drone.io/github.com/geb/geb-example-gradle/status.png)](https://drone.io/github.com/geb/geb-example-gradle/latest)

# Description

This is an example of incorporating Geb into a Gradle build. It shows the use of Spock and JUnit 4 tests.

The build is setup to work with Firefox, Chrome and PhantomJS. Have a look at the `build.gradle` and the `src/test/resources/GebConfig.groovy` files.

# Usage

The following commands will launch the tests with the individual browsers:
    
    ./gradlew chromeTest
    ./gradlew firefoxTest
    ./gradlew phantomJsTest

To run with all, you can run:

    ./gradlew test

Replace `./gradlew` with `gradlew.bat` in the above examples if you're on Windows.

# Questions and issues

Please ask questions on [Geb user mailing list](http://xircles.codehaus.org/lists/user@geb.codehaus.org) and raise issues in [Geb issue tracker](https://jira.codehaus.org/browse/GEB).