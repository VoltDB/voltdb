# Description

The gradle build is setup for automated tests of VDM with Firefox, Chrome and PhantomJS. 

# Usage

The following commands will run the tests with specified browser:
    
    ./gradlew chromeTest    or ./gradlew chrome
    ./gradlew firefoxTest   or ./gradlew firefox
    ./gradlew phantomJsTest or ./gradlew phantomjs

To run with all of the above, you can run:

    ./gradlew test

To run specific test file(say, DatabaseTest):
    
    ./gradlew firefoxTest --tests=*DatabaseTest*

Replace `./gradlew` with `gradlew.bat` in the above examples if you're on Windows.
