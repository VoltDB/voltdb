# Configure Logging

VoltDB uses Log4j for logging. The default configuration sends a subset of log messages to the console, while sending a much more detailed set of logging to a file named `voltdb.log` in the `log` directory under the specified voltdbroot directory.  In most cases this is sufficient, and no changes are needed, but sometimes it is helpful to modify the default configuration, or you may want to use Log4J within your own procedures and want to know how to configure them.

While explaining how Log4J works could take a while, here are a few basic principles for Log4J within VoltDB:
 - the default configuration is defined by the voltdb-ent-<version>/voltdb/log4j.xml file
 - by default, the log file is in the log/volt.log file within the voltdbroot directory


 Here are some of the default settings that VoltDB uses:
  - configuration file: voltdb-ent-<version>/voltdb/log4j.xml
  - log level: INFO
  - time zone:UTC
  - roll over: daily at 00:00 UTC, or when file exceeds 1GB


The documentation includes instructions for the most common Log4J configuration changes:

 - [Creating a new Log4J configuration file](https://docs.voltdb.com/AdminGuide/LogConfig.php)
 - [Starting the database with an alternative Log4J configuration file](https://docs.voltdb.com/AdminGuide/LogEnable.php)
 - [Changing the Timezone of Log Messages](https://docs.voltdb.com/AdminGuide/LogTimezone.php)


## Updating the configuration while the database is running

It is also possible to change the Log4J configuration dynamically while the database is running using the voltadmin command line tool.  For example:

    voltadmin log4j my_log4j.xml

Alternatively, you can use the [@UpdateLogging](https://docs.voltdb.com/UsingVoltDB/sysprocupdatelogging.php) system procedure.  This procedure takes a single String input parameter which is the contents of the log4j configuration XML file.  If you invoke this using sqlcmd, be aware that sqlcmd uses single quotes for strings, but often the XML file will contain single quotes.  If so, you can escape them and invoke the procedure using sqlcmd as shown in the following example command:

    cat log4j.xml | sed "s/'/\&quot;/g" | echo "exec @UpdateLogging '$(cat -)'" | sqlcmd

## Logging all AdHoc SQL statements

A common configuration change is to log every Ad Hoc SQL statement sent to the database.  This may be useful to keep track of what SQL statements were called during a time when AdHoc* procedures are showing higher execution times.

You can enable AdHoc SQL logging by un-commenting this logger in the voltdb-ent-x.x/voltdb/log4j.xml file, changing the logging level to DEBUG:

    <logger name="ADHOC">
        <level value="DEBUG"/>
    </logger>


Sometimes you may want these messages to be logged to a separate log file for further analysis.  You can configure a new output file by adding a new appender in the log4j.xml as in this example, which defines a sql.log file:

    <appender name="sqlfile" class="org.apache.log4j.DailyRollingFileAppender">
        <param name="file" value="log/sql.log"/>
        <param name="DatePattern" value="'.'yyyy-MM-dd" />
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%d   %-5p [%t] %c: %m%n"/>
        </layout>
    </appender>

Then in order to have the ADHOC logger messages go to this file rather than the default volt.log file, you need to add an appender-ref to the logger configuration:

    <logger name="ADHOC">
        <level value="DEBUG"/>
        <appender-ref ref="sqlfile"/>
    </logger>


## Logging from stored procedures

You can use Log4J to log messages from java stored procedures for debugging or troubleshooting purposes, but if you do, it is important to follow these best practices so as not to adversely affect performance.  Stored procedures in VoltDB are often invoked at high frequency, so you should consider this "fast path" code that needs to execute as quickly as possible.  That generally means that under normal circumstances you don't want it to log anything or generate any additional objects for logging purposes.

Inside your stored procedure, you can import the Logger class, declare a static Logger, and then use it to log messages as in this example:


    import org.apache.log4j.Logger;
    ...
    static Logger logger = Logger.getLogger("PROCEDURE");
    ...
    logger.info("SOME INFO");
    logger.debug("PROCEDURE reached step " + someString);

This is fine as long as you intend to use this for development only.  You may then remove or comment out these statements from the procedure before deploying to production.

If you want to leave logging statements within the procedure code, be aware that even if you set the logging level to INFO, the debug statement still incurs the cost of contstructing the message parameter, such as concatenating intermediate strings.  To avoid this, you should follow the Log4J best practice for ["What's the fastest way of (not) logging?"](https://logging.apache.org/log4j/1.2/faq.html#a2.3):

    if(logger.isDebugEnabled()) {
        logger.debug("PROCEDURE reached step " + someString);
    }

If you do not make any changes to the log4j.xml configuration, then messages from your stored procedures would appear in your volt.log file if they are at the default logging level (INFO), as set within the root tag of the log4j.xml file:

    <root>
        <priority value="info" />
        <appender-ref ref="file" />
        <appender-ref ref="consolefiltered" />
    </root>

It is best to leave the root settings alone, but if you want to change the logging level for messages coming from your procedures, you can use one or more logger tags to configure a different level, such as debug:

    <logger name="PROCEDURE">
      <level value="DEBUG"/>
    </logger>

You can name the logger whatever you want, but the string used in Logger.getLogger() within the procedure must match the <logger name=""> property value in the log4j.xml, otherwise it will not apply this configuration, and only the default logging level will be output.  The logger name property does not accept wildcards, such as "com.voltdb.example.procedures.*", it must be an exact string match.  Unless you prefer a complex logging configuration, you may find it easier to just use "PROCEDURE" as the name of the logger as in the examples here.

You may want log messages from your procedures to be logged to a separate log file.  You can configure a new output file by adding a new appender in the log4j.xml as in this example, which defines a sql.log file:

    <appender name=procedurefile" class="org.apache.log4j.DailyRollingFileAppender">
        <param name="file" value="log/procedures.log"/>
        <param name="DatePattern" value="'.'yyyy-MM-dd" />
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%d   %-5p [%t] %c: %m%n"/>
        </layout>
    </appender>

Then in order to have the ADHOC logger messages go to this file rather than the default volt.log file, you need to add an appender-ref to the logger configuration:

    <logger name="PROCEDURE">
        <level value="DEBUG"/>
        <appender-ref ref="procedurefile"/>
    </logger>


Additional Notes
-----------------------------------------

VoltDB has a built-in log collection tool that collects logs and bundles them up to be sent to support.

Administrator's Guide Section 8.3:
Collecting the Log Files
https://docs.voltdb.com/AdminGuide/TroubleshootCollect.php
