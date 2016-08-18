# Configure Logging

VoltDB uses Log4j for logging. The default configuration sends a subset of log messages to the console, while sending a much more detailed set of logging to a file named `voltdb.log` in the `log` directory under the specified voltdbroot directory

Relevant Documentation
-----------------------------------------

The docs on logging are the place to start. They cover changing the configuration before startup, as well as changing the logging configuration on a running server.

Administrator's Guide Chapter 7:
Logging and Analyzing Activity in a VoltDB Database
https://docs.voltdb.com/AdminGuide/ChapLogging.php

Using VoltDB Appendix G: System Procedures
@UpdateLogging
https://docs.voltdb.com/UsingVoltDB/sysprocupdatelogging.php

Additional Notes
-----------------------------------------

Enabling the ADHOC logger at the INFO level will allow you to log all ad-hoc SQL statements run against the database. This can be useful for troubleshooting.

VoltDB has a built-in log collection tool that collects logs and bundles them up to be sent to support.

Administrator's Guide Section 8.3:
Collecting the Log Files
https://docs.voltdb.com/AdminGuide/TroubleshootCollect.php
