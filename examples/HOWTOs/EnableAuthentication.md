# Enable Authentication

The steps to use security with examples are:

1. If you need more flexibility than the default roles provide, you will need to modify your DDL to create roles and/or add permissions to individual procedures.
2. Change or create a deployment file to enable authentication and create a set of users. There is an example deployment file in the `deployment-file-examples` directory called `deployment-authEnabled`.
3. Edit the run.sh file, under the `client` command. Make sure `--user=username` and `--password=password` are set with real values for user and password.

Basic authentication in VoltDB should be viewed as a safety and auditing tool to limit the damage possible from users and programs. It will not be effective against a sophisticated malicious attacker.

If stronger security is needed, contact VoltDB and we will walk through your options, including Kerberos authentication.

Relevant Documentation
-----------------------------------------

Using VoltDB Chapter 12: Security
https://docs.voltdb.com/UsingVoltDB/ChapSecurity.php

How to Use a Deployment File
-----------------------------------------

In order to use authentication, you're going to need to initialize the server with a deployment file.

When you start without one, the server uses a default 1-node deployment file and writes it out to the voltdbroot folder. If you've already run a VoltDB example, you can probably find this default file there. It should have the following contents:

```xml
<?xml version="1.0"?>
<deployment>
   <cluster hostcount="1" />
   <httpd enabled="true">
      <jsonapi enabled="true" />
   </httpd>
</deployment>
```

So copy that file up one level, or simply create a file named `deployment.xml` with the contents above. You can even borrow a file from the `/examples/HOWTOs/deployment-file-examples` directory.

To start with a deployment file add `-C path/to/deployment.xml` to the VoltDB `init` command. For example:

```bash
voltdb init --force -C deployment.xml
voltdb start

```
