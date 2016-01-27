# This file sets up terminal windows to view some help text
# and cd to the voltdb examples directory
# It should be sourced by .bashrc

volthelp()
{
    echo "\
~/$VOLTDB/examples has various runnable samples in separate subdirectories.

Useful commands in  ~/$VOLTDB/bin (see documentation):
  Database startup             voltdb create ...
  Database administration      voltadmin COMMAND ...
  Interactive SQL interpreter  sqlcmd

Command Help:
  General voltdb usage  voltdb -h
  Administration        voltadmin help [COMMAND]
  SQL interpreter       sqlcmd --help

Documentation:
  \"VoltDB Tutorial\"      http://voltdb.com/docs/tutorial/
  \"Using VoltDB\"         http://voltdb.com/docs/UsingVoltDB/

Quick example:
  $ cd voter
  $ voltdb create &  # (start VoltDB in the background)
  $ sqlcmd < ddl.sql # (load the schema and procedures)
  $ ./run.sh client  # (run the sample client for 2 minutes)
  $ kill %1          # (kill the background server)
"
}

cd ~
VOLTDB=voltdb

echo "
:: Welcome to the VoltDB VMware Demo ::

Please visit http://voltdb.com for more information.
"

if [ -n  "$VOLTDB" ]; then
   PATH="$HOME/$VOLTDB/bin:$PATH"
   volthelp
   cd $VOLTDB/examples
else
   echo "* There does not appear to be a VoltDB distribution here. *"
fi
