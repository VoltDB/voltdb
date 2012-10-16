# Bugs

* -d debug display option doesn't seem to work, e.g. in Verb constructor.

# TO-DO List for New Volt CLI

* Daemon process management for servers.
* Follow Andrew's CLI proposal more closely, e.g. separate sql to its own
  command and rename volt to voltdb.
* Add override options for classpath and catalog. Classpath option (-cp or
  --classpath) should add to configured classpath. Catalog option should
  override configured catalog.
* Aliases for commands plus arguments. Allow aliases to invoke a sequence of
  commands.
* Move user commands into config file, wherever possible, e.g. as aliases.
  replacement), e.g. as aliases. Not sure this is practical.
* Interactive shell with command recall and auto-completion.
* Log tailing (with colorizing).
