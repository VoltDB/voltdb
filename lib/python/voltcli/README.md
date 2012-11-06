# Goals

## Single command to run any Volt-related verb

Provide a git/subversion-like UI.

## Full command-line help

Require minimal effort of each verb implementation, e.g. just some constructor
arguments.

## Modularity

New commands can be added by configuration or small modules. New commands can
be added at a project level, e.g. to replace run.sh scripts. It will be picked
up as long as the module or configuration is in a well-known location.
