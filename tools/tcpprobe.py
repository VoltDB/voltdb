#!/usr/bin/env python

from __future__ import with_statement
from sys import argv
from socket import socket
from contextlib import closing

def main(args):
  host, port = args[1:]
  with closing(socket()) as s:
    try: s.connect((host,int(port)))
    except: return 1

if __name__ == '__main__':
  exit(main(argv))

# vim:et:sw=2:ts=2
