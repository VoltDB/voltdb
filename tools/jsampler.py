#!/usr/bin/python

from __future__ import with_statement
import re
import sys

FRAME_PARSE = re.compile(r"^([^(]+)\(([^)]+)\)$")

def parseStacks(fileobj):
    stacks = []
    stack = []
    for line in f:
        if line == "\n":
            stacks.append(stack)
            stack = []
            continue

        match = FRAME_PARSE.match(line)
        method = match.group(1)
        location = match.group(2)

        stack.append((method, location))
    if len(stack) > 0:
        stacks.append(stack)
    return stacks

filter = 'edu.mit.tpcc.Client'

def filterStacks(stacks):
  for stack in stacks:
    for method, location in stack:
      if method.startswith(filter): break
    else:
      yield stack


if len(sys.argv) != 2:
    sys.stderr.write("profile.py [profile text file]\n")
    sys.exit(1)

f = open(sys.argv[1])

# Maps method name -> [selfcount , self+children]
total_methods = {}
stacks = list(filterStacks(parseStacks(f)))
total_stacks = len(stacks)

for stack in stacks:
    # Used to avoid counting recursive methods more than once per stack
    stack_methods = {}
    for method, location in stack:
        stack_methods[method] = False
    stack_methods[stack[0][0]] = True

    for method, is_top in stack_methods.iteritems():
        if method not in total_methods:
            total_methods[method] = [0, 0]

        if is_top:
            total_methods[method][0] += 1
        total_methods[method][1] += 1

self = []
total = []
for method, (self_count, total_count) in total_methods.iteritems():
    assert self_count <= total_stacks
    assert total_count <= total_stacks
    self.append((self_count, method))
    total.append((total_count, method))

self.sort(reverse=True)
total.sort(reverse=True)


print "Self counts:"
for count, method in self:
    if count == 0: break
    print "%s\t%d\t%.1f%%" % (method, count, float(count)/total_stacks*100)

print
print "Total counts:"
for count, method in total:
    if count == 0: break
    print "%s\t%d\t%.1f%%" % (method, count, float(count)/total_stacks*100)
