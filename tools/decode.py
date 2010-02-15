#!/usr/bin/env python

import sys,re

# given a java backtrace file ("hs_err_pidNNN.log") and a symbol
# file (produced by make nativelibs/libhzee.sym), print a decoded
# backtrace for EE code.

class Decoder:
    "Decode native symbols in a java backtrace file"

    def loadSymbols(self, symfile):
        # read the .sym file and store a list of address and a list
        # of textual symbol names.
        f = open(symfile)
        for line in f:
            match = re.compile("([0-9a-e]+)\s[tT]\s(.*)").match(line)
            if match != None:
                self.symbols.append(match.group(1))
                self.symbols_strings.append(match.group(2))
        f.close()

    def decodeSymbol(self, aSym):
        # convert aSym to an integer. do a linear search for aSym.
        # (self.symbols is sorted, could improve to binary search)
        # print out the last symbol that less than aSym.
        # (could search backwards if this state offends you)
        nLast = 0
        nSym = int(aSym,16)
        for idx, sym in enumerate(self.symbols):
            nAddr = int("0x"+sym, 16)
            if nAddr > nSym:
                print hex(nSym) + " " + hex(nLast) + " " + self.symbols_strings[idx-1]
                return
            nLast = nAddr

    def decode(self, logfile):
        printnext = False
        decodenext = False
        f = open(logfile)
        for line in f:
            if re.compile("Stack").match(line):
                # look for the stack and print next two informational lines
                print line.strip()
                printnext = True
                continue
            elif printnext:
                print line.strip()
                printnext = False
                decodenext = True
                continue
            elif decodenext:
                # decode until blank line. if native symbol can't be
                # found, just print the line from the log file
                if line == "\n":
                    decodenext = False
                else:
                    match = re.compile(".*libhzee.so\+(.*)\]").match(line)
                    if match != None:
                        self.decodeSymbol(match.group(1))
                    else:
                        print line.strip()
                continue


    def __init__(self, symfile):
        self.symbols = []
        self.symbols_strings = []
        self.loadSymbols(symfile)


def main(argv=None):
    if (argv==None):
        argv = sys.argv
    if (len(argv) < 3):
        print "debug.py <hs_err_pidNNN.log> <path/to/libhzee.sym>"
        return -1

    decoder = Decoder(argv[2])
    decoder.decode(argv[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
