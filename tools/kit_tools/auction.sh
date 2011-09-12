#!/bin/sh
# run this after running auction.exp, to check the export output files.
! grep -v '^"[0-9]*","[0-9]*","[0-9]*","0","1","1","[0-9]*","[0-9]","-*[0-9]*","[0-9]*\-[0-9]*\-[0-9]* [012][0-9]:[0-5][0-9]:[0-5][0-9].[0-9]*","[0-9]*.[0-9]*"$' *EXPORTDEMO*BID*.csv
