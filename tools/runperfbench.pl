#!/usr/bin/perl

# Usage:
# runperfbench <revision>
#
# Expects to run from newtrunk/
# Builds the revision specified on the command line.
# Copies obj/release/voltbin/*  to volt3a:~/voltbin
# Runs the benchmark.
# appends result to perfout.txt in the cwd.
# moves to the previous revision on this branch
# Wash. Rinse. Repeat.

my $base_revision = $ARGV[0];
my $hostcount = 5;
my $sitesperhost = 12;
my $warehouses = $hostcount * $sitesperhost;

# my $benchcmd = "ant -Dbuild=release -Dhostcount=$hostcount -Dsitesperhost=$sitesperhost -Dwarehouses=$warehouses benchmarkcluster";

my $benchcmd = "ant benchmark -Dclientcount=3 -Dhostcount=5 -Dhost1=volt3a -Dhost2=volt3b -Dhost3=volt3c -Dhost4=volt3d -Dhost5=volt3e -Dhost6=volt3f -Dprocessesperclient=1 -Dwarehouses=60 -Dclienthost1=volt4a -Dclienthost2=volt4b -Dclienthost3=volt4c -Dsitesperhost=12 -Dduration=180000 -Dloadthreads=8";

open(PERFOUT, ">>perfout.txt") or die ("Can't open perfout.txt");
print PERFOUT "# $benchcmd\n";

printf "Updating svn to revision $base_revision\n";
`svn update -r $base_revision`;


sub getNextRevision() {
    my $revision = 0;     # make this invalid and discover next revision.

    @update_stdout = `svn update -r PREV`;
    foreach $line (@update_stdout) {
	if ($line =~ m/Updated to revision (\d+)/) {
	    $revision = $1;
	    break;
	}
    }
    return $revision;
}


for ($revision = $base_revision; $revision > 0; ) {
    printf "Running at svn revision %s\n", $revision;    
    printf "Building and copying voltbin to volt3a.\n";
    `rm -rf obj/release`; 
    $exitcode = `ant -Dbuild=release`;
    if ($exitcode != 0) {
	print PERFOUT "# $revision 'ant -Dbuild=release' failure\n";
	$revision = getNextRevision();
	next;
    }

    $exitcode = `ant -Dbuild=release voltbin`;
    if ($exitcode != 0) {
	print PERFOUT "# $revision 'ant -Dbuild=release voltbin' failure\n";
	$revision = getNextRevision();
	next;
    }

    $exitcode = `scp obj/release/voltbin/* volt3a:~/voltbin`;
    if ($exitcode != 0) {
	print PERFOUT "# $revision scp to volt3a failed\n";
	$revision = getNextRevision();
	next;
    }
   
    for ($run=0; $run < 3; $run++) {
	printf "TPC-C benchmark revision $revision run $run\n";
	@tpcc_stdout = `$benchcmd`;
    
	$printing = 0;
	foreach $line (@tpcc_stdout) {
	    if ($line =~ m/.*?BENCHMARK\sRESULTS.*?/) {
		$printing = 1;
	    }
	    elsif ($line =~ m/.*?===========.*?/) {
		$printing = 0;
	    }
	    if ($printing == 1) {
		print $line;
	    }
	    
	    if ($line =~ m/.*?Transactions per second:\s(\d+)/) {
		$tps = $1;
		print PERFOUT "$revision\t$tps\n";
	    }
	}
    }
    $revision = getNextRevision();
}
