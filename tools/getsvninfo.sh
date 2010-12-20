#!/usr/bin/perl

my $urlbase = "";
my $revision = "";

# Read the output of the shell command "svn info" into an array
# of lines. Then iterate those looking for lines that match URL
# or Revision regular expressions.

my @svnstatus = `svn status`;
my @svninfo = `svn info`;
my $dirty = false;

if (scalar(@svnstatus) != 0) {
    print "This is a dirty svn working copy or not an svn working copy\n";
    $dirty = true;
}

if ($? != 0) {
    @gitinfo = `git status`;
    if (@gitinfo[0] eq "# On branch master\n" &&
        @gitinfo[1] eq "nothing to commit (working directory clean)\n") {
        @svninfo = `git svn info`;
        $githash = `git log -1 --format=%H`;
    }
}

foreach (@svninfo) {
    if (/^URL:\s*(.*?)$/) {
        $urlbase = $1;
    }
    elsif (/^Revision:\s*(.*?)$/) {
        $revision = $1;
    }
}

# get the version number from ant
my $version = "0.0.0";
if (($#ARGV + 1) > 0) {
    $version = $ARGV[0];
}

# mark dirty if that is the case
if ($dirty == true) {
    $revision .= "-dirty";
}

# create a file, buildstring.txt, and print a reasonable URL. Sadly,
# real svn urls aren't this reasonable.
open(BUILDSTRING, ">buildstring.txt") or die "Can't create buildstring.txt";
if (defined $githash) {
    print BUILDSTRING "$version $urlbase?revision=$revision $githash\n"
} else {
    print BUILDSTRING "$version $urlbase?revision=$revision\n";
}
close BUILDSTRING;
