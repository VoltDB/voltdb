#! /usr/bin/env perl

# From: http://websvn.kde.org/trunk/KDE/kdesdk/scripts/svn-clean
#
# This script recursively (beginning with the current directory)
# wipes out everything not registered in SVN.
#
# rewritten in perl by Oswald Buddenhagen <ossi@kde.org>
#  based on bash version by Thiago Macieira <thiago@kde.org>
#   inspired by cvs-clean, written by Oswald Buddenhagen <ossi@kde.org>
#    inspired by the "old" cvs-clean target from Makefile.common
#
# This file is free software in terms of the BSD licence. That means
# that you can do anything with it except removing this license or
# the above copyright notice. There is NO WARRANTY of any kind.
#

# Warning:
# This script processes the output from the SVN executable
# Do not run it along with colorsvn

use File::Path;

my $version = "svn-clean v1.0";
my $heading = $version.": cleans up the Subversion working directory\n";
my $usage = $heading.
  "svn-clean [-h] [-n] [-q] [-i|-f] [dirname]\n\n".
  "Where:\n".
  "  -h      shows this help screen\n".
  "  -n      dry-run: doesn't actually erase the files, just show their names\n".
  "  -i      interactive: ask for confirmation before erasing the files\n".
  "  -f      force: doesn't ask for confirmation before erasing\n".
  "  -q      quiet: doesn't show output\n";


my $dry_run = 0;
my $force = 0;
my $quiet = 0;

sub check_confirm()
{
  return if ($force);

  open(TTY, "+< /dev/tty") or die "cannot open /dev/tty";

  print TTY "This will erase files and directories that aren't in Subversion\n".
            "Are you sure you want to continue? (y/n) ";
  
  if (<TTY> =~ /^[Yy]/) {
    $force = 1;
    close TTY;
    return;
  }
  
  # user cancelled
  exit 0;
}

# Parse arguments
my $rest = 0;
my @files = ();
foreach my $arg (@ARGV) {
  if ($rest) {
    push @files, $arg;
  } else {
    if ($arg eq '-h' || $arg eq '--help') {
      print $usage;
      exit (0);
    } elsif ($arg eq '-n' || $arg eq '--dry-run') {
      $dry_run = 1;
      $force = 1;
    } elsif ($arg eq '-f' || $arg eq '--force') {
      $force = 1;
    } elsif ($arg eq '-i' || $arg eq '--interactive') {
      $force = 0;
    } elsif ($arg eq '-q' || $arg eq '--quiet') {
      $quiet = 1;
    } elsif ($arg eq '--') {
      $rest = 1;
    } elsif ($arg =~ /^-/) {
      print STDERR "svn-clean: unknown argument '".$arg."'\n\n".$usage;
      exit (1);
    } else {
      push @files, $arg;
    }
  }
}
if (!@files) {
  push @files, '.';
}

# Unset TERM just so that no colours are output
# in case $SVN points to colorsvn
delete $ENV{'TERM'};

#print($heading."\n") unless $quiet;

foreach my $dir (@files) {
  open SVN, "svn status --no-ignore \"".$dir."\"|";
  while (<SVN>) {
    /^[I?] +(.*)$/ or next;
    my $file = $1;
    check_confirm();
    lstat $file;
    if (-d _) {
      print("D ".$file."\n") unless $quiet;
      rmtree($file, 0, 0) unless $dry_run;
    } else {
      print("F ".$file."\n") unless $quiet;
      unlink($file) unless $dry_run;
    }
  }
  close SVN;
}

