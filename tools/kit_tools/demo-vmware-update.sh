#!/bin/bash
set -x
#TODO: Add a method to change FF home pages

voltclean()
{
    (sleep 2; rm -fv ~/.bash_history) &
    exit
}

voltabort()
{
    if [[ $- == *i* ]]
    then
        echo "ERROR: $@"
        echo "Press Enter to continue (the shell will exit):"
        read line
        exit 1
    fi
}

voltpause()
{
    if [[ $- == *i* ]]
    then
       echo "$@"
       echo "Press Enter to continue:"
       read line
    fi
}

voltclear()
{
    voltpause "Clearing bash history (shell will exit)..."

    (sleep 2; rm -vf ~/.bash_history) &

    exit
}

voltzero()
{
	echo "\
This script zero-fills the unused portion of filesystem by creating /tmp/zero
to allow VMware to compact it. It tries to detect when it's almost full and
then delete the file. Manually delete /tmp/zero if the script fails to do so.
It maintains a 100 MB safety margin."
    voltpause
    sudo rm -Rf /tmp/*
    trap 'sudo rm -vf /tmp/zero exit' INT QUIT
    FREE=`df -m /  | awk '{print $4}' | tail -1`
    I=10
    while [ $FREE -gt 100 ]; do
        if [ $I -eq 10 ]; then
            I=1
            echo "$FREE MB free"
            sleep 1
        else
            I=$(($I+1))
        fi
        sudo dd if=/dev/zero of=/tmp/zero bs=1024x1024 count=100 conv=notrunc oflag=append 2> /dev/null
        FREE=`df -m /  | awk '{print $4}' | tail -1`
    done
    sudo rm -vf /tmp/zero
}

# This updates the OS and the VoltDB distribution (after wiping the previous on clean),
# and clears the bash history.
voltupdate()
{
    cd ~

    if [ -z "$NEW_VERSION" ]; then
        echo "* Version # is required *"
        return
    fi

    #NEW_VERSION=$1
    PACKAGE=LINUX-voltdb-ent-$NEW_VERSION.tar.gz
    KIT_URL=http://voltdb.com/downloads/technologies/server/$PACKAGE

    echo "Removing previous voltdb directory..."
    \rm -rf LINUX-voltdb* voltdb*

    echo "Updating packages"
    sudo apt-get -y -qq install openjdk-7-jdk ntp ntpdate sysstat python-setuptools
    sudo apt-get -y -qq remove unity-webapps-common
    sudo apt-get -y -qq update
    sudo apt-get -y -q upgrade
    sudo apt-get clean
    sudo apt-get autoclean

    #echo "Getting VoltDB software"
    if [ -f ~/Downloads/$PACKAGE ]; then
        echo "WARNING: Using ~/Downloads/$PACKAGE."
        \cp -fv ~/Downloads/$PACKAGE ~ || voltabort "Package copy failed."
    else
        wget -nv $KIT_URL || voltabort "\"wget $KIT_URL\" failed."
    fi

    echo "Extracting $PACKAGE..."
    tar -xzf $PACKAGE || voltabort "Package extraction failed."
    \rm -rf $PACKAGE

    echo "Making voltdb link to voltdb-ent-$NEW_VERSION"
    ln -s voltdb-ent-$NEW_VERSION voltdb

    if grep --quiet '.voltdbprofile' $HOME/.bashrc; then
        echo "Already sourcing .voltdbprofile"
    else
        echo "adding 'source ./.voltdbprofile' to .bashrc"
        echo "source ~/.voltdbprofile" >> .bashrc
    fi

    #echo "Zeroing the disk so that it can be compressed"
    #voltzero
    #echo "Clearing out tmp and history"
    #voltclear
}

NEW_VERSION=$1
echo $PS1
voltupdate
