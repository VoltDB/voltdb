#!/usr/bin/env bash

# I will start an instance and set up all the hz stuff on it.
#
# Checklist:
#
# - ec2-api tools are set up (env vars, etc.)
# - already created the keypair and stored the private key into $EC2_KEY
# - ~/.hzpass contains your subversion password (and it's chmodded 600!)
# - ~/.ssh/id_dsa.pub or ~/.ssh/id_rsa.pub is what will be used to ssh in as
#   the user, and your ssh-agent has this added to the keyring

set -o errexit -o nounset

host=$EC2_HOST
user=$EC2_HZUSER
pubkey="$( cat ~/.ssh/id_dsa.pub || cat ~/.ssh/id_rsa.pub )"
svnpass="$( cat ~/.hzpass )"
svnuser='y_z@mit.edu'
img=ami-f0e70399 # ubuntu 8.04 + dev tools + hz checkout
imgname=hz
keypair=hz
bucket=hzimg
secret="$( cat /var/cloud/keys/aws-secret-key-shhhhh.txt )"
access="$( cat /var/cloud/keys/aws-access-key-id.txt )"
acct="$( cat /var/cloud/keys/aws-account-id.txt )"

# prepare ssh keys

if ! ssh-add -l | grep $EC2_KEY > /dev/null
then ssh-add $EC2_KEY
fi

# create a large instance
run_inst() {
  ec2-run-instances -t m1.xlarge -k $keypair $img
}

# system/user setup
setup() {
  ssh root@$host << EOF
    set -o xtrace -o errexit -o nounset
    aptitude update
    # The echo is necessary, or the aptitude command will cause this ssh
    # session to terminate prematurely.
    echo | aptitude -y dist-upgrade
    echo | aptitude -y install build-essential subversion openjdk-6-jdk ant vim

    sed -i 's/^# %sudo/%sudo/' /etc/sudoers

    useradd $user -m
    mkdir -p ~$user/.ssh/
    echo '$pubkey' >> ~$user/.ssh/authorized_keys
    chown -R $user ~$user/.ssh/
    echo /bin/bash | chsh $user
    adduser $user sudo
EOF

  ssh $user@$host << EOF
    echo p | svn co -q --username '$svnuser' --password '$svnpass' \
      'https://hzproject.com/svn/repos/build/trunk' hz
EOF
}

create_image() {
  scp /var/cloud/keys/{pk-*,cert-*} root@$host:/mnt/
  ssh root@$host << EOF
    set -o xtrace -o errexit -o nounset
    ec2-bundle-vol -d /mnt -k /mnt/pk-* -c /mnt/cert-* -u '$acct' -r x86_64 \
      -p $imgname
    ec2-upload-bundle -b $bucket -m /mnt/$imgname.manifest.xml -a '$access' \
      -s '$secret'
EOF
  ec2-register $bucket/$imgname.manifest.xml
}

run() {
  ssh $user@$host << EOF
    set -o xtrace -o errexit -o nounset
    cd hz
    ant -quiet clean
    svn up -q
    ant "$@"
EOF
}

eval "$@"
