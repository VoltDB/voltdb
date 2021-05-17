#!/usr/bin/env python

# This Python script generates AWS CloudFormation templates to start a VoltDB
# cluster. Run the program without any arguments to see usage.
#
# For details on the CloudFormation template, you can check out the doc page at
# http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html

from string import Template
import sys

TEMPLATE = r"""
{
    "AWSTemplateFormatVersion": "2010-09-09",

    "Description" : "AWS CloudFormation template for launching a $hosts-node k-safety=$ksafety VoltDB cluster. **WARNING** This template creates an Amazon EC2 instance. You will be billed for the AWS resources used if you create a stack from this template.",

    "Parameters": {
        $zone

        "InstanceType": {
            "Description": "VoltDB server EC2 instance type",
            "Type": "String",
            "Default": "m2.xlarge",
            "AllowedValues": [ "m1.medium","m1.large","m1.xlarge","m2.xlarge","m2.2xlarge","m2.4xlarge","m3.xlarge","m3.2xlarge","c1.medium","c1.xlarge","cc1.4xlarge"],
            "ConstraintDescription": "must be a valid EC2 instance type."
        },

        "Example": {
            "Description": "VoltDB example to start. (voter, json-sessions, voltcache, voltkv, windowing)",
            "Type": "String",
            "Default": "voter",
            "AllowedValues": ["voter", "json-sessions", "voltkv", "windowing"],
            "ConstraintDescription": "must be a valid VoltDB example name."
        }
    },

    "Mappings": {
        "AWSInstanceType2Arch": {
            "m1.medium"  : { "Arch": "64" },
            "m1.large"   : { "Arch": "64" },
            "m1.xlarge"  : { "Arch": "64" },
            "m2.xlarge"  : { "Arch": "64" },
            "m2.2xlarge" : { "Arch": "64" },
            "m2.4xlarge" : { "Arch": "64" },
            "m3.xlarge"  : { "Arch": "64" },
            "m3.2xlarge" : { "Arch": "64" },
            "c1.medium"  : { "Arch": "64" },
            "c1.xlarge"  : { "Arch": "64" },
            "cc1.4xlarge": { "Arch": "64" }
        },

        "AWSRegionArch2AMI": {
            "us-east-1"     : { "64": "$amiid", "64HVM": "NOT YET SUPPORTED" }
        }
    },

    "Resources": {

        "DBServer1": {
            "Type": "AWS::EC2::Instance",
            "Metadata": {
                "Comment": "Configure the bootstrap helpers to install and start VoltDB",

                "AWS::CloudFormation::Init": {
                    "config": {
                        "packages": {
                        },

                        "sources": {
                        },

                        "files": {
                            "/etc/ntp.conf": {
                                "content": { "Fn::Join": ["", [
                                                              "driftfile /var/lib/ntp/ntp.drift\n",
                                                              "server time1.google.com burst iburst minpoll 4 maxpoll 4\n",
                                                              "server time2.google.com burst iburst minpoll 4 maxpoll 4\n",
                                                              "server time3.google.com burst iburst minpoll 4 maxpoll 4\n",
                                                              "server time4.google.com burst iburst minpoll 4 maxpoll 4\n",
                                                              "server 127.127.0.1\n",
                                                              "fudge 127.127.0.1 stratum 10\n"
                                                          ]]},
                                "mode": "000644",
                                "owner": "root",
                                "group": "root"
                            },

                            "/tmp/deployment.xml": {
                                "content": { "Fn::Join": ["", [
                                                              $deployment
                                                          ]]},
                                "mode" : "000644"
                            }
                        },

                        "services": {
                            "sysvinit": {
                                "ntp": {
                                    "enabled"      : "true",
                                    "ensureRunning": "true",
                                    "files": ["/etc/ntp.conf"]
                                }
                            }
                        }
                    }
                }
            },
            "Properties": {
                "ImageId": { "Fn::FindInMap": [ "AWSRegionArch2AMI", { "Ref": "AWS::Region" },
                                                { "Fn::FindInMap": [ "AWSInstanceType2Arch", { "Ref": "InstanceType" }, "Arch" ] } ] },
                ${zone_ref}
                "InstanceType"  : { "Ref": "InstanceType" },
                "SecurityGroups": [ {"Ref": "DBServerSecurityGroup"} ],
                "UserData"      : { "Fn::Base64": { "Fn::Join": ["", [
                                                                     "#!/bin/bash -v\n",
                                                                     "# Helper function\n",
                                                                     "function error_exit\n",
                                                                     "{\n",
                                                                     "  cfn-signal -e 1 -r \"$$1\" '", { "Ref": "WaitHandle" }, "'\n",
                                                                     "  exit 1\n",
                                                                     "}\n",

                                                                     "# Initialize\n",
                                                                     "cfn-init -s ", { "Ref": "AWS::StackId" }, " -r DBServer1 ",
                                                                     "    --region ", { "Ref": "AWS::Region" }, " || error_exit 'Failed to run cfn-init'\n",

                                                                     "# Sync clocks so that they are close enough to start a cluster\n",
                                                                     "service ntp stop\n",
                                                                     "a=\"1.0\"\n",
                                                                     "while [ $$(echo \"$${a#-}>0.05\" | bc) -eq 1 ];\n",
                                                                     "do a=`ntpdate -b -p 8 time1.google.com | awk '{ print $$10 }'`; done\n",
                                                                     "\n",

                                                                     "# Start VoltDB as user voltdb\n",
                                                                     "sudo su - voltdb -c bash << EOF\n",
                                                                     "exec > /tmp/script.out 2>&1\n",
                                                                     "set -x\n",
                                                                     "cd ", { "Ref": "Example" },
                                                                     ";echo \"Starting VoltDB server\"\n",
                                                                     "../../bin/voltdb init -l ../../voltdb/license.xml -C /tmp/deployment.xml\n",
                                                                     "../../bin/voltdb start -B -H `hostname -I`\n",
                                                                     "\n",
                                                                     "while true; do\n",
                                                                     "    sleep 1\n",
                                                                     "    ../../bin/sqlcmd --query=exec\\ @Statistics\\ MEMORY\\ 0 </dev/null && break\n",
                                                                     "done\n",
                                                                     "./run.sh init\n",
                                                                     "EOF\n",
                                                                     "\n",
                                                                     "# All is well so signal success\n",
                                                                     "cfn-signal -e 0 -r \"VoltDB setup complete\" '", { "Ref": "WaitHandle" }, "'\n"
                                                                 ]]}}
            }
        },

        $additional_servers

        "WaitHandle": {
            "Type": "AWS::CloudFormation::WaitConditionHandle"
        },

        "WaitCondition": {
            "Type": "AWS::CloudFormation::WaitCondition",
            "DependsOn": "DBServer1",
            "Properties": {
                "Handle": {"Ref": "WaitHandle"},
                "Timeout": "1200"
            }
        },

        "DBServerSecurityGroup": {
            "Type": "AWS::EC2::SecurityGroup",
            "Properties": {
                "GroupDescription": "Enable client access via port 21212 and HTTP access via port 8080",
                "SecurityGroupIngress": [
                    {"IpProtocol": "tcp", "FromPort": "3021", "ToPort": "3021", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "tcp", "FromPort": "8080", "ToPort": "8080", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "tcp", "FromPort": "9090", "ToPort": "9090", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "tcp", "FromPort": "21211", "ToPort": "21211", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "tcp", "FromPort": "21212", "ToPort": "21212", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "tcp", "FromPort": "22", "ToPort": "22", "CidrIp": "0.0.0.0/0"},
                    {"IpProtocol": "udp", "FromPort": "123", "ToPort": "123", "CidrIp": "0.0.0.0/0"}
                ]
            }
        }
    },

    "Outputs": {
        "VoltDBNodeURL": {
            "Value": { "Fn::Join": ["", ["http://", { "Fn::GetAtt": [ "DBServer1", "PublicDnsName" ]}, ":8080"]] },
            "Description": "URL to VoltDB Catalog Report page."
        },
        "VoltDBClientCmd": {
            "Value": { "Fn::Join": ["", ["ssh voltdb@", { "Fn::GetAtt": [ "DBServer1", "PublicDnsName" ]},
                                         " \"bash --login -c 'cd ", { "Ref": "Example" }, "; ./run.sh client'\""]] },
            "Description": "Command to start the client over an SSH connection. The password is 'voltdb'."
        }
    }
}
"""

ZONE_TEMPLATE = r""""AvailabilityZone": {
            "Description": "Availability zone to start the servers in",
            "Type": "String",
            "Default": "us-east-1e",
            "AllowedValues": ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1e"],
            "ConstraintDescription": "must be a valid availability zone in the U.S. East region."
        },
"""

ZONE_REF_TEMPLATE = r""""AvailabilityZone" : { "Ref": "AvailabilityZone" },"""

DEPLOYMENT_TEMPLATE = r"""
                                                              "<?xml version=\"1.0\"?>\n",
                                                              "<deployment>\n",
                                                              "    <cluster hostcount=\"$hosts\" kfactor=\"$ksafety\" />\n",
                                                              "    <httpd enabled=\"true\">\n",
                                                              "        <jsonapi enabled=\"true\" />\n",
                                                              "    </httpd>\n",
                                                              "</deployment>\n"
"""

SERVER_TEMPLATE = r"""
        "DBServer$seqId": {
            "Type": "AWS::EC2::Instance",
            "Metadata": {
                "Comment": "Configure the bootstrap helpers to install and start VoltDB",

                "AWS::CloudFormation::Init": {
                    "config": {
                        "packages": {
                        },

                        "sources": {
                        },

                        "files": {
                            "/etc/ntp.conf": {
                                "content": { "Fn::Join": ["", [
                                                              "driftfile /var/lib/ntp/ntp.drift\n",
                                                              "server ", { "Fn::GetAtt": [ "DBServer1", "PrivateIp" ]}, " burst iburst minpoll 4 maxpoll 4\n",
                                                              "server 127.127.0.1\n",
                                                              "fudge 127.127.0.1 stratum 10\n"
                                                          ]]},
                                "mode": "000644",
                                "owner": "root",
                                "group": "root"
                            },

                            "/tmp/deployment.xml": {
                                "content": { "Fn::Join": ["", [
                                                              $deployment
                                                          ]]},
                                "mode" : "000644"
                            }
                        },

                        "services": {
                            "sysvinit": {
                                "ntp": {
                                    "enabled"      : "true",
                                    "ensureRunning": "true",
                                    "files": ["/etc/ntp.conf"]
                                }
                            }
                        }
                    }
                }
            },
            "Properties": {
                "ImageId": { "Fn::FindInMap": [ "AWSRegionArch2AMI", { "Ref": "AWS::Region" },
                                                { "Fn::FindInMap": [ "AWSInstanceType2Arch", { "Ref": "InstanceType" }, "Arch" ] } ] },
                "AvailabilityZone" : { "Ref": "AvailabilityZone" },
                "InstanceType"  : { "Ref": "InstanceType" },
                "SecurityGroups": [ {"Ref": "DBServerSecurityGroup"} ],
                "UserData"      : { "Fn::Base64": { "Fn::Join": ["", [
                                                                     "#!/bin/bash -v\n",
                                                                     "# Helper function\n",
                                                                     "function error_exit\n",
                                                                     "{\n",
                                                                     "  cfn-signal -e 1 -r \"$$1\" '", { "Ref": "WaitHandle" }, "'\n",
                                                                     "  exit 1\n",
                                                                     "}\n",

                                                                     "# Sync clocks so that they are close enough to start a cluster\n",
                                                                     "service ntp stop\n",
                                                                     "a=\"1.0\"\n",
                                                                     "while [ $$(echo \"$${a#-}>0.05\" | bc) -eq 1 ];\n",
                                                                     "do a=`ntpdate -b -p 8 time1.google.com | awk '{ print $$10 }'`; done\n",
                                                                     "\n",

                                                                     "# Initialize\n",
                                                                     "cfn-init -s ", { "Ref": "AWS::StackId" }, " -r DBServer$seqId ",
                                                                     "    --region ", { "Ref": "AWS::Region" }, " || error_exit 'Failed to run cfn-init'\n",

                                                                     "# Start VoltDB as user voltdb\n",
                                                                     "sudo su - voltdb -c bash << EOF\n",
                                                                     "cd ", { "Ref": "Example" },
                                                                     ";echo \"Starting VoltDB server\"\n",
                                                                     "../../bin/voltdb init -l ../../voltdb/license.xml -C /tmp/deployment.xml\n",
                                                                     "../../bin/voltdb start -B -H ", { "Fn::GetAtt": [ "DBServer1", "PrivateIp" ]}, "\n",
                                                                     "EOF\n"
                                                                 ]]}}
            }
        },
"""

def main():
    if (len(sys.argv) != 4):
        print "Usage: %s HOSTCOUNT KSAFETY AMI-ID" % (sys.argv[0])
        return -1

    params = dict(hosts=int(sys.argv[1]),
                  ksafety=int(sys.argv[2]),
                  amiid=sys.argv[3],
                  zone="",
                  zone_ref="")

    # Order of template substitution:
    # - deployment
    # - servers
    # - zone
    # - top level template
    params["deployment"] = Template(DEPLOYMENT_TEMPLATE).substitute(params)

    servers = []
    if params["hosts"] > 1:
        params["zone"] = Template(ZONE_TEMPLATE).substitute(params)
        params["zone_ref"] = ZONE_REF_TEMPLATE
        for i in xrange(params["hosts"]-1):
            params["seqId"] = i + 2
            servers.append(Template(SERVER_TEMPLATE).substitute(params))
    params["additional_servers"] = "\n".join(servers)
    print Template(TEMPLATE).substitute(params)

    return 0


if __name__ == "__main__":
    sys.exit(main())
