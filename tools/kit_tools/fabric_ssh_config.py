################################################
# USE SSH CONFIG IF POSSIBLE
################################################

def getSSHInfoForHost(host):
    """ Inspired by:
        http://markpasc.typepad.com/blog/2010/04/loading-ssh-config-settings-for-fabric.html """

    from os.path import expanduser
    from paramiko.config import SSHConfig

    key = None
    key_filename = None
    host = host

    def hostinfo(host, config):
        hive = config.lookup(host)
        if 'hostname' in hive:
            host = hive['hostname']
        if 'user' in hive:
            host = '%s@%s' % (hive['user'], host)
        if 'port' in hive:
            host = '%s:%s' % (host, hive['port'])
        return host

    try:
        config_file = file(expanduser('~/.ssh/config'))
    except IOError:
        pass
    else:
        config = SSHConfig()
        config.parse(config_file)
        key = config.lookup(host).get('identityfile', None)
        if key != None: key_filename = expanduser(key)
        host = hostinfo(host, config)
    return key_filename, host