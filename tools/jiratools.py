#!/usr/bin/python

from restkit import Resource, BasicAuth, request
import getpass
import json
import sys

def get_jira_issue (server_base_url, user, password, key, fields=None):

    verbose = False

    # A pool of connections
    #pool = SimplePool(keepalive = 2)

    # This sends the user and password with the request.
    auth = BasicAuth(user, password)

    resource = Resource(server_base_url + 'rest/api/2/issue/%s?fields=%s' % (key, fields),  filters=[auth])


    try:
        response = resource.get (headers = {'Content-Type' : 'application/json'}) 

    except Exception, err:
        print "EXCEPTION: %s " % str(err)
        return
    
    if response.status_int != 200:
        print "ERROR: status %s" % reponse.status_int
        return

    issue = json.loads(response.body_string())

    if verbose:
        print json.dumps(issue, sort_keys = True, indent=4)
        
    return issue

if __name__ == '__main__':
    user = getpass.getuser()
    password = 'inmemory'
    #password = raw_input('Enter your Jira password: ')
    jira_url = 'https://issues.voltdb.com/'
    #server_url = 'http://localhost:8080'
    issue_key = "ENG-3982"

    issue  = get_jira_issue(jira_url, user, password, issue_key, 'summary,assignee')
  
    if issue:
        assignee = issue['fields']['assignee']['name']
        summary = issue['fields']['summary']
        issue_url = jira_url +  'browse/' + issue_key
    
        print summary
        print assignee
        print issue_url
    
    
