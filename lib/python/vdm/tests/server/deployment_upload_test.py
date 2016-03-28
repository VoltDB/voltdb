"""
This file is part of VoltDB.

Copyright (C) 2008-2015 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
"""

import unittest

import io
import requests
import socket
from cStringIO import StringIO
import xmlrunner
from flask import json

__host_name__ = socket.gethostname()
__host_or_ip__ = socket.gethostbyname(__host_name__)


__url__ = 'http://' + __host_or_ip__ + ':8000/api/1.0/servers/'
__db_url__ = 'http://'+__host_or_ip__+':8000/api/1.0/databases/'


class Database(unittest.TestCase):
    """
    test cases for Server
    """
    def setUp(self):
        """
        # Create a db
        """
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        db_data = {'name': 'testDB'}
        response = requests.post(__db_url__, json=db_data, headers=headers)
        if response.status_code == 201:
            self.assertEqual(response.status_code, 201)
        else:
            self.assertEqual(response.status_code, 404)

    def tearDown(self):
        """
        Delete database
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            # Delete database
            db_url = __db_url__ + str(last_db_id)
            response = requests.delete(db_url)
            self.assertEqual(response.status_code, 204)
        else:
            print "The database list is empty"


class UploadConfiguration(Database):
    """
    Test to upload the file and check error
    """
    def test_upload_errors(self):
        """
        Check the errors while uploading files.
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            res = requests.put(__db_url__ + str(last_db_id) + '/deployment/', files={'file': open('test-files/images.png', 'rb')})
            assert res.status_code == 200
            result = json.loads(res.content)
            self.assertEqual(result['status'],'failure')
            self.assertEqual(result['error'],'Invalid file type.')

            res = requests.put(__db_url__ + str(last_db_id) + '/deployment/', files={'file': open('test-files/Invalid.xml', 'rb')})
            assert res.status_code == 200
            result = json.loads(res.content)
            self.assertEqual(result['status'],'failure')
            self.assertEqual(result['error'],'Invalid file content.')

            res = requests.put(__db_url__ + str(last_db_id) + '/deployment/', files={'file': open('test-files/Sample.xml', 'rb')})
            assert res.status_code == 200
            result = json.loads(res.content)
            self.assertEqual(result['status'],'failure')
            self.assertEqual(result['error'],'Invalid file content.')

            res = requests.put(__db_url__ + str(last_db_id) + '/deployment/', files={'file': open('test-files/deployment.xml', 'rb')})
            assert res.status_code == 200
            result = json.loads(res.content)
            self.assertEqual(result['status'],'success')
        else:
            print "The database list is empty"

    def test_upload_success(self):
        """
        Test to upload file and check the result.
        """
        response = requests.get(__db_url__)
        value = response.json()
        if value:
            db_length = len(value['databases'])
            last_db_id = value['databases'][db_length-1]['id']
            res = requests.put(__db_url__ + str(last_db_id) + '/deployment/', files={'file': open('test-files/deployment.xml', 'rb')})
            assert res.status_code == 200
            result = json.loads(res.content)
            self.assertEqual(result['status'],'success')
            response = requests.get(__db_url__ + str(last_db_id) + '/deployment/')
            value = response.json()

            if value:
                self.assertEqual(value['deployment']['admin-mode']['adminstartup'], False)
                self.assertEqual(value['deployment']['admin-mode']['port'], 21211)
                self.assertEqual(value['deployment']['dr']['id'], 6)
                self.assertEqual(value['deployment']['dr']['listen'], True)
                self.assertEqual(value['deployment']['dr']['port'], 12112)
                self.assertEqual(value['deployment']['snapshot']['enabled'], True)
                self.assertEqual(value['deployment']['snapshot']['frequency'], '24h')
                self.assertEqual(value['deployment']['snapshot']['retain'], 2)
                self.assertEqual(value['deployment']['snapshot']['prefix'], 'AUTOSNAP')
                self.assertEqual(value['deployment']['partition-detection']['enabled'], True)
                self.assertEqual(value['deployment']['security']['enabled'], True)
                self.assertEqual(value['deployment']['security']['provider'], 'hash')
                self.assertEqual(value['deployment']['export']['configuration'][0]['enabled'], True)
                self.assertEqual(value['deployment']['export']['configuration'][0]['stream'], 'test')
                self.assertEqual(value['deployment']['export']['configuration'][0]['type'], 'kafka')
                self.assertEqual(value['deployment']['export']['configuration'][0]['exportconnectorclass'], '')
                self.assertEqual(value['deployment']['export']['configuration'][0]['property'][0]['name'], 'metadata.broker.list')
                self.assertEqual(value['deployment']['export']['configuration'][0]['property'][0]['value'], '1')
                self.assertEqual(value['deployment']['cluster']['elastic'], 'enabled')
                self.assertEqual(value['deployment']['cluster']['hostcount'], 1)
                self.assertEqual(value['deployment']['cluster']['kfactor'], 2)
                self.assertEqual(value['deployment']['cluster']['schema'], 'ddl')
                self.assertEqual(value['deployment']['cluster']['sitesperhost'], 1)
                self.assertEqual(value['deployment']['commandlog']['enabled'], True)
                self.assertEqual(value['deployment']['commandlog']['logsize'], 1024)
                self.assertEqual(value['deployment']['commandlog']['synchronous'], False)
                self.assertEqual(value['deployment']['commandlog']['frequency']['time'], 200)
                self.assertEqual(value['deployment']['commandlog']['frequency']['transactions'], 2147483647)
                self.assertEqual(value['deployment']['systemsettings']['query']['timeout'], 10000)
                self.assertEqual(value['deployment']['systemsettings']['temptables']['maxsize'], 100)
                self.assertEqual(value['deployment']['systemsettings']['snapshot']['priority'], 6)
                self.assertEqual(value['deployment']['systemsettings']['elastic']['duration'], 50)
                self.assertEqual(value['deployment']['systemsettings']['elastic']['throughput'], 2)
                self.assertEqual(value['deployment']['systemsettings']['resourcemonitor']['memorylimit']['size'], '80%')
                self.assertEqual(value['deployment']['systemsettings']['resourcemonitor']['disklimit']['feature'][0]['name'], 'snapshots')
                self.assertEqual(value['deployment']['systemsettings']['resourcemonitor']['disklimit']['feature'][0]['size'], '66')
                self.assertEqual(value['deployment']['httpd']['enabled'], True)
                self.assertEqual(value['deployment']['httpd']['port'], 8080)
                self.assertEqual(value['deployment']['httpd']['jsonapi']['enabled'], True)
                self.assertEqual(value['deployment']['paths']['snapshots']['path'], 'snapshots1')
                self.assertEqual(value['deployment']['paths']['commandlogsnapshot']['path'], 'command_log_snapshot2')
                self.assertEqual(value['deployment']['paths']['voltdbroot']['path'], 'voltdbroot3')
                self.assertEqual(value['deployment']['paths']['exportoverflow']['path'], 'export_overflow3')
                self.assertEqual(value['deployment']['paths']['droverflow']['path'], 'dr_overflow4')
                self.assertEqual(value['deployment']['paths']['commandlog']['path'], 'command_log')
                self.assertEqual(value['deployment']['heartbeat']['timeout'], 90)
                self.assertEqual(value['deployment']['users']['user'][0]['name'], 'abc')
                self.assertEqual(value['deployment']['users']['user'][0]['plaintext'], True)
                self.assertEqual(value['deployment']['users']['user'][0]['roles'], 'User')
            else:
                print "Deployment is not available."
        else:
            print "The database list is empty."


if __name__ == '__main__':
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
    unittest.main()