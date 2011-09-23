#!/usr/bin/env python

from os import curdir, sep
import sys
import mimetypes
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import cgi
from Query import VoltQueryClient
import socket
import traceback

# volt server IP address and port
volt_server_ip = 'volt3a'
volt_server_port = 21212

# volt username/password if database security is enabled
volt_username = ''
volt_password = ''

# port on local server to listen for http requests, URL is formatted as http://localhost:9001
http_server_port = 9001

# since the HTTPHandler does not maintain any state attributes, we have to put
# the client object here
client = None

class HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path.endswith("volt-db.gif"):
                # display the VoltDB logo
                self.wfile.write(file(r"volt-db.gif", "rb").read())
                return

            else:
                # send default page
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                self.wfile.write('<html>\n')
                self.wfile.write('<head>\n')
                self.wfile.write('<style type="text/css">\n')
                self.wfile.write('p {color: white; }\n')
                self.wfile.write('body {font-family: Arial, Helvetica, sans-serif;}\n')
                self.wfile.write('textarea#styled {width: 75%; height: 300px;}\n')
                self.wfile.write('textarea#snapshot_path {width: 37.5%; height: 25px;}\n')
                self.wfile.write('textarea#snapshot_nonce {width: 37.5%; height: 25px;}\n')
                self.wfile.write('</style>\n')
                self.wfile.write('<script type="text/javascript">\n')
                self.wfile.write('function submitenter(myfield,e)\n')
                self.wfile.write('{\n')
                self.wfile.write('var keycode;\n')
                self.wfile.write('if (window.event) keycode = window.event.keyCode;\n')
                self.wfile.write('else if (e) keycode = e.which;\n')
                self.wfile.write('else return true;\n')
                self.wfile.write('if (keycode == 13)\n')
                self.wfile.write('   {\n')
                self.wfile.write('   myfield.form.submit();\n')
                self.wfile.write('   return false;\n')
                self.wfile.write('   }\n')
                self.wfile.write('else\n')
                self.wfile.write('   return true;\n')
                self.wfile.write('}\n')
                self.wfile.write('</script>\n')
                self.wfile.write('</head>\n')
                self.wfile.write('<body>\n')
                self.wfile.write('<img src="volt-db.gif">\n')
                self.wfile.write('<form method="POST" enctype="multipart/form-data" action="do_query.volt">\n')
                self.wfile.write('  Enter SQL:<br>\n')
#                self.wfile.write('  <textarea name="sql" rows=20 cols=120 onKeyPress="return submitenter(this,event)"></textarea><br>\n')
                self.wfile.write('  <textarea name="sql" id="styled" ></textarea><br>\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Submit SQL"><br><br><br>\n')
                self.wfile.write('  Display VoltDB Statistics for:<br>\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Tables">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Indexes">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Procedures">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Initiators">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Node Memory">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="SystemInfo">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="IO">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Starvation">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Management">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Snapshot status" />\n' )
                self.wfile.write('  <input type=checkbox name="interval_poll">Report counters since last poll</input>\n' )
                self.wfile.write('  <br/> Snapshot:<br/>\n')
                self.wfile.write('  <input type=checkbox name="blocking_snapshot">Block VoltDB until snapshot completes</input><br/>\n' )
                self.wfile.write('  Path: <textarea name="snapshot_path" id="snapshot_path"> </textarea> <br/>\n')
                self.wfile.write('  Nonce: <textarea name="snapshot_nonce" id="snapshot_path"/> </textarea><br/>')
                self.wfile.write('  <input type=submit name="bsubmit" value="Initiate Snapshot" /> <input type=submit name="bsubmit" value="Restore Snapshot" />')
                self.wfile.write('  <input type=submit name="bsubmit" value="Scan Snapshots" />\n')
                self.wfile.write('  &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp')
                self.wfile.write('  &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp &nbsp')
                self.wfile.write('  <input type=submit name="bsubmit" value="Delete Snapshot" /><br/>\n')

                self.wfile.write('</form>\n')
                self.wfile.write('</body></html>\n')
                return

        except IOError:
            self.send_error(404,'Cannot locate index.html')


    def do_POST(self):
        global client

        try:
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                query=cgi.parse_multipart(self.rfile, pdict)
            sql_text = query.get('sql')[0]
            button_clicked = query.get('bsubmit')[0].upper()
            snapshot_path = query.get('snapshot_path')[0].strip()
            snapshot_nonce = query.get('snapshot_nonce')[0].strip()
            if query.has_key('blocking_snapshot'):
                blocking_snapshot = True
            else:
                blocking_snapshot = False

            if query.has_key('interval_poll'):
                interval_poll = 1
            else:
                interval_poll = 0
            ######################################################################

            self.send_response(200)
            self.send_header('Content-type','text/html')
            self.end_headers()
            self.wfile.write('<html>\n')
            self.wfile.write('<head>\n')
            self.wfile.write('<style type="text/css">\n')
            self.wfile.write('p {color: white; }\n')
            self.wfile.write('body {font-family: Arial, Helvetica, sans-serif;}\n')
            self.wfile.write('table {border-collapse: collapse; border: 1px solid black;}\n')
            self.wfile.write('th {text-align: center; font-weight: bold; color: white; border: 1px solid black; background: black;}\n')
            self.wfile.write('tr#whiterow {border: 1px solid black;}\n')
            self.wfile.write('tr#grayrow  {border: 1px solid black; background: #CCC;}\n')
            self.wfile.write('td#leftalign  {text-align: left;}\n')
            self.wfile.write('td#rightalign {text-align: right;}\n')
            self.wfile.write('</style>\n');
            self.wfile.write('</head>\n')
            self.wfile.write('<body>\n')

            try:
                if client == None:
                    connect_to_server()

                if (button_clicked == 'SUBMIT SQL'):
                    self.wfile.write('SQL<br>\n');
                    self.wfile.write(sql_text + '<br>\n<br>\n');
                    self.wfile.write('RESULTS<br>\n');
                    response = client.execute('adhoc %s' % (sql_text))
                elif (button_clicked == 'TABLES'):
                    self.wfile.write('Table Statistics<br>\n');
                    response = client.execute('stat table %d' % (interval_poll))
                elif (button_clicked == 'INDEXES'):
                    self.wfile.write('Index Statistics<br>\n');
                    response = client.execute('stat index %d' % (interval_poll))
                elif (button_clicked == 'PROCEDURES'):
                    self.wfile.write('Procedure Statistics<br>\n');
                    response = client.execute('stat procedure %d' % (interval_poll))
                elif (button_clicked == 'INITIATORS'):
                    self.wfile.write('Initiator Statistics<br>\n');
                    response = client.execute('stat initiator %d' % (interval_poll))
                elif (button_clicked == 'NODE MEMORY'):
                    self.wfile.write('Node Memory Statistics<br>\n');
                    response = client.execute('stat memory %d' % (interval_poll))
                elif (button_clicked == 'IO'):
                    self.wfile.write('IO Statistics<br>\n');
                    response = client.execute('stat iostats %d' % (interval_poll))
                elif (button_clicked == 'STARVATION'):
                    self.wfile.write('Starvation Statistics<br>\n');
                    response = client.execute('stat starvation %d' % (interval_poll))
                elif (button_clicked == 'MANAGEMENT'):
                    self.wfile.write('Management statistics<br>\n');
                    response = client.execute('stat management %d' % (interval_poll))
                elif (button_clicked == 'SYSTEMINFO'):
                    self.wfile.write('System Information<br>\n');
                    response = client.execute('sysinfo %s' % ("overview"))
                elif (button_clicked == "INITIATE SNAPSHOT"):
                    self.wfile.write("Attempting to initiate snapshot to ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    if blocking_snapshot:
                        response = client.execute('snapshotsave %s %s %d' %
                                                  (snapshot_path, snapshot_nonce, 1));
                    else:
                        response = client.execute('snapshotsave %s %s %d' %
                                                  (snapshot_path, snapshot_nonce, 0));
                elif (button_clicked == "RESTORE SNAPSHOT"):
                    self.wfile.write("Attempting to restore snapshot from ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    response = client.execute('snapshotrestore %s %s' %
                                              (snapshot_path, snapshot_nonce));
                elif (button_clicked == "SNAPSHOT STATUS"):
                    self.wfile.write("Retrieving snapshot status")
                    response = client.execute('snapshotstatus')
                elif (button_clicked == "SCAN SNAPSHOTS"):
                    self.wfile.write("Scanning for snapshots in ")
                    self.wfile.write(snapshot_path)
                    response = client.execute('snapshotscan %s' %
                                              (snapshot_path))
                elif (button_clicked == "DELETE SNAPSHOT"):
                    self.wfile.write("Attempting to delete snapshot in ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    response = client.execute('snapshotdelete %s %s' %
                                              (snapshot_path, snapshot_nonce))
                else:
                    print "Illegal call attempted by page."
                    exit(-1)

                # format the result set into an HTML table
                for i in range(len(response.tables)):
                    self.wfile.write('<table border="1">\n');

                    self.wfile.write('  <tr>\n');
                    # print column headers
                    colTypes = list()
                    for thisColumn in response.tables[i].columns:
                        # thisColumn.type for column type
                        colTypes.append(thisColumn.type)
                        self.wfile.write('    <th>'+str(thisColumn.name).strip()+'</th>\n');
                    self.wfile.write('  </tr>\n');

                    grayBar = 0
                    rowsDisplayed = 0
                    for thisTuple in response.tables[i].tuples:
                        if (grayBar == 0):
                            self.wfile.write('  <tr id="whiterow">\n');
                            grayBar = 1
                        else:
                            self.wfile.write('  <tr id="grayrow">\n');
                            grayBar = 0

                        rowsDisplayed = rowsDisplayed + 1
                        colCounter = 0
                        for thisValue in thisTuple:
                            if thisValue == None:
                                val = "NULL"
                            else:
                                val = thisValue
                            if (colTypes[colCounter] == 9):
                                self.wfile.write('    <td id="leftalign">'+str(val).strip()+'</td>\n');
                            else:
                                self.wfile.write('    <td id="rightalign">'+str(val).strip()+'</td>\n');
                            colCounter += 1
                        self.wfile.write('  </tr>\n');

                    self.wfile.write('</table>\n');
                    self.wfile.write('<br />' + str(rowsDisplayed) + ' Row(s) Displayed.\n');

                # In case of error from the server
                if response.status != 1:
                    self.wfile.write(cgi.escape(str(response))\
                                         .replace("\n", "<br />\n"))

            except IOError, e:
                print e
                self.wfile.write("Error: %s<br>\n" % (e))
                client = None
            except Exception, e:
                print "something really bad just happened"
                print e
                self.wfile.write("sql failure<br>\n");
                self.wfile.write(e);

            ######################################################################

            self.wfile.write("<br>\n<br>\n<a href='index.html>Click here to enter another SQL statement.</a>\n");
            self.wfile.write("</body>\n");
            self.wfile.write("</html>\n");

        except :
            pass



def connect_to_server():
    global client

    filename = None
    if len(sys.argv) >= 2:
        filename = sys.argv[1]

    try:
        if client != None:
            client.close()
        print "Connecting to server at ", volt_server_ip, \
            " on port ", volt_server_port
        client = VoltQueryClient(volt_server_ip, volt_server_port,
                                 volt_username, volt_password,
                                 dump_file = filename)
        client.set_quiet(True)
    except socket.error:
        raise IOError("Error connecting to the server")

def main():
    try:
        connect_to_server()

        server = HTTPServer(('', http_server_port), HTTPHandler)
        print 'starting server...'
        server.serve_forever()
    except KeyboardInterrupt:
        print 'stopping server...'
        server.socket.close()
        if client != None:
            client.close()

if __name__ == '__main__':
    main()
