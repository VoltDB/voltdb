from os import curdir, sep
import mimetypes
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import cgi
from fastserializer import *
import socket

# volt server IP address and port
volt_server_ip = 'localhost'
volt_server_port = 21212

# volt username/password if database security is enabled
volt_username = ''
volt_password = ''

# port on local server to listen for http requests, URL is formatted as http://localhost:9001 
http_server_port = 9001


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
                self.wfile.write('  <input type=submit name="bsubmit" value="Procedures">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Initiators">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="SystemInfo">\n')
                self.wfile.write('  <input type=submit name="bsubmit" value="Snapshot status" />\n' )
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
            ######################################################################
            print "Connecting to server at ", volt_server_ip, " on port ", volt_server_port

            try:
                fs = FastSerializer(volt_server_ip, volt_server_port, volt_username, volt_password)
            except socket.error:
                print "Error connecting to the server"
                exit(-1)

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
                if (button_clicked == 'SUBMIT SQL'):
                    self.wfile.write('SQL<br>\n');
                    self.wfile.write(sql_text + '<br>\n<br>\n');
                    self.wfile.write('RESULTS<br>\n');
                    adhoc = VoltProcedure(fs, "@adhoc",[FastSerializer.VOLTTYPE_STRING])
                    response = adhoc.call([sql_text], timeout = None)
                elif (button_clicked == 'TABLES'):
                    self.wfile.write('Table Statistics<br>\n');
                    stats = VoltProcedure(fs, "@Statistics",[FastSerializer.VOLTTYPE_STRING])
                    response = stats.call(['table'], timeout = None)
                elif (button_clicked == 'PROCEDURES'):
                    self.wfile.write('Procedure Statistics<br>\n');
                    stats = VoltProcedure(fs, "@Statistics",[FastSerializer.VOLTTYPE_STRING])
                    response = stats.call(['procedure'], timeout = None)
                elif (button_clicked == 'INITIATORS'):
                    self.wfile.write('Initiator Statistics<br>\n');
                    stats = VoltProcedure(fs, "@Statistics",[FastSerializer.VOLTTYPE_STRING])
                    response = stats.call(['initiator'], timeout = None)
                elif (button_clicked == 'SYSTEMINFO'):
                    self.wfile.write('System Information<br>\n');
                    stats = VoltProcedure(fs, "@SystemInformation",[])
                    response = stats.call([], timeout = None)
                elif (button_clicked == "INITIATE SNAPSHOT"):
                    self.wfile.write("Attempting to initiate snapshot to ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    initiate_proc = VoltProcedure(fs, "@SaveTablesToDisk", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_TINYINT]);
                    if blocking_snapshot:
                        response = initiate_proc.call([snapshot_path, snapshot_nonce, 1], timeout = None);
                    else:
                        response = initiate_proc.call([snapshot_path, snapshot_nonce, 0], timeout = None);
                elif (button_clicked == "RESTORE SNAPSHOT"):
                    self.wfile.write("Attempting to restore snapshot from ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    initiate_proc = VoltProcedure(fs, "@RestoreTablesFromDisk", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING]);
                    if blocking_snapshot:
                        response = initiate_proc.call([snapshot_path, snapshot_nonce], timeout = None);
                    else:
                        response = initiate_proc.call([snapshot_path, snapshot_nonce], timeout = None);
                elif (button_clicked == "SNAPSHOT STATUS"):
                    self.wfile.write("Retrieving snapshot status")
                    initiate_proc = VoltProcedure(fs, "@SnapshotStatus", [])
                    response = initiate_proc.call([], timeout = None);
                elif (button_clicked == "SCAN SNAPSHOTS"):
                    self.wfile.write("Scanning for snapshots in ")
                    self.wfile.write(snapshot_path)
                    initiate_proc = VoltProcedure(fs, "@SnapshotScan", [FastSerializer.VOLTTYPE_STRING]);
                    response = initiate_proc.call([snapshot_path], timeout = None);
                elif (button_clicked == "DELETE SNAPSHOT"):
                    self.wfile.write("Attempting to delete snapshot in ")
                    self.wfile.write(snapshot_path)
                    self.wfile.write(' with nonce ')
                    self.wfile.write(snapshot_nonce)
                    initiate_proc = VoltProcedure(fs, "@SnapshotDelete", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING]);
                    response = initiate_proc.call([[snapshot_path], [snapshot_nonce]], timeout = None);
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
                            if (colTypes[colCounter] == 9):
                                self.wfile.write('    <td id="leftalign">'+str(thisValue).strip()+'</td>\n');
                            else:
                                self.wfile.write('    <td id="rightalign">'+str(thisValue).strip()+'</td>\n');
                            colCounter += 1
                        self.wfile.write('  </tr>\n');

                    self.wfile.write('</table>\n');
                    self.wfile.write('<br />' + str(rowsDisplayed) + ' Row(s) Displayed.\n');

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





def main():
    try:
        server = HTTPServer(('', http_server_port), HTTPHandler)
        print 'starting server...'
        server.serve_forever()
    except KeyboardInterrupt:
        print 'stopping server...'
        server.socket.close()

if __name__ == '__main__':
    main()



