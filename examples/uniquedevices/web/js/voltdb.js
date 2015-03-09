/*
 This file is part of VoltDB.
 Copyright (C) 2008-2014 VoltDB Inc.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
*/

(function (window, unused){

var IVoltDB = (function(){

    this.Connections = {};

    Connection = function(server, port, admin, user, password, isHashedPassword)
    {
        server = server == null ? '' : jQuery.trim(server);
        port = port == null ? '' : jQuery.trim(port);
        user = user == null ? '' : jQuery.trim(user);
        password = password == null ? '' : jQuery.trim(password);

        this.Server = server == '' ? 'localhost' : server;
        this.Port = port == '' ? 8080 : port;
        this.Admin = admin == true;
        this.User = user == '' ? null : user;
        this.Password = password == '' ? null : (isHashedPassword == false ? password : null);
        this.HashedPassword = password == '' ? null : (isHashedPassword == true ? password : null);

        this.Metadata = {};
        this.Ready = false;

        this.Key = (this.Server + '_' + this.Port + '_' + (user == ''?'':user) + '_' + (this.Admin == true?'Admin':'')).replace(/[^_a-zA-Z0-9]/g,"_");
        this.Display = this.Server + ':' + this.Port + (user == ''?'':' (' + user + ')') + (this.Admin == true?' - Admin':'');

        this.BuildURI = function(procedure, parameters)
        {
            var s = [];
            if (!this.Procedures.hasOwnProperty(procedure)) {
                return ['Procedure "' + procedure + '" is undefined.'];
            }

            var signatures = this.Procedures[procedure];
            var localParameters = [];
            localParameters = localParameters.concat(parameters);

            if (!(signatures['' + localParameters.length])) {
                var retval = 'Invalid parameter count for procedure "' + procedure + '" (received: ' + localParameters.length + ', expected: ';
                for ( x in signatures ) {
                    retval += x + ', ';
                }
                return [ retval + ')' ];
            }
            var signature = signatures['' + localParameters.length];

            s[s.length] = encodeURIComponent('Procedure') + '=' + encodeURIComponent(procedure);
            if (localParameters != null)
            {
                var params = '[';
                var i = 0;
                for(i = 0; i < localParameters.length; i++) {
                    if (i > 0) {
                        params += ',';
                    }
                    switch(signature[i])
                    {
                        case 'tinyint':
                        case 'smallint':
                        case 'int':
                        case 'integer':
                        case 'bigint':
                        case 'float':
                            params += localParameters[i];
                            break;
                        case 'decimal':
                            params += '"' + localParameters[i] + '"';
                            break;
                        case 'bit':
                            if (localParameters[i] == "'true'" || localParameters[i] == 'true' ||
                                localParameters[i] == "'yes'" || localParameters[i] == 'yes' ||
                                localParameters[i] == '1' || localParameters[i] == 1)
                                params += '1';
                            else
                                params += '0';
                            break;
                        case 'varbinary':
                            params += localParameters[i];
                            break;
                        default:
                            if (procedure == '@SnapshotDelete')
                                params += '["' + localParameters[i].replace(/^'|'$/g,'') + '"]';
                            else
                                params += (typeof(localParameters[i]) == 'string'
                                            ? '"' + localParameters[i].replace(/^'|'$/g,'') + '"'
                                            : localParameters[i]).replace(/''/g,"'");
                    }
                }
                params += ']';
                s[s.length] = encodeURIComponent('Parameters') + '=' + encodeURIComponent(params);
            }
            if (this.User != null)
                s[s.length] = encodeURIComponent('User') + '=' + encodeURIComponent(this.User);
            if (this.Password != null)
                s[s.length] = encodeURIComponent('Password') + '=' + encodeURIComponent(this.Password);
            if (this.HashedPassword != null)
                s[s.length] = encodeURIComponent('Hashedpassword') + '=' + encodeURIComponent(this.HashedPassword);
            if (this.Admin)
                s[s.length] = 'admin=true';
            var uri = 'http://' + this.Server + ':' + this.Port + '/api/1.0/?' + s.join('&') + '&jsonp=?';
            return uri;
        }
        this.CallExecute = function(procedure, parameters, callback)
        {
            var uri = this.BuildURI(procedure, parameters);
            if (typeof(uri) == 'string')
                jQuery.getJSON(uri, callback);
            else
                if (callback != null)
                    callback({"status":-1,"statusstring":"PrepareStatement error: " + uri[0],"results":[]});
        }
        var CallbackWrapper = function(userCallback)
        {
            var CriticalErrorResponse = {"status":-1,"statusstring":"Query timeout.","results":[]};
            var UserCallback = userCallback;
            var TimeoutOccurred = 0;
            var Timeout = setTimeout(function() {TimeoutOccurred=1;UserCallback(CriticalErrorResponse);}, 20000);
            this.Callback = function(response) { clearTimeout(Timeout); if (TimeoutOccurred == 0) UserCallback(response); }
            return this;
        }
        this.BeginExecute = function(procedure, parameters, callback)
        {
            this.CallExecute(procedure, parameters, (new CallbackWrapper(callback)).Callback);
        }

        var IQueue = function(connection)
        {
            var ContinueOnFailure = false;
            var Executing = false;
            var Success = false;
            var Stack = [];
            var OnCompleteHandler = null;
            var Connection = connection;
            this.Start = function(continueOnFailure)
            {
                if (Executing)
                    return null;
                ContinueOnFailure = (continueOnFailure == true);
                OnCompleteHandler = null;
                Success = true;
                Stack.push(null);
                return this;
            }
            this.BeginExecute = function(procedure, parameters, callback)
            {
                Stack.push([procedure, parameters, callback]);
                return this;
            }
            this.EndExecute = function()
            {
                if (Stack.length > 0)
                    Stack.splice(0,1);
                if (Stack.length > 0 && (Success || ContinueOnFailure))
                {
                    var item = Stack[0];
                    Connection.CallExecute(item[0], item[1], (new CallbackWrapper(
                                                                (function(queue,item) {
                                                                    return function(response) {
                                                                        try
                                                                        {
                                                                            if (response.status != 1)
                                                                                Success = false;
                                                                            if (item[2] != null)
                                                                                item[2](response);
                                                                            queue.EndExecute();
                                                                        }
                                                                        catch(x)
                                                                        {
                                                                            Success = false;
                                                                            queue.EndExecute();
                                                                        }
                                                                    };
                                                                })(this,item))).Callback);
                }
                else
                {
                    Executing = false;
                    if (OnCompleteHandler != null)
                    {
                        try { OnCompleteHandler[0](OnCompleteHandler[1], Success); } catch(x) {}
                    }
                }
                return this;
            }
            this.End = function(fcn, state)
            {
                OnCompleteHandler = [fcn, state];
                if (!Executing)
                {
/*
                    if (Stack.length == 0)
                    {
                        this.EndExecute();
                        return;
                    }
*/
                    Executing = true;
                    this.EndExecute();
/*
                    var item = Stack[0];
                    Connection.CallExecute(item[0], item[1], (new CallbackWrapper((function(queue,item) {
                                                                return function(response) {
                                                                    try
                                                                    {
                                                                        if (response.status != 1)
                                                                            Success = false;
                                                                        if (item[2] != null)
                                                                            item[2](response);
                                                                        queue.EndExecute();
                                                                    }
                                                                    catch(x)
                                                                    {
                                                                        Success = false;
                                                                        queue.EndExecute();
                                                                    }
                                                                };
                                                            })(this,item))).Callback);
*/
                }
            }
        }
        this.getQueue = function()
        {
            return (new IQueue(this));
        }
        this.Procedures = {
                            '@AdHoc': { '1' : ['varchar'] }
                          , '@Explain': { '1' : ['varchar'] }
                          , '@ExplainProc': { '1' : ['varchar'] }
                          , '@Pause': { '0' : [] }
                          , '@Promote': { '0' : [] }
                          , '@Quiesce': { '0' : [] }
                          , '@Resume': { '0' : [] }
                          , '@Shutdown': { '0' : [] }
                          , '@SnapshotDelete': { '2' : ['varchar', 'varchar'] }
                          , '@SnapshotRestore': { '1' : ['varchar'],'2' : ['varchar', 'varchar'] }
                          , '@SnapshotSave': { '3' : ['varchar', 'varchar', 'bit'], '1' : ['varchar'] }
                          , '@SnapshotScan': { '1' : ['varchar'] }
                          , '@SnapshotStatus': { '0' : [] }
                          , '@Statistics': { '2' : ['StatisticsComponent', 'bit' ] }
                          , '@SystemCatalog': { '1' : ['CatalogComponent'] }
                          , '@SystemInformation': { '1' : ['SysInfoSelector'] }
                          , '@UpdateApplicationCatalog': { '2' : ['varchar', 'varchar'] }
                          , '@UpdateLogging': { '1' : ['xml'] }
                          , '@ValidatePartitioning': { '2': ['int', 'varbinary']}
                        };
        return this;
    }

    LoadConnectionMetadata = function(connection, onconnectionready) {
        var connectionQueue = connection.getQueue();
        connectionQueue.Start()
            .BeginExecute('@Statistics', ['TABLE',0], function(data) { connection.Metadata['rawTables'] = data.results[0]; })
            .BeginExecute('@Statistics', ['INDEX',0], function(data) { connection.Metadata['rawIndexes'] = data.results[0]; })
            .BeginExecute('@Statistics', ['PARTITIONCOUNT',0], function(data) { connection.Metadata['partitionCount'] = data.results[0].data[0][3]; })
            .BeginExecute('@Statistics', ['STARVATION',0], function(data) { connection.Metadata['siteCount'] = data.results[0].data.length; })
            .BeginExecute('@SystemCatalog', ['PROCEDURES'], function(data) { connection.Metadata['procedures'] = data.results[0]; })
            .BeginExecute('@SystemCatalog', ['PROCEDURECOLUMNS'], function(data) { connection.Metadata['procedurecolumns'] = data.results[0]; })
            .BeginExecute('@SystemCatalog', ['COLUMNS'], function(data) { connection.Metadata['rawColumns'] = data.results[0]; })
            .End(function(state){
                var tables = [];
                var exports = [];
                var views = [];
                for(var i = 0; i < connection.Metadata['rawTables'].data.length; i++)
                {
                    var tableName = connection.Metadata['rawTables'].data[i][5];
                    if (connection.Metadata['rawTables'].data[i][6] == 'StreamedTable')
                        exports[tableName] = { name: tableName };
                    else
                    {
                        var isView = false;
                        var item = { name: tableName, key: null, indexes: null, columns: null };
                        for(var j = 0; j < connection.Metadata['rawIndexes'].data.length; j++)
                        {
                            if (connection.Metadata['rawIndexes'].data[j][6].toUpperCase() == tableName.toUpperCase())
                            {
                                var indexName = connection.Metadata['rawIndexes'].data[j][5];
                                if (item.indexes == null)
                                    item.indexes = [];
                                item.indexes[indexName] = indexName + ' (' + ((connection.Metadata['rawIndexes'].data[j][7].toLowerCase().indexOf('hash') > -1)?'Hash':'Tree') + (connection.Metadata['rawIndexes'].data[j][8] == "1"?', Unique':'') + ')';
                                if (indexName.toUpperCase().indexOf("MATVIEW") > -1)
                                    isView = true;
                                if (indexName.toUpperCase().indexOf("PK_") > -1)
                                    item.key = indexName;
                            }
                        }
                        if (isView)
                            views[tableName] = item;
                        else
                            tables[tableName] = item;
                    }
                }
                connection.Metadata['tables'] = tables;
                connection.Metadata['views'] = views;
                connection.Metadata['exports'] = exports;
                connection.Metadata['sysprocs'] = {
                                                    '@Explain': { '1' : ['SQL (varchar)', 'Returns Table[]'] }
                                                  , '@ExplainProc': { '1' : ['Stored Procedure Name (varchar)', 'Returns Table[]'] }
                                                  , '@Pause': { '0' : ['Returns bit'] }
                                                  , '@Quiesce': { '0' : ['Returns bit'] }
                                                  , '@Resume': { '0' : ['Returns bit'] }
                                                  , '@Shutdown': { '0' : ['Returns bit'] }
                                                  , '@SnapshotDelete': { '2' : ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]'] }
                                                  , '@SnapshotRestore': { '2' : ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]'], '1' : ['JSON (varchar)', 'Returns Table[]'] }
                                                  , '@SnapshotSave': { '3' : ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Blocking (bit)', 'Returns Table[]'], '1' : ['JSON (varchar)', 'Returns Table[]']  }
                                                  , '@SnapshotScan': { '1' : ['DirectoryPath (varchar)', 'Returns Table[]'] }
                                                  , '@SnapshotStatus': { '0' : ['Returns Table[]'] }
                                                  , '@Statistics': { '2' : ['Statistic (StatisticsComponent)', 'Interval (bit)', 'Returns Table[]' ] }
                                                  , '@SystemCatalog': { '1' : ['SystemCatalog (CatalogComponent)', 'Returns Table[]'] }
                                                  , '@SystemInformation': { '1' : ['Selector (SysInfoSelector)', 'Returns Table[]'] }
                                                  , '@UpdateApplicationCatalog': { '2' : ['CatalogPath (varchar)', 'DeploymentConfigPath (varchar)', 'Returns Table[]'] }
                                                  , '@UpdateLogging': { '1' : ['Configuration (xml)', 'Returns Table[]'] }
                                                  , '@Promote': { '0' : ['Returns bit'] }
                                                  , '@ValidatePartitioning': { '2' : ['HashinatorType (int)', 'Config (varbinary', 'Returns Table[]'] }
                                                  };

                /*
                 * Accumulate the column names for tables from the SystemCatalog COLUMNS values
                 * c.Metadata[rawColumns] is the output from @SystemCatalog Columns.
                 * Walk those rows and fill in table or view column name and type.
                 */

                for(var i = 0; i < connection.Metadata['rawColumns'].data.length; i++)
                {
                    var Type = 'tables';
                    var TableName = connection.Metadata['rawColumns'].data[i][2].toUpperCase();
                    if (connection.Metadata['tables'][TableName] != null) {
                        if (connection.Metadata['tables'][TableName].columns == null) {
                            connection.Metadata['tables'][TableName].columns = [];
                        }
                        connection.Metadata['tables'][TableName].columns[connection.Metadata['rawColumns'].data[i][16]] =
                            connection.Metadata['rawColumns'].data[i][3].toUpperCase() +
                            ' (' + connection.Metadata['rawColumns'].data[i][5].toLowerCase() + ')';
                    }
                    else if (connection.Metadata['views'][TableName] != null) {
                        if (connection.Metadata['views'][TableName].columns == null) {
                            connection.Metadata['views'][TableName].columns = [];
                        }
                        connection.Metadata['views'][TableName].columns[connection.Metadata['rawColumns'].data[i][3].toUpperCase()] =
                            connection.Metadata['rawColumns'].data[i][3].toUpperCase() +
                            ' (' + connection.Metadata['rawColumns'].data[i][5].toLowerCase() + ')';
                    }
                }


                 // User Procedures
                 for (var i = 0; i < connection.Metadata['procedures'].data.length; ++i)
                 {
                    var connTypeParams = [];
                    var procParams = [];
                    var procName = connection.Metadata['procedures'].data[i][2];
                    for (var p = 0; p < connection.Metadata['procedurecolumns'].data.length; ++p)
                    {
                        if (connection.Metadata['procedurecolumns'].data[p][2] == procName)
                        {
                            paramType = connection.Metadata['procedurecolumns'].data[p][6];
                            paramName = connection.Metadata['procedurecolumns'].data[p][3];
                            paramOrder = connection.Metadata['procedurecolumns'].data[p][17] - 1;
                            procParams[paramOrder] = {'name': paramName, 'type': paramType.toLowerCase()};
                        }
                    }

                    for (var p = 0; p < procParams.length; ++p) {
                        connTypeParams[connTypeParams.length] = procParams[p].type;
                    }

                    // make the procedure callable.
                    connection.Procedures[procName] = {};
                    connection.Procedures[procName]['' + connTypeParams.length] = connTypeParams;
                 }


                var childConnectionQueue = connection.getQueue();
                childConnectionQueue.Start(true);
                childConnectionQueue.End(function(state) { connection.Ready = true; if (onconnectionready != null) onconnectionready(connection, state); }, null);
            }, null);
    }

    this.TestConnection = function(server, port, admin, user, password, isHashedPassword, onConnectionTested)
    {
        var conn = new Connection(server, port, admin, user, password, isHashedPassword);
        conn.BeginExecute('@Statistics', ['TABLE',0], function(response) { try { if (response.status == 1) {onConnectionTested(true); } else onConnectionTested(false);} catch(x) {onConnectionTested(true);} });
    }
    this.AddConnection = function(server, port, admin, user, password, isHashedPassword, onConnectionAdded)
    {
        var conn = new Connection(server, port, admin, user, password, isHashedPassword);
        this.Connections[conn.Key] = conn;
        LoadConnectionMetadata(this.Connections[conn.Key], onConnectionAdded);
        return this.Connections[conn.Key];
    }
    this.RemoveConnection = function(connection, onConnectionRemoved)
    {
        delete this.Connections[connection.Key];
        onConnectionRemoved(connection);
    }
    this.HasConnection = function(server, port, admin, user)
    {
        var conn = new Connection(server, port, admin, user, null, false);
        if (conn.Key in this.Connections)
            return true;
        return false;
    }
    this.HasConnectionKey = function(key)
    {
        if (key in this.Connections)
            return true;
        return false;
    }
    this.GetConnection = function(key)
    {
        if (key in this.Connections)
            return this.Connections[key];
        return null;
    }
    this.DBType = { '3': 'tinyint', '4': 'smallint', '5': 'int', '6': 'bigint', '8': 'float', '9': 'varchar', '11': 'timestamp', '22': 'decimal', '23': 'decimal', '25': 'varbinary' };
});
window.VoltDB = VoltDB = new IVoltDB();
})(window);

Object.size = function(obj) {
    var size = 0, key;
    for (key in obj) {
        if (obj.hasOwnProperty(key)) size++;
    }
    return size;
};

