(function( window, undefined ){

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
            if (!(procedure in this.Procedures))
                return ['Procedure "' + procedure + '" is undefined.'];
            if (jQuery.isArray(parameters) && (parameters.length != this.Procedures[procedure].length))
                return ['Invalid parameter count for procedure "' + procedure + '" (received: ' + parameters.length + ', expected: ' + this.Procedures[procedure].length + ')' ];

            s[s.length] = encodeURIComponent('Procedure') + '=' + encodeURIComponent(procedure);
            if (parameters != null)
            {
                var params = '[';
                if (!jQuery.isArray(parameters))
                    parameters = [parameters];
                for(var i = 0; i < parameters.length; i++)
                {
                    if (i > 0)
                        params += ',';
                    switch(this.Procedures[procedure][i])
                    {
                        case 'tinyint':
                        case 'smallint':
                        case 'int':
                        case 'integer':
                        case 'bigint':
                        case 'float':
                            params += parameters[i];
                            break;
                        case 'decimal':
                            params += '"' + parameters[i] + '"';
                            break;
                        case 'bit':
                            if (parameters[i] == "'true'" || parameters[i] == 'true' || parameters[i] == "'yes'" || parameters[i] == 'yes' || parameters[i] == '1' || parameters[i] == 1)
                                params += '1';
                            else
                                params += '0';
                            break;
                        default:
                            if (procedure == '@SnapshotDelete')
                                params += '["' + parameters[i].replace(/^'|'$/g,'') + '"]';
                            else
                                params += (typeof(parameters[i]) == 'string' ? '"' + parameters[i].replace(/^'|'$/g,'') + '"' : parameters[i]).replace(/''/g,"'");
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
            var CriticalErrorResponse = {"status":-1,"statusstring":"Timeout or critical execution error.","results":[]};
            var UserCallback = userCallback;
            var Timeout = setTimeout(function() {UserCallback(CriticalErrorResponse);}, 5000);
            this.Callback = function(response) { clearTimeout(Timeout); UserCallback(response); }
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
                            '@AdHoc': ['varchar']
                          , '@Pause': []
                          , '@Quiesce': []
                          , '@Resume': []
                          , '@Shutdown': []
                          , '@SnapshotDelete': ['varchar', 'varchar']
                          , '@SnapshotRestore': ['varchar', 'varchar']
                          , '@SnapshotSave': ['varchar', 'varchar', 'bit']
                          , '@SnapshotScan': ['varchar']
                          , '@SnapshotStatus': []
                          , '@Statistics': ['StatisticsComponent', 'bit' ]
                          , '@SystemCatalog': ['CatalogComponent']
                          , '@SystemInformation': ['SysInfoSelector']
                          , '@UpdateApplicationCatalog': ['varchar', 'varchar']
                          , '@UpdateLogging': ['xml']
                        };
        return this;
    }

    LoadConnectionMetadata = function(connection, onconnectionready) {
        var connectionQueue = connection.getQueue();
        connectionQueue.Start()
            .BeginExecute('@Statistics', ['TABLE',0], function(data) { connection.Metadata['rawTables'] = data.results[0]; })
            .BeginExecute('@Statistics', ['INDEX',0], function(data) { connection.Metadata['rawIndexes'] = data.results[0]; })
            .BeginExecute('@Statistics', ['PARTITIONCOUNT',0], function(data) { connection.Metadata['partitionCount'] = data.results[0].data[0][0]; })
            .BeginExecute('@Statistics', ['STARVATION',0], function(data) { connection.Metadata['siteCount'] = data.results[0].data.length; })
            .BeginExecute('@SystemCatalog', ['PROCEDURES'], function(data) { connection.Metadata['procedures'] = data.results[0]; })
            .BeginExecute('@SystemCatalog', ['PROCEDURECOLUMNS'], function(data) { connection.Metadata['procedurecolumns'] = data.results[0]; })
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
                                                    '@Pause': ['Returns bit']
                                                  , '@Quiesce': ['Returns bit']
                                                  , '@Resume': ['Returns bit']
                                                  , '@Shutdown': ['Returns bit']
                                                  , '@SnapshotDelete': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]']
                                                  , '@SnapshotRestore': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]']
                                                  , '@SnapshotSave': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Blocking (bit)', 'Returns Table[]']
                                                  , '@SnapshotScan': ['DirectoryPath (varchar)', 'Returns Table[]']
                                                  , '@SnapshotStatus': ['Returns Table[]']
                                                  , '@Statistics': ['Statistic (StatisticsComponent)', 'Interval (bit)', 'Returns Table[]' ]
                                                  , '@SystemCatalog':['SystemCatalog (CatalogComponent)', 'Returns Table[]']
                                                  , '@SystemInformation': ['Selector (SysInfoSelector)', 'Returns Table[]']
                                                  , '@UpdateApplicationCatalog': ['CatalogPath (varchar)', 'DeploymentConfigPath (varchar)', 'Returns Table[]']
                                                  , '@UpdateLogging': ['Configuration (xml)', 'Returns Table[]']
                                                  };
                var childConnectionQueue = connection.getQueue();
                childConnectionQueue.Start(true);
                for(var table in connection.Metadata['tables'])
                    childConnectionQueue.BeginExecute('@AdHoc', 'SELECT TOP 1 * FROM ' + table, (new OnGetSchema(connection, 'tables', table)).Callback);
                for(var view in connection.Metadata['views'])
                    childConnectionQueue.BeginExecute('@AdHoc', 'SELECT TOP 1 * FROM ' + view, (new OnGetSchema(connection, 'views', view)).Callback);
                childConnectionQueue.End(function(state) { connection.Ready = true; if (onconnectionready != null) onconnectionready(connection, state); }, null);
            }, null);
    }

    var OnGetSchema = function(connection,type, name)
    {
        var Connection = connection;
        var Type = type;
        var Name = name;
        this.Callback = function(tabledata)
        {
            if (tabledata.status == 1)
            {
                var DBType = { '3': 'tinyint', '4': 'smallint', '5': 'int', '6': 'bigint', '8': 'float', '9': 'varchar', '11': 'timestamp', '22': 'decimal', '23': 'decimal', '25': 'varbinary' };
                var columns = [];
                for (var j = 0; j < tabledata.results[0].schema.length; j++)
                    columns[tabledata.results[0].schema[j].name] = tabledata.results[0].schema[j].name + ' (' + DBType[''+tabledata.results[0].schema[j].type] + ')';
                Connection.Metadata[Type][Name].columns = columns;
            }
        }
    }

    this.TestConnection = function(server, port, admin, user, password, isHashedPassword, onConnectionTested)
    {
        var conn = new Connection(server, port, admin, user, password, isHashedPassword);
        var timeout = setTimeout(function() {onConnectionTested(false);}, 5000);
        conn.BeginExecute('@Statistics', ['TABLE',0], function(response) { try { if (response.status == 1) {clearTimeout(timeout); onConnectionTested(true); } else onConnectionTested(false);} catch(x) {clearTimeout(timeout); onConnectionTested(true);} });
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

