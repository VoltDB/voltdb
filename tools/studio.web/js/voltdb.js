(function( window, undefined ){

var IVoltDB = (function(){

	this.Connections = {};

	Connection = function(server, port, admin, user, password, isHashedPassword)
	{
		server = server == null ? '' : jQuery.trim(server);
		port = port == null ? '' : jQuery.trim(port);
		user = user == null ? '' : jQuery.trim(user);
		password = password == null ? '' : jQuery.trim(password);

		var Server = server == '' ? 'localhost' : server;
		var Port = port == '' ? 8080 : port;
		var Admin = admin == true;
		var User = user == '' ? null : user;
		var Password = password == '' ? null : (isHashedPassword == false ? password : null);
		var HashedPassword = password == '' ? null : (isHashedPassword == true ? password : null);

		this.Metadata = {};
		this.Ready = false;

		this.Key = (Server + '_' + Port + '_' + (user == ''?'':user) + '_' + (Admin == true?'Admin':'')).replace(/[^_a-zA-Z0-9]/g,"_");
		this.Display = Server + ':' + Port + (user == ''?'':' (' + user + ')') + (Admin == true?' - Admin':'');

		BuildURI = function(procedure, parameters)
		{
			var s = [];
			if (!(procedure in Procedures))
				return ['Procedure "' + procedure + '" is undefined.'];
			if (jQuery.isArray(parameters) && (parameters.length != Procedures[procedure].length))
				return ['Invalid parameter count for procedure "' + procedure + '" (received: ' + parameters.length + ', expected: ' + Procedures[procedure].length + ')' ];

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
					switch(Procedures[procedure][i])
					{
						case 'tinyint':
						case 'smallint':
						case 'int':
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
			if (User != null)
				s[s.length] = encodeURIComponent('User') + '=' + encodeURIComponent(User);
			if (Password != null)
				s[s.length] = encodeURIComponent('Password') + '=' + encodeURIComponent(Password);
			if (HashedPassword != null)
				s[s.length] = encodeURIComponent('Hashedpassword') + '=' + encodeURIComponent(HashedPassword);
			if (Admin)
				s[s.length] = 'admin=true';
			var uri = 'http://' + Server + ':' + Port + '/api/1.0/?' + s.join('&') + '&jsonp=?';
			return uri;
		}
		function CallExecute(procedure, parameters, callback)
		{
			var uri = BuildURI(procedure, parameters);
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
			CallExecute(procedure, parameters, (new CallbackWrapper(callback)).Callback);
		}

		this.Queue = new (function(){
			var ContinueOnFailure = false;
			var Executing = false;
			var Success = false;
			var Stack = [];
			var OnCompleteHandler = null;
			this.Start = function(continueOnFailure)
			{
				if (Executing)
					return null;
				ContinueOnFailure = (continueOnFailure == true);
				OnCompleteHandler = null;
				Success = true;
				return this;
			}
			this.BeginExecute = function(procedure, parameters, callback)
			{
				Stack.push([procedure, parameters, callback]);
/*
				if (!Executing)
				{
					Executing = true;
					var item = Stack[0];
					CallExecute(item[0], item[1], (new CallbackWrapper(function(response) { try { if (response.status != 1) {Success = false;} if (item[2] != null) item[2](response); EndExecute(); } catch(x) {alert(x);Success = false;EndExecute();} })).Callback);
				}
*/
				return this;
			}
			EndExecute = function()
			{
				if (Stack.length > 0)
					Stack.splice(0,1);
				if (Stack.length > 0 && (Success || ContinueOnFailure))
				{
					var item = Stack[0];
					CallExecute(item[0], item[1], (new CallbackWrapper(function(response) { try { if (response.status != 1) {Success = false;} if (item[2] != null) item[2](response); EndExecute(); } catch(x) {alert(x);Success = false;EndExecute();} })).Callback);
				}
				else
				{
					Executing = false;
					if (OnCompleteHandler != null)
						OnCompleteHandler[0](OnCompleteHandler[1], Success);
				}
				return this;
			}
			this.End = function(fcn, state)
			{
				OnCompleteHandler = [fcn, state];
				if (!Executing)
				{
					Executing = true;
					var item = Stack[0];
					CallExecute(item[0], item[1], (new CallbackWrapper(function(response) { try { if (response.status != 1) {Success = false;} if (item[2] != null) item[2](response); EndExecute(); } catch(x) {alert(x);Success = false;EndExecute();} })).Callback);
				}
/*
				if (!Executing && OnCompleteHandler != null)
					OnCompleteHandler[0](OnCompleteHandler[1], (Success || ContinueOnFailure));
*/
				return this;
			}
		});
		var Procedures = {
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
						  , '@SystemInformation': []
						  , '@UpdateApplicationCatalog': ['varchar', 'varchar']
						  , '@UpdateLogging': ['xml']
						};
		this.DeclareProcedure = function(procedureInfo)
		{
			if (procedureInfo.name.indexOf('@') == -1)
			{
				for(var i = 0;i < procedureInfo.params.length; i++)
				{
					procedureInfo.params[i] = procedureInfo.params[i].toLowerCase();
					if (procedureInfo.params[i] == 'string')
						procedureInfo.params[i] = 'varchar';
				}
				if (procedureInfo.name in Procedures)
				{
					if (Procedures[procedureInfo.name].length == procedureInfo.params.length)
					{
						var identical = true;
						for (var i = 0;i < procedureInfo.params.length; i++)
							if (procedureInfo.params[i] != Procedures[procedureInfo.name][i])
								identical = false;
						if (identical)
							return;
					}
				}
				delete Procedures[procedureInfo.name];
				Procedures[procedureInfo.name] = procedureInfo.params;
				MainUI.DeclareProcedure(this, procedureInfo);
			}
		}
		this.UndeclareProcedure = function(procedureName)
		{
			if (procedureName.indexOf('@') == -1)
			{
				delete Procedures[procedureName];
				MainUI.UndeclareProcedure(this, procedureName);
			}
		}
		return this;
	}

	LoadConnectionMetadata = function(connection, onconnectionready) {
		connection.Queue.Start()
			.BeginExecute('@Statistics', ['TABLE',0], function(data) { connection.Metadata['rawTables'] = data.results[0]; })
			.BeginExecute('@Statistics', ['INDEX',0], function(data) { connection.Metadata['rawIndexes'] = data.results[0]; })
			.BeginExecute('@Statistics', ['PARTITIONCOUNT',0], function(data) { connection.Metadata['partitionCount'] = data.results[0].data[0][0]; })
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
								item.indexes[indexName] = indexName + ' (' + (connection.Metadata['rawIndexes'].data[j][7].toLowerCase().indexOf('hash')?'Hash':'Tree') + (connection.Metadata['rawIndexes'].data[j][8] == "1"?', Unique':'') + ')';
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
												  , '@Statistics': ['Statitic (StatisticsComponent)', 'Interval (bit)', 'Returns Table[]' ]
												  , '@SystemInformation': ['Returns Table[]']
												  , '@UpdateApplicationCatalog': ['CatalogPath (varchar)', 'DeploymentConfigPath (varchar)', 'Returns Table[]']
												  , '@UpdateLogging': ['Configuration (xml)', 'Returns Table[]']
												  };
				connection.Queue.Start(true);
				for(var table in connection.Metadata['tables'])
					connection.Queue.BeginExecute('@AdHoc', 'SELECT TOP 1 * FROM ' + table, (new OnGetSchema(connection, 'tables', table)).Callback);
				for(var view in connection.Metadata['views'])
					connection.Queue.BeginExecute('@AdHoc', 'SELECT TOP 1 * FROM ' + view, (new OnGetSchema(connection, 'views', view)).Callback);
				connection.Queue.End(function(state) { connection.Ready = true; if (onconnectionready != null) onconnectionready(connection); }, null);
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
				var DBType = { '3': 'tinyint', '4': 'smallint', '5': 'int', '6': 'bigint', '8': 'float', '9': 'varchar', '11': 'timestamp', '22': 'decimal', '23': 'decimal' };
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
	this.DBType = { '3': 'tinyint', '4': 'smallint', '5': 'int', '6': 'bigint', '8': 'float', '9': 'varchar', '11': 'timestamp', '22': 'decimal', '23': 'decimal' };
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

