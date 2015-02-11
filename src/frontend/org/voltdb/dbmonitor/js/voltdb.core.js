

(function (window, unused) {

    var iVoltDbCore = (function () {
        this.connections = {};
        this.isServerConnected = true;
        this.hostIP = "";
        this.shortApiCredentials = "";
        this.isLoginVerified = false;

        this.authorization = null;
        DbConnection = function (aServer, aPort, aAdmin, aUser, aPassword, aIsHashPassword, aProcess) {
            this.server = aServer == null ? 'localhost' : $.trim(aServer);
            this.port = aPort == null ? '8080' : $.trim(aPort);
            this.admin = (aAdmin == true || aAdmin == "true");
            this.user = (aUser == '' || aUser == 'null') ? null : aUser;
            this.password = (aPassword === '' || aPassword === 'null') ? null : (aIsHashPassword == false ? aPassword : null);
            this.isHashedPassword = (aPassword === '' || aPassword === 'null') ? null : (aIsHashPassword == true ? aPassword : null);
            this.process = aProcess;
            this.key = (this.server + '_' + this.port + '_' + (this.user == '' ? '' : this.user) + '_' + (this.admin == true ? 'Admin' : '') + "_" + this.process).replace(/[^_a-zA-Z0-9]/g, "_");
            this.display = this.server + ':' + this.port + (this.user == '' ? '' : ' (' + this.user + ')') + (this.admin == true ? ' - Admin' : '');
            this.Metadata = {};
            this.ready = false;
            this.procedureCommands = {};
            this.authorization = VoltDBService.BuildAuthorization(this.user, this.isHashedPassword, this.password)

            this.getQueue = function () {
                return (new iQueue(this));
            };

            this.BuildParamSetForClusterState = function (procedure) {
                var credentials = [];
                credentials[credentials.length] = encodeURIComponent('Procedure') + '=' + encodeURIComponent(procedure);
                if (this.admin)
                    credentials[credentials.length] = 'admin=true';

                var param = credentials.join('&') + '&jsonp=?';
                return param;
            };

            this.BuildParamSet = function (procedure, parameters, shortApiCallDetails) {
                var s = [];
                if (!(shortApiCallDetails != null && shortApiCallDetails != null)) {
                    if (!this.procedures.hasOwnProperty(procedure)) {
                        return ['Procedure "' + procedure + '" is undefined.'];
                    }

                    var signatures = this.procedures[procedure];
                    var localParameters = [];
                    localParameters = localParameters.concat(parameters);

                    if (!(signatures['' + localParameters.length])) {
                        var retval = 'Invalid parameter count for procedure "' + procedure + '" (received: ' + localParameters.length + ', expected: ';
                        for (x in signatures) {
                            retval += x + ', ';
                        }
                        return [retval + ')'];
                    }
                    var signature = signatures['' + localParameters.length];

                    s[s.length] = encodeURIComponent('Procedure') + '=' + encodeURIComponent(procedure);
                    if (localParameters != null) {
                        var params = '[';
                        var i = 0;
                        for (i = 0; i < localParameters.length; i++) {
                            if (i > 0) {
                                params += ',';
                            }
                            switch (signature[i]) {
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
                                        params += '["' + localParameters[i].replace(/^'|'$/g, '') + '"]';
                                    else
                                        params += (typeof (localParameters[i]) == 'string'
                                            ? '"' + localParameters[i].replace(/^'|'$/g, '') + '"'
                                            : localParameters[i]).replace(/''/g, "'");
                            }
                        }
                        params += ']';
                        s[s.length] = encodeURIComponent('Parameters') + '=' + encodeURIComponent(params);
                    }
                }
                if (this.admin)
                    s[s.length] = 'admin=true';
                var paramSet = s.join('&') + '&jsonp=?';

                if (VoltDBCore.shortApiCredentials == "" && VoltDBCore.isLoginVerified) {
                    var credentials = [];
                    if (this.user != null)
                        credentials[credentials.length] = encodeURIComponent('User') + '=' + encodeURIComponent(this.user);
                    if (this.password != null)
                        credentials[credentials.length] = encodeURIComponent('Password') + '=' + encodeURIComponent(this.password);
                    if (this.isHashedPassword != null)
                        credentials[credentials.length] = encodeURIComponent('Hashedpassword') + '=' + encodeURIComponent(this.isHashedPassword);
                    if (this.admin)
                        credentials[credentials.length] = 'admin=true';

                    VoltDBCore.shortApiCredentials = credentials.join('&');
                }

                return paramSet;
            };

            this.CallExecute = function (procedure, parameters, callback, shortApiCallDetails) {
                var uri;
                if (shortApiCallDetails != null && shortApiCallDetails.isShortApiCall) {
                    if (shortApiCallDetails.apiPath == null || shortApiCallDetails.apiPath == "") {
                        callback({ "status": -1, "statusstring": "Error: Please specify apiPath.", "results": [] });
                    }

                    uri = 'http://' + this.server + ':' + this.port + '/' + shortApiCallDetails.apiPath + '/';
                } else {
                    uri = 'http://' + this.server + ':' + this.port + '/api/1.0/';
                }
                var params = '';
                if (procedure == '@Pause' || procedure == '@Resume' || procedure == '@Shutdown' || procedure == '@Promote') {
                    params = this.BuildParamSetForClusterState(procedure);
                } else {
                    params = this.BuildParamSet(procedure, parameters, shortApiCallDetails);
                }
                if (typeof (params) == 'string') {
                    if (VoltDBCore.isServerConnected) {
                        var ah = null;
                        if (this.authorization != null) {
                            ah = this.authorization;
                        } else {
                            VoltDBService.BuildAuthorization(this.user, this.isHashedPassword, this.password);
                        }
                        jQuery.getJSON(uri, params, callback, ah);
                    }
                } else if (callback != null)
                    callback({ "status": -1, "statusstring": "PrepareStatement error: " + params[0], "results": [] });
            };

            this.CallExecuteUpdate = function (procedure, parameters, callback, shortApiCallDetails) {
                var uri;
                if (shortApiCallDetails != null && shortApiCallDetails.isShortApiCall) {
                    if (shortApiCallDetails.apiPath == null || shortApiCallDetails.apiPath == "") {
                        callback({ "status": -1, "statusstring": "Error: Please specify apiPath.", "results": [] });
                    }

                    if (shortApiCallDetails.updatedData == null) {
                        callback({ "status": -1, "statusstring": "Error: Please specify parameters", "results": [] });
                    }

                    uri = 'http://' + this.server + ':' + this.port + '/' + shortApiCallDetails.apiPath + '/?admin=true';

                    if (VoltDBCore.isServerConnected) {
                        var ah = null;
                        if (this.authorization != null) {
                            ah = this.authorization;
                        } else {
                            VoltDBService.BuildAuthorization(this.user, this.isHashedPassword, this.password);
                        }
                        jQuery.postJSON(uri, shortApiCallDetails.updatedData, callback, ah);
                    }
                } else {
                    uri = 'http://' + this.server + ':' + this.port + '/api/1.0/';

                    var params = this.BuildParamSet(procedure, parameters, shortApiCallDetails);
                    if (typeof (params) == 'string') {
                        if (VoltDBCore.isServerConnected) {
                            var ah = null;
                            if (this.authorization != null) {
                                ah = this.authorization;
                            } else {
                                VoltDBService.BuildAuthorization(this.user, this.isHashedPassword, this.password);
                            }
                            jQuery.postJSON(uri, params, callback, ah);
                        }
                    } else if (callback != null)
                        callback({ "status": -1, "statusstring": "PrepareStatement error: " + params[0], "results": [] });
                }
            };

            var callbackWrapper = function (userCallback, isHighTimeout) {
                var criticalErrorResponse = { "status": -1, "statusstring": "Query timeout.", "results": [] };
                var UserCallback = userCallback;
                var timeoutOccurred = 0;
                var timeout = setTimeout(function () {
                    timeoutOccurred = 1;
                    UserCallback(criticalErrorResponse);
                }, !isHighTimeout ? 20000 : 6000000);
                this.Callback = function (response, headerInfo) {
                    clearTimeout(timeout);
                    if (timeoutOccurred == 0) UserCallback(response, headerInfo);
                };
                return this;
            };

            this.BeginExecute = function (procedure, parameters, callback, shortApiCallDetails) {
                var isHighTimeout = (procedure == "@SnapshotRestore" || procedure == "@AdHoc");
                this.CallExecute(procedure, parameters, (new callbackWrapper(callback, isHighTimeout)).Callback, shortApiCallDetails);
            };

            var iQueue = function (connection) {
                var continueOnFailure = false;
                var executing = false;
                var success = false;
                var stack = [];
                var onCompleteHandler = null;
                var Connection = connection;
                this.Start = function (continueOnFailure) {
                    if (executing)
                        return null;
                    continueOnFailure = (continueOnFailure == true);
                    onCompleteHandler = null;
                    success = true;
                    stack.push(null);
                    return this;
                };

                this.BeginExecute = function (procedure, parameters, callback, shortApiCallDetails) {
                    stack.push([procedure, parameters, callback, shortApiCallDetails]);
                    return this;
                };
                this.EndExecute = function () {
                    if (stack.length > 0)
                        stack.splice(0, 1);
                    if (stack.length > 0 && (success || continueOnFailure)) {
                        var item = stack[0];
                        var shortApiCallDetails = item[3];
                        var isHighTimeout = (item[0] == "@SnapshotRestore" || item[0] == "@AdHoc");
                        var callback =
                        (new callbackWrapper(
                            (function (queue, item) {
                                return function (response, headerInfo) {
                                    try {

                                        if (VoltDBCore.hostIP == "") {
                                            VoltDBCore.hostIP = headerInfo;
                                        }

                                        if (response.status != 1)
                                            success = false;
                                        if (item[2] != null)
                                            item[2](response);

                                        queue.EndExecute();
                                    } catch (x) {
                                        success = false;
                                        queue.EndExecute();
                                    }
                                };
                            })(this, item), isHighTimeout)).Callback;

                        if (shortApiCallDetails != null && shortApiCallDetails.isShortApiCall && shortApiCallDetails.isUpdateConfiguration)
                            Connection.CallExecuteUpdate(item[0], item[1], callback, item[3]);
                        else
                            Connection.CallExecute(item[0], item[1], callback, item[3]);

                    } else {
                        executing = false;
                        if (onCompleteHandler != null) {
                            try {
                                onCompleteHandler[0](onCompleteHandler[1], success);
                            } catch (x) {
                                console.log(x.message);
                            }
                        }
                    }
                    return this;
                };
                this.End = function (fcn, state) {
                    onCompleteHandler = [fcn, state];
                    if (!executing) {
                        executing = true;
                        this.EndExecute();
                    }
                };

            };
            this.procedures = {
                '@AdHoc': { '1': ['varchar'] },
                '@Explain': { '1': ['varchar'] },
                '@ExplainProc': { '1': ['varchar'] },
                '@Pause': { '0': [] },
                '@Promote': { '0': [] },
                '@Quiesce': { '0': [] },
                '@Resume': { '0': [] },
                '@Shutdown': { '0': [] },
                '@SnapshotDelete': { '2': ['varchar', 'varchar'] },
                '@SnapshotRestore': { '1': ['varchar'], '2': ['varchar', 'varchar'] },
                '@SnapshotSave': { '3': ['varchar', 'varchar', 'bit'], '1': ['varchar'] },
                '@SnapshotScan': { '1': ['varchar'] },
                '@SnapshotStatus': { '0': [] },
                '@Statistics': { '2': ['StatisticsComponent', 'bit'] },
                '@SystemCatalog': { '1': ['CatalogComponent'] },
                '@SystemInformation': { '1': ['SysInfoSelector'] },
                '@UpdateApplicationCatalog': { '2': ['varchar', 'varchar'] },
                '@UpdateLogging': { '1': ['xml'] },
                '@ValidatePartitioning': { '2': ['int', 'varbinary'] },
                '@GetPartitionKeys': { '1': ['varchar'] },
                '@GC': { '0': [] },
                '@StopNode': { '1': ['int'] }
            };
            return this;
        };

        this.BuildParamSet = function (procedure, parameters, shortApiCallDetails) {
            var s = [];

            if (!(shortApiCallDetails != null && shortApiCallDetails != null)) {
                if (!procedures.hasOwnProperty(procedure)) {
                    return ['Procedure "' + procedure + '" is undefined.'];
                }

                var signatures = procedures[procedure];
                var localParameters = [];
                localParameters = localParameters.concat(parameters);

                if (!(signatures['' + localParameters.length])) {
                    var retval = 'Invalid parameter count for procedure "' + procedure + '" (received: ' + localParameters.length + ', expected: ';
                    for (x in signatures) {
                        retval += x + ', ';
                    }
                    return [retval + ')'];
                }
                var signature = signatures['' + localParameters.length];

                s[s.length] = encodeURIComponent('Procedure') + '=' + encodeURIComponent(procedure);
                if (localParameters != null) {
                    var params = '[';
                    var i = 0;
                    for (i = 0; i < localParameters.length; i++) {
                        if (i > 0) {
                            params += ',';
                        }
                        switch (signature[i]) {
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
                                    params += '["' + localParameters[i].replace(/^'|'$/g, '') + '"]';
                                else
                                    params += (typeof (localParameters[i]) == 'string'
                                        ? '"' + localParameters[i].replace(/^'|'$/g, '') + '"'
                                        : localParameters[i]).replace(/''/g, "'");
                        }
                    }
                    params += ']';
                    s[s.length] = encodeURIComponent('Parameters') + '=' + encodeURIComponent(params);
                }
            }
            if (this.User != null)
                s[s.length] = encodeURIComponent('User') + '=' + encodeURIComponent(this.User);
            if (this.Password != null)
                s[s.length] = encodeURIComponent('Password') + '=' + encodeURIComponent(this.Password);
            if (this.HashedPassword != null)
                s[s.length] = encodeURIComponent('Hashedpassword') + '=' + encodeURIComponent(this.HashedPassword);
            if (this.Admin)
                s[s.length] = 'admin=true';
            var paramSet = s.join('&') + '&jsonp=?';
            return paramSet;
        };

        this.TestConnection = function (server, port, admin, user, password, isHashedPassword, processName, onConnectionTested, isLoginTest) {

            var callback = function (result, response, loginTest) {
                if (loginTest == true) {
                    onConnectionTested(result, response);
                } else {
                    onConnectionTested(result);
                }
            };

            var callbackTimeout = isLoginTest ? 10000 : 5000;
            var conn = new DbConnection(server, port, admin, user, password, isHashedPassword, processName);
            var timeout = setTimeout(function () {
                callback(false, { "status": -100, "statusstring": "Server is not available." }, isLoginTest);
            }, callbackTimeout);

            conn.BeginExecute('@Statistics', ['TABLE', 0], function (response) {
                try {
                    clearTimeout(timeout);
                    if (response.status == 1) {
                        VoltDBCore.isLoginVerified = true;
                        callback(true, response, isLoginTest);
                    } else {
                        callback(false, response, isLoginTest);
                    }
                } catch (x) {
                    clearTimeout(timeout);
                    callback(true, response, isLoginTest);
                }
            });
        };

        this.CheckServerConnection = function (server, port, admin, user, password, isHashedPassword, processName, checkConnection) {
            var conn = new DbConnection(server, port, admin, user, password, isHashedPassword, processName);
            var uri = 'http://' + server + ':' + port + '/api/1.0/';
            var params = conn.BuildParamSet('@Statistics', ['TABLE', 0]);
            $.ajax({
                url: uri + '?' + params,
                dataType: "jsonp",
                beforeSend: function (request) {
                    if (conn.authorization != null) {
                        request.setRequestHeader("Authorization", conn.authorization);
                    }
                },
                success: function (e) {
                    if (e.status === 200) {
                        checkConnection(true);
                    }
                },
                complete: function (e) {
                    if (e.status === 200) {
                        checkConnection(true);
                    }
                },
                error: function (e) {
                    if (e.status != 200) {
                        checkConnection(false);
                    }
                },
                timeout: 60000
            });

        };

        this.AddConnection = function (server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, onConnectionAdded, shortApiCallDetails) {
            var conn = new DbConnection(server, port, admin, user, password, isHashedPassword, processName);
            compileProcedureCommands(conn, procedureNames, parameters, values);
            this.connections[conn.key] = conn;
            loadConnectionMetadata(this.connections[conn.key], onConnectionAdded, processName, shortApiCallDetails);

        };

        this.updateConnection = function (server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, connection, onConnectionAdded, shortApiCallDetails) {
            compileProcedureCommands(connection, procedureNames, parameters, values);
            loadConnectionMetadata(connection, onConnectionAdded, processName, shortApiCallDetails);
        };

        this.HasConnection = function (server, port, admin, user, processName) {
            var serverName = server == null ? 'localhost' : $.trim(server);
            var portId = port == null ? '8080' : $.trim(port);
            var userName = user == '' ? null : user;
            var key = (serverName + '_' + portId + '_' + (userName == '' ? '' : userName) + '_' +
                (admin == true ? 'Admin' : '') + "_" + processName).replace(/[^_a-zA-Z0-9]/g, "_");

            if (this.connections[key] != undefined) {
                var conn = this.connections[key];
                if (conn.key in this.connections) {
                    return conn;
                }
            }
            return null;
        };

        var loadConnectionMetadata = function (connection, onConnectionAdded, processName, shortApiCallDetails) {
            var i = 0;
            var connectionQueue = connection.getQueue();
            connectionQueue.Start();

            if (shortApiCallDetails != null && shortApiCallDetails.isShortApiCall) {
                connectionQueue.BeginExecute([], [], function (data) {                                      
                    connection.Metadata[processName] = data;
                    
                }, shortApiCallDetails);
            } else {
                jQuery.each(connection.procedureCommands.procedures, function (id, procedure) {
                    connectionQueue.BeginExecute(procedure['procedure'], (procedure['value'] === undefined ? procedure['parameter'] : [procedure['parameter'], procedure['value']]), function (data) {
                        var suffix = (processName == "GRAPH_MEMORY" || processName == "GRAPH_TRANSACTION") || processName == "TABLE_INFORMATION" || processName == "CLUSTER_INFORMATION" ? "_" + processName : "";
                        if (processName == "SYSTEMINFORMATION_STOPSERVER") {
                            connection.Metadata[procedure['procedure'] + "_" + procedure['parameter'] + suffix + "_status"] = data.status;
                            connection.Metadata[procedure['procedure'] + "_" + procedure['parameter'] + suffix + "_statusString"] = data.statusstring;
                        }
                        else if (processName == "SYSTEMINFORMATION_PAUSECLUSTER" || processName == "SYSTEMINFORMATION_RESUMECLUSTER" || processName == "SYSTEMINFORMATION_SHUTDOWNCLUSTER") {
                            connection.Metadata[procedure['procedure'] + "_" + "status"] = data.status;
                        }
                        else if (processName == "SYSTEMINFORMATION_SAVESNAPSHOT" || processName == "SYSTEMINFORMATION_RESTORESNAPSHOT") {
                            connection.Metadata[procedure['procedure'] + "_" + "status"] = data.status;
                            connection.Metadata[procedure['procedure'] + "_data"] = data.results[0];
                            connection.Metadata[procedure['procedure'] + "_statusstring"] = data.statusstring;
                        }
                        else if (processName == "SYSTEMINFORMATION_SCANSNAPSHOTS") {
                            connection.Metadata[procedure['procedure'] + "_" + "status"] = data.status;
                            connection.Metadata[procedure['procedure'] + "_data"] = data.results;
                            connection.Metadata[procedure['procedure'] + "_statusstring"] = data.statusstring;
                        }
                        else if (processName == "SYSTEMINFORMATION_PROMOTECLUSTER") {
                            connection.Metadata[procedure['procedure'] + "_" + "status"] = data.status;
                            connection.Metadata[procedure['procedure'] + "_statusstring"] = data.statusstring;
                        }
                        else
                            connection.Metadata[procedure['procedure'] + "_" + procedure['parameter'] + suffix] = data.results[0];
                    });
                });
            }

            connectionQueue.End(function (state) {
                connection.Metadata['sysprocs'] = {
                    '@Explain': { '1': ['SQL (varchar)', 'Returns Table[]'] },
                    '@ExplainProc': { '1': ['Stored Procedure Name (varchar)', 'Returns Table[]'] },
                    '@Pause': { '0': ['Returns bit'] },
                    '@Quiesce': { '0': ['Returns bit'] },
                    '@Resume': { '0': ['Returns bit'] },
                    '@Shutdown': { '0': ['Returns bit'] },
                    '@SnapshotDelete': { '2': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]'] },
                    '@SnapshotRestore': { '2': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Returns Table[]'], '1': ['JSON (varchar)', 'Returns Table[]'] },
                    '@SnapshotSave': { '3': ['DirectoryPath (varchar)', 'UniqueId (varchar)', 'Blocking (bit)', 'Returns Table[]'], '1': ['JSON (varchar)', 'Returns Table[]'] },
                    '@SnapshotScan': { '1': ['DirectoryPath (varchar)', 'Returns Table[]'] },
                    '@SnapshotStatus': { '0': ['Returns Table[]'] },
                    '@Statistics': { '2': ['Statistic (StatisticsComponent)', 'Interval (bit)', 'Returns Table[]'] },
                    '@SystemCatalog': { '1': ['SystemCatalog (CatalogComponent)', 'Returns Table[]'] },
                    '@SystemInformation': { '1': ['Selector (SysInfoSelector)', 'Returns Table[]'] },
                    '@UpdateApplicationCatalog': { '2': ['CatalogPath (varchar)', 'DeploymentConfigPath (varchar)', 'Returns Table[]'] },
                    '@UpdateLogging': { '1': ['Configuration (xml)', 'Returns Table[]'] },
                    '@Promote': { '0': ['Returns bit'] },
                    '@ValidatePartitioning': { '2': ['HashinatorType (int)', 'Config (varbinary)', 'Returns Table[]'] },
                    '@GetPartitionKeys': { '1': ['VoltType (varchar)', 'Returns Table[]'] }
                };

                var childConnectionQueue = connection.getQueue();
                childConnectionQueue.Start(true);
                childConnectionQueue.End(function (state) {
                    connection.Ready = true;
                    if (onConnectionAdded != null)
                        onConnectionAdded(connection, state);
                }, null);
            }, null);
        };

        var compileProcedureCommands = function (connection, procedureNames, parameters, values) {
            var i = 0;
            var lConnection = connection;
            lConnection.procedureCommands["procedures"] = {};
            for (i = 0; i < procedureNames.length; i++) {
                lConnection.procedureCommands["procedures"][i] = {};
                lConnection.procedureCommands["procedures"][i]["procedure"] = procedureNames[i];
                if (procedureNames[i] == '@SnapshotSave')
                    lConnection.procedureCommands["procedures"][i]["parameter"] = [parameters[i], parameters[i + 1], parameters[i + 2]];
                else if (procedureNames[i] == '@SnapshotRestore') {
                    lConnection.procedureCommands["procedures"][i]["parameter"] = [parameters[i], parameters[i + 1]];
                } else
                    lConnection.procedureCommands["procedures"][i]["parameter"] = parameters[i];
                lConnection.procedureCommands["procedures"][i]["value"] = values[i];
            }

        };
        return this;
    });
    window.VoltDBCore = VoltDBCore = new iVoltDbCore();

})(window);

jQuery.extend({
    postJSON: function (url, formData, callback, authorization) {
        
        if (VoltDBCore.hostIP == "") {

            jQuery.ajax({
                type: 'POST',
                url: url,
                data: formData,
                dataType: 'json',
                beforeSend: function (request) {
                    if (authorization != null) {
                        request.setRequestHeader("Authorization", authorization);
                    }
                },
                success: function (data, textStatus, request) {
                    var host = request.getResponseHeader("Host") != null ? request.getResponseHeader("Host").split(":")[0] : "-1";
                    callback(data, host);
                },
                error: function (e) {
                    console.log(e);
                }
            });

        } else {
            jQuery.ajax({
                type: 'POST',
                url: url,
                data: formData,
                dataType: 'json',
                beforeSend: function (request) {
                    if (authorization != null) {
                        request.setRequestHeader("Authorization", authorization);
                    }
                },
                success: callback,
                error: function (e) {
                    console.log(e.message);
                }
            });
        }
    }
});

jQuery.extend({
    getJSON: function (url, formData, callback, authorization) {
        
        if (VoltDBCore.hostIP == "") {
            jQuery.ajax({
                type: 'GET',
                url: url,
                data: formData,
                dataType: 'jsonp',
                beforeSend: function (request) {
                    if (authorization != null) {
                        request.setRequestHeader("Authorization", authorization);
                    }
                },
                success: function (data, textStatus, request) {
                    var host = request.getResponseHeader("Host") != null ? request.getResponseHeader("Host").split(":")[0] : "-1";
                    callback(data, host);
                },
                error: function (e) {
                    console.log(e.message);
                }
            });

        } else {
            jQuery.ajax({
                type: 'GET',
                url: url,
                data: formData,
                dataType: 'jsonp',
                beforeSend: function (request) {
                    if (authorization != null) {
                        request.setRequestHeader("Authorization", authorization);
                    }
                },
                success: callback,
                error: function (e) {
                    console.log(e.message);
                }
            });
        }
    }

});

