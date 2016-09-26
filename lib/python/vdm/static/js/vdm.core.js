(function (window, unused) {

    var iVdmCore = (function () {
        this.connections = {};
        this.isServerConnected = true;
        this.hostIP = "";
        this.shortApiCredentials = "";
        this.isLoginVerified = false;

        this.authorization = null;
        DbConnection = function (aServer, aPort, aAdmin, aUser, aPassword, aIsHashPassword, aProcess) {
            this.server = aServer == null ? 'localhost' : $.trim(aServer);
            this.port = aPort == null ? '8000' : $.trim(aPort);
            this.admin = (aAdmin == true || aAdmin == "true");
            this.user = (aUser == '' || aUser == 'null') ? null : aUser;
            this.password = (aPassword === '' || aPassword === 'null') ? null : (aIsHashPassword == false ? aPassword : null);
            this.isHashedPassword = (aPassword === '' || aPassword === 'null') ? null : (aIsHashPassword == true ? aPassword : null);
            this.process = aProcess;
            this.key = (this.server + '_' + (this.user == '' ? '' : this.user) + '_' + this.process).replace(/[^_a-zA-Z0-9]/g, "_");
            this.display = this.server + ':' + this.port + (this.user == '' ? '' : ' (' + this.user + ')') + (this.admin == true ? ' - Admin' : '');
            this.Metadata = {};
            this.ready = false;
            this.procedureCommands = {};
            this.authorization = VdmService.BuildAuthorization(this.user, this.isHashedPassword, this.password);

            this.getQueue = function () {
                return (new iQueue(this));
            };

            this.BuildParamSet = function (requestMethod, details) {
                param = ''
                var apiName = details.apiName;
                switch(apiName) {
                    case 'SERVER':
                        param = 'databases/'
                        break;
                    case 'SERVER_ADD':
                        param = 'databases/'
                        break;
                    case 'DATABASE':
                        param = 'databases/'
                        break;
                    case 'MEMBER':
                        param = 'databases/'
                        break;
                    case 'DEPLOYMENT':
                        param = 'databases/'
                        break;
                    case 'DEPLOYMENTUSER':
                        param = 'databases/'
                        break;
                    case 'VDM_STATUS':
                        param = 'voltdeploy/status/'
                        break;
                    case 'VDM_SYNC':
                        param = 'voltdeploy/sync_configuration/'
                        break;
                    case 'SERVER_IP':
                        param = 'ServerIP/'
                        break;
                    case 'START_CLUSTER':
                        param = 'databases/'
                        break;
                    case 'STOP_CLUSTER':
                        param = 'databases/'
                        break;
                    case 'KILL_CLUSTER':
                        param = 'databases/'
                        break;
                    case 'RECOVER_CLUSTER':
                        param = 'databases/'
                        break;
                    case 'UPLOAD_CONFIG':
                        param = 'databases/'
                        break;
                    case 'DATABASE_STATUS':
                        param = 'databases/'
                        break;
                    case 'DATABASE_SERVER_STATUS':
                        param = 'databases/'
                        break;
                    case 'STOP_SERVER':
                        param = 'databases/'
                        break;
                    case 'START_SERVER':
                        param = 'databases/'
                        break;
                    case 'ADD_SERVER':
                        param = 'databases/'
                        break;
                    case 'SERVER_STATUS':
                        param = 'databases/'
                        break;
                    case 'GET_STATISTIC_INFORMATION':
                        param = '?Procedure=@Statistics&Parameters=[ksafety,0]'
                        break;
                    case 'GET_SYSTEM_INFORMATION':
                        param = '?Procedure=@SystemInformation&Parameters=[deployment]'
                        break;
                    default:
                        param = 'servers/'
                }

                if (requestMethod.toLowerCase() == "delete" || requestMethod.toLowerCase() == "put"
                    || apiName == 'MEMBER' || (requestMethod.toLowerCase() == "get" && apiName == 'SERVER') ||(requestMethod.toLowerCase() == "post" && apiName == 'SERVER_ADD') || apiName == 'DEPLOYMENT' || apiName == 'DEPLOYMENTUSER' || apiName == 'DATABASE_STATUS' || apiName == 'SERVER_STATUS' || apiName == 'DATABASE_SERVER_STATUS')
                    if(details.dataObj != undefined && details.dataObj.id != undefined)
                        param += details.dataObj.id.toString();
                if (apiName == 'START_CLUSTER'){
                    if (details.dataObj.pause == true)
                    {
                        param += '/start?pause=true'
                    }
                    else
                    {
                        param += '/start?pause=false'
                    }

                    if (details.dataObj.force == true){
                        param += '&force=true'
                    }else{
                        param += '&force=false'
                    }
                }
                if (apiName == 'RECOVER_CLUSTER'){
                    if (details.dataObj.pause == true)
                    {
                        param += '/recover?pause=true'
                    }
                    else
                    {
                        param += '/recover?pause=false'
                    }
                }
                else if (apiName == 'STOP_CLUSTER')
                    param += '/stop?force=true'
                else if (apiName == 'KILL_CLUSTER')
                    param += '/stop?force=false'
                else if (apiName == 'DATABASE_STATUS')
                    param += '/status'
                else if (apiName == 'DATABASE_SERVER_STATUS' || apiName == 'SERVER_STATUS')
                    param += '/servers/' + details.dataObj.server_id.toString() + '/status'
                else if (apiName == 'MEMBER' || apiName == 'SERVER_ADD')
                    param += '/servers/'
                else if (apiName == 'SERVER')
                    param += '/servers/' + details.dataObj.server_id.toString() + '/';
                else if (apiName == 'DEPLOYMENT')
                    param += '/deployment/'
                else if (apiName == 'STOP_SERVER')
                    param += '/servers/' + details.dataObj.server_id.toString() + '/stop?force=false'
                else if (apiName == 'START_SERVER')
                    param += '/servers/' + parseInt(details.dataObj.server_id) + '/start'
                else if (apiName == 'ADD_SERVER')
                    param += '/servers/' + parseInt(details.dataObj.server_id) + '/add'
                if(apiName == 'UPLOAD_CONFIG')
                    param += '/deployment/'


                if ((apiName == "DEPLOYMENTUSER" && requestMethod.toLowerCase()=="get") || (apiName == "DEPLOYMENTUSER" && requestMethod.toLowerCase()=="post"))
                    param += '/users/';
                if ((apiName == "DEPLOYMENTUSER" && requestMethod.toLowerCase()== "put")|| (apiName == "DEPLOYMENTUSER" && requestMethod.toLowerCase()=="delete"))
                    param += '/users/' + details.requestUser;
                return param;
            };

            this.CallExecute = function (callback, requestMethod, details) {
                var uri;
                if(details.apiName == 'GET_SYSTEM_INFORMATION' || details.apiName == 'GET_STATISTIC_INFORMATION'){
                    uri = 'http://' + details.dataObj.serverIp + ':' + '8080'  + '/api/1.0/'
                } else if(details.apiName != 'VDM_STATUS' && details.apiName != 'VDM_SYNC'
                    && details.apiName != 'SERVER_STATUS' && details.apiName != 'STOP_SERVER'){
                    uri = 'http://' + this.server + ':' + this.port + '/api/1.0/';
                } else {
                    uri = 'http://' + details.dataObj.serverIp + ':' + this.port + '/api/1.0/';
                }

                var params = '';
                params = this.BuildParamSet(requestMethod, details);
                uri = uri + params;

                if (typeof (params) == 'string') {
                   if (requestMethod.toLowerCase() == "get") {
                        jQuery.getJSON(uri, callback);
                    } else if (requestMethod.toLowerCase() == "put") {
                        jQuery.putJSON(uri, details.dataObj.data, callback, details.apiName);
                    } else if (requestMethod.toLowerCase() == "delete") {
                        jQuery.deleteJSON(uri, callback);
                    } else if (requestMethod.toLowerCase() == "post") {
                        jQuery.postJSON(uri, details.dataObj.data, callback);
                    }
                } else if (callback != null)
                    callback({ "status": -1, "statusstring": "PrepareStatement error: " + params[0], "results": [] });
            };

            var callbackWrapper = function (userCallback, isHighTimeout) {
                var criticalErrorResponse = { "status": -1, "statusstring": "Query timeout.", "results": [] };
                var UserCallback = userCallback;
                var timeoutOccurred = 0;
                var timeout = setTimeout(function () {
                    timeoutOccurred = 1;
                    UserCallback(criticalErrorResponse);
                }, !isHighTimeout ? 20000 : 3600000);
                this.Callback = function (response, headerInfo) {
                    clearTimeout(timeout);
                    if (timeoutOccurred == 0) UserCallback(response, headerInfo);
                };
                return this;
            };

            this.BeginExecute = function (callback, requestMethod,serverDetails, isLongOutput) {
                if (isLongOutput== undefined){
                    isLongOutput = false;
                }
                var isHighTimeout = (isLongOutput === true);
                this.CallExecute((new callbackWrapper(callback, isHighTimeout)).Callback, requestMethod),serverDetails;
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
                this.BeginExecute = function (callback, requestMethod, serverDetails, isLongTimeout) {
                    stack.push([callback, requestMethod, serverDetails,isLongTimeout]);
                    return this;
                };
                this.EndExecute = function () {
                    if (stack.length > 0)
                        stack.splice(0, 1);
                    if (stack.length > 0 && (success || continueOnFailure)) {
                        var item = stack[0];
                        var shortApiCallDetails = item[3];
                        var isHighTimeout = item[3];
                        var callback =
                        (new callbackWrapper(
                            (function (queue, item) {
                                return function (response, headerInfo) {
                                    try {
                                        if ($.type(response) == "string") {
                                            response = json_parse(response, function (key, value) {
                                                return value;
                                            });
                                        }
                                        if (VdmCore.hostIP == "") {
                                            VdmCore.hostIP = headerInfo;
                                        }

                                        if (response.status != 1)
                                            success = false;
                                        if (item[0] != null)
                                            item[0](response);

                                        queue.EndExecute();
                                    } catch (x) {
                                        success = false;
                                        queue.EndExecute();
                                    }
                                };
                            })(this, item), isHighTimeout)).Callback;

                            Connection.CallExecute(callback, item[1], item[2]);
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
            return this;
        };

        this.AddConnection = function (server, port, admin, user, password, isHashedPassword, processName, onConnectionAdded, requestMethod, serverDetails, isLongTimeout) {
            var conn = new DbConnection(server, port, admin, user, password, isHashedPassword, processName);
            this.connections[conn.key] = conn;
            loadConnectionMetadata(this.connections[conn.key], onConnectionAdded, processName, requestMethod, serverDetails, isLongTimeout);
        };

        this.updateConnection = function (server, port, admin, user, password, isHashedPassword, processName, connection, onConnectionAdded, requestMethod, serverDetails, isLongTimeout) {
            loadConnectionMetadata(connection, onConnectionAdded, processName, requestMethod, serverDetails, isLongTimeout);
        };

        this.HasConnection = function (server, port, admin, user, processName) {
            var serverName = server == null ? 'localhost' : $.trim(server);
            var portId = port == null ? '8000' : $.trim(port);
            var userName = user == '' ? null : user;
            var key = (serverName + '_' + (userName == '' ? '' : userName) + '_' + processName).replace(/[^_a-zA-Z0-9]/g, "_");

            if (this.connections[key] != undefined) {
                var conn = this.connections[key];
                if (conn.key in this.connections) {
                    return conn;
                }
            }
            return null;
        };

        var loadConnectionMetadata = function (connection, onConnectionAdded, processName, requestMethod, serverDetails, isLongTimeout) {
            var i = 0;
            var connectionQueue = connection.getQueue();
            connectionQueue.Start();

            if (requestMethod != undefined ) {
                connectionQueue.BeginExecute(function (data) {
                    connection.Metadata[processName] = data;
                }, requestMethod, serverDetails, isLongTimeout);
            }

            connectionQueue.End(function (state) {
                var childConnectionQueue = connection.getQueue();
                childConnectionQueue.Start(true);
                childConnectionQueue.End(function (state) {
                    connection.Ready = true;
                    if (onConnectionAdded != null)
                        onConnectionAdded(connection, state);
                }, null);
            }, null);
        };

        return this;
    });
    window.VdmCore = VdmCore = new iVdmCore();

})(window);

jQuery.extend({
    postJSON: function (url, formData, callback) {
       jQuery.ajax({
             type: 'POST',
             url: url,
             contentType: "application/json; charset=utf-8",
             data: JSON.stringify(formData),
             dataType: 'json',
             success: callback,
             error: function (e) {
                 console.log(e.message);
             }
         });
    }
});

jQuery.extend({

    getJSON: function (url,  callback) {
        if(url.indexOf('voltdeploy/status') > -1 || url.indexOf('@SystemInformation') > -1
        || url.indexOf('@Statistics') > -1){
            if (url.indexOf('voltdeploy/status') > -1)
                url = url + '?jsonp=?';
            else
                url = url + '&jsonp=?';
            jQuery.ajax({
                type: 'GET',

                url: url,
                contentType: "application/json; charset=utf-8",
                dataType: 'jsonp',
                success: callback,
                error: function (e) {
                    console.log(e.message);
                }
            });
        } else {
            jQuery.ajax({
                type: 'GET',
                url: url,
                dataType: 'json',
                cache: false,
                success: callback,
                error: function (e) {
                    console.log(e.message);
                }
            });
        }

    }
});

jQuery.extend({
    putJSON: function (url, formData, callback, apiName) {
        if (apiName != 'UPLOAD_CONFIG'){
            if(!$.isEmptyObject(formData)){
                jQuery.ajax({
                    type: 'PUT',
                    url: url,
                    contentType: "application/json; charset=utf-8",
                    data: JSON.stringify(formData),
                    dataType: 'json',
                    success: callback,
                    error: function (e) {
                        response = $.parseJSON(e.responseText);
                        response.status = e.status;
                        callback(response, 'failure');
                    }
                });
            } else {
                jQuery.ajax({
                    type: 'PUT',
                    url: url,
                    contentType: "application/json; charset=utf-8",
                    dataType: 'json',
                    success: callback,
                    error: function (e) {
                        response = $.parseJSON(e.responseText);
                        response.status = e.status;
                        callback(response, 'failure');
                    }
                });
            }
        } else {
            jQuery.ajax({
                type: 'PUT',
                url: url,
                data: formData.deploy_file,
                contentType: false,
                cache: false,
                processData: false,
                success: callback,
                error: function(e){
                    response = $.parseJSON(e.responseText);
                    response.status = e.status;
                    callback(response, 'failure');
                }
            });
        }
    }
});

jQuery.extend({
    deleteJSON: function (url, callback) {
        jQuery.ajax({
            type: 'DELETE',
            url: url,
            contentType: "application/json; charset=utf-8",
            dataType: 'json',
            success: function(data, textStatus, xhr) {
                if(xhr.status == 204){
                    data = {'result': true}
                    textStatus = 'success'
                }
                callback(data, textStatus)
            },
            error: function (e) {
                response = $.parseJSON(e.responseText)
                response.result = false
                callback(response, 'failure');
            }
        });
    }
});