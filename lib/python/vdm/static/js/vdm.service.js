(function (window) {
    var procedures = {};
    var tips = $(".validateTips");
    var server = VdmConfig.GetDefaultServerIP();
    var port = VdmConfig.GetPortId();
    var user = "";
    var password = "";
    var admin = true;
    var isHashedPassword = true;
    this.connection = null;
    var iVdmService = (function () {
        var _connection = connection;

        this.SetUserCredentials = function (lUsername, lPassword, lAdmin) {
            user = lUsername;
            password = lPassword;
            admin = lAdmin;
        };

        // build Authorization header based on scheme you could flip to diff header. Server understands both.
        this.BuildAuthorization = function(user, isHashedPassword, password) {
            var authz = null;
            if (user != null && isHashedPassword != null) {
                authz = "Hashed " + user + ":" + isHashedPassword;
            } else if (user != null && password != null) {
                var up = user + ":" + password;
                authz = "Basic " + CryptoJS.SHA256({ method: "b64enc", source: up });
            }
            return authz;
        };

        this.ChangeServerConfiguration = function (serverName, portId, userName, pw, isHashPw, isAdmin) {
            server = serverName != null ? serverName : server;
            port = portId != null ? portId : port;
            user = userName != undefined ? userName : "";
            password = pw != undefined ? pw : "";
            isHashedPassword = isHashPw;
            admin = isAdmin != undefined ? isAdmin : true;

        };

        this.CreateServer = function (onConnectionAdded, serverData) {
            try {
                var processName = "SERVER_CREATE";
                var requestMethod = "POST"
                var serverDetails = {
                    dataObj: serverData,
                    apiName: "SERVER_ADD"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.DeleteServer = function (onConnectionAdded,serverData){
            try {
                var processName = "SERVER_DELETE";
                var requestMethod = "DELETE"
                var serverDetails = {
                    dataObj: serverData,
                    apiName: "SERVER"
                };
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.UpdateServer = function (onConnectionAdded, serverInfo) {
            try {
                var processName = "SERVER_UPDATE";
                var requestMethod = "PUT"
                var serverDetails = {
                    dataObj: serverInfo,
                    apiName: "SERVER"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetServer = function(onConnectionAdded, serverInfo){
            try {
                var processName = "GET_SERVER";
                var requestMethod = "GET"
                var serverDetails = {
                    dataObj: serverInfo,
                    apiName: "SERVER"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,serverDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetDatabaseList = function (onConnectionAdded) {
            try {
                var processName = "DATABASE_LISTING";
                var requestMethod = "get"
                var dbDetails = {
                    apiName: "DATABASE"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, dbDetails);

                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, dbDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.CreateDatabase = function (onConnectionAdded, dbData) {
            try {
                var processName = "DATABASE_CREATE";
                var requestMethod = "POST"
                var dbDetails = {
                    dataObj: dbData,
                    apiName: "DATABASE"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.UpdateDatabase = function (onConnectionAdded, dbInfo) {
            try {
                var processName = "DATABASE_UPDATE";
                var requestMethod = "PUT"
                var dbDetails = {
                    dataObj: dbInfo,
                    apiName: "DATABASE"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.DeleteDatabase = function (onConnectionAdded,dbData){
            try {
                var processName = "DATABASE_DELETE";
                var requestMethod = "DELETE"
                var dbDetails = {
                    dataObj: dbData,
                    apiName: "DATABASE"
                };
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod,dbDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetMemberList = function (onConnectionAdded, memberData) {
            try {
                var processName = "MEMBER_LISTING";
                var requestMethod = "get"
                var memberDetails = {
                    apiName: "MEMBER",
                    dataObj: memberData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, memberDetails);

                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, memberDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.UpdateMembers = function (onConnectionAdded, memberData) {
            try {
                var processName = "MEMBER_UPDATE";
                var requestMethod = "put"
                var memberDetails = {
                    apiName: "MEMBER",
                    dataObj: memberData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, memberDetails);

                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, memberDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.SaveDeployment = function (onConnectionAdded, deploymentData){
            try {
                    var processName = "DEPLOYMENT_SAVE";
                    var requestMethod = "put"
                    var deploymentDetails = {
                        apiName: "DEPLOYMENT",
                        dataObj: deploymentData
                    }
                    _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                    if (_connection == null){
                        VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                            onConnectionAdded(connection, status);
                        }, requestMethod, deploymentDetails);

                    } else {
                        VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                            onConnectionAdded(connection, status);
                        }, requestMethod, deploymentDetails);
                    }
                } catch (e) {
                    console.log(e.message);
                }
        };

        this.SaveDeploymentUser = function (onConnectionAdded, deploymentUserData, requestType, requestUser){
            try {
                    var processName = "DEPLOYMENT_USER_SAVE";
                    var requestMethod = requestType
                    var deploymentDetails = {
                        apiName: "DEPLOYMENTUSER",
                        dataObj: deploymentUserData,
                        requestUser: requestUser
                    }
                    _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                    if (_connection == null){
                        VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                            onConnectionAdded(connection, status);
                        }, requestMethod, deploymentDetails);

                    } else {
                        VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                            onConnectionAdded(connection, status);
                        }, requestMethod, deploymentDetails);
                    }
                } catch (e) {
                    console.log(e.message);
                }
        };

        this.GetDeployment = function (onConnectionAdded, dbData) {
            try {
                var processName = "DEPLOYMENT";
                var requestMethod = "get"
                var deploymentDetails = {
                    apiName: "DEPLOYMENT",
                    dataObj: dbData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, deploymentDetails);

                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, deploymentDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetDeploymentUsers = function (onConnectionAdded, dbData){
            try {
                var processName = "DEPLOYMENT_USER";
                var requestMethod = "get"
                var deploymentDetails = {
                    apiName: "DEPLOYMENTUSER",
                    dataObj: dbData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, deploymentDetails);

                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, deploymentDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetVdmStatus = function (onConnectionAdded, vdmData) {
            try {
                var processName = "VDM_STATUS";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "VDM_STATUS",
                    dataObj: vdmData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.SyncVdmConfiguration = function (onConnectionAdded, vdmData) {
            try {
                var processName = "VDM_SYNC";
                var requestMethod = "post"
                var vdmDetails = {
                    apiName: "VDM_SYNC",
                    dataObj: vdmData
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetServerIp = function (onConnectionAdded){
             try {
                var processName = "VDM_GET_IP";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "SERVER_IP"
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.StartCluster = function (onConnectionAdded, details, isHighTimeout){
            try {
                var processName = "VDM_START_CLUSTER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "START_CLUSTER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails, isHighTimeout);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails, isHighTimeout);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.StopCluster = function (onConnectionAdded, details){
            try {
                var processName = "VDM_STOP_CLUSTER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "STOP_CLUSTER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.KillCluster = function (onConnectionAdded, details){
            try {
                var processName = "VDM_KILL_CLUSTER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "KILL_CLUSTER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.RecoverCluster = function (onConnectionAdded, details){
            try {
                var processName = "VDM_RECOVER_CLUSTER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "RECOVER_CLUSTER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetDatabaseStatus = function(onConnectionAdded, details){
        try {
                var processName = "DATABASE_STATUS";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "DATABASE_STATUS",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetDatabaseServerStatus = function(onConnectionAdded, details){
        try {
                var processName = "DATABASE_SERVER_STATUS";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "DATABASE_SERVER_STATUS",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.UploadDeploymentXml = function(onConnectionAdded, details){
            try {
                var processName = "UPLOAD_DEPLOYMENT_CONFIG";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "UPLOAD_CONFIG",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.StopServer = function (onConnectionAdded, details){
            try {
                var processName = "VDM_STOP_SERVER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "STOP_SERVER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.StartServer = function (onConnectionAdded, details){
            try {
                var processName = "VDM_START_SERVER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "START_SERVER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.AddServer = function (onConnectionAdded, details){
            try {
                var processName = "VDM_ADD_SERVER";
                var requestMethod = "put"
                var vdmDetails = {
                    apiName: "ADD_SERVER",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetServerStatus = function(onConnectionAdded, details){
            try {
                var processName = "SERVER_STATUS";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "SERVER_STATUS",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetSystemInformation = function(onConnectionAdded, details){
            try {
                var processName = "SYSTEM_INFORMATION";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "GET_SYSTEM_INFORMATION",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

        this.GetStatisticsInformation = function(onConnectionAdded, details){
            try {
                var processName = "STATISTIC_INFORMATION";
                var requestMethod = "get"
                var vdmDetails = {
                    apiName: "GET_STATISTIC_INFORMATION",
                    dataObj: details
                }
                _connection = VdmCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null){
                    VdmCore.AddConnection(server, port, admin, user, password, isHashedPassword, processName, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                } else {
                    VdmCore.updateConnection(server, port, admin, user, password, isHashedPassword, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, requestMethod, vdmDetails);
                }
            } catch (e) {
                console.log(e.message);
            }
        }

    });

    window.VdmService = VdmService = new iVdmService();
})(window);

