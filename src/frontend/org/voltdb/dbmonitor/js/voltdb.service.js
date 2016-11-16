
(function (window) {

    var procedures = {};
    var tips = $(".validateTips");
    var server = VoltDBConfig.GetDefaultServerIP();
    var port = "8080";
    var user = "";
    var password = "";
    var admin = true;
    var isHashedPassword = true;
    this.connection = null;
    var iVoltDbService = (function () {
        var _connection = connection;

        this.TestConnection = function (lServerName, lPort, lUsername, lPassword, lAdmin, onConnectionAdded, isLoginTest) {
            try {
                var serverName = lServerName != null ? lServerName : server;
                var portId = lPort != null ? lPort : port;

                VoltDBCore.TestConnection(serverName, portId, lAdmin, lUsername, lPassword, isHashedPassword, "DATABASE_LOGIN", function (result, response) {
                    onConnectionAdded(result, response);
                }, isLoginTest);

            } catch (e) {
                console.log(e.message);
            }
        };

        this.CheckServerConnection = function (checkConnection) {
            try {
                VoltDBCore.CheckServerConnection(server, port, admin, user, password, isHashedPassword, "DATABASE_LOGIN", checkConnection);
            } catch (e) {
                console.log(e.message);
            }
        };

        this.SetUserCredentials = function (lUsername, lPassword, lAdmin) {
            user = lUsername;
            password = lPassword;
            admin = lAdmin;
        };

        // build Authorization header based on scheme you could flip to diff header. Server understands both.
        this.BuildAuthorization = function (user, isHashedPassword, password) {
            var authz = null;
            if (user != null && isHashedPassword != null) {
                authz = "Hashed " + user + ":" + isHashedPassword;
            } else if (user != null && password != null) {
                var up = user + ":" + password;
                authz = "Basic " + CryptoJS.SHA256({ method: "b64enc", source: up });
            }
            return authz;
        }

        this.ChangeServerConfiguration = function (serverName, portId, userName, pw, isHashPw, isAdmin) {
            server = serverName != null ? serverName : server;
            port = portId != null ? portId : port;
            user = userName != undefined ? userName : "";
            password = pw != undefined ? pw : "";
            isHashedPassword = isHashPw;
            admin = isAdmin != undefined ? isAdmin : true;

        };

        this.GetSystemInformation = function (onConnectionAdded) {
            try {
                var processName = "SYSTEM_INFORMATION";
                var procedureNames = ['@SystemInformation', '@Statistics'];
                var parameters = ["OVERVIEW", "MEMORY"];
                var values = [undefined, '0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            updateTips("Connection successful.");

                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        } else updateTips("Unable to connect.");

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }
            } catch (e) {
                console.log(e.message);
            }

            function updateTips(t) {
                tips
                    .text(t)
                    .addClass("ui-state-highlight");
                setTimeout(function () {
                    tips.removeClass("ui-state-highlight", 1500);
                }, 500);
            }


        };

        this.GetClusterInformation = function (onConnectionAdded) {
            try {
                var processName = "CLUSTER_INFORMATION";
                var procedureNames = ['@SystemInformation'];
                var parameters = ["OVERVIEW"];
                var values = [undefined];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetSystemInformationDeployment = function (onConnectionAdded) {
            try {
                var processName = "SYSTEM_INFORMATION_DEPLOYMENT";
                var procedureNames = ['@SystemInformation'];
                var parameters = ["DEPLOYMENT"];
                var values = [];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            updateTips("Connection successful.");

                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection);
                            });
                        } else updateTips("Unable to connect.");

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection);

                    });

                }
            } catch (e) {
                console.log(e.message);
            }

            function updateTips(t) {
                tips
                    .text(t)
                    .addClass("ui-state-highlight");
                setTimeout(function () {
                    tips.removeClass("ui-state-highlight", 1500);
                }, 500);
            }


        };

        this.GetDataTablesInformation = function (onConnectionAdded) {
            try {
                var processName = "DATABASE_INFORMATION";
                var procedureNames = ['@Statistics', '@SystemCatalog', '@SystemCatalog'];
                var parameters = ["TABLE", "TABLES"];
                var values = ['0', undefined];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetProceduresInformation = function (onConnectionAdded) {
            try {
                var processName = "DATABASE_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["PROCEDUREPROFILE"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.getProcedureContextForSorting = function () {
            try {
                var processName = "DATABASE_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["PROCEDUREPROFILE"];
                var values = ['0'];
                var lconnection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (lconnection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                lconnection = connection;
                            });
                        }

                    });
                }
                return lconnection;

            } catch (e) {
                console.log(e.message);
            }
        };

        this.getTablesContextForSorting = function () {
            try {
                var processName = "DATABASE_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["TABLE"];
                var values = ['0'];
                var lconnection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (lconnection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                lconnection = connection;
                            });
                        }

                    });
                }
                return lconnection;

            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetExportTablesInformation = function (onConnectionAdded) {
            try {
                var processName = "EXPORT_TABLE_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["TABLE"];
                var values = ['0'];
                var lconnection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (lconnection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, lconnection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetImportRequestInformation = function (onConnectionAdded) {
            try {
                var processName = "IMPORT_REQUEST_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["IMPORTER"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetMemoryInformation = function (onConnectionAdded) {
            try {
                var processName = "GRAPH_MEMORY";
                var procedureNames = ['@Statistics'];
                var parameters = ["MEMORY"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetGraphLatencyInformation = function (onConnectionAdded) {
            try {
                var processName = "GRAPH_LATENCY";
                var procedureNames = ['@Statistics'];
                var parameters = ["LATENCY_HISTOGRAM"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetPartitionIdleTimeInformation = function (onConnectionAdded) {
            try {
                var processName = "GRAPH_PARTITIONIDLETIME";
                var procedureNames = ['@Statistics'];
                var parameters = ["STARVATION"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetCPUInformation = function (onConnectionAdded) {
            try {
                //GRAPH_CPU
                var processName = "GRAPH_CPU";
                var procedureNames = ['@Statistics'];
                var parameters = ["CPU"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        //Render Cluster Transaction Graph
        this.GetTransactionInformation = function (onConnectionAdded) {
            try {
                var processName = "GRAPH_TRANSACTION";
                var procedureNames = ['@Statistics'];
                var parameters = ["PROCEDUREPROFILE"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetTableInformation = function (onConnectionAdded) {
            try {
                var processName = "TABLE_INFORMATION";
                var procedureNames = ['@Statistics', '@Statistics', '@SystemCatalog', '@SystemCatalog', '@SystemCatalog'];
                var parameters = ["TABLE", "INDEX", "COLUMNS", "PROCEDURES", "PROCEDURECOLUMNS"];
                var values = ['0', '0', undefined];
                var isAdmin = true;
                _connection = VoltDBCore.HasConnection(server, port, isAdmin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, isAdmin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetTableInformationClientPort = function (onConnectionAdded) {
            try {
                var processName = "TABLE_INFORMATION_CLIENTPORT";
                var procedureNames = ['@Statistics', '@Statistics', '@SystemCatalog', '@SystemCatalog', '@SystemCatalog'];
                var parameters = ["TABLE", "INDEX", "COLUMNS", "PROCEDURES", "PROCEDURECOLUMNS"];
                var values = ['0', '0', undefined];
                var isAdmin = true;
                _connection = VoltDBCore.HasConnection(server, port, isAdmin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, isAdmin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                connection.admin = false; //Once necessary data has been fetched, set the admin privileges to false.
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    _connection.admin = true;
                    VoltDBCore.updateConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        connection.admin = false; //Once necessary data has been fetched, set the admin privileges to false.
                        onConnectionAdded(connection, status);
                    });
                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.SetConnectionForSQLExecution = function (useAdminPort) {
            try {
                var processNameSuffix = useAdminPort ? '' : '_CLIENTPORT';
                var processName = "TABLE_INFORMATION" + processNameSuffix;
                var procedureNames = ['@Statistics'];
                var parameters = ["TABLE"];
                var values = ['0'];
                //For SQL Query tab, we need to pass admin as false. This way, if the database is paused, users can't accidentally send 
                //requests that might change database contents. However, if the user has admin privileges he should be given an option to 
                //run the query even when the database is paused. This is done by passing admin as true.
                var isAdmin = useAdminPort ? true : false;
                _connection = VoltDBCore.HasConnection(server, port, isAdmin, user, processName);
                if (_connection == null) {
                    VoltDBCore.AddConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                    }, null, false);
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.GetShortApiProfile = function (onConnectionAdded) {
            try {
                var processName = "SHORTAPI_PROFILE";
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var shortApiDetails = {
                    isShortApiCall: true,
                    apiPath: 'profile'
                };

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            }, shortApiDetails);
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, shortApiDetails);

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetShortApiDeployment = function (onConnectionAdded) {
            try {
                var processName = "SHORTAPI_DEPLOYMENT";
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var shortApiDetails = {
                    isShortApiCall: true,
                    apiPath: 'deployment'
                };

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    var status = "";
                    var statusString = "";

                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@SHORTAPI_DEPLOYMENT_status'];
                                statusString = connection.Metadata['@SHORTAPI_DEPLOYMENT_statusString'];
                                onConnectionAdded(connection);

                            }, shortApiDetails);
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@SHORTAPI_DEPLOYMENT_status'];
                        statusString = connection.Metadata['@SHORTAPI_DEPLOYMENT_statusString'];
                        onConnectionAdded(connection);

                    }, shortApiDetails);

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetExportProperties = function (onConnectionAdded) {
            try {
                var processName = "SHORTAPI_DEPLOYMENT_EXPORTTYPES";
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var shortApiDetails = {
                    isShortApiCall: true,
                    apiPath: 'deployment/export/types'
                };

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    var types = "";

                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                types = connection.Metadata['@SHORTAPI_DEPLOYMENT_EXPORTTYPES'];
                                onConnectionAdded(connection);

                            }, shortApiDetails);
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        types = connection.Metadata['@SHORTAPI_DEPLOYMENT_EXPORTTYPES'];
                        onConnectionAdded(connection);

                    }, shortApiDetails);

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        //Update admin configuration
        this.UpdateAdminConfiguration = function (updatedData, onConnectionAdded) {
            try {
                if (updatedData.systemsettings.resourcemonitor != null) {
                    if (updatedData.systemsettings.resourcemonitor.memorylimit != null) {
                        var memorySize = "";
                        if (updatedData.systemsettings.resourcemonitor.memorylimit.size.indexOf("%")>-1) {
                            memorySize = parseInt(updatedData.systemsettings.resourcemonitor.memorylimit.size.replace("%", ""));
                            updatedData.systemsettings.resourcemonitor.memorylimit.size = memorySize + encodeURIComponent("%");
                        } else {
                            updatedData.systemsettings.resourcemonitor.memorylimit.size = encodeURIComponent(parseInt(updatedData.systemsettings.resourcemonitor.memorylimit.size));
                        }
                    }
                }

                var features = [];

                if (updatedData.systemsettings.resourcemonitor != null) {
                    if (updatedData.systemsettings.resourcemonitor.disklimit != null) {
                        for (var i = 0; i < updatedData.systemsettings.resourcemonitor.disklimit.feature.length; i++) {
                            var diskSize = "";
                            if (updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size.indexOf("%")>-1) {
                                diskSize = parseInt(updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size.replace("%", ""));
                                updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size = diskSize + encodeURIComponent("%");
                            } else {
                                updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size = encodeURIComponent(parseInt(updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size));
                            }

                            features.push({
                                name: updatedData.systemsettings.resourcemonitor.disklimit.feature[i].name,
                                size: updatedData.systemsettings.resourcemonitor.disklimit.feature[i].size
                            });
                        }
                        updatedData.systemsettings.resourcemonitor.disklimit.feature = features;
                    }

                }

                var processName = "SHORTAPI_UPDATEDEPLOYMENT";
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var shortApiDetails = {
                    isShortApiCall: true,
                    isUpdateConfiguration: true,
                    apiPath: 'deployment',
                    updatedData: 'deployment=' + JSON.stringify(updatedData)
                };

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            }, shortApiDetails);
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, shortApiDetails);

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        //admin configuration
        this.editConfigurationItem = function (configGroup, configMember, configValue, onConnectionSucceeded) {
            try {
                var processName = "ADMIN_".concat(configGroup);
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var isAdmin = true;

                switch (configGroup) {
                    case 'OVERVIEW':
                        procedureNames = ['@SystemInformation'];
                        parameters = [configMember];
                        values = [configValue];
                        break;

                    case 'PORT':
                        procedureNames = ['@SystemInformation'];
                        parameters = [configMember];
                        values = [configValue];
                        break;

                    case 'DIRECTORIES':
                        procedureNames = ['@SystemInformation'];
                        parameters = [configMember];
                        values = [configValue];
                        break;

                }

                _connection = VoltDBCore.HasConnection(server, port, isAdmin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, isAdmin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionSucceeded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, isAdmin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionSucceeded(connection, status);

                    });

                }

            }
            catch (e) {

            }
        };

        this.stopServerNode = function (nodeId, onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_STOPSERVER";
                var procedureNames = ['@StopNode'];
                var parameters = [nodeId.toString()];
                var values = [undefined];
                var statusString = "";

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            var status = 0;

                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@StopNode_' + nodeId.toString() + '_status'];
                                statusString = connection.Metadata['@StopNode_' + nodeId.toString() + '_statusString'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status, statusString);
                                }

                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@StopNode_' + nodeId.toString() + '_status'];
                        statusString = connection.Metadata['@StopNode_' + nodeId.toString() + '_statusString'];

                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status, statusString);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.PauseClusterState = function (onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_PAUSECLUSTER";
                var procedureNames = ['@Pause'];
                var parameters = [undefined];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            var status = 0;
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@Pause_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status);
                                }


                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@Pause_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.ResumeClusterState = function (onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_RESUMECLUSTER";
                var procedureNames = ['@Resume'];
                var parameters = [undefined];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@Resume_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@Resume_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.ShutdownClusterState = function (onConnectionAdded, zk_pause_txn_id) {
            try {
                var processName = "SYSTEMINFORMATION_SHUTDOWNCLUSTER";
                var procedureNames = ['@Shutdown'];
                var parameters = [zk_pause_txn_id];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@Shutdown_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@Shutdown_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.PrepareShutdownCluster = function (onConnectionAdded) {
            try {
                var processName = "PREPARE_SHUTDOWN_CLUSTER";
                var procedureNames = ['@PrepareShutdown'];
                var parameters = [undefined];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                            onConnectionAdded(connection, status);
                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.QuiesceCluster = function (onConnectionAdded) {
            try {
                var processName = "QUIESCE_CLUSTER";
                var procedureNames = ['@Quiesce'];
                var parameters = [undefined];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                            onConnectionAdded(connection, status);
                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.GetDrProducerInformation = function (onConnectionAdded) {
            try {
                var processName = "DR_PRODUCER_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["DRPRODUCER"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }
        };

        this.PromoteCluster = function (onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_PROMOTECLUSTER";
                var procedureNames = ['@Promote'];
                var parameters = [undefined];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@Promote_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status, connection.Metadata['@Promote_statusstring']);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@Promote_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status, connection.Metadata['@Promote_statusstring']);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }


        };

        this.SaveSnapShot = function (snapshotDir, snapshotFileName, onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_SAVESNAPSHOT";
                var procedureNames = ['@SnapshotSave'];
                var parameters = ["'" + snapshotDir + "'", snapshotFileName, 0];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@SnapshotSave_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@SnapshotSave_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetSnapshotList = function (snapshotDirectory, onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_SCANSNAPSHOTS";
                var procedureNames = ['@SnapshotScan'];
                var parameters = [snapshotDirectory];
                var values = [undefined];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@SnapshotScan_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@SnapshotScan_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        this.RestoreSnapShot = function (snapshotDir, snapshotFileName, onConnectionAdded) {
            try {
                var processName = "SYSTEMINFORMATION_RESTORESNAPSHOT";
                var procedureNames = ['@SnapshotRestore'];
                var parameters = ["'" + snapshotDir + "'", snapshotFileName, 0];
                var values = [undefined];

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                status = connection.Metadata['@SnapshotRestore_status'];
                                if (!(status == "" || status == undefined)) {
                                    onConnectionAdded(connection, status, connection.Metadata['@SnapshotRestore_statusstring']);
                                }
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        status = connection.Metadata['@SnapshotRestore_status'];
                        if (!(status == "" || status == undefined)) {
                            onConnectionAdded(connection, status, connection.Metadata['@SnapshotRestore_statusstring']);
                        }

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        //end admin configuration

        //Update User configuration
        this.UpdateUserConfiguration = function (updatedData, onConnectionAdded, userId, requestType) {
            try {
                var processName = "SHORTAPI_USERUPDATEDEPLOYMENT";
                var procedureNames = [];
                var parameters = [];
                var values = [];
                var shortApiDetails = {
                    isShortApiCall: true,
                    isUpdateConfiguration: true,
                    apiPath: 'deployment/users/' + userId,
                    updatedData: 'user=' + JSON.stringify(updatedData),
                    requestType: requestType
                };

                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            }, shortApiDetails);
                        }
                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    }, shortApiDetails);

                }

            } catch (e) {
                console.log(e.message);
            }

        };

        //Check if DR is enable or not
        this.GetDrStatusInformation = function (onConnectionAdded) {
            try {
                var processName = "DR_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["DR"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        //

        //Check if cluster is replica or not
        this.GetClusterReplicaInformation = function (onConnectionAdded) {
            try {
                var processName = "CLUSTER_REPLICA_INFORMATION";
                var procedureNames = ['@SystemInformation'];
                var parameters = ["Overview"];
                var values = [undefined];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        //

        //Get datas for DR Replication Graph
        this.GetDrReplicationInformation = function (onConnectionAdded) {
            try {
                var processName = "DR_REPLICATION_GRAPH";
                var procedureNames = ['@Statistics'];
                var parameters = ["DRCONSUMER"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        //

        this.GetLiveClientsInfo = function (onConnectionAdded) {
            try {
                var processName = "LIVE_CLIENTS_INFORMATION";
                var procedureNames = ['@Statistics'];
                var parameters = ["LIVECLIENTS"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });
                }
            } catch (e) {
                console.log(e.message);
            }

        };

        //Get host and site count
        this.GetHostAndSiteCount = function (onConnectionAdded) {
            try {
                var processName = "GET_HOST_SITE_COUNT";
                var procedureNames = ['@Statistics'];
                var parameters = ["STARVATION"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetCommandLogInformation = function (onConnectionAdded) {
            try {
                var processName = "CMD_LOG_INFO";
                var procedureNames = ['@Statistics'];
                var parameters = ["COMMANDLOG"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }

        };

        this.GetSnapshotStatus = function (onConnectionAdded) {
            try {
                var processName = "SNAPSHOT_STATUS";
                var procedureNames = ['@Statistics'];
                var parameters = ["SNAPSHOTSTATUS"];
                var values = ['0'];
                _connection = VoltDBCore.HasConnection(server, port, admin, user, processName);
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, processName, function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }
                    });
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);
                    });
                }
            } catch (e) {
                console.log(e.message);
            }
        };
    });



    window.VoltDBService = VoltDBService = new iVoltDbService();

})(window);

