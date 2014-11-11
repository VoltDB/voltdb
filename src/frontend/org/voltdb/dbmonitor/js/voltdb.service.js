
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

        this.CheckServerConnection = function(checkConnection) {
            try {
                VoltDBCore.CheckServerConnection(server, port, admin, user, password, isHashedPassword, "DATABASE_LOGIN",checkConnection);
            } catch(e) {
                console.log(e.message);
            }
        };

        this.SetUserCredentials = function(lUsername, lPassword, lAdmin) {
            user = lUsername;
            password = lPassword;
            admin = lAdmin;
        };
        
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

        this.GetDataTablesInformation = function(onConnectionAdded) {
            try {
                var processName = "DATABASE_INFORMATION";
                var procedureNames = ['@Statistics', '@SystemCatalog', '@SystemCatalog'];
                var parameters = ["TABLE", "TABLES","COLUMNS"];
                var values = ['0', undefined, undefined];
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

            } catch(e) {
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
                } //else {
                //    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, procedureNames, parameters, values, processName, lconnection, function (connection, status) {
                //        lconnection = connection;
                //    });

                //}
                return lconnection;


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
                var isAdmin = false;
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
    });

    window.VoltDBService = VoltDBService = new iVoltDbService();
    
})(window);

