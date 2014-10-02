
(function (window) {

    var procedures = {};
    var tips = $(".validateTips");
    var server = "184.73.30.156";
    var port = "8080";
    var user = "";
    var password = "";
    var admin = true;
    var isHashedPassword = false;
    this.connection = null;
    var iVoltDbService = (function () {
        var _connection = connection;

        this.TestConnection = function (lUsername, lPassword, lAdmin, onConnectionAdded) {
            try {
                VoltDBCore.TestConnection(server, port, lAdmin, lUsername, lPassword, isHashedPassword, "DATABASE_LOGIN", function (result) {
                    onConnectionAdded(result);
                });

            } catch (e) {
                console.log(e.message);
            }
        };

        this.SetUserCredentials = function(lUsername, lPassword, lAdmin) {
            user = lUsername;
            password = lPassword;
            admin = lAdmin;
        };
        
        this.ChangeServerConfiguration = function(serverName,portId,userName,pw,isHashPw,isAdmin) {
            server = serverName;
            port = portId;
            user = userName;
            password = pw;
            isHashedPassword = isHashPw;
            admin = isAdmin;
        };

        this.GetSystemInformation = function (onConnectionAdded) {
            try {
                _connection = VoltDBCore.HasConnection(server, port, admin, user, "SYSTEM_INFORMATION");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "SYSTEM_INFORMATION", function (result) {
                        if (result == true) {
                            updateTips("Connection successful.");
                                                       
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@SystemInformation', '@Statistics'], ["OVERVIEW", "MEMORY"], [undefined, '0'],"SYSTEM_INFORMATION", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        } else updateTips("Unable to connect.");

                    });
                    
                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
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
                _connection = VoltDBCore.HasConnection(server, port, false, user, "SYSTEM_INFORMATION_DEPLOYMENT");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "SYSTEM_INFORMATION_DEPLOYMENT", function (result) {
                        if (result == true) {
                            updateTips("Connection successful.");

                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@SystemInformation'], ["DEPLOYMENT"], [], "SYSTEM_INFORMATION_DEPLOYMENT", function (connection, status) {
                                onConnectionAdded(connection);
                            });
                        } else updateTips("Unable to connect.");

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
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
                _connection = VoltDBCore.HasConnection(server, port, false, user, "DATABASE_INFORMATION");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword,"DATABASE_INFORMATION", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics', '@SystemCatalog'], ["TABLE", "TABLES"], ['0', undefined], "DATABASE_INFORMATION", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        } 

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch(e) {
                console.log(e.message);
            } 

        };
        
        this.GetProceduresInformation = function (onConnectionAdded) {
            try {
                _connection = VoltDBCore.HasConnection(server, port, false, user, "DATABASE_INFORMATION");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword,"DATABASE_INFORMATION", function (result) {
                        if (result == true) {                            
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics'], ["PROCEDUREPROFILE"], ['0'], "DATABASE_INFORMATION", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        } 

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        
        this.GetMemoryInformation = function (onConnectionAdded) {
            try {
                _connection = VoltDBCore.HasConnection(server, port, false, user, "GRAPH_MEMORY");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "GRAPH_MEMORY", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics'], ["MEMORY"], ['0'], "GRAPH_MEMORY", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        
        this.GetGraphLatencyInformation = function (onConnectionAdded) {
            try {
                _connection = VoltDBCore.HasConnection(server, port, false, user, "GRAPH_LATENCY");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "GRAPH_LATENCY", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics'], ["LATENCY_HISTOGRAM"], ['0'], "GRAPH_LATENCY", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
        
        //TODO: Render CPU Graph
        this.GetCPUInformation = function (onConnectionAdded) {
            try {
                //GRAPH_CPU
                _connection = VoltDBCore.HasConnection(server, port, false, user, "GRAPH_CPU");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "GRAPH_CPU", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics'], ["CPU"], ['0'], "GRAPH_CPU", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
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
                _connection = VoltDBCore.HasConnection(server, port, false, user, "GRAPH_TRANSACTION");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "GRAPH_TRANSACTION", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics'], ["PROCEDUREPROFILE"], ['0'], "GRAPH_TRANSACTION", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
                        onConnectionAdded(connection, status);

                    });

                }

            } catch (e) {
                console.log(e.message);
            }

        };
    
        this.GetTableInformation = function (onConnectionAdded) {
            try {
                _connection = VoltDBCore.HasConnection(server, port, false, user, "TABLE_INFORMATION");
                if (_connection == null) {
                    VoltDBCore.TestConnection(server, port, admin, user, password, isHashedPassword, "TABLE_INFORMATION", function (result) {
                        if (result == true) {
                            VoltDBCore.AddConnection(server, port, admin, user, password, isHashedPassword, ['@Statistics', '@Statistics', '@SystemCatalog'], ["TABLE", "INDEX", "COLUMNS"], ['0', '0', undefined], "TABLE_INFORMATION", function (connection, status) {
                                onConnectionAdded(connection, status);
                            });
                        }

                    });

                } else {
                    VoltDBCore.updateConnection(server, port, admin, user, password, isHashedPassword, _connection, function (connection, status) {
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
