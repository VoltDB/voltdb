
function alertNodeClicked(obj) {
    var clickedServer = $(obj).html();

    if ($('#activeServerName').html() != clickedServer) {

        $('.activeServerName').html(clickedServer).attr('title', clickedServer);

        //Change the active server name in the node list
        $("#serversList>li").removeClass("monitoring");

        $("#serversList").find("li a:contains(" + clickedServer + ")").filter(function () {
            return $(this).html() == clickedServer;
        }).parent().addClass("monitoring");

        var serverIp = voltDbRenderer.getServerIP($(obj).attr('data-ip'));
        var currentUrl = window.location.href.split('?')[0];
        var urlArray = currentUrl.split('/');
        var newUrl = '';
        if (urlArray != null && urlArray.length > 0) {
            var urlArray2 = urlArray[2].split(':');
            urlArray2[0] = serverIp;
            urlArray[2] = urlArray2.join(':');
            newUrl = urlArray.join('/');
        }

        var data = {
            CurrentServer: clickedServer,
            GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
            DisplayPreferences: VoltDbUI.getFromLocalStorage("user-preferences"),
            AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
            username: VoltDbUI.getCookie("username"),
            password: VoltDbUI.getCookie("password")
        };

        var win = window.open(newUrl + '?data=' + JSON.stringify(data), '_parent');
        win.focus();
    }

    $('.popup_close').trigger('click');
};

(function (window) {

    var iVoltDbRenderer = (function () {
        this.hostNames = [];
        this.currentHost = "";
        this.isHost = false;
        this.nodeStatus = new Array();
        this.isProcedureSearch = false;
        this.isTableSearch = false;
        this.isSearchTextCleaned = false;
        this.isProcedureSortClicked = false;
        this.isTableSortClicked = false;
        this.isNextClicked = false;
        this.userPreferences = {};
        this.procedureTableIndex = 0;
        this.procedureDataSize = 0;
        this.procedureSearchDataSize = 0;
        this.tableIndex = 0;
        this.tableDataSize = 0;
        this.tableSearchDataSize = 0;
        this.tupleCount = {};
        this.searchData = {};
        this.searchText = "";
        this.serverIPs = {};
        this.tupleMaxCount = {};
        this.tupleMinCount = {};
        this.maxVisibleRows = 5;
        this.sortOrder = "";
        this.sortTableOrder = "";
        this.refreshTables = false;
        var totalServerCount = 0;
        var kFactor = 0;
        var procedureData = {};
        var procedureJsonArray = [];
        var procedureSearchJsonArray = [];
        var tableData = {};
        var tableJsonArray = [];
        var tableSearchJsonArray = [];
        var schemaCatalogTableTypes = {};
        var schemaCatalogColumnTypes = {};
        var systemOverview = {};
        var systemMemory = {};
        var htmlMarkups = { "SystemInformation": [] };
        var htmlMarkup;
        var htmlTableMarkups = { "SystemInformation": [] };
        var htmlTableMarkup = "";
        var minLatency = 0;
        var maxLatency = 0;
        var avgLatency = 0;
        var procedureNameIndex = 0;
        var invocationsIndex = 0;
        var minLatencyIndex = 0;
        var maxLatencyIndex = 0;
        var avgLatencyIndex = 0;
        var perExecutionIndex = 0;
        var gCurrentServer = "";

        var tableNameIndex = 5;
        var partitionIndex = 4;
        var hostIndex = 1;
        var tupleCountIndex = 3;

        //sorting related variables
        this.isSortProcedures = false;
        this.isSortTables = false;
        this.sortColumn = "";
        this.tableSortColumn = "";
        this.isPageAction = false;

        this.hint = "";

        var activeCount = 0;
        var activeCountCopied = 0;
        var joiningCount = 0;
        var missingCount = 0;
        var alertCount = 0;
        var serverSettings = false;

        this.drTablesArray = [];

        this.exportTablesArray = [];

        this.memoryDetails = [];

        this.ChangeServerConfiguration = function (serverName, portId, userName, pw, isHashPw, isAdmin) {
            VoltDBService.ChangeServerConfiguration(serverName, portId, userName, pw, isHashPw, isAdmin);
        };

        var testConnection = function (serverName, portId, username, password, admin, onInformationLoaded) {
            VoltDBService.TestConnection(serverName, portId, username, password, admin, function (result, response) {

                onInformationLoaded(result, response);
            }, true);
        };

        this.CheckServerConnection = function (checkConnection) {
            VoltDBService.CheckServerConnection(checkConnection);
        };

        this.GetLoginPopup = function () {
            var loginHtml =
                '<a href="#loginBoxDialogue" id="loginLink" style="display: none;">Login</a>' +
                '<!-- POPUP Login -->' +
                '<div id="loginBoxDialogue" style="overflow: hidden" >' +
                    '<div class="overlay-title">Login</div>' +
                        '<div id="UnableToLoginMsg" style="padding: 5px 0 0 20px; color: #ff0000; display: none;">Unable to connect. Please try to login using another username/password.</div>' +
                            '<div class="clear"></div>' +
                            '<div  class="overlay-content" style="height:215px; min-width: 441px; padding: 0" >' +
                            '<div id="loginBox">' +
                                '<label for="username">Username:</label>' +
                                '<input type="text" id="username" name="username"><br/>' +
                                '<label for="password">Password:</label>' +
                                '<input type="password" id="password" name="password">' +
                                '<div class="lower">' +
                                    '<input type="submit" id="LoginBtn" value="Login">' +
                                '</div>' +
                            '</div>' +
                    '</div>    ' +
                '</div>' +
                '<!-- POPUP Login -->';
            return loginHtml;
        };

        this.HandleLogin = function (serverName, portId, pageLoadCallback) {

            var responseObtained = false;
            $("#username").data("servername", serverName);
            $("#username").data("portid", portId);
            $("#loginBoxDialogue").hide();
            $("#loginLink").popup({
                open: function (event, ui, ele) {
                },
                login: function (popupCallback) {

                    $("#overlay").show();
                    $("#UnableToLoginMsg").hide();
                    var usernameVal = $("#username").val();
                    var passwordVal = $("#password").val() != '' ? CryptoJS.SHA256($("#password").val()).toString(CryptoJS.enc.Hex) : $("#password").val();
                    responseObtained = false;

                    testConnection($("#username").data("servername"), $("#username").data("portid"), usernameVal, passwordVal, true, function (result, response) {

                        if (responseObtained || (response != undefined && response.hasOwnProperty("status") && response.status == -1))
                            return;
                        responseObtained = true;

                        $("#overlay").hide();
                        if (result) {

                            //Save user details to cookie.
                            saveSessionCookie("username", usernameVal);
                            saveSessionCookie("password", passwordVal);
                            voltDbRenderer.ShowUsername(usernameVal);

                            pageLoadCallback();
                            popupCallback();
                            $("#loginBoxDialogue").hide();
                            setTimeout(function () {
                                $("#username").val("");
                                $("#password").val("");
                            }, 300);
                            $("#logOut").css('display', 'block');
                            $('#logOut').prop('title', VoltDbUI.getCookie("username"));
                        } else {

                            //Error: Server is not available(-100) or Connection refused(-5) but is not "Authentication rejected(-3)"
                            if (response != undefined && response.status != -3) {
                                popupCallback();
                                $("#loginBoxDialogue").hide();
                                $("#serUnavailablePopup").trigger("click");
                                return;
                            }

                            $("#UnableToLoginMsg").show();
                            $("#logOut").css('display', 'none');
                            $('#logOut').prop('title', '');
                        }
                    });
                }
            });

            $('#username').keypress(function (e) {
                var key = e.which;
                if (key == 13)  // the enter key code
                {
                    $("#LoginBtn").trigger("click");
                    return false;
                }
                return true;
            });
            $('#password').keypress(function (e) {
                var key = e.which;
                if (key == 13)  // the enter key code
                {
                    $("#LoginBtn").trigger("click");
                    return false;
                }
                return true;
            });

            var username = (VoltDbUI.getCookie("username") != undefined) ? VoltDbUI.getCookie("username") : "";
            var password = (username != "" && VoltDbUI.getCookie("password") != undefined) ? VoltDbUI.getCookie("password") : "";

            $("#serUnavailablePopup").popup({
                open: function (event, ui, ele) {
                },
                autoLogin: function (popupCallback) {
                    tryAutoLogin();
                    popupCallback();
                }
            });

            //Try to login with saved username/password or no username and password
            var tryAutoLogin = function () {
                $("#overlay").show();
                responseObtained = false;
                serverName = VoltDBConfig.GetDefaultServerIP(true);
                testConnection(serverName, portId, username, password, true, function (result, response) {

                    if (responseObtained || (response != undefined && response.hasOwnProperty("status") && response.status == -1))
                        return;
                    responseObtained = true;

                    $("#overlay").hide();

                    if (!result) {

                        if (response != undefined && response.hasOwnProperty("status")) {

                            //Error: Hashedpassword must be a 40-byte hex-encoded SHA-1 hash.
                            if (response.status == -3 && response.hasOwnProperty("statusstring") && response.statusstring.indexOf("Hashedpassword must be a 40-byte") > -1) {
                                //Try to auto login after clearing username and password
                                saveSessionCookie("username", null);
                                saveSessionCookie("password", null);
                                tryAutoLogin();
                                return;
                            }
                            else if (response.status == 401){
                                $("#unAuthorized").trigger("click");
                                return;
                            }
                                //Error: Server is not available(-100) or Connection refused(-5) but is not "Authentication rejected(-3)"
                            else if (response.status != -3) {
                                $("#serUnavailablePopup").trigger("click");
                                return;
                            }
                        }

                        //If security is enabled, then display popup to get username and password.
                        saveSessionCookie("username", null);
                        saveSessionCookie("password", null);

                        $("#loginLink").trigger("click");
                    } else {
                        pageLoadCallback();
                    }

                });
            };

            tryAutoLogin();
        };

        this.ShowUsername = function (userName) {
            if (userName != undefined && userName != 'null' && userName != '') {
                $(".userN").attr("title", userName).html(userName);
                $("#userLine").show();
            } else {
                $(".userN").attr("title", "").html("");
                $("#userLine").hide();
            }
        };

        this.GetSystemInformation = function (onInformationLoaded, onAdminPagePortAndOverviewDetailsLoaded, onAdminPageServerListLoaded) {

            VoltDBService.GetSystemInformation(function (connection) {
                populateSystemInformation(connection);
                getMemoryDetails(connection, systemMemory);


                if (VoltDbAdminConfig.isAdmin) {
                    onAdminPagePortAndOverviewDetailsLoaded(getPortAndOverviewDetails(), serverSettings);
                    onAdminPageServerListLoaded(getAdminServerList());
                }

                if (gCurrentServer == "")
                    configureRequestedHost(VoltDBCore.hostIP);

                onInformationLoaded();
            });

        };

        this.GetClusterInformation = function (onInformationLoaded) {
            var clusterDetails = {};
            var clusterState = {};
            VoltDBService.GetClusterInformation(function (connection) {
                if (VoltDbAdminConfig.isAdmin) {
                    getClusterDetails(connection, clusterDetails, "CLUSTER_INFORMATION");
                    getClusterState(clusterDetails, clusterState);
                    onInformationLoaded(clusterState);
                }
            });
        };

        this.CheckAdminPriviledges = function (onInformationLoaded) {

            VoltDBService.GetShortApiProfile(function (connection) {
                onInformationLoaded(hasAdminPrivileges(connection));
            });
        };

        this.GetAdminDeploymentInformation = function (checkSecurity, onInformationLoaded) {
            if (VoltDbAdminConfig.isAdmin || checkSecurity) {
                VoltDBService.GetShortApiDeployment(function (connection) {
                    var rawData;
                    if (connection != null)
                        rawData = connection.Metadata['SHORTAPI_DEPLOYMENT'];
                    onInformationLoaded(loadAdminDeploymentInformation(connection), rawData);
                });
            }
        };

        this.GetCommandLogStatus = function(onInformationLoaded){
            if (VoltDbAdminConfig.isAdmin || checkSecurity) {
                VoltDBService.GetShortApiDeployment(function (connection) {
                    var rawData;
                    var isCmdLogEnabled = false;
                    if (connection != null){
                        rawData = connection.Metadata['SHORTAPI_DEPLOYMENT'];
                        if(rawData.hasOwnProperty('commandlog') && rawData['commandlog'].hasOwnProperty('enabled')){
                            isCmdLogEnabled = rawData['commandlog']['enabled'];
                        }
                    }
                    onInformationLoaded(isCmdLogEnabled);
                });
            } else {
                onInformationLoaded(false);
            }
        }

        this.GetExportProperties = function (onInformationLoaded) {

            VoltDBService.GetExportProperties(function (connection) {
                var rawData;
                if (connection != null)
                    rawData = connection.Metadata['SHORTAPI_DEPLOYMENT_EXPORTTYPES'];

                onInformationLoaded(loadExportProperties(connection), rawData);
            });
        };

        this.GetProceduresInfo = function (onProceduresDataLoaded) {
            var procedureMetadata = "";

            VoltDBService.GetSystemInformationDeployment(function (connection) {
                setKFactor(connection);
                VoltDBService.GetProceduresInformation(function (nestConnection) {
                    populateProceduresInformation(nestConnection);
                    procedureMetadata = procedureData;
                    onProceduresDataLoaded(procedureMetadata);
                });

            });

            var setKFactor = function (connection) {
                connection.Metadata['@SystemInformation_DEPLOYMENT'].data.forEach(function (entry) {
                    if (entry[0] == 'kfactor')
                        kFactor = entry[1];
                });

            };

        };

        this.getTablesInformation = function (onTableDataLoaded) {
            VoltDBService.GetDataTablesInformation(function (inestConnection) {
                populateTableTypes(inestConnection);
                populateTablesInformation(inestConnection);
                onTableDataLoaded(inestConnection.Metadata['@Statistics_TABLE'].data);
            });

        };

        this.GetDataTablesInformation = function (contextConnectionReturned) {
            VoltDBService.GetDataTablesInformation(function (inestConnection) {
                contextConnectionReturned(inestConnection);
            });
        };

        this.getMemoryGraphInformation = function (onInformationLoaded) {
            var memoryDetails = {};

            VoltDBService.GetMemoryInformation(function (connection) {
                getMemoryDetails(connection, memoryDetails, "GRAPH_MEMORY");
                onInformationLoaded(memoryDetails);
            });
        };

        this.getLatencyGraphInformation = function (onInformationLoaded) {
            var latencyDetails = {};

            VoltDBService.GetGraphLatencyInformation(function (connection) {
                getLatencyDetails(connection, latencyDetails);
                onInformationLoaded(latencyDetails);
            });
        };

        this.GetPartitionIdleTimeInformation = function (onInformationLoaded) {
            var partitionDetails = {};
            VoltDBService.GetPartitionIdleTimeInformation(function (connection) {
                getPartitionIdleTimeDetails(connection, partitionDetails);
                onInformationLoaded(partitionDetails);
            });

        };

        this.getCpuGraphInformation = function (onInformationLoaded) {
            var cpuDetails = {};

            VoltDBService.GetCPUInformation(function (connection) {
                getCpuDetails(connection, cpuDetails);
                onInformationLoaded(cpuDetails);
            });

        };

        //Check if DR is enable or not
        this.GetDrStatusInformation = function (onInformationLoaded) {
            var drStatus = {};
            VoltDBService.GetDrStatusInformation(function (connection) {
                getDrStatus(connection, drStatus);
                onInformationLoaded(drStatus);
            });
        };
        //

        //GET DR Details
        this.GetDrDetails = function (onInformationLoaded) {
            var drDetails = {};
            VoltDBService.GetDrStatusInformation(function (connection) {

                getDrDetails(connection, drDetails);
                onInformationLoaded(drDetails);
            });
        };
        //

        //Check if cluster is replica or not
        this.GetClusterReplicaInformation = function (onInformationLoaded) {
            var replicationStatus = {};

            VoltDBService.GetClusterReplicaInformation(function (connection) {
                getReplicationDetails(connection, replicationStatus, "CLUSTER_REPLICA_INFORMATION");
                onInformationLoaded(replicationStatus);
            });
        };
        //

        //Render DR Replication Graph
        this.GetDrReplicationInformation = function (onInformationLoaded) {
            var replicationData = {};

            VoltDBService.GetDrReplicationInformation(function (connection) {
                getDrReplicationData(connection, replicationData);
                onInformationLoaded(replicationData);
            });
        };
        //

        //Render DR Replication Graph
        this.GetDrConsumerInformation = function (onInformationLoaded) {
            var drConsumerData = {};

            VoltDBService.GetDrReplicationInformation(function (connection) {
                getDrConsumerData(connection, drConsumerData);
                onInformationLoaded(drConsumerData);
            });
        };
        //

        this.GetLiveClientsInfo = function (onInformationLoaded) {
            var clientsInfo = {};
            VoltDBService.GetLiveClientsInfo(function (connection) {
                getLiveClientData(connection, clientsInfo);
                onInformationLoaded(clientsInfo);
            });
        };

        //Get Cluster Id 
        this.GetDrInformations = function (onInformationLoaded) {
            var replicationData = {};
            VoltDBService.GetDrReplicationInformation(function (connection) {
                getDrInformations(connection, replicationData);
                onInformationLoaded(replicationData);
            });
        };
        //
        //Render Cluster Transaction Graph
        this.GetTransactionInformation = function (onInformationLoaded) {
            var transactionDetails = {};

            VoltDBService.GetTransactionInformation(function (connection) {
                getTransactionDetails(connection, transactionDetails);
                onInformationLoaded(transactionDetails);
            });
        };
        //

        //Get host and site count
        this.GetDeploymentInformation = function (onInformationLoaded) {
            var deploymentDetails = {};
            VoltDBService.GetSystemInformationDeployment(function (connection) {
                getDeploymentDetails(connection, deploymentDetails);
                onInformationLoaded(deploymentDetails);
            });
        };

        this.GetCommandLogInformation = function (onInformationLoaded) {
            var cmdLogDetails = {};
            VoltDBService.GetCommandLogInformation(function (connection) {
                getCommandLogDetails(connection, cmdLogDetails);
                onInformationLoaded(cmdLogDetails);
            });
        };

        this.GetSnapshotStatus = function (onInformationLoaded) {
            var snapshotDetails = {};
            VoltDBService.GetSnapshotStatus(function (connection) {
                getSnapshotStatus(connection, snapshotDetails);
                onInformationLoaded(snapshotDetails);
            });
        };

        this.GetTableInformation = function (onInformationLoaded) {
            VoltDBService.GetTableInformation(function (connection) {
                var tablesData = {};
                var viewsData = {};
                var proceduresData = {};
                var procedureColumnsData = {};
                var sysProceduresData = {};
                var exportTableData = {}
                getTableData(connection, tablesData, viewsData, proceduresData, procedureColumnsData, sysProceduresData, 'TABLE_INFORMATION', exportTableData);
                onInformationLoaded(tablesData, viewsData, proceduresData, procedureColumnsData, sysProceduresData, exportTableData);
            });
        };

        this.GetTableInformationClientPort = function () {
            VoltDBService.GetTableInformationClientPort(function (connection) {
                var tablesData = {};
                var viewsData = {};
                var proceduresData = {};
                var procedureColumnsData = {};
                var sysProceduresData = {};
                getTableData(connection, tablesData, viewsData, proceduresData, procedureColumnsData, sysProceduresData, 'TABLE_INFORMATION_CLIENTPORT');
            });
        };

        this.GetHostNodesHtml = function (callback) {
            try {
                VoltDBService.GetHostNodes(function (connection, state) {
                    populateSystemInformation(connection);
                    callback();
                });
            } catch (e) {

            }
        };

        this.GetClusterHealth = function (callback) {
            if (systemOverview == null || systemOverview == undefined) {
                alert("Error: Unable to extract cluster health information.");
                return;
            }
            var hostCount =  0
            VoltDBService.GetSystemInformationDeployment(function(connection){
                activeCount = 0;
                joiningCount = 0;
                missingCount = 0;
                alertCount = 0;
                hostCount = getHostCount(connection.Metadata['@SystemInformation_DEPLOYMENT'])

                jQuery.each(systemOverview, function (id, val) {
                    if (val["CLUSTERSTATE"] == "RUNNING" || val["CLUSTERSTATE"] == "PAUSED")
                        activeCount++;

                });

                totalServerCount = hostCount

                missingCount = totalServerCount - (activeCount + joiningCount);

                if (missingCount < 0)
                    missingCount = 0;

                var html =
                    '<li class="activeIcon">Active <span id="activeCount">(' + activeCount + ')</span></li>' +
                        '<!--<li class="joiningIcon">Joining <span id="joiningCount">(' + joiningCount + ')</span></li>-->' +
                        '<li class="missingIcon">Missing <span id="missingCount">(' + missingCount + ')</span></li>';

                var alertHtml = "";

                jQuery.each(systemOverview, function(id, val) {
                    var hostName;
                    var hostIp;
                    hostName = val["HOSTNAME"];
                    hostIp = val["IPADDRESS"];
                    var threshold = VoltDbUI.getFromLocalStorage("alert-threshold") != undefined ? VoltDbUI.getFromLocalStorage("alert-threshold") : 70;
                    if (systemMemory[hostName]["MEMORYUSAGE"] >= threshold) {
                        alertHtml += '<tr><td class="active alertAlign"  width="40%"><a data-ip="' + systemMemory[val['HOSTNAME']]['HOST_ID'] + '" onclick="alertNodeClicked(this);" href="#">' + hostName + '</a> </td>' +
                            '<td width="30%">' + hostIp + '</td>' +
                            '<td width="30%"><span class="alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        alertCount++;
                    }

                });
                if (alertCount > 0) {
                    html += '<li class="alertIcon"><a href="#memoryAlerts" id="showMemoryAlerts">Alert <span>(' + alertCount + ')</span></a></li>';
                }

                callback(html, alertHtml);
            })

        };

        var getHostCount = function(deploymentInfo){
            var hostCount = 0;
            if(deploymentInfo != undefined && !$.isEmptyObject(deploymentInfo.data)){
                deploymentInfo.data.forEach(function (entry) {
                    if (entry[0] == 'hostcount')
                        hostCount = entry[1];
                });
            }
            return hostCount;
        }

        var configureRequestedHost = function (hostName) {
            $.each(systemOverview, function (id, val) {
                if (val["IPADDRESS"] == hostName) {
                    gCurrentServer = val["HOSTNAME"];
                    saveInLocalStorage("currentServer", val["HOSTNAME"]);
                    return false;

                }
                return true;
            });

            if (gCurrentServer == "") {
                $.each(systemOverview, function (id, val) {
                    if (val["CLUSTERSTATE"] == "RUNNING") {
                        gCurrentServer = val["HOSTNAME"];
                        saveInLocalStorage("currentServer", val["HOSTNAME"]);
                        return false;
                    }
                    return true;
                });
            }

        };

        var hasAdminPrivileges = function (connection) {
            var isAdmin = false;
            if (connection != null && connection.Metadata['SHORTAPI_PROFILE'] != null) {
                var data = connection.Metadata['SHORTAPI_PROFILE'];

                if (data.permissions != null) {
                    $.each(data.permissions, function (index, value) {
                        if (value.toUpperCase().trim() == 'ADMIN') {
                            isAdmin = true;
                            return false;
                        }
                        return true;
                    });
                }
            }

            return isAdmin;
        };

        var loadAdminDeploymentInformation = function (connection) {
            var adminConfigValues = {};
            if (connection != null && connection.Metadata['SHORTAPI_DEPLOYMENT'] != null) {
                var data = connection.Metadata['SHORTAPI_DEPLOYMENT'];

                //The user does not have permission to view admin details.
                if (data.status == -3) {
                    adminConfigValues.VMCNoPermission = true;
                    return adminConfigValues;
                }

                adminConfigValues['sitesperhost'] = data.cluster.sitesperhost;
                adminConfigValues['kSafety'] = data.cluster.kfactor;

                adminConfigValues['partitionDetection'] = data.partitionDetection != null ? data.partitionDetection.enabled : false;
                adminConfigValues['securityEnabled'] = data.security != null ? data.security.enabled : false;

                if (data.users != null && data.users.user != null)
                    adminConfigValues['users'] = data.users.user.length > 0 ? data.users.user : null;
                else
                    adminConfigValues['users'] = null;

                //HTTP Access
                if (data.httpd != null) {
                    adminConfigValues['httpEnabled'] = data.httpd.enabled;
                    adminConfigValues['jsonEnabled'] = data.httpd.jsonapi.enabled;
                }

                //Auto Snapshot
                if (data.snapshot != null) {
                    adminConfigValues['snapshotEnabled'] = data.snapshot.enabled;
                    adminConfigValues['frequency'] = data.snapshot.frequency;
                    adminConfigValues['retained'] = data.snapshot.retain;
                    adminConfigValues['filePrefix'] = data.snapshot.prefix;
                }

                //Command Logging
                if (data.commandlog != null) {
                    adminConfigValues['commandLogEnabled'] = data.commandlog.enabled;
                    if (data.commandlog.frequency != null) {
                        adminConfigValues['commandLogFrequencyTime'] = data.commandlog.frequency.time;
                        adminConfigValues['commandLogFrequencyTransactions'] = data.commandlog.frequency.transactions;
                    }
                    adminConfigValues['logSegmentSize'] = data.commandlog.logsize;
                }

                //Export
                if (data.export != null) {
                    adminConfigValues['export'] = data.export.enabled;
                    adminConfigValues['targets'] = data.export.target;
                    adminConfigValues['configuration'] = data.export.configuration;
                }

                //Advanced 
                if (data.heartbeat != null) {
                    adminConfigValues['heartBeatTimeout'] = data.heartbeat.timeout;
                }

                if (data.systemsettings != null && data.systemsettings.query != null) {
                    adminConfigValues['queryTimeout'] = data.systemsettings.query.timeout;
                }

                if (data.systemsettings != null) {
                    if (data.systemsettings.temptables != null)
                        adminConfigValues['tempTablesMaxSize'] = data.systemsettings.temptables.maxsize;

                    if (data.systemsettings.snapshot != null)
                        adminConfigValues['snapshotPriority'] = data.systemsettings.snapshot.priority;

                    if (data.systemsettings.resourcemonitor != null) {
                        if (data.systemsettings.resourcemonitor.memorylimit != null) {
                            adminConfigValues['memorylimit'] = data.systemsettings.resourcemonitor.memorylimit.size;
                        }
                    }
                    
                    if (data.systemsettings.resourcemonitor != null) {
                        if (data.systemsettings.resourcemonitor.disklimit != null) {
                            adminConfigValues['disklimit'] = data.systemsettings.resourcemonitor.disklimit;
                        }
                    }
                }

                //Directory
                if (data.paths != null) {
                    if (data.paths.voltdbroot != null)
                        adminConfigValues['voltdbRoot'] = data.paths.voltdbroot.path;

                    if (data.paths.snapshots != null)
                        adminConfigValues['snapshotPath'] = data.paths.snapshots.path;

                    if (data.paths.exportoverflow != null)
                        adminConfigValues['exportOverflow'] = data.paths.exportoverflow.path;

                    if (data.paths.commandlog != null)
                        adminConfigValues['commandLogPath'] = data.paths.commandlog.path;

                    if (data.paths.commandlogsnapshot != null)
                        adminConfigValues['commandLogSnapshotPath'] = data.paths.commandlogsnapshot.path;
                    
                    if (data.paths.droverflow != null)
                        adminConfigValues['drOverflowPath'] = data.paths.droverflow.path;
                }

                //dr
                if (data.dr != null) {
                    adminConfigValues['drConnectionSource'] = data.dr.connection != null ? data.dr.connection.source : "";
                    adminConfigValues['drId'] = data.dr.id;
                    adminConfigValues['drListen'] = data.dr.listen;
                    adminConfigValues['drPort'] = data.dr.port;
                }

                //import

                if (data.import != null) {
                    adminConfigValues['importConfiguration'] = data.import.configuration;
                }
            }

            return adminConfigValues;
        };

        var loadExportProperties = function (connection) {
            var exportProperties = {};
            if (connection != null && connection.Metadata['SHORTAPI_DEPLOYMENT_EXPORTTYPES'] != null) {
                var data = connection.Metadata['SHORTAPI_DEPLOYMENT_EXPORTTYPES'];
                exportProperties['type'] = data.types;
            }

            return exportProperties;
        };


        var populateSystemInformation = function (connection) {
            var updatedSystemOverview = [];
            var currentServerOverview = {};
            var serverOverview = {};
            var iterator = 0;
            var ipAddress = "";

            //Error: "Authentication rejected(-3)"
            if (connection.Metadata['@SystemInformation_OVERVIEW_status'] == -3) {
                VoltDbUI.hasPermissionToView = false;

                if (!$("#loginWarningPopup").is(":visible")) {
                    $("#loginWarningPopupMsg").text("Security settings have been changed. You no longer have permission to view this page.");
                    $("#loginWarnPopup").click();
                }
                return;
            } else if (connection.Metadata['@SystemInformation_OVERVIEW'] == null) {
                return;
            }

            connection.Metadata['@SystemInformation_OVERVIEW'].data.forEach(function (entry) {
                var singleData = entry;
                var id = singleData[0];
                if (singleData[1] == 'IPADDRESS') {
                    if (singleData[2] == VoltDBConfig.GetDefaultServerIP()) {
                        voltDbRenderer.isHost = true;

                    } else {
                        voltDbRenderer.isHost = false;
                        serverOverview[id] = {};
                        ipAddress = singleData[2];
                    }
                }

                if (singleData[1] == 'HOSTNAME') {
                    if (voltDbRenderer.isHost) {
                        voltDbRenderer.currentHost = singleData[2];

                    }
                    if ($.inArray(singleData[2], voltDbRenderer.hostNames) == -1)
                        voltDbRenderer.hostNames.push(singleData[2]);

                }

                //assign entry in data object to 'currentServerOverview' if object being iterated is not a current host object
                //otherwise to a updatedSystemOverview 
                if (voltDbRenderer.isHost) {
                    currentServerOverview[singleData[1]] = singleData[2];

                    if (singleData[1] == "LOG4JPORT") {
                        currentServerOverview["NODEID"] = id;
                    }
                }
                else {
                    serverOverview[id][singleData[1]] = singleData[2];

                    if (singleData[1] == "LOG4JPORT") {
                        serverOverview[id]["NODEID"] = id;
                        updatedSystemOverview[iterator] = serverOverview[id];
                        iterator++;
                    }
                }
            });

            systemOverview = {};
            if (!$.isEmptyObject(currentServerOverview))
                systemOverview[0] = currentServerOverview;

            //iterate through updatedSystemOverview to add remaining server to the list 'systemOverview'
            for (iterator = 0; iterator < updatedSystemOverview.length; iterator++) {
                if (!$.isEmptyObject(currentServerOverview))
                    systemOverview[iterator + 1] = updatedSystemOverview[iterator];
                else {
                    systemOverview[iterator] = updatedSystemOverview[iterator];
                }
            };

        };

        var populateTablesInformation = function (connection) {
            var counter = 0;
            voltDbRenderer.refreshTables = true;
            connection.Metadata['@Statistics_TABLE'].schema.forEach(function (columnInfo) {

                if (columnInfo["name"] == "HOST_ID")
                    hostIndex = counter;

                if (columnInfo["name"] == "TABLE_NAME")
                    tableNameIndex = counter;

                else if (columnInfo["name"] == "PARTITION_ID")
                    partitionIndex = counter;

                else if (columnInfo["name"] == "TUPLE_COUNT")
                    tupleCountIndex = counter;

                counter++;

            });

            counter = 0;

            if (voltDbRenderer.isSortTables && !voltDbRenderer.isTableSearch) { //is sorting is enabled create json array first to contain sort data on it,
                //then after sorting add it to a parent json object procedureData
                populateTableJsonArray(connection);
                if (voltDbRenderer.sortTableOrder == "ascending") {
                    tableJsonArray = ascendingSortJSON(tableJsonArray, voltDbRenderer.tableSortColumn);

                } else if (voltDbRenderer.sortTableOrder == "descending") {
                    tableJsonArray = descendingSortJSON(tableJsonArray, voltDbRenderer.tableSortColumn);

                }
                mapJsonArrayToTables();

            } else if (voltDbRenderer.isSortTables && voltDbRenderer.isTableSearch) {
                voltDbRenderer.formatSearchTablesDataToJsonArray(connection, voltDbRenderer.searchText);
                if (voltDbRenderer.sortTableOrder == "ascending") {
                    tableSearchJsonArray = ascendingSortJSON(tableSearchJsonArray, voltDbRenderer.tableSortColumn);

                } else if (voltDbRenderer.sortTableOrder == "descending") {
                    tableSearchJsonArray = descendingSortJSON(tableSearchJsonArray, voltDbRenderer.tableSortColumn);

                }
                mapJsonArrayToSearchedTables();

            } else {
                formatTableData(connection);

            }
            voltDbRenderer.refreshTables = false;
        };

        var populateProceduresInformation = function (connection) {
            var counter = 0;

            if (connection != null) {
                connection.Metadata['@Statistics_PROCEDUREPROFILE'].schema.forEach(function (columnInfo) {
                    if (columnInfo["name"] == "PROCEDURE")
                        procedureNameIndex = counter;
                    else if (columnInfo["name"] == "INVOCATIONS")
                        invocationsIndex = counter;
                    else if (columnInfo["name"] == "MIN")
                        minLatencyIndex = counter;
                    else if (columnInfo["name"] == "MAX")
                        maxLatencyIndex = counter;
                    else if (columnInfo["name"] == "AVG")
                        avgLatencyIndex = counter;
                    else if (columnInfo["name"] == "WEIGHTED_PERC")
                        perExecutionIndex = counter;

                    counter++;

                });

                if (voltDbRenderer.isSortProcedures && !voltDbRenderer.isProcedureSearch) { //is sorting is enabled create json array first to contain sort data on it,
                    //then after sorting add it to a parent json object procedureData
                    populateProcedureJsonArray(connection);
                    if (voltDbRenderer.sortOrder == "ascending") {
                        procedureJsonArray = ascendingSortJSON(procedureJsonArray, voltDbRenderer.sortColumn);

                    } else if (voltDbRenderer.sortOrder == "descending") {
                        procedureJsonArray = descendingSortJSON(procedureJsonArray, voltDbRenderer.sortColumn);

                    }
                    mapJsonArrayToProcedures();

                } else if (voltDbRenderer.isSortProcedures && voltDbRenderer.isProcedureSearch) {
                    voltDbRenderer.formatSearchDataToJsonArray(false);
                    if (voltDbRenderer.sortOrder == "ascending") {
                        procedureSearchJsonArray = ascendingSortJSON(procedureSearchJsonArray, voltDbRenderer.sortColumn);

                    } else if (voltDbRenderer.sortOrder == "descending") {
                        procedureSearchJsonArray = descendingSortJSON(procedureSearchJsonArray, voltDbRenderer.sortColumn);

                    }
                    mapJsonArrayToSearchedProcedures();

                } else {
                    populateProcedureData(connection);

                }

                voltDbRenderer.procedureDataSize = connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.length;

            }
        };

        var populateProcedureData = function (connection) {
            var procedureCount = 0;
            var procedure = {};
            procedureData = [];

            if (connection.Metadata['@Statistics_PROCEDUREPROFILE'].data != "" &&
                connection.Metadata['@Statistics_PROCEDUREPROFILE'].data != [] &&
                connection.Metadata['@Statistics_PROCEDUREPROFILE'].data != undefined) {
                connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.forEach(function (entry) {
                    var name = entry[procedureNameIndex];
                    minLatency = entry[minLatencyIndex] * Math.pow(10, -6);
                    maxLatency = entry[maxLatencyIndex] * Math.pow(10, -6);
                    avgLatency = entry[avgLatencyIndex] * Math.pow(10, -6);

                    minLatency = parseFloat(minLatency.toFixed(2));
                    maxLatency = parseFloat(maxLatency.toFixed(2));
                    avgLatency = parseFloat(avgLatency.toFixed(2));

                    if (!procedureData.hasOwnProperty(name)) {
                        procedure = {
                            'PROCEDURE': entry[procedureNameIndex],
                            'INVOCATIONS': entry[invocationsIndex],
                            'MIN_LATENCY': minLatency,
                            'MAX_LATENCY': maxLatency,
                            'AVG_LATENCY': avgLatency,
                            'PERC_EXECUTION': entry[perExecutionIndex]
                        };
                        procedureData.push(procedure);

                        procedureCount++;
                    }

                });
            } else {
                formatTableNoData("PROCEDURE");

            }
        };

        var populateProcedureJsonArrayForSorting = function (connection) {
            var procedureCount = 0;
            if (connection != null) {

                var isPopulateSortData = checkSortColumnSortable();
                if (connection.Metadata['@Statistics_PROCEDUREPROFILE'] != null) {
                    connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.forEach(function (entry) {
                        var name = entry[procedureNameIndex];
                        minLatency = entry[minLatencyIndex] * Math.pow(10, -6);
                        maxLatency = entry[maxLatencyIndex] * Math.pow(10, -6);
                        avgLatency = entry[avgLatencyIndex] * Math.pow(10, -6);

                        minLatency = parseFloat(minLatency.toFixed(2));
                        maxLatency = parseFloat(maxLatency.toFixed(2));
                        avgLatency = parseFloat(avgLatency.toFixed(2));

                        if (!procedureData.hasOwnProperty(name)) {
                            procedureData[name] = {};
                        } else {
                            procedureData[name]['PROCEDURE'] = entry[procedureNameIndex];
                            procedureData[name]['INVOCATIONS'] = entry[invocationsIndex];
                            procedureData[name]['MIN_LATENCY'] = minLatency;
                            procedureData[name]['MAX_LATENCY'] = maxLatency;
                            procedureData[name]['AVG_LATENCY'] = avgLatency;
                            procedureData[name]['PERC_EXECUTION'] = entry[perExecutionIndex];
                            procedureCount++;

                        }

                    });
                }

                procedureJsonArray = [];
                procedureCount = 0;
                if (voltDbRenderer.searchText == "" || voltDbRenderer.searchText == undefined || isPopulateSortData) {
                    jQuery.each(procedureData, function (key, data) {
                        if (!checkIfDuplicateJson(procedureJsonArray, key)) {
                            procedureJsonArray[procedureCount] = {
                                "PROCEDURE": data.PROCEDURE,
                                "INVOCATIONS": data.INVOCATIONS,
                                "MIN_LATENCY": data.MIN_LATENCY,
                                "MAX_LATENCY": data.MAX_LATENCY,
                                "AVG_LATENCY": data.AVG_LATENCY,
                                "PERC_EXECUTION": data.PERC_EXECUTION
                            };
                            procedureCount++;
                        }
                    });

                }

            }

        };

        var populateProcedureJsonArray = function (connection) {
            var procedureCount = 0;
            if (connection != undefined) {
                if (connection.Metadata['@Statistics_PROCEDUREPROFILE'].data != undefined) {
                    //apply search only if column is "PROCEDURE"
                    var isPopulateSortData = checkSortColumnSortable();
                    procedureJsonArray = [];

                    connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.forEach(function (entry) {
                        if (voltDbRenderer.searchText == "" || voltDbRenderer.searchText == undefined) {
                            var name = entry[procedureNameIndex];
                            minLatency = entry[minLatencyIndex] * Math.pow(10, -6);
                            maxLatency = entry[maxLatencyIndex] * Math.pow(10, -6);
                            avgLatency = entry[avgLatencyIndex] * Math.pow(10, -6);

                            minLatency = parseFloat(minLatency.toFixed(2));
                            maxLatency = parseFloat(maxLatency.toFixed(2));
                            avgLatency = parseFloat(avgLatency.toFixed(2));

                            if (!checkIfDuplicateJson(procedureJsonArray, entry[procedureNameIndex])) {
                                procedureJsonArray[procedureCount] = {
                                    "PROCEDURE": entry[procedureNameIndex],
                                    "INVOCATIONS": entry[invocationsIndex],
                                    "MIN_LATENCY": minLatency,
                                    "MAX_LATENCY": maxLatency,
                                    "AVG_LATENCY": avgLatency,
                                    "PERC_EXECUTION": entry[perExecutionIndex]
                                };
                                procedureCount++;
                            }
                        } else {
                            if (isPopulateSortData) {
                                var name = entry[procedureNameIndex];
                                minLatency = entry[minLatencyIndex] * Math.pow(10, -6);
                                maxLatency = entry[maxLatencyIndex] * Math.pow(10, -6);
                                avgLatency = entry[avgLatencyIndex] * Math.pow(10, -6);

                                minLatency = parseFloat(minLatency.toFixed(2));
                                maxLatency = parseFloat(maxLatency.toFixed(2));
                                avgLatency = parseFloat(avgLatency.toFixed(2));

                                if (!checkIfDuplicateJson(procedureJsonArray, entry[procedureNameIndex])) {
                                    procedureJsonArray[procedureCount] = {
                                        "PROCEDURE": entry[procedureNameIndex],
                                        "INVOCATIONS": entry[invocationsIndex],
                                        "MIN_LATENCY": minLatency,
                                        "MAX_LATENCY": maxLatency,
                                        "AVG_LATENCY": avgLatency,
                                        "PERC_EXECUTION": entry[perExecutionIndex]
                                    };
                                    procedureCount++;
                                }
                            }
                        }

                    });

                }
            }
        };

        var populateTableJsonArray = function (connection) {
            var tableCount = 0;
            formatTableData(connection);
            if (tableData != undefined || tableData != "") {

                //apply search only if column is "TABLENAME"                
                if (!voltDbRenderer.isTableSearch) {

                    tableJsonArray = [];

                    $.each(tableData, function (key, data) {
                        tableJsonArray[tableCount] = {
                            "TABLE_NAME": key,
                            "MAX_ROWS": data["MAX_ROWS"],
                            "MIN_ROWS": data["MIN_ROWS"],
                            "AVG_ROWS": data["AVG_ROWS"],
                            "TUPLE_COUNT": data["TUPLE_COUNT"],
                            "TABLE_TYPE": schemaCatalogTableTypes[key].REMARKS
                        };
                        tableCount++;
                    });
                }
                else {

                    tableSearchJsonArray = [];

                    $.each(lSearchData.tables, function (key, data) {
                        tableSearchJsonArray[tableCount] = {
                            "TABLE_NAME": key,
                            "MAX_ROWS": data["MAX_ROWS"],
                            "MIN_ROWS": data["MIN_ROWS"],
                            "AVG_ROWS": data["AVG_ROWS"],
                            "TUPLE_COUNT": data["TUPLE_COUNT"],
                            "TABLE_TYPE": schemaCatalogTableTypes[key].REMARKS
                        };
                        tableCount++;
                    });
                }

            }
        };

        var populateTableTypes = function (connection) {
            var counter = 0;
            var tableName;
            var tableNameIndex = 0;
            var tableTypeIndex = 0;
            var remarksIndex = 0;

            connection.Metadata['@SystemCatalog_TABLES'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "TABLE_NAME")
                    tableNameIndex = counter;

                if (columnInfo["name"] == "TABLE_TYPE")
                    tableTypeIndex = counter;

                if (columnInfo["name"] == "REMARKS")
                    remarksIndex = counter;

                counter++;
            });

            connection.Metadata['@SystemCatalog_TABLES'].data.forEach(function (entry) {
                tableName = entry[tableNameIndex];
                if (!schemaCatalogTableTypes.hasOwnProperty(tableName)) {
                    schemaCatalogTableTypes[tableName] = {};
                }
                schemaCatalogTableTypes[tableName]['TABLE_NAME'] = entry[tableNameIndex];

                if (entry[remarksIndex] != null) {
                    schemaCatalogTableTypes[tableName]['REMARKS'] = jQuery.parseJSON(entry[remarksIndex]).partitionColumn != null ? "PARTITIONED" : "REPLICATED";
                    schemaCatalogTableTypes[tableName]['drEnabled'] = jQuery.parseJSON(entry[remarksIndex]).drEnabled;
                } else {
                    schemaCatalogTableTypes[tableName]['REMARKS'] = "REPLICATED";
                }
                schemaCatalogTableTypes[tableName]['TABLE_TYPE'] = entry[tableTypeIndex];
            });

        };

        this.mapNodeInformationByStatus = function (callback) {
            var counter = 0;
            var memoryThreshold = VoltDbUI.getFromLocalStorage("alert-threshold") != '' ? VoltDbUI.getFromLocalStorage("alert-threshold") : -1;
            var htmlMarkups = { "ServerInformation": [] };
            var htmlMarkup;
            var currentServerHtml = "";

            if (systemOverview == null || systemOverview == undefined) {
                alert("Error: Unable to extract Node Status");
                return;
            }

            var currentServer = getCurrentServer();
            if (currentServer != "" || currentServer != null) {
                currentServerHtml = currentServer;
            }
            jQuery.each(systemOverview, function (id, val) {
                var hostName;
                var hostIP;
                hostName = val["HOSTNAME"];
                hostIP = val["IPADDRESS"];
                if (counter == 0) {
                    /*************************************************************************
                    //CLUSTERSTATE implies if server is running or joining
                    **************************************************************************/
                    if (hostName != null && currentServer == hostName && val["CLUSTERSTATE"] == "RUNNING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = '<tr class="filterClass serverActive" ><td class="active monitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        } else {
                            htmlMarkup = '<tr class="filterClass serverActive"><td class="active monitoring" width="40%"> <a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }
                    }
                    else if (hostName != null && currentServer != hostName && val["CLUSTERSTATE"] == "RUNNING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = '<tr class="filterClass"><td class="active" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span>' +
                                '<span class=\"hostIdHidden\" style=\"display:none\">"' + systemMemory[hostName]["HOST_ID"] + '</span></td></tr>';
                        } else {
                            htmlMarkup = '<tr class="filterClass"><td class="active" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                    else if (hostName != null && currentServer == hostName && val["CLUSTERSTATE"] == "PAUSED") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span>' +
                                '<span class=\"hostIdHidden\" style=\"display:none\">"' + systemMemory[hostName]["HOST_ID"] + '</span></td></tr>';
                        } else {
                            htmlMarkup = '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                    else if (hostName != null && currentServer != hostName && val["CLUSTERSTATE"] == "PAUSED") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td  width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td  width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span>' +
                                '<span class=\"hostIdHidden\" style=\"display:none\">"' + systemMemory[hostName]["HOST_ID"] + '</span></td></tr>';
                        } else {
                            htmlMarkup = '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                    else if (hostName != null && val["CLUSTERSTATE"] == "JOINING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="joining" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span>' +
                                '<span class=\"hostIdHidden\" style=\"display:none\">"' + systemMemory[hostName]["HOST_ID"] + '</span></td></tr>';

                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="joining" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }
                    }

                } else {
                    /********************************************************************************************
                    "currentServerHtml" is validated to verify if current server to be monitored is already set
                    *********************************************************************************************/
                    if (hostName != null && currentServerHtml != "" && currentServerHtml == hostName && val["CLUSTERSTATE"] == "RUNNING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass serverActive"><td class="active monitoring"  width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';

                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass serverActive"><td class="active monitoring"  width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span data-ip="' + systemMemory[hostName]["HOST_ID"] + '" class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }
                    }
                    if (hostName != null && currentServerHtml != hostName && val["CLUSTERSTATE"] == "RUNNING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="active"  width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="active"  width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                    if (hostName != null && currentServerHtml == hostName && val["CLUSTERSTATE"] == "PAUSED") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span>' +
                               '<span class=\"hostIdHidden\" style=\"display:none\">"' + systemMemory[hostName]["HOST_ID"] + '</span></td></tr>';
                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }

                    if (hostName != null && currentServerHtml != hostName && val["CLUSTERSTATE"] == "PAUSED") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                               '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                               '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                    if (hostName != null && val["CLUSTERSTATE"] == "JOINING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="joining" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                                '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                                '<td width="30%"><span class="memory-status alert">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';

                        } else {
                            htmlMarkup = htmlMarkup + '<tr class="filterClass"><td class="joining" width="40%"><a class="serverNameAlign" data-ip="' + systemMemory[hostName]["HOST_ID"] + '" href="javascript:void(0);">' + hostName + '</a></td>' +
                              '<td width="30%"><span class="servIpNum">' + hostIP + '</span></td>' +
                              '<td width="30%"><span class="memory-status">' + systemMemory[hostName]["MEMORYUSAGE"] + '%</span></td></tr>';
                        }

                    }
                }
                counter++;
            });
            htmlMarkups.ServerInformation.push({ "ServersList": htmlMarkup });
            htmlMarkups.ServerInformation.push({ "CurrentServer": currentServerHtml });
            callback(htmlMarkups);
        };

        this.mapProcedureInformation = function (currentAction, priorAction, callback) {
            var counter = 0;
            var pageStartIndex = 0;
            var isNextButtonClicked = false;
            htmlMarkup = "";
            htmlMarkups.SystemInformation = [];

            if (procedureData == null || procedureData == undefined) {
                alert("Error: Unable to extract Procedure Data");
                return;
            }

            //if checks if tuple count is greater than 5
            //other no needs for pagination action validation
            if ((((voltDbRenderer.procedureTableIndex + 1) * this.maxVisibleRows < voltDbRenderer.procedureDataSize) && currentAction == VoltDbUI.ACTION_STATES.NEXT) ||
                (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && voltDbRenderer.procedureTableIndex > 0) ||
                currentAction == VoltDbUI.ACTION_STATES.REFRESH ||
                currentAction == VoltDbUI.ACTION_STATES.SEARCH ||
                currentAction == VoltDbUI.ACTION_STATES.NONE) {

                if (currentAction == VoltDbUI.ACTION_STATES.NEXT) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows;

                }

                // pageStartIndex need not be initialized if isNext is undefined(when page loads intially or during reload operation)
                if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex - 1) * voltDbRenderer.maxVisibleRows;
                }
                if ((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) ||
                    (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex) * voltDbRenderer.maxVisibleRows;

                }

                isNextButtonClicked = voltDbRenderer.isNextClicked;
                if (isNextButtonClicked == false) {
                    if (currentAction == VoltDbUI.ACTION_STATES.SEARCH || currentAction == VoltDbUI.ACTION_STATES.NONE) {
                        pageStartIndex = 0;
                        voltDbRenderer.procedureTableIndex = 0;
                    }
                }

                var lProcedureData = voltDbRenderer.isProcedureSearch ? this.searchData.procedures : procedureData;
                jQuery.each(lProcedureData, function (id, val) {
                    if (currentAction == VoltDbUI.ACTION_STATES.NEXT && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                            if (counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureDataSize - 1) {
                                voltDbRenderer.procedureTableIndex++;
                                return false;

                            }

                        } else if (counter == pageStartIndex * 2) {
                            voltDbRenderer.procedureTableIndex++;
                            return false;

                        }

                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);

                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.SEARCH && priorAction == VoltDbUI.ACTION_STATES.NONE)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.SEARCH) || (currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.NEXT)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;

                        }
                    } else {
                        if (counter < voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }
                    }
                    counter++;
                });


                if (voltDbRenderer.isProcedureSearch) {
                    if (htmlMarkup != "") {
                        if ((currentAction == VoltDbUI.ACTION_STATES.SEARCH || currentAction == VoltDbUI.ACTION_STATES.REFRESH) && (priorAction == VoltDbUI.ACTION_STATES.SEARCH || priorAction == VoltDbUI.ACTION_STATES.REFRESH)) {
                            $('#storeProcedureBody').html(htmlMarkup);

                        }
                        callback(currentAction, htmlMarkup);
                    }
                    priorAction = currentAction;

                } else {
                    htmlMarkups.SystemInformation.push(htmlMarkup);
                    htmlMarkup = undefined;

                    if (htmlMarkups.SystemInformation[0] != "")
                        callback(currentAction, htmlMarkups);
                }

            }

            if (voltDbRenderer.isSortProcedures && VoltDbUI.sortStatus == VoltDbUI.SORT_STATES.SORTED) {
                VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.NONE;
            }
        };

        this.mapProcedureInformationSorting = function (currentAction, priorAction, callback) {
            var counter = 0;
            var pageStartIndex = 0;
            var traverse = false;

            htmlMarkup = "";
            htmlMarkups.SystemInformation = [];

            var iterateProcedureData = function () {
                counter = 0;
                var lProcedureData = voltDbRenderer.isProcedureSearch ? voltDbRenderer.searchData.procedures : procedureData;
                jQuery.each(lProcedureData, function (id, val) {
                    if (currentAction == VoltDbUI.ACTION_STATES.NEXT && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                            if (counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureDataSize - 1) {
                                voltDbRenderer.procedureTableIndex++;
                                return false;

                            }

                        } else if (counter == pageStartIndex * 2) {
                            voltDbRenderer.procedureTableIndex++;
                            return false;

                        }

                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);

                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.SEARCH && priorAction == VoltDbUI.ACTION_STATES.NONE)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.SEARCH) || (currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.NEXT)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;
                        }
                    } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;

                        }
                    } else {
                        if (counter < voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }
                    }
                    counter++;
                });
            };

            if (procedureData == null || procedureData == undefined) {
                alert("Error: Unable to extract Procedure Data");
                return;
            }

            //if checks if tuple count is greater than 5
            //other no needs for pagination action validation
            if ((((voltDbRenderer.procedureTableIndex + 1) * this.maxVisibleRows < voltDbRenderer.procedureDataSize) && currentAction == VoltDbUI.ACTION_STATES.NEXT) ||
                (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && voltDbRenderer.procedureTableIndex > 0) ||
                (currentAction == VoltDbUI.ACTION_STATES.SEARCH && !voltDbRenderer.isProcedureSortClicked) ||
                (priorAction == VoltDbUI.ACTION_STATES.SEARCH && currentAction == VoltDbUI.ACTION_STATES.SORT) ||
                currentAction == VoltDbUI.ACTION_STATES.REFRESH ||
                currentAction == VoltDbUI.ACTION_STATES.SORT ||
                currentAction == VoltDbUI.ACTION_STATES.NONE) {

                if (currentAction == VoltDbUI.ACTION_STATES.NEXT) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows;

                }

                if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS) { // pageStartIndex need not be initialized if isNext is undefined(when page loads intially or during reload operation)
                    pageStartIndex = (voltDbRenderer.procedureTableIndex - 1) * voltDbRenderer.maxVisibleRows;
                }

                if ((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) ||
                    (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex) * voltDbRenderer.maxVisibleRows;

                }

                if (currentAction == VoltDbUI.ACTION_STATES.SEARCH || currentAction == VoltDbUI.ACTION_STATES.NONE ||
                    (currentAction == VoltDbUI.ACTION_STATES.REFRESH && voltDbRenderer.isSortProcedures == true) ||
                    (currentAction == VoltDbUI.ACTION_STATES.SORT)) {
                    pageStartIndex = 0;
                    voltDbRenderer.procedureTableIndex = 0;
                }
                iterateProcedureData();


            } else {
                //if previous is infinitely and sorting is clicked
                if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS || voltDbRenderer.isProcedureSortClicked) {
                    pageStartIndex = 0;
                    voltDbRenderer.procedureTableIndex = 0;

                    var lProcedureData = voltDbRenderer.isProcedureSearch ? this.searchData.procedures : procedureData;
                    jQuery.each(lProcedureData, function (id, val) {
                        if (counter >= pageStartIndex && counter <= voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                            counter++;
                        }
                    });
                    priorAction = currentAction;

                }

            }

            if (voltDbRenderer.isSortProcedures && VoltDbUI.sortStatus == VoltDbUI.SORT_STATES.SORTED) {
                VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.NONE;
            }

            if (voltDbRenderer.getLatencyGraphInformationcurrentProcedureAction == VoltDbUI.ACTION_STATES.SEARCH) {
                voltDbRenderer.priorProcedureAction = voltDbRenderer.currentProcedureAction;
            }

            voltDbRenderer.currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
            VoltDbUI.CurrentProcedureDataProgress = VoltDbUI.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;

            callback(htmlMarkup);

        };

        this.mapTableInformation = function (currentAction, priorAction, isSearch, callback) {
            var counter = 0;
            var tablePageStartIndex = 0;

            if (tableData == null || tableData == undefined) {
                alert("Error: Unable to extract Table Data");
                return;
            }

            htmlTableMarkup = "";
            htmlTableMarkups.SystemInformation = [];

            if ((((voltDbRenderer.tableIndex + 1) * this.maxVisibleRows < voltDbRenderer.tableDataSize) && currentAction == VoltDbUI.ACTION_STATES.NEXT) ||
                (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && voltDbRenderer.tableIndex > 0) ||
                currentAction == VoltDbUI.ACTION_STATES.REFRESH ||
                currentAction == VoltDbUI.ACTION_STATES.SEARCH ||
                currentAction == VoltDbUI.ACTION_STATES.NONE ||
                voltDbRenderer.isTableSortClicked) {
                if (currentAction == VoltDbUI.ACTION_STATES.NEXT) {
                    tablePageStartIndex = (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows;

                }

                else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS) { // pageStartIndex need not be initialized if isNext is undefined(when page loads intially or during reload operation)
                    tablePageStartIndex = (voltDbRenderer.tableIndex - 1) * voltDbRenderer.maxVisibleRows;

                }

                else if (((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) ||
                    (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) && !voltDbRenderer.isTableSortClicked) {
                    if (voltDbRenderer.isSearchTextCleaned) {
                        tablePageStartIndex = 0;
                        voltDbRenderer.tableIndex = 0;
                    }

                    else
                        tablePageStartIndex = (voltDbRenderer.tableIndex) * voltDbRenderer.maxVisibleRows;

                }

                else if (currentAction == VoltDbUI.ACTION_STATES.SEARCH || currentAction == VoltDbUI.ACTION_STATES.NONE || voltDbRenderer.isTableSortClicked == true) {
                    tablePageStartIndex = 0;
                    voltDbRenderer.tableIndex = 0;

                }

                var lTableData = this.isTableSearch ? this.searchData.tables : tableData;
                if (this.isTableSearch == false) voltDbRenderer.tableDataSize = Object.keys(tableData).length;

                voltDbRenderer.drTablesArray = [];
                voltDbRenderer.exportTablesArray = [];

                $.each(lTableData, function (id, val) {
                    if (val['drEnabled'] == "true") {
                        voltDbRenderer.drTablesArray.push(val['TABLE_NAME']);
                    }

                    if (val['TABLE_TYPE1'] == "EXPORT") {
                        voltDbRenderer.exportTablesArray.push(val['TABLE_NAME']);
                    }

                    if (lTableData)
                        if (currentAction == VoltDbUI.ACTION_STATES.NEXT && (voltDbRenderer.isTableSearch == false || voltDbRenderer.isTableSearch == undefined)) {
                            if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                                setTableTupleDataHtml(val, id);
                                if (counter == (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableDataSize - 1) {
                                    voltDbRenderer.tableIndex++;
                                    return false;
                                }

                            } else if (counter == tablePageStartIndex * 2) {
                                voltDbRenderer.tableIndex++;
                                return false;
                            }

                        } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && (voltDbRenderer.isTableSearch == false || voltDbRenderer.isTableSearch == undefined)) {
                            if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows)) {
                                setTableTupleDataHtml(val, id);
                            }
                            if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                                voltDbRenderer.tableIndex--;
                            }

                        } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                            if (counter >= 0 && counter >= tablePageStartIndex && counter < voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows) {
                                setTableTupleDataHtml(val, id);
                            }

                            if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                                voltDbRenderer.tableIndex--;
                            }

                        } else if (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                            if (counter >= 0 && counter >= tablePageStartIndex && counter < voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows) {
                                setTableTupleDataHtml(val, id);
                            }

                            if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                                voltDbRenderer.tableIndex--;
                            }

                        } else if (currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.NEXT) {
                            if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows - 1) {
                                setTableTupleDataHtml(val, id);
                            }

                        } else if ((currentAction == VoltDbUI.ACTION_STATES.REFRESH && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                            if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < ((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                                setTableTupleDataHtml(val, id);

                            }

                        } else if ((currentAction == VoltDbUI.ACTION_STATES.SEARCH && priorAction == VoltDbUI.ACTION_STATES.NONE) || (currentAction == VoltDbUI.ACTION_STATES.SEARCH && priorAction == VoltDbUI.ACTION_STATES.SEARCH) ||
                        (currentAction == VoltDbUI.ACTION_STATES.SEARCH && priorAction == VoltDbUI.ACTION_STATES.REFRESH)) {
                            if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < ((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                                setTableTupleDataHtml(val, id);
                            }

                        } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.SEARCH) || (currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.NEXT)) {
                            if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                                setTableTupleDataHtml(val, id);
                            }

                            if ((counter == (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableSearchDataSize - 1) && htmlTableMarkup != "") {
                                voltDbRenderer.tableIndex++;
                                return false;
                            }

                        } else if ((currentAction == VoltDbUI.ACTION_STATES.NEXT && priorAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                            if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                                setTableTupleDataHtml(val, id);
                            }

                            if ((counter == (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableSearchDataSize - 1) && htmlTableMarkup != "") {
                                voltDbRenderer.tableIndex++;
                                return false;
                            }

                        } else {
                            if (counter < voltDbRenderer.maxVisibleRows) {
                                setTableTupleDataHtml(val, id);
                            }

                        }
                    counter++;

                });


                if (voltDbRenderer.isSortTables) {
                    callback(htmlTableMarkup);
                    htmlTableMarkup = "";
                }
                else {
                    htmlTableMarkups.SystemInformation.push(htmlTableMarkup);
                    htmlTableMarkup = "";
                    callback(htmlTableMarkups.SystemInformation);

                }

                if (voltDbRenderer.isSortTables && VoltDbUI.tableSortStatus == VoltDbUI.SORT_STATES.SORTED) {
                    VoltDbUI.tableSortStatus = VoltDbUI.SORT_STATES.NONE;
                }
            }

        };

        this.getVersion = function (serverName) {
            var version;
            $.each(systemOverview, function (key, val) {
                if (val["HOSTNAME"] == serverName) {
                    version = val["VERSION"];
                    return false;
                }
            });
            return version;
        };

        this.getServerIP = function (hostId) {
            var serverAddress;
            $.each(systemOverview, function (key, val) {
                if (val.NODEID == hostId) {
                    serverAddress = val["IPADDRESS"];
                    return false;
                }

            });
            return serverAddress;
        };

        this.getClusterDetail = function (serverName) {
            var clusterInfo = [];
            $.each(systemOverview, function (key, val) {
                if (val["HOSTNAME"] == serverName) {
                    clusterInfo["VERSION"] = val["VERSION"];
                    clusterInfo["MODE"] = val["CLUSTERSTATE"];
                    clusterInfo["BUILDSTRING"] = val["BUILDSTRING"];
                    clusterInfo["STARTTIME"] = val["STARTTIME"];
                    clusterInfo["UPTIME"] = val["UPTIME"];
                    clusterInfo["LICENSE"] = val["LICENSE"];
                    return false;
                }
            });
            return clusterInfo;
        };

        this.sortTablesByColumns = function (isSearched) {
            var lConnection = VoltDBService.getTablesContextForSorting();

            if (voltDbRenderer.isTableSearch) {
                voltDbRenderer.formatSearchTablesDataToJsonArray(lConnection, $('#filterDatabaseTable')[0].value, isSearched);

                if (voltDbRenderer.sortTableOrder == "descending") {
                    tableSearchJsonArray = descendingSortJSON(tableSearchJsonArray, this.tableSortColumn);
                }

                else if (voltDbRenderer.sortTableOrder == "ascending") {
                    tableSearchJsonArray = ascendingSortJSON(tableSearchJsonArray, this.tableSortColumn);
                }
                mapJsonArrayToSearchedTables();
            }

            else if (!voltDbRenderer.isTableSearch) {
                populateTableJsonArray(lConnection);

                if (voltDbRenderer.sortTableOrder == "descending") {
                    tableJsonArray = descendingSortJSON(tableJsonArray, this.tableSortColumn);
                }

                else if (voltDbRenderer.sortTableOrder == "ascending") {
                    tableJsonArray = ascendingSortJSON(tableJsonArray, this.tableSortColumn);
                }
                mapJsonArrayToTables();

            }

        };

        this.sortProceduresByColumns = function (isSearched) {
            var isSorted = false;
            if (!voltDbRenderer.isProcedureSearch) {
                var lConnection = VoltDBService.getProcedureContextForSorting();
                if (lConnection != null) {
                    populateProcedureJsonArrayForSorting(lConnection);

                    if (voltDbRenderer.sortOrder == "descending") {
                        procedureJsonArray = descendingSortJSON(procedureJsonArray, this.sortColumn);
                    }

                    else if (voltDbRenderer.sortOrder == "ascending") {
                        procedureJsonArray = ascendingSortJSON(procedureJsonArray, this.sortColumn);
                    }
                    mapJsonArrayToProcedures();
                    isSorted = true;

                } else {
                    isSorted = false;
                }

            }
            else if (voltDbRenderer.isProcedureSearch) {
                voltDbRenderer.formatSearchDataToJsonArray(isSearched);

                if (voltDbRenderer.sortOrder == "descending") {
                    procedureSearchJsonArray = descendingSortJSON(procedureSearchJsonArray, this.sortColumn);
                }

                else if (voltDbRenderer.sortOrder == "ascending") {
                    procedureSearchJsonArray = ascendingSortJSON(procedureSearchJsonArray, this.sortColumn);
                }
                mapJsonArrayToSearchedProcedures();
                isSorted = true;
            }
            return isSorted;
        };

        var getLatencyDetails = function (connection, latency) {

            var colIndex = {};
            var counter = 0;

            connection.Metadata['@Statistics_LATENCY_HISTOGRAM'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "UNCOMPRESSED_HISTOGRAM" || columnInfo["name"] == "TIMESTAMP")
                    colIndex[columnInfo["name"]] = counter;

                counter++;
            });

            connection.Metadata['@Statistics_LATENCY_HISTOGRAM'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!latency.hasOwnProperty(hostName)) {
                    latency[hostName] = {};
                }
                latency[hostName]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                latency[hostName]["UNCOMPRESSED_HISTOGRAM"] = info[colIndex["UNCOMPRESSED_HISTOGRAM"]];
            });
        };

        var getMemoryDetails = function (connection, sysMemory, processName) {
            var counter = 0;
            var hostNameIndex = 0;
            var tupledDataIndex = 0;
            var tupleCountIndex = 0;
            var rssIndex = 0;
            var physicalMemoryIndex = -1;
            var suffix = "";
            var timeStampIndex = 0;
            var idIndex = 0;
            var hostNameList = {};
            var javaMaxHeapIndex = 0;

            if (processName == "GRAPH_MEMORY") {
                suffix = "_" + processName;
                hostNameList = { "1": { "HOSTNAME": getCurrentServer() } };
            } else {
                hostNameList = systemOverview;
            }

            if (connection.Metadata['@Statistics_MEMORY' + suffix] == null) {
                return;
            }
            connection.Metadata['@Statistics_MEMORY' + suffix].schema.forEach(function (columnInfo) {

                if (columnInfo["name"] == "HOSTNAME")
                    hostNameIndex = counter;
                else if (columnInfo["name"] == "TUPLEDATA")
                    tupledDataIndex = counter;
                else if (columnInfo["name"] == "TUPLECOUNT")
                    tupleCountIndex = counter;
                else if (columnInfo["name"] == "RSS")
                    rssIndex = counter;
                else if (columnInfo["name"] == "PHYSICALMEMORY")
                    physicalMemoryIndex = counter;
                else if (columnInfo["name"] == "TIMESTAMP")
                    timeStampIndex = counter;
                else if (columnInfo["name"] == "HOST_ID")
                    idIndex = counter;
                else if (columnInfo["name"] == "JAVAMAXHEAP")
                    javaMaxHeapIndex = counter;
                counter++;
            });


            connection.Metadata['@Statistics_MEMORY' + suffix].data.forEach(function (memoryInfo) {
                jQuery.each(hostNameList, function (id, val) {
                    if (val["HOSTNAME"] == memoryInfo[hostNameIndex]) {

                        var hostName = memoryInfo[hostNameIndex];

                        if (!sysMemory.hasOwnProperty(hostName)) {
                            sysMemory[hostName] = {};
                        }

                        sysMemory[hostName]["TIMESTAMP"] = memoryInfo[timeStampIndex];
                        sysMemory[hostName]["HOSTNAME"] = hostName;
                        sysMemory[hostName]["TUPLEDATA"] = memoryInfo[tupledDataIndex];
                        sysMemory[hostName]["TUPLECOUNT"] = memoryInfo[tupleCountIndex];
                        sysMemory[hostName]["RSS"] = memoryInfo[rssIndex];
                        sysMemory[hostName]["HOST_ID"] = memoryInfo[idIndex];
                        sysMemory[hostName]["PHYSICALMEMORY"] = memoryInfo[physicalMemoryIndex];
                        sysMemory[hostName]["MAXJAVAHEAP"] = memoryInfo[javaMaxHeapIndex];

                        var memoryUsage = (sysMemory[hostName]["RSS"] / sysMemory[hostName]["PHYSICALMEMORY"]) * 100;
                        sysMemory[hostName]["MEMORYUSAGE"] = Math.round(memoryUsage * 100) / 100;

                        voltDbRenderer.memoryDetails.push(memoryInfo[timeStampIndex])
                    }

                });
            });
        };

        var getCpuDetails = function (connection, sysMemory) {
            var colIndex = {};
            var counter = 0;

            if (connection.Metadata['@Statistics_CPU'] == null) {
                return;
            }

            connection.Metadata['@Statistics_CPU'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "PERCENT_USED" || columnInfo["name"] == "TIMESTAMP")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });


            connection.Metadata['@Statistics_CPU'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!sysMemory.hasOwnProperty(hostName)) {
                    sysMemory[hostName] = {};
                }
                sysMemory[hostName]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                sysMemory[hostName]["PERCENT_USED"] = info[colIndex["PERCENT_USED"]];
            });
        };

        var getLiveClientData = function (connection, clientInfo) {
            var trans = 0
            var bytes = 0
            var msgs  = 0
            if(!clientInfo.hasOwnProperty('CLIENTS'))
                clientInfo['CLIENTS'] = {}

            if (connection.Metadata['@Statistics_LIVECLIENTS'] == undefined || $.isEmptyObject(connection.Metadata['@Statistics_LIVECLIENTS'].data)) {
                clientInfo['CLIENTS']['bytes'] = 0;
                clientInfo['CLIENTS']['msgs'] = 0;
                clientInfo['CLIENTS']['trans'] = 0;
                return;
            }

            connection.Metadata['@Statistics_LIVECLIENTS'].data.forEach(function (info) {
                bytes += info[6]
                msgs += info[7]
                trans += info[8]
            });

            clientInfo['CLIENTS']['bytes'] = bytes;
            clientInfo['CLIENTS']['msgs'] = msgs;
            clientInfo['CLIENTS']['trans'] = trans;
        };

        //Get DR Status Information
        var getDrStatus = function (connection, drDetails) {
            var colIndex = {};
            var counter = 0;

            if (!drDetails.hasOwnProperty("Details")) {
                drDetails["Details"] = {};
            }
            drDetails["Details"]["STATUS"] = connection.Metadata["@Statistics_DR_status"];

            if (connection.Metadata['@Statistics_DR_completeData'] == null || $.isEmptyObject(connection.Metadata['@Statistics_DR_completeData'])) {
                return;
            }

            connection.Metadata['@Statistics_DR_completeData'][1].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "SYNCSNAPSHOTSTATE" || columnInfo["name"] == "STATE")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@Statistics_DR_completeData'][1].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!drDetails.hasOwnProperty(hostName)) {
                    drDetails[hostName] = {};
                }
                var isEnable = false;
                if (info[colIndex["STATE"]] != null && info[colIndex["STATE"]].toLowerCase() != "off")
                    isEnable = true;
                drDetails[hostName]["MASTERENABLED"] = isEnable;
                drDetails[hostName]["STATE"] = info[colIndex["STATE"]];
                drDetails[hostName]["SYNCSNAPSHOTSTATE"] = info[colIndex["SYNCSNAPSHOTSTATE"]];
            });
        };

        //Get DR Details Information
        var getDrDetails = function (connection, drDetails) {
            var colIndex = {};
            var counter = 0;

            if (connection.Metadata['@Statistics_DR'] == null) {
                return;
            }



            connection.Metadata['@Statistics_DR_completeData'][0].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "PARTITION_ID" || columnInfo["name"] == "TOTALBUFFERS" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "TOTALBYTES" || columnInfo["name"] == "MODE" || columnInfo["name"] == "LASTQUEUEDDRID" || columnInfo["name"] == "LASTACKDRID" || columnInfo["name"] == "LASTQUEUEDTIMESTAMP" || columnInfo["name"] == "LASTACKTIMESTAMP")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            counter = 0;

            connection.Metadata['@Statistics_DR_completeData'][0].data.forEach(function (info) {
                //Filter Master from Replica
                if (info[colIndex["MODE"]] == "NORMAL") {
                    var partitionId = info[colIndex["PARTITION_ID"]];
                    if (!drDetails.hasOwnProperty(partitionId)) {
                        drDetails[partitionId] = [];
                    }

                    var partitionDetails = {};
                    partitionDetails["TOTALBUFFERS"] = info[colIndex["TOTALBUFFERS"]];
                    partitionDetails["TOTALBYTES"] = info[colIndex["TOTALBYTES"]];
                    partitionDetails["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                    partitionDetails["LASTQUEUEDDRID"] = info[colIndex["LASTQUEUEDDRID"]];
                    partitionDetails["LASTACKDRID"] = info[colIndex["LASTACKDRID"]];
                    partitionDetails["LASTQUEUEDTIMESTAMP"] = info[colIndex["LASTQUEUEDTIMESTAMP"]];
                    partitionDetails["LASTACKTIMESTAMP"] = info[colIndex["LASTACKTIMESTAMP"]];
                    drDetails[partitionId].push(partitionDetails);
                }
            });
        };


        //Get Replication Information
        var getReplicationDetails = function (connection, replicationDetails, processName) {
            var colIndex = {};
            var counter = 0;
            var replicaStatus = false;
            var hostName = "";
            var suffix = "_" + processName;
            if (connection.Metadata['@SystemInformation_Overview' + suffix] == null) {
                return;
            }

            connection.Metadata['@SystemInformation_Overview' + suffix].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "KEY" || columnInfo["name"] == "VALUE")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });


            connection.Metadata['@SystemInformation_Overview' + suffix].data.forEach(function (info) {
                if (info[colIndex["KEY"]] == "HOSTNAME") {
                    hostName = info[colIndex["VALUE"]];
                }
                if (info[colIndex["KEY"]] == "REPLICATIONROLE") {
                    replicaStatus = info[colIndex["VALUE"]];
                    if (!replicationDetails.hasOwnProperty(hostName)) {
                        replicationDetails[hostName] = {};
                    }
                    replicationDetails[hostName]["status"] = replicaStatus;
                }

            });
        };
        //

        //Get DR Replication Data
        var getDrReplicationData = function (connection, replicationDetails) {
            var colIndex = {};
            var colIndex2 = {};
            var counter = 0;
            var replicationRate1M = 0;
            if (connection.Metadata['@Statistics_DRCONSUMER'] == null) {
                return;
            }

            connection.Metadata['@Statistics_DRCONSUMER'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "REPLICATION_RATE_1M" || columnInfo["name"] == "HOST_ID" || columnInfo["name"] == "STATE" || columnInfo["name"] == "REPLICATION_RATE_5M" || columnInfo["name"] == "CLUSTER_ID")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            counter = 0;
            connection.Metadata['@Statistics_DRCONSUMER_completeData'][1].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == 'IS_COVERED')
                    colIndex2[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@Statistics_DRCONSUMER'].data.forEach(function (info) {
                if (!replicationDetails.hasOwnProperty("DR_GRAPH")) {
                    replicationDetails["DR_GRAPH"] = {};
                    replicationDetails["DR_GRAPH"]["REPLICATION_DATA"] = [];
                }

                replicationRate1M += (info[colIndex["REPLICATION_RATE_1M"]] == null || info[colIndex["REPLICATION_RATE_1M"]] < 0) ? 0 : info[colIndex["REPLICATION_RATE_1M"]];

                var repData = {};
                repData["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                replicationDetails["DR_GRAPH"]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                replicationDetails["DR_GRAPH"]["CLUSTER_ID"] = info[colIndex["CLUSTER_ID"]];
                repData["HOST_ID"] = info[colIndex["HOST_ID"]];
                repData["HOSTNAME"] = info[colIndex["HOSTNAME"]];
                repData["STATE"] = info[colIndex["STATE"]];
                repData["REPLICATION_RATE_5M"] = info[colIndex["REPLICATION_RATE_5M"]] / 1000;
                repData["REPLICATION_RATE_1M"] = info[colIndex["REPLICATION_RATE_1M"]] / 1000;
                replicationDetails["DR_GRAPH"]["REPLICATION_DATA"].push(repData);

            });

            replicationDetails["DR_GRAPH"]['WARNING_COUNT'] = getReplicationNotCovered(connection.Metadata['@Statistics_DRCONSUMER_completeData'][1], colIndex2['IS_COVERED']);
            replicationDetails["DR_GRAPH"]["REPLICATION_RATE_1M"] = replicationRate1M / 1000;
        };

        var getDrConsumerData = function(connection, drConsumerDetails) {
            var colIndex = {};
            var counter = 0;
            if (connection.Metadata['@Statistics_DRCONSUMER'] == null) {
                return;
            }

            connection.Metadata['@Statistics_DRCONSUMER'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "STATE")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@Statistics_DRCONSUMER'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]].split('/')[0];
                if (!drConsumerDetails.hasOwnProperty(hostName)) {
                    drConsumerDetails[hostName] = {};
                }
                drConsumerDetails[hostName]['STATE'] = info[colIndex["STATE"]];
            });
        }

        var getDrInformations = function (connection, replicationDetails) {
            var colIndex = {};
            var counter = 0;
            if (connection.Metadata['@Statistics_DRCONSUMER'] == null) {
                return;
            }

            connection.Metadata['@Statistics_DRCONSUMER'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "HOST_ID" || columnInfo["name"] == "CLUSTER_ID")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@Statistics_DRCONSUMER'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]].split('/')[0];
                if (!replicationDetails.hasOwnProperty(hostName)) {
                    replicationDetails[hostName] = {};
                }
                var clusterId = (info[colIndex["CLUSTER_ID"]] == undefined ? "N/A" : info[colIndex["CLUSTER_ID"]]);
                replicationDetails[hostName]["CLUSTER_ID"] = clusterId;
            });
        };

        var getDrProducerInfo = function (connection, drDetails) {
            if (connection.Metadata['@Statistics_DRPRODUCER_completeData'] == null || $.isEmptyObject(connection.Metadata['@Statistics_DRPRODUCER_completeData'])) {
                return;
            }

            var partition_max = drDetails["DrProducer"]["partition_max"];
            var partition_min = drDetails["DrProducer"]["partition_min"];
            var partition_min_host = drDetails["DrProducer"]["partition_min_host"];

            $.each(partition_min, function(key, value){
                // reset all min values to find the new min
                if($.inArray(key, partition_max_key) != -1){
                    var partition_max_key = Object.keys(partition_max);
                    partition_min[key] = partition_max[key]
                }
            });

            connection.Metadata['@Statistics_DRPRODUCER_completeData'][0].data.forEach(function (info) {
                var partition_min_key = Object.keys(partition_min);
                var partition_max_key = Object.keys(partition_max);

                var pid = info[3];
                var hostname = info[2].toString();
                var last_queued = -1
                var last_acked = -1

                if(info[8].toString() != 'None')
                    last_queued = info[8]

                if(info[9].toString() != 'None')
                    last_acked = info[9]

                // check TOTALBYTES
                if (info[5] > 0){
                    // track the highest seen drId for each partition. use last queued to get the upper bound
                    if($.inArray(pid, partition_max_key) != -1)
                        partition_max[pid] = Math.max(last_queued, partition_max[pid])
                    else
                        partition_max[pid] = last_queued

                    if($.inArray(pid, partition_min_key) != -1){
                        if(last_acked < partition_min[pid]){
                            // this replica is farther behind
                            partition_min[pid] = last_acked
                        }
                    } else {
                        partition_min_host[pid] = []
                        partition_min[pid] = last_acked
                    }
                    partition_min_host[pid].push(hostname)
                } else {
                    // this hostname's partition has an empty InvocationBufferQueue
                    if($.inArray(pid, partition_min_key) != -1){
                        // it was not empty on a previous call
                        partition_min_host[pid] = $.grep(partition_min_host[pid], function(value) {
                                                      return value != hostname;
                                                    });
                        if (partition_min_host[pid] == undefined || partition_min_host[pid].length == 0){
                            delete partition_min_host[pid]
                            delete partition_min[pid]
                        }
                    }
                    if($.inArray(pid, partition_max_key) != -1){

                        if(partition_max[pid] > last_acked){
                            console.log("DR Producer reports no data for partition "+ pid +" on host "+ hostname +
                            " but last acked drId ("+ last_acked +
                            ") does not match other hosts last acked drId ("+ partition_max[pid] +")");
                        }
                        partition_max[pid] = Math.max(last_acked, partition_max[pid])
                    } else {
                        partition_max[pid] = last_acked
                    }
                }
            });

            if (!drDetails.hasOwnProperty("DrProducer")) {
                drDetails["DrProducer"] = {};
            }

            drDetails["DrProducer"]["partition_max"] = partition_max;
            drDetails["DrProducer"]["partition_min"] = partition_min;
            drDetails["DrProducer"]["partition_min_host"] = partition_min_host;

        };

        var getExportTableInfo = function (connection, exportTableDetails) {
            if (connection.Metadata['@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData'] == null || $.isEmptyObject(connection.Metadata['@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData'])) {
                exportTableDetails["ExportTables"]["collection_time"] = 1
                return;
            }
            var export_tables_with_data = exportTableDetails["ExportTables"]["export_tables_with_data"];
            var last_collection_time = exportTableDetails["ExportTables"]["last_collection_time"];
            var tablestats = null;
            var collection_time = 0;
            var export_tables = 0;
            if(connection.Metadata['@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData'][0].data.length == 0){
                exportTableDetails["ExportTables"]["collection_time"] = 1
                return;
            } else {
                tablestats = connection.Metadata['@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData'][0].data
                if(tablestats.length == 0){
                    exportTableDetails["ExportTables"]["collection_time"] = 1
                    return;
                }
                var firsttuple = tablestats[0]
                if(firsttuple[0] == last_collection_time){
                    // this statistic is the same cached set as the last call
                    exportTableDetails["ExportTables"]["collection_time"] = collection_time
                    return;
                } else {
                    collection_time = firsttuple[0]
                }
            }

            connection.Metadata['@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData'][0].data.forEach(function (info) {
                // first look for streaming (export) tables
                if(info[6].toString() == 'StreamedTable'){
                    var pendingData = info[8]
                    tablename = info[5].toString()
                    pid = info[4]
                    hostname = info[2].toString()
                    if(pendingData > 0){
                        var export_tables_with_data_key = Object.keys(export_tables_with_data)
                        if($.inArray(tablename, export_tables_with_data_key) == -1)
                            export_tables_with_data[tablename] = {}
                        var tabledata = export_tables_with_data[tablename]
                        var tableDataKeys =  Object.keys(tabledata)
                        if($.inArray(hostname, tableDataKeys) == -1)
                            tabledata[hostname] = []
                        tabledata[hostname].push(pid)
                    }
                    else {
                        var export_tables_with_data_key = Object.keys(export_tables_with_data)
                        if($.inArray(tablename, export_tables_with_data_key) != -1){
                            var tabledata = export_tables_with_data[tablename]
                            var tableDataKeys =  Object.keys(tabledata)
                            if($.inArray(hostname, tableDataKeys) != -1){
                                tabledata[hostname] = $.grep(tabledata[hostname], function(value) {
                                                      return value != pid;
                                                    });
                                if($.isEmptyObject(tabledata[hostname])){
                                    delete tabledata[hostname]
                                    if($.isEmptyObject(export_tables_with_data[tablename]))
                                        delete export_tables_with_data[tablename]
                                }
                            }
                        }
                    }
                }
            });

            exportTableDetails["ExportTables"]["export_tables_with_data"] = export_tables_with_data;
            exportTableDetails["ExportTables"]["last_collection_time"] = last_collection_time;
            exportTableDetails["ExportTables"]["collection_time"] = collection_time

        };

        var getReplicationNotCovered = function (replicationData, index) {
            var count = 0;
            if (index != undefined) {
                replicationData.data.forEach(function (columnInfo) {
                    columnInfo.forEach(function (col, i) {
                        if (col == 'false' && i == index) {
                            count++;
                        }
                    });
                });
            }
            return count;
        }

        var getPartitionIdleTimeDetails = function (connection, partitionDetail) {
            var colIndex = {};
            var counter = 0;
            var keys = [];
            var starvStats = {};
            var starvMpiData = {};
            var starvMaxData = {};
            var starvMinData = {};
            var currentServer = getCurrentServer();
            var hostName;
            var timeStamp;
            var siteId = 0;
            var mpiIndex = [];
            var minPer = 100;
            var maxPer = 0;
            var previousHost = "";
            var previousHostKey = "";
            var previousSiteId = "";
            if (connection.Metadata['@Statistics_STARVATION'] == null) {
                return;
            }

            connection.Metadata['@Statistics_STARVATION'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "SITE_ID" || columnInfo["name"] == "PERCENT" || columnInfo["name"] == "TIMESTAMP")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            //to get MPI site id
            connection.Metadata['@Statistics_STARVATION'].data.forEach(function (info) {
                if (currentServer == info[colIndex["HOSTNAME"]]) {
                    if (parseInt(info[colIndex["SITE_ID"]]) > siteId) {
                        siteId = parseInt(info[colIndex["SITE_ID"]]);
                    }
                } else {
                    if (!mpiIndex.hasOwnProperty(info[colIndex["HOSTNAME"]])) {
                        mpiIndex[info[colIndex["HOSTNAME"]]] = 0;
                    }
                    if (parseInt(info[colIndex["SITE_ID"]]) > mpiIndex[info[colIndex["HOSTNAME"]]]) {
                        mpiIndex[info[colIndex["HOSTNAME"]]] = parseInt(info[colIndex["SITE_ID"]]);
                    }
                }
            });
            //

            connection.Metadata['@Statistics_STARVATION'].data.forEach(function (info) {
                if (currentServer == info[colIndex["HOSTNAME"]]) {
                    if (siteId == parseInt(info[colIndex["SITE_ID"]])) {
                        var keyMpi = info[colIndex["HOSTNAME"]] + ': ' + info[colIndex["SITE_ID"]];
                        starvMpiData[keyMpi] = info[colIndex["PERCENT"]];
                    } else {
                        var key = info[colIndex["HOSTNAME"]] + ': ' + info[colIndex["SITE_ID"]];
                        keys.push(key);
                        starvStats[key] = info[colIndex["PERCENT"]];
                    }
                } else {
                    if (parseInt(info[colIndex["SITE_ID"]]) != mpiIndex[info[colIndex["HOSTNAME"]]]) {
                        if (info[colIndex["HOSTNAME"]] != hostName) {
                            hostName = info[colIndex["HOSTNAME"]];
                            if (previousHostKey != "") {
                                starvMinData[previousHostKey + '(Min)'] = minPer;
                                starvMaxData[previousHostKey + '(Max)'] = maxPer;
                                minPer = 100;
                                maxPer = 0;
                            }
                            if (parseFloat(info[colIndex["PERCENT"]]) < minPer) {
                                minPer = parseFloat(info[colIndex["PERCENT"]]);
                            }
                            if (parseFloat(info[colIndex["PERCENT"]]) > maxPer) {
                                maxPer = parseFloat(info[colIndex["PERCENT"]]);
                            }
                        } else {
                            if (parseFloat(info[colIndex["PERCENT"]]) < minPer) {
                                minPer = parseFloat(info[colIndex["PERCENT"]]);
                            }
                            if (parseFloat(info[colIndex["PERCENT"]]) > maxPer) {
                                maxPer = parseFloat(info[colIndex["PERCENT"]]);
                            }
                            previousHost = info[colIndex["HOSTNAME"]];
                            previousHostKey = info[colIndex["HOSTNAME"]] + ': ' + info[colIndex["SITE_ID"]];
                            previousSiteId = info[colIndex["SITE_ID"]];
                        }
                    }
                }
                timeStamp = info[colIndex["TIMESTAMP"]];
            });
            if (previousHostKey != "" && previousSiteId != mpiIndex[previousHost]) {
                starvMinData[previousHostKey + '(Min)'] = minPer;
                starvMaxData[previousHostKey + '(Max)'] = maxPer;
            }
            keys.sort();

            if (!partitionDetail.hasOwnProperty("partitionDetail")) {
                partitionDetail["partitionDetail"] = {};
            }
            partitionDetail["partitionDetail"]["data"] = starvStats;
            partitionDetail["partitionDetail"]["dataMPI"] = starvMpiData;
            if (!$.isEmptyObject(starvMaxData))
                partitionDetail["partitionDetail"]["dataMax"] = starvMaxData;
            if (!$.isEmptyObject(starvMinData))
                partitionDetail["partitionDetail"]["dataMin"] = starvMinData;
            partitionDetail["partitionDetail"]["timeStamp"] = timeStamp;
        };


        var getClusterDetails = function (connection, clusterDetails, processName) {
            var suffix = "";
            suffix = "_" + processName;

            if (connection.Metadata['@SystemInformation_OVERVIEW' + suffix] == null) {
                return;
            }
            connection.Metadata['@SystemInformation_OVERVIEW' + suffix].data.forEach(function (info) {
                var singleData = info;
                var id = singleData[0];

                if (!clusterDetails.hasOwnProperty(id)) {
                    clusterDetails[id] = {};
                }
                if ($.inArray('CLUSTERSTATE', info) > 0) {
                    clusterDetails[id][singleData[1]] = singleData[2];
                } else if ($.inArray('HOSTNAME', info) > 0) {
                    clusterDetails[id][singleData[1]] = singleData[2];
                }
            });
        };

        var getClusterState = function (clusterDetails, clusterState) {
            var currentServer = getCurrentServer();
            $.each(clusterDetails, function (key, val) {
                if (val["HOSTNAME"] == currentServer) {
                    if (!clusterState.hasOwnProperty('CLUSTERSTATE')) {
                        clusterState['CLUSTERSTATE'] = {};
                    }
                    clusterState['CLUSTERSTATE'] = val["CLUSTERSTATE"];
                }
            });
        };

        var getTransactionDetails = function (connection, sysTransaction) {
            var colIndex = {};
            var counter = 0;
            var currentTimerTick = 0;
            var procStats = {};

            if (connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'] == null) {
                return;
            }
            if (connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].schema != null) {
                connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].schema.forEach(function (columnInfo) {
                    colIndex[columnInfo["name"]] = counter;
                    counter++;
                });
            }

            var dataCount = 0;
            if(jQuery.isEmptyObject(connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].data) && voltDbRenderer.memoryDetails.length != 0){
                sysTransaction["TimeStamp"] = voltDbRenderer.memoryDetails[voltDbRenderer.memoryDetails.length - 1]
                currentTimerTick =sysTransaction["TimeStamp"];
              }
            else{
                connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].data.forEach(function (table) {
                var srcData = table;
                var data = null;
                currentTimerTick = srcData[colIndex["TIMESTAMP"]];
                if (srcData[colIndex["PROCEDURE"]] in procStats) {
                    data = procStats[srcData[colIndex["PROCEDURE"]]];
                    data[1] = srcData[colIndex["INVOCATIONS"]];
                    data[2] = srcData[colIndex["WEIGHTED_PERC"]];
                    data[3] = srcData[colIndex["MIN"]];
                    data[4] = srcData[colIndex["AVG"]];
                    data[5] = srcData[colIndex["MAX"]];
                } else {
                    data = [srcData[colIndex["PROCEDURE"]], srcData[colIndex["INVOCATIONS"]], srcData[colIndex["WEIGHTED_PERC"]], srcData[colIndex["MIN"]], srcData[colIndex["AVG"]], srcData[colIndex["MAX"]]];
                }
                procStats[srcData[colIndex["PROCEDURE"]]] = data;
                if (dataCount == connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].data.length - 1) {
                    sysTransaction["TimeStamp"] = srcData[colIndex["TIMESTAMP"]];
                }
                dataCount++;
            });
            }

            var currentTimedTransactionCount = 0.0;
            for (var proc in procStats) {
                currentTimedTransactionCount += procStats[proc][1];
            }
            sysTransaction["CurrentTimedTransactionCount"] = currentTimedTransactionCount;
            sysTransaction["currentTimerTick"] = currentTimerTick;

        };

        var getPortAndOverviewDetails = function () {
            var portConfigValues = [];
            var currentServer = getCurrentServer();
            var httpPort = VoltDBConfig.GetPortId();

            $.each(systemOverview, function (key, val) {
                if (val["HOSTNAME"] == currentServer && val["HTTPPORT"] == httpPort) {
                    portConfigValues['adminPort'] = val["ADMINPORT"];
                    portConfigValues['httpPort'] = val["HTTPPORT"];
                    portConfigValues['clientPort'] = val["CLIENTPORT"];
                    portConfigValues['internalPort'] = val["INTERNALPORT"];
                    portConfigValues['zookeeperPort'] = val["ZKPORT"];
                    portConfigValues['replicationPort'] = val["DRPORT"];
                    portConfigValues['clusterState'] = val["CLUSTERSTATE"];
                    portConfigValues['replicationRole'] = val["REPLICATIONROLE"];

                    if (validateServerSpecificSettings(val)) {
                        portConfigValues['adminInterface'] = val['ADMININTERFACE'];
                        portConfigValues['httpInterface'] = val['HTTPINTERFACE'];
                        portConfigValues['clientInterface'] = val['CLIENTINTERFACE'];
                        portConfigValues['internalInterface'] = val['INTERNALINTERFACE'];
                        portConfigValues['zookeeperInterface'] = val['ZKINTERFACE'];
                        portConfigValues['replicationInterface'] = val['DRINTERFACE'];
                        serverSettings = true;

                    } else {
                        serverSettings = false;
                        return false;
                    }

                }
                return true;
            });

            $.each(systemMemory, function (key, val) {
                if (val["HOSTNAME"] == currentServer) {
                    portConfigValues['maxJavaHeap'] = val["MAXJAVAHEAP"];
                    return false;
                }
                return true;
            });

            return portConfigValues;
        };

        var getDeploymentDetails = function (connection, countDetails) {
            var colIndex = {};
            var counter = 0;
            var hostCount = 0;
            var siteCount = 0;
            var commandLogStatus = false;
            if (connection.Metadata['@SystemInformation_DEPLOYMENT'] == null) {
                return;
            }

            connection.Metadata['@SystemInformation_DEPLOYMENT'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "PROPERTY" || columnInfo["name"] == "VALUE")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@SystemInformation_DEPLOYMENT'].data.forEach(function (info) {
                if (info[colIndex["PROPERTY"]] == "hostcount") {
                    hostCount = info[colIndex["VALUE"]];
                }
                if (info[colIndex["PROPERTY"]] == "sitesperhost") {
                    siteCount = info[colIndex["VALUE"]];
                }
                if (info[colIndex["PROPERTY"]] == "commandlogenabled") {
                    commandLogStatus = info[colIndex["VALUE"]];
                }
            });
            if (!countDetails.hasOwnProperty("DETAILS")) {
                countDetails["DETAILS"] = {};
            }
            countDetails["DETAILS"]["HOSTCOUNT"] = hostCount;
            countDetails["DETAILS"]["SITECOUNT"] = siteCount;
            countDetails["DETAILS"]["COMMANDLOGSTATUS"] = commandLogStatus;
        };

        var getCommandLogDetails = function (connection, cmdLogDetails) {
            var colIndex = {};
            var counter = 0;

            if (connection.Metadata['@Statistics_COMMANDLOG'] == null) {
                return;
            }

            connection.Metadata['@Statistics_COMMANDLOG'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "OUTSTANDING_TXNS" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "OUTSTANDING_BYTES" || columnInfo["name"] == "SEGMENT_COUNT" ||
                    columnInfo["name"] == "FSYNC_INTERVAL" || columnInfo["name"] == "IN_USE_SEGMENT_COUNT")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });


            connection.Metadata['@Statistics_COMMANDLOG'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!cmdLogDetails.hasOwnProperty(hostName)) {
                    cmdLogDetails[hostName] = {};
                }
                cmdLogDetails[hostName]["OUTSTANDING_TXNS"] = info[colIndex["OUTSTANDING_TXNS"]];
                cmdLogDetails[hostName]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
                cmdLogDetails[hostName]["OUTSTANDING_BYTES"] = info[colIndex["OUTSTANDING_BYTES"]];
                cmdLogDetails[hostName]["SEGMENT_COUNT"] = info[colIndex["SEGMENT_COUNT"]];
                cmdLogDetails[hostName]["FSYNC_INTERVAL"] = info[colIndex["FSYNC_INTERVAL"]];
                cmdLogDetails[hostName]["IN_USE_SEGMENT_COUNT"] = info[colIndex["IN_USE_SEGMENT_COUNT"]];
            });
        };

        var getSnapshotStatus = function (connection, snapshotDetails) {
            var colIndex = {};
            var counter = 0;

            if (connection.Metadata['@Statistics_SNAPSHOTSTATUS'] == null) {
                return;
            }

            connection.Metadata['@Statistics_SNAPSHOTSTATUS'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "TIMESTAMP" || columnInfo["name"] == "PATH" || columnInfo["name"] == "START_TIME" || columnInfo["name"] == "END_TIME")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@Statistics_SNAPSHOTSTATUS'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!snapshotDetails.hasOwnProperty(hostName)) {
                    snapshotDetails[hostName] = [];
                }
                var snapshot = {                    
                    "TIMESTAMP": info[colIndex["TIMESTAMP"]],
                    "PATH": info[colIndex["PATH"]],
                    "START_TIME": info[colIndex["START_TIME"]],
                    "END_TIME": info[colIndex["END_TIME"]]
                };
                snapshotDetails[hostName].push(snapshot);
            });
        };

        var validateServerSpecificSettings = function (overviewValues) {
            if (overviewValues['ADMININTERFACE'] == "" && overviewValues['HTTPINTERFACE'] == "" &&
                overviewValues['CLIENTINTERFACE'] == "" && overviewValues['INTERNALINTERFACE'] == "" &&
                overviewValues['ZKINTERFACE'] == "" && overviewValues['DRINTERFACE'] == "") {
                return false;
            }

            else if (overviewValues['ADMININTERFACE'] != "" || overviewValues['HTTPINTERFACE'] != ""
                || overviewValues['CLIENTINTERFACE'] != "" || overviewValues['INTERNALINTERFACE'] != ""
                || overviewValues['ZKINTERFACE'] != "" || overviewValues['DRINTERFACE'] != "") {
                return true;
            }
            return false;
        };

        this.editConfigurationItem = function (configGroup, configMember, configValue, onConfigurationUpdated) {
            VoltDBService.editConfigurationItem(configGroup, configMember, configValue, function () {
                onConfigurationUpdated();
            });
        };

        var getAdminServerList = function () {
            var htmlServerListHtml = "";
            var serverDetails;
            var className;
            var currentServerRowClass;
            var currentServerColumnClass;
            var count = 0;

            this.setServerDetails = function (hostId, serverInfo, iteratorCount) {
                var count = 0;
                var stopperServerCount = 0;
                if ((VoltDbAdminConfig.servers != "" || VoltDbAdminConfig.servers != null || VoltDbAdminConfig.servers != undefined)
                    && VoltDbAdminConfig.servers.length > 0) {

                    $.each(VoltDbAdminConfig.servers, function (id, value) {
                        {
                            if (value.serverName != serverInfo['HOSTNAME'] && count == VoltDbAdminConfig.servers.length - 1) {
                                serverDetails = new VoltDbAdminConfig.server(hostId, serverInfo['HOSTNAME'], serverInfo['CLUSTERSTATE'],serverInfo['IPADDRESS']);
                                VoltDbAdminConfig.servers[iteratorCount] = serverDetails;

                                $.each(VoltDbAdminConfig.stoppedServers, function (key, val) {
                                    if (val.HOSTNAME == value.serverName) {
                                        //remove server from the stopped server list if server stopped while ago is already in running state
                                        VoltDbAdminConfig.stoppedServers.splice(stopperServerCount, 1);
                                    }
                                    stopperServerCount++;
                                });

                            }
                            else if (value.serverName == serverInfo['HOSTNAME']) {
                                value.hostId = hostId;
                                value.serverState = serverInfo['CLUSTERSTATE'];
                                return false;
                            }
                            count++;
                        }

                    });

                } else {
                    serverDetails = new VoltDbAdminConfig.server(hostId, serverInfo['HOSTNAME'], serverInfo['CLUSTERSTATE'], serverInfo['IPADDRESS']);
                    VoltDbAdminConfig.servers[count] = serverDetails;

                }
            };

            this.updateServers = function (hostId, hostName, serverState) {
                if ((VoltDbAdminConfig.servers != "" || VoltDbAdminConfig.servers != null || VoltDbAdminConfig.servers != undefined)
                    && VoltDbAdminConfig.servers.length > 0) {

                    $.each(VoltDbAdminConfig.servers, function (id, value) {
                        if (value.serverName == hostName) {
                            value.hostId = hostId;
                            value.serverState = serverState;
                            return false;
                        }
                    });
                }
            };

            var updateAdminServerList = function () {
                VoltDbAdminConfig.runningServerIds = "";
                var runningServerCounter = 0;

                $.each(VoltDbAdminConfig.servers, function (id, value) {
                    if (voltDbRenderer.currentHost != value.serverName) {
                        if (value.serverState == "RUNNING") {
                            if (runningServerCounter == 0)
                                VoltDbAdminConfig.runningServerIds = VoltDbAdminConfig.runningServerIds.concat("#stopServer_", value.serverName);
                            else {
                                VoltDbAdminConfig.runningServerIds = VoltDbAdminConfig.runningServerIds.concat(",", "#stopServer_", value.serverName);
                            }
                            runningServerCounter++;
                        }

                    }

                });

            };
            if (adminClusterObjects.ignoreServerListUpdateCount > 0) {
                adminClusterObjects.ignoreServerListUpdateCount--;

            } else {
                if (systemOverview != null || systemOverview != undefined) {
                    VoltDbAdminConfig.servers = [];
                    $.each(systemOverview, function (id, val) {
                        setServerDetails(val.NODEID, val, count);
                        count++;
                    });

                    $.each(VoltDbAdminConfig.stoppedServers, function (id, val) {
                        setServerDetails(val.HOSTID, val, count);
                    });
                }

            }

            if (VoltDbAdminConfig.servers != null || VoltDbAdminConfig.servers != undefined) {
                $.each(VoltDbAdminConfig.servers, function (id, val) {
                    if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == 'RUNNING') {
                        className = voltDbRenderer.currentHost == val.serverName ? "disableServer" : "shutdown";
                        currentServerRowClass = voltDbRenderer.currentHost == val.serverName ? "activeHostMonitoring" : "activeHost";
                        currentServerColumnClass = voltDbRenderer.currentHost == val.serverName ? "shutdownServer stopDisable" : "shutdownServer";

                        htmlServerListHtml = htmlServerListHtml.concat("<tr class=\"" + currentServerRowClass + "\"><td class=\"configLabel\" width=\"40%\"><a class=\"serNameTruncate\" href=\"#\" >" + val.serverName + "</a></td>" +
                            "<td  align='center' >" + val.ipAddress + "</td>" +
                            "<td align=\"right\"><a href=\"javascript:void(0);\" data-HostId=\"" + val.hostId + "\" data-HostName=\"" + val.serverName + "\" class=\"" + className + "\" id=\"stopServer_" + val.serverName + "\">" +
                            "<span class=\"" + currentServerColumnClass + "\">Stop</span></a></td></tr>");

                    } else if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == 'PAUSED') {
                        className = voltDbRenderer.currentHost == val.serverName ? "disableServer" : "shutdown";
                        currentServerRowClass = voltDbRenderer.currentHost == val.serverName ? "activeHostMonitoring" : "activeHost";
                        currentServerColumnClass = "shutdownServerPause";

                        htmlServerListHtml = htmlServerListHtml.concat("<tr class=\"" + currentServerRowClass + "\"><td class=\"configLabel\" width=\"40%\"><a class=\"serNameTruncate\" href=\"#\" >" + val.serverName + "</a></td>" +
                            "<td  align='center' >" + val.ipAddress + "</td>" +
                            "<td align=\"right\" class=\"pauseCursorDefault\"><a href=\"javascript:void(0);\" data-HostId=\"" + val.hostId + "\" data-HostName=\"" + val.serverName + "\"class=\"resume\" id=\"stopServer_" + val.serverName + "\">" +
                            "<span class=\"" + currentServerColumnClass + "\">Paused</span></a></td></tr>");


                    } else if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == 'JOINING') {
                        htmlServerListHtml = htmlServerListHtml.concat("<tr><td class=\"configLabel\" width=\"40%\"><a class=\"serNameTruncate\" href=\"#\">" + val.serverName + "</a></td>" +
                            "<td  align='center' >" + val.ipAddress + "</td>" +
                            "<td align=\"right\"><a href=\"javascript:void(0);\" class=\"shutdownDisabled\">" +
                            "<span>Stop</span></a></td></tr>");
                    } else if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) || val.serverState == 'MISSING') {
                        htmlServerListHtml = htmlServerListHtml.concat("<tr><td class=\"configLabel\" width=\"40%\"><a class=\"serNameTruncate\" href=\"#\">" + val.serverName + "</a></td>" +
                            "<td  align='center' >" + val.ipAddress + "</td>" +
                            "<td align=\"right\"><a href=\"javascript:void(0);\" data-HostId=\"" + val.hostId + "\" data-HostName=\"" + val.serverName + "\" class=\"disableServer\"  id=\"stopServer_" + val.serverName + "\ onclick=\"VoltDbUI.openPopup(this);\">" +
                            "<span class=\"shutdownServer stopDisable\">Stop</span></a></td></tr>");
                    }

                });

                updateAdminServerList();
                return htmlServerListHtml;

            }
            return "";

        };

        this.stopServer = function (nodeId, hostNameValue, onServerStopped) {
            VoltDBService.stopServerNode(nodeId, function (connection, status, statusString) {
                if (status == 1) {
                    VoltDbAdminConfig.stoppedServers[VoltDbAdminConfig.stoppedServers.length] = new VoltDbAdminConfig.stoppedServer(nodeId, hostNameValue);
                    onServerStopped(true, statusString);
                } else {
                    onServerStopped(false, statusString);
                }
            });
        };

        this.pauseCluster = function (onServerPaused) {
            VoltDBService.PauseClusterState(function (connection, status) {
                if (status == 1) {
                    onServerPaused(true);
                }
            });
        };

        this.resumeCluster = function (onServerResumed) {
            VoltDBService.ResumeClusterState(function (connection, status) {
                if (status == 1) {
                    onServerResumed(true);
                }
            });
        };

        this.shutdownCluster = function (onServerShutdown, zk_pause_txn_id) {
            VoltDBService.ShutdownClusterState(function (connection, status) {
                if (status == 1) {
                    onServerShutdown(true);
                }
            }, zk_pause_txn_id);
        };

        this.prepareShutdownCluster = function (onPrepareServerShutdown) {
            VoltDBService.PrepareShutdownCluster(function (connection) {
                var prepare_status = {}
                var status = -1;
                var zk_pause_txn_id = '0'
                var data = connection.Metadata['@PrepareShutdown_data']
                if(data != undefined){
                    zk_pause_txn_id = connection.Metadata['@PrepareShutdown_data']['data'][0][0]
                    status = parseInt(connection.Metadata['@PrepareShutdown_status'])
                }
                prepare_status.zk_pause_txn_id = zk_pause_txn_id;
                prepare_status.status = status;
                onPrepareServerShutdown(prepare_status);
            });
        };

        this.QuiesceCluster = function (onPrepareServerQuiesce) {
            VoltDBService.QuiesceCluster(function (connection, status) {
                    var data = connection.Metadata['@Quiesce_data']
                    if(data == undefined)
                        status = -1;
                    else
                        status = connection.Metadata['@Quiesce_data']['data'][0][0]
                    onPrepareServerQuiesce(status);
            });
        };

        this.GetDrProducerInformation = function (onInformationLoaded, drDetails) {
            VoltDBService.GetDrProducerInformation(function (connection) {
                getDrProducerInfo(connection, drDetails);
                onInformationLoaded(drDetails);
            });
        };

        this.GetExportTablesInformation = function (onInformationLoaded, tableDetails) {
            VoltDBService.GetExportTablesInformation(function (connection) {
                getExportTableInfo(connection, tableDetails);
                onInformationLoaded(tableDetails);
            });
        };

        this.GetImportRequestInformation = function (onInformationLoaded, tableDetails) {
            VoltDBService.GetImportRequestInformation(function (connection) {
                var data = connection.Metadata['@Statistics_IMPORTER']
                if(data == undefined || $.isEmptyObject(data['data']))
                    outstanding = 0;
                else
                    outstanding = connection.Metadata['@Statistics_IMPORTER']['data'][0][0]
                onInformationLoaded(outstanding);
            });
        };

        this.saveSnapshot = function (snapshotDir, snapshotFileName, onSaveSnapshot) {
            VoltDBService.SaveSnapShot(snapshotDir, snapshotFileName, function (connection, status) {
                var snapshotStatus = {};
                if (status == 1 || status == -2) {
                    voltDbRenderer.getSaveSnapshotStatus(connection, snapshotStatus, status);
                    onSaveSnapshot(true, snapshotStatus);
                }
            });
        };

        this.restoreSnapShot = function (snapshotDir, snapshotFileName, onSaveSnapshot) {
            VoltDBService.RestoreSnapShot(snapshotDir, snapshotFileName, function (connection, status, statusString) {
                var snapshotStatus = {};
                if (status == 1) {
                    voltDbRenderer.getRestoreSnapshotStatus(connection, snapshotStatus);
                    onSaveSnapshot(true, snapshotStatus, statusString);
                } else {
                    onSaveSnapshot(false, null, statusString);
                }
            });
        };

        this.GetSnapshotList = function (snapshotDirectory, onInformationLoaded) {
            var snapshotList = {};
            VoltDBService.GetSnapshotList(snapshotDirectory, function (connection) {
                getSnapshotDetails(connection, snapshotList, snapshotDirectory);
                onInformationLoaded(snapshotList);
            });
        };

        var getSnapshotDetails = function (connection, snapshotList, snapshotDirectory) {
            var colIndex = {};
            var counter = 0;

            if (connection.Metadata['@SnapshotScan_data'][0] == undefined) {

                if (connection.Metadata['@SnapshotScan_status'] == -2) {
                    snapshotList[0] = {};
                    snapshotList[0]["RESULT"] = "FAILURE";
                    snapshotList[0]["ERR_MSG"] = connection.Metadata['@SnapshotScan_statusstring'] != null ? connection.Metadata['@SnapshotScan_statusstring'] : "";
                }

                return;
            }

            if (connection.Metadata['@SnapshotScan_data'][0].data.length != 0)
                connection.Metadata['@SnapshotScan_data'] = connection.Metadata['@SnapshotScan_data'][0];
            else
                connection.Metadata['@SnapshotScan_data'] = connection.Metadata['@SnapshotScan_data'][1];

            connection.Metadata['@SnapshotScan_data'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "PATH" || columnInfo["name"] == "NONCE" || columnInfo["name"] == "RESULT" || columnInfo["name"] == "ERR_MSG")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });
            var count = 0;

            connection.Metadata['@SnapshotScan_data'].data.forEach(function (info) {
                if (!snapshotList.hasOwnProperty(count)) {
                    snapshotList[count] = {};
                }
                snapshotList[count]["PATH"] = info[colIndex["PATH"]];
                snapshotList[count]["NONCE"] = info[colIndex["NONCE"]];
                snapshotList[count]["RESULT"] = info[colIndex["RESULT"]];
                snapshotList[count]["ERR_MSG"] = info[colIndex["ERR_MSG"]];
                count++;
            });
        };

        this.getSaveSnapshotStatus = function (connection, snapshotStatus, saveStatus) {
            var colIndex = {};
            var counter = 0;

            //Handle error for community edition of VoltDB.
            if (saveStatus == -2) {
                var currentServer = getCurrentServer();
                snapshotStatus[currentServer] = {};

                snapshotStatus[currentServer]["RESULT"] = "Failure";
                snapshotStatus[currentServer]["ERR_MSG"] = connection.Metadata['@SnapshotSave_statusstring'] != undefined ? connection.Metadata['@SnapshotSave_statusstring'] : "";

                return;
            }

            connection.Metadata['@SnapshotSave_data'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "RESULT" || columnInfo["name"] == "ERR_MSG")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@SnapshotSave_data'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!snapshotStatus.hasOwnProperty(hostName)) {
                    snapshotStatus[hostName] = {};
                }
                snapshotStatus[hostName]["RESULT"] = info[colIndex["RESULT"]];
                snapshotStatus[hostName]["ERR_MSG"] = info[colIndex["ERR_MSG"]];
            });
        };

        this.promoteCluster = function (onClusterPromote) {
            VoltDBService.PromoteCluster(function (connection, status, statusstring) {
                onClusterPromote(status, statusstring);
            });
        };

        this.getRestoreSnapshotStatus = function (connection, snapshotStatus) {
            var colIndex = {};
            var counter = 0;

            connection.Metadata['@SnapshotRestore_data'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "HOSTNAME" || columnInfo["name"] == "RESULT" || columnInfo["name"] == "ERR_MSG")
                    colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            connection.Metadata['@SnapshotRestore_data'].data.forEach(function (info) {
                var hostName = info[colIndex["HOSTNAME"]];
                if (!snapshotStatus.hasOwnProperty(hostName)) {
                    snapshotStatus[hostName] = {};
                }
                snapshotStatus[hostName]["RESULT"] = info[colIndex["RESULT"]];
                snapshotStatus[hostName]["ERR_MSG"] = info[colIndex["ERR_MSG"]];
            });
        };

        this.getAdminconfiguration = function (onInformationLoaded) {
            VoltDBService.GetSystemInformationDeployment(function (connection) {
                onInformationLoaded(connection);
            });
        };


        this.updateAdminConfiguration = function (updatedData, onInformationLoaded) {
            VoltDBService.UpdateAdminConfiguration(updatedData, function (connection) {
                var result = {};

                if (connection != null && connection.Metadata['SHORTAPI_UPDATEDEPLOYMENT'] != null) {
                    result = connection.Metadata['SHORTAPI_UPDATEDEPLOYMENT'];
                }

                onInformationLoaded(result);
            });
        };

        //end admin configuration

        this.UpdateUserConfiguration = function (updatedData, onInformationLoaded, userId, requestType) {
            VoltDBService.UpdateUserConfiguration(updatedData, function (connection) {
                var result = {};

                if (connection != null && connection.Metadata['SHORTAPI_USERUPDATEDEPLOYMENT'] != null) {
                    result = connection.Metadata['SHORTAPI_USERUPDATEDEPLOYMENT'];
                }

                onInformationLoaded(result);
            }, userId, requestType);
        };


        function getTableData(connection, tablesData, viewsData, proceduresData, procedureColumnsData, sysProceduresData, processName, exportTableData) {
            exportTableData = exportTableData ==  undefined ? {} : exportTableData
            var suffix = "";
            if (processName == "TABLE_INFORMATION" || processName == "TABLE_INFORMATION_CLIENTPORT") {
                suffix = "_" + processName;
            }

            var rawTables = connection.Metadata['@Statistics_TABLE' + suffix].data;
            var rawIndexes = connection.Metadata['@Statistics_INDEX' + suffix].data;
            var rawColumns = connection.Metadata['@SystemCatalog_COLUMNS' + suffix].data;
            var procedures = connection.Metadata['@SystemCatalog_PROCEDURES' + suffix].data;
            var procedureColumns = connection.Metadata['@SystemCatalog_PROCEDURECOLUMNS' + suffix].data;

            var tables = [];
            var exports = [];
            var views = [];

            for (var k = 0; k < rawTables.length; k++) {
                var tableName = rawTables[k][5];
                var isView = false;
                var item = { name: tableName, key: null, indexes: null, columns: null };
                for (var j = 0; j < rawIndexes.length; j++) {
                    if (rawIndexes[j][6].toUpperCase() == tableName.toUpperCase()) {
                        var indexName = rawIndexes[j][5];
                        if (item.indexes == null)
                            item.indexes = [];
                        item.indexes[indexName] = indexName + ' (' + ((rawIndexes[j][7].toLowerCase().indexOf('hash') > -1) ? 'Hash' : 'Tree') + (rawIndexes[j][8] == "1" ? ', Unique' : '') + ')';
                        if (indexName.toUpperCase().indexOf("MATVIEW") > -1)
                            isView = true;
                        if (indexName.toUpperCase().indexOf("PK_") > -1)
                            item.key = indexName;
                    }
                }
                if (isView)
                    views[tableName] = item;
                else if (rawTables[k][6] == 'StreamedTable')
                    exports[tableName] = item;
                else
                    tables[tableName] = item;
            }

            connection.Metadata['tables'] = tables;
            connection.Metadata['views'] = views;
            connection.Metadata['exports'] = exports;
            for (var i = 0; i < rawColumns.length; i++) {
                var TableName = rawColumns[i][2].toUpperCase();
                if (connection.Metadata['tables'][TableName] != null) {
                    if (connection.Metadata['tables'][TableName].columns == null) {
                        connection.Metadata['tables'][TableName].columns = [];
                    }
                    connection.Metadata['tables'][TableName].columns[rawColumns[i][16]] =
                        rawColumns[i][3].toUpperCase() +
                        ' (' + rawColumns[i][5].toLowerCase() + ')';
                }
                else if (connection.Metadata['exports'][TableName] != null) {
                    if (connection.Metadata['exports'][TableName].columns == null) {
                        connection.Metadata['exports'][TableName].columns = [];
                    }
                    connection.Metadata['exports'][TableName].columns[rawColumns[i][16]] =
                        rawColumns[i][3].toUpperCase() +
                        ' (' + rawColumns[i][5].toLowerCase() + ')';
                }
                else if (connection.Metadata['views'][TableName] != null) {
                    if (connection.Metadata['views'][TableName].columns == null) {
                        connection.Metadata['views'][TableName].columns = [];
                    }
                    connection.Metadata['views'][TableName].columns[rawColumns[i][3].toUpperCase()] =
                        rawColumns[i][3].toUpperCase() +
                        ' (' + rawColumns[i][5].toLowerCase() + ')';
                }
            }

            // User Procedures
            for (var i = 0; i < procedures.length; ++i) {
                var connTypeParams = [];
                var procParams = [];
                var procName = procedures[i][2];
                for (var p = 0; p < procedureColumns.length; ++p) {
                    if (procedureColumns[p][2] == procName) {
                        paramType = procedureColumns[p][6];
                        paramName = procedureColumns[p][3];
                        paramOrder = procedureColumns[p][17] - 1;
                        procParams[paramOrder] = { 'name': paramName, 'type': paramType.toLowerCase() };
                    }
                }

                for (var p = 0; p < procParams.length; ++p) {
                    connTypeParams[connTypeParams.length] = procParams[p].type;
                }

                // make the procedure callable.
                connection.procedures[procName] = {};
                connection.procedures[procName]['' + connTypeParams.length] = connTypeParams;
            }

            if (!tablesData.hasOwnProperty('tables')) {
                tablesData['tables'] = {};
            }
            if (!viewsData.hasOwnProperty('views')) {
                viewsData['views'] = {};
            }
            if (!proceduresData.hasOwnProperty('procedures')) {
                proceduresData['procedures'] = {};
            }
            if (!procedureColumnsData.hasOwnProperty('procedureColumns')) {
                procedureColumnsData['procedureColumns'] = {};
            }
            if (!sysProceduresData.hasOwnProperty('sysProcedures')) {
                sysProceduresData['sysProcedures'] = {};
            }

            tablesData['tables'] = connection.Metadata['tables'];
            viewsData['views'] = connection.Metadata['views'];
            proceduresData['procedures'] = procedures;
            procedureColumnsData['procedureColumns'] = procedureColumns;
            sysProceduresData['sysProcedures'] = connection.Metadata['sysprocs'];
            exportTableData['exportTables'] = connection.Metadata['exports']
        }


        //common methods
        var formatTableNoData = function (listName) {
            if (listName == "PROCEDURE") {
                lblPrevious.innerHTML = " ".concat(0, ' ');
                lblTotalPages.innerHTML = " ".concat(0);
                $('#storeProcedureBody').html("<tr><td colspan=6>No data to be displayed</td></tr>");

            } else if (listName == "TABLE") {
                lblPreviousTable.innerHTML = " ".concat(0, ' ');
                lblTotalPagesofTables.innerHTML = " ".concat(0);
                $('#tablesBody').html("<tr><td colspan=6>No data to be displayed</td></tr>");
            }

        };

        var formatTableData = function (connection) {
            var i = 0;
            var tableMetadata = [];
            var totalTupleCount = 0;
            var partitionEntryCount = 0;
            var duplicatePartition = false;
            var tupleCountPartitions = [];
            var partitionData = {};
            var averageRowCount = 0;

            if (voltDbRenderer.refreshTables) {
                if (connection.Metadata["@Statistics_TABLE"] != undefined || connection.Metadata["@Statistics_TABLE"] != null) {
                    if (connection.Metadata["@Statistics_TABLE"].data != "" &&
                        connection.Metadata["@Statistics_TABLE"].data != [] &&
                        connection.Metadata["@Statistics_TABLE"].data != undefined) {

                        tableMetadata = connection.Metadata["@Statistics_TABLE"].data;
                        tableData = {};

                        $.each(tableMetadata, function (key, tupleData) {
                            duplicatePartition = false;
                            if (tupleData != undefined) {
                                partitionEntryCount = 0;

                                if (!partitionData.hasOwnProperty(tupleData[tableNameIndex])) {
                                    partitionData[tupleData[tableNameIndex]] = [];
                                    partitionData[tupleData[tableNameIndex]].push(tupleData);

                                } else {
                                    $.each(partitionData[tupleData[tableNameIndex]], function (nestKey, nestData) {
                                        for (i = 0; i < partitionData[tupleData[tableNameIndex]].length; i++) {
                                            partitionEntryCount++;
                                            //if partition is repeated for a given table in "partitionData"
                                            if (tupleData[partitionIndex] == partitionData[tupleData[tableNameIndex]][i][partitionIndex]) {
                                                duplicatePartition = true;
                                                return false;
                                            }

                                        }
                                        if (partitionEntryCount == partitionData[tupleData[tableNameIndex]].length && !duplicatePartition) {
                                            partitionData[tupleData[tableNameIndex]].push(tupleData);
                                            return false;

                                        }
                                    });
                                }
                            }
                        });

                        //formulate max, min, average for each table
                        $.each(partitionData, function (key, data) {
                            totalTupleCount = 0;

                            if (!tableData.hasOwnProperty(key)) {
                                tableData[key] = {};
                            }

                            for (i = 0; i < data.length; i++) {
                                totalTupleCount += parseInt(data[i][tupleCountIndex]);
                                tupleCountPartitions[i] = data[i][tupleCountIndex];
                            }


                            tableData[key] = {
                                "TABLE_NAME": key,
                                "MAX_ROWS": Math.max.apply(null, tupleCountPartitions),
                                "MIN_ROWS": Math.min.apply(null, tupleCountPartitions),
                                "AVG_ROWS": getAverage(tupleCountPartitions),
                                "TUPLE_COUNT": schemaCatalogTableTypes[key].REMARKS == "REPLICATED" ? data[0][tupleCountIndex] : totalTupleCount,
                                "TABLE_TYPE": schemaCatalogTableTypes[key].REMARKS,
                                "drEnabled": schemaCatalogTableTypes[key].drEnabled,
                                "TABLE_TYPE1": schemaCatalogTableTypes[key].TABLE_TYPE
                            };

                        });
                    }
                    else {
                        formatTableNoData("TABLE");

                    }

                }
            }

        };

        var mapJsonArrayToProcedures = function () {
            var i = 0;
            var procedureName;
            procedureData = [];
            var procedure = {};

            if (procedureJsonArray != undefined) {
                for (i = 0; i < procedureJsonArray.length; i++) {
                    procedureName = procedureJsonArray[i].PROCEDURE;
                    if (!procedureData.hasOwnProperty(procedureName)) {
                        procedure = {
                            'PROCEDURE': procedureJsonArray[i].PROCEDURE,
                            'INVOCATIONS': procedureJsonArray[i].INVOCATIONS,
                            'MIN_LATENCY': procedureJsonArray[i].MIN_LATENCY,
                            'MAX_LATENCY': procedureJsonArray[i].MAX_LATENCY,
                            'AVG_LATENCY': procedureJsonArray[i].AVG_LATENCY,
                            'PERC_EXECUTION': procedureJsonArray[i].PERC_EXECUTION
                        };
                        procedureData.push(procedure);
                    }
                }
            }

        };

        var mapJsonArrayToSearchedProcedures = function () {
            var i = 0;
            var procedureName;
            lSearchData['procedures'] = [];
            var searchTuple = {};

            if (procedureSearchJsonArray != undefined) {
                for (i = 0; i < procedureSearchJsonArray.length; i++) {
                    procedureName = procedureSearchJsonArray[i].PROCEDURE;
                    if (!lSearchData.hasOwnProperty(procedureName)) {
                        searchTuple = {};
                        searchTuple['PROCEDURE'] = procedureSearchJsonArray[i].PROCEDURE;
                        searchTuple['INVOCATIONS'] = procedureSearchJsonArray[i].INVOCATIONS;
                        searchTuple['MIN_LATENCY'] = procedureSearchJsonArray[i].MIN_LATENCY;
                        searchTuple['MAX_LATENCY'] = procedureSearchJsonArray[i].MAX_LATENCY;
                        searchTuple['AVG_LATENCY'] = procedureSearchJsonArray[i].AVG_LATENCY;
                        searchTuple['PERC_EXECUTION'] = procedureSearchJsonArray[i].PERC_EXECUTION;

                        lSearchData['procedures'].push(searchTuple);

                    }
                }
            }
        };

        var setProcedureTupleHtml = function (val) {
            if (!$.isEmptyObject(val)) {
                if (htmlMarkup == "") {
                    htmlMarkup = "<tr><td>" + val['PROCEDURE'] + "</td>" +
                        "<td class=\"txt-center\">" + val['INVOCATIONS'] + "</td>" +
                        "<td class=\"txt-center\">" + val['MIN_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['MAX_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['AVG_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['PERC_EXECUTION'] + "</td></tr>";

                } else {

                    htmlMarkup += "<tr><td>" + val['PROCEDURE'] + "</td>" +
                        "<td class=\"txt-center\">" + val['INVOCATIONS'] + "</td>" +
                        "<td class=\"txt-center\">" + val['MIN_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['MAX_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['AVG_LATENCY'] + "</td>" +
                        "<td class=\"txt-center\">" + val['PERC_EXECUTION'] + "</td></tr>";
                }
            }
        };

        var mapJsonArrayToTables = function () {
            var i = 0;
            var tableName = "";
            tableData = {};
            if (tableJsonArray != undefined) {
                for (i = 0; i < tableJsonArray.length; i++) {
                    if (i > 0) {
                        if (tableJsonArray[i].TABLE_NAME != tableJsonArray[i - 1].TABLE_NAME) {
                            tableName = tableJsonArray[i].TABLE_NAME;
                        }
                    }
                    if (!tableData.hasOwnProperty(tableName)) {
                        tableName = tableJsonArray[i].TABLE_NAME;
                        tableData[tableName] = {};
                    }

                    tableData[tableName] = {
                        "TABLE_NAME": tableName,
                        "MAX_ROWS": tableJsonArray[i].MAX_ROWS,
                        "MIN_ROWS": tableJsonArray[i].MIN_ROWS,
                        "AVG_ROWS": tableJsonArray[i].AVG_ROWS,
                        "TUPLE_COUNT": tableJsonArray[i].TUPLE_COUNT,
                        "TABLE_TYPE": tableJsonArray[i].TABLE_TYPE
                    };
                }
            }

        };

        var mapJsonArrayToSearchedTables = function () {
            var i = 0;
            var counter = 0;
            var tableName = "";
            lSearchData.tables = {};

            if (tableSearchJsonArray != undefined) {
                for (i = 0; i < tableSearchJsonArray.length; i++) {
                    if (i > 0) {
                        if (tableSearchJsonArray[i].TABLE_NAME != tableSearchJsonArray[i - 1].TABLE_NAME) {
                            tableName = tableSearchJsonArray[i].TABLE_NAME;
                        }
                    }
                    if (!lSearchData.tables.hasOwnProperty(tableName)) {
                        tableName = tableSearchJsonArray[i].TABLE_NAME;
                        lSearchData.tables[tableName] = {};
                        counter = 0;
                    }

                    lSearchData.tables[tableName] = {
                        "TABLE_NAME": tableName,
                        "MAX_ROWS": tableSearchJsonArray[i].MAX_ROWS,
                        "MIN_ROWS": tableSearchJsonArray[i].MIN_ROWS,
                        "AVG_ROWS": tableSearchJsonArray[i].AVG_ROWS,
                        "TUPLE_COUNT": tableSearchJsonArray[i].TUPLE_COUNT,
                        "TABLE_TYPE": tableSearchJsonArray[i].TABLE_TYPE
                    };
                    counter++;

                }
            }
        };

        var setTableTupleDataHtml = function (tuple, tableName) {
            if (htmlTableMarkup == undefined || htmlTableMarkup == "") {
                htmlTableMarkup = "<tr><td>" + tableName + "</td>" +
                         "<td class=\"txt-center\">" + tuple['TUPLE_COUNT'] + "</td>" +
                         "<td class=\"txt-center\">" + tuple['MAX_ROWS'] + "</td>" +
                         "<td class=\"txt-center\">" + tuple['MIN_ROWS'] + "</td>" +
                         "<td class=\"txt-center\">" + tuple['AVG_ROWS'] + "</td>" +
                         "<td class=\"txt-center\">" + tuple['TABLE_TYPE'] + "</td></tr>";
            } else {
                htmlTableMarkup += "<tr><td>" + tableName + "</td>" +
                        "<td class=\"txt-center\">" + tuple['TUPLE_COUNT'] + "</td>" +
                        "<td class=\"txt-center\">" + tuple['MAX_ROWS'] + "</td>" +
                        "<td class=\"txt-center\">" + tuple['MIN_ROWS'] + "</td>" +
                       "<td class=\"txt-center\">" + tuple['AVG_ROWS'] + "</td>" +
                       "<td class=\"txt-center\">" + tuple['TABLE_TYPE'] + "</td></tr>";
            }
        };

        var getAverage = function (arrayData) {
            var i;
            var dataSum = 0;
            var average;
            if (arrayData != null) {
                for (i = 0; i < arrayData.length; i++) {
                    dataSum += parseInt(arrayData[i]);
                }
                average = Math.round(dataSum / arrayData.length, 2);
                return average;

            }
            return 0;
        };

        var ascendingSortJSON = function (data, key) {
            return data.sort(function (a, b) {
                var x = a[key]; var y = b[key];
                return ((x < y) ? -1 : ((x > y) ? 1 : 0));
            });
        };

        var descendingSortJSON = function (data, key) {
            return data.sort(function (a, b) {
                var x = a[key]; var y = b[key];
                return ((x > y) ? -1 : ((x < y) ? 1 : 0));
            });
        };

        var checkIfDuplicateJson = function (jsonArray, keyValue) {
            var i = 0;
            var isDuplicate = false;
            for (i = 0; i < jsonArray.length; i++) {
                if (jsonArray[i].PROCEDURE == keyValue) {
                    isDuplicate = true;
                    break;

                }
            }
            return isDuplicate;
        };

        var checkSortColumnSortable = function () {
            var isSearchable = 0;
            if (voltDbRenderer.isSortProcedures) {
                if (voltDbRenderer.sortColumn == "TABLE_NAME")
                    isSearchable = true;

                else
                    isSearchable = false;

            }
            else if (voltDbRenderer.isSortTables) {
                if (voltDbRenderer.tableSortColumn == "TABLE_NAME")
                    isSearchable = true;

                else
                    isSearchable = false;
            }

            return isSearchable;
        };

        var getColumnTypes = function (tableName) {
            var columnType = "";
            $.each(schemaCatalogColumnTypes, function (key, typeVal) {
                if (tableName == typeVal['TABLE_NAME']) {
                    columnType = typeVal['REMARKS'];
                    return false;
                }
            });

            if (columnType == "PARTITION_COLUMN") {
                return columnType;
            } else {
                return columnType;
            }

        };

        //Search methods
        var lSearchData = this.searchData;
        this.searchProcedures = function (searchType, searchKey, onProcedureSearched) {
            var searchDataCount = 0;

            if (procedureData == null || procedureData == undefined) {
                return;
            }

            lSearchData['procedures'] = [];
            $.each(procedureData, function (nestKey, tupleData) {
                if (tupleData != undefined) {
                    if (tupleData.PROCEDURE.toLowerCase().indexOf(searchKey.toLowerCase()) >= 0) {
                        lSearchData['procedures'][searchDataCount] = tupleData;
                        searchDataCount++;

                    }

                }
            });

            this.procedureSearchDataSize = searchDataCount;
            onProcedureSearched(searchDataCount > 0);

        };

        this.searchTables = function (connection, searchKey, onTablesSearched) {
            var searchDataCount = 0;

            if (tableData == null || tableData == undefined) {
                return;
            }
            lSearchData.tables = {};

            $.each(tableData, function (nestKey, tupleData) {
                if (tupleData != undefined) {
                    if (nestKey.toLowerCase().indexOf(searchKey.toLowerCase()) >= 0) {
                        lSearchData.tables[nestKey] = tupleData;
                        searchDataCount++;

                    }
                }
            });

            if (searchDataCount == 0)
                lSearchData.tables = {};

            this.tableSearchDataSize = searchDataCount;
            onTablesSearched(searchDataCount > 0);
        };

        this.formatSearchDataToJsonArray = function (isSearched) {
            var searchProcedureCount = 0;
            procedureSearchJsonArray = [];

            function iterateSearchProcedureData() {
                $.each(lSearchData.procedures, function (key, data) {
                    minLatency = data.MIN_LATENCY * Math.pow(10, -6);
                    maxLatency = data.MAX_LATENCY * Math.pow(10, -6);
                    avgLatency = data.AVG_LATENCY * Math.pow(10, -6);

                    minLatency = parseFloat(minLatency.toFixed(2));
                    maxLatency = parseFloat(maxLatency.toFixed(2));
                    avgLatency = parseFloat(avgLatency.toFixed(2));

                    procedureSearchJsonArray[searchProcedureCount] = {
                        "PROCEDURE": data.PROCEDURE,
                        "INVOCATIONS": data.INVOCATIONS,
                        "MIN_LATENCY": data.MIN_LATENCY,
                        "MAX_LATENCY": data.MAX_LATENCY,
                        "AVG_LATENCY": data.AVG_LATENCY,
                        "PERC_EXECUTION": data.PERC_EXECUTION
                    };

                    searchProcedureCount++;
                });
            }

            if (isSearched) {
                if (lSearchData.procedures != "" || lSearchData.procedures != undefined) {
                    iterateSearchProcedureData();
                }

            } else {
                voltDbRenderer.searchProcedures(connection, $('#filterStoredProc')[0].value, function (searchResult) {
                    iterateSearchProcedureData();
                });
            }
        };

        this.formatSearchTablesDataToJsonArray = function (connection, searchKey, isSearched) {
            var searchTableCount = 0;
            tableSearchJsonArray = [];

            function iterateSearchTableData() {
                $.each(lSearchData.tables, function (nestKey, tupleData) {
                    if (tupleData != undefined) {
                        if (nestKey.toLowerCase().indexOf(searchKey.toLowerCase()) >= 0) {
                            tableSearchJsonArray[searchTableCount] = {
                                "TABLE_NAME": nestKey,
                                "MAX_ROWS": tupleData["MAX_ROWS"],
                                "MIN_ROWS": tupleData["MIN_ROWS"],
                                "AVG_ROWS": tupleData["AVG_ROWS"],
                                "TUPLE_COUNT": tupleData["TUPLE_COUNT"],
                                "TABLE_TYPE": schemaCatalogTableTypes[nestKey].REMARKS
                            };
                            searchTableCount++;

                        }
                    }

                });
            }

            if (isSearched) {
                if (lSearchData.tables != "" || lSearchData.tables != undefined) {
                    iterateSearchTableData();
                }

            } else {
                voltDbRenderer.searchTables(connection, $('#filterDatabaseTable')[0].value, function (searchResult) {
                    if (searchResult) {
                        iterateSearchTableData();
                    }

                });
            }

        };

    });
    window.voltDbRenderer = voltDbRenderer = new iVoltDbRenderer();

})(window);


//Navigation responsive	
$(function () {
    $('#toggleMenu').click(function () {
        $("#nav").slideToggle('slow');
        $("#nav").css('left', '0');
        $("#nav ul li").click(function () {
            $("#nav").css('display', 'none');
            $(window).resize();
        });
    });
});

$(window).resize(function () {
    var windowWidth = $(window).width();
    if (windowWidth > 699) {
        $("#nav").css('display', 'block');

    } else if (windowWidth < 699) {
        $("#nav").css('display', 'none');
    }

});

