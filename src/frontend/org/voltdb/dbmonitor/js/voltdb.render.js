
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
            urlArray2[0] = privateToPublicIP[serverIp];
            urlArray[2] = urlArray2.join(':');
            newUrl = urlArray.join('/');
        }

        var data = {
            CurrentServer: clickedServer,
            GraphView: $.cookie("graph-view"),
            DisplayPreferences: $.cookie("user-preferences"),
            AlertThreshold: $.cookie("alert-threshold"),
            username: $.cookie("username"),
            password: $.cookie("password"),
            admin: $.cookie("admin")
        };

        var win = window.open(newUrl + '?data=' + JSON.stringify(data), '_parent');
        win.focus();
    }

    $('.popup_close').trigger('click');
};

(function (window) {

    var iVoltDbRenderer = (function () {
        this.nodeStatus = new Array();
        this.isProcedureSearch = false;
        this.isTableSearch = false;
        this.userPreferences = {};
        this.procedureTableIndex = 0;
        this.procedureDataSize = 0;
        this.procedureSearchDataSize = 0;
        this.tableIndex = 0;
        this.tableDataSize = 0;
        this.tableSearchDataSize = 0;
        this.tupleCount = {};
        this.searchData = {};
        this.serverIPs = {};
        this.tupleMaxCount = {};
        this.tupleMinCount = {};
        this.maxVisibleRows = 5;
        var kFactor = 0;
        var procedureData = {};
        var tableData = {};
        var schemaCatalogTableTypes = {};
        var schemaCatalogColumnTypes = {};
        var systemOverview = {};
        var systemMemory = {};
        var htmlMarkups = { "SystemInformation": [] };
        var htmlMarkup;
        var htmlTableMarkups = { "SystemInformation": [] };
        var htmlTableMarkup;

        this.ChangeServerConfiguration = function (serverName, portId, userName, pw, isHashPw, isAdmin) {
            VoltDBService.ChangeServerConfiguration(serverName, portId, userName, pw, isHashPw, isAdmin);
        };


        var testConnection = function (serverName, portId, username, password, admin, onInformationLoaded) {
            VoltDBService.TestConnection(serverName, portId, username, password, admin, function (result) {

                onInformationLoaded(result);
            });
        };

        this.GetLoginPopup = function () {
            var loginHtml =
                '<a href="#loginBoxDialogue" id="loginLink" style="display: none;">Login</a>' +
                '<!-- POPUP Login -->' +
                '<div id="loginBoxDialogue" style="overflow: hidden" >' +
                    '<div class="overlay-title">Login</div>' +
                        '<div id="UnableToLoginMsg" style="padding: 5px 0 0 20px; color: #ff0000; display: none;">Unable to connect!! Please try to login using another username/password.</div>' +
                            '<div class="clear"></div>' +
                            '<div  class="overlay-content" style="height:235px; min-width: 441px; padding: 0" >' +
                            '<div id="loginBox">' +
                                '<label for="username">Username:</label>' +
                                '<input type="text" id="username" name="username">' +
                                '<label for="password">Password:</label>' +
                                '<input type="password" id="password" name="password">' +
                                '<input type="checkbox" name="chkAdmin" id="chkAdmin" checked="checked" style="margin:0 0 0 74px;">' +
                                '<label for="chkAdmin" style="font-weight:normal; font-size:13px;">Allow Admin Mode operations</label>' +
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

            var popupDisplayed = false;
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
                    var passwordVal = $().crypt({ method: "sha1", source: $("#password").val() });
                    var chkAdmin = $("#chkAdmin").is(':checked');

                    testConnection($("#username").data("servername"), $("#username").data("portid"), usernameVal, passwordVal, chkAdmin, function (result) {
                        $("#overlay").hide();
                        if (result) {

                            //Save user details to cookie.
                            saveSessionCookie("username", usernameVal);
                            saveSessionCookie("password", passwordVal);
                            saveSessionCookie("admin", chkAdmin);
                            popupDisplayed = true;

                            pageLoadCallback();
                            popupCallback();
                            $("#loginBoxDialogue").hide();
                            $("#username").val("");
                            $("#password").val("");

                        } else {
                            $("#UnableToLoginMsg").show();
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

            var username = ($.cookie("username") != undefined) ? $.cookie("username") : "";
            var password = (username != "" && $.cookie("password") != undefined) ? $.cookie("password") : "";
            var admin = ($.cookie("admin") != undefined) ? $.cookie("admin") == "true" : true;

            $("#overlay").show();
            //Try to login with saved username/password or no username and password
            testConnection(serverName, portId, username, password, admin, function (result) {

                $("#overlay").hide();
                if (!popupDisplayed) {
                    //If security is enabled, then display popup to get username and password.
                    if (!result) {
                        $("#loginLink").trigger("click");
                    } else {
                        popupDisplayed = true;
                        pageLoadCallback();
                    }
                }
                popupDisplayed = true;
            });
        };


        this.ShowUsername = function (userName) {
            if (userName != undefined) {
                $(".userN").html(userName);
            } else {
                $(".userN").html("");
            }
        };

        this.GetSystemInformation = function (onInformationLoaded) {
            VoltDBService.GetSystemInformation(function (connection) {
                populateSystemInformation(connection);
                getMemoryDetails(connection, systemMemory);
                onInformationLoaded();
            });
        };

        this.getDatabaseInformation = function (onInformationLoaded) {
            var procedureMetadata = "";
            var isProcedurePopulated = false;
            var isTablePopulated = false;

            VoltDBService.GetSystemInformationDeployment(function (connection) {
                setKFactor(connection);
                VoltDBService.GetProceduresInformation(function (nestConnection) {
                    populateProceduresInformation(nestConnection);
                    procedureMetadata = nestConnection.Metadata['@Statistics_PROCEDUREPROFILE'].data;

                    VoltDBService.GetDataTablesInformation(function (inestConnection) {
                        populateTablesInformation(inestConnection);
                        populateTableTypes(inestConnection);
                        populatePartitionColumnTypes(inestConnection);
                        onInformationLoaded(procedureMetadata, inestConnection.Metadata['@Statistics_TABLE'].data);
                    });
                });
            });

            var setKFactor = function (connection) {
                connection.Metadata['@SystemInformation_DEPLOYMENT'].data.forEach(function (entry) {
                    if (entry[0] == 'kfactor')
                        kFactor = entry[1];
                });

            };


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

        this.getCpuGraphInformation = function (onInformationLoaded) {
            var cpuDetails = {};

            VoltDBService.GetCPUInformation(function (connection) {
                getCpuDetails(connection, cpuDetails);
                onInformationLoaded(cpuDetails);
            });

        };

        //Render Cluster Transaction Graph
        this.GetTransactionInformation = function (onInformationLoaded) {
            var transactionDetails = {};

            VoltDBService.GetTransactionInformation(function (connection) {
                getTransactionDetails(connection, transactionDetails);
                onInformationLoaded(transactionDetails);
            });
        };

        this.GetTableInformation = function (onInformationLoaded) {
            VoltDBService.GetTableInformation(function (connection) {
                var tablesData = {};
                var viewsData = {};
                getTableData(connection, tablesData, viewsData, 'TABLE_INFORMATION');
                onInformationLoaded(tablesData, viewsData);
            });
        };

        this.getStoredProceduresAndTableInformation = function (onProcedureAndDataTablesInformationLoaded) {
            if (this.userPreferences) {
                if (this.userPreferences['DatabaseTables'] == true) {
                    VoltDBService.GetDataTablesInformation(function (connection) {
                        populateTablesInformation(connection);

                    });
                }

                if (this.userPreferences['StoredProcedures'] == true) {
                    VoltDBService.GetProceduresInformation(function (connection) {
                        populateProceduresInformation(connection);
                    });
                }
                onProcedureAndDataTablesInformationLoaded();
            }
        };

        this.getTablesInformationByIndex = function (onDataTablesInformationLoaded) {
            VoltDBService.GetDataTablesInformation(function (connection) {
                populateTablesInformation(connection);
            });
            onDataTablesInformationLoaded();
        };

        this.getProceduresInformationByIndex = function (onProcedureInformationLoaded) {
            VoltDBService.GetDataTablesInformation(function (connection) {
                populateTablesInformation(connection);
            });
            onProcedureInformationLoaded();

        };

        this.getProcedureData = function (onProcedureDataTraversed) {
            VoltDBService.GetProceduresInformation(function (nestConnection) {
                populateProceduresInformation(nestConnection);

            });

            VoltDBService.GetDataTablesInformation(function (nestConnection) {
                populateTablesInformation(nestConnection);
                populateTableTypes(nestConnection);
                onProcedureDataTraversed();
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
                alert("Error: Unable to extract cluster health information!!");
                return;
            }

            var activeCount = 0;
            var joiningCount = 0;
            var missingCount = 0;
            var alertCount = 0;

            jQuery.each(systemOverview, function (id, val) {
                if (val["CLUSTERSTATE"] == "RUNNING")
                    activeCount++;
                else if (val["CLUSTERSTATE"] == "JOINING")
                    joiningCount++;
                else if (val["CLUSTERSTATE"] == "MISSING")
                    missingCount++;
            });


            var html =
                '<li class="activeIcon">Active <span id="activeCount">(' + activeCount + ')</span></li>' +
                    '<li class="joiningIcon">Joining <span id="joiningCount">(' + joiningCount + ')</span></li>' +
                    '<li class="missingIcon">Missing <span id="missingCount">(' + missingCount + ')</span></li>';

            var alertHtml = "";

            jQuery.each(systemMemory, function (id, val) {

                var threshold = $.cookie("alert-threshold") != undefined ? $.cookie("alert-threshold") : 90;
                if (val["MEMORYUSAGE"] * 1 >= threshold) {
                    alertHtml += '<li class="active"><a data-ip="' + systemMemory[val['HOSTNAME']]['HOST_ID'] + '" onclick=\"alertNodeClicked(this);\" href=\"#\">' + val['HOSTNAME'] + '</a> <span class=\"memory-status alert\">' + val['MEMORYUSAGE'] + '%</span></li>';
                    alertCount++;
                }
            });

            if (alertCount > 0) {
                html += '<li class="alertIcon"><a href="#memoryAlerts" id="showMemoryAlerts">Alert <span>(' + alertCount + ')</span></a></li>';
            }

            callback(html, alertHtml);
        };

        var populateSystemInformation = function (connection) {
            connection.Metadata['@SystemInformation_OVERVIEW'].data.forEach(function (entry) {
                var singleData = entry;
                var id = singleData[0];

                if (!systemOverview.hasOwnProperty(id)) {
                    systemOverview[id] = {};
                }
                systemOverview[id][singleData[1]] = singleData[2];
            });
        };

        var populateTablesInformation = function (connection) {
            var counter = 0;
            var tableNameIndex = 5;
            var partitionIndex = 4;
            var hostIndex = 1;
            var tupleCountIndex = 7;

            //connection.Metadata['@Statistics_TABLE'] = GetTestTableData(connection);

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
            var tableName = "";

            if (connection.Metadata['@Statistics_TABLE'].data != undefined) {
                connection.Metadata['@Statistics_TABLE'].data.forEach(function (entry) {
                    tableName = entry[tableNameIndex];

                    if (!tableData.hasOwnProperty(tableName)) {
                        voltDbRenderer.tableDataSize++;
                        tableData[tableName] = [];

                        connection.Metadata['@Statistics_TABLE'].data.forEach(function (nestEntry) {
                            if (nestEntry[tableNameIndex] === tableName) {
                                tableData[tableName][counter] = {};
                                tableData[tableName][counter]['HOST_ID'] = nestEntry[hostIndex];
                                tableData[tableName][counter]['PARTITION_ID'] = nestEntry[partitionIndex];
                                tableData[tableName][counter]['TUPLE_COUNT'] = nestEntry[tupleCountIndex];
                                counter++;
                            }
                        });
                    }

                });
            }

        };

        var populateProceduresInformation = function (connection) {
            var counter = 0;
            var procedureNameIndex = 0;
            var invocationsIndex = 0;
            var minLatencyIndex = 0;
            var maxLatencyIndex = 0;
            var avgLatencyIndex = 0;
            var perExecutionIndex = 0;


            //connection.Metadata['@Statistics_PROCEDUREPROFILE'] = GetTestProcedureData(connection);

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

            counter = 0;
            if (connection.Metadata['@Statistics_PROCEDUREPROFILE'].data != undefined) {
                connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.forEach(function (entry) {
                    var name = entry[procedureNameIndex];

                    if (!procedureData.hasOwnProperty(name)) {
                        procedureData[name] = {};
                    }

                    procedureData[name]['PROCEDURE'] = entry[procedureNameIndex];
                    procedureData[name]['INVOCATIONS'] = entry[invocationsIndex];
                    procedureData[name]['MIN_LATENCY'] = entry[minLatencyIndex];
                    procedureData[name]['MAX_LATENCY'] = entry[maxLatencyIndex];
                    procedureData[name]['AVG_LATENCY'] = entry[avgLatencyIndex];
                    procedureData[name]['PERC_EXECUTION'] = entry[perExecutionIndex];
                    counter++;

                });
                voltDbRenderer.procedureDataSize = connection.Metadata['@Statistics_PROCEDUREPROFILE'].data.length;

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
                    schemaCatalogTableTypes[tableName]['TABLE_NAME'] = entry[tableNameIndex];
                    schemaCatalogTableTypes[tableName]['TABLE_TYPE'] = entry[tableTypeIndex];
                    schemaCatalogTableTypes[tableName]['REMARKS'] = entry[remarksIndex];

                }
            });

        };

        var populatePartitionColumnTypes = function (connection) {
            var counterColumns = 0;
            var columnName;
            var columnNameIndex = 0;
            var tableNameColumnsIndex = 0;
            var remarksColumnsIndex = 0;

            connection.Metadata['@SystemCatalog_COLUMNS'].schema.forEach(function (columnInfo) {
                if (columnInfo["name"] == "COLUMN_NAME")
                    columnNameIndex = counterColumns;

                if (columnInfo["name"] == "TABLE_NAME")
                    tableNameColumnsIndex = counterColumns;

                if (columnInfo["name"] == "REMARKS")
                    remarksColumnsIndex = counterColumns;

                counterColumns++;
            });

            connection.Metadata['@SystemCatalog_COLUMNS'].data.forEach(function (entry) {
                columnName = entry[columnNameIndex];

                if (entry[remarksColumnsIndex] == "PARTITION_COLUMN") {
                    if (!schemaCatalogColumnTypes.hasOwnProperty(columnName)) {
                        schemaCatalogColumnTypes[columnName] = {};
                        schemaCatalogColumnTypes[columnName]['COLUMN_NAME'] = entry[columnNameIndex];
                        schemaCatalogColumnTypes[columnName]['TABLE_NAME'] = entry[tableNameColumnsIndex];
                        schemaCatalogColumnTypes[columnName]['REMARKS'] = entry[remarksColumnsIndex];

                    }
                }
            });

        };

        this.mapNodeInformationByStatus = function (callback) {
            var counter = 0;
            var memoryThreshold = $.cookie("alert-threshold") != '' ? $.cookie("alert-threshold") : -1;
            var htmlMarkups = { "ServerInformation": [] };
            var htmlMarkup;
            var currentServerHtml;

            if (systemOverview == null || systemOverview == undefined) {
                alert("Error: Unable to extract Node Status");
                return;
            }

            var currentServer = getCurrentServer();
            if (currentServer != null) {
                currentServerHtml = currentServer;
            } else {
                currentServerHtml = "";
            }

            jQuery.each(systemOverview, function (id, val) {
                var hostName;
                hostName = val["HOSTNAME"];

                if (counter == 0) {
                    /*************************************************************************
                    //CLUSTERSTATE implies if server is running or joining
                    **************************************************************************/
                    if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "RUNNING" && (currentServerHtml == "" || currentServer == hostName)) {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = "<li class=\"active monitoring\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\"  href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                            currentServerHtml = hostName;
                            //if (!serverIPs.hasOwnProperty(hostName)) {
                            //    serverIPs["hostName"] = {};
                            //    serverIPs["hostName"]= 

                            //}

                        } else {
                            htmlMarkup = "<li class=\"active monitoring\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                            currentServerHtml = hostName;
                        }

                    } else if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "RUNNING" && currentServer != hostName) {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = "<li class=\"active\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span><span class=\"hostIdHidden\" style=\"display:none\">" + systemMemory[hostName]["HOST_ID"] + "</span></li>";
                        } else {
                            htmlMarkup = "<li class=\"active\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                        }

                    } else if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "JOINING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + "<li class=\"joining\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span><span class=\"hostIdHidden\" style=\"display:none\">" + systemMemory[hostName]["HOST_ID"] + "</span></li>";

                        } else {
                            htmlMarkup = htmlMarkup + "<li class=\"joining\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                        }
                    }


                } else {
                    /********************************************************************************************
                    "currentServerHtml" is validated to verify if current server to be monitored is already set
                    *********************************************************************************************/
                    if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "RUNNING" && currentServerHtml != "" && currentServerHtml == val["HOSTNAME"]) {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + "<li class=\"active monitoring\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";

                        } else {
                            htmlMarkup = htmlMarkup + "<li class=\"active monitoring\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\"class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                        }
                    }

                    if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "RUNNING" && currentServerHtml != val["HOSTNAME"]) {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + "<li class=\"active\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";

                        } else {
                            htmlMarkup = htmlMarkup + "<li class=\"active\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                        }

                    }

                    if (val["HOSTNAME"] != null && val["CLUSTERSTATE"] == "JOINING") {
                        if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
                            htmlMarkup = htmlMarkup + "<li class=\"joining\"><a class=\"alertIcon\" data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status alert\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";

                        } else {
                            htmlMarkup = htmlMarkup + "<li class=\"joining\"><a data-ip=\"" + systemMemory[hostName]["HOST_ID"] + "\" href=\"#\">" + hostName + "</a> <span class=\"memory-status\">" + systemMemory[hostName]["MEMORYUSAGE"] + "%</span></li>";
                        }

                    }
                }
                counter++;
            });
            htmlMarkups.ServerInformation.push({ "ServersList": htmlMarkup });
            htmlMarkups.ServerInformation.push({ "CurrentServer": currentServerHtml });
            callback(htmlMarkups);
        };

        this.mapProcedureInformation = function (currentAction, priorAction, isSearch, callback) {
            var counter = 0;
            var pageStartIndex = 0;
            var traverse = false;

            htmlMarkup = "";
            htmlMarkups.SystemInformation = [];

            if (procedureData == null || procedureData == undefined) {
                alert("Error: Unable to extract Procedure Data");
                return;
            }

            if ((((voltDbRenderer.procedureTableIndex + 1) * this.maxVisibleRows < voltDbRenderer.procedureDataSize) && currentAction == VoltDbUI.ACTION_STATES.NEXT) || (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && voltDbRenderer.procedureTableIndex > 0) || currentAction == VoltDbUi.ACTION_STATES.REFRESH || currentAction == VoltDbUi.ACTION_STATES.SEARCH || currentAction == VoltDbUi.ACTION_STATES.NONE) {
                if (currentAction == VoltDbUi.ACTION_STATES.NEXT) {
                    // alert('next');
                    pageStartIndex = (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows;

                }

                if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS) { // pageStartIndex need not be initialized if isNext is undefined(when page loads intially or during reload operation)
                    //alert('previous');
                    pageStartIndex = (voltDbRenderer.procedureTableIndex - 1) * voltDbRenderer.maxVisibleRows;
                }
                if ((currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.NEXT)) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex) * voltDbRenderer.maxVisibleRows;

                }
                if ((currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                    pageStartIndex = (voltDbRenderer.procedureTableIndex) * voltDbRenderer.maxVisibleRows;
                }
                if (currentAction == VoltDbUi.ACTION_STATES.SEARCH || currentAction == VoltDbUi.ACTION_STATES.NONE) {
                    pageStartIndex = 0;
                    voltDbRenderer.procedureTableIndex = 0;
                }

                var lProcedureData = voltDbRenderer.isProcedureSearch ? this.searchData.procedures : procedureData;

                jQuery.each(lProcedureData, function (id, val) {
                    if (currentAction == VoltDbUi.ACTION_STATES.NEXT && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                            if (counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureDataSize - 1) {
                                voltDbRenderer.procedureTableIndex++;
                                return false;
                                //counter = 0;
                            }

                        } else if (counter == pageStartIndex * 2) {
                            voltDbRenderer.procedureTableIndex++;
                            return false;
                            //counter = 0;
                        }

                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && (voltDbRenderer.isProcedureSearch == false || voltDbRenderer.isProcedureSearch == undefined)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorAction == VoltDbUi.ACTION_STATES.NEXT) {
                        if (counter >= 0 && counter >= pageStartIndex && counter < voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }

                        if (pageStartIndex >= 0 && counter == (voltDbRenderer.procedureTableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.procedureTableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.NEXT) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);

                        }
                    } else if ((currentAction == VoltDbUi.ACTION_STATES.SEARCH && priorAction == VoltDbUi.ACTION_STATES.NONE)) {
                        if (pageStartIndex >= 0 && counter >= pageStartIndex && counter < ((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setProcedureTupleHtml(val);
                        }
                    } else if ((currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.SEARCH) || (currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.NEXT)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;
                            //counter = 0;
                        }
                    } else if ((currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                        if (counter >= pageStartIndex && counter <= (voltDbRenderer.procedureTableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setProcedureTupleHtml(val);
                        }

                        if ((counter == (voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.procedureSearchDataSize - 1) && htmlMarkup != "") {
                            voltDbRenderer.procedureTableIndex++;
                            return false;
                            //counter = 0;
                        }
                    } else {
                        if (counter < voltDbRenderer.maxVisibleRows) {
                            setProcedureTupleHtml(val);
                        }
                    }
                    counter++;
                });


                if (voltDbRenderer.isProcedureSearch) {
                    if (htmlMarkup != "")
                        callback(currentAction, htmlMarkup);

                    priorAction = currentAction;

                } else {
                    htmlMarkups.SystemInformation.push(htmlMarkup);
                    htmlMarkup = undefined;

                    if (htmlMarkups.SystemInformation[0] != "")
                        callback(currentAction, htmlMarkups);
                }


            }
        };

        this.mapTableInformation = function (currentAction, priorAction, isSearch, callback) {
            var counter = 0;
            var tablePageStartIndex = 0;
            var replicationCount = 0;
            var tupleCountPartitions = [];
            var partitionKeyPairData = [];
            var table_type = "";

            var formatTableTupleData = function (key, tupleData) {
                var tableName = "";
                var counter = 0;
                var partitionId = "";
                var totalTupleCount = 0;
                var partitionEntryCount = 0;


                var maxTupleValue;
                var minTupleValue;
                var avgTupleValue;

                var newPartition = false;

                $.each(tupleData, function (nestKey, partitionData) {
                    if (partitionData != undefined) {
                        if (counter == 0) {
                            partitionKeyPairData.push(partitionData['PARTITION_ID']);
                            totalTupleCount += parseInt(partitionData['TUPLE_COUNT']);
                            tupleCountPartitions[counter] = partitionData['TUPLE_COUNT'];
                            counter++;
                        } else {
                            partitionEntryCount = 0;
                            partitionKeyPairData.forEach(function (partitionId) {
                                if (partitionId == partitionData['PARTITION_ID']) {
                                    newPartition = false;
                                    partitionEntryCount++;
                                    return false;

                                } else if (partitionEntryCount == partitionKeyPairData.length - 1) {
                                    newPartition = true;
                                    partitionEntryCount++;
                                    return true;

                                }

                            });

                        }


                        if (newPartition) {
                            partitionKeyPairData.push(partitionData['PARTITION_ID']); //added new partition id under the table name
                            totalTupleCount += parseInt(partitionData['TUPLE_COUNT']);
                            tupleCountPartitions[counter] = partitionData['TUPLE_COUNT'];
                            counter++;

                        } else if (!newPartition && counter > 0) { //not if it is the just the first partition
                            replicationCount++;

                        }


                    }
                });

                if (replicationCount > 0 && table_type != "VIEW" && table_type != "PARTITIONED")
                    table_type = "REPLICATED";


                maxTupleValue = Math.max.apply(null, tupleCountPartitions);
                minTupleValue = Math.max.apply(null, tupleCountPartitions);
                avgTupleValue = tupleCountPartitions.reduce(function (a) {
                    return a;
                });

                setTableTupleDataHtml(key, totalTupleCount, maxTupleValue, minTupleValue, totalTupleCount / (parseInt(kFactor) + 1) * 1, table_type);

            };

            var setTableTypes = function (id) {
                if (counter < voltDbRenderer.maxVisibleRows) {
                    var tableName = id;

                    //before formatting individual table data validate table type with reference to SystemCatalog data
                    $.each(schemaCatalogTableTypes, function (key, typeVal) {
                        if (tableName == typeVal['TABLE_NAME']) {
                            if (typeVal['TABLE_TYPE'] == 'VIEW') {
                                table_type = "VIEW";
                            } 

                            else if (typeVal['REMARKS'] == null) {
                                var columnType = getColumnTypes(tableName);

                                if (columnType == "PARTITIONED")
                                    table_type = columnType;
                                
                                else {
                                    table_type = "";
                                }
                                
                            }
                            else {
                                table_type = "";
                            }
                            
                        }

                    });

                }
            };

            var getColumnTypes = function (tableName) {
                var columnType;
                $.each(schemaCatalogColumnTypes, function(key, typeVal) {
                    if (tableName == typeVal['TABLE_NAME']) {
                        columnType = typeVal['REMARKS'];
                        return false;
                    }                    
                });

                if (columnType == "PARTITION_COLUMN") {
                    return "PARTITIONED";
                } else {
                    return columnType;
                }
                    
                
            };
            
            if (tableData == null || tableData == undefined) {
                alert("Error: Unable to extract Table Data");
                return;
            }

            htmlTableMarkup = "";
            htmlTableMarkups.SystemInformation = [];

            if ((((voltDbRenderer.tableIndex + 1) * this.maxVisibleRows < voltDbRenderer.tableDataSize) && currentAction == VoltDbUI.ACTION_STATES.NEXT) ||
                (currentAction == VoltDbUI.ACTION_STATES.PREVIOUS && voltDbRenderer.tableIndex > 0) ||
                currentAction == VoltDbUi.ACTION_STATES.REFRESH || currentAction == VoltDbUi.ACTION_STATES.SEARCH || currentAction == VoltDbUi.ACTION_STATES.NONE) {
                if (currentAction == VoltDbUi.ACTION_STATES.NEXT) {
                    // alert('next');
                    tablePageStartIndex = (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows;

                }

                if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS) { // pageStartIndex need not be initialized if isNext is undefined(when page loads intially or during reload operation)
                    //alert('previous');
                    tablePageStartIndex = (voltDbRenderer.tableIndex - 1) * voltDbRenderer.maxVisibleRows;

                }

                if ((currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.NEXT) ||
                    (currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                    tablePageStartIndex = (voltDbRenderer.tableIndex) * voltDbRenderer.maxVisibleRows;

                }

                if (currentAction == VoltDbUi.ACTION_STATES.SEARCH || currentAction == VoltDbUi.ACTION_STATES.NONE) {
                    tablePageStartIndex = 0;
                    voltDbRenderer.tableIndex = 0;

                }

                var lTableData = this.isTableSearch ? this.searchData.tables : tableData;
                if (this.isTableSearch == false) voltDbRenderer.tableDataSize = Object.keys(tableData).length;


                $.each(lTableData, function (id, val) {
                    if (currentAction == VoltDbUi.ACTION_STATES.NEXT && (voltDbRenderer.isTableSearch == false || voltDbRenderer.isTableSearch == undefined)) {
                        if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            formatTableTupleData(id, val);
                            if (counter == (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableDataSize - 1) {
                                voltDbRenderer.tableIndex++;
                                return false;
                                //counter = 0;
                            }

                        } else if (counter == tablePageStartIndex * 2) {
                            voltDbRenderer.tableIndex++;
                            return false;
                            //counter = 0;
                        }

                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && (voltDbRenderer.isTableSearch == false || voltDbRenderer.isTableSearch == undefined)) {
                        if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows)) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }
                        if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.tableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS) {
                        if (counter >= 0 && counter >= tablePageStartIndex && counter < voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                        if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.tableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorAction == VoltDbUi.ACTION_STATES.NEXT) {
                        if (counter >= 0 && counter >= tablePageStartIndex && counter < voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                        if (tablePageStartIndex >= 0 && counter == (voltDbRenderer.tableIndex * voltDbRenderer.maxVisibleRows - 1)) {
                            voltDbRenderer.tableIndex--;
                        }
                    } else if (currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.NEXT) {
                        if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows - 1) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                    } else if ((currentAction == VoltDbUi.ACTION_STATES.REFRESH && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                        if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < ((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);

                        }

                    } else if ((currentAction == VoltDbUi.ACTION_STATES.SEARCH && priorAction == VoltDbUi.ACTION_STATES.NONE) || (currentAction == VoltDbUi.ACTION_STATES.SEARCH && priorAction == VoltDbUi.ACTION_STATES.SEARCH) ||
                    (currentAction == VoltDbUi.ACTION_STATES.SEARCH && priorAction == VoltDbUi.ACTION_STATES.REFRESH)) {
                        if (tablePageStartIndex >= 0 && counter >= tablePageStartIndex && counter < ((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows)) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                    } else if ((currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.SEARCH) || (currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.NEXT)) {
                        if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                        if ((counter == (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableSearchDataSize - 1) && htmlTableMarkup != "") {
                            voltDbRenderer.tableIndex++;
                            return false;
                            //counter = 0;
                        }

                    } else if ((currentAction == VoltDbUi.ACTION_STATES.NEXT && priorAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                        if (counter >= tablePageStartIndex && counter <= (voltDbRenderer.tableIndex + 2) * voltDbRenderer.maxVisibleRows - 1) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }

                        if ((counter == (voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows - 1 || counter == voltDbRenderer.tableSearchDataSize - 1) && htmlTableMarkup != "") {
                            voltDbRenderer.tableIndex++;
                            return false;
                            //counter = 0;
                        }

                    } else {
                        if (counter < voltDbRenderer.maxVisibleRows) {
                            setTableTypes(id);
                            formatTableTupleData(id, val);
                        }
                    }
                    counter++;

                });


                if (this.isTableSearch)
                    callback(htmlTableMarkup);

                else {
                    htmlTableMarkups.SystemInformation.push(htmlTableMarkup);
                    htmlTableMarkup = undefined;
                    callback(htmlTableMarkups);
                }
            }

        };


        this.getServerIP = function (hostId) {
            var serverAddress;
            $.each(systemOverview, function (key, val) {
                if (key == hostId) {
                    serverAddress = val["IPADDRESS"];
                }

            });
            return serverAddress;
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
            var totalMemoryIndex = -1;
            var suffix = "";
            var timeStampIndex = 0;
            var idIndex = 0;
            var hostNameList = {};

            if (processName == "GRAPH_MEMORY") {
                suffix = "_" + processName;
                hostNameList = { "1": { "HOSTNAME": getCurrentServer() } };
            } else {
                hostNameList = systemOverview;
            }

            getCurrentServer();

            connection.Metadata['@Statistics_MEMORY' + suffix].schema.forEach(function (columnInfo) {

                if (columnInfo["name"] == "HOSTNAME")
                    hostNameIndex = counter;
                else if (columnInfo["name"] == "TUPLEDATA")
                    tupledDataIndex = counter;
                else if (columnInfo["name"] == "TUPLECOUNT")
                    tupleCountIndex = counter;
                else if (columnInfo["name"] == "RSS")
                    rssIndex = counter;
                else if (columnInfo["name"] == "TOTALMEMORY")
                    totalMemoryIndex = counter;
                else if (columnInfo["name"] == "TIMESTAMP")
                    timeStampIndex = counter;
                else if (columnInfo["name"] == "HOST_ID")
                    idIndex = counter;
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

                        //If the value of TotalMemory is passed, then totalMemoryIndex will be greater than -1.
                        //TODO: Remove the condition "totalMemoryIndex > -1" and just set it to "memoryInfo[totalMemoryIndex]" after it has been implemented in the API.
                        sysMemory[hostName]["TOTALMEMORY"] = totalMemoryIndex > -1 ? memoryInfo[totalMemoryIndex] : 0;

                        //TODO: Use TotalMemory after it has been implemented in the API.
                        //sysMemory[hostName]["MEMORYUSAGE"] = (sysMemory[hostName]["RSS"] / sysMemory[hostName]["TOTALMEMORY"]) * 100;
                        var memoryUsage = (sysMemory[hostName]["TUPLEDATA"] / sysMemory[hostName]["RSS"]) * 100;
                        sysMemory[hostName]["MEMORYUSAGE"] = Math.round(memoryUsage * 100) / 100;
                    }

                });
            });
        };

        var getCpuDetails = function (connection, sysMemory) {
            var colIndex = {};
            var counter = 0;

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

        var getTransactionDetails = function (connection, sysTransaction) {
            var colIndex = {};
            var counter = 0;
            var currentTimerTick = 0;
            var procStats = {};

            //connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'] = GetTestProcedureData(connection);
            connection.Metadata['@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION'].schema.forEach(function (columnInfo) {
                colIndex[columnInfo["name"]] = counter;
                counter++;
            });

            var dataCount = 0;
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
            var currentTimedTransactionCount = 0.0;
            for (var proc in procStats) {
                currentTimedTransactionCount += procStats[proc][1];
            }
            sysTransaction["CurrentTimedTransactionCount"] = currentTimedTransactionCount;
            sysTransaction["currentTimerTick"] = currentTimerTick;

        };

        function getTableData(connection, tablesData, viewsData, processName) {
            var suffix = "";
            if (processName == "TABLE_INFORMATION") {
                suffix = "_" + processName;
            }

            var rawTables = connection.Metadata['@Statistics_TABLE' + suffix].data;
            var rawIndexes = connection.Metadata['@Statistics_INDEX' + suffix].data;
            var rawColumns = connection.Metadata['@SystemCatalog_COLUMNS' + suffix].data;

            var tables = [];
            var exports = [];
            var views = [];

            for (var k = 0; k < rawTables.length; k++) {
                var tableName = rawTables[k][5];
                if (rawTables[k][6] == 'StreamedTable')
                    exports[tableName] = { name: tableName };
                else {
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
                    else
                        tables[tableName] = item;
                }
            }

            connection.Metadata['tables'] = tables;
            connection.Metadata['views'] = views;

            for (var i = 0; i < rawColumns.length; i++) {
                var Type = 'tables';
                var TableName = rawColumns[i][2].toUpperCase();
                if (connection.Metadata['tables'][TableName] != null) {
                    if (connection.Metadata['tables'][TableName].columns == null) {
                        connection.Metadata['tables'][TableName].columns = [];
                    }
                    connection.Metadata['tables'][TableName].columns[rawColumns[i][16]] =
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
            if (!tablesData.hasOwnProperty('tables')) {
                tablesData['tables'] = {};
            }
            if (!viewsData.hasOwnProperty('views')) {
                viewsData['views'] = {};
            }
            tablesData['tables'] = connection.Metadata['tables'];
            viewsData['views'] = connection.Metadata['views'];
        }


        //common methods
        var setProcedureTupleHtml = function (val) {
            if (htmlMarkup == undefined || htmlMarkup == "") {
                //alert("if null");
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
        };

        var setTableTupleDataHtml = function (tableName, totalTupleCount, maxTupleCountPartition, minTupleCountPartition, avgTupleCountPartition, tableType) {
            if (htmlTableMarkup == undefined || htmlTableMarkup == "") {
                htmlTableMarkup = "<tr><td>" + tableName + "</td>" +
                    "<td class=\"txt-center\">" + totalTupleCount + "</td>" +
                    "<td class=\"txt-center\">" + maxTupleCountPartition + "</td>" +
                    "<td class=\"txt-center\">" + minTupleCountPartition + "</td>" +
                    "<td class=\"txt-center\">" + avgTupleCountPartition + "</td>" +
                    "<td class=\"txt-center\">" + tableType + "</td></tr>";
            } else {
                htmlTableMarkup += "<tr><td>" + tableName + "</td>" +
                    "<td class=\"txt-center\">" + totalTupleCount + "</td>" +
                    "<td class=\"txt-center\">" + maxTupleCountPartition + "</td>" +
                   "<td class=\"txt-center\">" + minTupleCountPartition + "</td>" +
                   "<td class=\"txt-center\">" + avgTupleCountPartition + "</td>" +
                   "<td class=\"txt-center\">" + tableType + "</td></tr>";
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

        this.searchTables = function (searchType, searchKey, onTablesSearched) {
            var searchDataCount = 0;
            var jsonTupleData = {};

            if (tableData == null || tableData == undefined) {
                return;
            }

            lSearchData['tables'] = {};
            $.each(tableData, function (nestKey, tupleData) {
                if (tupleData != undefined) {
                    if (nestKey.toLowerCase().indexOf(searchKey.toLowerCase()) >= 0) {
                        lSearchData['tables'][nestKey] = tupleData;
                        searchDataCount++;

                    }
                }
            });

            if (searchDataCount == 0)
                lSearchData['tables'] = "";

            this.tableSearchDataSize = searchDataCount;
            onTablesSearched(searchDataCount > 0);
        };


    });
    window.voltDbRenderer = voltDbRenderer = new iVoltDbRenderer();

})(window);


//Navigation responsive	
$(function () {
    $('#toggleMenu').click(function () {
        $("#nav").slideToggle('slow');
    });
});

$(window).resize(function () {
    //alert("resized");
    var windowWidth = $(window).width();
    if (windowWidth > 699) {
        //alert(windowWidth);
        $("#nav").css('display', 'block');
    } else if (windowWidth < 699) {
        $("#nav").css('display', 'none');
    }

});


