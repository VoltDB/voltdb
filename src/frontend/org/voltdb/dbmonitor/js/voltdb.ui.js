
$(document).ready(function () {
    if ($.cookie("username") != undefined && $.cookie("username") != 'null') {
        $("#logOut").css('display', 'block');
    } else {
        $("#logOut").css('display', 'none');
    }
    
    try {
        var savedData = getParameterByName("data");

        if (savedData != undefined && savedData != "") {
            var json = jQuery.parseJSON(decodeURIComponent(savedData));

            if (json["DisplayPreferences"] != undefined && json["DisplayPreferences"] != "")
                saveCookie("user-preferences", json["DisplayPreferences"]);

            if (json["GraphView"] != undefined && json["GraphView"] != "")
                saveCookie("graph-view", json["GraphView"]);

            if (json["CurrentServer"] != undefined && json["CurrentServer"] != "")
                saveCurrentServer(json["CurrentServer"]);

            if (json["username"] != undefined && json["username"] != "")
                saveSessionCookie("username", json["username"]);

            if (json["password"] != undefined && json["password"] != "")
                saveSessionCookie("password", json["password"]);

            if (json["AlertThreshold"] != undefined && json["AlertThreshold"] != "")
                saveCookie("alert-threshold", json["AlertThreshold"]);

            location.href = location.href.split("?")[0];
        }
    } catch (e) {/*Ignore error.*/ }

    var refreshCss = function () {
        //Enable Schema specific css only for Schema Tab.
        if (VoltDbUI.CurrentTab == NavigationTabs.Schema) {
            $('#styleBootstrapMin').removeAttr('disabled');
            $('#styleBootstrapResponsive').removeAttr('disabled');
            $('#styleThemeBootstrap').removeAttr('disabled');
            $("#nav").css("left", navLeft - 100);

            $('#styleBootstrapMin').removeProp("disabled");
            $('#styleBootstrapResponsive').removeProp("disabled");
            $('#styleThemeBootstrap').removeProp("disabled");
        } else {
            $("#styleBootstrapMin").attr('disabled', 'disabled');
            $("#styleBootstrapResponsive").attr('disabled', 'disabled');
            $("#styleThemeBootstrap").attr('disabled', 'disabled');
            $("#nav").css("left", navLeft);

            //This is required for firefox
            $('#styleBootstrapMin').prop("disabled", true);
            $('#styleBootstrapResponsive').prop("disabled", true);
            $('#styleThemeBootstrap').prop("disabled", true);
        }
    };
    
    refreshCss();
    var navLeft = $("#nav").css("left");
    $("#nav li").click(function () {
        $('.contents').hide().eq($(this).index()).show();
        $("#nav li").removeClass('active');
        $(this).addClass('active');
        VoltDbUI.CurrentTab = getCurrentTab();
        refreshCss();
        saveSessionCookie("current-tab", VoltDbUI.CurrentTab);
        
        //Activate Shortcut keys only if the current tab is "SQL Query".
        if (VoltDbUI.CurrentTab == NavigationTabs.SQLQuery) {
            if (navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1) {
                shortcut.add("f6", function () {
                    $("#runBTn").button().click();
                });
            } else {
                shortcut.add("f5", function () {
                    $("#runBTn").button().click();
                });
            }
        } else {
            
            //Refresh the charts if the current tab is "DB Monitor"
            if (VoltDbUI.CurrentTab == NavigationTabs.DBMonitor) {
                MonitorGraphUI.ChartRam.update();
                MonitorGraphUI.ChartCpu.update();
                MonitorGraphUI.ChartLatency.update();
                MonitorGraphUI.ChartTransactions.update();
            }
            else if (VoltDbUI.CurrentTab == NavigationTabs.Schema) {
                setTimeout(function () {
                    window.scrollTo(0, 0);
                }, 10);
            }
            
            shortcut.remove("f5");
            shortcut.remove("f6");
        }
    });
    
    //Attach the login popup to the page.
    $("body").append(voltDbRenderer.GetLoginPopup());
    
    var serverName = window.location.hostname == "localhost" ? null : window.location.hostname;
    var portid = window.location.hostname == "localhost" ? null : window.location.port;

    //If security is enabled, then it displays login popup. After user is verified, it calls loadPage().
    //If security is not enabled, then it simply calls loadPage().
    voltDbRenderer.HandleLogin(serverName, portid, function() { loadPage(serverName, portid); });
});

function logout() {
    saveSessionCookie("username", null);
    saveSessionCookie("password", null);
    saveSessionCookie("current-tab", NavigationTabs.DBMonitor);
    $('#logOut').prop('title', '');
    location.reload(true);
};

var loadPage = function (serverName, portid) {
    var userName = $.cookie('username') != undefined ? $.cookie('username') : "";
    var password = $.cookie('password') != undefined ? $.cookie('password') : "";
    var isConnectionChecked = false;
    voltDbRenderer.ChangeServerConfiguration(serverName, portid, userName, password, true, false);
    voltDbRenderer.ShowUsername(userName);
    loadSQLQueryPage(serverName, portid, userName, password, false);

    var loadSchemaTab = function () {
        var templateUrl = window.location.protocol + '//' + window.location.host;
        var templateJavascript = "js/template.js";

        $.get(templateUrl, function (result) {
            var body = $(result).filter("#wrapper").html();
            $("#schema").html(body);
            
            $("#schemaLinkSqlQuery").on("click", function (e) {
                $("#navSqlQuery").trigger("click");
                e.preventDefault();
            });
            
            $.getScript(templateJavascript);
            $("#overlay").hide();
        });
    };
    loadSchemaTab();
    
    //Retain current tab while page refreshing.
    var curTab = $.cookie("current-tab");
    if (curTab != undefined) {
        curTab = curTab * 1;
        if (curTab == NavigationTabs.Schema) {
            $("#overlay").show();
            setTimeout(function() { $("#navSchema > a").trigger("click"); }, 100);
        } else if (curTab == NavigationTabs.SQLQuery) {
            $("#overlay").show();
            setTimeout(function() { $("#navSqlQuery > a").trigger("click"); }, 100);
        }
    }
    
    var defaultSearchTextProcedure = 'Search Stored Procedures';
    var defaultSearchTextTable = 'Search Database Tables';

    var currentProcedureAction = VoltDbUi.ACTION_STATES.NONE;
    var priorProcedureAction = VoltDbUi.ACTION_STATES.NONE;
    var currentTableAction = VoltDbUi.ACTION_STATES.NONE;
    var priorTableAction = VoltDbUi.ACTION_STATES.NONE;
    VoltDbUI.CurrentTab = getCurrentTab();
    
    RefreshServerUI();

    var refreshClusterHealth = function () {
        //loads cluster health and other details on the top banner
        voltDbRenderer.GetSystemInformation(function () {
            voltDbRenderer.GetClusterHealth(function (htmlData, alertHtmlData) {
                $("#clusterHealth").html(htmlData).show();
                $("#memoryAlertsList").html(alertHtmlData);
            });

            voltDbRenderer.mapNodeInformationByStatus(function (htmlData) {
                var currentServer = getCurrentServer();
                if (currentServer == undefined) {
                    saveCurrentServer(htmlData.ServerInformation[1].CurrentServer);
                }

                $(".activeServerName").html(htmlData.ServerInformation[1].CurrentServer).attr('title', htmlData.ServerInformation[1].CurrentServer);
                $("#serversList").html(htmlData.ServerInformation[0].ServersList);

                //Trigger search on the newly loaded list. This is required to 
                //search server since we are refreshing the server list.
                if ($("#popServerSearch").val() != "Search Server")
                    $("#popServerSearch").trigger("keyup");
            });

            //hide loading icon
            $("#overlay").hide();

            $('#serversList > li.active > a').click(function () {
                var clickedServer = $(this).html();
                $('.activeServerName').html(clickedServer).attr('title', clickedServer);

                if ($('#popServerList').css('display') == 'none') {
                    $('#btnPopServerList').removeClass('showServers');
                    $('#btnPopServerList').addClass('hideServers');
                } else {
                    $('#btnPopServerList').removeClass('hideServers');
                    $('#btnPopServerList').addClass('showServers');

                };
                $('#popServerList').toggle('slide', '', 1500);
                $(this).parent().prevAll().removeClass('monitoring');
                $(this).parent().nextAll().removeClass('monitoring');
                $(this).parent().addClass('monitoring');


                var serverIp = voltDbRenderer.getServerIP($(this).attr('data-ip'));
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
                    GraphView: $.cookie("graph-view"),
                    DisplayPreferences: $.cookie("user-preferences"),
                    AlertThreshold: $.cookie("alert-threshold"),
                    username: $.cookie("username"),
                    password: $.cookie("password")
                };

                var win = window.open(newUrl + '?data=' + encodeURIComponent(JSON.stringify(data)), '_parent');
                win.focus();

            });

            var lUserPreferences = getUserPreferences();
            showHideGraph(lUserPreferences);

        });


    };

    var refreshGraphAndData = function (graphView, currentTab) {

            voltDbRenderer.getMemoryGraphInformation(function (memoryDetails) {
                MonitorGraphUI.RefreshMemory(memoryDetails, getCurrentServer(), graphView, currentTab);
            });
           
            voltDbRenderer.getLatencyGraphInformation(function (latencyDetails) {
                MonitorGraphUI.RefreshLatency(latencyDetails, graphView, currentTab);
            });
               
            voltDbRenderer.GetTransactionInformation(function (transactionDetails) {
                MonitorGraphUI.RefreshTransaction(transactionDetails, graphView, currentTab);
            });
               
            voltDbRenderer.getCpuGraphInformation(function (cpuDetails) {
                MonitorGraphUI.RefreshCpu(cpuDetails, getCurrentServer(), graphView, currentTab);
            });



        voltDbRenderer.getDatabaseInformation(function (procedureMetadata, tableMetadata) {               
                    if ((procedureMetadata != "" &&  procedureMetadata != undefined)) {
                        voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, voltDbRenderer.isProcedureSearch, function(traverse, htmlData) {
                            
                            if (!voltDbRenderer.isProcedureSearch) {
                                if ((currentProcedureAction == VoltDbUi.ACTION_STATES.REFRESH && currentProcedureAction != VoltDbUi.ACTION_STATES.NONE) || (currentProcedureAction != VoltDbUi.ACTION_STATES.REFRESH && currentProcedureAction == VoltDbUi.ACTION_STATES.NONE)) {
                                    lblTotalPages.innerHTML = voltDbRenderer.procedureDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureDataSize / voltDbRenderer.maxVisibleRows));

                                }

                                if (htmlData.SystemInformation[0] != "") {
                                    if ((currentProcedureAction == VoltDbUi.ACTION_STATES.NONE || currentProcedureAction == VoltDbUi.ACTION_STATES.REFRESH) && priorProcedureAction == VoltDbUi.ACTION_STATES.NONE) //only during initial load
                                        lblPrevious.innerHTML = " ".concat(1, ' ');

                                    $('#storeProcedureBody').html(htmlData.SystemInformation);

                                } else {
                                    lblPrevious.innerHTML = " ".concat(0, ' ');
                                    lblTotalPages.innerHTML = " ".concat(0);
                                    $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

                                }
                                
                            }

                            if (currentProcedureAction == VoltDbUi.ACTION_STATES.SEARCH) {
                                priorProcedureAction = currentProcedureAction;
                            }
                            currentProcedureAction = VoltDbUi.ACTION_STATES.REFRESH;
                            VoltDbUi.CurrentProcedureDataProgress = VoltDbUi.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;

                        });

                    } else {
                        lblPrevious.innerHTML = " ".concat(0, ' ');
                        lblTotalPages.innerHTML = " ".concat(0);
                        $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

                    }

                    if (tableMetadata != "" && tableMetadata!=undefined) {
                        voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function(htmlData) {
                            
                            if (!voltDbRenderer.isTableSearch) {
                                if ((currentTableAction == VoltDbUi.ACTION_STATES.REFRESH && currentTableAction != VoltDbUi.ACTION_STATES.NONE) || (currentTableAction != VoltDbUi.ACTION_STATES.REFRESH && currentTableAction == VoltDbUi.ACTION_STATES.NONE)) {
                                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableDataSize / voltDbRenderer.maxVisibleRows));
                                }

                                if (htmlData.SystemInformation[0] != "") {
                                    if (currentTableAction == VoltDbUi.ACTION_STATES.NONE && currentTableAction == VoltDbUi.ACTION_STATES.NONE) //only during initial load
                                    {
                                        lblPreviousTable.innerHTML = " ".concat(1, ' ');
                                    }
                                    $('#tablesBody').html(htmlData.SystemInformation);

                                } else {
                                    lblPreviousTable.innerHTML = " ".concat(0, ' ');
                                    lblTotalPagesofTables.innerHTML = " ".concat(0);
                                    $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
                                }

                            }

                            if (currentTableAction == VoltDbUi.ACTION_STATES.SEARCH) {
                                priorTableAction = currentTableAction;
                            }
                            currentTableAction = VoltDbUi.ACTION_STATES.REFRESH;
                            
                        });

                    } else {
                        lblPreviousTable.innerHTML = " ".concat(0, ' ');
                        lblTotalPagesofTables.innerHTML = " ".concat(0);
                        $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

                    }

            });



        $("#previousProcedures").unbind("click");
        $('#previousProcedures').on('click', function () {
            currentProcedureAction = VoltDbUi.ACTION_STATES.PREVIOUS;
            voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, voltDbRenderer.isProcedureSearch, function (currentAction, htmlData) {
                setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);

                if (!voltDbRenderer.isProcedureSearch) {
                    $('#storeProcedureBody').html(htmlData.SystemInformation);

                } else {
                    $('#storeProcedureBody').html(htmlData);

                }
                priorProcedureAction = currentAction;
                currentProcedureAction = VoltDbUi.ACTION_STATES.REFRESH;

            });

        });

        $("#nextProcedures").unbind("click");
        $('#nextProcedures').on('click', function () {
            currentProcedureAction = VoltDbUi.ACTION_STATES.NEXT;
            voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, voltDbRenderer.isProcedureSearch, function (currentAction, htmlData) {
                setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);

                if (!voltDbRenderer.isProcedureSearch) {
                    $('#storeProcedureBody').html(htmlData.SystemInformation);

                } else {
                    $('#storeProcedureBody').html(htmlData);
                }
                priorProcedureAction = currentAction;
                currentProcedureAction = VoltDbUi.ACTION_STATES.REFRESH;

            });
        });

        $("#previousTables").unbind("click");
        $('#previousTables').on('click', function () {
            currentTableAction = VoltDbUi.ACTION_STATES.PREVIOUS;
            voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                if (!voltDbRenderer.isTableSearch) {
                    $('#tablesBody').html(htmlData.SystemInformation);

                } else {
                    $('#tablesBody').html(htmlData);
                }

                priorTableAction = currentTableAction;
                currentTableAction = VoltDbUi.ACTION_STATES.REFRESH;

            });

        });

        $("#nextTables").unbind("click");
        $('#nextTables').on('click', function () {
            currentTableAction = VoltDbUi.ACTION_STATES.NEXT;
            voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                if (!voltDbRenderer.isTableSearch && (htmlData.SystemInformation != undefined || htmlData.SystemInformation != ""))
                    $('#tablesBody').html(htmlData.SystemInformation);

                else if (htmlData != undefined && htmlData != "") {
                    $('#tablesBody').html(htmlData);

                }
                priorTableAction = currentTableAction;
                currentTableAction = VoltDbUi.ACTION_STATES.REFRESH;
            });
        });

        $("#filterStoredProc").unbind("click");
        $('#filterStoredProc').on('click', function () {
            if (this.value == defaultSearchTextProcedure)
                this.value = '';
        });

        $("#filterStoredProc").unbind("keyup");
        $('#filterStoredProc').on('keyup', function () {
            if (this.value != '' && this.value != defaultSearchTextProcedure) {
                voltDbRenderer.isProcedureSearch = true;
                currentProcedureAction = VoltDbUi.ACTION_STATES.SEARCH;

            }
            else {
                voltDbRenderer.isProcedureSearch = false;
                currentProcedureAction = VoltDbUi.ACTION_STATES.REFRESH;
            }

            if (voltDbRenderer.isProcedureSearch) {
                voltDbRenderer.searchProcedures('', this.value, function (searchResult) {
                    priorProcedureAction = currentProcedureAction;
                    currentProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

                    if (searchResult) {
                        voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, voltDbRenderer.isProcedureSearch, function (traverse, htmlSearchData) {
                            if (htmlSearchData != "")
                                $('#storeProcedureBody').html(htmlSearchData);

                            else
                                $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

                        });

                    } else {
                        $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

                    }

                    //set pagination
                    setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch, voltDbRenderer.procedureSearchDataSize);
                    priorProcedureAction = VoltDbUi.ACTION_STATES.SEARCH;

                });
            } else {
                voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, false, function (traverse, htmlData) {
                    if (htmlData.SystemInformation[0] != "")
                        $('#storeProcedureBody').html(htmlData.SystemInformation);
                    else {
                        $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
                    }

                });
                setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);
            }
        });

        $("#filterDatabaseTable").unbind("keyup");
        $('#filterDatabaseTable').on('keyup', function () {
            if ($('#filterDatabaseTable')[0].value != '' && $('#filterDatabaseTable')[0].value != defaultSearchTextTable) {
                voltDbRenderer.isTableSearch = true;
                currentTableAction = VoltDbUi.ACTION_STATES.SEARCH;

            } else {
                voltDbRenderer.isTableSearch = false;
                currentTableAction = VoltDbUi.ACTION_STATES.REFRESH;
            }


            if (voltDbRenderer.isTableSearch) {
                voltDbRenderer.searchTables('', $('#filterDatabaseTable')[0].value, function (searchResult) {
                    priorTableAction = currentTableAction;
                    currentTableAction = VoltDbUI.ACTION_STATES.SEARCH;
                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                    if (searchResult) {
                        voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlSearchData) {
                            if (htmlSearchData != undefined) {
                                $('#tablesBody').html(htmlSearchData);
                            }
                        });

                    }
                    else {
                        $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
                    }
                    priorTableAction = VoltDbUi.ACTION_STATES.SEARCH;

                });
            }
            else {
                setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    if (htmlData.SystemInformation != undefined)
                        $('#tablesBody').html(htmlData.SystemInformation);
                    else {
                        $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
                    }

                });


            }
        });

        $("#filterDatabaseTable").unbind("click");
        $('#filterDatabaseTable').on('click', function () {
            if (this.value == defaultSearchTextTable)
                this.value = '';

        });

        //$("#searchStoredProc").unbind("click");
        //$('#searchStoredProc').on('click', function () {
        //    if ($('#filterStoredProc')[0].value != '' && $('#filterStoredProc')[0].value != defaultSearchTextProcedure) {
        //        voltDbRenderer.isProcedureSearch = true;
        //        currentProcedureAction = VoltDbUi.ACTION_STATES.SEARCH;

        //    } else {
        //        voltDbRenderer.isProcedureSearch = false;
        //        currentProcedureAction = VoltDbUi.ACTION_STATES.REFRESH;
        //    }


        //    if (voltDbRenderer.isProcedureSearch) {
        //        voltDbRenderer.searchProcedures('', $('#filterStoredProc')[0].value, function (searchResult) {
        //            priorProcedureAction = currentProcedureAction;
        //            currentProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

        //            //set pagination
        //            setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);

        //            if (searchResult) {
        //                voltDbRenderer.mapProcedureInformation(undefined, voltDbRenderer.isProcedureSearch, function (traverse, htmlSearchData) {
        //                    $('#storeProcedureBody').html(htmlSearchData);

        //                });

        //            } else {
        //                $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

        //            }
        //            priorProcedureAction = VoltDbUi.ACTION_STATES.SEARCH;

        //        });

        //    } else {
        //        voltDbRenderer.mapProcedureInformation(undefined, false, function (traverse, htmlData) {
        //            if (htmlData.SystemInformation[0] != "")
        //                $('#storeProcedureBody').html(htmlData.SystemInformation);
        //            else {
        //                $('#storeProcedureBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
        //            }

        //        });
        //        setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);
        //    }
        //});

        //$("#searchDataTable").unbind("click");
        //$('#searchDataTable').on('click', function () {
        //    voltDbRenderer.isTableSearch = true;
        //    if ($('#filterDatabaseTable')[0].value != '' && $('#filterDatabaseTable')[0].value != defaultSearchTextTable) {
        //        voltDbRenderer.isTableSearch = true;
        //        currentTableAction = VoltDbUi.ACTION_STATES.SEARCH;

        //    } else {
        //        voltDbRenderer.isTableSearch = false;
        //        currentTableAction = VoltDbUi.ACTION_STATES.REFRESH;
        //    }

        //    if (voltDbRenderer.isTableSearch) {
        //        voltDbRenderer.searchTables('', $('#filterDatabaseTable')[0].value, function (searchResult) {
        //            setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
        //            if (searchResult) {
        //                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlSearchData) {
        //                    $('#tablesBody').html(htmlSearchData);

        //                });

        //            } else {
        //                $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");

        //            }
        //            priorTableAction = VoltDbUi.ACTION_STATES.SEARCH;

        //        });

        //    } else {
        //        voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, false, function (htmlData) {
        //            if (htmlData.SystemInformation != undefined)
        //                $('#tablesBody').html(htmlData.SystemInformation);
        //            else {
        //                $('#tablesBody').html("<tr><td colspan=6> No Data to be displayed</td></tr>");
        //            }

        //        });
        //        setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

        //    }
        //});

        var setPaginationIndicesOfTables = function (isTableSearch) {
            //set pagination
            if (isTableSearch) {
                if (voltDbRenderer.tableSearchDataSize > 0) {
                    if (currentTableAction == VoltDbUi.ACTION_STATES.NEXT &&
                        (priorTableAction == VoltDbUi.ACTION_STATES.SEARCH || priorTableAction == VoltDbUi.ACTION_STATES.PREVIOUS || priorTableAction == VoltDbUi.ACTION_STATES.NEXT)) {

                        lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                        lblTotalPagesofTables.innerText = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));


                    } else if ((currentTableAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorTableAction == VoltDbUi.ACTION_STATES.SEARCH || priorTableAction == VoltDbUi.ACTION_STATES.PREVIOUS)) {
                        lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                        lblTotalPagesofTables.innerText = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));

                    } else if (currentTableAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorTableAction == VoltDbUi.ACTION_STATES.NEXT) {
                        lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                        lblTotalPagesofTables.innerText = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));

                    } else {
                        lblPreviousTable.innerHTML = " ".concat(1, ' ');
                        lblTotalPagesofTables.innerText = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));
                    }

                }
                else {
                    lblPreviousTable.innerText = " ".concat(0, ' ');
                    lblTotalPagesofTables.innerText = " ".concat(0);

                }

            }
            else {
                if (voltDbRenderer.tableDataSize > 0) {
                    if (currentTableAction == VoltDbUi.ACTION_STATES.NEXT) {
                        lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                    }
                    else if (currentTableAction == VoltDbUi.ACTION_STATES.PREVIOUS) {
                        lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                    } else {
                        lblPreviousTable.innerHTML = " ".concat(1, ' ');
                    }
                    lblTotalPagesofTables.innerText = voltDbRenderer.tableDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableDataSize / voltDbRenderer.maxVisibleRows));


                } else {
                    lblPreviousTable.innerText = " ".concat(0, ' ');
                    lblTotalPagesofTables.innerText = " ".concat(0);

                }
            }
        };

        var setPaginationIndicesOfProcedures = function (isProcedureSearch) {
            if (isProcedureSearch) {
                if (voltDbRenderer.procedureSearchDataSize > 0) {
                    if (currentProcedureAction == VoltDbUi.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUi.ACTION_STATES.SEARCH) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                    }
                    else if ((currentProcedureAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorProcedureAction == VoltDbUi.ACTION_STATES.SEARCH)) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                    }

                    else if (currentProcedureAction == VoltDbUi.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUi.ACTION_STATES.PREVIOUS) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                    }
                    else if (currentProcedureAction == VoltDbUi.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUi.ACTION_STATES.NEXT) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                    }
                    else if (currentProcedureAction == VoltDbUi.ACTION_STATES.PREVIOUS && priorProcedureAction == VoltDbUi.ACTION_STATES.NEXT) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                    }
                    else {
                        lblPrevious.innerHTML = " ".concat(1, ' ');
                        lblTotalPages.innerText = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));
                    }


                } else {
                    lblPrevious.innerText = " ".concat(0, ' ');
                    lblTotalPages.innerText = " ".concat(0);

                }

            } else {
                if (voltDbRenderer.procedureDataSize > 0) {
                    if (currentProcedureAction == VoltDbUi.ACTION_STATES.NEXT) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    }
                    else if (currentProcedureAction == VoltDbUi.ACTION_STATES.PREVIOUS) {
                        lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    }
                    else {
                        lblPrevious.innerHTML = " ".concat(1, ' ');
                    }
                    lblTotalPages.innerText = voltDbRenderer.procedureDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureDataSize / voltDbRenderer.maxVisibleRows));

                } else {
                    lblPrevious.innerText = " ".concat(0, ' ');
                    lblTotalPages.innerText = " ".concat(0);

                }
            }

        };

    };

    var saveThreshold = function () {

        var defaultThreshold = 90;
        var thresholdInput = $("#threshold");

        if (thresholdInput.val() == "") {
            thresholdInput.val(defaultThreshold);
        }

        if ($.cookie("alert-threshold") == undefined || $.cookie("alert-threshold") == null) {
            saveCookie("alert-threshold", defaultThreshold);
        }

        $("#saveThreshold").on("click", function () {

            if (thresholdInput.val() == "" || thresholdInput.val() * 1 > 100) {
                alert("The value of \"Alert Threshold\" should be between 0 and 100.");
                return false;
            }

            var thresholdValue = (thresholdInput.val() != "") ? thresholdInput.val() : defaultThreshold;
            saveCookie("alert-threshold", thresholdValue);
            refreshClusterHealth();
            $("#popServerList").hide();
            $("#overlay").show();
            $('#btnPopServerList').removeClass('hideServers');
            $('#btnPopServerList').addClass('showServers');
            return true;
        });

        $("#threshold").on("keypress", function (e) {
            if (e.which != 8 && e.which != 0 && (e.which < 48 || e.which > 57)) {
                return false;
            }
            return true;

        });

        //Set the value of threshold
        $("#threshold").val($.cookie("alert-threshold"));

    };

    if ($.cookie("graph-view") == undefined || $.cookie("graph-view") == null)
        saveCookie("graph-view", $("#graphView").val());

    $("#graphView").val($.cookie("graph-view"));
    MonitorGraphUI.AddGraph($.cookie("graph-view"), $('#chartServerCPU'), $('#chartServerRAM'), $('#chartClusterLatency'), $('#chartClusterTransactions'));

    var graphInterval = null;
    var refreshTime = 5000; //In milli seconds (i.e, 5 sec)
    var refreshGraphAndDataInLoop = function (seconds, graphView) {
        if (graphInterval != null)
            window.clearInterval(graphInterval);
        
        refreshGraphAndData(graphView, VoltDbUI.CurrentTab);
        graphInterval = window.setInterval(function () { refreshGraphAndData(graphView, VoltDbUI.CurrentTab); }, seconds);
    };

    var getRefreshTime = function () {
        var graphViewValue = $.cookie("graph-view").toLowerCase();

        if (graphViewValue == "seconds")
            refreshTime = 5000;
        else if (graphViewValue == "minutes")
            refreshTime = 60000;
        else if (graphViewValue == "days")
            refreshTime = 86400000;

        return refreshTime;
    };

    $("#graphView").on("change", function () {
        var graphView = $("#graphView").val();
        saveCookie("graph-view", graphView);
        MonitorGraphUI.RefreshGraph(graphView);
        MonitorGraphUI.ChartRam.update();
        MonitorGraphUI.ChartCpu.update();
        MonitorGraphUI.ChartLatency.update();
        MonitorGraphUI.ChartTransactions.update();
        
        //refreshGraphAndDataInLoop(getRefreshTime(), graphView);
    });

    //slides the element with class "menu_body" when paragraph with class "menu_head" is clicked 
    $("#firstpane div.menu_head").click(function () {
        var userPreferences = getUserPreferences();
        if (userPreferences != null) {
            if (userPreferences['DatabaseTables'] != false || userPreferences['StoredProcedures'] != false) {
                var headerState = $("#firstpane div.menu_body").css('display');
                if (headerState == 'none') {
                    $(this).removeClass('collapsed');
                    $(this).addClass('expanded');
                } else {
                    $(this).removeClass('expanded');
                    $(this).addClass('collapsed');
                }
                $(this).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");
            }
        } else {

            $('ul.user-preferences > li Input:checkbox').each(function (value) {
                userPreferences[$(this)[0].id] = true;
            });
            saveUserPreferences(userPreferences);

            $(this).removeClass('collapsed');
            $(this).addClass('expanded');
            $(this).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");

        }

    });

    refreshClusterHealth();
    refreshGraphAndData($.cookie("graph-view"), VoltDbUI.CurrentTab);
    setInterval(refreshClusterHealth, 5000);
    setInterval(function() {
        refreshGraphAndData($.cookie("graph-view"), VoltDbUI.CurrentTab);
    }, 5000);
    //refreshGraphAndDataInLoop(getRefreshTime(), $.cookie("graph-view"));
    configureUserPreferences();
    adjustGraphSpacing();
    saveThreshold();

    var connectionTimeInterval = null;
    var refreshConnectionTime = function (seconds) {
            if (connectionTimeInterval != null)
                window.clearInterval(connectionTimeInterval);

            connectionTimeInterval = window.setInterval(checkServerConnection, seconds);
    };

    var checkServerConnection = function() {
        if (!isConnectionChecked) {
            isConnectionChecked = true;
            voltDbRenderer.CheckServerConnection(
                function(result) {
                    if (result == false) {
                        VoltDBCore.isServerConnected = false;
                        if (!$('#conpop').is(':visible')) {
                            window.clearInterval(connectionTimeInterval);
                            $('#conPopup').click();
                        }
                    } else {
                        isConnectionChecked = false;
                    }
                }
            );
        }
    };

    $("#conPopup").popup({
        closeDialog: function () {
            isConnectionChecked = false;
            refreshConnectionTime('20000');
            $('#connectionPopup').hide();
        }
    });
    
    refreshConnectionTime('20000');
};


/*********************************************************************************************
/Configure My Graph dialog
/*********************************************************************************************/
var configureUserPreferences = function () {
    var userPreference = {};
    userPreference["serverCPU"] = {};
    userPreference["serverRAM"] = {};
    userPreference["clusterLatency"] = {};
    userPreference["clusterTransactions"] = {};
    userPreference["storedProcedures "] = {};
    userPreference["databaseTables "] = {};

    $('#showMyPreference').popup(
        {
            open: function (event, ui, ele) {
                userPreference = getUserPreferences();
                $('ul.user-preferences > li Input:checkbox').each(function (value) {
                    $(this)[0].checked = userPreference[$(this)[0].id];
                });
            },
            save: function () {
                $('ul.user-preferences > li Input:checkbox').each(function (value) {
                    userPreference[$(this)[0].id] = $(this)[0].checked;

                });
                saveUserPreferences(userPreference);

                MonitorGraphUI.ChartRam.update();
                MonitorGraphUI.ChartCpu.update();
                MonitorGraphUI.ChartLatency.update();
                MonitorGraphUI.ChartTransactions.update();
            }





        });



};


/*******************************************************************************************
//common methods
/*******************************************************************************************/

//Dummy wrapper for console.log for IE9
if (!(window.console && console.log)) {
    console = {
        log: function () { },
        debug: function () { },
        info: function () { },
        warn: function () { },
        error: function () { }
    };
}

var saveCookie = function (name, value) {
    $.cookie(name, value, { expires: 365 });
};

var saveSessionCookie = function (name, value) {
    $.cookie(name, value, { path: '/', domain: window.location.hostname });
};

var saveUserPreferences = function (preferences) {
    var lPreferences = preferences;
    saveCookie("user-preferences", JSON.stringify(preferences));
    showHideGraph(lPreferences);
};

var NavigationTabs = {
    DBMonitor: 1,
    Schema: 2,
    SQLQuery:3
};

var getCurrentTab = function() {
    var activeLinkId = "";
    var activeLink = $("#nav .active");
    if (activeLink.length > 0) {
        activeLinkId = activeLink.attr("id");
    }

    if (activeLinkId == "navSqlQuery")
        return NavigationTabs.SQLQuery;
    else if (activeLinkId == "navSchema")
        return NavigationTabs.Schema;

    return NavigationTabs.DBMonitor;
};

var getUserPreferences = function () {
    try {
        voltDbRenderer.userPreferences = $.parseJSON($.cookie("user-preferences"));
    } catch (e) {

        voltDbRenderer.userPreferences = {};
        var preferencesList = ["ServerCPU", "ServerRAM", "ClusterLatency", "ClusterTransactions", "StoredProcedures", "DatabaseTables"];
        for (var i = 0; i < preferencesList.length; i++) {
            voltDbRenderer.userPreferences[preferencesList[i]] = true;
        }
    }
    return voltDbRenderer.userPreferences;
};

var saveCurrentServer = function (serverName) {
    saveCookie("currentServer", serverName);
};

var getCurrentServer = function () {
    return $.cookie("currentServer");

};

var showHideGraph = function (userpreferences) {
    if (userpreferences["ServerCPU"] == false)
        $("#chartServerCPU").hide();
    else
        $("#chartServerCPU").show();

    if (userpreferences["ServerRAM"] == false)
        $("#chartServerRAM").hide();
    else
        $("#chartServerRAM").show();

    if (userpreferences["ClusterLatency"] == false)
        $("#chartClusterLatency").hide();
    else
        $("#chartClusterLatency").show();

    if (userpreferences["ClusterTransactions"] == false)
        $("#chartClusterTransactions").hide();
    else
        $("#chartClusterTransactions").show();

    if (userpreferences["StoredProcedures"] == false)
        $("#tblStoredProcedures").hide();
    else
        $("#tblStoredProcedures").show();

    if (userpreferences["DatabaseTables"] == false)
        $("#tblDataTables").hide();
    else
        $("#tblDataTables").show();

    if (userpreferences["DatabaseTables"] == false && userpreferences["StoredProcedures"] == false)
        $("#firstpane").hide();
    else
        $("#firstpane").show();

    adjustGraphSpacing();
    ChangeGraphLabelColor();
    ChangeTableProcedureLabelColor();
};

function ChangeGraphLabelColor() {
    if ($.cookie("user-preferences") != undefined) {
        var userPreferences = $.parseJSON($.cookie("user-preferences"));
        if (userPreferences['ClusterLatency'] != false || userPreferences['ClusterTransactions'] != false || userPreferences['ServerCPU'] != false || userPreferences['ServerRAM'] != false) {
            $('#showHideGraphBlock').css('color', '#000000');
            $("#GraphBlock").removeClass("graphOpacity");
        } else {
            $('#showHideGraphBlock').css('color', 'gray');
            $("#GraphBlock").addClass("graphOpacity");
        }
    }
}

function ChangeTableProcedureLabelColor() {
    if ($.cookie("user-preferences") != undefined) {
        var userPreferences = $.parseJSON($.cookie("user-preferences"));
        if (userPreferences['DatabaseTables'] != false || userPreferences['StoredProcedures'] != false) {
            $('#ShowHideBlock').css('color', '#000000');
        } else {
            $('#ShowHideBlock').css('color', 'gray');
        }
    }
}

// Graph Spacing adjustment on preference change
var adjustGraphSpacing = function () {
    var graphList = [$("#chartServerCPU"), $("#chartServerRAM"), $("#chartClusterLatency"), $("#chartClusterTransactions")];

    var css = "left";

    for (var i = 0; i < graphList.length; i++) {
        if (graphList[i].is(':visible')) {
            graphList[i].removeClass("left right");
            graphList[i].addClass(css);

            if (css == "left")
                css = "right";
            else
                css = "left";
        }
    }
};

(function (window) {
    var iVoltDbUi = (function () {
        this.ACTION_STATES = {
            NONE: -1,
            NEXT: 0,
            PREVIOUS: 1,
            REFRESH: 2,
            SEARCH: 3
        };

        this.DASHBOARD_PROGRESS_STATES = {
            REFRESHMEMORY: 0,
            REFRESHMEMORY_NONE: 1,
            REFRESHCPU: 2,
            REFRESHCPU_NONE: 3,
            REFRESHLATENCY: 4,
            REFRESHLATENCY_NONE: 5,
            REFRESHTRANSACTION: 6,
            REFRESHTRANSACTION_NONE: 7,

            REFRESH_PROCEDUREDATA: 8,
            REFRESH_PROCEDUREDATA_NONE:9,
            REFRESH_TABLEDATA: 10,
            REFRESH_TABLEDATA_NONE: 11            
        };
        
        this.CurrentTab = NavigationTabs.DBMonitor;

        this.CurrentMemoryProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHMEMORY_NONE;
        this.CurrentCpuProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHCPU_NONE;
        this.CurrentLatencyProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHLATENCY_NONE;
        this.CurrentTransactionProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHTRANSACTION_NONE;
        this.CurrentTableDataProgress = this.DASHBOARD_PROGRESS_STATES.REFRESH_TABLEDATA_NONE;
        this.CurrentProcedureDataProgress = this.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;
        
    });
    window.VoltDbUI = VoltDbUi = new iVoltDbUi();

})(window);


function RefreshServerUI() {
    var clickedServer = getParameterByName("currentServer");
    if (clickedServer != "") {
        $('.activeServerName').html(clickedServer).attr('title', clickedServer);
        saveCurrentServer(clickedServer);

        $('#popServerList').toggle('slide', '', 1500);
        $(this).parent().prevAll().removeClass('monitoring');
        $(this).parent().nextAll().removeClass('monitoring');
        $(this).parent().addClass('monitoring');

        $('#btnPopServerList').removeClass('hideServers');
        $('#btnPopServerList').addClass('showServers');

        $("#popServerList").hide();

    }


}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
    var regexS = "[\\?&]" + name + "=([^&#]*)";
    var regex = new RegExp(regexS);
    var results = regex.exec(window.location.href);
    if (results == null)
        return "";
    else
        return decodeURIComponent(results[1].replace(/\+/g, " "));
}






