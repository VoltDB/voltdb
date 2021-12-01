function alertNodeClicked(obj) {
  var clickedServer = $(obj).html();

  if ($("#activeServerName").html() != clickedServer) {
    $(".activeServerName").html(clickedServer).attr("title", clickedServer);

    //Change the active server name in the node list
    $("#serversList>li").removeClass("monitoring");

    $("#serversList")
      .find("li a:contains(" + clickedServer + ")")
      .filter(function () {
        return $(this).html() == clickedServer;
      })
      .parent()
      .addClass("monitoring");

    var serverIp = voltDbRenderer.getServerIP($(obj).attr("data-ip"));
    var currentUrl = window.location.href.split("?")[0];
    var urlArray = currentUrl.split("/");
    var newUrl = "";
    if (urlArray != null && urlArray.length > 0) {
      var urlArray2 = urlArray[2].split(":");
      urlArray2[0] = serverIp;
      urlArray[2] = urlArray2.join(":");
      newUrl = urlArray.join("/");
    }

    var data = {
      CurrentServer: clickedServer,
      GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
      DisplayPreferences: VoltDbUI.getFromLocalStorage("user-preferences"),
      AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
      username: VoltDbUI.getCookie("username"),
      password: VoltDbUI.getCookie("password"),
    };

    var win = window.open(newUrl + "?data=" + JSON.stringify(data), "_parent");
    win.focus();
  }

  $(".popup_close").trigger("click");
}

(function (window) {
  var iVoltDbRenderer = function () {
    this.hostNames = [];
    this.currentHost = "";
    this.usersList = [];
    this.usersRoles = [];

    this.isHost = false;
    this.userPreferences = {};
    var totalServerCount = 0;
    var kFactor = 0;
    var procedureData = {};
    var procedureDetailData = {};
    var tableData = {};
    var schemaCatalogTableTypes = {};
    var schemaCatalogColumnTypes = {};
    var systemOverview = {};
    var systemMemory = {};
    var htmlMarkups = { SystemInformation: [] };
    var htmlMarkup;
    var minLatency = 0;
    var maxLatency = 0;
    var avgLatency = 0;
    var procedureNameIndex = 0;
    var invocationsIndex = 0;
    var minLatencyIndex = 0;
    var maxLatencyIndex = 0;
    var avgLatencyIndex = 0;
    var perExecutionIndex = 0;
    var statementIndex = 0;
    var minExecutionTimeIndex = 0;
    var maxExecutionTimeIndex = 0;
    var avgExecutionTimeIndex = 0;
    var gCurrentServer = "";
    var tableNameIndex = 5;
    var partitionIndex = 4;
    var hostIndex = 1;
    var tupleCountIndex = 3;
    var activeCount = 0;
    var activeCountCopied = 0;
    var joiningCount = 0;
    var missingCount = 0;
    var alertCount = 0;
    var serverSettings = false;
    this.memoryDetails = [];
    this.drTablesArray = [];
    this.exportTablesArray = [];
    this.ChangeServerConfiguration = function (
      serverName,
      portId,
      userName,
      pw,
      isHashPw,
      isAdmin
    ) {
      VoltDBService.ChangeServerConfiguration(
        serverName,
        portId,
        userName,
        pw,
        isHashPw,
        isAdmin
      );
    };

    var testConnection = function (
      serverName,
      portId,
      username,
      password,
      admin,
      onInformationLoaded
    ) {
      VoltDBService.TestConnection(
        serverName,
        portId,
        username,
        password,
        admin,
        function (result, response) {
          onInformationLoaded(result, response);
        },
        true
      );
    };

    this.CheckServerConnection = function (checkConnection) {
      VoltDBService.CheckServerConnection(checkConnection);
    };

    this.GetLoginPopup = function () {
      var loginHtml =
        '<a href="#loginBoxDialogue" id="loginLink" style="display: none;">Login</a>' +
        "<!-- POPUP Login -->" +
        '<div id="loginBoxDialogue" style="overflow: hidden" >' +
        '<div class="overlay-title">Login</div>' +
        '<div id="UnableToLoginMsg" class="errMsgBox" style=" display: none;"><div class="errMsg">Unable to connect. Please try to login using another username/password.</div></div>' +
        '<div id="PasswordChangeMsg" class="errMsgBox" style="display: none;"><div class="errMsg">Your credentials has been changed. Please login with new credentials.</div></div>' +
        '<div id="dbNotReadyMsg" class="errMsgBox" style="display: none;"><div class="errMsg">Database is not ready. Please wait few seconds.</div></div>' +
        '<div class="clear"></div>' +
        '<div  class="overlay-content" style="height:auto; min-width: auto; padding: 0" >' +
        '<div id="loginBox">' +
        '<label for="username">Username:</label>' +
        '<input type="text" id="username" name="username"><br/>' +
        '<label for="password">Password:</label>' +
        '<input type="password" id="password" name="password">' +
        '<div class="lower">' +
        '<input type="submit" id="LoginBtn" value="Login">' +
        "</div>" +
        "</div>" +
        "</div>    " +
        "</div>" +
        "<!-- POPUP Login -->";
      return loginHtml;
    };

    this.HandleLogin = function (serverName, portId, pageLoadCallback) {
      var responseObtained = false;
      $("#username").data("servername", serverName);
      $("#username").data("portid", portId);
      $("#loginBoxDialogue").hide();
      $("#loginLink").popup({
        open: function (event, ui, ele) {
          var roleUpdated = parseInt(VoltDbUI.getCookie("role"));
          var passwordUpdated = parseInt(VoltDbUI.getCookie("password"))
          if (roleUpdated === -1) {
            $("#RoleChangeMsg").show();
            saveSessionCookie("role", null);
          }

          if (passwordUpdated === -1) {
            $("#PasswordChangeMsg").show();
            saveSessionCookie("password", null);
          }
        },
        login: function (popupCallback) {
          $("#overlay").show();
          $("#UnableToLoginMsg").hide();
          var usernameVal = $("#username").val();
          var passwordVal =
            $("#password").val() != ""
              ? CryptoJS.SHA256($("#password").val()).toString(CryptoJS.enc.Hex)
              : $("#password").val();
          responseObtained = false;

          testConnection(
            $("#username").data("servername"),
            $("#username").data("portid"),
            usernameVal,
            passwordVal,
            true,
            function (result, response) {
              if (
                responseObtained ||
                (response != undefined &&
                  response.hasOwnProperty("status") &&
                  response.status == -1)
              )
                return;
              responseObtained = true;

              $("#overlay").hide();
              if (result) {
                const url = `api/1.0/?Procedure=%40SystemCatalog&Parameters=%5B"USERS"%5D`;
                var usersList = []
                $.ajax({
                  url: url,
                  type: 'get',
                  success: function (response) {
                    var result = response.results[0];
                    usersList = result.data.map((item) => {
                      return {
                        name: item[0],
                        role: item[1],
                      }
                    })
                  },
                  error: function (response) {
                    $("#RoleChangeMsg").hide();
                    $("#PasswordChangeMsg").hide();
                    $("#dbNotReadyMsg").show();
                  }
                }).done(function () {
                  voltDbRenderer.usersList = usersList;
                  $("#RoleChangeMsg").hide();
                  $("#PasswordChangeMsg").hide();
                  $("#dbNotReadyMsg").hide();
                  //Save user details to cookie.
                  loadAdminPage();
                  saveSessionCookie("username", usernameVal);
                  saveSessionCookie("password", passwordVal);
                  if (usersList.length > 0) {
                    currentUserRole = usersList.filter(user => user.name === usernameVal)[0].role;
                  }
                  saveSessionCookie("role", currentUserRole)
                  voltDbRenderer.ShowUsername(usernameVal);
                  pageLoadCallback();
                  popupCallback();
                  $("#loginBoxDialogue").hide();
                  setTimeout(function () {
                    $("#username").val("");
                    $("#password").val("");
                    window.location.reload();
                  }, 300);
                  $("#logOut").css("display", "block");
                  $("#logOut").prop("title", VoltDbUI.getCookie("username"));
                })
              } else {
                $("#RoleChangeMsg").hide();
                $("#PasswordChangeMsg").hide();
                $("#dbNotReadyMsg").hide();

                //Error: Server is not available(-100) or Connection refused(-5) but is not "Authentication rejected(-3)"
                if (response != undefined && response.status != -3) {
                  popupCallback();
                  $("#loginBoxDialogue").hide();
                  $("#serUnavailablePopup").trigger("click");
                  return;
                }

                $("#UnableToLoginMsg").show();
                $("#logOut").css("display", "none");
                $("#logOut").prop("title", "");
              }
            }
          );
        },
      });

      $("#username").on("keypress", function (e) {
        var key = e.which;
        if (key == 13) {
          // the enter key code
          $("#LoginBtn").trigger("click");
          return false;
        }
        return true;
      });
      $("#password").on("keypress", function (e) {
        var key = e.which;
        if (key == 13) {
          // the enter key code
          $("#LoginBtn").trigger("click");
          return false;
        }
        return true;
      });

      var username =
        VoltDbUI.getCookie("username") != undefined
          ? VoltDbUI.getCookie("username")
          : "";
      var password =
        username != "" && VoltDbUI.getCookie("password") != undefined
          ? VoltDbUI.getCookie("password")
          : "";

      $("#serUnavailablePopup").popup({
        open: function (event, ui, ele) { },
        autoLogin: function (popupCallback) {
          tryAutoLogin();
          popupCallback();
        },
      });

      //Try to login with saved username/password or no username and password
      var tryAutoLogin = function () {
        $("#overlay").show();
        responseObtained = false;
        serverName = VoltDBConfig.GetDefaultServerIP(true);
        testConnection(
          serverName,
          portId,
          username,
          password,
          true,
          function (result, response) {
            if (
              responseObtained ||
              (response != undefined &&
                response.hasOwnProperty("status") &&
                response.status == -1)
            )
              return;
            responseObtained = true;

            $("#overlay").hide();

            if (!result) {
              if (response != undefined && response.hasOwnProperty("status")) {
                //Error: Hashedpassword must be a 40-byte hex-encoded SHA-1 hash.
                if (
                  response.status == -3 &&
                  response.hasOwnProperty("statusstring") &&
                  response.statusstring.indexOf(
                    "Hashedpassword must be a 40-byte"
                  ) > -1
                ) {
                  //Try to auto login after clearing username and password
                  saveSessionCookie("username", null);
                  tryAutoLogin();
                  return;
                } else if (response.status == 401) {
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
              $("#loginLink").trigger("click");
            } else {
              pageLoadCallback();
            }
          }
        );
      };
      tryAutoLogin();
    };

    this.ShowUsername = function (userName) {
      if (userName != undefined && userName != "null" && userName != "") {
        $(".userN").attr("title", userName).html(userName);
        $("#userLine").show();
      } else {
        $(".userN").attr("title", "").html("");
        $("#userLine").hide();
      }
    };

    this.GetSystemInformation = function (
      onInformationLoaded,
      onAdminPagePortAndOverviewDetailsLoaded,
      onAdminPageServerListLoaded
    ) {
      VoltDBService.GetSystemInformation(function (connection) {
        populateSystemInformation(connection);
        getMemoryDetails(connection, systemMemory);

        if (VoltDbAdminConfig.isAdmin) {
          onAdminPagePortAndOverviewDetailsLoaded(
            getPortAndOverviewDetails(),
            serverSettings
          );
          onAdminPageServerListLoaded(getAdminServerList());
        }

        if (gCurrentServer == "") configureRequestedHost(VoltDBCore.hostIP);

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

    this.GetAdminDeploymentInformation = function (
      checkSecurity,
      onInformationLoaded
    ) {
      voltDbRenderer.CheckAdminPriviledges(function (hasAdminPrivileges) {
        if (hasAdminPrivileges) {
          VoltDbAdminConfig.isAdmin = true;
        }
      });
      if (VoltDbAdminConfig.isAdmin || checkSecurity) {
        VoltDBService.GetShortApiDeployment(function (connection) {
          var rawData;
          if (connection != null)
            rawData = connection.Metadata["SHORTAPI_DEPLOYMENT"];
          onInformationLoaded(
            loadAdminDeploymentInformation(connection),
            rawData
          );
        });
      }
    };

    this.CheckAdminPriviledges = function (onInformationLoaded) {
      VoltDBService.GetShortApiProfile(function (connection) {
        onInformationLoaded(hasAdminPrivileges(connection));
      });
    };

    this.GetCommandLogStatus = function (onInformationLoaded) {
      if (VoltDbAdminConfig.isAdmin || checkSecurity) {
        VoltDBService.GetShortApiDeployment(function (connection) {
          var rawData;
          var isCmdLogEnabled = false;
          if (connection != null) {
            rawData = connection.Metadata["SHORTAPI_DEPLOYMENT"];
            if (
              rawData.hasOwnProperty("commandlog") &&
              rawData["commandlog"].hasOwnProperty("enabled")
            ) {
              isCmdLogEnabled = rawData["commandlog"]["enabled"];
            }
          }
          onInformationLoaded(isCmdLogEnabled);
        });
      } else {
        onInformationLoaded(false);
      }
    };

    this.GetExportProperties = function (onInformationLoaded) {
      VoltDBService.GetExportProperties(function (connection) {
        var rawData;
        if (connection != null)
          rawData = connection.Metadata["SHORTAPI_DEPLOYMENT_EXPORTTYPES"];

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
        connection.Metadata["@SystemInformation_DEPLOYMENT"].data.forEach(
          function (entry) {
            if (entry[0] == "kfactor") kFactor = entry[1];
          }
        );
      };
    };

    this.getTablesInformation = function (onTableDataLoaded) {
      VoltDBService.GetDataTablesInformation(function (inestConnection) {
        populateTableTypes(inestConnection);
        populateTablesInformation(inestConnection);
        onTableDataLoaded(inestConnection.Metadata["@Statistics_TABLE"].data);
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

    this.getImporterGraphInformation = function (onInformationLoaded) {
      var importerDetails = {};
      VoltDBService.GetImporterInformation(function (connection) {
        getImporterDetails(connection, importerDetails);
        onInformationLoaded(importerDetails);
      });
    };

    this.getExportTableInformation = function (onInformationLoaded) {
      var exportDetails = {};
      var deploymentDetails = {};
      VoltDBService.GetShortApiDeployment(function (connection) {
        deploymentDetails = connection.Metadata["SHORTAPI_DEPLOYMENT"];
      });
      VoltDBService.GetExporterInformation(function (connection) {
        getExportTableDetails(connection, exportDetails);
        onInformationLoaded(exportDetails, deploymentDetails);
      });
    };

    this.getThroughputGraphInformation = function (onInformationLoaded) {
      var throughputDetails = {};
      VoltDBService.GetExporterInformation(function (connection) {
        getThroughputDetails(connection, throughputDetails);
        onInformationLoaded(throughputDetails);
      });
    };

    this.getQueuedGraphInformation = function (onInformationLoaded) {
      var queuedDetails = {};
      VoltDBService.GetExporterInformation(function (connection) {
        getQueuedDetails(connection, queuedDetails);
        onInformationLoaded(queuedDetails);
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
        getReplicationDetails(
          connection,
          replicationStatus,
          "CLUSTER_REPLICA_INFORMATION"
        );
        onInformationLoaded(replicationStatus);
      });
    };
    //

    this.GetDrRoleInformation = function (onInformationLoaded) {
      var drRoleInfo = {};
      VoltDBService.GetDrRoleInformation(function (connection) {
        getDrRoleDetails(connection, drRoleInfo);
        onInformationLoaded(drRoleInfo);
      });
    };

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
        var exportTableData = {};
        getTableData(
          connection,
          tablesData,
          viewsData,
          proceduresData,
          procedureColumnsData,
          sysProceduresData,
          "TABLE_INFORMATION",
          exportTableData
        );
        onInformationLoaded(
          tablesData,
          viewsData,
          proceduresData,
          procedureColumnsData,
          sysProceduresData,
          exportTableData
        );
      });
    };

    this.GetTableInformationOnly = function (onInformationLoaded) {
      VoltDBService.GetTableInformationOnly(function (connection) {
        var tablesData = {};
        getTableDataOnly(connection, tablesData, "TABLE_INFORMATION_ONLY");
        onInformationLoaded(tablesData);
      });
    };

    this.GetTableInformationClientPort = function () {
      VoltDBService.GetTableInformationClientPort(function (connection) {
        var tablesData = {};
        var viewsData = {};
        var proceduresData = {};
        var procedureColumnsData = {};
        var sysProceduresData = {};
        getTableData(
          connection,
          tablesData,
          viewsData,
          proceduresData,
          procedureColumnsData,
          sysProceduresData,
          "TABLE_INFORMATION_CLIENTPORT"
        );
      });
    };

    this.GetProcedureProfileInformation = function (onInformationLoaded) {
      var procedureProfileObj = {};

      VoltDBService.GetProceduresInformation(function (connection) {
        getProcedureProfileInfo(connection, procedureProfileObj);
        onInformationLoaded(procedureProfileObj);
      });
    };

    this.GetProcedureDetailInformation = function (onProceduresDataLoaded) {
      var procedureDetailObj = [];

      VoltDBService.GetProcedureDetailInformation(function (nestConnection) {
        getProcedureDetailInfo(nestConnection, procedureDetailObj);
        onProceduresDataLoaded(procedureDetailObj);
      });
    };

    this.GetHostNodesHtml = function (callback) {
      try {
        VoltDBService.GetHostNodes(function (connection, state) {
          populateSystemInformation(connection);
          callback();
        });
      } catch (e) { }
    };

    this.GetClusterHealth = function (callback) {
      if (systemOverview == null || systemOverview == undefined) {
        alert("Error: Unable to extract cluster health information.");
        return;
      }
      var hostCount = 0;
      VoltDBService.GetSystemInformationDeployment(function (connection) {
        activeCount = 0;
        joiningCount = 0;
        missingCount = 0;
        alertCount = 0;
        hostCount = getHostCount(
          connection.Metadata["@SystemInformation_DEPLOYMENT"]
        );

        jQuery.each(systemOverview, function (id, val) {
          if (
            val["CLUSTERSTATE"] == "RUNNING" ||
            val["CLUSTERSTATE"] == "PAUSED"
          )
            activeCount++;
        });

        totalServerCount = hostCount;
        missingCount = totalServerCount - (activeCount + joiningCount);

        if (missingCount < 0) missingCount = 0;

        var html =
          '<li class="activeIcon">Active <span id="activeCount">(' +
          activeCount +
          ")</span></li>" +
          '<!--<li class="joiningIcon">Joining <span id="joiningCount">(' +
          joiningCount +
          ")</span></li>-->" +
          '<li class="missingIcon">Missing <span id="missingCount">(' +
          missingCount +
          ")</span></li>";

        var alertHtml = "";

        jQuery.each(systemOverview, function (id, val) {
          var hostName;
          var hostIp;
          hostName = val["HOSTNAME"];
          hostIp = val["IPADDRESS"];
          var threshold =
            VoltDbUI.getFromLocalStorage("alert-threshold") != undefined
              ? VoltDbUI.getFromLocalStorage("alert-threshold")
              : 70;
          if (systemMemory[hostName]["MEMORYUSAGE"] >= threshold) {
            alertHtml +=
              '<tr><td class="active alertAlign"  width="40%"><a data-ip="' +
              systemMemory[val["HOSTNAME"]]["HOST_ID"] +
              '" onclick="alertNodeClicked(this);" href="#">' +
              hostName +
              "</a> </td>" +
              '<td width="30%">' +
              hostIp +
              "</td>" +
              '<td width="30%"><span class="alert">' +
              systemMemory[hostName]["MEMORYUSAGE"] +
              "%</span></td></tr>";
            alertCount++;
          }
        });

        if (alertCount > 0) {
          html +=
            '<li class="alertIcon"><a href="#memoryAlerts" id="showMemoryAlerts">Alert <span>(' +
            alertCount +
            ")</span></a></li>";
        }

        callback(html, alertHtml);
      });
    };

    var getHostCount = function (deploymentInfo) {
      var hostCount = 0;
      if (
        deploymentInfo != undefined &&
        !$.isEmptyObject(deploymentInfo.data)
      ) {
        deploymentInfo.data.forEach(function (entry) {
          if (entry[0] == "hostcount") hostCount = entry[1];
        });
      }
      return hostCount;
    };

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
      if (
        connection != null &&
        connection.Metadata["SHORTAPI_PROFILE"] != null
      ) {
        var data = connection.Metadata["SHORTAPI_PROFILE"];
        if (data.permissions != null) {
          $.each(data.permissions, function (index, value) {
            if (value.toUpperCase().trim() == "ADMIN") {
              isAdmin = true;
              return true;
            }
            return false;
          });
        }
      }
      return isAdmin;
    };

    this.checkRolesUpdate = function () {
      const url = `api/1.0/?Procedure=%40SystemCatalog&Parameters=%5B"USERS"%5D`;
      var usersList = [];
      $.ajax({
        type: "get",
        url: url,
        success: function (response) {
          var result = response.results[0].data;
          usersList = result.map((user) => {
            var user = {
              name: user[0],
              role: user[1],
            }
            return user;
          });
          voltDbRenderer.usersList = usersList;
        }
      }).done(function () {
        var currentUserRole = VoltDbUI.getCookie("role");
        var currentUser = VoltDbUI.getCookie("username");

        if (currentUserRole !== 'null' && usersList.length > 0) {
          var updatedUserRole = usersList.length > 0 && usersList.filter(user => user.name === currentUser)[0].role;
          var isRoleChanged = currentUserRole === updatedUserRole ? false : true;
          if (updatedUserRole.toLowerCase().includes('administrator')) {
            VoltDbAdminConfig.isAdmin = true;
          } else {
            VoltDBService.GetShortApiProfile(function (connection) {
              var permissionList = connection.Metadata["SHORTAPI_PROFILE"].permissions;
              $.each(permissionList, function (index, value) {
                if (value.toUpperCase() === "ADMIN") {
                  VoltDbAdminConfig.isAdmin = true;
                } else {
                  VoltDbAdminConfig.isAdmin = false;
                }
              })
            });
          }
          VoltDbAdminConfig.isRoleChanged = isRoleChanged;
          if (isRoleChanged) {
            $("#rolePopup").trigger("click");
            saveSessionCookie('role', updatedUserRole);
            VoltDbAdminConfig.isReloadRequired = true;

            if (VoltDbAdminConfig.isAdmin) {
              $("#navAdmin").show();
              // loadAdminPage();
            } else {
              $("#navAdmin").hide();
            }
          }
        } else {
          VoltDbAdminConfig.isAdmin = true;
          VoltDbAdminConfig.isReloadRequired = false;
        }
      });
    }

    var loadAdminDeploymentInformation = function (connection) {
      var adminConfigValues = {};
      var currentUser = VoltDbUI.getCookie('username');
      var currentRole = VoltDbUI.getCookie('role');

      if (
        connection != null &&
        connection.Metadata["SHORTAPI_DEPLOYMENT"] != null
      ) {
        var data = connection.Metadata["SHORTAPI_DEPLOYMENT"];
        var usersList = voltDbRenderer.usersList;
        getListOfRoles();

        if (data.status === -1) {
          return;
        }

        if (usersList.length === 0) {
          VoltDbAdminConfig.isSecurityEnabled = false;
        } else VoltDbAdminConfig.isSecurityEnabled = true;

        //The user does not have permission to view admin details.
        if (currentUser !== 'null' && currentRole === 'administrator') {
          adminConfigValues.VMCNoPermission = false;
          VoltDbAdminConfig.isAdmin = true;
        } else if (!hasAdminPrivileges) {
          adminConfigValues.VMCNoPermission = true;
          return adminConfigValues;
        } else if (usersList.length <= 0 && currentUser === 'null') {
          saveSessionCookie('role', 'administrator')
          adminConfigValues.VMCNoPermission = false;
          VoltDbAdminConfig.isAdmin = true;
        }
        adminConfigValues["sitesperhost"] = data.cluster.sitesperhost;
        adminConfigValues["kSafety"] = data.cluster.kfactor;
        adminConfigValues["partitionDetection"] =
          data.partitionDetection != null
            ? data.partitionDetection.enabled
            : false;
        adminConfigValues["securityEnabled"] =
          data.security != null ? data.security.enabled : false;

        if (data.users != null && data.users.user != null)
          adminConfigValues["users"] =
            data.users.user.length > 0 ? data.users.user : null;
        else adminConfigValues["users"] = null;

        //HTTP Access
        if (data.httpd != null) {
          adminConfigValues["httpEnabled"] = data.httpd.enabled;
          adminConfigValues["jsonEnabled"] = data.httpd.jsonapi.enabled;
        }

        //Auto Snapshot
        if (data.snapshot != null) {
          adminConfigValues["snapshotEnabled"] = data.snapshot.enabled;
          adminConfigValues["frequency"] = data.snapshot.frequency;
          adminConfigValues["retained"] = data.snapshot.retain;
          adminConfigValues["filePrefix"] = data.snapshot.prefix;
        }

        //Command Logging
        if (data.commandlog != null) {
          adminConfigValues["commandLogEnabled"] = data.commandlog.enabled;
          if (data.commandlog.frequency != null) {
            adminConfigValues["commandLogFrequencyTime"] =
              data.commandlog.frequency.time;
            adminConfigValues["commandLogFrequencyTransactions"] =
              data.commandlog.frequency.transactions;
          }
          adminConfigValues["logSegmentSize"] = data.commandlog.logsize;
        }

        //Export
        if (data["export"] != null) {
          adminConfigValues["export"] = data["export"].enabled;
          adminConfigValues["targets"] = data["export"].target;
          adminConfigValues["configuration"] = data["export"].configuration;
        }

        //Advanced
        if (data.heartbeat != null) {
          adminConfigValues["heartBeatTimeout"] = data.heartbeat.timeout;
        }

        if (data.systemsettings != null && data.systemsettings.query != null) {
          adminConfigValues["queryTimeout"] = data.systemsettings.query.timeout;
        }

        if (data.systemsettings != null) {
          if (data.systemsettings.temptables != null)
            adminConfigValues["tempTablesMaxSize"] =
              data.systemsettings.temptables.maxsize;

          if (data.systemsettings.snapshot != null)
            adminConfigValues["snapshotPriority"] =
              data.systemsettings.snapshot.priority;

          if (data.systemsettings.resourcemonitor != null) {
            if (data.systemsettings.resourcemonitor.memorylimit != null) {
              adminConfigValues["memorylimit"] =
                data.systemsettings.resourcemonitor.memorylimit.size;
            }
          }

          if (data.systemsettings.resourcemonitor != null) {
            if (data.systemsettings.resourcemonitor.disklimit != null) {
              adminConfigValues["disklimit"] =
                data.systemsettings.resourcemonitor.disklimit;
            }
          }
        }

        //Directory
        if (data.paths != null) {
          if (data.paths.voltdbroot != null)
            adminConfigValues["voltdbRoot"] = data.paths.voltdbroot.path;

          if (data.paths.snapshots != null)
            adminConfigValues["snapshotPath"] = data.paths.snapshots.path;

          if (data.paths.exportoverflow != null)
            adminConfigValues["exportOverflow"] =
              data.paths.exportoverflow.path;

          if (data.paths.commandlog != null)
            adminConfigValues["commandLogPath"] = data.paths.commandlog.path;

          if (data.paths.commandlogsnapshot != null)
            adminConfigValues["commandLogSnapshotPath"] =
              data.paths.commandlogsnapshot.path;

          if (data.paths.droverflow != null)
            adminConfigValues["drOverflowPath"] = data.paths.droverflow.path;
        }

        //dr
        if (data.dr != null) {
          adminConfigValues["drConnectionSource"] =
            data.dr.connection != null ? data.dr.connection.source : "";
          adminConfigValues["drId"] = data.dr.id;
          adminConfigValues["drListen"] = data.dr.listen;
          adminConfigValues["drPort"] = data.dr.port;
        }

        //import
        if (data["import"] != null) {
          adminConfigValues["importConfiguration"] =
            data["import"].configuration;
        }
        //snmp

        if (data["snmp"] != null) {
          adminConfigValues["target"] = data["snmp"].target;
          adminConfigValues["community"] = data["snmp"].community;
          adminConfigValues["username"] = data["snmp"].username;
          adminConfigValues["enabled"] = data["snmp"].enabled;
          adminConfigValues["authprotocol"] = data["snmp"].authprotocol;
          adminConfigValues["authkey"] = data["snmp"].authkey;
          adminConfigValues["privacyprotocol"] = data["snmp"].privacyprotocol;
          adminConfigValues["privacykey"] = data["snmp"].privacykey;
        }
      }

      return adminConfigValues;
    };

    var loadExportProperties = function (connection) {
      var exportProperties = {};
      if (
        connection != null &&
        connection.Metadata["SHORTAPI_DEPLOYMENT_EXPORTTYPES"] != null
      ) {
        var data = connection.Metadata["SHORTAPI_DEPLOYMENT_EXPORTTYPES"];
        exportProperties["type"] = data.types;
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
      if (connection.Metadata["@SystemInformation_OVERVIEW_status"] == -3) {
        VoltDbUI.hasPermissionToView = false;

        if (!$("#loginWarningPopup").is(":visible")) {
          $("#loginWarningPopupMsg").text(
            "Security settings have been changed. You no longer have permission to view this page."
          );
          $("#loginWarnPopup").trigger("click");
        }
        return;
      } else if (connection.Metadata["@SystemInformation_OVERVIEW"] == null) {
        return;
      }

      connection.Metadata["@SystemInformation_OVERVIEW"].data.forEach(function (
        entry
      ) {
        var singleData = entry;
        var id = singleData[0];
        if (singleData[1] == "IPADDRESS") {
          if (singleData[2] == VoltDBConfig.GetDefaultServerIP()) {
            voltDbRenderer.isHost = true;
          } else {
            voltDbRenderer.isHost = false;
            serverOverview[id] = {};
            ipAddress = singleData[2];
          }
        }

        if (singleData[1] == "HOSTNAME") {
          if (voltDbRenderer.isHost) {
            voltDbRenderer.currentHost = singleData[2];
          }
          if ($.inArray(singleData[2], voltDbRenderer.hostNames) == -1)
            voltDbRenderer.hostNames.push(singleData[2]);
        }

        if (singleData[1] == "KUBERNETES") {
          if (singleData[2] == "true") {
            console.log("Detected Kubernetes.");
            set_kubernetes_admin();
          }
        }

        //assign entry in data object to 'currentServerOverview' if object being iterated is not a current host object
        //otherwise to a updatedSystemOverview
        if (voltDbRenderer.isHost) {
          currentServerOverview[singleData[1]] = singleData[2];

          if (singleData[1] == "LOG4JPORT") {
            currentServerOverview["NODEID"] = id;
          }
        } else {
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
      }
    };

    var populateTablesInformation = function (connection) {
      var counter = 0;
      connection.Metadata["@Statistics_TABLE"].schema.forEach(function (
        columnInfo
      ) {
        if (columnInfo["name"] == "HOST_ID") hostIndex = counter;

        if (columnInfo["name"] == "TABLE_NAME") tableNameIndex = counter;
        else if (columnInfo["name"] == "PARTITION_ID") partitionIndex = counter;
        else if (columnInfo["name"] == "TUPLE_COUNT") tupleCountIndex = counter;

        counter++;
      });

      counter = 0;

      formatTableData(connection);
    };

    var populateProceduresInformation = function (connection) {
      var counter = 0;

      if (connection != null) {
        connection.Metadata["@Statistics_PROCEDUREPROFILE"].schema.forEach(
          function (columnInfo) {
            if (columnInfo["name"] == "PROCEDURE") procedureNameIndex = counter;
            else if (columnInfo["name"] == "INVOCATIONS")
              invocationsIndex = counter;
            else if (columnInfo["name"] == "MIN") minLatencyIndex = counter;
            else if (columnInfo["name"] == "MAX") maxLatencyIndex = counter;
            else if (columnInfo["name"] == "AVG") avgLatencyIndex = counter;
            else if (columnInfo["name"] == "WEIGHTED_PERC")
              perExecutionIndex = counter;

            counter++;
          }
        );

        populateProcedureData(connection);
      }
    };

    var populateProcedureData = function (connection) {
      var procedureCount = 0;
      var procedure = {};
      procedureData = [];
      connection.Metadata["@Statistics_PROCEDUREPROFILE"].data.forEach(
        function (entry) {
          var name = entry[procedureNameIndex];
          minLatency = entry[minLatencyIndex] * Math.pow(10, -6);
          maxLatency = entry[maxLatencyIndex] * Math.pow(10, -6);
          avgLatency = entry[avgLatencyIndex] * Math.pow(10, -6);

          minLatency = parseFloat(minLatency.toFixed(2));
          maxLatency = parseFloat(maxLatency.toFixed(2));
          avgLatency = parseFloat(avgLatency.toFixed(2));

          if (!procedureData.hasOwnProperty(name)) {
            procedure = {
              PROCEDURE: entry[procedureNameIndex],
              INVOCATIONS: entry[invocationsIndex],
              MIN_LATENCY: minLatency,
              MAX_LATENCY: maxLatency,
              AVG_LATENCY: avgLatency,
              PERC_EXECUTION: entry[perExecutionIndex],
            };
            procedureData.push(procedure);

            procedureCount++;
          }
        }
      );
    };

    var populateTableTypes = function (connection) {
      var counter = 0;
      var tableName;
      var tableNameIndex = 0;
      var tableTypeIndex = 0;
      var remarksIndex = 0;

      connection.Metadata["@SystemCatalog_TABLES"].schema.forEach(function (
        columnInfo
      ) {
        if (columnInfo["name"] == "TABLE_NAME") tableNameIndex = counter;

        if (columnInfo["name"] == "TABLE_TYPE") tableTypeIndex = counter;

        if (columnInfo["name"] == "REMARKS") remarksIndex = counter;

        counter++;
      });

      connection.Metadata["@SystemCatalog_TABLES"].data.forEach(function (
        entry
      ) {
        tableName = entry[tableNameIndex];
        if (!schemaCatalogTableTypes.hasOwnProperty(tableName)) {
          schemaCatalogTableTypes[tableName] = {};
        }
        schemaCatalogTableTypes[tableName]["TABLE_NAME"] =
          entry[tableNameIndex];

        if (entry[remarksIndex] != null) {
          schemaCatalogTableTypes[tableName]["REMARKS"] =
            JSON.parse(entry[remarksIndex]).partitionColumn != null
              ? "PARTITIONED"
              : "REPLICATED";
          schemaCatalogTableTypes[tableName]["drEnabled"] = JSON.parse(
            entry[remarksIndex]
          ).drEnabled;
        } else {
          schemaCatalogTableTypes[tableName]["REMARKS"] = "REPLICATED";
        }
        schemaCatalogTableTypes[tableName]["TABLE_TYPE"] =
          entry[tableTypeIndex];
      });
    };

    this.mapNodeInformationByStatus = function (callback) {
      var counter = 0;
      var memoryThreshold =
        VoltDbUI.getFromLocalStorage("alert-threshold") != ""
          ? VoltDbUI.getFromLocalStorage("alert-threshold")
          : -1;
      var htmlMarkups = { ServerInformation: [] };
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
          if (
            hostName != null &&
            currentServer == hostName &&
            val["CLUSTERSTATE"] == "RUNNING"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                '<tr class="filterClass serverActive" ><td class="active monitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            } else {
              htmlMarkup =
                '<tr class="filterClass serverActive"><td class="active monitoring" width="40%"> <a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          } else if (
            hostName != null &&
            currentServer != hostName &&
            val["CLUSTERSTATE"] == "RUNNING"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                '<tr class="filterClass"><td class="active" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span>" +
                '<span class="hostIdHidden" style="display:none">"' +
                systemMemory[hostName]["HOST_ID"] +
                "</span></td></tr>";
            } else {
              htmlMarkup =
                '<tr class="filterClass"><td class="active" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          } else if (
            hostName != null &&
            currentServer == hostName &&
            val["CLUSTERSTATE"] == "PAUSED"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span>" +
                '<span class="hostIdHidden" style="display:none">"' +
                systemMemory[hostName]["HOST_ID"] +
                "</span></td></tr>";
            } else {
              htmlMarkup =
                '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          } else if (
            hostName != null &&
            currentServer != hostName &&
            val["CLUSTERSTATE"] == "PAUSED"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td  width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td  width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span>" +
                '<span class="hostIdHidden" style="display:none">"' +
                systemMemory[hostName]["HOST_ID"] +
                "</span></td></tr>";
            } else {
              htmlMarkup =
                '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          } else if (hostName != null && val["CLUSTERSTATE"] == "JOINING") {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="joining" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span>" +
                '<span class="hostIdHidden" style="display:none">"' +
                systemMemory[hostName]["HOST_ID"] +
                "</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="joining" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }
        } else {
          /********************************************************************************************
                    "currentServerHtml" is validated to verify if current server to be monitored is already set
                    *********************************************************************************************/
          if (
            hostName != null &&
            currentServerHtml != "" &&
            currentServerHtml == hostName &&
            val["CLUSTERSTATE"] == "RUNNING"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass serverActive"><td class="active monitoring"  width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass serverActive"><td class="active monitoring"  width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }

          if (
            hostName != null &&
            currentServerHtml != hostName &&
            val["CLUSTERSTATE"] == "RUNNING"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="active"  width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="active"  width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }

          if (
            hostName != null &&
            currentServerHtml == hostName &&
            val["CLUSTERSTATE"] == "PAUSED"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span>" +
                '<span class="hostIdHidden" style="display:none">"' +
                systemMemory[hostName]["HOST_ID"] +
                "</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass serverActive"><td class="pauseActiveMonitoring" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }

          if (
            hostName != null &&
            currentServerHtml != hostName &&
            val["CLUSTERSTATE"] == "PAUSED"
          ) {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="pauseMonitoring" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }
          if (hostName != null && val["CLUSTERSTATE"] == "JOINING") {
            if (systemMemory[hostName]["MEMORYUSAGE"] >= memoryThreshold) {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="joining" width="40%"><a class="alertIconServ serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status alert">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            } else {
              htmlMarkup =
                htmlMarkup +
                '<tr class="filterClass"><td class="joining" width="40%"><a class="serverNameAlign" data-ip="' +
                systemMemory[hostName]["HOST_ID"] +
                '" href="javascript:void(0);">' +
                hostName +
                "</a></td>" +
                '<td width="30%"><span class="servIpNum">' +
                hostIP +
                "</span></td>" +
                '<td width="30%"><span class="memory-status">' +
                systemMemory[hostName]["MEMORYUSAGE"] +
                "%</span></td></tr>";
            }
          }
        }
        counter++;
      });
      htmlMarkups.ServerInformation.push({ ServersList: htmlMarkup });
      htmlMarkups.ServerInformation.push({ CurrentServer: currentServerHtml });
      callback(htmlMarkups);
    };

    this.mapTableInformation = function (callback) {
      var counter = 0;
      var tablePageStartIndex = 0;

      if (tableData == null || tableData == undefined) {
        alert("Error: Unable to extract Table Data");
        return;
      } else {
        voltDbRenderer.drTablesArray = [];
        for (var key in tableData) {
          if (tableData[key]["drEnabled"] == "true") {
            voltDbRenderer.drTablesArray.push(tableData[key]["TABLE_NAME"]);
          }
        }
      }
      callback(tableData);
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

    var getLatencyDetails = function (connection, latency) {
      var colIndex = {};
      var counter = 0;
      var tpsClusterValue = 0;
      var latencyClusterValue = 0;
      var timeStamp = new Date().getTime();
      connection.Metadata["@Statistics_LATENCY"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "P99" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "TPS"
        )
          colIndex[columnInfo["name"]] = counter;

        counter++;
      });
      latency["NODE_DETAILS"] = {};
      connection.Metadata["@Statistics_LATENCY"].data.forEach(function (info) {
        //Get node details
        var hostName = info[colIndex["HOSTNAME"]];
        if (!latency["NODE_DETAILS"].hasOwnProperty(hostName)) {
          latency["NODE_DETAILS"][hostName] = {};
        }
        latency["NODE_DETAILS"][hostName]["TIMESTAMP"] =
          info[colIndex["TIMESTAMP"]];
        latency["NODE_DETAILS"][hostName]["P99"] = info[colIndex["P99"]] / 1000;
        latency["NODE_DETAILS"][hostName]["TPS"] = info[colIndex["TPS"]];

        //Get cluster details
        latencyClusterValue =
          info[colIndex["P99"]] > latencyClusterValue
            ? info[colIndex["P99"]]
            : latencyClusterValue;
        tpsClusterValue += info[colIndex["TPS"]];
        timeStamp = info[colIndex["TIMESTAMP"]];
      });
      latency["CLUSTER_DETAILS"] = {};
      latency["CLUSTER_DETAILS"]["TPS"] = tpsClusterValue;
      latency["CLUSTER_DETAILS"]["P99"] = latencyClusterValue / 1000;
      latency["CLUSTER_DETAILS"]["TIMESTAMP"] = timeStamp;
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
        hostNameList = { 1: { HOSTNAME: getCurrentServer() } };
      } else {
        hostNameList = systemOverview;
      }

      if (connection.Metadata["@Statistics_MEMORY" + suffix] == null) {
        return;
      }

      connection.Metadata["@Statistics_MEMORY" + suffix].schema.forEach(
        function (columnInfo) {
          if (columnInfo["name"] == "HOSTNAME") hostNameIndex = counter;
          else if (columnInfo["name"] == "TUPLEDATA") tupledDataIndex = counter;
          else if (columnInfo["name"] == "TUPLECOUNT")
            tupleCountIndex = counter;
          else if (columnInfo["name"] == "RSS") rssIndex = counter;
          else if (columnInfo["name"] == "PHYSICALMEMORY")
            physicalMemoryIndex = counter;
          else if (columnInfo["name"] == "TIMESTAMP") timeStampIndex = counter;
          else if (columnInfo["name"] == "HOST_ID") idIndex = counter;
          else if (columnInfo["name"] == "JAVAMAXHEAP")
            javaMaxHeapIndex = counter;
          counter++;
        }
      );

      connection.Metadata["@Statistics_MEMORY" + suffix].data.forEach(function (
        memoryInfo
      ) {
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
            sysMemory[hostName]["PHYSICALMEMORY"] =
              memoryInfo[physicalMemoryIndex];
            sysMemory[hostName]["MAXJAVAHEAP"] = memoryInfo[javaMaxHeapIndex];

            var memoryUsage =
              (sysMemory[hostName]["RSS"] /
                sysMemory[hostName]["PHYSICALMEMORY"]) *
              100;
            sysMemory[hostName]["MEMORYUSAGE"] =
              Math.round(memoryUsage * 100) / 100;

            voltDbRenderer.memoryDetails.push(memoryInfo[timeStampIndex]);
          }
        });
      });
    };

    var getCpuDetails = function (connection, sysMemory) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_CPU"] == null) {
        return;
      }

      connection.Metadata["@Statistics_CPU"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "PERCENT_USED" ||
          columnInfo["name"] == "TIMESTAMP"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_CPU"].data.forEach(function (info) {
        var hostName = info[colIndex["HOSTNAME"]];
        if (!sysMemory.hasOwnProperty(hostName)) {
          sysMemory[hostName] = {};
        }
        sysMemory[hostName]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
        sysMemory[hostName]["PERCENT_USED"] = info[colIndex["PERCENT_USED"]];
      });
    };

    var getImporterDetails = function (connection, importerDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_IMPORTER"] == null) {
        return;
      }

      connection.Metadata["@Statistics_IMPORTER"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "SUCCESSES" ||
          columnInfo["name"] == "FAILURES" ||
          columnInfo["name"] == "OUTSTANDING_REQUESTS" ||
          columnInfo["name"] == "IMPORTER_NAME"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      if (connection.Metadata["@Statistics_IMPORTER"].data.length > 0) {
        var rowCount = connection.Metadata["@Statistics_IMPORTER"].data.length;
        var success = 0;
        var failures = 0;
        var outStanding = 0;
        var importerName = "";
        connection.Metadata["@Statistics_IMPORTER"].data.forEach(function (
          info
        ) {
          if (importerName != info[colIndex["IMPORTER_NAME"]]) {
            importerName = info[colIndex["IMPORTER_NAME"]];
            success = 0;
            failures = 0;
            outStanding = 0;
          }
          if (!importerDetails.hasOwnProperty("SUCCESSES")) {
            importerDetails["SUCCESSES"] = {};
            importerDetails["FAILURES"] = {};
            importerDetails["OUTSTANDING_REQUESTS"] = {};
          }
          outStanding += info[colIndex["OUTSTANDING_REQUESTS"]];
          success += info[colIndex["SUCCESSES"]];
          failures += info[colIndex["FAILURES"]];

          importerDetails["SUCCESSES"][importerName] = success;
          importerDetails["SUCCESSES"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
          importerDetails["FAILURES"][importerName] = failures;
          importerDetails["FAILURES"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
          importerDetails["OUTSTANDING_REQUESTS"][importerName] = outStanding;
          importerDetails["HOSTNAME"] = info[colIndex["HOSTNAME"]];
          importerDetails["OUTSTANDING_REQUESTS"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
        });
      }
    };

    var getExportTableDetails = function (connection, exporterDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_EXPORT"] == null) {
        return;
      }

      connection.Metadata["@Statistics_EXPORT"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TUPLE_COUNT" ||
          columnInfo["name"] == "TUPLE_PENDING" ||
          columnInfo["name"] == "SOURCE" ||
          columnInfo["name"] == "TARGET" ||
          columnInfo["name"] == "ACTIVE"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      if (connection.Metadata["@Statistics_EXPORT"].data.length > 0) {
        var tuple_count = {}; // For keeping separate sums for each id
        var tuple_pending = {};
        var id = "";
        connection.Metadata["@Statistics_EXPORT"].data.forEach(function (info) {
          if (id != info[colIndex["SOURCE"]]) {
            id = info[colIndex["SOURCE"]];
          }
          if (isNaN(tuple_count[id])) {
            // Catches when tuple_count has not started tracking a certain id
            tuple_count[id] = 0;
          }
          if (isNaN(tuple_pending[id])) {
            tuple_pending[id] = 0;
          }
          if (!exporterDetails.hasOwnProperty("TARGET")) {
            // Add property for each group of data we want to get
            exporterDetails["TARGET"] = {};
          }
          if (!exporterDetails.hasOwnProperty("TUPLE_COUNT")) {
            exporterDetails["TUPLE_COUNT"] = {};
          }
          if (!exporterDetails.hasOwnProperty("TUPLE_PENDING")) {
            exporterDetails["TUPLE_PENDING"] = {};
          }
          if (!exporterDetails.hasOwnProperty("ACTIVE")) {
            exporterDetails["ACTIVE"] = {};
          }

          tuple_count[id] += info[colIndex["TUPLE_COUNT"]];
          tuple_pending[id] += info[colIndex["TUPLE_PENDING"]];

          exporterDetails["TARGET"][id] = info[colIndex["TARGET"]];
          exporterDetails["TUPLE_COUNT"][id] = tuple_count[id];
          exporterDetails["TUPLE_PENDING"][id] = tuple_pending[id];
          exporterDetails["TUPLE_COUNT"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
          exporterDetails["HOSTNAME"] = info[colIndex["HOSTNAME"]];
          exporterDetails["ACTIVE"][id] = info[colIndex["ACTIVE"]];
        });
      }
    };

    var getThroughputDetails = function (connection, exporterDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_EXPORT"] == null) {
        return;
      }

      connection.Metadata["@Statistics_EXPORT"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TUPLE_COUNT" ||
          columnInfo["name"] == "SOURCE"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      if (connection.Metadata["@Statistics_EXPORT"].data.length > 0) {
        var tuple_count = {};
        var id = "";
        connection.Metadata["@Statistics_EXPORT"].data.forEach(function (info) {
          if (id != info[colIndex["SOURCE"]]) {
            id = info[colIndex["SOURCE"]];
          }
          if (isNaN(tuple_count[id])) {
            tuple_count[id] = 0;
          }
          if (!exporterDetails.hasOwnProperty("TUPLE_COUNT")) {
            exporterDetails["TUPLE_COUNT"] = {};
          }
          tuple_count[id] += info[colIndex["TUPLE_COUNT"]];
          exporterDetails["TUPLE_COUNT"][id] = tuple_count[id];
          exporterDetails["TUPLE_COUNT"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
          exporterDetails["HOSTNAME"] = info[colIndex["HOSTNAME"]];
        });
      }
    };

    var getQueuedDetails = function (connection, exporterDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_EXPORT"] == null) {
        return;
      }

      connection.Metadata["@Statistics_EXPORT"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TUPLE_PENDING" ||
          columnInfo["name"] == "SOURCE"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      if (connection.Metadata["@Statistics_EXPORT"].data.length > 0) {
        var tuple_count = {};
        var id = "";
        connection.Metadata["@Statistics_EXPORT"].data.forEach(function (info) {
          if (id != info[colIndex["SOURCE"]]) {
            id = info[colIndex["SOURCE"]];
          }
          if (isNaN(tuple_count[id])) {
            tuple_count[id] = 0;
          }
          if (!exporterDetails.hasOwnProperty("TUPLE_PENDING")) {
            exporterDetails["TUPLE_PENDING"] = {};
          }
          tuple_count[id] += info[colIndex["TUPLE_PENDING"]];
          exporterDetails["TUPLE_PENDING"][id] = tuple_count[id];
          exporterDetails["TUPLE_PENDING"]["TIMESTAMP"] =
            info[colIndex["TIMESTAMP"]];
          exporterDetails["HOSTNAME"] = info[colIndex["HOSTNAME"]];
        });
      }
    };

    var getLiveClientData = function (connection, clientInfo) {
      var trans = 0;
      var bytes = 0;
      var msgs = 0;
      if (!clientInfo.hasOwnProperty("CLIENTS")) clientInfo["CLIENTS"] = {};

      if (
        connection.Metadata["@Statistics_LIVECLIENTS"] == undefined ||
        $.isEmptyObject(connection.Metadata["@Statistics_LIVECLIENTS"].data)
      ) {
        clientInfo["CLIENTS"]["bytes"] = 0;
        clientInfo["CLIENTS"]["msgs"] = 0;
        clientInfo["CLIENTS"]["trans"] = 0;
        return;
      }

      connection.Metadata["@Statistics_LIVECLIENTS"].data.forEach(function (
        info
      ) {
        bytes += info[6];
        msgs += info[7];
        trans += info[8];
      });

      clientInfo["CLIENTS"]["bytes"] = bytes;
      clientInfo["CLIENTS"]["msgs"] = msgs;
      clientInfo["CLIENTS"]["trans"] = trans;
    };

    //Get DR Status Information
    var getDrStatus = function (connection, drDetails) {
      var colIndex = {};
      var counter = 0;

      if (!drDetails.hasOwnProperty("Details")) {
        drDetails["Details"] = {};
      }
      drDetails["Details"]["STATUS"] =
        connection.Metadata["@Statistics_DR_status"];

      if (
        connection.Metadata["@Statistics_DR_completeData"] == null ||
        $.isEmptyObject(connection.Metadata["@Statistics_DR_completeData"])
      ) {
        return;
      }

      connection.Metadata["@Statistics_DR_completeData"][1].schema.forEach(
        function (columnInfo) {
          if (
            columnInfo["name"] == "HOSTNAME" ||
            columnInfo["name"] == "TIMESTAMP" ||
            columnInfo["name"] == "SYNCSNAPSHOTSTATE" ||
            columnInfo["name"] == "STATE"
          )
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      connection.Metadata["@Statistics_DR_completeData"][1].data.forEach(
        function (info) {
          var hostName = info[colIndex["HOSTNAME"]];
          if (!drDetails.hasOwnProperty(hostName)) {
            drDetails[hostName] = {};
          }
          var isEnable = false;
          if (
            info[colIndex["STATE"]] != null &&
            info[colIndex["STATE"]].toLowerCase() != "off"
          )
            isEnable = true;
          drDetails[hostName]["MASTERENABLED"] = isEnable;
          drDetails[hostName]["STATE"] = info[colIndex["STATE"]];
          drDetails[hostName]["SYNCSNAPSHOTSTATE"] =
            info[colIndex["SYNCSNAPSHOTSTATE"]];
        }
      );
    };

    //Get DR Details Information
    var getDrDetails = function (connection, drDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_DR"] == null) {
        return;
      }

      connection.Metadata["@Statistics_DR_completeData"][0].schema.forEach(
        function (columnInfo) {
          if (
            columnInfo["name"] == "PARTITION_ID" ||
            columnInfo["name"] == "TOTALBUFFERS" ||
            columnInfo["name"] == "TIMESTAMP" ||
            columnInfo["name"] == "TOTALBYTES" ||
            columnInfo["name"] == "MODE" ||
            columnInfo["name"] == "LASTQUEUEDDRID" ||
            columnInfo["name"] == "LASTACKDRID" ||
            columnInfo["name"] == "LASTQUEUEDTIMESTAMP" ||
            columnInfo["name"] == "LASTACKTIMESTAMP" ||
            columnInfo["name"] == "CLUSTER_ID" ||
            columnInfo["name"] == "REMOTE_CLUSTER_ID"
          )
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      counter = 0;

      connection.Metadata["@Statistics_DR_completeData"][0].data.forEach(
        function (info) {
          var cluster_id = info[colIndex["CLUSTER_ID"]];
          var producer_cluster_id = info[colIndex["REMOTE_CLUSTER_ID"]];
          //Filter Master from Replica
          if (info[colIndex["MODE"]] == "NORMAL") {
            var partitionId = info[colIndex["PARTITION_ID"]];
            if (
              !drDetails.hasOwnProperty(cluster_id + "_" + producer_cluster_id)
            ) {
              drDetails[cluster_id + "_" + producer_cluster_id] = {};
            }

            if (
              !drDetails[cluster_id + "_" + producer_cluster_id].hasOwnProperty(
                partitionId
              )
            ) {
              drDetails[cluster_id + "_" + producer_cluster_id][partitionId] =
                [];
            }
            var partitionDetails = {};
            partitionDetails["TOTALBUFFERS"] = info[colIndex["TOTALBUFFERS"]];
            partitionDetails["TOTALBYTES"] = info[colIndex["TOTALBYTES"]];
            partitionDetails["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
            partitionDetails["LASTQUEUEDDRID"] =
              info[colIndex["LASTQUEUEDDRID"]];
            partitionDetails["LASTACKDRID"] = info[colIndex["LASTACKDRID"]];
            partitionDetails["LASTQUEUEDTIMESTAMP"] =
              info[colIndex["LASTQUEUEDTIMESTAMP"]];
            partitionDetails["LASTACKTIMESTAMP"] =
              info[colIndex["LASTACKTIMESTAMP"]];
            partitionDetails["REMOTE_CLUSTER_ID"] =
              info[colIndex["REMOTE_CLUSTER_ID"]];
            drDetails[cluster_id + "_" + producer_cluster_id][partitionId].push(
              partitionDetails
            );
          }
          drDetails["CLUSTER_ID"] = info[colIndex["CLUSTER_ID"]];
        }
      );
    };

    //Get Replication Information
    var getReplicationDetails = function (
      connection,
      replicationDetails,
      processName
    ) {
      var colIndex = {};
      var counter = 0;
      var replicaStatus = false;
      var hostName = "";
      var suffix = "_" + processName;
      if (connection.Metadata["@SystemInformation_Overview" + suffix] == null) {
        return;
      }

      connection.Metadata[
        "@SystemInformation_Overview" + suffix
      ].schema.forEach(function (columnInfo) {
        if (columnInfo["name"] == "KEY" || columnInfo["name"] == "VALUE")
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@SystemInformation_Overview" + suffix].data.forEach(
        function (info) {
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
        }
      );
    };
    //

    var getDrRoleDetails = function (connection, drRoleDetails) {
      var hostName = "";
      var drRoles = [];

      if (
        connection.Metadata["@Statistics_DRROLE"] == null ||
        $.isEmptyObject(connection.Metadata["@Statistics_DRROLE"])
      ) {
        return;
      }

      connection.Metadata["@Statistics_DRROLE"].data.forEach(function (info) {
        drRoles.push(info);
      });

      drRoleDetails["DRROLE"] = drRoles;
    };

    //Get DR Replication Data
    var getDrReplicationData = function (connection, replicationDetails) {
      var colIndex = {};
      var colIndex2 = {};
      var counter = 0;
      var replicationRate1M = 0;
      if (connection.Metadata["@Statistics_DRCONSUMER"] == null) {
        return;
      }
      connection.Metadata["@Statistics_DRCONSUMER"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "REPLICATION_RATE_1M" ||
          columnInfo["name"] == "HOST_ID" ||
          columnInfo["name"] == "STATE" ||
          columnInfo["name"] == "REPLICATION_RATE_5M" ||
          columnInfo["name"] == "CLUSTER_ID" ||
          columnInfo["name"] == "REMOTE_CLUSTER_ID"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      counter = 0;
      connection.Metadata[
        "@Statistics_DRCONSUMER_completeData"
      ][1].schema.forEach(function (columnInfo) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "IS_COVERED"
        )
          colIndex2[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_DRCONSUMER"].data.forEach(function (
        info
      ) {
        var cluster_id = info[colIndex["CLUSTER_ID"]];
        var producer_cluster_id = info[colIndex["REMOTE_CLUSTER_ID"]];

        if (!replicationDetails.hasOwnProperty("DR_GRAPH")) {
          replicationDetails["DR_GRAPH"] = {};
        }
        if (
          !replicationDetails["DR_GRAPH"].hasOwnProperty(
            cluster_id + "_" + producer_cluster_id
          )
        ) {
          replicationDetails["DR_GRAPH"][
            cluster_id + "_" + producer_cluster_id
          ] = {};
          replicationDetails["DR_GRAPH"][
            cluster_id + "_" + producer_cluster_id
          ]["REPLICATION_DATA"] = [];
          replicationDetails["DR_GRAPH"][
            cluster_id + "_" + producer_cluster_id
          ]["REPLICATION_RATE_1M"] = 0;
        }
        replicationRate1M =
          info[colIndex["REPLICATION_RATE_1M"]] == null ||
            info[colIndex["REPLICATION_RATE_1M"]] < 0
            ? 0
            : info[colIndex["REPLICATION_RATE_1M"]] / 1000;
        replicationDetails["DR_GRAPH"][cluster_id + "_" + producer_cluster_id][
          "REPLICATION_RATE_1M"
        ] += replicationRate1M;
        replicationDetails["DR_GRAPH"][cluster_id + "_" + producer_cluster_id][
          "TIMESTAMP"
        ] = info[colIndex["TIMESTAMP"]];
        var repData = {};
        repData["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
        replicationDetails["DR_GRAPH"]["TIMESTAMP"] =
          info[colIndex["TIMESTAMP"]];
        replicationDetails["DR_GRAPH"]["CLUSTER_ID"] =
          info[colIndex["CLUSTER_ID"]];
        repData["HOST_ID"] = info[colIndex["HOST_ID"]];
        repData["HOSTNAME"] = info[colIndex["HOSTNAME"]];
        repData["STATE"] = info[colIndex["STATE"]];
        repData["REPLICATION_RATE_5M"] =
          info[colIndex["REPLICATION_RATE_5M"]] / 1000;
        repData["REPLICATION_RATE_1M"] =
          info[colIndex["REPLICATION_RATE_1M"]] / 1000;
        replicationDetails["DR_GRAPH"]["REMOTE_CLUSTER_ID"] =
          info[colIndex["REMOTE_CLUSTER_ID"]];
        replicationDetails["DR_GRAPH"][cluster_id + "_" + producer_cluster_id][
          "REPLICATION_DATA"
        ].push(repData);
      });

      replicationDetails["DR_GRAPH"]["WARNING_COUNT"] =
        getReplicationNotCovered(
          connection.Metadata["@Statistics_DRCONSUMER_completeData"][1],
          colIndex2["IS_COVERED"]
        );
    };

    var getDrConsumerData = function (connection, drConsumerDetails) {
      var colIndex = {};
      var counter = 0;
      if (connection.Metadata["@Statistics_DRCONSUMER"] == null) {
        return;
      }

      connection.Metadata["@Statistics_DRCONSUMER"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "STATE"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_DRCONSUMER"].data.forEach(function (
        info
      ) {
        var hostName = info[colIndex["HOSTNAME"]].split("/")[0];
        if (!drConsumerDetails.hasOwnProperty(hostName)) {
          drConsumerDetails[hostName] = {};
        }
        drConsumerDetails[hostName]["STATE"] = info[colIndex["STATE"]];
      });
    };

    var getDrInformations = function (connection, replicationDetails) {
      var colIndex = {};
      var counter = 0;
      if (connection.Metadata["@Statistics_DRCONSUMER"] == null) {
        return;
      }

      connection.Metadata["@Statistics_DRCONSUMER"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "HOST_ID" ||
          columnInfo["name"] == "CLUSTER_ID"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_DRCONSUMER"].data.forEach(function (
        info
      ) {
        var hostName = info[colIndex["HOSTNAME"]].split("/")[0];
        if (!replicationDetails.hasOwnProperty(hostName)) {
          replicationDetails[hostName] = {};
        }
        var clusterId =
          info[colIndex["CLUSTER_ID"]] == undefined
            ? "N/A"
            : info[colIndex["CLUSTER_ID"]];
        replicationDetails[hostName]["CLUSTER_ID"] = clusterId;
      });
    };

    var getDrProducerInfo = function (connection, drDetails) {
      if (
        connection.Metadata["@Statistics_DRPRODUCER_completeData"] == null ||
        $.isEmptyObject(
          connection.Metadata["@Statistics_DRPRODUCER_completeData"]
        )
      ) {
        return;
      }

      var partition_max = drDetails["DrProducer"]["partition_max"];
      var partition_min = drDetails["DrProducer"]["partition_min"];
      var partition_min_host = drDetails["DrProducer"]["partition_min_host"];
      var colIndex = {};
      var counter = 0;

      $.each(partition_min, function (key, value) {
        // reset all min values to find the new min
        if ($.inArray(key, partition_max_key) != -1) {
          var partition_max_key = Object.keys(partition_max);
          partition_min[key] = partition_max[key];
        }
      });

      connection.Metadata[
        "@Statistics_DRPRODUCER_completeData"
      ][0].schema.forEach(function (columnInfo) {
        if (
          columnInfo["name"] == "PARTITION_ID" ||
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "LASTQUEUEDDRID" ||
          columnInfo["name"] == "LASTACKDRID" ||
          columnInfo["name"] == "STREAMTYPE" ||
          columnInfo["name"] == "TOTALBYTES"
        ) {
          colIndex[columnInfo["name"]] = counter;
        }
        counter++;
      });

      connection.Metadata[
        "@Statistics_DRPRODUCER_completeData"
      ][0].data.forEach(function (info) {
        var partition_min_key = Object.keys(partition_min);
        var partition_max_key = Object.keys(partition_max);

        var pid = info[colIndex["PARTITION_ID"]];
        var hostname = info[colIndex["HOSTNAME"]].toString();
        var last_queued = -1;
        var last_acked = -1;

        if (info[colIndex["LASTQUEUEDDRID"]].toString() != "None")
          last_queued = info[colIndex["LASTQUEUEDDRID"]];

        if (info[colIndex["LASTACKDRID"]].toString() != "None")
          last_acked = info[colIndex["LASTACKDRID"]];

        if (last_queued == -1 && last_acked == -1) return true;

        // check TOTALBYTES
        if (info[colIndex["TOTALBYTES"]] > 0) {
          // track the highest seen drId for each partition. use last queued to get the upper bound
          if ($.inArray(pid, partition_max_key) != -1)
            partition_max[pid] = Math.max(last_queued, partition_max[pid]);
          else partition_max[pid] = last_queued;

          if ($.inArray(pid, partition_min_key) != -1) {
            if (last_acked < partition_min[pid]) {
              // this replica is farther behind
              partition_min[pid] = last_acked;
            }
          } else {
            partition_min_host[pid] = [];
            partition_min[pid] = last_acked;
          }
          partition_min_host[pid].push(hostname);
        } else {
          // this hostname's partition has an empty InvocationBufferQueue
          if ($.inArray(pid, partition_min_key) != -1) {
            // it was not empty on a previous call
            partition_min_host[pid] = $.grep(
              partition_min_host[pid],
              function (value) {
                return value != hostname;
              }
            );
            if (
              partition_min_host[pid] == undefined ||
              partition_min_host[pid].length == 0
            ) {
              delete partition_min_host[pid];
              delete partition_min[pid];
            }
          }
          if ($.inArray(pid, partition_max_key) != -1) {
            if (partition_max[pid] > last_acked) {
              console.log(
                "DR Producer reports no data for partition " +
                pid +
                " on host " +
                hostname +
                " but last acked drId (" +
                last_acked +
                ") does not match other hosts last acked drId (" +
                partition_max[pid] +
                ")"
              );
            }
            partition_max[pid] = Math.max(last_acked, partition_max[pid]);
          } else {
            partition_max[pid] = last_acked;
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
      if (
        connection.Metadata[
        "@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData"
        ] == null ||
        $.isEmptyObject(
          connection.Metadata[
          "@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData"
          ]
        )
      ) {
        exportTableDetails["ExportTables"]["collection_time"] = 1;
        return;
      }
      var export_tables_with_data =
        exportTableDetails["ExportTables"]["export_tables_with_data"];
      var last_collection_time =
        exportTableDetails["ExportTables"]["last_collection_time"];
      var tablestats = null;
      var collection_time = 0;
      var export_tables = 0;
      if (
        connection.Metadata[
          "@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData"
        ][0].data.length == 0
      ) {
        exportTableDetails["ExportTables"]["collection_time"] = 1;
        return;
      } else {
        tablestats =
          connection.Metadata[
            "@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData"
          ][0].data;
        if (tablestats.length == 0) {
          exportTableDetails["ExportTables"]["collection_time"] = 1;
          return;
        }
        var firsttuple = tablestats[0];
        if (firsttuple[0] == last_collection_time) {
          // this statistic is the same cached set as the last call
          exportTableDetails["ExportTables"]["collection_time"] =
            collection_time;
          return;
        } else {
          collection_time = firsttuple[0];
        }
      }

      connection.Metadata[
        "@Statistics_TABLE_EXPORT_TABLE_INFORMATION_completeData"
      ][0].data.forEach(function (info) {
        // first look for streaming (export) tables
        if (info[6].toString() == "StreamedTable") {
          var pendingData = info[8];
          tablename = info[5].toString();
          pid = info[4];
          hostname = info[2].toString();
          if (pendingData > 0) {
            var export_tables_with_data_key = Object.keys(
              export_tables_with_data
            );
            if ($.inArray(tablename, export_tables_with_data_key) == -1)
              export_tables_with_data[tablename] = {};
            var tabledata = export_tables_with_data[tablename];
            var tableDataKeys = Object.keys(tabledata);
            if ($.inArray(hostname, tableDataKeys) == -1)
              tabledata[hostname] = [];
            tabledata[hostname].push(pid);
          } else {
            var export_tables_with_data_key = Object.keys(
              export_tables_with_data
            );
            if ($.inArray(tablename, export_tables_with_data_key) != -1) {
              var tabledata = export_tables_with_data[tablename];
              var tableDataKeys = Object.keys(tabledata);
              if ($.inArray(hostname, tableDataKeys) != -1) {
                tabledata[hostname] = $.grep(
                  tabledata[hostname],
                  function (value) {
                    return value != pid;
                  }
                );
                if ($.isEmptyObject(tabledata[hostname])) {
                  delete tabledata[hostname];
                  if ($.isEmptyObject(export_tables_with_data[tablename]))
                    delete export_tables_with_data[tablename];
                }
              }
            }
          }
        }
      });

      exportTableDetails["ExportTables"]["export_tables_with_data"] =
        export_tables_with_data;
      exportTableDetails["ExportTables"]["last_collection_time"] =
        last_collection_time;
      exportTableDetails["ExportTables"]["collection_time"] = collection_time;
    };

    var getProcedureProfileInfo = function (connection, procedureProfile) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_PROCEDUREPROFILE"] == undefined) {
        return;
      }

      connection.Metadata["@Statistics_PROCEDUREPROFILE"].schema.forEach(
        function (columnInfo) {
          if (
            columnInfo["name"] == "TIMESTAMP" ||
            columnInfo["name"] == "PROCEDURE" ||
            columnInfo["name"] == "INVOCATIONS" ||
            columnInfo["name"] == "AVG" ||
            columnInfo["name"] == "MIN" ||
            columnInfo["name"] == "MAX" ||
            columnInfo["name"] == "WEIGHTED_PERC"
          )
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      if (!procedureProfile.hasOwnProperty("PROCEDURE_PROFILE")) {
        procedureProfile["PROCEDURE_PROFILE"] = [];
      }

      connection.Metadata["@Statistics_PROCEDUREPROFILE"].data.forEach(
        function (info) {
          var profileObj = {
            TIMESTAMP: info[colIndex["TIMESTAMP"]],
            PROCEDURE: info[colIndex["PROCEDURE"]],
            INVOCATIONS: info[colIndex["INVOCATIONS"]],
            AVG: info[colIndex["AVG"]],
            MIN: info[colIndex["MIN"]],
            MAX: info[colIndex["MAX"]],
            WEIGHTED_PERC: info[colIndex["WEIGHTED_PERC"]],
          };
          procedureProfile["PROCEDURE_PROFILE"].push(profileObj);
        }
      );
    };

    var getProcedureDetailInfo = function (connection, procedureDetail) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_PROCEDUREDETAIL"] == undefined) {
        return;
      }

      connection.Metadata["@Statistics_PROCEDUREDETAIL"].schema.forEach(
        function (columnInfo) {
          if (
            columnInfo["name"] == "TIMESTAMP" ||
            columnInfo["name"] == "PROCEDURE" ||
            columnInfo["name"] == "INVOCATIONS" ||
            columnInfo["name"] == "AVG_EXECUTION_TIME" ||
            columnInfo["name"] == "PARTITION_ID" ||
            columnInfo["name"] == "STATEMENT" ||
            columnInfo["name"] == "MIN_EXECUTION_TIME" ||
            columnInfo["name"] == "MAX_EXECUTION_TIME"
          )
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      if (!procedureDetail.hasOwnProperty("PROCEDURE_DETAIL")) {
        procedureDetail["PROCEDURE_DETAIL"] = [];
      }

      connection.Metadata["@Statistics_PROCEDUREDETAIL"].data.forEach(function (
        info
      ) {
        var profileObj = {
          TIMESTAMP: info[colIndex["TIMESTAMP"]],
          PROCEDURE: info[colIndex["PROCEDURE"]],
          INVOCATIONS: info[colIndex["INVOCATIONS"]],
          AVG_EXECUTION_TIME: info[colIndex["AVG_EXECUTION_TIME"]],
          MIN_EXECUTION_TIME: info[colIndex["MIN_EXECUTION_TIME"]],
          MAX_EXECUTION_TIME: info[colIndex["MAX_EXECUTION_TIME"]],
          PARTITION_ID: info[colIndex["PARTITION_ID"]],
          STATEMENT: info[colIndex["STATEMENT"]],
        };
        procedureDetail["PROCEDURE_DETAIL"].push(profileObj);
      });
    };

    var getReplicationNotCovered = function (replicationData, index) {
      var count = 0;
      if (index != undefined) {
        replicationData.data.forEach(function (columnInfo) {
          columnInfo.forEach(function (col, i) {
            if (col == "false" && i == index) {
              count++;
            }
          });
        });
      }
      return count;
    };

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
      if (connection.Metadata["@Statistics_STARVATION"] == null) {
        return;
      }

      connection.Metadata["@Statistics_STARVATION"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "SITE_ID" ||
          columnInfo["name"] == "PERCENT" ||
          columnInfo["name"] == "TIMESTAMP"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      //to get MPI site id
      connection.Metadata["@Statistics_STARVATION"].data.forEach(function (
        info
      ) {
        if (currentServer == info[colIndex["HOSTNAME"]]) {
          if (parseInt(info[colIndex["SITE_ID"]]) > siteId) {
            siteId = parseInt(info[colIndex["SITE_ID"]]);
          }
        } else {
          if (!mpiIndex.hasOwnProperty(info[colIndex["HOSTNAME"]])) {
            mpiIndex[info[colIndex["HOSTNAME"]]] = 0;
          }
          if (
            parseInt(info[colIndex["SITE_ID"]]) >
            mpiIndex[info[colIndex["HOSTNAME"]]]
          ) {
            mpiIndex[info[colIndex["HOSTNAME"]]] = parseInt(
              info[colIndex["SITE_ID"]]
            );
          }
        }
      });
      //

      connection.Metadata["@Statistics_STARVATION"].data.forEach(function (
        info
      ) {
        if (currentServer == info[colIndex["HOSTNAME"]]) {
          if (siteId == parseInt(info[colIndex["SITE_ID"]])) {
            var keyMpi =
              info[colIndex["HOSTNAME"]] + ": " + info[colIndex["SITE_ID"]];
            starvMpiData[keyMpi] = info[colIndex["PERCENT"]];
          } else {
            var key =
              info[colIndex["HOSTNAME"]] + ": " + info[colIndex["SITE_ID"]];
            keys.push(key);
            starvStats[key] = info[colIndex["PERCENT"]];
          }
        } else {
          if (
            parseInt(info[colIndex["SITE_ID"]]) !=
            mpiIndex[info[colIndex["HOSTNAME"]]]
          ) {
            if (info[colIndex["HOSTNAME"]] != hostName) {
              hostName = info[colIndex["HOSTNAME"]];
              if (previousHostKey != "") {
                starvMinData[previousHostKey + "(Min)"] = minPer;
                starvMaxData[previousHostKey + "(Max)"] = maxPer;
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
              previousHostKey =
                info[colIndex["HOSTNAME"]] + ": " + info[colIndex["SITE_ID"]];
              previousSiteId = info[colIndex["SITE_ID"]];
            }
          }
        }
        timeStamp = info[colIndex["TIMESTAMP"]];
      });
      if (previousHostKey != "" && previousSiteId != mpiIndex[previousHost]) {
        starvMinData[previousHostKey + "(Min)"] = minPer;
        starvMaxData[previousHostKey + "(Max)"] = maxPer;
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

      if (connection.Metadata["@SystemInformation_OVERVIEW" + suffix] == null) {
        return;
      }
      connection.Metadata["@SystemInformation_OVERVIEW" + suffix].data.forEach(
        function (info) {
          var singleData = info;
          var id = singleData[0];

          if (!clusterDetails.hasOwnProperty(id)) {
            clusterDetails[id] = {};
          }
          if ($.inArray("CLUSTERSTATE", info) > 0) {
            clusterDetails[id][singleData[1]] = singleData[2];
          } else if ($.inArray("HOSTNAME", info) > 0) {
            clusterDetails[id][singleData[1]] = singleData[2];
          }
        }
      );
    };

    var getClusterState = function (clusterDetails, clusterState) {
      var currentServer = getCurrentServer();
      $.each(clusterDetails, function (key, val) {
        if (val["HOSTNAME"] == currentServer) {
          if (!clusterState.hasOwnProperty("CLUSTERSTATE")) {
            clusterState["CLUSTERSTATE"] = {};
          }
          clusterState["CLUSTERSTATE"] = val["CLUSTERSTATE"];
        }
      });
    };

    var getTransactionDetails = function (connection, sysTransaction) {
      var colIndex = {};
      var counter = 0;
      var currentTimerTick = 0;
      var procStats = {};

      if (
        connection.Metadata["@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"] ==
        null
      ) {
        return;
      }
      if (
        connection.Metadata["@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"]
          .schema != null
      ) {
        connection.Metadata[
          "@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"
        ].schema.forEach(function (columnInfo) {
          colIndex[columnInfo["name"]] = counter;
          counter++;
        });
      }

      var dataCount = 0;
      if (
        jQuery.isEmptyObject(
          connection.Metadata["@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"]
            .data
        ) &&
        voltDbRenderer.memoryDetails.length != 0
      ) {
        sysTransaction["TimeStamp"] =
          voltDbRenderer.memoryDetails[voltDbRenderer.memoryDetails.length - 1];
        currentTimerTick = sysTransaction["TimeStamp"];
      } else {
        connection.Metadata[
          "@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"
        ].data.forEach(function (table) {
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
            data = [
              srcData[colIndex["PROCEDURE"]],
              srcData[colIndex["INVOCATIONS"]],
              srcData[colIndex["WEIGHTED_PERC"]],
              srcData[colIndex["MIN"]],
              srcData[colIndex["AVG"]],
              srcData[colIndex["MAX"]],
            ];
          }
          procStats[srcData[colIndex["PROCEDURE"]]] = data;
          if (
            dataCount ==
            connection.Metadata[
              "@Statistics_PROCEDUREPROFILE_GRAPH_TRANSACTION"
            ].data.length -
            1
          ) {
            sysTransaction["TimeStamp"] = srcData[colIndex["TIMESTAMP"]];
          }
          dataCount++;
        });
      }

      var currentTimedTransactionCount = 0.0;
      for (var proc in procStats) {
        currentTimedTransactionCount += procStats[proc][1];
      }
      sysTransaction["CurrentTimedTransactionCount"] =
        currentTimedTransactionCount;
      sysTransaction["currentTimerTick"] = currentTimerTick;
    };

    var getPortAndOverviewDetails = function () {
      var portConfigValues = [];
      var currentServer = getCurrentServer();
      var httpPort = VoltDBConfig.GetPortId();

      $.each(systemOverview, function (key, val) {
        if (val["HOSTNAME"] == currentServer && val["HTTPPORT"] == httpPort) {
          portConfigValues["adminPort"] = val["ADMINPORT"];
          portConfigValues["httpPort"] = val["HTTPPORT"];
          portConfigValues["clientPort"] = val["CLIENTPORT"];
          portConfigValues["internalPort"] = val["INTERNALPORT"];
          portConfigValues["zookeeperPort"] = val["ZKPORT"];
          portConfigValues["replicationPort"] = val["DRPORT"];
          portConfigValues["clusterState"] = val["CLUSTERSTATE"];
          portConfigValues["replicationRole"] = val["REPLICATIONROLE"];

          if (validateServerSpecificSettings(val)) {
            portConfigValues["adminInterface"] = val["ADMININTERFACE"];
            portConfigValues["httpInterface"] = val["HTTPINTERFACE"];
            portConfigValues["clientInterface"] = val["CLIENTINTERFACE"];
            portConfigValues["internalInterface"] = val["INTERNALINTERFACE"];
            portConfigValues["zookeeperInterface"] = val["ZKINTERFACE"];
            portConfigValues["replicationInterface"] = val["DRINTERFACE"];
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
          portConfigValues["maxJavaHeap"] = val["MAXJAVAHEAP"];
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
      if (connection.Metadata["@SystemInformation_DEPLOYMENT"] == null) {
        return;
      }

      connection.Metadata["@SystemInformation_DEPLOYMENT"].schema.forEach(
        function (columnInfo) {
          if (columnInfo["name"] == "PROPERTY" || columnInfo["name"] == "VALUE")
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      connection.Metadata["@SystemInformation_DEPLOYMENT"].data.forEach(
        function (info) {
          if (info[colIndex["PROPERTY"]] == "hostcount") {
            hostCount = info[colIndex["VALUE"]];
          }
          if (info[colIndex["PROPERTY"]] == "sitesperhost") {
            siteCount = info[colIndex["VALUE"]];
          }
          if (info[colIndex["PROPERTY"]] == "commandlogenabled") {
            commandLogStatus = info[colIndex["VALUE"]];
          }
        }
      );
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

      if (connection.Metadata["@Statistics_COMMANDLOG"] == null) {
        return;
      }

      connection.Metadata["@Statistics_COMMANDLOG"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "OUTSTANDING_TXNS" ||
          columnInfo["name"] == "TIMESTAMP" ||
          columnInfo["name"] == "OUTSTANDING_BYTES" ||
          columnInfo["name"] == "SEGMENT_COUNT" ||
          columnInfo["name"] == "FSYNC_INTERVAL" ||
          columnInfo["name"] == "IN_USE_SEGMENT_COUNT"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_COMMANDLOG"].data.forEach(function (
        info
      ) {
        var hostName = info[colIndex["HOSTNAME"]];
        if (!cmdLogDetails.hasOwnProperty(hostName)) {
          cmdLogDetails[hostName] = {};
        }
        cmdLogDetails[hostName]["OUTSTANDING_TXNS"] =
          info[colIndex["OUTSTANDING_TXNS"]];
        cmdLogDetails[hostName]["TIMESTAMP"] = info[colIndex["TIMESTAMP"]];
        cmdLogDetails[hostName]["OUTSTANDING_BYTES"] =
          info[colIndex["OUTSTANDING_BYTES"]];
        cmdLogDetails[hostName]["SEGMENT_COUNT"] =
          info[colIndex["SEGMENT_COUNT"]];
        cmdLogDetails[hostName]["FSYNC_INTERVAL"] =
          info[colIndex["FSYNC_INTERVAL"]];
        cmdLogDetails[hostName]["IN_USE_SEGMENT_COUNT"] =
          info[colIndex["IN_USE_SEGMENT_COUNT"]];
      });
    };

    var getTableDetails = function (connection, dTDetails) {
      var colIndex = {};
      var counter = 0;
      if (connection.Metadata["@Statistics_TABLE"] == null) {
        return;
      }

      connection.Metadata["@Statistics_TABLE"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOST_ID" ||
          columnInfo["name"] == "TABLE_NAME" ||
          columnInfo["name"] == "TUPLE_COUNT" ||
          columnInfo["name"] == "TABLE_TYPE" ||
          columnInfo["name"] == "TUPLE_LIMIT" ||
          columnInfo["name"] == "SEGMENT_COUNT" ||
          columnInfo["name"] == "FSYNC_INTERVAL" ||
          columnInfo["name"] == "IN_USE_SEGMENT_COUNT"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@Statistics_TABLE"].data.forEach(function (info) {
        var hostName = info[colIndex["HOST_ID"]];
        if (!dTDetails.hasOwnProperty(hostName)) {
          dTDetails[hostName] = {};
        }
        dTDetails[hostName]["TABLE_NAME"] = info[colIndex["TABLE_NAME"]];
        dTDetails[hostName]["TUPLE_COUNT"] = info[colIndex["TUPLE_COUNT"]];
        dTDetails[hostName]["TABLE_TYPE"] = info[colIndex["TABLE_TYPE"]];
        dTDetails[hostName]["TUPLE_LIMIT"] = info[colIndex["TUPLE_LIMIT"]];
        //                dTDetails[hostName]["FSYNC_INTERVAL"] = info[colIndex["FSYNC_INTERVAL"]];
        //                dTDetails[hostName]["IN_USE_SEGMENT_COUNT"] = info[colIndex["IN_USE_SEGMENT_COUNT"]];
      });
    };

    var getSnapshotStatus = function (connection, snapshotDetails) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@Statistics_SNAPSHOTSTATUS"] == null) {
        return;
      }

      connection.Metadata["@Statistics_SNAPSHOTSTATUS"].schema.forEach(
        function (columnInfo) {
          if (
            columnInfo["name"] == "HOSTNAME" ||
            columnInfo["name"] == "TIMESTAMP" ||
            columnInfo["name"] == "PATH" ||
            columnInfo["name"] == "START_TIME" ||
            columnInfo["name"] == "END_TIME"
          )
            colIndex[columnInfo["name"]] = counter;
          counter++;
        }
      );

      connection.Metadata["@Statistics_SNAPSHOTSTATUS"].data.forEach(function (
        info
      ) {
        var hostName = info[colIndex["HOSTNAME"]];
        if (!snapshotDetails.hasOwnProperty(hostName)) {
          snapshotDetails[hostName] = [];
        }
        var snapshot = {
          TIMESTAMP: info[colIndex["TIMESTAMP"]],
          PATH: info[colIndex["PATH"]],
          START_TIME: info[colIndex["START_TIME"]],
          END_TIME: info[colIndex["END_TIME"]],
        };
        snapshotDetails[hostName].push(snapshot);
      });
    };

    var validateServerSpecificSettings = function (overviewValues) {
      if (
        overviewValues["ADMININTERFACE"] == "" &&
        overviewValues["HTTPINTERFACE"] == "" &&
        overviewValues["CLIENTINTERFACE"] == "" &&
        overviewValues["INTERNALINTERFACE"] == "" &&
        overviewValues["ZKINTERFACE"] == "" &&
        overviewValues["DRINTERFACE"] == ""
      ) {
        return false;
      } else if (
        overviewValues["ADMININTERFACE"] != "" ||
        overviewValues["HTTPINTERFACE"] != "" ||
        overviewValues["CLIENTINTERFACE"] != "" ||
        overviewValues["INTERNALINTERFACE"] != "" ||
        overviewValues["ZKINTERFACE"] != "" ||
        overviewValues["DRINTERFACE"] != ""
      ) {
        return true;
      }
      return false;
    };

    this.editConfigurationItem = function (
      configGroup,
      configMember,
      configValue,
      onConfigurationUpdated
    ) {
      VoltDBService.editConfigurationItem(
        configGroup,
        configMember,
        configValue,
        function () {
          onConfigurationUpdated();
        }
      );
    };

    var check_hostid = function (hostId) {
      $.each(VoltDbAdminConfig.servers, function (id, val) {
        if (val["hostId"] == hostId) {
          return true;
        }
      });
      return false;
    };

    var getAdminServerList = function () {
      var htmlServerListHtml = "";
      var serverDetails;
      var className;
      var currentServerRowClass;
      var currentServerColumnClass;
      var count = 0;

      function sortByHostId(v1, v2) {
        var hostIdV1 = parseInt(v1.hostId);
        var hostIdV2 = parseInt(v2.hostId)
        return ((hostIdV1 < hostIdV2) ? -1 : ((hostIdV1 > hostIdV2) ? 1 : 0));
      }

      this.setServerDetails = function (hostId, serverInfo, iteratorCount) {
        var count = 0;
        var stopperServerCount = 0;
        if (
          (VoltDbAdminConfig.servers != "" ||
            VoltDbAdminConfig.servers != null ||
            VoltDbAdminConfig.servers != undefined) &&
          VoltDbAdminConfig.servers.length > 0 &&
          check_hostid(hostId)
        ) {
          VoltDbAdminConfig.servers.sort(sortByHostId);
          $.each(VoltDbAdminConfigservers, function (id, value) {
            {
              if (
                value.serverName != serverInfo["HOSTNAME"] &&
                count == VoltDbAdminConfig.servers.length - 1
              ) {
                serverDetails = new VoltDbAdminConfig.server(
                  hostId,
                  serverInfo["HOSTNAME"],
                  serverInfo["CLUSTERSTATE"],
                  serverInfo["IPADDRESS"],
                  serverInfo["HTTPPORT"],
                  serverInfo["CLIENTPORT"]
                );
                VoltDbAdminConfig.servers[iteratorCount] = serverDetails;

                $.each(VoltDbAdminConfig.stoppedServers, function (key, val) {
                  if (val.HOSTNAME == value.serverName) {
                    //remove server from the stopped server list if server stopped while ago is already in running state
                    VoltDbAdminConfig.stoppedServers.splice(
                      stopperServerCount,
                      1
                    );
                  }
                  stopperServerCount++;
                });
              } else if (value.serverName == serverInfo["HOSTNAME"]) {
                value.hostId = hostId;
                value.serverState = serverInfo["CLUSTERSTATE"];
                return false;
              }
              count++;
            }
          });
        } else {
          var hostname = serverInfo["HOSTNAME"];
          serverDetails = new VoltDbAdminConfig.server(
            hostId,
            hostname,
            serverInfo["CLUSTERSTATE"],
            serverInfo["IPADDRESS"],
            serverInfo["HTTPPORT"],
            serverInfo["CLIENTPORT"]
          );
          VoltDbAdminConfig.servers.push(serverDetails);
          VoltDbAdminConfig.servers.sort(sortByHostId);
          count++;
        }
      };

      this.updateServers = function (hostId, hostName, serverState) {
        if (
          (VoltDbAdminConfig.servers != "" ||
            VoltDbAdminConfig.servers != null ||
            VoltDbAdminConfig.servers != undefined) &&
          VoltDbAdminConfig.servers.length > 0
        ) {
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
          // if (voltDbRenderer.currentHost != value.serverName) {
          if (value.serverState == "RUNNING") {
            if (runningServerCounter == 0)
              VoltDbAdminConfig.runningServerIds =
                VoltDbAdminConfig.runningServerIds.concat(
                  "#stopServer_",
                  value.serverName
                );
            else {
              VoltDbAdminConfig.runningServerIds =
                VoltDbAdminConfig.runningServerIds.concat(
                  ",",
                  "#stopServer_",
                  value.serverName
                );
            }
            runningServerCounter++;
          }
          // }
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

      if (
        VoltDbAdminConfig.servers != null ||
        VoltDbAdminConfig.servers != undefined
      ) {
        var count = 0;
        $.each(VoltDbAdminConfig.servers, function (id, val) {
          if (val.serverState == "PAUSED" || val.serverState == "MISSING") {
            count++;
          }
        });
        var con = false;
        if (count < parseInt($("#kSafety").text())) {
          con = true;
        }
        $.each(VoltDbAdminConfig.servers, function (id, val) {
          var conn = true;
          if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == "RUNNING") {
            currentServerRowClass =
              voltDbRenderer.currentHost == val.serverName
                ? "activeHostMonitoring"
                : "activeHost";
            // if (voltDbRenderer.currentHost != val.serverName && con) {
            if (con) {
              className = "shutdown";
              currentServerColumnClass = "shutdownServer";
            } else {
              className = "disableServer";
              currentServerColumnClass = "shutdownServer stopDisable";
              conn = false;
            }
            // if (location.port == val.httpPort){
            //   if (client_port == val.clientPort){
            //     conn = false;
            //     className = "disableServer";
            //     currentServerColumnClass = "shutdownServer stopDisable";
            //   }
            // }
            // currentServerColumnClass =
            //   voltDbRenderer.currentHost == val.serverName
            //     ? "shutdownServer stopDisable"
            //     : "shutdownServer";

            htmlServerListHtml = htmlServerListHtml.concat(
              '<tr class="' +
              currentServerRowClass +
              '"><td class="configLabel" width="40%"><a class="serNameTruncate" href="#" >' +
              val.serverName + "-" + val.hostId +
              "</a></td>" +
              "<td  align='center' >" +
              val.ipAddress +
              "</td>" +
              '<td align="right"><a href="javascript:void(0);" data-HostId="' +
              val.hostId +
              '" data-HostName="' +
              val.serverName +
              '" class="' +
              className + ' k8s_hidden' +
              '"'
            );
            if (conn) {
              htmlServerListHtml = htmlServerListHtml.concat(
                'id="stopServer_' +
                val.serverName +
                '"'
              );
            }
            htmlServerListHtml = htmlServerListHtml.concat(
              '>' +
              '<span class="' +
              currentServerColumnClass +
              '">Stop</span></a></td></tr>'
            );
          } else if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == "PAUSED") {
            if (voltDbRenderer.currentHost != val.serverName && con) {
              className = "shutdown";
            } else {
              conn = false;
              className = "disableServer";
            }
            currentServerRowClass =
              voltDbRenderer.currentHost == val.serverName
                ? "activeHostMonitoring"
                : "activeHost";
            currentServerColumnClass = "shutdownServerPause";

            htmlServerListHtml = htmlServerListHtml.concat(
              '<tr class="' +
              currentServerRowClass +
              '"><td class="configLabel" width="40%"><a class="serNameTruncate" href="#" >' +
              val.serverName + "-" + val.hostId +
              "</a></td>" +
              "<td  align='center' >" +
              val.ipAddress +
              "</td>" +
              '<td align="right" class="pauseCursorDefault"><a href="javascript:void(0);" data-HostId="' +
              val.hostId +
              '" data-HostName="' +
              val.serverName +
              '"class="resume" '
            );
            if (conn) {
              htmlServerListHtml = htmlServerListHtml.concat(
                'id="stopServer_' +
                val.serverName +
                '"'
              );
            }
            htmlServerListHtml = htmlServerListHtml.concat(
              '><span class="' +
              currentServerColumnClass +
              '">Paused</span></a></td></tr>'
            );
          } else if ((val.serverName != null || val.serverName != "" || val.serverName != undefined) && val.serverState == "JOINING") {
            htmlServerListHtml = htmlServerListHtml.concat(
              '<tr><td class="configLabel" width="40%"><a class="serNameTruncate" href="#">' +
              val.serverName + "-" + val.hostId +
              "</a></td>" +
              "<td  align='center' >" +
              val.ipAddress +
              "</td>" +
              '<td align="right"><a href="javascript:void(0);" class="shutdownDisabled">' +
              "<span>Stop</span></a></td></tr>"
            );
          } else if (val.serverName != null || val.serverName != "" || val.serverName != undefined || val.serverState == "MISSING") {
            // htmlServerListHtml = htmlServerListHtml.concat(
            //   '<tr><td class="configLabel" width="40%"><a class="serNameTruncate" href="#">' +
            //     val.serverName + "-" + val.hostId + 
            //     "</a></td>" +
            //     "<td  align='center' >" +
            //     val.ipAddress +
            //     "</td>" +
            //     '<td align="right"><a href="javascript:void(0);" data-HostId="' +
            //     val.hostId +
            //     '" data-HostName="' +
            //     val.serverName +
            //     '" class="disableServer">' +
            //     '<span class="shutdownServer stopDisable">Stop</span></a></td></tr>'
            // );
          }
        });
        updateAdminServerList();
        return htmlServerListHtml;
      }
      return "";
    };

    this.stopServer = function (nodeId, hostNameValue, onServerStopped) {
      VoltDBService.stopServerNode(
        nodeId,
        function (connection, status, statusString) {
          if (status == 1) {
            VoltDbAdminConfig.stoppedServers[
              VoltDbAdminConfig.stoppedServers.length
            ] = new VoltDbAdminConfig.stoppedServer(nodeId, hostNameValue);
            onServerStopped(true, statusString);
          } else {
            onServerStopped(false, statusString);
          }
        }
      );
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
        var prepare_status = {};
        var status = -1;
        var zk_pause_txn_id = "0";
        var data = connection.Metadata["@PrepareShutdown_data"];
        if (data != undefined) {
          zk_pause_txn_id =
            connection.Metadata["@PrepareShutdown_data"]["data"][0][0];
          status = parseInt(connection.Metadata["@PrepareShutdown_status"]);
        }
        prepare_status.zk_pause_txn_id = zk_pause_txn_id;
        prepare_status.status = status;
        onPrepareServerShutdown(prepare_status);
      });
    };

    this.QuiesceCluster = function (onPrepareServerQuiesce) {
      VoltDBService.QuiesceCluster(function (connection, status) {
        var data = connection.Metadata["@Quiesce_data"];
        if (data == undefined) status = -1;
        else status = connection.Metadata["@Quiesce_data"]["data"][0][0];
        onPrepareServerQuiesce(status);
      });
    };

    this.GetDrProducerInformation = function (onInformationLoaded, drDetails) {
      VoltDBService.GetDrProducerInformation(function (connection) {
        getDrProducerInfo(connection, drDetails);
        onInformationLoaded(drDetails);
      });
    };

    this.GetExportTablesInformation = function (
      onInformationLoaded,
      tableDetails
    ) {
      VoltDBService.GetExportTablesInformation(function (connection) {
        getExportTableInfo(connection, tableDetails);
        onInformationLoaded(tableDetails);
      });
    };

    this.GetImportRequestInformation = function (
      onInformationLoaded,
      tableDetails
    ) {
      VoltDBService.GetImportRequestInformation(function (connection) {
        var data = connection.Metadata["@Statistics_IMPORTER"];
        if (data == undefined || $.isEmptyObject(data["data"])) outstanding = 0;
        else
          outstanding =
            connection.Metadata["@Statistics_IMPORTER"]["data"][0][0];
        onInformationLoaded(outstanding);
      });
    };

    this.saveSnapshot = function (
      snapshotDir,
      snapshotFileName,
      onSaveSnapshot
    ) {
      VoltDBService.SaveSnapShot(
        snapshotDir,
        snapshotFileName,
        function (connection, status) {
          var snapshotStatus = {};
          if (status == 1 || status == -2) {
            voltDbRenderer.getSaveSnapshotStatus(
              connection,
              snapshotStatus,
              status
            );
            onSaveSnapshot(true, snapshotStatus);
          }
        }
      );
    };

    this.restoreSnapShot = function (
      snapshotDir,
      snapshotFileName,
      onSaveSnapshot
    ) {
      VoltDBService.RestoreSnapShot(
        snapshotDir,
        snapshotFileName,
        function (connection, status, statusString) {
          var snapshotStatus = {};
          if (status == 1) {
            voltDbRenderer.getRestoreSnapshotStatus(connection, snapshotStatus);
            onSaveSnapshot(true, snapshotStatus, statusString);
          } else {
            onSaveSnapshot(false, null, statusString);
          }
        }
      );
    };

    this.GetSnapshotList = function (snapshotDirectory, onInformationLoaded) {
      var snapshotList = {};
      VoltDBService.GetSnapshotList(snapshotDirectory, function (connection) {
        getSnapshotDetails(connection, snapshotList, snapshotDirectory);
        onInformationLoaded(snapshotList);
      });
    };

    var getSnapshotDetails = function (
      connection,
      snapshotList,
      snapshotDirectory
    ) {
      var colIndex = {};
      var counter = 0;

      if (connection.Metadata["@SnapshotScan_data"][0] == undefined) {
        if (connection.Metadata["@SnapshotScan_status"] == -2) {
          snapshotList[0] = {};
          snapshotList[0]["RESULT"] = "FAILURE";
          snapshotList[0]["ERR_MSG"] =
            connection.Metadata["@SnapshotScan_statusstring"] != null
              ? connection.Metadata["@SnapshotScan_statusstring"]
              : "";
        }
        return;
      }

      if (connection.Metadata["@SnapshotScan_data"][0].data.length != 0)
        connection.Metadata["@SnapshotScan_data"] =
          connection.Metadata["@SnapshotScan_data"][0];
      else
        connection.Metadata["@SnapshotScan_data"] =
          connection.Metadata["@SnapshotScan_data"][1];

      connection.Metadata["@SnapshotScan_data"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "PATH" ||
          columnInfo["name"] == "NONCE" ||
          columnInfo["name"] == "RESULT" ||
          columnInfo["name"] == "ERR_MSG"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });
      var count = 0;

      connection.Metadata["@SnapshotScan_data"].data.forEach(function (info) {
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

    this.getSaveSnapshotStatus = function (
      connection,
      snapshotStatus,
      saveStatus
    ) {
      var colIndex = {};
      var counter = 0;

      //Handle error for community edition of VoltDB.
      if (saveStatus == -2) {
        var currentServer = getCurrentServer();
        snapshotStatus[currentServer] = {};
        snapshotStatus[currentServer]["RESULT"] = "Failure";
        snapshotStatus[currentServer]["ERR_MSG"] =
          connection.Metadata["@SnapshotSave_statusstring"] != undefined
            ? connection.Metadata["@SnapshotSave_statusstring"]
            : "";

        return;
      }

      connection.Metadata["@SnapshotSave_data"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "RESULT" ||
          columnInfo["name"] == "ERR_MSG"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@SnapshotSave_data"].data.forEach(function (info) {
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

      connection.Metadata["@SnapshotRestore_data"].schema.forEach(function (
        columnInfo
      ) {
        if (
          columnInfo["name"] == "HOSTNAME" ||
          columnInfo["name"] == "RESULT" ||
          columnInfo["name"] == "ERR_MSG"
        )
          colIndex[columnInfo["name"]] = counter;
        counter++;
      });

      connection.Metadata["@SnapshotRestore_data"].data.forEach(function (
        info
      ) {
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

    this.updateAdminConfiguration = function (
      updatedData,
      onInformationLoaded
    ) {
      VoltDBService.UpdateAdminConfiguration(
        updatedData,
        function (connection) {
          var result = {};

          if (
            connection != null &&
            connection.Metadata["SHORTAPI_UPDATEDEPLOYMENT"] != null
          ) {
            result = connection.Metadata["SHORTAPI_UPDATEDEPLOYMENT"];
          }

          onInformationLoaded(result);
        }
      );
    };

    //end admin configuration
    this.UpdateUserConfiguration = function (
      updatedData,
      onInformationLoaded,
      userId,
      requestType
    ) {
      VoltDBService.UpdateUserConfiguration(
        updatedData,
        function (connection) {
          var result = {};

          if (
            connection != null &&
            connection.Metadata["SHORTAPI_USERUPDATEDEPLOYMENT"] != null
          ) {
            result = connection.Metadata["SHORTAPI_USERUPDATEDEPLOYMENT"];
          }

          onInformationLoaded(result);
        },
        userId,
        requestType
      );
    };

    function getTableData(
      connection,
      tablesData,
      viewsData,
      proceduresData,
      procedureColumnsData,
      sysProceduresData,
      processName,
      exportTableData
    ) {
      exportTableData = exportTableData == undefined ? {} : exportTableData;
      var suffix = "";
      if (
        processName == "TABLE_INFORMATION" ||
        processName == "TABLE_INFORMATION_CLIENTPORT"
      ) {
        suffix = "_" + processName;
      }
      var rawTables = connection.Metadata["@Statistics_TABLE" + suffix].data;
      var rawIndexes = connection.Metadata["@Statistics_INDEX" + suffix].data;
      var rawColumns =
        connection.Metadata["@SystemCatalog_COLUMNS" + suffix].data;
      var procedures =
        connection.Metadata["@SystemCatalog_PROCEDURES" + suffix].data;
      var procedureColumns =
        connection.Metadata["@SystemCatalog_PROCEDURECOLUMNS" + suffix].data;
      const rawExportStreams =
        typeof connection.Metadata["@Statistics_EXPORT" + suffix] !==
          "undefined"
          ? connection.Metadata["@Statistics_EXPORT" + suffix].data
          : null;

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
            if (item.indexes == null) item.indexes = [];
            item.indexes[indexName] =
              indexName +
              " (" +
              (rawIndexes[j][7].toLowerCase().indexOf("hash") > -1
                ? "Hash"
                : "Tree") +
              (rawIndexes[j][8] == "1" ? ", Unique" : "") +
              ")";
            if (indexName.toUpperCase().indexOf("MATVIEW") > -1) isView = true;
            if (indexName.toUpperCase().indexOf("PK_") > -1)
              item.key = indexName;
          }
        }
        if (isView) views[tableName] = item;
        else if (rawTables[k][6] == "StreamedTable") exports[tableName] = item;
        else tables[tableName] = item;
      }

      // update the stream schema from @Statistic EXPORT.
      if (rawExportStreams !== null) {
        for (var k = 0; k < rawExportStreams.length; k++) {
          const streamName = rawExportStreams[k][5];
          exports[streamName] = {
            name: streamName,
            key: null,
            indexes: null,
            columns: null,
          };
        }
      }

      connection.Metadata["tables"] = tables;
      connection.Metadata["views"] = views;
      connection.Metadata["exports"] = exports;
      for (var i = 0; i < rawColumns.length; i++) {
        var TableName = rawColumns[i][2].toUpperCase();
        if (connection.Metadata["tables"][TableName] != null) {
          if (connection.Metadata["tables"][TableName].columns == null) {
            connection.Metadata["tables"][TableName].columns = [];
          }
          connection.Metadata["tables"][TableName].columns[rawColumns[i][16]] =
            rawColumns[i][3].toUpperCase() +
            " (" +
            rawColumns[i][5].toLowerCase() +
            ")";
        } else if (connection.Metadata["exports"][TableName] != null) {
          if (connection.Metadata["exports"][TableName].columns == null) {
            connection.Metadata["exports"][TableName].columns = [];
          }
          connection.Metadata["exports"][TableName].columns[rawColumns[i][16]] =
            rawColumns[i][3].toUpperCase() +
            " (" +
            rawColumns[i][5].toLowerCase() +
            ")";
        } else if (connection.Metadata["views"][TableName] != null) {
          if (connection.Metadata["views"][TableName].columns == null) {
            connection.Metadata["views"][TableName].columns = [];
          }
          connection.Metadata["views"][TableName].columns[
            rawColumns[i][3].toUpperCase()
          ] =
            rawColumns[i][3].toUpperCase() +
            " (" +
            rawColumns[i][5].toLowerCase() +
            ")";
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
            procParams[paramOrder] = {
              name: paramName,
              type: paramType.toLowerCase(),
            };
          }
        }

        for (var p = 0; p < procParams.length; ++p) {
          connTypeParams[connTypeParams.length] = procParams[p].type;
        }
        // make the procedure callable.
        connection.procedures[procName] = {};
        connection.procedures[procName]["" + connTypeParams.length] =
          connTypeParams;
      }

      if (!tablesData.hasOwnProperty("tables")) {
        tablesData["tables"] = {};
      }
      if (!viewsData.hasOwnProperty("views")) {
        viewsData["views"] = {};
      }
      if (!proceduresData.hasOwnProperty("procedures")) {
        proceduresData["procedures"] = {};
      }
      if (!procedureColumnsData.hasOwnProperty("procedureColumns")) {
        procedureColumnsData["procedureColumns"] = {};
      }
      if (!sysProceduresData.hasOwnProperty("sysProcedures")) {
        sysProceduresData["sysProcedures"] = {};
      }

      tablesData["tables"] = connection.Metadata["tables"];
      viewsData["views"] = connection.Metadata["views"];
      proceduresData["procedures"] = procedures;
      procedureColumnsData["procedureColumns"] = procedureColumns;
      sysProceduresData["sysProcedures"] = connection.Metadata["sysprocs"];
      exportTableData["exportTables"] = connection.Metadata["exports"];
    }

    function getTableDataOnly(connection, tablesData, processName) {
      var suffix = "";
      if (processName == "TABLE_INFORMATION_ONLY") {
        suffix = "_" + processName;
      }

      var rawTablePartitions =
        connection.Metadata["@Statistics_TABLE" + suffix].data;
      var rawTables =
        connection.Metadata["@SystemCatalog_TABLES" + suffix].data;

      var tablesArr = [];
      var tableObj = {};

      for (var k = 0; k < rawTables.length; k++) {
        var tableName = rawTables[k][2];
        var tableType = rawTables[k][3];
        var tableRemarks = rawTables[k][4] == null ? "" : rawTables[k][4];
        if (tableType == "TABLE") {
          if (!tableObj.hasOwnProperty(tableName)) {
            tableObj[tableName] = {};
          }
          tableObj[tableName]["TYPE"] = tableType;
          tableObj[tableName]["PARTITION_TYPE"] =
            tableRemarks.indexOf("partitionColumn") > -1
              ? "Partitioned"
              : "Replicated";
        }
      }

      $.each(tableObj, function (key, value) {
        var tableName = key;
        for (var i = 0; i < rawTablePartitions.length; i++) {
          if (tableName == rawTablePartitions[i][5]) {
            var partitionObj = {};
            if (!tableObj[tableName].hasOwnProperty("PARTITIONS")) {
              tableObj[tableName]["PARTITIONS"] = [];
              tableObj[tableName]["TUPLE_COUNT"] = 0;
              tableObj[tableName]["TIMESTAMP"] = rawTablePartitions[i][0];
            }

            if (value["PARTITION_TYPE"] == "Partitioned") {
              tableObj[tableName]["TUPLE_COUNT"] += rawTablePartitions[i][7];
              tableObj[tableName]["PARTITIONS"].push({
                partition_id: rawTablePartitions[i][4],
                tupleCount: rawTablePartitions[i][7],
              });
            } else {
              tableObj[tableName]["TUPLE_COUNT"] = rawTablePartitions[i][7];
              tableObj[tableName]["PARTITIONS"].push({
                partition_id: rawTablePartitions[i][4],
                tupleCount: rawTablePartitions[i][7],
              });
            }
          }
        }
      });
      if (!tablesData.hasOwnProperty("TABLES")) {
        tablesData["TABLES"] = {};
      }

      tablesData["TABLES"] = tableObj;
    }

    var formatTableData = function (connection) {
      var i = 0;
      var tableMetadata = [];
      var totalTupleCount = 0;
      var partitionEntryCount = 0;
      var duplicatePartition = false;
      var tupleCountPartitions = [];
      var partitionData = {};
      var averageRowCount = 0;

      // update the exportTablesArray from @Statistics EXPORT.
      if (
        connection.Metadata["@Statistics_EXPORT"] !== undefined ||
        connection.Metadata["@Statistics_EXPORT"] != null
      ) {
        const rawExportStreams = connection.Metadata["@Statistics_EXPORT"].data;
        const exportTablesSet = new Set();
        for (let i = 0; i < rawExportStreams.length; i++) {
          exportTablesSet.add(rawExportStreams[i][5]);
        }
        voltDbRenderer.exportTablesArray = Array.from(exportTablesSet);
      }

      if (
        connection.Metadata["@Statistics_TABLE"] != undefined ||
        connection.Metadata["@Statistics_TABLE"] != null
      ) {
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
              $.each(
                partitionData[tupleData[tableNameIndex]],
                function (nestKey, nestData) {
                  for (
                    i = 0;
                    i < partitionData[tupleData[tableNameIndex]].length;
                    i++
                  ) {
                    partitionEntryCount++;
                    //if partition is repeated for a given table in "partitionData"
                    if (
                      tupleData[partitionIndex] ==
                      partitionData[tupleData[tableNameIndex]][i][
                      partitionIndex
                      ]
                    ) {
                      duplicatePartition = true;
                      return false;
                    }
                  }
                  if (
                    partitionEntryCount ==
                    partitionData[tupleData[tableNameIndex]].length &&
                    !duplicatePartition
                  ) {
                    partitionData[tupleData[tableNameIndex]].push(tupleData);
                    return false;
                  }
                }
              );
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
            TABLE_NAME: key,
            MAX_ROWS: Math.max.apply(null, tupleCountPartitions),
            MIN_ROWS: Math.min.apply(null, tupleCountPartitions),
            AVG_ROWS: getAverage(tupleCountPartitions),
            TUPLE_COUNT:
              schemaCatalogTableTypes[key].REMARKS == "REPLICATED"
                ? data[0][tupleCountIndex]
                : totalTupleCount,
            TABLE_TYPE: schemaCatalogTableTypes[key].REMARKS,
            drEnabled: schemaCatalogTableTypes[key].drEnabled,
            TABLE_TYPE1: schemaCatalogTableTypes[key].TABLE_TYPE,
          };
        });
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
  };

  window.voltDbRenderer = voltDbRenderer = new iVoltDbRenderer();
})(window);

//Navigation responsive
$(function () {
  $("#toggleMenu").on("click", function () {
    $("#nav").slideToggle("slow");
    $("#nav").css("left", "0");
    $("#nav ul li").on("click", function () {
      $("#nav").css("display", "none");
      $(window).trigger("resize");
    });
  });
});

$(window).on("resize", function () {
  var windowWidth = $(window).width();
  if (windowWidth > 699) {
    $("#nav").css("display", "block");
  } else if (windowWidth < 699) {
    $("#nav").css("display", "none");
  }
});
