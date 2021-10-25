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
  var iVoltDbService = function () {
    var _connection = connection;

    this.TestConnection = function (
      lServerName,
      lPort,
      lUsername,
      lPassword,
      lAdmin,
      onConnectionAdded,
      isLoginTest
    ) {
      try {
        var serverName = lServerName != null ? lServerName : server;
        var portId = lPort != null ? lPort : port;

        VoltDBCore.TestConnection(
          serverName,
          portId,
          lAdmin,
          lUsername,
          lPassword,
          isHashedPassword,
          "DATABASE_LOGIN",
          function (result, response) {
            onConnectionAdded(result, response);
          },
          isLoginTest
        );
      } catch (e) {
        console.log(e.message);
      }
    };

    this.CheckServerConnection = function (checkConnection) {
      try {
        VoltDBCore.CheckServerConnection(
          server,
          port,
          admin,
          user,
          password,
          isHashedPassword,
          "DATABASE_LOGIN",
          checkConnection
        );
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
    };

    this.ChangeServerConfiguration = function (
      serverName,
      portId,
      userName,
      pw,
      isHashPw,
      isAdmin
    ) {
      server = serverName != null ? serverName : server;
      port = portId != null ? portId : port;
      user = userName != undefined ? userName : "";
      password = pw != undefined ? pw : "";
      isHashedPassword = isHashPw;
      admin = isAdmin != undefined ? isAdmin : true;
    };

    this.GetSystemInformation = function (onConnectionAdded) {
      var procedureNames = ["@SystemInformation", "@Statistics"];
      var parameters = ["OVERVIEW", "MEMORY"];
      var values = [undefined, "0"];
      this.processTaskWithUIUpdate(
        onConnectionAdded,
        "SYSTEM_INFORMATION",
        procedureNames,
        parameters,
        values
      );
    };

    this.GetClusterInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "CLUSTER_INFORMATION",
        ["@SystemInformation"],
        ["OVERVIEW"],
        []
      );
    };

    this.GetSystemInformationDeployment = function (onConnectionAdded) {
      this.processTaskWithUIUpdate(
        onConnectionAdded,
        "SYSTEM_INFORMATION_DEPLOYMENT",
        ["@SystemInformation"],
        ["DEPLOYMENT"],
        []
      );
    };

    this.GetDataTablesInformation = function (onConnectionAdded) {
      var procedureNames = [
        "@Statistics",
        "@Statistics",
        "@SystemCatalog",
        "@SystemCatalog",
      ];
      var parameters = ["TABLE", "EXPORT", "TABLES"];
      var values = ["0", "0", undefined];
      this.processTask(
        onConnectionAdded,
        "DATABASE_INFORMATION",
        procedureNames,
        parameters,
        values
      );
    };

    this.GetProceduresInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "DATABASE_INFORMATION",
        ["@Statistics"],
        ["PROCEDUREPROFILE"],
        ["0"]
      );
    };

    this.getProcedureContextForSorting = function () {
      this.processDatabaseInfoTask(
        "DATABASE_INFORMATION",
        ["@Statistics"],
        ["PROCEDUREPROFILE"],
        ["0"]
      );
    };

    this.getTablesContextForSorting = function () {
      this.processDatabaseInfoTask(
        "DATABASE_INFORMATION",
        ["@Statistics"],
        ["TABLE"],
        ["0"]
      );
    };

    this.processDatabaseInfoTask = function (
      processName,
      procedureNames,
      parameters,
      values
    ) {
      try {
        var lconnection = VoltDBCore.HasConnection(
          server,
          port,
          admin,
          user,
          processName
        );
        if (lconnection == null) {
          VoltDBCore.TestConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            processName,
            function (result) {
              if (result == true) {
                VoltDBCore.AddConnection(
                  server,
                  port,
                  admin,
                  user,
                  password,
                  isHashedPassword,
                  procedureNames,
                  parameters,
                  values,
                  processName,
                  function (connection, status) {
                    lconnection = connection;
                  }
                );
              }
            }
          );
        }
        return lconnection;
      } catch (e) {
        console.log(e.message);
      }
    };

    this.GetExportTablesInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "EXPORT_TABLE_INFORMATION",
        ["@Statistics"],
        ["TABLE"],
        ["0"]
      );
    };

    this.GetProcedureDetailInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "PROCEDURE_DETAIL_INFORMATION",
        ["@Statistics"],
        ["PROCEDUREDETAIL"],
        ["0"]
      );
    };

    this.GetImportRequestInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "IMPORT_REQUEST_INFORMATION",
        ["@Statistics"],
        ["IMPORTER"],
        ["0"]
      );
    };

    this.GetMemoryInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_MEMORY",
        ["@Statistics"],
        ["MEMORY"],
        ["0"]
      );
    };

    this.GetGraphLatencyInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_LATENCY",
        ["@Statistics"],
        ["LATENCY"],
        ["0"]
      );
    };

    this.GetPartitionIdleTimeInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_PARTITIONIDLETIME",
        ["@Statistics"],
        ["STARVATION"],
        ["0"]
      );
    };

    this.GetCPUInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_CPU",
        ["@Statistics"],
        ["CPU"],
        ["0"]
      );
    };

    this.GetImporterInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_IMPORTER",
        ["@Statistics"],
        ["IMPORTER"],
        ["0"]
      );
    };

    this.GetExporterInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GRAPH_EXPORT",
        ["@Statistics"],
        ["EXPORT"],
        ["0"]
      );
    };

    this.GetTableInformation = function (onConnectionAdded) {
      var procedureNames = [
        "@Statistics",
        "@Statistics",
        "@Statistics",
        "@SystemCatalog",
        "@SystemCatalog",
        "@SystemCatalog",
      ];
      var parameters = [
        "TABLE",
        "INDEX",
        "EXPORT",
        "COLUMNS",
        "PROCEDURES",
        "PROCEDURECOLUMNS",
      ];
      var values = ["0", "0", "0", undefined];
      this.processTaskAdmin(
        onConnectionAdded,
        "TABLE_INFORMATION",
        procedureNames,
        parameters,
        values,
        true,
        false
      );
    };

    this.GetTableInformationOnly = function (onConnectionAdded) {
      var procedureNames = ["@Statistics", "@SystemCatalog"];
      var parameters = ["TABLE", "TABLES"];
      var values = ["0", undefined];
      this.processTaskAdmin(
        onConnectionAdded,
        "TABLE_INFORMATION_ONLY",
        procedureNames,
        parameters,
        values,
        true,
        false
      );
    };

    this.GetTableInformationClientPort = function (onConnectionAdded) {
      var procedureNames = [
        "@Statistics",
        "@Statistics",
        "@SystemCatalog",
        "@SystemCatalog",
        "@SystemCatalog",
      ];
      var parameters = [
        "TABLE",
        "INDEX",
        "COLUMNS",
        "PROCEDURES",
        "PROCEDURECOLUMNS",
      ];
      var values = ["0", "0", undefined];
      this.processTaskAdmin(
        onConnectionAdded,
        "TABLE_INFORMATION_CLIENTPORT",
        procedureNames,
        parameters,
        values,
        true,
        true
      );
    };

    this.SetConnectionForSQLExecution = function (useAdminPort) {
      try {
        var processNameSuffix = useAdminPort ? "" : "_CLIENTPORT";
        var processName = "TABLE_INFORMATION" + processNameSuffix;
        var procedureNames = ["@Statistics"];
        var parameters = ["TABLE"];
        var values = ["0"];
        //For SQL Query tab, we need to pass admin as false. This way, if the database is paused, users can't accidentally send
        //requests that might change database contents. However, if the user has admin privileges he should be given an option to
        //run the query even when the database is paused. This is done by passing admin as true.
        var isAdmin = useAdminPort ? true : false;
        _connection = VoltDBCore.HasConnection(
          server,
          port,
          isAdmin,
          user,
          processName
        );
        if (_connection == null) {
          VoltDBCore.AddConnection(
            server,
            port,
            isAdmin,
            user,
            password,
            isHashedPassword,
            procedureNames,
            parameters,
            values,
            processName,
            function (connection, status) { },
            null,
            false
          );
        }
      } catch (e) {
        console.log(e.message);
      }
    };

    this.GetShortApiProfile = function (onConnectionAdded) {
      this.processTaskWithAPIDetail(
        onConnectionAdded,
        "SHORTAPI_PROFILE",
        [],
        [],
        [],
        { isShortApiCall: true, apiPath: "profile" }
      );
    };

    this.GetShortApiDeployment = function (onConnectionAdded) {
      this.processTaskWithAPIDetail(
        onConnectionAdded,
        "SHORTAPI_DEPLOYMENT",
        [],
        [],
        [],
        { isShortApiCall: true, apiPath: "deployment" }
      );
    };

    this.GetExportProperties = function (onConnectionAdded) {
      this.processTaskWithAPIDetail(
        onConnectionAdded,
        "SHORTAPI_DEPLOYMENT_EXPORTTYPES",
        [],
        [],
        [],
        { isShortApiCall: true, apiPath: "deployment/export/types" }
      );
    };

    //Update admin configuration
    this.UpdateAdminConfiguration = function (updatedData, onConnectionAdded) {
      try {
        if (updatedData.systemsettings.resourcemonitor != null) {
          if (updatedData.systemsettings.resourcemonitor.memorylimit != null) {
            var memorySize = "";
            if (
              updatedData.systemsettings.resourcemonitor.memorylimit.size.indexOf(
                "%"
              ) > -1
            ) {
              rawMemSize = updatedData.systemsettings.resourcemonitor.memorylimit.size;
              memorySize = rawMemSize.slice(0, rawMemSize.indexOf("%") + 1)
              memorySize = parseInt(memorySize.replace("%", ""));
              updatedData.systemsettings.resourcemonitor.memorylimit.size = memorySize + encodeURIComponent("%");
            } else {
              updatedData.systemsettings.resourcemonitor.memorylimit.size =
                encodeURIComponent(
                  parseInt(
                    updatedData.systemsettings.resourcemonitor.memorylimit.size
                  )
                );
            }

            if (
              "alert" in updatedData.systemsettings.resourcemonitor.memorylimit
            ) {
              var memoryAlert = "";
              if (
                updatedData.systemsettings.resourcemonitor.memorylimit.alert.indexOf(
                  "%"
                ) > -1
              ) {
                rawMemAlert = updatedData.systemsettings.resourcemonitor.memorylimit.alert;
                memoryAlert = rawMemAlert.slice(0, rawMemAlert.indexOf("%") + 1)
                memoryAlert = parseInt(memoryAlert.replace("%", ""));
                updatedData.systemsettings.resourcemonitor.memorylimit.alert =
                  memoryAlert + encodeURIComponent("%");
              } else {
                updatedData.systemsettings.resourcemonitor.memorylimit.alert =
                  encodeURIComponent(
                    parseInt(
                      updatedData.systemsettings.resourcemonitor.memorylimit
                        .alert
                    )
                  );
              }
            }
          }
        }

        var features = [];

        if (updatedData.systemsettings.resourcemonitor != null) {
          if (updatedData.systemsettings.resourcemonitor.disklimit != null) {
            for (
              var i = 0;
              i <
              updatedData.systemsettings.resourcemonitor.disklimit.feature
                .length;
              i++
            ) {
              var diskSize = "";
              if (
                updatedData.systemsettings.resourcemonitor.disklimit.feature[
                  i
                ].size.indexOf("%") > -1
              ) {
                diskSize = parseInt(
                  updatedData.systemsettings.resourcemonitor.disklimit.feature[
                    i
                  ].size.replace("%", "")
                );
                updatedData.systemsettings.resourcemonitor.disklimit.feature[
                  i
                ].size = diskSize + encodeURIComponent("%");
              } else {
                updatedData.systemsettings.resourcemonitor.disklimit.feature[
                  i
                ].size = encodeURIComponent(
                  parseInt(
                    updatedData.systemsettings.resourcemonitor.disklimit
                      .feature[i].size
                  )
                );
              }

              features.push({
                name: updatedData.systemsettings.resourcemonitor.disklimit
                  .feature[i].name,
                size: updatedData.systemsettings.resourcemonitor.disklimit
                  .feature[i].size,
              });

              if (
                "alert" in
                updatedData.systemsettings.resourcemonitor.disklimit.feature[i]
              ) {
                var diskAlert = "";
                if (updatedData.systemsettings.resourcemonitor.disklimit.feature[i].alert !== null) {
                  if (
                    updatedData.systemsettings.resourcemonitor.disklimit.feature[
                      i
                    ].alert.indexOf("%") > -1
                  ) {
                    diskAlert = parseInt(
                      updatedData.systemsettings.resourcemonitor.disklimit.feature[
                        i
                      ].alert.replace("%", "")
                    );
                    updatedData.systemsettings.resourcemonitor.disklimit.feature[
                      i
                    ].alert = diskAlert + encodeURIComponent("%");
                  } else {
                    updatedData.systemsettings.resourcemonitor.disklimit.feature[
                      i
                    ].alert = encodeURIComponent(
                      parseInt(
                        updatedData.systemsettings.resourcemonitor.disklimit
                          .feature[i].alert
                      )
                    );
                  }
                  features.push({
                    alert:
                      updatedData.systemsettings.resourcemonitor.disklimit
                        .feature[i].alert,
                  });
                }
              }
            }
            updatedData.systemsettings.resourcemonitor.disklimit.feature =
              features;
          }
        }

        var shortApiDetails = {
          isShortApiCall: true,
          isUpdateConfiguration: true,
          apiPath: "deployment",
          updatedData: "deployment=" + JSON.stringify(updatedData),
        };
        this.processTaskWithAPIDetail(
          onConnectionAdded,
          "SHORTAPI_UPDATEDEPLOYMENT",
          [],
          [],
          [],
          shortApiDetails
        );
      } catch (e) {
        console.log(e.message);
      }
    };

    //admin configuration
    this.editConfigurationItem = function (
      configGroup,
      configMember,
      configValue,
      onConnectionAdded
    ) {
      var processName = "ADMIN_".concat(configGroup);
      var procedureNames = [];
      var parameters = [];
      var values = [];
      var isAdmin = true;

      switch (configGroup) {
        case "OVERVIEW":
          procedureNames = ["@SystemInformation"];
          parameters = [configMember];
          values = [configValue];
          break;
        case "PORT":
          procedureNames = ["@SystemInformation"];
          parameters = [configMember];
          values = [configValue];
          break;
        case "DIRECTORIES":
          procedureNames = ["@SystemInformation"];
          parameters = [configMember];
          values = [configValue];
          break;
      }
      this.processTask(
        onConnectionAdded,
        processName,
        procedureNames,
        parameters,
        values
      );
    };

    this.stopServerNode = function (nodeId, onConnectionAdded) {
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_STOPSERVER",
        ["@StopNode"],
        [nodeId.toString()],
        [undefined],
        function (connection) {
          status =
            connection.Metadata["@StopNode_" + nodeId.toString() + "_status"];
          statusString =
            connection.Metadata[
            "@StopNode_" + nodeId.toString() + "_statusString"
            ];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status, statusString);
          }
        }
      );
    };

    this.PauseClusterState = function (onConnectionAdded) {
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_PAUSECLUSTER",
        ["@Pause"],
        [undefined],
        [undefined],
        function (connection) {
          status = connection.Metadata["@Pause_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status);
          }
        }
      );
    };

    this.ResumeClusterState = function (onConnectionAdded) {
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_RESUMECLUSTER",
        ["@Resume"],
        [undefined],
        [undefined],
        function (connection) {
          status = connection.Metadata["@Resume_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status);
          }
        }
      );
    };

    this.ShutdownClusterState = function (onConnectionAdded, zk_pause_txn_id) {
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_SHUTDOWNCLUSTER",
        ["@Shutdown"],
        [zk_pause_txn_id],
        [undefined],
        function (connection) {
          status = connection.Metadata["@Shutdown_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status);
          }
        }
      );
    };

    this.PrepareShutdownCluster = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "PREPARE_SHUTDOWN_CLUSTER",
        ["@PrepareShutdown"],
        [undefined],
        [undefined]
      );
    };

    this.QuiesceCluster = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "QUIESCE_CLUSTER",
        ["@Quiesce"],
        [undefined],
        [undefined]
      );
    };

    this.GetDrProducerInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "DR_PRODUCER_INFORMATION",
        ["@Statistics"],
        ["DRPRODUCER"],
        ["0"]
      );
    };

    this.PromoteCluster = function (onConnectionAdded) {
      var parameters = ["'" + snapshotDir + "'", snapshotFileName, 0];
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_PROMOTECLUSTER",
        ["@Promote"],
        [undefined],
        [undefined],
        function (connection) {
          status = connection.Metadata["@Promote_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(
              connection,
              status,
              connection.Metadata["@Promote_statusstring"]
            );
          }
        }
      );
    };

    this.SaveSnapShot = function (
      snapshotDir,
      snapshotFileName,
      onConnectionAdded
    ) {
      var parameters = ["'" + snapshotDir + "'", snapshotFileName, 0];
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_SAVESNAPSHOT",
        ["@SnapshotSave"],
        parameters,
        [undefined],
        function (connection) {
          status = connection.Metadata["@SnapshotSave_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status);
          }
        }
      );
    };

    this.GetSnapshotList = function (snapshotDirectory, onConnectionAdded) {
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_SCANSNAPSHOTS",
        ["@SnapshotScan"],
        [snapshotDirectory],
        [undefined],
        function (connection) {
          status = connection.Metadata["@SnapshotScan_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(connection, status);
          }
        }
      );
    };

    this.RestoreSnapShot = function (
      snapshotDir,
      snapshotFileName,
      onConnectionAdded
    ) {
      var parameters = ["'" + snapshotDir + "'", snapshotFileName, 0];
      this.processTaskWithCallBack(
        "SYSTEMINFORMATION_RESTORESNAPSHOT",
        ["@SnapshotRestore"],
        parameters,
        [undefined],
        function (connection) {
          status = connection.Metadata["@SnapshotRestore_status"];
          if (!(status == "" || status == undefined)) {
            onConnectionAdded(
              connection,
              status,
              connection.Metadata["@SnapshotRestore_statusstring"]
            );
          }
        }
      );
    };

    //Update User configuration
    this.UpdateUserConfiguration = function (
      updatedData,
      onConnectionAdded,
      userId,
      requestType
    ) {
      var shortApiDetails = {
        isShortApiCall: true,
        isUpdateConfiguration: true,
        apiPath: "deployment/users/" + userId,
        updatedData: "user=" + JSON.stringify(updatedData),
        requestType: requestType,
      };
      this.processTaskWithAPIDetail(
        onConnectionAdded,
        "SHORTAPI_USERUPDATEDEPLOYMENT",
        [],
        [],
        [],
        shortApiDetails
      );
    };

    //Check if DR is enable or not
    this.GetDrStatusInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "DR_INFORMATION",
        ["@Statistics"],
        ["DR"],
        ["0"]
      );
    };

    //Check if cluster is replica or not
    this.GetClusterReplicaInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "CLUSTER_REPLICA_INFORMATION",
        ["@SystemInformation"],
        ["Overview"],
        []
      );
    };

    this.GetDrRoleInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "DR_ROLES",
        ["@Statistics"],
        ["DRROLE"],
        ["0"]
      );
    };

    //Get datas for DR Replication Graph
    this.GetDrReplicationInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "DR_REPLICATION_GRAPH",
        ["@Statistics"],
        ["DRCONSUMER"],
        ["0"]
      );
    };

    this.GetLiveClientsInfo = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "LIVE_CLIENTS_INFORMATION",
        ["@Statistics"],
        ["LIVECLIENTS"],
        ["0"]
      );
    };

    //Get host and site count
    this.GetHostAndSiteCount = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "GET_HOST_SITE_COUNT",
        ["@Statistics"],
        ["STARVATION"],
        ["0"]
      );
    };

    this.GetCommandLogInformation = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "CMD_LOG_INFO",
        ["@Statistics"],
        ["COMMANDLOG"],
        ["0"]
      );
    };

    this.GetSnapshotStatus = function (onConnectionAdded) {
      this.processTask(
        onConnectionAdded,
        "SNAPSHOT_STATUS",
        ["@Statistics"],
        ["SNAPSHOTSTATUS"],
        ["0"]
      );
    };

    this.processTask = function (
      onConnectionAdded,
      processName,
      procedureNames,
      parameters,
      values
    ) {
      this.processTaskAdmin(
        onConnectionAdded,
        processName,
        procedureNames,
        parameters,
        values,
        admin,
        false
      );
    };

    this.processTaskAdmin = function (
      onConnectionAdded,
      processName,
      procedureNames,
      parameters,
      values,
      isAdmin,
      adminReset
    ) {
      try {
        _connection = VoltDBCore.HasConnection(
          server,
          port,
          isAdmin,
          user,
          processName
        );
        if (_connection == null) {
          VoltDBCore.TestConnection(
            server,
            port,
            isAdmin,
            user,
            password,
            isHashedPassword,
            processName,
            function (result) {
              if (result == true) {
                VoltDBCore.AddConnection(
                  server,
                  port,
                  isAdmin,
                  user,
                  password,
                  isHashedPassword,
                  procedureNames,
                  parameters,
                  values,
                  processName,
                  function (connection, status) {
                    if (adminReset) {
                      connection.admin = false; //Once necessary data has been fetched, set the admin privileges to false.
                    }
                    onConnectionAdded(connection, status);
                  }
                );
              }
            }
          );
        } else {
          if (adminReset) {
            _connection.admin = true;
          }
          VoltDBCore.updateConnection(
            server,
            port,
            isAdmin,
            user,
            password,
            isHashedPassword,
            procedureNames,
            parameters,
            values,
            processName,
            _connection,
            function (connection, status) {
              if (adminReset) {
                connection.admin = false; //Once necessary data has been fetched, set the admin privileges to false.
              }
              onConnectionAdded(connection, status);
            }
          );
        }
      } catch (e) {
        console.log(e.message);
      }
    };

    this.processTaskWithAPIDetail = function (
      onConnectionAdded,
      processName,
      procedureNames,
      parameters,
      values,
      shortApiDetails
    ) {
      try {
        _connection = VoltDBCore.HasConnection(
          server,
          port,
          admin,
          user,
          processName
        );
        if (_connection == null) {
          VoltDBCore.TestConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            processName,
            function (result) {
              if (result == true) {
                VoltDBCore.AddConnection(
                  server,
                  port,
                  admin,
                  user,
                  password,
                  isHashedPassword,
                  procedureNames,
                  parameters,
                  values,
                  processName,
                  function (connection, status) {
                    onConnectionAdded(connection, status);
                  },
                  shortApiDetails
                );
              }
            }
          );
        } else {
          VoltDBCore.updateConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            procedureNames,
            parameters,
            values,
            processName,
            _connection,
            function (connection, status) {
              onConnectionAdded(connection, status);
            },
            shortApiDetails
          );
        }
      } catch (e) {
        console.log(e.message);
      }
    };

    this.processTaskWithCallBack = function (
      processName,
      procedureNames,
      parameters,
      values,
      statusCallback
    ) {
      try {
        _connection = VoltDBCore.HasConnection(
          server,
          port,
          admin,
          user,
          processName
        );
        if (_connection == null) {
          VoltDBCore.TestConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            processName,
            function (result) {
              if (result == true) {
                VoltDBCore.AddConnection(
                  server,
                  port,
                  admin,
                  user,
                  password,
                  isHashedPassword,
                  procedureNames,
                  parameters,
                  values,
                  processName,
                  function (connection, status) {
                    statusCallback(connection);
                  }
                );
              }
            }
          );
        } else {
          VoltDBCore.updateConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            procedureNames,
            parameters,
            values,
            processName,
            _connection,
            function (connection, status) {
              statusCallback(connection);
            }
          );
        }
      } catch (e) {
        console.log(e.message);
      }
    };

    this.processTaskWithUIUpdate = function (
      onConnectionAdded,
      processName,
      procedureNames,
      parameters,
      values
    ) {
      try {
        _connection = VoltDBCore.HasConnection(
          server,
          port,
          admin,
          user,
          processName
        );
        if (_connection == null) {
          VoltDBCore.TestConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            processName,
            function (result) {
              if (result == true) {
                updateTips("Connection successful.");
                VoltDBCore.AddConnection(
                  server,
                  port,
                  admin,
                  user,
                  password,
                  isHashedPassword,
                  procedureNames,
                  parameters,
                  values,
                  processName,
                  function (connection, status) {
                    onConnectionAdded(connection);
                  }
                );
              } else {
                updateTips("Unable to connect.");
              }
            }
          );
        } else {
          VoltDBCore.updateConnection(
            server,
            port,
            admin,
            user,
            password,
            isHashedPassword,
            procedureNames,
            parameters,
            values,
            processName,
            _connection,
            function (connection, status) {
              onConnectionAdded(connection);
            }
          );
        }
      } catch (e) {
        console.log(e.message);
      }
      function updateTips(t) {
        tips.text(t).addClass("ui-state-highlight");
        setTimeout(function () {
          tips.removeClass("ui-state-highlight", 1500);
        }, 500);
      }
    };
  };

  window.VoltDBService = VoltDBService = new iVoltDbService();
})(window);
