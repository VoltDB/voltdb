var adminDOMObjects = {};
var adminEditObjects = {};
var adminClusterObjects = {};
var duplicateValue = "";
var editStates = {
    ShowEdit: 0,
    ShowOkCancel: 1,
    ShowLoading: 2
};
var INT_MAX_VALUE = 2147483647;
var client_port = 0;

function getListOfRoles() {
    const url = `api/1.0/?Procedure=%40SystemCatalog&Parameters=%5B"ROLES"%5D`;
    var rolesList = [];
    $.ajax({
        url: url,
        type: 'get',
        success: function (response) {
            var result = response.results[0].data;
            rolesList = result.map((item) => {
                return item[0]
            })
        }
    }).done(function () {
        voltDbRenderer.usersRoles = rolesList;
    })

}

// function rolehtml() {
//     var roles = voltDbRenderer.usersRoles;
//     var role_options = "";
//     for (var i = 0; i < roles.length; i++) {
//         role_options += '<option value="' + roles[i] + '">' + roles[i] + '</option>';
//     }
//     return role_options
// }

function rolehtml() {
    var roles = voltDbRenderer.usersRoles;
    var role_options = "";
    for (var i = 0; i < roles.length; i++) {
        role_options += '<label>' + '<input type="checkbox" id="' + roles[i] + '" value="' + roles[i] + '"/>'
            + "<span style='margin-left: 4px'>" + roles[i] + "</span>" + '</label>';
    }
    return role_options
}
/*
* Set the admin tab to read-only if we are running under Kubernetes 
*/
function set_kubernetes_admin() {
    if ($("#admin").hasClass("kubernetes")) return;
    console.log("Setting Kubernetes read-only");
    $("#admin").addClass("kubernetes");
    var k8s_banner =
        '<div class="kubernetes-content">' +
        '<div class="kubernetes-logo"><p class="kubernetes-title">Managed by Kubernetes</p><p class="kubernetes-subtitle">Use Helm to manage and administer your cluster</p></div>' +
        '</div>';
    $(".adminLeft").html(k8s_banner);
}

function loadAdminPage() {

    adminClusterObjects = {
        btnClusterPause: $('#pauseConfirmation'),
        btnClusterResume: $('#resumeConfirmation'),
        btnClusterShutdown: $('#shutDownConfirmation'),
        btnClusterSaveSnapshot: $('#saveConfirmation'),
        txtSnapshotDirectory: $('#txtSnapshotDirectory'),
        btnClusterPromote: $('#promoteConfirmation'),
        enablePromote: false,
        ignorePromoteUpdateCount: 0,
        ignoreServerListUpdateCount: 0,
        btnErrorClusterPromote: $('#btnErrorPromotePopup'),
        errorPromoteMessage: $('#promoteErrorMessage'),
        updateMessageBar: $('#snapshotBar'),
        errorRestoreMsgContainer: $('#errorRestoreMsgContainer'),
        userListObj: []
    };

    adminDOMObjects = {
        addConfigLink: $("#addConfigPopupLink"),
        addImportConfigPopupLink: $("#addImportConfigPopupLink"),
        siteNumberHeader: $("#sitePerHost"),
        kSafety: $("#kSafety"),
        partitionDetection: $("#partitionDetectionIcon"),
        partitionDetectionLabel: $("#partitionDetectionLabel"),
        security: $('#securityOptionIcon'),
        securityLabel: $("#spanSecurity"),
        httpAccess: $("#httpAccessIcon"),
        httpAccessLabel: $("#httpAccessLabel"),
        jsonAPI: $("#jsonAPIIcon"),
        jsonAPILabel: $("#txtJsonAPI"),
        autoSnapshot: $("#autoSnapshotIcon"),
        autoSnapshotLabel: $("#txtAutoSnapshot"),
        filePrefix: $("#prefixSpan"),
        frequency: $("#txtFrequency"),
        frequencyLabel: $("#spanFrequencyUnit"),
        retained: $("#retainedSpan"),
        retainedLabel: $("#retainedSpanUnit"),
        commandLog: $("#commandLogIcon"),
        commandLogLabel: $("#commandLogLabel"),
        commandLogFrequencyTime: $("#commandLogFrequencyTime"),
        commandLogFrequencyTimeLabel: $("#commandLogFrequencyUnit"),
        commandLogFrequencyTransactions: $("#commandLogFrequencyTxns"),
        commandLogSegmentSize: $("#commandLogSegmentSize"),
        commandLogSegmentSizeLabel: $("#commandLogSegmentSizeUnit"),
        properties: $("#properties"),
        maxJavaHeap: $("#maxJavaHeap"),
        maxJavaHeapLabel: $("#maxJavaHeapUnit"),
        heartBeatTimeout: $("#hrtTimeOutSpan"),
        heartBeatTimeoutLabel: $("#hrtTimeOutUnitSpan"),
        queryTimeout: $("#queryTimeOutSpan"),
        queryTimeoutLabel: $("#queryTimeOutUnitSpan"),
        tempTablesMaxSize: $("#temptablesmaxsize"),
        tempTablesMaxSizeLabel: $("#temptablesmaxsizeUnit"),
        snapshotPriority: $("#snapshotpriority"),
        memoryLimitSize: $("#memoryLimitSize"),
        memoryLimitSizeUnit: $("#memoryLimitSizeUnit"),
        clientPort: $('#clientport'),
        adminPort: $('#adminport'),
        httpPort: $('#httpport'),
        internalPort: $('#internalPort'),
        zookeeperPort: $('#zookeeperPort'),
        replicationPort: $('#replicationPort'),
        voltdbRoot: $('#voltdbroot'),
        snapshotPath: $('#snapshotpath'),
        exportOverflow: $('#exportOverflow'),
        commandLogPath: $('#commandlogpath'),
        commandLogSnapshotPath: $('#commandlogsnapshotpath'),
        drOverflowPath: $('#droverflowpath'),
        clusterClientPort: $('#clusterClientport'),
        clusterAdminPort: $('#clusterAdminport'),
        clusterHttpPort: $('#clusterHttpport'),
        clusterInternalPort: $('#clusterInternalPort'),
        clusterZookeeperPort: $('#clusterZookeeperPort'),
        clusterReplicationPort: $('#clusterReplicationPort'),
        //ServerList Section
        adminServerList: $("#serverListWrapperAdmin > .tblshutdown > tbody"),
        serverSettingHeader: $('#hServerSettings'),
        lstReplicatedTables: $('#lstDrTblLink'),
        lstExportTable: $('#lstExportTblLink'),
        addDiskLimitLink: $("#addDiskLimitPopupLink"),
    };

    adminEditObjects = {
        //Edit Security objects
        loadingSecurity: $("#loadingSecurity"),
        btnEditSecurityOk: $("#btnEditSecurityOk"),
        btnEditSecurityCancel: $("#btnEditSecurityCancel"),
        LinkSecurityEdit: $("#securityEdit"),
        chkSecurity: $("#chkSecurity"),
        chkSecurityValue: $("#chkSecurity").is(":checked"),
        iconSecurityOption: $("#securityOptionIcon"),
        spanSecurity: $("#spanSecurity"),
        securityLabel: $("#row-6").find("td:first-child").text(),
        editStateSecurity: editStates.ShowEdit,
        securityStateOriginal: { "SecurityStatus": false, "linkSecurityEdit": false },
        loadingUserList: $("#loadingUserList"),

        //Edit Auto Snapshot objects
        btnEditAutoSnapshotOk: $("#btnEditAutoSnapshotOk"),
        btnEditAutoSnapshotCancel: $("#btnEditAutoSnapshotCancel"),
        LinkAutoSnapshotEdit: $("#autoSnapshotEdit"),
        chkAutoSnapsot: $("#chkAutoSnapshot"),
        chkAutoSnapshotValue: $("#chkAutoSnapshot").is(":checked"),
        iconAutoSnapshotOption: $("#autoSnapshotIcon"),
        txtAutoSnapshot: $("#txtAutoSnapshot"),

        //        //Edit Snmp objects
        btnEditSnmpOk: $("#btnEditSnmpOk"),
        btnEditSnmpCancel: $("#btnEditSnmpCancel"),
        LinkSnmpEdit: $("#snmpEdit"),
        chkSnmp: $("#chkSnmp"),
        chkSnmpValue: $("#chkSnmp").is(":checked"),
        iconSnmpOption: $("#snmpIcon"),
        txtSnmp: $("#txtSnmp"),
        txtTarget: $("#txtTarget"),
        txtTargetValue: $("#txtTarget").text(),
        txtCommunity: $("#txtCommunity"),
        txtCommunityValue: $("#txtCommunity").text(),
        txtUsername: $("#txtUsername"),
        txtUsernameValue: $("#txtUsername").text(),
        targetSpan: $("#targetSpan"),
        communitySpan: $("#communitySpan"),
        usernameSpan: $("#usernameSpan"),
        authKeySpan: $("#authKeySpan"),
        privProtocolSpan: $("#privProtocolSpan"),
        privKeySpan: $("#privKeySpan"),
        ddlAuthProtocol: $("#ddlAuthProtocol"),
        ddlAuthProtocolValue: $("#authProtocolSpan").value,
        ddlPrivProtocol: $("#ddlPrivProtocol"),
        ddlPrivProtocolValue: $("#privProtocolSpan").value,
        txtPrivKey: $("#txtPrivKey"),
        txtPrivkeyValue: $("#txtPrivKey").text(),
        authProtocolSpan: $("#authProtocolSpan"),
        txtAuthkey: $("#txtAuthkey"),
        txtAuthkeyValue: $("#txtAuthkey").text(),
        snmpLabel: $("#row-7").find("td:first-child").text(),
        errorTarget: $("#errorTarget"),
        errorCommunity: $("#errorCommunity"),
        errorUserName: $("#errorUsername"),
        errorAuthkey: $("#errorAuthkey"),
        errorPrivKey: $("#errorPrivKey"),
        targetSpanValue: $("#targetSpan").text(),
        communitySpanValue: $("#communitySpan").text(),
        usernameSpanValue: $("#usernameSpan").text(),
        authKeySpanValue: $("#authKeySoan").text(),
        privProtocolSpanValue: $("#privProtocolSpan").text(),
        privKeySpanValue: $("#privKeySpan").text(),
        authProtocolSpanValue: $("#authProtocolSpan").text(),




        //File Prefix objects
        tBoxFilePrefix: $("#txtPrefix"),
        tBoxFilePrefixValue: $("#txtPrefix").text(),
        spanAutoSnapshotFilePrefix: $("#prefixSpan"),
        errorAutoSnapshotFilePrefix: $("#errorSnapshotFilePrefix"),
        loadingSnapshotPrefix: $("#loadingSnapshotPrefix"),

        //Frequency objects
        tBoxAutoSnapshotFreq: $("#txtFrequency"),
        tBoxAutoSnapshotFreqValue: $("#frequencySpan").text(),
        spanAutoSnapshotFreq: $("#frequencySpan"),
        ddlAutoSnapshotFreqUnit: $("#ddlfrequencyUnit"),
        ddlAutoSnapshotFreqUnitValue: $("#spanFrequencyUnit").text(),
        spanAutoSnapshotFreqUnit: $("#spanFrequencyUnit"),
        errorAutoSnapshotFreq: $("#errorSnapshotFrequency"),
        loadingSnapshotFrequency: $("#loadingSnapshotFrequency"),

        //Retained objects
        tBoxAutoSnapshotRetained: $("#txtRetained"),
        tBoxAutoSnapshotRetainedValue: $("#retainedSpan").text(),
        spanAutoSnapshotRetained: $("#retainedSpan"),
        errorAutoSnapshotRetained: $("#errorSnapshotRetained"),
        loadingSnapshotRetained: $("#loadingSnapshotRetained"),

        //snapshot
        editStateSnapshot: editStates.ShowEdit,
        loadingSnapshot: $("#loadingSnapshot"),

        //snmp
        editStateSnmp: editStates.ShowEdit,
        loadingSnmp: $("#loadingSnmp"),
        loadingTarget: $("#loadingTarget"),
        loadingCommunity: $("#loadingCommunity"),
        loadingUsername: $("#loadingUsername"),
        loadingAuthProtocol: $("#loadingAuthProtocol"),
        loadingAuthkey: $("#loadingAuthkey"),
        loadingPrivProtocol: $("#loadingPrivProtocol"),
        loadingPrivKey: $("#loadingPrivKey"),

        //Heartbeat Timeout
        rowHeartbeatTimeout: $("#heartbeatTimeoutRow"),
        btnEditHeartbeatTimeoutOk: $("#btnEditHeartbeatTimeoutOk"),
        btnEditHeartbeatTimeoutCancel: $("#btnEditHeartbeatTimeoutCancel"),
        LinkHeartbeatEdit: $("#btnEditHrtTimeOut"),
        tBoxHeartbeatTimeout: $("#txtHrtTimeOut"),
        tBoxHeartbeatTimeoutValue: $("#hrtTimeOutSpan").text(),
        spanHeartbeatTimeOut: $("#hrtTimeOutSpan"),
        loadingHeartbeatTimeout: $("#loadingHeartbeatTimeout"),
        errorHeartbeatTimeout: $("#errorHeartbeatTimeout"),
        editStateHeartbeatTimeout: editStates.ShowEdit,

        //Query Timeout
        rowQueryTimeout: $("#queryTimoutRow"),
        btnEditQueryTimeoutOk: $("#btnEditQueryTimeoutOk"),
        btnEditQueryTimeoutCancel: $("#btnEditQueryTimeoutCancel"),
        LinkQueryTimeoutEdit: $("#btnEditQueryTimeout"),
        tBoxQueryTimeout: $("#txtQueryTimeout"),
        tBoxQueryTimeoutValue: $("#queryTimeOutSpan").text(),
        spanqueryTimeOut: $("#queryTimeOutSpan"),
        loadingQueryTimeout: $("#loadingQueryTimeout"),
        errorQueryTimeout: $("#errorQueryTimeout"),
        editStateQueryTimeout: editStates.ShowEdit,

        //Memory Size Limit
        rowMemorySizeTimeout: $("#memorySizeTimeout"),
        btnEditMemorySize: $("#btnEditMemorySize"),
        btnEditMemorySizeOk: $("#btnEditMemorySizeOk"),
        btnEditMemorySizeCancel: $("#btnEditMemorySizeCancel"),
        btnDeleteMemory: $("#btnDeleteMemory"),
        spanMemoryLimitSizeValue: $("#memoryLimitSize").text(),
        spanMemoryLimitSize: $("#memoryLimitSize"),
        txtMemoryLimitSize: $("#txtMemoryLimitSize"),
        spanMemoryLimitSizeUnit: $("#memoryLimitSizeUnit"),
        ddlMemoryLimitSizeUnit: $("#ddlMemoryLimitUnit"),
        loadingMemoryLimit: $("#loadingMemoryLimit"),
        errorMemorySize: $("#errorMemorySize"),
        editStateMemorySize: editStates.ShowEdit,

        //Update Error
        updateErrorFieldMsg: $("#updateErrorFieldMsg"),
        updateSnapshotErrorFieldMsg: $("#updateSnapshotErrorFieldMsg"),
        updateSnmpErrorFieldMsg: $("#updateSnapshotErrorFieldMsg"),
        heartbeatTimeoutLabel: $("#heartbeatTimeoutRow").find("td:first-child").text(),
        queryTimeoutUpdateErrorFieldMsg: $("#queryTimeoutUpdateErrorFieldMsg"),
        snapshotLabel: $("#row-2").find("td:first-child").text(),
        queryTimeoutFieldLabel: $("#queryTimoutRow").find("td:first-child").text(),
        securityUserErrorFieldMsg: $("#securityUserErrorFieldMsg"),
        memoryLimitErrorFieldMsg: $("#memorySizeTimeout").find("td:first-child").text(),
        memoryLimitUpdateErrorFieldMsg: $("#memorySizeUpdateErrorFieldMsg"),
        diskLimitErrorFieldMsg: $("#diskSizeTimeout").find("td:first-child").text(),
        diskLimitUpdateErrorFieldMsg: $("#diskSizeUpdateErrorFieldMsg"),

        //Export Settings
        addNewConfigLink: $("#addNewConfigLink"),
        loadingConfiguration: $("#loadingConfiguration"),

        exportConfiguration: $("#exportConfiguration"),
        exportConfigurationLoading: $('#exportConfigurationLoading'),

        //Import Settings
        addNewImportConfigLink: $("#addNewImportConfigLink"),

        loadingImportConfig: $("#loadingImportConfig"),
        importConfiguration: $("#importConfiguration"),

        //Disk Limit Settings
        editDiskLimitLink: $("#btnEditDiskLimit"),
        loadingDiskLimit: $("#loadingDiskLimit"),
        diskLimitConfiguration: $("#diskLimitConfiguration"),


        //Dr Mode object
        labelDrmode: $("#drMode"),
        labelDrId: $("#drId"),

        //Edit Dr Master objects
        btnEditDrMasterOk: $("#btnEditDrMasterOk"),
        btnEditDrMasterCancel: $("#btnEditDrMasterCancel"),
        LinkDrMasterEdit: $("#drMasterEdit"),
        chkDrMaster: $("#chkDrMaster"),
        chkDrMasterValue: $("#chkDrMaster").is(":checked"),
        iconDrMasterOption: $("#drMasterIcon"),
        txtDrMaster: $("#txtDrMaster"),
        spanDrMasterEdited: "",
        loadingDrMaster: $("#loadingDrMaster"),
        updateDrMasterErrorFieldMsg: $("#updateDrMasterErrorFieldMsg"),
        drMasterLabel: $("#row-DrConfig").find("td:first-child").text(),

        //Edit Dr Replica objects
        chkDrReplica: $("#chkDrReplica"),
        chkDrReplicaValue: $("#chkDrReplica").is(":checked"),
        iconDrReplicaOption: $("#drReplicaIcon"),
        txtDrReplica: $("#txtDrReplica"),
        labelReplicaSource: $("#replicaSource"),
    };

    var adminValidationRules = {
        numericRules: {
            required: true,
            min: 1,
            max: INT_MAX_VALUE,
            digits: true,
        },
        numericMessages: {
            required: "Please enter a valid positive number.",
            min: "Please enter a positive number. Its minimum value should be 1.",
            max: "Please enter a positive number between 1 and " + INT_MAX_VALUE + ".",
            digits: "Please enter a positive number without any decimal."
        },
        memoryLimitRules: {
            required: true,
            min: 1,
            max: 99,
            digits: true
        },
        memoryLimitMessages: {
            required: "This field is required",
            min: "Please enter a positive number.",
            max: "Please enter a positive number less than 100.",
            digits: "Please enter a positive number without any decimal." //"Only four digits are allowed after decimal."
        },
        fileNameRules: {
            required: true,
            minlength: 2,
            regex: /^[a-zA-Z0-9_.]+$/
        },
        fileNameMessages: {
            required: "Please enter a valid file name.",
            minlength: "Please enter at least 2 characters.",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        directoryPathRules: {
            required: true,
            minlength: 2,
        },
        directoryPathMessages: {
            required: "Please enter a valid directory path.",
            minlength: "Please enter at least 2 characters.",
        },
        restoreSnapshotRules: {
            required: true
        },
        restoreSnapshotMessages: {
            required: "Please select a snapshot to restore."
        },
        streamNameRules: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/
        },
        streamNameMessages: {
            required: "This field is required",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        userNameRule: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/,
            checkDuplicate: []
        },
        userNameMessage: {
            required: "Username field is required",
            regex: 'Only alphabets, numbers, _ and . are allowed.',
            checkDuplicate: 'This username already exists.'
        },
        passwordRule: {
            required: true

        },
        passwordMessage: {
            required: "Password field is required",
        },
        queryTimeoutRules: {
            required: true,
            min: 0,
            max: INT_MAX_VALUE,
            digits: true,
        },
        queryTimeoutMessages: {
            required: "Please enter a valid positive number.",
            min: "Please enter a positive number.",
            max: "Please enter a positive number between 0 and " + INT_MAX_VALUE + ".",
            digits: "Please enter a positive number without any decimal."
        },
        authKeyRules: {
            required: true,
            minlength: 8
        },
        authKeyMessages: {
            required: "This field is required",
            minlength: "Please enter at least 8 characters."
        },
        privKeyRules: {
            required: true,
            minlength: 8
        },
        privKeyMessages: {
            required: "This field is required",
            minlength: "Please enter at least 8 characters.",
        },
        targetRules: {
            required: true,
            portRegex: /^[a-zA-Z0-9.-]+$/,
        },
        targetMessages: {
            required: "This field is required",
            portRegex: "Please enter a valid value.(e.g, hostname:(1-65535))"
        }
    };

    //Admin Page download link
    $('#downloadAdminConfigurations').on('click', function (e) {
        var port = VoltDBConfig.GetPortId() != null ? VoltDBConfig.GetPortId() : '8080';
        var url = window.location.protocol + '//' + VoltDBConfig.GetDefaultServerIP() + ":" + port + '/deployment/download/?' + VoltDBCore.shortApiCredentials;
        $(this).attr("href", url);
        setTimeout(function () {
            $('#downloadAdminConfigurations').attr("href", "#");
        }, 100);
    });

    adminEditObjects.chkSecurity.on('ifChanged', function () {
        adminEditObjects.chkSecurityValue = adminEditObjects.chkSecurity.is(":checked");
        adminEditObjects.spanSecurity.text(getOnOffText(adminEditObjects.chkSecurity.is(":checked")));
    });

    adminEditObjects.chkAutoSnapsot.on('ifChanged', function () {
        adminEditObjects.txtAutoSnapshot.text(getOnOffText(adminEditObjects.chkAutoSnapsot.is(":checked")));
    });

    adminEditObjects.chkDrMaster.on('ifChanged', function () {
        adminEditObjects.txtDrMaster.text(getOnOffText(adminEditObjects.chkDrMaster.is(":checked")));
    });

    adminEditObjects.chkSnmp.on('ifChanged', function () {
        adminEditObjects.txtSnmp.text(getOnOffText(adminEditObjects.chkSnmp.is(":checked")));
        if (adminEditObjects.txtSnmp.text() == "Off") {
            $("#txtAuthkey").rules("remove");
            $("#txtPrivKey").rules("remove");
            $("#txtTarget").rules("remove");
        }
        else {
            if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() != "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() == "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }

            if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() != "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() == "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }

            $("#txtTarget").rules("add", {
                required: true,
                portRegex: /^[a-zA-Z0-9.-]+$/,
                messages: {
                    required: "This field is required",
                    portRegex: "Please enter a valid value.(e.g, hostname:(1-65535))"
                }
            })
        }
    });

    $(".tblshutdown").find(".edit").on("click", function () {
        var $this = $(this).closest("tr");

        var tdVal = $this.find("td:nth-child(3)");
        var val = tdVal.text();

        if (val == "On") {
            $this.find("td:nth-child(2)").find("div").removeClass("onIcon").addClass("offIcon");
            tdVal.text("Off");
        } else {
            $this.find("td:nth-child(2)").find("div").removeClass("offIcon").addClass("onIcon");
            tdVal.text("On");
        }
    });

    // Make Expandable Rows.
    $('tr.parent > td:first-child' || 'tr.parent > td:fourth-child')
        .css("cursor", "pointer")
        .attr("title", "Click to expand/collapse")
        .click(function () {
            var parent = $(this).parent();
            parent.siblings('.child-' + parent.attr("id")).toggle();
            parent.find(".labelCollapsed").toggleClass("labelExpanded");

            //Handle export configuration
            if ($(this).text().trim() == "Export") {
                //If parent is closed, then hide export configuration
                if (!parent.find('td:first-child > a').hasClass('labelExpanded')) {
                    adminEditObjects.exportConfiguration.hide();
                    adminEditObjects.exportConfigurationLoading.hide();
                    //If parent is open, then open the export configuration.
                } else {
                    adminEditObjects.exportConfiguration.show();
                    adminEditObjects.exportConfigurationLoading.show();
                }
            }

            //Handle import configuration
            if ($(this).text().trim() == "Import") {
                //If parent is closed, then hide export configuration
                if (!parent.find('td:first-child > a').hasClass('labelExpanded')) {
                    adminEditObjects.importConfiguration.hide();
                    //If parent is open, then open the export configuration.
                } else {
                    adminEditObjects.importConfiguration.show();
                }
            }

            //Handle import configuration
            if ($(this).text().trim() == "Advanced") {
                //If parent is closed, then hide export configuration
                if (!parent.find('td:first-child > a').hasClass('labelExpanded')) {
                    adminEditObjects.diskLimitConfiguration.hide();
                    //If parent is open, then open the export configuration.
                } else {
                    adminEditObjects.diskLimitConfiguration.show();
                }
            }
        });
    $('tr[class^=child-]').hide().children('td');

    // btnServerConfigAdmin
    $('#btnServerConfigAdmin').click(function () {
        $('#serverConfigAdmin').slideToggle("slide");
    });

    $('#serverName').click(function () {
        $('#serverConfigAdmin').slideToggle("slide");
    });

    // Implements Scroll in Server List div
    $('#serverListWrapperAdmin').slimscroll({
        disableFadeOut: true,
        height: '225px'
    });

    $('#shutDownConfirmation').popup({
        open: function (event, ui, ele) {
            var isProVersion = false;
            voltDbRenderer.GetDeploymentInformation(function (deploymentDetails) {
                if (deploymentDetails != undefined) {
                    var clusterDetails = voltDbRenderer.getClusterDetail(getCurrentServer());
                    if (clusterDetails != undefined && clusterDetails.LICENSE != undefined) {
                        licenseInfo = clusterDetails.LICENSE;
                        if (licenseInfo != undefined && licenseInfo != "") {
                            isProVersion = true;
                        }
                    }
                }
                var htmlResult = '';

                if (isProVersion) {
                    htmlResult += '<label>Save Snapshot:</label>' +
                        '&nbsp <input type="checkbox" checked="true" id="chkSaveSnaps" class="chkStream"/>';
                }

                htmlResult += '<p class="txt-bold">Are you sure you want to shutdown the cluster?</p>' +
                    '<p id="shutdownWarningMsg" style="display:none">Any data not saved to a ' +
                    'snapshot will be lost.</p>' +
                    '<p id="continueShutdownMsg" style="display:none">Continue with the shutdown?</p>';

                $('#divSaveSnapshot').html(htmlResult);

                $('#chkSaveSnaps').iCheck({
                    checkboxClass: 'icheckbox_square-aero customCheckbox',
                    increaseArea: '20%'
                });

                voltDbRenderer.GetCommandLogStatus(function (commandLogStatus) {
                    VoltDbAdminConfig.isCommandLogEnabled = commandLogStatus;
                    showHideIntSnapshotMsg(isProVersion)
                });

            });

        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnShutdownConfirmationOk").unbind("click");
            $("#btnShutdownConfirmationOk").on("click", function () {
                VoltDbAdminConfig.isSaveSnapshot = $('#chkSaveSnaps').is(":checked");
                $("#btnPrepareShutdown").trigger("click")
                $(".popup_close").hide();
                //Close the popup
                popup.close();
            });

            $("#btnShutdownConfirmationCancel").unbind("click");
            $("#btnShutdownConfirmationCancel").on("click", function () {
                popup.close();
            });
        }
    });


    var displayShutdownStatus = function (btnId, status) {
        if (status == "normal") {
            $("#" + btnId + "Normal").show()
            $("#" + btnId + "Load").hide()
            $("#" + btnId + "Ok").hide()
            $("#" + btnId + "Failure").hide()
        } else if (status == "load") {
            $("#" + btnId + "Normal").hide()
            $("#" + btnId + "Load").show()
            $("#" + btnId + "Ok").hide()
            $("#" + btnId + "Failure").hide()
        } else if (status == "failure") {
            $("#" + btnId + "Normal").hide()
            $("#" + btnId + "Load").hide()
            $("#" + btnId + "Ok").hide()
            $("#" + btnId + "Failure").show()
        } else {
            $("#" + btnId + "Normal").hide()
            $("#" + btnId + "Load").hide()
            $("#" + btnId + "Ok").show()
            $("#" + btnId + "Failure").hide()
        }
    }

    var showHideShutdownErrMsg = function (status, msg) {
        if (!status) {
            $("#shutdownErrMsg").html(msg);
            $("#divRetryBtn").show()
            $("#divShutdownErrMsg").show()
            $("#divShutdownRetryMsg").show()
        } else {
            $("#divRetryBtn").hide()
            $("#divShutdownErrMsg").hide()
            $("#divShutdownRetryMsg").hide()
        }
    }

    var serverShutdown = function (popup) {
        showHideShutdownErrMsg(true);
        displayShutdownStatus("liPrepareShutdown", "load")
        voltDbRenderer.prepareShutdownCluster(function (prepare_status) {
            console.log("Preparing for shutdown.");
            displayShutdownStatus("liPrepareShutdown", "load");
            displayShutdownStatus("liQueueExpData", "normal");
            displayShutdownStatus("liCompleteOutstandingExpDr", "normal");
            displayShutdownStatus("liCompleteClientTrans", "normal");
            displayShutdownStatus("liCompleteOutstandingImp", "normal");
            displayShutdownStatus("liShutdownReady", "normal");
            showHideShutdownErrMsg(true);
            var status = prepare_status.status;
            var zk_pause_txn_id = prepare_status.zk_pause_txn_id;
            if (status != 1) {
                console.log("The preparation for shutdown failed with status: " + status + ".");
                displayShutdownStatus("liPrepareShutdown", "failure");
                showHideShutdownErrMsg(false, "The preparation for shutdown failed with status: " + status + ".")
                $(".popup_close").show();
                return;
            } else {
                console.log('The cluster is paused prior to shutdown.')
                console.log('Writing out all queued export data.')
                displayShutdownStatus("liPrepareShutdown", "ok");
                displayShutdownStatus("liQueueExpData", "load")
                voltDbRenderer.QuiesceCluster(function (quiesceStatus) {
                    if (quiesceStatus != 0) {
                        console.log("The cluster has failed to be quiesce with status: " + quiesceStatus + ".")
                        displayShutdownStatus("liQueueExpData", "failure");
                        showHideShutdownErrMsg(false, "The cluster has failed to be quiesce with status: " + quiesceStatus + ".")
                        $(".popup_close").show();
                        return;
                    } else {
                        console.log('Completing outstanding export and DR transactions.')
                        displayShutdownStatus("liQueueExpData", "ok")
                        displayShutdownStatus("liCompleteOutstandingExpDr", "load")
                        var drDetails = {
                            "DrProducer": {
                                "partition_min": {},
                                "partition_max": {},
                                "partition_min_host": {}
                            }
                        }
                        voltDbRenderer.GetDrProducerInformation(function (drDetails) {
                            // check the export stats twice because they are periodic
                            var export_tables_with_data = {}
                            voltDbRenderer.GetDrProducerInformation(function (drDetails) {
                                var exportTableDetails = {
                                    "ExportTables": {
                                        "export_tables_with_data": {},
                                        "last_table_stat_time": 0
                                    }
                                }
                                voltDbRenderer.GetExportTablesInformation(function (exportTableDetails) {
                                    exportTableDetails["ExportTables"]["last_table_stat_time"] = exportTableDetails["ExportTables"]["collection_time"];
                                    if ((drDetails["DrProducer"].partition_min == undefined || $.isEmptyObject(drDetails["DrProducer"].partition_min)) &&
                                        exportTableDetails["ExportTables"]["last_table_stat_time"] == 1) {
                                        // there are no outstanding export or dr transactions
                                        continueShutdown(popup, zk_pause_txn_id)
                                        return
                                    }
                                    // after 10 seconds notify admin of what transactions have not drained
                                    notifyInterval = 10
                                    // have to get two samples of table stats because the cached value could be
                                    // from before Quiesce

                                    var setDrExpInterval = setInterval(function () {
                                        var result = false;
                                        curr_table_stat_time = 0;
                                        if (!$.isEmptyObject(drDetails["DrProducer"].partition_min) || drDetails["DrProducer"].partition_min != undefined) {
                                            voltDbRenderer.GetDrProducerInformation(function (drDetails) {
                                                if (exportTableDetails["ExportTables"]["last_table_stat_time"] > 1) {
                                                    voltDbRenderer.GetExportTablesInformation(function (exportTableDetails) {
                                                        curr_table_stat_time = exportTableDetails["ExportTables"]["collection_time"]
                                                        notifyInterval -= 1
                                                        result = checkExportAndDr(exportTableDetails["ExportTables"]["last_table_stat_time"],
                                                            curr_table_stat_time, exportTableDetails["ExportTables"]["export_tables_with_data"],
                                                            drDetails["DrProducer"].partition_min, drDetails["DrProducer"].partition_max,
                                                            drDetails["DrProducer"].partition_min_host, notifyInterval);
                                                        if (result) {
                                                            clearInterval(setDrExpInterval)
                                                            continueShutdown(popup, zk_pause_txn_id)
                                                            return;
                                                        }
                                                    }, exportTableDetails);
                                                } else {
                                                    notifyInterval -= 1
                                                    result = checkExportAndDr(exportTableDetails["ExportTables"]["last_table_stat_time"],
                                                        curr_table_stat_time, exportTableDetails["ExportTables"]["export_tables_with_data"],
                                                        drDetails["DrProducer"].partition_min, drDetails["DrProducer"].partition_max,
                                                        drDetails["DrProducer"].partition_min_host, notifyInterval);
                                                    if (result) {
                                                        clearInterval(setDrExpInterval)
                                                        continueShutdown(popup, zk_pause_txn_id)
                                                        return;
                                                    }
                                                }

                                            }, drDetails);
                                        } else {
                                            if (exportTableDetails["ExportTables"]["last_table_stat_time"] > 1) {
                                                voltDbRenderer.GetExportTablesInformation(function (exportTableDetails) {
                                                    curr_table_stat_time = exportTableDetails["ExportTables"]["collection_time"]
                                                    notifyInterval -= 1
                                                    result = checkExportAndDr(exportTableDetails["ExportTables"]["last_table_stat_time"],
                                                        curr_table_stat_time, exportTableDetails["ExportTables"]["export_tables_with_data"],
                                                        drDetails["DrProducer"].partition_min, drDetails["DrProducer"].partition_max,
                                                        drDetails["DrProducer"].partition_min_host, notifyInterval);
                                                    if (result) {
                                                        clearInterval(setDrExpInterval)
                                                        continueShutdown(popup, zk_pause_txn_id)
                                                        return;
                                                    }
                                                });
                                            } else {
                                                notifyInterval -= 1
                                                result = checkExportAndDr(exportTableDetails["ExportTables"]["last_table_stat_time"],
                                                    curr_table_stat_time, exportTableDetails["ExportTables"]["export_tables_with_data"],
                                                    drDetails["DrProducer"].partition_min, drDetails["DrProducer"].partition_max,
                                                    drDetails["DrProducer"].partition_min_host, notifyInterval);
                                                if (result) {
                                                    clearInterval(setDrExpInterval)
                                                    continueShutdown(popup, zk_pause_txn_id)
                                                    return;
                                                }
                                            }
                                        }
                                    }, 2000);

                                }, exportTableDetails);

                            }, drDetails);
                        }, drDetails);

                    }
                });
            }
            $("#overlay").hide();
        });
    }

    var isClientTransFinish = false;
    var isImportRequestFinish = false

    var continueShutdown = function (popup, zk_pause_txn_id) {
        displayShutdownStatus("liCompleteOutstandingExpDr", "ok")
        console.log('All export and DR transactions have been processed.')
        isClientTransFinish = false;
        check_client();
        var clientTransInterval = setInterval(function () {
            if (isClientTransFinish) {
                clearInterval(clientTransInterval);
                isImportRequestFinish = false;
                check_importer()

                var importReqInterval = setInterval(function () {
                    if (isImportRequestFinish) {
                        clearInterval(importReqInterval);
                        console.log('Cluster is ready for shutdown.')
                        displayShutdownStatus("liShutdownReady", "ok")
                        var shutdownTimeout = setTimeout(function () {
                            $('#serverShutdownPopup').click();
                            VoltDBCore.isServerConnected = false;
                            window.clearInterval(VoltDbUI.connectionTimeInterval);
                        }, 10000);
                        $(".popup_close").show();
                        if (VoltDbAdminConfig.isSaveSnapshot) {
                            voltDbRenderer.shutdownCluster(function (success) {
                                if (!success) {
                                    clearTimeout(shutdownTimeout);
                                    console.log("Unable to shutdown cluster.");
                                    displayShutdownStatus("liShutdownReady", "failure");
                                    showHideShutdownErrMsg(false, "Unable to shutdown cluster.")
                                    $(".popup_close").show();
                                }
                                $("#overlay").hide();

                            }, zk_pause_txn_id);
                        } else {
                            voltDbRenderer.shutdownCluster(function (success) {
                                if (!success) {
                                    clearTimeout(shutdownTimeout);
                                    console.log("Unable to shutdown cluster.");
                                    displayShutdownStatus("liShutdownReady", "failure");
                                    showHideShutdownErrMsg(false, "Unable to shutdown cluster.")
                                    $(".popup_close").show();
                                }
                                $("#overlay").hide();

                            });
                        }
                    }
                }, 2000)
            }
        }, 2000)
    }

    var check_client = function () {
        console.log('Completing outstanding client transactions.');
        displayShutdownStatus("liCompleteClientTrans", "load")
        voltDbRenderer.GetLiveClientsInfo(function (clientInfo) {
            trans = clientInfo['CLIENTS']['bytes'];
            bytes = clientInfo['CLIENTS']['msgs'];
            msgs = clientInfo['CLIENTS']['trans'];

            console.log('Outstanding transactions= ' + trans + ', Outstanding request bytes= ' + bytes + ', Outstanding response messages= ' + msgs)
            if (trans == 0 && bytes == 0 && msgs == 0) {
                isClientTransFinish = true
                displayShutdownStatus("liCompleteClientTrans", "ok")
                return
            } else {
                sleepTime(1000);
                check_Client();
            }
            if (isClientTransFinish)
                return
        });
    }

    var check_importer = function () {
        console.log('Completing outstanding importer requests.')
        displayShutdownStatus("liCompleteOutstandingImp", "load")
        voltDbRenderer.GetImportRequestInformation(function (outstanding) {
            console.log('Outstanding importer requests= ' + outstanding)
            if (outstanding == 0) {
                isImportRequestFinish = true
                displayShutdownStatus("liCompleteOutstandingImp", "ok")
                return;
            } else {
                sleepTime(1000);
                check_importer();
            }
            if (isImportRequestFinish)
                return
        });
    }

    var checkExportAndDr = function (last_table_stat_time, curr_table_stat_time, export_tables_with_data, partition_min, partition_max,
        partition_min_host, notifyInterval) {
        if (last_table_stat_time == 1 || curr_table_stat_time > last_table_stat_time) {
            // have a new sample from table stat cache or there are no tables
            if ($.isEmptyObject(export_tables_with_data) && $.isEmptyObject(partition_min)) {
                console.log('All export and DR transactions have been processed.')
                return true;
            }
        }

        notifyInterval -= 1
        if (notifyInterval == 0) {
            notifyInterval = 10
            if (last_table_stat_time > 1 && (!$.isEmptyObject(export_tables_with_data)))
                print_export_pending(export_tables_with_data)
            if (!$.isEmptyObject(partition_min))
                print_dr_pending(partition_min_host, partition_min, partition_max)
        }
        return false;
    }


    var print_export_pending = function (export_tables_with_data) {
        console.log('The following export tables have unacknowledged transactions:');
        var summaryLine = "    {0} needs acknowledgement on host(s) {1} for partition(s) {2}."
        $.each(export_tables_with_data, function (key, value) {
            var pidlist = []
            hostlist = Object.keys(export_tables_with_data[key])
            for (var i = 0; i < hostlist.length; i++) {
                for (var j = 0; j < export_tables_with_data[key][hostlist[i]].length; j++)
                    pidlist.push(export_tables_with_data[key][hostlist[i]][j])
            }
            var partlist = "";

            for (var j = 0; j < pidlist.length; j++) {
                if (j == 0)
                    partlist = pidlist[j].toString()
                else
                    partlist = partlist + "," + pidlist[j].toString()
            }

            console.log(summaryLine.format(table, hostlist.join(), partlist))
        });
    }

    var print_dr_pending = function (partition_min_host, partition_min, partition_max) {
        console.log('The following partitions have pending DR transactions that the consumer cluster has not processed:')
        var summaryline = "    Partition {0} needs acknowledgement for drIds {1} to {2} on hosts: {3}."
        $.each(partition_min_host, function (key, value) {
            pid = key;
            console.log(summaryline.format(pid, partition_min[pid] + 1, partition_max[pid], partition_min_host[pid].join()))
        });
    }

    var sleepTime = function (milliseconds) {
        var start = new Date().getTime();
        for (var i = 0; i < 1e7; i++) {
            if ((new Date().getTime() - start) > milliseconds) {
                break;
            }
        }
    }

    String.prototype.format = function () {
        var args = arguments;
        return this.replace(/\{\{|\}\}|\{(\d+)\}/g, function (m, n) {
            if (m == "{{") { return "{"; }
            if (m == "}}") { return "}"; }
            return args[n];
        });
    };

    $('#btnPrepareShutdown').popup({
        open: function (event, ui, ele) {
            var popup = $(this)[0];
            serverShutdown(popup)
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPrepareShutdownOk").unbind("click");
            $("#btnPrepareShutdownOk").on("click", function () {
                popup.close();
                setTimeout(function () {
                    $("#btnPrepareShutdown").trigger("click")
                }, 500)
                //$("#btnPrepareShutdown").trigger("click")
            })
            $("#btnPrepareShutdownCancel").unbind("click");
            $("#btnPrepareShutdownCancel").on("click", function () {
                popup.close();
            })
        }
    });

    $("#serverShutdownPopup").popup({
        closeDialog: function () {
            VoltDbUI.isConnectionChecked = false;
            VoltDbUI.refreshConnectionTime('5000');
            $('#shutdownInfoPopup').hide();
        }
    });

    $('#pauseConfirmation').popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPauseConfirmationOk").unbind("click");
            $("#btnPauseConfirmationOk").on("click", function () {
                $("#overlay").show();
                voltDbRenderer.GetClusterInformation(function (clusterState) {
                    if (clusterState.CLUSTERSTATE.toLowerCase() == 'paused') {
                        alert("The cluster is already in paused state.");
                        $("#pauseConfirmation").hide();
                        $("#resumeConfirmation").show();
                    } else {
                        voltDbRenderer.pauseCluster(function (success) {
                            if (success) {
                                $("#pauseConfirmation").hide();
                                $("#resumeConfirmation").show();
                            } else {
                                alert("Unable to pause cluster.");
                            }
                            $("#overlay").hide();
                        });
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnPauseConfirmationCancel").unbind("click");
            $("#btnPauseConfirmationCancel").on("click", function () {
                popup.close();
            });
        }
    });

    $('#resumeConfirmation').popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnResumeConfirmationOk").unbind("click");
            $("#btnResumeConfirmationOk").on("click", function () {
                $("#overlay").show();
                voltDbRenderer.GetClusterInformation(function (clusterState) {
                    if (clusterState.CLUSTERSTATE.toLowerCase() == 'running') {
                        alert("The cluster is already in running state.");
                        $("#resumeConfirmation").hide();
                        $("#pauseConfirmation").show();
                    } else {
                        voltDbRenderer.resumeCluster(function (success) {
                            if (success) {
                                $("#resumeConfirmation").hide();
                                $("#pauseConfirmation").show();
                            } else {
                                alert("Unable to resume the cluster.");
                            }
                            $("#overlay").hide();
                        });
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnResumeConfirmationCancel").unbind("click");
            $("#btnResumeConfirmationCancel").on("click", function () {
                popup.close();
            });
        }
    });

    var toggleSecurityEdit = function (state) {
        var userList = $("#UsersList");
        var userListEditable = $("#UsersListEditable");
        if (adminEditObjects.chkSecurityValue) {
            adminEditObjects.chkSecurity.iCheck('check');
        } else {
            adminEditObjects.chkSecurity.iCheck('uncheck');
        }

        adminEditObjects.spanSecurity.text(getOnOffText(adminEditObjects.chkSecurityValue));
        adminEditObjects.editStateSecurity = state;

        if (state == editStates.ShowLoading) {
            adminEditObjects.chkSecurity.parent().removeClass("customCheckbox");
            adminEditObjects.btnEditSecurityOk.hide();
            adminEditObjects.btnEditSecurityCancel.hide();
            adminEditObjects.LinkSecurityEdit.hide();
            adminEditObjects.iconSecurityOption.hide();
            adminEditObjects.spanSecurity.hide();
            adminEditObjects.loadingSecurity.show();
            userList.hide();
            adminEditObjects.loadingUserList.show();
        } else if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingSecurity.hide();
            adminEditObjects.iconSecurityOption.hide();
            adminEditObjects.LinkSecurityEdit.hide();
            adminEditObjects.spanSecurity.show();
            adminEditObjects.btnEditSecurityOk.show();
            adminEditObjects.btnEditSecurityCancel.show();
            adminEditObjects.chkSecurity.parent().addClass("customCheckbox");
            userList.show();
            adminEditObjects.loadingUserList.hide();
        } else {
            adminEditObjects.loadingSecurity.hide();
            adminEditObjects.spanSecurity.show();
            adminEditObjects.iconSecurityOption.show();
            adminEditObjects.LinkSecurityEdit.show();
            adminEditObjects.btnEditSecurityOk.hide();
            adminEditObjects.btnEditSecurityCancel.hide();
            adminEditObjects.chkSecurity.parent().removeClass("customCheckbox");
            userList.show();
            userListEditable.hide();
            adminEditObjects.loadingUserList.hide();
        }
    };

    adminEditObjects.LinkSecurityEdit.on("click", function () {
        if (adminEditObjects.securityStateOriginal.linkSecurityEdit == true) {
            toggleSecurityEdit(editStates.ShowOkCancel);
        }
    });

    adminEditObjects.btnEditSecurityCancel.on("click", function () {
        adminEditObjects.chkSecurityValue = adminEditObjects.securityStateOriginal.SecurityStatus;
        toggleSecurityEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditSecurityOk.on("click", function (e) {
        var passwordValidation = $('.passwordtxt');
        for (var i = 0; i < passwordValidation.length; i++) {
            $(passwordValidation[i]).rules("add", {
                required: true,
                messages: {
                    required: "This field is required",
                }
            });
        }

        var usernameValidation = $('.usernametxt');
        for (var j = 0; j < usernameValidation.length; j++) {
            $(usernameValidation[j]).rules("add", {
                required: true,
                regex: /^[a-zA-Z0-9_.]+$/,
                messages: {
                    required: "This field is required",
                    regex: 'Only alphabets, numbers, <br/> _ and . are allowed.'
                }
            });
        }

        if (!$("#frmUserList").valid()) {
            e.stopPropagation();
            e.preventDefault();
        }
    });

    adminEditObjects.btnEditSecurityOk.popup({
        open: function (event, ui, ele) {

        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnSecurityOk").unbind("click");
            $("#btnSecurityOk").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();

                if (!adminConfigurations.hasOwnProperty("security")) {
                    adminConfigurations.security = {};
                }

                //Set the new value to be saved.
                adminConfigurations.security.enabled = adminEditObjects.chkSecurity.is(':checked');

                //Call the loading image only after setting the new value to be saved.
                toggleSecurityEdit(editStates.ShowLoading);

                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {
                        adminEditObjects.chkSecurityValue = adminConfigurations.security.enabled;

                        //reload the page if security is enabled, user is asked to login upon reload action if user session no longer exist
                        if (adminConfigurations.security.enabled) {
                            logout();
                        }

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleSecurityEdit(editStates.ShowEdit);
                        });

                    } else {
                        toggleSecurityEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.securityLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        $("#updateErrorFieldMsg").text(msg);
                        $("#updateErrorPopupLink").trigger("click");
                    }
                });

                if (adminEditObjects.chkSecurity.is(':checked')) {
                    adminEditObjects.iconSecurityOption.removeClass().addClass("onIcon");
                    adminEditObjects.chkSecurityValue = true;
                } else {
                    adminEditObjects.iconSecurityOption.removeClass().addClass("offIcon");
                    adminEditObjects.chkSecurityValue = false;
                }

                //Close the popup
                popup.close();
            });

            $("#btnPopupSecurityCancel").unbind("click");
            $("#btnPopupSecurityCancel").on("click", function () {
                toggleSecurityEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                adminEditObjects.chkSecurityValue = adminEditObjects.securityStateOriginal.SecurityStatus;
                toggleSecurityEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                adminEditObjects.chkSecurityValue = adminEditObjects.securityStateOriginal.SecurityStatus;
                toggleSecurityEdit(editStates.ShowEdit);
            });
        }
    });

    var showUpdateMessage = function (msg) {
        adminClusterObjects.updateMessageBar.html(msg);
        adminClusterObjects.updateMessageBar.css('display', 'block');
        adminClusterObjects.updateMessageBar.fadeOut(4000);
    };

    $('#saveConfirmation').popup({
        open: function (event, ui, ele) {
            var textName = '<input id="txtSnapshotName" type="text" name="txtSnapshotName" value=' + 'SNAPSHOT_' + getDateTime() + '  />';
            var errorMsg = '<div class="errorLabelMsg"><label id="errorSnapshotFileName" for="txtSnapshotName" class="error" style="display: none;">This field is required.</label></div>';
            $('#tdSnapshotName').html(textName + errorMsg);
            var textDirectory = '<input id="txtSnapshotDirectory" name="txtSnapshotDirectory" type="text"/>';
            var errorDirectoryMsg = '<div class="errorLabelMsg"><label id="errorSnapshotDirectoryPath" for="txtSnapshotDirectory" class="error" style="display: none;">This field is required.</label></div>';
            $('#tdSnapshotDirectory').html(textDirectory + errorDirectoryMsg);

            $("#formSaveSnapshot").validate({
                rules: {
                    txtSnapshotName: adminValidationRules.fileNameRules,
                    txtSnapshotDirectory: adminValidationRules.directoryPathRules,
                },
                messages: {
                    txtSnapshotName: adminValidationRules.fileNameMessages,
                    txtSnapshotDirectory: adminValidationRules.directoryPathMessages,
                }
            });
        },
        afterOpen: function (event) {
            var popup = $(this)[0];
            $("#btnSaveSnapshots").unbind("click");
            $("#btnSaveSnapshots").on("click", function (e) {
                $("#formSaveSnapshot").valid();
                var errorSnapshotFileName = $("#errorSnapshotFileName");
                var errorDirectoryPath = $("#errorSnapshotDirectoryPath");
                if (errorSnapshotFileName.is(":visible") || errorDirectoryPath.is(":visible")) {
                    e.preventDefault();
                    e.stopPropagation();

                    if (errorSnapshotFileName.is(":visible")) {
                        errorSnapshotFileName.css("background-color", "yellow");
                        setTimeout(function () {
                            errorSnapshotFileName.animate({ backgroundColor: 'white' }, 'slow');
                        }, 2000);
                    }

                    if (errorDirectoryPath.is(":visible")) {
                        errorDirectoryPath.css("background-color", "yellow");
                        setTimeout(function () {
                            errorDirectoryPath.animate({ backgroundColor: 'white' }, 'slow');
                        }, 2000);
                    }
                    return;
                }

                var snapShotDirectory = $('#txtSnapshotDirectory').val();
                var snapShotFileName = $('#txtSnapshotName').val();
                voltDbRenderer.saveSnapshot(snapShotDirectory, snapShotFileName, function (success, snapshotStatus) {
                    if (success) {
                        if (snapshotStatus[getCurrentServer()].RESULT.toLowerCase() == "success") {
                            $('#saveSnapshotStatus').html('Snapshot queued successfully');
                            $('#saveSnapshotMessage').html('To verify snapshot completion, please check the server logs.');
                            $('#btnSaveSnapshotPopup').click();
                        } else {
                            $('#saveSnapshotStatus').html('Failed to save snapshot');
                            $('#saveSnapshotMessage').html(snapshotStatus[getCurrentServer()].ERR_MSG);
                            $('#btnSaveSnapshotPopup').click();
                        }
                    } else {
                        alert("Unable to save snapshot.");
                    }
                });
                //Close the popup
                popup.close();

            });

            $("#btnSaveSnapshotCancel").unbind("click");
            $("#btnSaveSnapshotCancel").on("click", function () {
                popup.close();
            });

            $("#btnSaveSnapshotCancel").unbind("click");
            $("#btnSaveSnapshotCancel").on("click", function () {
                popup.close();
            });
        }
    });

    adminClusterObjects.btnClusterPromote.on("click", function (e) {
        if (!adminClusterObjects.enablePromote) {
            e.preventDefault();
            e.stopPropagation();
        }
    });

    adminClusterObjects.btnErrorClusterPromote.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPromoteErrorOk").unbind("click");
            $("#btnPromoteErrorOk").on("click", function () {
                popup.close();
            });
        }
    });

    adminClusterObjects.btnClusterPromote.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function (event) {
            var popup = $(this)[0];
            $("#promoteConfirmOk").unbind("click");
            $("#promoteConfirmOk").on("click", function (e) {
                $("#adminActionOverlay").show();

                voltDbRenderer.promoteCluster(function (status, statusstring) {
                    if (status == 1) {
                        showUpdateMessage('Cluster promoted successfully.');
                        adminClusterObjects.enablePromote = false;
                        adminClusterObjects.ignorePromoteUpdateCount = 2;
                        adminClusterObjects.btnClusterPromote.removeClass().addClass("promote-disabled");
                    } else {
                        var msg = statusstring;

                        if (msg == null || msg == "") {
                            msg = "An error occurred while promoting the cluster.";
                        }
                        adminClusterObjects.errorPromoteMessage.html(msg);
                        adminClusterObjects.btnErrorClusterPromote.trigger("click");
                    }

                    $("#adminActionOverlay").hide();
                });

                //Close the popup 
                popup.close();
            });

            $("#promoteConfirmCancel").unbind("click");
            $("#promoteConfirmCancel").on("click", function () {
                popup.close();
            });
        }
    });

    var getDateTime = function () {
        var currentDate = new Date();
        return (currentDate.getFullYear() + '.' + (currentDate.getMonth() + 1) + '.' + currentDate.getDate() + '.' + currentDate.getHours() + '.' + currentDate.getMinutes() + '.' + currentDate.getSeconds()).toString();
    };

    $('#restoreConfirmation').popup({
        open: function (event, ui, ele) {
            $('#tblSearchList').html('');
            var textName = '<input id="txtSearchSnapshots" name="txtSearchSnapshots" type="text" value=' + $('#voltdbroot').text() + '/' + $('#snapshotpath').text() + '></td>';
            var errorMsg = '<div class="errorLabelMsgRestore"><label id="errorSearchSnapshotDirectory" for="txtSearchSnapshots" class="error" style="display: none;"></label></div>';
            $('#tdSearchSnapshots').html(textName + errorMsg);
            var btnName = '<a id="btnSearchSnapshots" class="save-search" title="Search" href="#">Search</a>';
            $('#tdSearchSnapshotsBtn').html(btnName);
            $('#btnRestore').addClass('restoreBtn');
            $('#btnRestore').removeClass('btn');
            $('.restoreConfirmation').hide();
            $('.restoreInfo').show();

            $("#formSearchSnapshot").validate({
                rules: {
                    txtSearchSnapshots: adminValidationRules.directoryPathRules,
                },
                messages: {
                    txtSearchSnapshots: adminValidationRules.directoryPathMessages,
                }
            });

            if ($('#voltdbroot').text() + '/' + $('#snapshotpath').text() != '/')
                searchSnapshots();
        },
        afterOpen: function () {
            var popup = $(this)[0];

            $('#btnSearchSnapshots').unbind('click');
            $('#btnSearchSnapshots').on('click', function (e) {
                if (!$("#formSearchSnapshot").valid()) {
                    e.preventDefault();
                    e.stopPropagation();

                    var errorSearchSnapshotDirectory = $("#errorSearchSnapshotDirectory");
                    errorSearchSnapshotDirectory.css("background-color", "yellow");
                    setTimeout(function () {
                        errorSearchSnapshotDirectory.animate({ backgroundColor: 'white' }, 'slow');
                    }, 2000);

                    return;
                }
                searchSnapshots();
            });

            //restore cluster
            $("#btnRestore").unbind("click");
            $("#btnRestore").on("click", function (e) {
                if ($('#btnRestore').hasClass('restoreBtn')) {
                    return;
                }

                if (!$("#formRestoreSnapshot").valid()) {
                    var errorRestoreSnapshot = $("#errorRestoreSnapshot");
                    errorRestoreSnapshot.css("background-color", "yellow");
                    setTimeout(function () {
                        errorRestoreSnapshot.animate({ backgroundColor: 'white' }, 'slow');
                    }, 2000);
                    return;
                }

                $('.restoreInfo').hide();
                $('.restoreConfirmation').show();
            });

            $("#btnRestoreCancel").unbind("click");
            $("#btnRestoreCancel").on("click", function () {
                popup.close();
            });

            $(".confirmNoRestore").unbind("click");
            $(".confirmNoRestore").on("click", function (e) {
                $('.restoreConfirmation').hide();
                $('.restoreInfo').show();
            });

            $("#btnRestoreSnapshotOk").unbind("click");
            $("#btnRestoreSnapshotOk").on("click", function (e) {

                var checkedValue = $('input:radio[name=vemmanual]:checked').val();

                if (checkedValue == undefined) {
                    $($(this).siblings()[0]).trigger("click");
                    $('#saveSnapshotStatus').html('Failed to restore snapshot');
                    $('#saveSnapshotMessage').html('Please select the snapshot file.');
                    $('#btnSaveSnapshotPopup').click();
                    return;
                }
                var value = checkedValue.split('#');
                $("#adminActionOverlay").show();
                voltDbRenderer.restoreSnapShot(value[0], value[1], function (status, snapshotResult, statusString) {
                    if (status) {
                        if (snapshotResult[getCurrentServer()].RESULT.toLowerCase() == "success") {
                            $('#snapshotBar').html('Snapshot restored successfully.');
                            $('#snapshotBar').css('display', 'block');
                            $('#snapshotBar').fadeOut(4000);
                        } else {
                            $('#saveSnapshotStatus').html('Failed to restore snapshot');
                            $('#saveSnapshotMessage').html(snapshotResult[getCurrentServer()].ERR_MSG);
                            $('#btnSaveSnapshotPopup').click();
                        }
                    } else {
                        $('#saveSnapshotStatus').html('Failed to restore snapshot');
                        $('#saveSnapshotMessage').html(statusString);
                        $('#btnSaveSnapshotPopup').click();
                    }
                    $("#adminActionOverlay").hide();
                });

                //Close the popup
                popup.close();
            });
        }
    });

    var searchSnapshots = function (e) {
        $('#btnRestore').removeClass('btn').addClass('restoreBtn');
        $('#tblSearchList').html('<tr style="border:none"><td colspan="3" align="center"><img src="images/loader-small.GIF"></td></tr>');
        voltDbRenderer.GetSnapshotList($('#txtSearchSnapshots').val(), function (snapshotList) {
            var result = '';
            var searchBox = '';
            searchBox += '<tr>' +
                '<th colspan="3" align="left">Snapshot Name</th>' +
                '</tr>';

            var count = 0;
            var searchError = false;
            $.each(snapshotList, function (id, snapshot) {
                if (snapshot.RESULT == "FAILURE") {
                    result += '<tr><td style="color:#c70000" colspan="3"> Error: Failure getting snapshots.' + snapshot.ERR_MSG + '</td></tr>';
                    searchError = true;
                    return false;
                } else if (snapshot.NONCE == undefined) {
                    result += '<tr><td colspan="3">No snapshot is available.' + snapshot.ERR_MSG + '</td></tr>';
                    searchError = true;
                    return false;
                }
                var option = 'checked="checked"';
                if (count != 0)
                    option = '';
                result += '<tr>' +
                    '<td colspan="2" align="left">' + snapshot.NONCE + '</td>' +
                    '<td align="left">' +
                    '<div class="restoreRadio">' +
                    '<input type="radio" value="' + snapshot.PATH + '#' + snapshot.NONCE + '" name="vemmanual" ' + option + '>' +
                    '</div>' +
                    '</td>' +
                    '</tr>';
                count++;
            });

            if (result == '') {
                result = '<td>No snapshots available.</td>';
                $('#btnRestore').addClass('restoreBtn');
                $('#btnRestore').removeClass('btn');
            } else if (searchError) {
                $('#btnRestore').removeClass('btn').addClass('restoreBtn');
            } else {
                $('#btnRestore').addClass('btn');
                $('#btnRestore').removeClass('restoreBtn');
            }

            $('#tblSearchList').html(searchBox + result);
            $("#overlay").hide();

            adminClusterObjects.errorRestoreMsgContainer.html('<label id="errorRestoreSnapshot" for="vemmanual" class="error">Please select a snapshot to restore.</label>');
            $("#formRestoreSnapshot").validate({
                rules: {
                    vemmanual: adminValidationRules.restoreSnapshotRules,
                },
                messages: {
                    vemmanual: adminValidationRules.restoreSnapshotMessages,
                }
            });

            $("#errorRestoreSnapshot").hide();
        });
    };

    var restoreInterval = null;
    var showHideRestoreBtn = function () {
        if (!$('#restoredPopup').is(":visible")) {
            if (!VoltDbAdminConfig.firstResponseReceived) {
                $('#restoreConfirmation').addClass('restoreConfirmationDisable');
                $('#restoreConfirmation').removeClass('restore');
            } else {
                $('#restoreConfirmation').addClass('restore');
                $('#restoreConfirmation').removeClass('restoreConfirmationDisable');
                clearInterval(restoreInterval);
            }
        }
    };

    $('#restoreConfirmation').on("click", function (e) {
        if ($('#restoreConfirmation').hasClass('restoreConfirmationDisable')) {
            e.preventDefault();
            e.stopPropagation();
        }
    });

    restoreInterval = setInterval(showHideRestoreBtn, 2000);
    showHideRestoreBtn();

    $('#stopConfirmation').popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function (event) {
            $("#StoptConfirmOK").unbind("click");
            $("#StoptConfirmOK").on("click", function () {
                $("#stopConfirmation").hide();
                $("#startConfirmation").show();
                //Close the popup
                $($(this).siblings()[0]).trigger("click");

            });
        }
    });


    $('#startConfirmation').popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#startConfirmOk").unbind("click");
            $("#startConfirmOk").on("click", function () {
                $("#startConfirmation").hide();
                $("#stopConfirmation").show();
                //Close the popup
                popup.close();
            });

            $("#startConfirmCancel").unbind("click");
            $("#startConfirmCancel").on("click", function () {
                popup.close();
            });
        }
    });


    var toggleAutoSnapshotEdit = function (state) {
        adminEditObjects.editStateSnapshot = state;

        if (adminEditObjects.chkAutoSnapshotValue) {
            adminEditObjects.chkAutoSnapsot.iCheck('check');
        } else {
            adminEditObjects.chkAutoSnapsot.iCheck('uncheck');
        }
        adminEditObjects.tBoxAutoSnapshotFreq.val(adminEditObjects.tBoxAutoSnapshotFreqValue);
        adminEditObjects.tBoxAutoSnapshotRetained.val(adminEditObjects.tBoxAutoSnapshotRetainedValue);
        adminEditObjects.tBoxFilePrefix.val(adminEditObjects.tBoxFilePrefixValue);
        adminEditObjects.ddlAutoSnapshotFreqUnit.val(adminEditObjects.ddlAutoSnapshotFreqUnitValue);
        adminEditObjects.txtAutoSnapshot.text(getOnOffText(adminEditObjects.chkAutoSnapshotValue));
        VoltDbAdminConfig.isSnapshotEditMode = false;
        if (state == editStates.ShowLoading) {
            adminEditObjects.chkAutoSnapsot.parent().removeClass("customCheckbox");
            adminEditObjects.iconAutoSnapshotOption.hide();
            adminDOMObjects.autoSnapshotLabel.hide();
            adminEditObjects.LinkAutoSnapshotEdit.hide();
            adminEditObjects.btnEditAutoSnapshotOk.hide();
            adminEditObjects.btnEditAutoSnapshotCancel.hide();

            adminEditObjects.spanAutoSnapshotFreqUnit.hide();
            adminEditObjects.spanAutoSnapshotFreq.hide();
            adminEditObjects.spanAutoSnapshotRetained.hide();
            adminEditObjects.spanAutoSnapshotFilePrefix.hide();

            adminEditObjects.tBoxAutoSnapshotFreq.hide();
            adminEditObjects.ddlAutoSnapshotFreqUnit.hide();
            adminEditObjects.tBoxAutoSnapshotRetained.hide();
            adminEditObjects.tBoxFilePrefix.hide();
            adminDOMObjects.retainedLabel.hide();

            adminEditObjects.errorAutoSnapshotFreq.hide();
            adminEditObjects.errorAutoSnapshotFilePrefix.hide();
            adminEditObjects.errorAutoSnapshotRetained.hide();

            adminEditObjects.loadingSnapshot.show();
            adminEditObjects.loadingSnapshotFrequency.show();
            adminEditObjects.loadingSnapshotPrefix.show();
            adminEditObjects.loadingSnapshotRetained.show();

        } else if (state == editStates.ShowOkCancel) {
            adminEditObjects.iconAutoSnapshotOption.hide();
            adminDOMObjects.autoSnapshotLabel.show();
            adminEditObjects.LinkAutoSnapshotEdit.hide();
            adminEditObjects.btnEditAutoSnapshotOk.show();
            adminEditObjects.btnEditAutoSnapshotCancel.show();
            adminEditObjects.chkAutoSnapsot.parent().addClass("customCheckbox");

            adminEditObjects.spanAutoSnapshotFreqUnit.hide();
            adminEditObjects.spanAutoSnapshotFreq.hide();
            adminEditObjects.spanAutoSnapshotRetained.hide();
            adminEditObjects.spanAutoSnapshotFilePrefix.hide();
            adminEditObjects.tBoxAutoSnapshotFreq.show();
            adminEditObjects.ddlAutoSnapshotFreqUnit.show();
            adminEditObjects.tBoxAutoSnapshotRetained.show();
            adminEditObjects.tBoxFilePrefix.show();
            adminDOMObjects.retainedLabel.show();
            adminEditObjects.loadingSnapshotFrequency.hide();
            adminEditObjects.loadingSnapshotPrefix.hide();
            adminEditObjects.loadingSnapshotRetained.hide();
            adminEditObjects.loadingSnapshot.hide();
            VoltDbAdminConfig.isSnapshotEditMode = true;

        } else {
            adminEditObjects.chkAutoSnapsot.parent().removeClass("customCheckbox");
            adminEditObjects.btnEditAutoSnapshotOk.hide();
            adminEditObjects.btnEditAutoSnapshotCancel.hide();
            adminEditObjects.errorAutoSnapshotFreq.hide();
            adminEditObjects.errorAutoSnapshotFilePrefix.hide();
            adminEditObjects.errorAutoSnapshotRetained.hide();
            adminEditObjects.LinkAutoSnapshotEdit.show();
            adminEditObjects.iconAutoSnapshotOption.show();
            adminDOMObjects.autoSnapshotLabel.show();

            adminEditObjects.tBoxAutoSnapshotFreq.hide();
            adminEditObjects.ddlAutoSnapshotFreqUnit.hide();
            adminEditObjects.tBoxAutoSnapshotRetained.hide();
            adminEditObjects.tBoxFilePrefix.hide();
            adminEditObjects.spanAutoSnapshotFreq.show();
            adminEditObjects.spanAutoSnapshotFreqUnit.show();
            adminEditObjects.spanAutoSnapshotRetained.show();
            adminEditObjects.spanAutoSnapshotFilePrefix.show();
            adminDOMObjects.retainedLabel.show();
            adminEditObjects.loadingSnapshotFrequency.hide();
            adminEditObjects.loadingSnapshotPrefix.hide();
            adminEditObjects.loadingSnapshotRetained.hide();
            adminEditObjects.loadingSnapshot.hide();
        }
    };

    var toggleSnmpEdit = function (state) {
        adminEditObjects.editStateSnmp = state;
        adminEditObjects.txtTarget.val(adminEditObjects.txtTargetValue);
        adminEditObjects.txtCommunity.val(adminEditObjects.txtCommunityValue);
        adminEditObjects.txtUsername.val(adminEditObjects.txtUsernameValue);
        adminEditObjects.ddlAuthProtocol.val(adminEditObjects.ddlAuthProtocolValue);
        adminEditObjects.ddlPrivProtocol.val(adminEditObjects.ddlPrivProtocolValue);
        adminEditObjects.txtAuthkey.val(adminEditObjects.txtAuthkeyValue);
        adminEditObjects.txtPrivKey.val(adminEditObjects.txtPrivkeyValue);

        if (state == editStates.ShowLoading) {

            adminEditObjects.iconSnmpOption.hide();
            adminEditObjects.LinkSnmpEdit.hide();
            adminEditObjects.btnEditSnmpOk.hide();
            adminEditObjects.btnEditSnmpCancel.hide();
            adminEditObjects.chkSnmp.parent().removeClass("customCheckbox");
            adminEditObjects.txtSnmp.hide();
            adminEditObjects.errorTarget.hide();
            adminEditObjects.errorUserName.hide();
            adminEditObjects.errorCommunity.hide();
            adminEditObjects.errorAuthkey.hide();
            adminEditObjects.errorPrivKey.hide();


            adminEditObjects.targetSpan.hide();
            adminEditObjects.communitySpan.hide();
            adminEditObjects.usernameSpan.hide();
            adminEditObjects.authKeySpan.hide();
            adminEditObjects.privProtocolSpan.hide();
            adminEditObjects.privKeySpan.hide();
            adminEditObjects.authProtocolSpan.hide();
            adminEditObjects.txtAuthkey.hide();
            adminEditObjects.txtPrivKey.hide();
            adminEditObjects.txtTarget.hide();
            adminEditObjects.txtCommunity.hide();
            adminEditObjects.txtUsername.hide();
            adminEditObjects.ddlAuthProtocol.hide();
            adminEditObjects.ddlPrivProtocol.hide();



            adminEditObjects.loadingSnmp.show();
            adminEditObjects.loadingTarget.show();
            adminEditObjects.loadingCommunity.show();
            adminEditObjects.loadingUsername.show();
            adminEditObjects.loadingAuthProtocol.show();
            adminEditObjects.loadingPrivProtocol.show();
            adminEditObjects.loadingAuthkey.show();
            adminEditObjects.loadingPrivKey.show()



        } else if (state == editStates.ShowOkCancel) {

            adminEditObjects.iconSnmpOption.hide();
            adminEditObjects.LinkSnmpEdit.hide();
            adminEditObjects.btnEditSnmpOk.show();
            adminEditObjects.btnEditSnmpCancel.show();
            adminEditObjects.chkSnmp.parent().addClass("customCheckbox");

            adminEditObjects.targetSpan.hide();
            adminEditObjects.communitySpan.hide();
            adminEditObjects.usernameSpan.hide();
            adminEditObjects.authKeySpan.hide();
            adminEditObjects.privProtocolSpan.hide();
            adminEditObjects.privKeySpan.hide();
            adminEditObjects.authProtocolSpan.hide();
            adminEditObjects.txtAuthkey.show();
            adminEditObjects.txtPrivKey.show();
            adminEditObjects.txtTarget.show();
            adminEditObjects.txtCommunity.show();
            adminEditObjects.txtUsername.show();
            adminEditObjects.ddlAuthProtocol.html('');
            if ($("#authProtocolSpan").text() === 'SHA'){
                adminEditObjects.ddlAuthProtocol.append("<option selected='selected'>SHA</option><option>MD5</option><option>NoAuth</option>")
            } else if ($("#authProtocolSpan").text() === 'MD5') {
                adminEditObjects.ddlAuthProtocol.append("<option>SHA</option><option selected='selected'>MD5</option><option>NoAuth</option>")
            } else {
                adminEditObjects.ddlAuthProtocol.append("<option>SHA</option><option>MD5</option><option selected='selected'>NoAuth</option>")
            }
            adminEditObjects.ddlAuthProtocol.show();
            adminEditObjects.ddlPrivProtocol.html('');
            if ($("#privProtocolSpan").text() === "AES") {
                adminEditObjects.ddlPrivProtocol.append("<option selected='selected'>AES</option><option>DES</option><option>NoPriv</option><option>3DES</option><option>AES192</option><option>AES256</option>");
            } else if ($("#privProtocolSpan").text() === "DES") {
                adminEditObjects.ddlPrivProtocol.append("<option>AES</option><option selected='selected'>DES</option><option>NoPriv</option><option>3DES</option><option>AES192</option><option>AES256</option>");
            } else if ($("#privProtocolSpan").text() === "NoPriv") {
                adminEditObjects.ddlPrivProtocol.append("<option>AES</option><option>DES</option><option selected='selected'>NoPriv</option><option>3DES</option><option>AES192</option><option>AES256</option>");
            } else if ($("#privProtocolSpan").text() === "3DES") {
                adminEditObjects.ddlPrivProtocol.append("<option>AES</option><option>DES</option><option>NoPriv</option><option selected='selected'>3DES</option><option>AES192</option><option>AES256</option>");
            } else if ($("#privProtocolSpan").text() === "AES192") {
                adminEditObjects.ddlPrivProtocol.append("<option>AES</option><option>DES</option><option>NoPriv</option><option>3DES</option><option selected='selected'>AES192</option><option>AES256</option>");
            } else {
                adminEditObjects.ddlPrivProtocol.append("<option>AES</option><option>DES</option><option>NoPriv</option><option>3DES</option><option>AES192</option><option selected='selected'>AES256</option>");
            }
            adminEditObjects.ddlPrivProtocol.show();

            adminEditObjects.loadingSnmp.hide();
            adminEditObjects.loadingTarget.hide();
            adminEditObjects.loadingCommunity.hide();
            adminEditObjects.loadingUsername.hide();
            adminEditObjects.loadingAuthProtocol.hide();
            adminEditObjects.loadingPrivProtocol.hide();
            adminEditObjects.loadingAuthkey.hide();
            adminEditObjects.loadingPrivKey.hide();

            if (adminEditObjects.txtCommunity.val() == "" || adminEditObjects.txtCommunity.val() == null) {
                adminEditObjects.txtCommunity.val("public");
            }

            if (adminEditObjects.txtAuthkey.val() == "" || adminEditObjects.txtAuthkey.val() == null) {
                adminEditObjects.txtAuthkey.val("voltdbauthkey")
            }

            if (adminEditObjects.txtPrivKey.val() == "" || adminEditObjects.txtPrivKey.val() == null) {
                adminEditObjects.txtPrivKey.val("voltdbprivacykey")
            }

            VoltDbAdminConfig.isSnmpEditMode = true;

        } else {
            adminEditObjects.iconSnmpOption.show();
            adminEditObjects.LinkSnmpEdit.show();
            adminEditObjects.btnEditSnmpOk.hide();
            adminEditObjects.btnEditSnmpCancel.hide();
            adminEditObjects.chkSnmp.parent().removeClass("customCheckbox");

            adminEditObjects.errorTarget.hide();
            adminEditObjects.errorUserName.hide();
            adminEditObjects.errorCommunity.hide();
            adminEditObjects.errorAuthkey.hide();
            adminEditObjects.errorPrivKey.hide();

            adminEditObjects.txtSnmp.show();
            adminEditObjects.targetSpan.show();
            adminEditObjects.communitySpan.show();
            adminEditObjects.usernameSpan.show();
            adminEditObjects.authKeySpan.show();
            adminEditObjects.privProtocolSpan.show();
            adminEditObjects.privKeySpan.show();
            adminEditObjects.authProtocolSpan.show();
            adminEditObjects.txtTarget.hide();
            adminEditObjects.txtCommunity.hide();
            adminEditObjects.txtUsername.hide();
            adminEditObjects.ddlAuthProtocol.hide();
            adminEditObjects.ddlPrivProtocol.hide();
            adminEditObjects.txtAuthkey.hide();
            adminEditObjects.txtPrivKey.hide();

            adminEditObjects.loadingSnmp.hide();
            adminEditObjects.loadingTarget.hide();
            adminEditObjects.loadingCommunity.hide();
            adminEditObjects.loadingUsername.hide();
            adminEditObjects.loadingAuthProtocol.hide();
            adminEditObjects.loadingPrivProtocol.hide();
            adminEditObjects.loadingAuthkey.hide();
            adminEditObjects.loadingPrivKey.hide();

        }

        if (adminEditObjects.chkSnmpValue) {
            adminEditObjects.chkSnmp.iCheck('check');
        } else {
            adminEditObjects.chkSnmp.iCheck('uncheck');
        }
    }

    adminEditObjects.btnEditAutoSnapshotCancel.on("click", function () {
        toggleAutoSnapshotEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditAutoSnapshotOk.on("click", function (e) {
        if (!$("#frmSnapshotFrequency").valid()) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxAutoSnapshotFreq.focus();

            adminEditObjects.errorAutoSnapshotFreq.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorAutoSnapshotFreq.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
        if (!$("#frmSnapshotFilePrefix").valid()) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxFilePrefix.focus();

            adminEditObjects.errorAutoSnapshotFilePrefix.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorAutoSnapshotFilePrefix.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
        if (!$("#frmSnapshotRetained").valid()) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxAutoSnapshotRetained.focus();

            adminEditObjects.errorAutoSnapshotRetained.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorAutoSnapshotRetained.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }

    });


    adminEditObjects.btnEditSnmpCancel.on("click", function () {
        toggleSnmpEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditSnmpOk.on("click", function (e) {
        if (adminEditObjects.txtSnmp.text() == "On") {
            if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() != "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() == "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }

            if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() != "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() == "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }


            if (!$("#frmAuthkey").valid()) {
                e.preventDefault();
                e.stopPropagation();

                adminEditObjects.txtAuthkey.focus();

                adminEditObjects.errorAuthkey.css("background-color", "yellow");
                setTimeout(function () {
                    adminEditObjects.errorAuthkey.animate({ backgroundColor: 'white' }, 'slow');
                }, 2000);
            }


            if (!$("#frmPrivKey").valid()) {
                e.preventDefault();
                e.stopPropagation();

                adminEditObjects.txtPrivKey.focus();

                adminEditObjects.errorPrivKey.css("background-color", "yellow");
                setTimeout(function () {
                    adminEditObjects.errorPrivKey.animate({ backgroundColor: 'white' }, 'slow');
                }, 2000);
            }
        }
        else {

            $("#txtAuthkey").rules("remove");
            $("#txtPrivKey").rules("remove");
            $("#txtTarget").rules("remove");

        }


        if (!$("#frmTarget").valid()) {
            e.preventDefault();
            e.stopPropagation();

            adminEditObjects.txtTarget.focus();

            adminEditObjects.errorTarget.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorTarget.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
    });


    adminEditObjects.ddlAuthProtocol.on("change", function (e) {
        if (adminEditObjects.txtSnmp.text() == "Off") {
            $("#txtAuthkey").rules("remove");
            $("#txtPrivKey").rules("remove");
            $("#txtTarget").rules("remove");
        }
        else {

            if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() != "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() == "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }
            else {
                $("#txtAuthkey").rules("remove");
            }


            if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() != "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }
            else if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() == "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }
            else {
                $("#txtPrivKey").rules("remove");
            }

            $("#txtTarget").rules("add", {
                required: true,
                portRegex: /^[a-zA-Z0-9.-]+$/,
                messages: {
                    required: "This field is required",
                    portRegex: "Please enter a valid value.(e.g, hostname:(1-65535))"
                }
            })
        }
    })

    adminEditObjects.ddlPrivProtocol.on("change", function (e) {
        if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() == "nopriv") {
            $("#txtPrivKey").rules("remove");
        }
    })

    adminEditObjects.txtUsername.on("change", function (e) {
        $("#txtAuthkey").rules("remove");
        $("#txtPrivKey").rules("remove");
        if (adminEditObjects.txtSnmp.text() == "On") {
            if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() != "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }

            else if (adminEditObjects.ddlAuthProtocol.val().toLowerCase() != "noauth" && adminEditObjects.txtUsername.val() == "") {
                $("#txtAuthkey").rules("add", {
                    required: true,
                    messages: {
                        required: "This field is required",
                    }
                })
            }

            if (adminEditObjects.ddlPrivProtocol.val().toLowerCase() != "nopriv" && adminEditObjects.txtUsername.val() != "") {
                $("#txtPrivKey").rules("add", {
                    required: true,
                    minlength: 8,
                    messages: {
                        required: "This field is required",
                        minlength: "Please enter at least 8 characters.",
                    }
                })
            }

        }
    })


    $("#frmSnapshotFrequency").validate({
        rules: {
            txtFrequency: adminValidationRules.numericRules
        },
        messages: {
            txtFrequency: adminValidationRules.numericMessages
        }
    });

    $("#frmSnapshotFilePrefix").validate({
        rules: {
            txtPrefix: adminValidationRules.fileNameRules
        },
        messages: {
            txtPrefix: adminValidationRules.fileNameMessages
        }
    });

    $("#frmSnapshotRetained").validate({
        rules: {
            txtRetained: adminValidationRules.numericRules
        },
        messages: {
            txtRetained: adminValidationRules.numericMessages
        }
    });

    $("#formQueryTimeout").validate({
        rules: {
            txtQueryTimeout: adminValidationRules.queryTimeoutRules
        },
        messages: {
            txtQueryTimeout: adminValidationRules.queryTimeoutMessages
        }
    });

    $("#formMemoryLimit").validate({
        rules: {
            txtMemoryLimitSize: adminValidationRules.memoryLimitRules
        },
        messages: {
            txtMemoryLimitSize: adminValidationRules.memoryLimitMessages
        }
    });

    $("#formAddDiskLimit").validate();

    $("#frmAuthkey").validate()

    $("#frmPrivKey").validate()

    $("#frmTarget").validate({
        rules: {
            txtTarget: adminValidationRules.targetRules
        },
        messages: {
            txtTarget: adminValidationRules.targetMessages
        }
    })

    adminEditObjects.btnEditAutoSnapshotOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnSaveSnapshot").unbind("click");
            $("#btnSaveSnapshot").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (!adminConfigurations.hasOwnProperty("snapshot")) {
                    adminConfigurations.snapshot = {};
                }
                //Set the new value to be saved.
                var frequencyUnit = "s";
                if (adminEditObjects.ddlAutoSnapshotFreqUnit.val().toLowerCase() == "min")
                    frequencyUnit = "m";
                else if (adminEditObjects.ddlAutoSnapshotFreqUnit.val().toLowerCase() == "sec")
                    frequencyUnit = "s";
                else if (adminEditObjects.ddlAutoSnapshotFreqUnit.val().toLowerCase() == "hrs")
                    frequencyUnit = "h";
                adminConfigurations.snapshot.frequency = adminEditObjects.tBoxAutoSnapshotFreq.val() + frequencyUnit;
                adminConfigurations.snapshot.prefix = adminEditObjects.tBoxFilePrefix.val();
                adminConfigurations.snapshot.retain = adminEditObjects.tBoxAutoSnapshotRetained.val();
                adminConfigurations.snapshot.enabled = adminEditObjects.chkAutoSnapsot.is(':checked');
                //Call the loading image only after setting the new value to be saved.
                toggleAutoSnapshotEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {
                        adminEditObjects.tBoxAutoSnapshotFreqValue = adminEditObjects.tBoxAutoSnapshotFreq.val();
                        adminEditObjects.ddlAutoSnapshotFreqUnitValue = adminEditObjects.ddlAutoSnapshotFreqUnit.val();
                        adminEditObjects.tBoxAutoSnapshotRetainedValue = adminEditObjects.tBoxAutoSnapshotRetained.val();
                        adminEditObjects.tBoxFilePrefixValue = adminEditObjects.tBoxFilePrefix.val();

                        adminEditObjects.spanAutoSnapshotFreq.html(adminEditObjects.tBoxAutoSnapshotFreqValue);
                        adminEditObjects.spanAutoSnapshotFreqUnit.html(adminEditObjects.ddlAutoSnapshotFreqUnitValue);
                        adminEditObjects.spanAutoSnapshotRetained.html(adminEditObjects.tBoxAutoSnapshotRetainedValue);
                        adminEditObjects.spanAutoSnapshotFilePrefix.html(adminEditObjects.tBoxFilePrefixValue);

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleAutoSnapshotEdit(editStates.ShowEdit);
                        });
                    } else {
                        toggleAutoSnapshotEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.snapshotLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.updateSnapshotErrorFieldMsg.text(msg);
                        $("#updateErrorSnapshotPopupLink").trigger("click");
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnPopupAutoSnapshotCancel").on("click", function () {
                toggleAutoSnapshotEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleAutoSnapshotEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleAutoSnapshotEdit(editStates.ShowEdit);
            });
        }
    });

    adminEditObjects.btnEditSnmpOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnSaveSnmp").unbind("click");
            $("#btnSaveSnmp").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();

                adminConfigurations.snmp = {};

                //Set the new value to be saved.

                adminConfigurations.snmp.username = adminEditObjects.txtUsername.val();
                adminConfigurations.snmp.enabled = adminEditObjects.chkSnmp.is(':checked');
                adminConfigurations.snmp.community = adminEditObjects.txtCommunity.val();
                adminConfigurations.snmp.authprotocol = adminEditObjects.ddlAuthProtocol.val();
                adminConfigurations.snmp.target = adminEditObjects.txtTarget.val();
                adminConfigurations.snmp.authkey = adminEditObjects.txtAuthkey.val();
                adminConfigurations.snmp.privacyprotocol = adminEditObjects.ddlPrivProtocol.val();
                adminConfigurations.snmp.privacykey = adminEditObjects.txtPrivKey.val();
                //Call the loading image only after setting the new value to be saved.

                toggleSnmpEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {

                        adminEditObjects.txtTargetValue = adminEditObjects.txtTarget.val();
                        adminEditObjects.txtCommunityValue = adminEditObjects.txtCommunity.val();
                        adminEditObjects.txtUsernameValue = adminEditObjects.txtUsername.val();
                        adminEditObjects.ddlPrivProtocolValue = adminEditObjects.ddlPrivProtocol.val();
                        adminEditObjects.ddlAuthProtocolValue = adminEditObjects.ddlAuthProtocol.val();
                        adminEditObjects.txtAuthkeyValue = adminEditObjects.txtAuthkey.val();
                        adminEditObjects.txtPrivkeyValue = adminEditObjects.txtPrivKey.val();

                        adminEditObjects.targetSpan.html(adminEditObjects.targetSpanValue)
                        adminEditObjects.usernameSpan.html(adminEditObjects.usernameSpanValue);
                        adminEditObjects.communitySpan.html(adminEditObjects.communitySpanValue);
                        adminEditObjects.authProtocolSpan.html(adminEditObjects.authProtocolSpanValue);
                        adminEditObjects.privProtocolSpan.html(adminEditObjects.privProtocolSpanValue);
                        adminEditObjects.authKeySpan.html(adminEditObjects.authKeySpanValue);
                        adminEditObjects.privKeySpan.html(adminEditObjects.privKeySpanValue)

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleSnmpEdit(editStates.ShowEdit);
                        });
                    } else {
                        toggleSnmpEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.snmpLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.updateSnmpErrorFieldMsg.text(msg);
                        $("#updateErrorSnmpPopupLink").trigger("click");
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnPopupSnmpCancel").on("click", function () {
                toggleSnmpEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleSnmpEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleSnmpEdit(editStates.ShowEdit);
            });
        }
    });

    adminEditObjects.LinkAutoSnapshotEdit.click(function () {
        var parent = $(this).parent().parent();
        parent.siblings('.child-' + parent.attr("id")).show();
        parent.find(".labelCollapsed").addClass("labelExpanded");
        toggleAutoSnapshotEdit(editStates.ShowOkCancel);
    });


    adminEditObjects.LinkSnmpEdit.click(function () {
        var parent = $(this).parent().parent();
        parent.siblings('.child-' + parent.attr("id")).show();
        parent.find(".labelCollapsed").addClass("labelExpanded");
        toggleSnmpEdit(editStates.ShowOkCancel);
    })

    $("#formHeartbeatTimeout").validate({
        rules: {
            txtHrtTimeOut: adminValidationRules.numericRules
        },
        messages: {
            txtHrtTimeOut: adminValidationRules.numericMessages
        }
    });

    //Heartbeat timeout
    var toggleHeartbeatTimeoutEdit = function (state) {

        adminEditObjects.editStateHeartbeatTimeout = state;
        adminEditObjects.tBoxHeartbeatTimeout.val(adminEditObjects.tBoxHeartbeatTimeoutValue);

        if (state == editStates.ShowLoading) {
            adminDOMObjects.heartBeatTimeoutLabel.hide();
            adminEditObjects.LinkHeartbeatEdit.hide();
            adminEditObjects.spanHeartbeatTimeOut.hide();
            adminEditObjects.tBoxHeartbeatTimeout.hide();
            adminEditObjects.btnEditHeartbeatTimeoutOk.hide();
            adminEditObjects.btnEditHeartbeatTimeoutCancel.hide();
            adminEditObjects.errorHeartbeatTimeout.hide();

            adminEditObjects.loadingHeartbeatTimeout.show();

        } else if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingHeartbeatTimeout.hide();
            adminEditObjects.LinkHeartbeatEdit.hide();
            adminEditObjects.btnEditHeartbeatTimeoutOk.show();
            adminEditObjects.btnEditHeartbeatTimeoutCancel.show();

            adminEditObjects.spanHeartbeatTimeOut.hide();
            adminEditObjects.tBoxHeartbeatTimeout.show();
            adminDOMObjects.heartBeatTimeoutLabel.show();
        } else {
            adminEditObjects.loadingHeartbeatTimeout.hide();
            adminEditObjects.btnEditHeartbeatTimeoutOk.hide();
            adminEditObjects.btnEditHeartbeatTimeoutCancel.hide();
            adminEditObjects.LinkHeartbeatEdit.show();
            adminEditObjects.errorHeartbeatTimeout.hide();

            adminEditObjects.tBoxHeartbeatTimeout.hide();
            adminEditObjects.spanHeartbeatTimeOut.show();
            adminDOMObjects.heartBeatTimeoutLabel.show();
        }

    };

    adminEditObjects.LinkHeartbeatEdit.on("click", function () {
        toggleHeartbeatTimeoutEdit(editStates.ShowOkCancel);
        $("td.heartbeattd span").toggleClass("unit");
    });

    adminEditObjects.btnEditHeartbeatTimeoutCancel.on("click", function () {
        toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditHeartbeatTimeoutOk.on("click", function (e) {
        $("#formHeartbeatTimeout").valid();

        if (adminEditObjects.errorHeartbeatTimeout.is(":visible")) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxHeartbeatTimeout.focus();

            adminEditObjects.errorHeartbeatTimeout.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorHeartbeatTimeout.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
    });

    adminEditObjects.btnEditHeartbeatTimeoutOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPopupHeartbeatTimeoutOk").unbind("click");
            $("#btnPopupHeartbeatTimeoutOk").on("click", function () {

                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (!adminConfigurations.hasOwnProperty("heartbeat")) {
                    adminConfigurations.heartbeat = {};
                }
                //Set the new value to be saved.
                adminConfigurations.heartbeat.timeout = adminEditObjects.tBoxHeartbeatTimeout.val();
                //Call the loading image only after setting the new value to be saved.
                toggleHeartbeatTimeoutEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {

                    if (result.status == "1") {
                        adminEditObjects.tBoxHeartbeatTimeoutValue = adminEditObjects.tBoxHeartbeatTimeout.val();
                        adminEditObjects.spanHeartbeatTimeOut.html(adminEditObjects.tBoxHeartbeatTimeoutValue);

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
                        });
                    } else {
                        toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.heartbeatTimeoutLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        $("#updateErrorFieldMsg").text(msg);
                        $("#updateErrorPopupLink").trigger("click");
                    }
                });

                //Close the popup 
                popup.close();
            });

            $("#btnPopupHeartbeatTimeoutCancel").on("click", function () {
                toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
            });
        }
    });

    $("#formQueryTimeout").validate({
        rules: {
            txtQueryTimeout: adminValidationRules.numericRules
        },
        messages: {
            txtQueryTimeout: adminValidationRules.numericMessages
        }
    });

    //Query timeout
    var toggleQueryTimeoutEdit = function (state) {

        adminEditObjects.tBoxQueryTimeout.val(adminEditObjects.tBoxQueryTimeoutValue);

        if (state == editStates.ShowLoading) {
            adminDOMObjects.queryTimeoutLabel.hide();
            adminEditObjects.LinkQueryTimeoutEdit.hide();
            adminEditObjects.btnEditQueryTimeoutOk.hide();
            adminEditObjects.btnEditQueryTimeoutCancel.hide();
            adminEditObjects.tBoxQueryTimeout.hide();
            adminEditObjects.errorQueryTimeout.hide();

            adminEditObjects.loadingQueryTimeout.show();

        } else if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingQueryTimeout.hide();
            adminEditObjects.LinkQueryTimeoutEdit.hide();
            adminEditObjects.btnEditQueryTimeoutOk.show();
            adminEditObjects.btnEditQueryTimeoutCancel.show();

            adminEditObjects.spanqueryTimeOut.hide();
            adminEditObjects.tBoxQueryTimeout.show();
            adminDOMObjects.queryTimeoutLabel.show();

        } else {
            adminEditObjects.loadingQueryTimeout.hide();
            adminEditObjects.btnEditQueryTimeoutOk.hide();
            adminEditObjects.btnEditQueryTimeoutCancel.hide();
            adminEditObjects.errorQueryTimeout.hide();
            adminEditObjects.LinkQueryTimeoutEdit.show();

            adminEditObjects.tBoxQueryTimeout.hide();
            adminEditObjects.spanqueryTimeOut.show();
            adminDOMObjects.queryTimeoutLabel.show();
        }
    };

    //Memory Limit
    var toggleMemorySizeEdit = function (state) {
        adminEditObjects.txtMemoryLimitSize.val(adminEditObjects.spanMemoryLimitSizeValue);
        if (adminEditObjects.spanMemoryLimitSizeUnit.text() != "")
            adminEditObjects.ddlMemoryLimitSizeUnit.val(adminEditObjects.spanMemoryLimitSizeUnit.text());
        else {
            adminEditObjects.ddlMemoryLimitSizeUnit.val("GB");
        }
        setMemoryLimitValidation();
        VoltDbAdminConfig.isMemoryLimitEditMode = false;
        if (state == editStates.ShowLoading) {
            adminEditObjects.spanMemoryLimitSizeUnit.hide();
            adminEditObjects.ddlMemoryLimitSizeUnit.hide();
            adminEditObjects.btnEditMemorySize.hide();
            adminEditObjects.btnEditMemorySizeOk.hide();
            adminEditObjects.btnEditMemorySizeCancel.hide();
            adminEditObjects.txtMemoryLimitSize.hide();
            adminEditObjects.spanMemoryLimitSize.hide();
            adminEditObjects.errorMemorySize.hide();

            adminEditObjects.loadingMemoryLimit.show();

        } else if (state == editStates.ShowOkCancel) {
            VoltDbAdminConfig.isMemoryLimitEditMode = true;
            adminEditObjects.loadingMemoryLimit.hide();
            adminEditObjects.btnEditMemorySize.hide();
            adminEditObjects.btnEditMemorySizeOk.show();
            adminEditObjects.btnEditMemorySizeCancel.show();

            adminEditObjects.spanMemoryLimitSize.hide();
            adminEditObjects.spanMemoryLimitSizeUnit.hide();
            adminEditObjects.txtMemoryLimitSize.show();
            adminEditObjects.ddlMemoryLimitSizeUnit.show();

        } else {
            adminEditObjects.loadingMemoryLimit.hide();
            adminEditObjects.btnEditMemorySize.show();
            adminEditObjects.btnEditMemorySizeOk.hide();
            adminEditObjects.btnEditMemorySizeCancel.hide();
            adminEditObjects.errorMemorySize.hide();

            adminEditObjects.txtMemoryLimitSize.hide();
            adminEditObjects.spanMemoryLimitSize.show();
            if (adminEditObjects.spanMemoryLimitSize.text() != "Not Enforced") {
                adminEditObjects.btnDeleteMemory.show();
            }
            adminEditObjects.ddlMemoryLimitSizeUnit.hide();
            adminEditObjects.spanMemoryLimitSizeUnit.show();
        }
    };

    var setMemoryLimitValidation = function () {
        $("#errorMemorySize").val("");
        $("#errorMemorySize").hide();
        $("#txtMemoryLimitSize").rules("remove");
        var unit = $('#ddlMemoryLimitUnit').val();
        if (unit == "%") {
            $("#txtMemoryLimitSize").rules("add", {
                required: true,
                min: 1,
                max: 99,
                digits: true,
                messages: {
                    required: "This field is required",
                    min: "Please enter a positive number.",
                    max: "Maximum value of percentage cannot be greater than 99.",
                    digits: "Please enter a positive number without any decimal."
                }
            });
        } else if (unit == "GB") {
            $("#txtMemoryLimitSize").rules("add", {
                required: true,
                min: 1,
                max: 2147483647,
                digits: true,
                messages: {
                    required: "This field is required",
                    min: "Please enter a positive number.",
                    max: "Maximum value of GB cannot be greater than 2147483647.",
                    digits: "Please enter a positive number without any decimal."
                }
            });
        }
    };


    adminEditObjects.LinkQueryTimeoutEdit.on("click", function () {
        toggleQueryTimeoutEdit(editStates.ShowOkCancel);
        $("td.queryTimeOut span").toggleClass("unit");
    });

    adminEditObjects.btnEditQueryTimeoutCancel.on("click", function () {
        toggleQueryTimeoutEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditQueryTimeoutOk.on("click", function (e) {
        if (!$("#formQueryTimeout").valid()) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxQueryTimeout.focus();

            adminEditObjects.errorQueryTimeout.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorQueryTimeout.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
    });

    adminEditObjects.btnEditQueryTimeoutOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPopupQueryTimeoutOk").unbind("click");
            $("#btnPopupQueryTimeoutOk").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (adminConfigurations.systemsettings.query == null) {
                    adminConfigurations.systemsettings.query = {};
                }
                //Set the new value to be saved.
                adminConfigurations.systemsettings.query.timeout = adminEditObjects.tBoxQueryTimeout.val();

                //Call the loading image only after setting the new value to be saved.
                toggleQueryTimeoutEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {
                        adminEditObjects.tBoxQueryTimeoutValue = adminEditObjects.tBoxQueryTimeout.val();
                        adminEditObjects.spanqueryTimeOut.html(adminEditObjects.tBoxQueryTimeoutValue);

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleQueryTimeoutEdit(editStates.ShowEdit);
                        });

                    } else {
                        toggleQueryTimeoutEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.queryTimeoutFieldLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.queryTimeoutUpdateErrorFieldMsg.text(msg);
                        $("#queryTimeoutUpdateErrorPopupLink").trigger("click");
                    }
                });

                //Close the popup
                popup.close();
            });

            $("#btnPopupQueryTimeoutCancel").on("click", function () {
                toggleQueryTimeoutEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleQueryTimeoutEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleQueryTimeoutEdit(editStates.ShowEdit);
            });
        }
    });

    //Memory Limit 
    $('#ddlMemoryLimitUnit').change(function () {
        setMemoryLimitValidation();
    });



    adminEditObjects.btnEditMemorySize.on("click", function () {
        toggleMemorySizeEdit(editStates.ShowOkCancel);
        adminEditObjects.btnDeleteMemory.hide();
        $("td.memorySize span").toggleClass("unit");
    });

    adminEditObjects.btnEditMemorySizeOk.on("click", function (e) {
        if (!$("#formMemoryLimit").valid()) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.txtMemoryLimitSize.focus();

            adminEditObjects.errorMemorySize.css("background-color", "yellow");
            setTimeout(function () {
                adminEditObjects.errorMemorySize.animate({ backgroundColor: 'white' }, 'slow');
            }, 2000);
        }
    });


    adminEditObjects.btnEditMemorySizeCancel.on("click", function () {
        toggleMemorySizeEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditMemorySizeOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnPopupMemoryLimitOk").unbind("click");
            $("#btnPopupMemoryLimitOk").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (adminConfigurations.systemsettings.resourcemonitor == null) {
                    adminConfigurations.systemsettings.resourcemonitor = {};
                }
                if (adminConfigurations.systemsettings.resourcemonitor.memorylimit == null) {
                    adminConfigurations.systemsettings.resourcemonitor.memorylimit = {};
                }
                //Set the new value to be saved.
                var memoryLimitSize = "";
                memoryLimitSize = adminEditObjects.txtMemoryLimitSize.val() + (adminEditObjects.ddlMemoryLimitSizeUnit.val() == "%" ? "%" : "");
                console.log(memoryLimitSize);
                if (adminEditObjects.txtMemoryLimitSize.val() != "") {
                    adminConfigurations.systemsettings.resourcemonitor.memorylimit.size = memoryLimitSize;
                } else {
                    adminConfigurations.systemsettings.resourcemonitor.memorylimit = null;
                }
                //Call the loading image only after setting the new value to be saved.
                toggleMemorySizeEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            adminEditObjects.btnDeleteMemory.show();
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleMemorySizeEdit(editStates.ShowEdit);
                        });

                    } else {
                        toggleMemorySizeEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.memoryLimitErrorFieldMsg + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.memoryLimitUpdateErrorFieldMsg.text(msg);
                        $("#memorySizeUpdateErrorPopupLink").trigger("click");
                    }
                });

                //Close the popup
                popup.close();
            });

            $("#btnPopupMemoryLimitCancel").on("click", function () {
                toggleMemorySizeEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleMemorySizeEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleMemorySizeEdit(editStates.ShowEdit);
            });
        }
    });
    //


    adminEditObjects.btnDeleteMemory.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnDelPopupMemoryLimitOk").on("click", function () {
                adminEditObjects.btnDeleteMemory.hide();
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (adminConfigurations.systemsettings.resourcemonitor == null) {
                    adminConfigurations.systemsettings.resourcemonitor = {};
                }
                if (adminConfigurations.systemsettings.resourcemonitor.memorylimit == null) {
                    adminConfigurations.systemsettings.resourcemonitor.memorylimit = {};
                }

                //Set the memory limit value to null
                adminConfigurations.systemsettings.resourcemonitor.memorylimit = null;

                //Call the loading image only after setting the new value to be saved.
                toggleMemorySizeEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleMemorySizeEdit(editStates.ShowEdit);
                        });

                    } else {
                        toggleMemorySizeEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.memoryLimitErrorFieldMsg + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.memoryLimitUpdateErrorFieldMsg.text(msg);
                        $("#memorySizeUpdateErrorPopupLink").trigger("click");
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnDelPopupMemoryLimitCancel").on("click", function () {
                popup.close();
            });

        }
    });


    $("#addNewConfigLink").on("click", function () {
        adminDOMObjects.addConfigLink.data("id", -1);
        adminDOMObjects.addConfigLink.trigger("click");
    });

    $("#addNewImportConfigLink").on("click", function () {
        adminDOMObjects.addImportConfigPopupLink.data("id", -1);
        adminDOMObjects.addImportConfigPopupLink.trigger("click");
    });

    $("#lstDrTbl").on("click", function () {
        adminDOMObjects.lstReplicatedTables.trigger("click");
    });

    $("#lstExportTbl").on("click", function () {
        adminDOMObjects.lstExportTable.trigger("click");
    });

    $("#lstDrTblLink").popup({
        open: function (event, ui, ele) {
            var content = '';
            if (voltDbRenderer.drTablesArray.length == 0) {
                $("#drPopup").html("<span style='font-size: 14px'>No DR tables available.</span>");
            } else {
                content = "<table width='100%' border='0' cellspacing='0' cellpadding='0' class='tblPopup'><tbody id='drTableBody'>";
                for (var i = 0; i <= voltDbRenderer.drTablesArray.length - 1; i++) {
                    content = content + "<tr><td>" + voltDbRenderer.drTablesArray[i] + "</td></tr>";
                }
                content = content + "</tbody></table>";
                $("#drPopup").html(content);
            }

        },
        afterOpen: function () {
            var popup = $(this)[0];
            $(".closeBtn").on("click", function () {
                //Close the popup
                popup.close();
            });
        }
    });

    $("#lstExportTblLink").popup({
        open: function (event, ui, ele) {
            var content = '';
            if (voltDbRenderer.exportTablesArray.length == 0) {
                $("#exportPopup").html("<span style='font-size: 14px'>No export streams available.</span>");
            } else {
                content = "<table width='100%' border='0' cellspacing='0' cellpadding='0' class='tblPopup'><tbody id='exportTableBody'>";
                for (var i = 0; i <= voltDbRenderer.exportTablesArray.length - 1; i++) {
                    content = content + "<tr><td>" + voltDbRenderer.exportTablesArray[i] + "</td></tr>";
                }
                content = content + "</tbody></table>";
                $("#exportPopup").html(content);
            }

        },
        afterOpen: function () {
            var popup = $(this)[0];
            $(".closeBtn").on("click", function () {
                //Close the popup
                popup.close();
            });
        }
    });
    var editId = -1;

    function showHideConnectorClass() {
        if ($('#txtType').val() == "CUSTOM") {
            $("#TrExportConnectorClass").show();
        } else {
            $("#TrExportConnectorClass").hide();
        }
    };

    $("#addConfigPopupLink").popup({
        open: function (event, ui, ele) {
            editId = adminDOMObjects.addConfigLink.data("id");

            //For adding a new configuration
            if (editId == "-1") {
                $("#addConfigHeader").text("Add Configuration");
                $("#deleteAddConfig").hide();
            }//For editing an existing configuration
            else {
                $("#addConfigHeader").text("Edit Configuration");
                $("#deleteAddConfig").show();
            }

            var exporttypes = VoltDbAdminConfig.exportTypes;

            $("#expotSaveConfigText").text("save").data("status", "save");
            var contents = '' +
                '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tr id="Tr1">' +
                '    <td>Target</td>' +
                '    <td width="15%">' +
                '       <input id="txtStream" name="txtStream" type="text" size="38">' +
                '       <label id="errorStream" for="txtStream" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td width="8%" align="right"><input type="checkbox" checked="true" id="chkStream" class="chkStream"/></td>' +
                '    <td id="chkStreamValue">On</td>' +
                '</tr>' +
                '<tr>' +
                '    <td>Type </td>' +
                '    <td>' +
                '       <select id="txtType" name="txtType"> ';

            var customIndex = $.inArray('CUSTOM', exporttypes.type);

            exporttypes.type.splice(customIndex, 1);

            exporttypes.type.push("CUSTOM");

            for (var i = 0; i <= exporttypes.type.length - 1; i++) {
                contents = contents +
                    "<option>" + exporttypes.type[i] + "</option> ";
            }
            contents = contents +
                '       </select>' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '<tr id="TrExportConnectorClass" style="display:none">' +
                '    <td>Custom connector class</td>' +
                '    <td width="15%" id="TdExportConnectorClass">' +
                '       <input id="txtExportConnectorClass" name="txtExportConnectorClass" type="text" size="38">' +
                '       <label id="errorExportConnectorClass" for="txtExportConnectorClass" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '</table>' +

                '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tr>' +
                '    <td class="configLabe1">' +
                '        <div class="propertiesAlign">' +
                '            <div class="proLeft ">Properties</div>' +
                '            <div class="editBtn addProBtn"> ' +
                '                <a href="javascript:void(0)" id="lnkAddNewProperty" class="btnEd"> <span class="userPlus">+</span> Add Property</a> ' +
                '            </div>' +
                '            <div class="clear"> </div>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '<tr>' +
                '    <td>' +
                '        <div class="addConfigProperWrapper">' +
                '            <table id="tblAddNewProperty" width="100%" cellpadding="0" cellspacing="0" class="addConfigProperTbl">' +
                '                <tr class="headerProperty">' +
                '                    <th>Name</th>' +
                '                    <th align="right">Value</th>' +
                '                    <th>Delete</th>' +
                '                </tr>' +

                '            </table>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '</table>';

            $("#addConfigWrapper").html(contents);

            $("#addConfigControls").show();
            $("#saveConfigConfirmation").hide();

            $('#chkStream').iCheck({
                checkboxClass: 'icheckbox_square-aero customCheckbox',
                increaseArea: '20%'
            });

            $('#chkStream').on('ifChanged', function () {
                $("#chkStreamValue").text(getOnOffText($('#chkStream').is(":checked")));
            });

            $('#txtType').change(function () {
                showHideConnectorClass();
                if (typeof type === "undefined") {
                    addExportProperties();
                }
            });

            $('#txtCustomType').focusout(function () {
                // Uppercase-ize contents
                this.value = this.value.toUpperCase();
            });

            var count = 0;

            $("#lnkAddNewProperty").on("click", function () {
                count++;
                var nameId = 'txtName' + count;
                var valueId = 'txtValue' + count;

                var newRow = '<tr>' +
                    '   <td>' +
                    '       <input size="15" id="' + nameId + '" name="' + nameId + '" class="newStreamPropertyName newStreamProperty" type="text">' +
                    '       <label id="errorName' + count + '" for="' + nameId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorValue' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="securityDelete" id="deleteFirstProperty" onclick="deleteRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewProperty").append(newRow);
            });

            $("#formAddConfiguration").validate({
                rules: {
                    txtStream: adminValidationRules.streamNameRules,
                    txtCustomType: adminValidationRules.streamNameRules,
                    txtExportConnectorClass: adminValidationRules.streamNameRules
                },
                messages: {
                    txtStream: adminValidationRules.streamNameMessages,
                    txtCustomType: adminValidationRules.streamNameMessages,
                    txtExportConnectorClass: adminValidationRules.streamNameMessages
                }
            });
        },
        afterOpen: function () {
            //For editing an existing configuration
            if (editId != "-1") {

                var existingAdminConfig = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                var config = existingAdminConfig["export"].configuration[editId * 1];
                $("#txtType").val(config.type);
                addExportProperties();
                VoltDbAdminConfig.orgTypeValue = config.type;
                $("#txtStream").val(config.target);

                $("#chkStream").iCheck(config.enabled ? 'check' : 'uncheck');
                $("#txtExportConnectorClass").val(config.exportconnectorclass);
                var properties = config.property;

                if (properties.length == 0) {
                    $("#deleteFirstProperty").trigger("click");
                }
                var count = 1;
                var multiPropertyCount = 0;
                var kafkaBootstrapServerStatus = false;

                if (config.type.toLowerCase() == "kafka") {
                    for (var j = 0; j < properties.length; j++) {
                        if (properties[j].name == "bootstrap.servers") {
                            kafkaBootstrapServerStatus = true;
                            break;
                        }
                    }
                }

                for (var i = 0; i < properties.length; i++) {
                    if (properties[i].name == "metadata.broker.list" && !kafkaBootstrapServerStatus) {
                        properties[i].name = "bootstrap.servers"
                        kafkaBootstrapServerStatus = true;
                    }

                    if (VoltDbAdminConfig.newStreamMinmPropertyName.hasOwnProperty(properties[i].name) || VoltDbAdminConfig.newStreamMinmPropertyName.hasOwnProperty(properties[i].name + '_' + config.type)) {
                        if ($(VoltDbAdminConfig.newStreamMinmPropertyName[properties[i].name]).length) {
                            $(VoltDbAdminConfig.newStreamMinmPropertyName[properties[i].name]).val(properties[i].value);
                            $(".newStreamMinProperty").addClass("orgProperty");
                        } else if ($(VoltDbAdminConfig.newStreamMinmPropertyName[properties[i].name + '_' + config.type]).length && multiPropertyCount == 0) {
                            $(VoltDbAdminConfig.newStreamMinmPropertyName[properties[i].name + '_' + config.type]).val(properties[i].value);
                            $(".newStreamMinProperty").addClass("orgProperty");
                            multiPropertyCount++;
                        } else {
                            $("#lnkAddNewProperty").trigger("click");
                            $("#txtName" + count).val(properties[i].name);
                            $("#txtValue" + count).val(properties[i].value);
                            count++;
                        }
                    } else {
                        $("#lnkAddNewProperty").trigger("click");
                        $("#txtName" + count).val(properties[i].name);
                        $("#txtValue" + count).val(properties[i].value);
                        count++;
                    }
                }
            } else {
                addExportProperties();
            }
            showHideConnectorClass();
            var popup = $(this)[0];
            $("#btnAddConfigSave").unbind("click");
            $("#btnAddConfigSave").on("click", function (e) {
                var newStreamPropertyNames = $(".newStreamPropertyName");
                for (var i = 0; i < newStreamPropertyNames.length; i++) {
                    $(newStreamPropertyNames[i]).rules("add", {
                        required: true,
                        regex: /^[a-zA-Z0-9_\-.]+$/,
                        messages: {
                            required: "This field is required",
                            regex: 'Only alphabets, numbers, <br/> _, - and . are allowed.'
                        }
                    });
                }

                var newStreamPropertyValues = $(".newStreamPropertyValue");
                for (var j = 0; j < newStreamPropertyValues.length; j++) {
                    $(newStreamPropertyValues[j]).rules("add", {
                        required: true,
                        messages: {
                            required: "This field is required"
                        }
                    });
                }

                if (!$("#formAddConfiguration").valid()) {
                    e.preventDefault();
                    e.stopPropagation();
                } else {
                    $("#addConfigControls").hide();
                    $("#deleteAddConfig").hide();
                    $("#saveConfigConfirmation").show();
                }
            });

            $("#btnAddConfigCancel").unbind("click");
            $("#btnAddConfigCancel").on("click", function () {

                //Close the popup
                popup.close();
            });

            //Center align the popup
            popup.center();

            $("#deleteAddConfig").on("click", function () {
                $("#addConfigControls").hide();
                $("#deleteAddConfig").hide();
                $("#expotSaveConfigText").text("delete").data("status", "delete");
                $("#saveConfigConfirmation").show();
            });

            $("#btnSaveConfigOk").unbind("click");
            var conCounter = 0;
            $(document).on("click", "#btnSaveConfigOk", function () {
                if (conCounter === 0) {
                    var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                    if ($("#expotSaveConfigText").data("status") == "delete") {
                        adminConfigurations["export"].configuration.splice(editId * 1, 1);
                    }
                    else {
                        var newConfig = {};
                        newConfig["property"] = [];

                        var newStreamProperties = $(".newStreamProperty");
                        for (var i = 0; i < newStreamProperties.length; i += 2) {
                            newConfig["property"].push({
                                "name": encodeURIComponent($(newStreamProperties[i]).val()),
                                "value": encodeURIComponent($(newStreamProperties[i + 1]).val()),
                            });
                        }
                        newConfig["target"] = $("#txtStream").val();
                        newConfig["type"] = $("#txtType").val().trim();
                        newConfig["enabled"] = $("#chkStream").is(':checked');
                        if ($("#txtType").val().trim().toUpperCase() == "CUSTOM") {
                            newConfig["exportconnectorclass"] = $("#txtExportConnectorClass").val();
                        }

                        if (!adminConfigurations["export"]) {
                            adminConfigurations["export"] = {};
                            adminConfigurations["export"]["configuration"] = [];
                        }

                        //For editing an existing configuration
                        if (editId == "-1") {
                            adminConfigurations["export"].configuration.push(newConfig);
                        } else {
                            var updatedConfig = adminConfigurations["export"].configuration[editId * 1];
                            updatedConfig.target = newConfig.target;
                            updatedConfig.type = newConfig.type;
                            updatedConfig.enabled = newConfig.enabled;
                            updatedConfig.property = newConfig.property;
                            updatedConfig.exportconnectorclass = newConfig.exportconnectorclass;
                        }
                    }

                    var currentConfig = adminEditObjects.exportConfiguration.html();

                    var loadingConfig = '<tr class="child-row-4 subLabelRow">' +
                        '   <td colspan="4" style="position:relative">&nbsp;<div class="loading-small loadExportConfig"></div></td>' +
                        '</tr>';

                    adminEditObjects.addNewConfigLink.hide();
                    adminEditObjects.exportConfiguration.html(loadingConfig);
                    adminEditObjects.loadingConfiguration.show();
                    VoltDbAdminConfig.isExportLoading = true;
                    //Close the popup
                    popup.close();

                    voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                        if (result.status == "1") {

                            //Reload Admin configurations for displaying the updated value
                            voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                                adminEditObjects.addNewConfigLink.show();
                                adminEditObjects.loadingConfiguration.hide();
                                adminEditObjects.exportConfiguration.data("status", "value");

                                VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            });
                        } else {
                            setTimeout(function () {
                                adminEditObjects.addNewConfigLink.show();
                                adminEditObjects.loadingConfiguration.hide();
                                adminEditObjects.exportConfiguration.data("status", "value");
                                adminEditObjects.exportConfiguration.html(currentConfig);

                                var msg = '"Export Configuration". ';
                                if (result.status == "-1" && result.statusstring == "Query timeout.") {
                                    msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                                } else if (result.statusstring != "") {
                                    msg += result.statusstring;
                                } else {
                                    msg += "Please try again later.";
                                }

                                $("#updateErrorFieldMsg").text(msg);

                                $("#updateErrorPopupLink").trigger("click");
                            }, 3000);
                        }
                        popup.close();
                        $("#btnSaveConfigOk").unbind("click");
                        VoltDbAdminConfig.isExportLoading = false;
                    });
                }
                conCounter++;
            });

            $("#btnSaveConfigCancel").unbind("click");
            $("#btnSaveConfigCancel").on("click", function () {

                $("#saveConfigConfirmation").hide();
                $("#addConfigControls").show();
                $("#expotSaveConfigText").text("save").data("status", "save");

                if (editId != "-1") {
                    $("#deleteAddConfig").show();
                }
                popup.close();
            });
        }
    });

    $("#addImportConfigPopupLink").popup({
        open: function (event, ui, ele) {
            editId = adminDOMObjects.addImportConfigPopupLink.data("id");

            //For adding a new import configuration
            if (editId == "-1") {
                $("#addImportConfigHeader").text("Add Configuration");
                $("#deleteImportConfig").hide();
            }//For editing an existing configuration
            else {
                $("#addImportConfigHeader").text("Edit Configuration");
                $("#deleteImportConfig").show();
            }

            $("#importSaveConfigText").text("save").data("status", "save");
            var contents = '' +
                '<table width="100%" cellpadding="0" cellspacing="0" class="configureImportTbl">' +
                '<tr>' +
                '    <td style="width:25%">Type </td>' +
                '    <td style="width:60%">' +
                '       <select id="txtImportType" name="txtImportType"> ' +
                '           <option value="KAFKA">KAFKA</option> ' +
                '           <option value="KINESIS">KINESIS</option> ' +
                '       </select>' +
                '    </td>' +
                '    <td width="8%" align="right"><input type="checkbox" checked="true" id="chkImportStream" class="chkStream"/></td>' +
                '    <td id="chkImportStreamValue">On</td>' +
                '</tr>' +
                '<tr> ' +
                '   <td>Format</td> ' +
                '   <td> ' +
                '       <div class="form-group formatImport"> ' +
                '           <div class="input-group"> ' +
                '               <input type="text" class="formatHeight" id="txtImportFormat" name="txtImportFormat"> ' +
                '               <div id="dropDownImg" class="input-group-addon drop-down-trigger"><span class="downImg"></span></div> ' +
                '           </div> ' +
                '           <ul id="ddlFormat" class="drop-down-list"> ' +
                '               <li class="formatOption">csv</li> ' +
                '               <li class="formatOption">tsv</li> ' +
                '           </ul> ' +
                '       </div> ' +
                '       <label id="errorImportFormat" for="txtImportFormat" class="error" style="display: none;"></label> ' +
                '   </td> ' +
                '   <td>&nbsp;</td> ' +
                '   <td>&nbsp;</td> ' +
                '</tr> ' +
                '<tr id="TrImportConnectorClass" style="display:none">' +
                '    <td>Module</td>' +
                '    <td width="15%" id="TdImportConnectorClass">' +
                '       <input id="txtImportModule" name="txtImportModule" type="text" size="38">' +
                '       <label id="errorImportModule" for="txtImportModule" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '</table>' +

                '<table width="100%" cellpadding="0" cellspacing="0" class="configureImportTbl">' +
                '<tr>' +
                '    <td class="configLabe1">' +
                '        <div class="propertiesAlign">' +
                '            <div class="proLeft ">Properties</div>' +
                '            <div class="editBtn addProBtn"> ' +
                '                <a href="javascript:void(0)" id="lnkAddNewImportProperty" class="btnEd"> <span class="userPlus">+</span> Add Property</a> ' +
                '            </div>' +
                '            <div class="clear"> </div>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '<tr>' +
                '    <td>' +
                '        <div class="addConfigProperWrapper">' +
                '            <table id="tblAddNewImportProperty" width="100%" cellpadding="0" cellspacing="0" class="addConfigProperTbl">' +
                '                <tr class="headerProperty">' +
                '                    <th>Name</th>' +
                '                    <th align="right">Value</th>' +
                '                    <th>Delete</th>' +
                '                </tr>' +

                '            </table>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '</table>';

            $("#addImportConfigWrapper").html(contents);

            $("#dropDownImg").on("click", function () {
                if ($("#ddlFormat").is(":visible")) {
                    $("#ddlFormat").hide();
                } else {
                    $("#ddlFormat").show();
                }
            })
            $(".formatOption").on("click", function () {
                $("#txtImportFormat").val($(this).text())
                $("#ddlFormat").hide()
            });

            $("#addImportConfigControls").show();
            $("#saveImportConfigConfirmation").hide();

            $('#chkImportStream').iCheck({
                checkboxClass: 'icheckbox_square-aero customCheckbox',
                increaseArea: '20%'
            });

            $('#chkImportStream').on('ifChanged', function () {
                $("#chkImportStreamValue").text(getOnOffText($('#chkImportStream').is(":checked")));
            });

            $('#txtImportType').change(function () {
                if (typeof type === "undefined") {
                    addImportProperties();
                }
            });

            var count = 0;

            $("#lnkAddNewImportProperty").on("click", function () {

                count++;
                var nameId = 'txtImportName' + count;
                var valueId = 'txtImportValue' + count;

                var newRow = '<tr>' +
                    '   <td>' +
                    '       <input size="15" id="' + nameId + '" name="' + nameId + '" class="newImportStreamPropertyName newImportStreamProperty" type="text">' +
                    '       <label id="errorImportName' + count + '" for="' + nameId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorImportValue' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="securityDelete" id="deleteFirstImportProperty" onclick="deleteRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewImportProperty").append(newRow);
            });

            $("#formAddImportConfiguration").validate({
                rules: {
                    txtImportModule: adminValidationRules.streamNameRules,
                    txtImportFormat: adminValidationRules.streamNameRules,
                },
                messages: {
                    txtImportModule: adminValidationRules.streamNameMessages,
                    txtImportFormat: adminValidationRules.streamNameMessages,
                }
            });
        },
        afterOpen: function () {
            //For editing an existing configuration
            if (editId != "-1") {
                var existingAdminConfig = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                var config = existingAdminConfig["import"].configuration[editId * 1];
                $("#txtImportType").val(config.type);
                addImportProperties();
                VoltDbAdminConfig.orgTypeValue = config.type;
                $("#txtImportFormat").val(config.format);

                $("#chkImportStream").iCheck(config.enabled ? 'check' : 'uncheck');
                $("#txtImportModule").val(config.model);
                var properties = config.property;

                if (properties.length == 0) {
                    $("#deleteFirstProperty").trigger("click");
                }
                var count = 1;
                var multiPropertyCount = 0;
                for (var i = 0; i < properties.length; i++) {
                    if (VoltDbAdminConfig.newImportStreamMinPropertyName.hasOwnProperty(properties[i].name) || VoltDbAdminConfig.newImportStreamMinPropertyName.hasOwnProperty(properties[i].name + '_' + config.type)) {
                        if ($(VoltDbAdminConfig.newImportStreamMinPropertyName[properties[i].name]).length) {
                            $(VoltDbAdminConfig.newImportStreamMinPropertyName[properties[i].name]).val(properties[i].value);
                            $(".newImportStreamMinProperty").addClass("orgProperty");
                        } else if ($(VoltDbAdminConfig.newImportStreamMinPropertyName[properties[i].name + '_' + config.type]).length && multiPropertyCount == 0) {
                            $(VoltDbAdminConfig.newImportStreamMinPropertyName[properties[i].name + '_' + config.type]).val(properties[i].value);
                            $(".newImportStreamMinProperty").addClass("orgProperty");
                            multiPropertyCount++;
                        } else {
                            $("#lnkAddNewImportProperty").trigger("click");
                            $("#txtImportName" + count).val(properties[i].name);
                            $("#txtImportValue" + count).val(properties[i].value);
                            count++;
                        }
                    } else {
                        $("#lnkAddNewImportProperty").trigger("click");
                        $("#txtImportName" + count).val(properties[i].name);
                        $("#txtImportValue" + count).val(properties[i].value);
                        count++;
                    }
                }
            } else {
                addImportProperties();
            }
            var popup = $(this)[0];
            $("#btnAddImportConfigSave").unbind("click");
            $("#btnAddImportConfigSave").on("click", function (e) {
                var newStreamPropertyNames = $(".newImportStreamPropertyName");
                for (var i = 0; i < newStreamPropertyNames.length; i++) {
                    $(newStreamPropertyNames[i]).rules("add", {
                        required: true,
                        regex: /^[a-zA-Z0-9_\-.]+$/,
                        messages: {
                            required: "This field is required",
                            regex: 'Only alphabets, numbers, <br/> _, - and . are allowed.'
                        }
                    });
                }

                var newStreamPropertyValues = $(".newImportStreamPropertyValue");
                for (var j = 0; j < newStreamPropertyValues.length; j++) {
                    $(newStreamPropertyValues[j]).rules("add", {
                        required: true,
                        messages: {
                            required: "This field is required"
                        }
                    });
                }

                if (!$("#formAddImportConfiguration").valid()) {
                    e.preventDefault();
                    e.stopPropagation();
                    return;
                } else {
                    $("#addImportConfigControls").hide();
                    $("#deleteImportConfig").hide();
                    $("#saveImportConfigConfirmation").show();
                }
            });
            $("#btnAddImportConfigCancel").unbind("click");
            $("#btnAddImportConfigCancel").on("click", function () {
                //Close the popup
                popup.close();
            });

            //Center align the popup
            popup.center();

            $("#deleteImportConfig").on("click", function () {
                $("#addImportConfigControls").hide();
                $("#deleteImportConfig").hide();
                $("#importSaveConfigText").text("delete").data("status", "delete");
                $("#saveImportConfigConfirmation").show();
            });

            $(document).off("click", "#btnSaveImportConfigOk");
            $(document).on("click", "#btnSaveImportConfigOk", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if ($("#importSaveConfigText").data("status") == "delete") {
                    adminConfigurations["import"].configuration.splice(editId * 1, 1);
                }
                else {
                    var newConfig = {};
                    newConfig["property"] = [];

                    var newStreamProperties = $(".newImportStreamProperty");
                    for (var i = 0; i < newStreamProperties.length; i += 2) {
                        newConfig["property"].push({
                            "name": encodeURIComponent($(newStreamProperties[i]).val()),
                            "value": encodeURIComponent($(newStreamProperties[i + 1]).val()),
                        });
                    }
                    newConfig["format"] = $("#txtImportFormat").val();
                    newConfig["type"] = $("#txtImportType").val().trim();
                    newConfig["enabled"] = $("#chkImportStream").is(':checked');
                    if ($("#txtImportType").val().trim().toUpperCase() == "CUSTOM") {
                        newConfig["module"] = $("#txtImportModule").val();
                    } else {
                        newConfig["module"] = null
                    }

                    if (!adminConfigurations["import"]) {
                        adminConfigurations["import"] = {};
                        adminConfigurations["import"]["configuration"] = [];
                    }

                    //For editing an existing configuration
                    if (editId == "-1") {
                        adminConfigurations["import"].configuration.push(newConfig);
                    } else {
                        var updatedConfig = adminConfigurations["import"].configuration[editId * 1];
                        updatedConfig.format = newConfig.format;
                        updatedConfig.type = newConfig.type;
                        updatedConfig.enabled = newConfig.enabled;
                        updatedConfig.property = newConfig.property;
                        updatedConfig.module = newConfig.module;
                    }
                }

                var currentConfig = adminEditObjects.importConfiguration.html();

                var loadingConfig = '<tr class="child-row-4 subLabelRow">' +
                    '   <td colspan="4" style="position:relative">&nbsp;<div class="loading-small loadExportConfig"></div></td>' +
                    '</tr>';
                adminEditObjects.addNewImportConfigLink.hide();
                adminEditObjects.importConfiguration.html(loadingConfig);
                adminEditObjects.loadingImportConfig.show();
                adminEditObjects.importConfiguration.data("status", "loading");

                //Close the popup
                popup.close();

                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            adminEditObjects.loadingImportConfig.hide();
                            adminEditObjects.addNewImportConfigLink.show();
                            adminEditObjects.importConfiguration.data("status", "value");

                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                        });

                    } else {
                        setTimeout(function () {
                            adminEditObjects.loadingImportConfig.hide();
                            adminEditObjects.addNewImportConfigLink.show();
                            adminEditObjects.importConfiguration.data("status", "value");
                            adminEditObjects.importConfiguration.html(currentConfig);

                            var msg = '"Import Configuration". ';
                            if (result.status == "-1" && result.statusstring == "Query timeout.") {
                                msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                            } else if (result.statusstring != "") {
                                msg += result.statusstring;
                            } else {
                                msg += "Please try again later.";
                            }

                            $("#updateImportErrorFieldMsg").text(msg);

                            $("#updateImportErrorPopupLink").trigger("click");
                        }, 3000);
                    }
                });
            });

            $("#btnSaveImportConfigCancel").unbind("click");
            $("#btnSaveImportConfigCancel").on("click", function () {
                $("#saveImportConfigConfirmation").hide();
                $("#addImportConfigControls").show();
                $("#importSaveConfigText").text("save").data("status", "save");

                if (editId != "-1") {
                    $("#deleteImportConfig").show();
                }
            });
        }
    });

    $("#addDiskLimitPopupLink").popup({
        open: function (event, ui, ele) {

            $("#addDiskLimitHeader").text("Edit Disk Limit");

            $("#expotSaveDiskLimitText").text("save").data("status", "save");

            var contents = '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tr>' +
                '    <td class="configLabe1">' +
                '        <div class="featuresAlign">' +
                '            <div class="proLeft ">Feature</div>' +
                '            <div class="editBtn addProBtn"> ' +
                '                <a href="javascript:void(0)" id="lnkAddNewFeature" class="btnEd"> <span class="userPlus">+</span> Add Feature</a> ' +
                '            </div>' +
                '            <div class="clear"> </div>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '<tr>' +
                '    <td>' +
                '        <div class="addConfigProperWrapper">' +
                '            <table id="tblAddNewFeature" width="100%" cellpadding="0" cellspacing="0" class="addConfigProperTbl">' +
                '                <tr class="headerFeature">' +
                '                    <th width="50%">Name</th>' +
                '                    <th align="right" width="14%">Value</th>' +
                '                    <th align="right" width="14%">Unit</th>' +
                '                    <th width="5%">Delete</th>' +
                '                </tr>' +

                '            </table>' +
                '        </div>' +
                '    </td>' +
                '</tr>' +
                '</table>';

            $("#addDiskLimitWrapper").html(contents);

            $("#addDiskLimitControls").show();
            $("#saveDiskLimitConfirmation").hide();


            var count = 0;

            $("#lnkAddNewFeature").on("click", function () {
                count = $('.newFeatureValue').length;
                count++;

                var nameId = 'txtNameDL' + count;
                var valueId = 'txtValueDL' + count;
                var unitId = 'txtUnitDL' + count;

                var newRow = '<tr>' +
                    '   <td>' +
                    '<select  id="' + nameId + '" name="' + nameId + '" class="newFeatureName newFeature">' +
                    '<option>SNAPSHOTS</option><option>COMMANDLOG</option><option>EXPORTOVERFLOW</option><option>DROVERFLOW</option><option>COMMANDLOGSNAPSHOT</option>' +
                    '</select>' +
                    '       <label id="error_' + nameId + '" class="error duplicateError" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newFeatureValue newFeature" type="text" style="width:auto;">' +
                    '       <label id="errorValueDL' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><select id="' + unitId + '" name="' + unitId + '" class="newFeatureUnit newFeature" style="width:auto;"><option>GB</option><option>%</option></select>' +
                    '       <label id="errorValueUnit' + count + '" for="' + unitId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="securityDelete" id="deleteFirstFeature" onclick="deleteRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewFeature").append(newRow);
            });

            $("#formAddDiskLimit").validate();
        },
        afterOpen: function () {
            $(".duplicateError").hide();

            var existingAdminConfig = VoltDbAdminConfig.getLatestRawAdminConfigurations();
            if (existingAdminConfig.systemsettings.resourcemonitor != null) {
                if (existingAdminConfig.systemsettings.resourcemonitor.disklimit != null) {

                    var config = existingAdminConfig.systemsettings.resourcemonitor.disklimit;

                    var features = config.feature;

                    if (features.length == 0) {
                        $("#deleteFirstProperty").trigger("click");
                    }
                    var count = 1;
                    for (var i = 0; i < features.length; i++) {

                        var nameId = 'txtNameDL' + count;
                        var valueId = 'txtValueDL' + count;
                        var unitId = 'txtUnitDL' + count;

                        var newRow = '<tr>' +
                            '   <td>' +
                            '<select  id="' + nameId + '" name="' + nameId + '" class="newFeatureName newFeature">' +
                            '<option>SNAPSHOTS</option><option>COMMANDLOG</option><option>EXPORTOVERFLOW</option><option>DROVERFLOW</option><option>COMMANDLOGSNAPSHOT</option>' +
                            '</select>' +
                            '       <label id="error_' + nameId + '" class="error" style="display: none;"></label>' +
                            '   </td>' +
                            '   <td>' +
                            '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newFeatureValue newFeature" type="text" style="width:auto;">' +
                            '       <label id="errorValueDL' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                            '   </td>' +
                            '   <td><select id="' + unitId + '" name="' + unitId + '" class="newFeatureUnit newFeature" style="width:auto;"><option>GB</option><option>%</option></select>' +
                            '       <label id="errorValueUnit' + count + '" for="' + unitId + '" class="error" style="display: none;"></label>' +
                            '   </td>' +
                            '   <td><div class="securityDelete" id="deleteFirstFeature" onclick="deleteRow(this)"></div></td>' +
                            '</tr>';


                        $("#tblAddNewFeature").append(newRow);

                        $("#" + nameId).val(features[i].name);


                        if (features[i].size.indexOf("%") === -1) {
                            $("#" + valueId).val(features[i].size);

                        } else {
                            features[i].size = features[i].size.replace("%", "");
                            $("#" + valueId).val(features[i].size);
                            $("#" + unitId).val("%");
                        }

                        count++;
                    }
                }
            }

            var popup = $(this)[0];
            $("#btnAddDiskLimitSave").unbind("click");
            $("#btnAddDiskLimitSave").on("click", function (e) {
                $(".duplicateError").hide();
                var featuresNames = [];


                var newFeatureNames = $(".newFeatureName");
                for (var i = 0; i < newFeatureNames.length; i++) {
                    featuresNames.push(newFeatureNames[i].value);
                    $(newFeatureNames[i]).rules("add", {
                        required: true,
                        messages: {
                            required: "This field is required",
                        }
                    });
                }

                var newFeatureValues = $(".newFeatureValue");
                var newFeatureUnits = $(".newFeatureUnit");
                for (var j = 0; j < newFeatureValues.length; j++) {
                    $(newFeatureValues[j]).rules("remove");
                    var unit = newFeatureUnits[j].value;
                    if (unit == "%") {
                        $(newFeatureValues[j]).rules("add", {
                            required: true,
                            min: 0,
                            max: 99,
                            digits: true,
                            messages: {
                                required: "This field is required",
                                min: "Please enter a positive number.",
                                max: "Maximum value of percentage cannot be greater than 99.",
                                digits: "Please enter a positive number without any decimal."
                            }
                        });
                    } else if (unit == "GB") {
                        $(newFeatureValues[j]).rules("add", {
                            required: true,
                            min: 0,
                            max: 2147483647,
                            digits: true,
                            messages: {
                                required: "This field is required",
                                min: "Please enter a positive number.",
                                max: "Maximum value of GB cannot be greater than 2147483647.",
                                digits: "Please enter a positive number without any decimal."
                            }
                        });
                    }
                }




                if (!$("#formAddDiskLimit").valid()) {
                    e.preventDefault();
                    e.stopPropagation();
                } else {
                    if (hasDuplicates(featuresNames)) {

                        for (var i = 0; i < newFeatureNames.length; i++) {
                            if (newFeatureNames[i].value == duplicateValue) {
                                $('#error_' + newFeatureNames[i].id).show();
                                $('#error_' + newFeatureNames[i].id).html("Duplicate feature names are not allowed.");
                            }
                        }
                        e.preventDefault();
                        e.stopPropagation();
                    } else {
                        $("#addDiskLimitControls").hide();
                        $("#saveDiskLimitConfirmation").show();
                    }
                }
            });


            $("#btnAddDiskLimitCancel").unbind("click");
            $("#btnAddDiskLimitCancel").on("click", function () {

                //Close the popup
                popup.close();

            });

            //Center align the popup
            popup.center();


            $("#deleteAddConfig").on("click", function () {
                $("#addConfigControls").hide();
                $("#deleteAddConfig").hide();
                $("#expotSaveConfigText").text("delete").data("status", "delete");
                $("#saveConfigConfirmation").show();
            });

            $(document).off("click", "#btnSaveDiskLimitOk");
            $(document).on("click", "#btnSaveDiskLimitOk", function () {


                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (adminConfigurations.systemsettings.resourcemonitor == null) {
                    adminConfigurations.systemsettings.resourcemonitor = {};
                }
                if (adminConfigurations.systemsettings.resourcemonitor.disklimit == null) {
                    adminConfigurations.systemsettings.resourcemonitor.disklimit = {};
                }

                var feature = [];

                var newFeatureNames = $(".newFeatureName");
                var newFeatureValues = $(".newFeatureValue");
                var newFeatureUnits = $(".newFeatureUnit");
                for (var i = 0; i < newFeatureNames.length; i++) {
                    if (newFeatureUnits[i].value == "GB") {
                        feature.push(
                            {
                                name: newFeatureNames[i].value,
                                size: newFeatureValues[i].value
                            }
                        );
                    } else {
                        feature.push(
                            {
                                name: newFeatureNames[i].value,
                                size: newFeatureValues[i].value + newFeatureUnits[i].value
                            }
                        );

                    }
                }

                if (feature.length == 0) {
                    adminConfigurations.systemsettings.resourcemonitor.disklimit = null;

                } else {
                    adminConfigurations.systemsettings.resourcemonitor.disklimit.feature = feature;
                }

                //Set the new value to be saved. 



                popup.close();

                var currentDiskLimit = adminEditObjects.diskLimitConfiguration.html();

                var loadingConfig = '<tr class="childprop-row-60 subLabelRow" id="loadDL" style="display:none">' +
                    '   <td colspan="4" style="position:relative;">&nbsp;<div class="loading-small loadExportConfig"></div></td>' +
                    '</tr>';

                $("#btnEditDiskLimit").hide();
                $("#loadingDiskLimit").show();
                adminEditObjects.editDiskLimitLink.hide();
                adminEditObjects.diskLimitConfiguration.find(".childprop-row-60").remove();

                adminEditObjects.diskLimitConfiguration.append(loadingConfig);
                if ($("#row-60").find('a:first').hasClass('labelExpanded')) {
                    $("#loadDL").show();
                }

                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {

                    if (result.status == "1") {

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            $("#loadingDiskLimit").hide();
                            $("#btnEditDiskLimit").show();
                            adminEditObjects.diskLimitConfiguration.data("status", "value");

                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                        });

                    } else {
                        setTimeout(function () {
                            $("#loadingDiskLimit").hide();
                            $("#btnEditDiskLimit").show();
                            adminEditObjects.diskLimitConfiguration.data("status", "value");
                            adminEditObjects.diskLimitConfiguration.html(currentDiskLimit);

                            var msg = '"Disk Limit Configuration". ';
                            if (result.status == "-1" && result.statusstring == "Query timeout.") {
                                msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                            } else if (result.statusstring != "") {
                                msg += result.statusstring;
                            } else {
                                msg += "Please try again later.";
                            }

                            $("#updateErrorFieldMsg").text(msg);

                            $("#updateErrorPopupLink").trigger("click");
                        }, 3000);
                    }
                });

            });

            $("#btnSaveDiskLimitCancel").unbind("click");
            $("#btnSaveDiskLimitCancel").on("click", function () {

                $("#saveDiskLimitConfirmation").hide();
                $("#addDiskLimitControls").show();
                $("#expotSaveDiskLimitText").text("save").data("status", "save");

            });

            var hasDuplicates = function (array) {
                var valuesSoFar = Object.create(null);
                for (var i = 0; i < array.length; ++i) {
                    var value = array[i];
                    if (value in valuesSoFar) {
                        duplicateValue = value;
                        return true;
                    }
                    valuesSoFar[value] = true;
                }
                return false;
            };

        }
    });

    var addExportProperties = function () {
        var exportType = $('#txtType').val();
        if (editId == 1)
            VoltDbAdminConfig.orgTypeValue = "";
        for (var i = 0; i < $(".newStreamMinProperty").length; i++) {
            if (!$($(".newStreamMinProperty")[i]).hasClass("orgProperty")) {
                $($(".newStreamMinProperty")[i]).addClass("propertyToRemove");
            }
        }
        $(".propertyToRemove").not(".addedProperty").remove();

        var exportProperties = '';
        if (exportType.toUpperCase() == "FILE") {
            if (!$('#txtOutdir').length) {
                exportProperties += '' +
                    '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtOutdir" name="txtOutdir" value="outdir" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorOutdir" for="txtOutdir" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtOutdirValue" name="txtOutdirValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorOutdirValue" for="txtOutdirValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtOutdir').attr("disabled", "disabled");
            }
            if (!$('#txtnonce').length) {
                exportProperties += '' +
                    '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtnonce" name="txtnonce" value="nonce" disabled="disabled" class="newStreamPropertyName newStreamProperty  requiredProperty" type="text">' +
                    '       <label id="errornonce" for="txtnonce" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtnonceValue" name="txtnonceValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errornonceValue" for="txtnonceValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtnonce').attr("disabled", "disabled");
            }
            if (!$('#txtFileType').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtFileType" name="txtFileType" value="type" disabled="disabled" class="newStreamPropertyName newStreamProperty  requiredProperty" type="text">' +
                    '       <label id="errorFileType" for="txtFileType" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtFileTypeValue" name="txtFileTypeValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorFileTypeValue" for="txtFileTypeValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtFileType').attr("disabled", "disabled");
            }
        } else if (exportType.toUpperCase() == "HTTP") {
            if (!$('#txtEndpoint').length) {
                exportProperties = '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpoint" name="txtEndpoint" value="endpoint" disabled="disabled" class="newStreamPropertyName newStreamProperty  requiredProperty" type="text">' +
                    '       <label id="errorEndpoint" for="txtEndpoint" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointValue" name="txtEndpointValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorEndpointValue" for="txtEndpointValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtEndpoint').attr("disabled", "disabled");
            }
        } else if (exportType.toUpperCase() == "KAFKA") {
            if (!$('#txtBootstrapServersList').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtBootstrapServersList" name="txtBootstrapServersList" value="bootstrap.servers" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorMetadataBrokerList" for="txtBootstrapServersList" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtBootstrapServersListValue" name="txtBootstrapServersListValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorMetadataBrokerListValue" for="txtBootstrapServersListValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtBootstrapServersList').attr("disabled", "disabled");
            }
        } else if (exportType.toUpperCase() == "JDBC") {
            if (!$('#txtJdbcUrl').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcUrl" name="txtJdbcUrl" value="jdbcurl" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorJdbcUrl" for="txtJdbcUrl" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcUrlValue" name="txtJdbcUrlValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorJdbcUrlValue" for="txtJdbcUrlValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtJdbcUrl').attr("disabled", "disabled");
            }
            if (!$('#txtJdbcDriver').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcDriver" name="txtJdbcDriver" value="jdbcdriver" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorJdbcDriver" for="txtJdbcDriver" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcDriverValue" name="txtJdbcDriverValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorJdbcDriverValue" for="txtJdbcDriverValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtJdbcDriver').attr("disabled", "disabled");
            }
        } else if (exportType.toUpperCase() == "ELASTICSEARCH") {
            if (!$('#txtEndpointES').length) {
                exportProperties = '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointES" name="txtEndpointES" value="endpoint" disabled="disabled" class="newStreamPropertyName newStreamProperty  requiredProperty" type="text">' +
                    '       <label id="errorEndpoint" for="txtEndpoint" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointESValue" name="txtEndpointESValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorEndpointESValue" for="txtEndpointESValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtEndpointES').attr("disabled", "disabled");
            }
        }
        $('#tblAddNewProperty tr.headerProperty').after(exportProperties);

        removeDuplicateProperty();
        setDefaultProperty();
    };

    var removeDuplicateProperty = function () {
        $('#tblAddNewProperty :input').each(function () {
            if ($(this).val() == "outdir") {
                removeDuplicate(this, "outdir");
            } else if ($(this).val() == "nonce") {
                removeDuplicate(this, "nonce");
            } else if ($(this).val() == "type") {
                removeDuplicate(this, "type");
            } else if ($(this).val() == "endpoint") {
                removeDuplicate(this, "endpoint");
            } else if ($(this).val() == "bootstrap.servers") {
                removeDuplicate(this, "bootstrap.servers");
            } else if ($(this).val() == "jdbcurl") {
                removeDuplicate(this, "jdbcurl");
            } else if ($(this).val() == "jdbcdriver") {
                removeDuplicate(this, "jdbcdriver");
            } else if ($(this).val() == "broker.host") {
                removeDuplicate(this, "broker.host");
            } else if ($(this).val() == "amqp.uri") {
                removeDuplicate(this, "amqp.uri");
            }
        });
    };

    var removeDuplicate = function (object, propertyName) {
        var exportType = $('#txtType').val();
        if (!$(object).hasClass("requiredProperty")) {
            var val = $(':input:eq(' + ($(':input').index(object) + 1) + ')').val();
            if ($(VoltDbAdminConfig.newStreamMinmPropertyName[propertyName]).length) {
                $(VoltDbAdminConfig.newStreamMinmPropertyName[propertyName]).val(val);
                $(".newStreamMinProperty").addClass("addedProperty");
                var $row = $(object).closest("tr");
                $row.remove();
            } else if ($(VoltDbAdminConfig.newStreamMinmPropertyName[propertyName + '_' + exportType]).length) {
                $(VoltDbAdminConfig.newStreamMinmPropertyName[propertyName + '_' + exportType]).val(val);
                $(".newStreamMinProperty").addClass("addedProperty");
                var $row1 = $(object).closest("tr");
                $row1.remove();
            }
        }
    };

    var setDefaultProperty = function () {

        var exportType = $('#txtType').val();
        if (exportType.toUpperCase() == "FILE") {
            setDefaultDisplay($("#txtOutdir"));
            setDefaultDisplay($("#txtnonce"));
            setDefaultDisplay($("#txtFileType"));
        } else {
            setNormalDisplay($("#txtOutdir"));
            setNormalDisplay($("#txtnonce"));
            setNormalDisplay($("#txtFileType"));
        }

        if (exportType.toUpperCase() == "HTTP") {
            setDefaultDisplay($("#txtEndpoint"));
        } else {
            setNormalDisplay($("#txtEndpoint"));
        }

        if (exportType.toUpperCase() == "KAFKA") {
            setDefaultDisplay($("#txtBootstrapServersList"));
        } else {
            setNormalDisplay($("#txtBootstrapServersList"));
        }

        if (exportType.toUpperCase() == "JDBC") {
            setDefaultDisplay($("#txtJdbcUrl"));
            setDefaultDisplay($("#txtJdbcDriver"));
        } else {
            setNormalDisplay($("#txtJdbcUrl"));
            setNormalDisplay($("#txtJdbcDriver"));
        }

        if (exportType.toUpperCase() == "ELASTICSEARCH") {
            setDefaultDisplay($("#txtEndpointES"));
        } else {
            setNormalDisplay($("#txtEndpointES"));
        }

    };

    var setDefaultDisplay = function (txtbox) {
        var $row = txtbox.closest("tr");
        $('#tblAddNewProperty tr.headerProperty').after($row);
        var $td = $row.find("td:last-child");
        $td.html('');
    };

    var setNormalDisplay = function (txtbox) {
        txtbox.removeAttr('disabled');
        var $row = txtbox.closest("tr");
        var $td = $row.find("td:last-child");
        $td.html('<div class="securityDelete" onclick="deleteRow(this)"></div>');
    };

    //set import properties
    var addImportProperties = function () {
        var exportType = $('#txtImportType').val();
        if (editId == 1)
            VoltDbAdminConfig.orgTypeValue = "";
        for (var i = 0; i < $(".newImportStreamMinProperty").length; i++) {
            if (!$($(".newImportStreamMinProperty")[i]).hasClass("orgProperty")) {
                $($(".newImportStreamMinProperty")[i]).addClass("propertyToRemove");
            }
        }
        $(".propertyToRemove").not(".addedImportProperty").remove();

        var exportProperties = '';
        if (exportType.toUpperCase() == "KAFKA") {
            if (!$('#txtBrokers').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtBrokers" name="txtBrokers" value="brokers" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorBrokers" for="txtBrokers" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtBrokersValue" name="txtBrokersValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorBrokersValue" for="txtBrokersValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtBrokers').attr("disabled", "disabled");
            }

            if (!$('#txtProcedure').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtProcedure" name="txtProcedure" value="procedure" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorProcedure" for="txtProcedure" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtProcedureValue" name="txtProcedureValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorProcedureValue" for="txtProcedureValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtProcedure').attr("disabled", "disabled");
            }

            if (!$('#txtTopics').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtTopics" name="txtTopics" value="topics" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorTopics" for="txtTopics" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtTopicsValue" name="txtTopicsValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorTopicValue" for="txtTopicsValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtTopics').attr("disabled", "disabled");
            }

        } else if (exportType.toUpperCase() == "KINESIS") {
            if (!$('#txtAppName').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtAppName" name="txtAppName" value="app.name" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorAppName" for="txtAppName" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtAppNameValue" name="txtAppNameValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorAppNameValue" for="txtAppNameValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtAppName').attr("disabled", "disabled");
            }

            if (!$('#txtProcedureKi').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtProcedureKi" name="txtProcedureKi" value="procedure" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorProcedureKi" for="txtProcedureKi" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtProcedureKiValue" name="txtProcedureKiValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorProcedureKiValue" for="txtProcedureKiValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtProcedureKi').attr("disabled", "disabled");
            }

            if (!$('#txtRegion').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtRegion" name="txtRegion" value="region" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorRegion" for="txtRegion" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtRegionValue" name="txtRegionValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorRegionValue" for="txtRegionValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtRegion').attr("disabled", "disabled");
            }

            if (!$('#txtStreamName').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtStreamName" name="txtStreamName" value="stream.name" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorStreamName" for="txtStreamName" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtStreamNameValue" name="txtStreamNameValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorStreamNameValue" for="txtStreamNameValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtStreamName').attr("disabled", "disabled");
            }

            if (!$('#txtAccessKey').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtAccessKey" name="txtAccessKey" value="access.key" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorAccessKey" for="txtAccessKey" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtAccessKeyValue" name="txtAccessKeyValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorAccessKeyValue" for="txtAccessKeyValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtAccessKey').attr("disabled", "disabled");
            }

            if (!$('#txtSecretKey').length) {
                exportProperties += '<tr class="newImportStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtSecretKey" name="txtSecretKey" value="secret.key" disabled="disabled" class="newImportStreamPropertyName newImportStreamProperty requiredImportProperty" type="text">' +
                    '       <label id="errorSecretKey" for="txtSecretKey" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtSecretKeyValue" name="txtSecretKeyValue" class="newImportStreamPropertyValue newImportStreamProperty" type="text">' +
                    '       <label id="errorSecretKeyValue" for="txtSecretKeyValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtSecretKey').attr("disabled", "disabled");
            }
        }
        $('#tblAddNewImportProperty tr.headerProperty').after(exportProperties);

        removeDuplicateImportProperty();
        setDefaultImportProperty();
    };

    var removeDuplicateImportProperty = function () {
        $('#tblAddNewImportProperty :input').each(function () {
            if ($(this).val() == "brokers") {
                removeDuplicateImport(this, "brokers");
            } else if ($(this).val() == "procedure") {
                removeDuplicateImport(this, "procedure");
            } else if ($(this).val() == "topics") {
                removeDuplicateImport(this, "topics");
            } else if ($(this).val() == "app.name") {
                removeDuplicateImport(this, "app.name");
            } else if ($(this).val() == "stream.name") {
                removeDuplicateImport(this, "stream.name");
            } else if ($(this).val() == "access.key") {
                removeDuplicateImport(this, "access.key");
            } else if ($(this).val() == "secret.key") {
                removeDuplicateImport(this, "secret.key");
            } else if ($(this).val() == "region") {
                removeDuplicateImport(this, "region");
            }
        });
    };

    var removeDuplicateImport = function (object, propertyName) {
        var importType = $('#txtImportType').val();
        if (!$(object).hasClass("requiredImportProperty")) {
            var val = $(':input:eq(' + ($(':input').index(object) + 1) + ')').val();
            if ($(VoltDbAdminConfig.newImportStreamMinPropertyName[propertyName]).length) {
                $(VoltDbAdminConfig.newImportStreamMinPropertyName[propertyName]).val(val);
                $(".newImportStreamMinProperty").addClass("addedImportProperty");
                var $row = $(object).closest("tr");
                $row.remove();
            } else if ($(VoltDbAdminConfig.newImportStreamMinPropertyName[propertyName + '_' + importType]).length) {
                $(VoltDbAdminConfig.newImportStreamMinPropertyName[propertyName + '_' + importType]).val(val);
                $(".newImportStreamMinProperty").addClass("addedImportProperty");
                var $row1 = $(object).closest("tr");
                $row1.remove();
            }
        }
    };

    var setDefaultImportProperty = function () {
        var importType = $('#txtImportType').val();
        var isKafka = importType.toUpperCase() == "KAFKA"
        setDefaultNormalImportDisplay(isKafka, $("#txtBrokers"));
        setDefaultNormalImportDisplay(isKafka, $("#txtProcedure"));
        setDefaultNormalImportDisplay(isKafka, $("#txtTopics"));

        var isKinesis = importType.toUpperCase() == "KINESIS"
        setDefaultNormalImportDisplay(isKinesis, $("#txtAppName"));
        setDefaultNormalImportDisplay(isKinesis, $("#txtProcedureKi"));
        setDefaultNormalImportDisplay(isKinesis, $("#txtStreamName"));
        setDefaultNormalImportDisplay(isKinesis, $("#txtAccessKey"));
        setDefaultNormalImportDisplay(isKinesis, $("#txtSecretKey"));
        setDefaultNormalImportDisplay(isKinesis, $("#txtRegion"));
    };

    var setDefaultImportDisplay = function (txtbox) {
        var $row = txtbox.closest("tr");
        $('#tblAddNewImportProperty tr.headerProperty').after($row);
        var $td = $row.find("td:last-child");
        $td.html('');
    };

    var setNormalImportDisplay = function (txtbox) {
        txtbox.removeAttr('disabled');
        var $row = txtbox.closest("tr");
        var $td = $row.find("td:last-child");
        $td.html('<div class="securityDelete" onclick="deleteRow(this)"></div>');
    };

    var setDefaultNormalImportDisplay = function (isType, txtBox) {
        if (isType) {
            setDefaultImportDisplay(txtBox)
        } else {
            setNormalImportDisplay(txtBox)
        }
    }
    //

    var editUserState = -1;
    var orguser = '';
    $("#addNewUserLink").popup({
        open: function (event, ui, ele) {
            getListOfRoles();
            $("#addUserControl").show();
            $("#deleteSecUser").show();
            $("#saveUserControl").hide();
            editUserState = $('#addUserInnerPopup').data('isupdate');
            if (editUserState == 1) {
                $("#deleteUser").css('display', 'block');
            } else {
                $("#deleteUser").css('display', 'none');
            }
            var content = '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tbody>' +
                '<tr>' +
                '<td width="30%">Username</td>' +
                '<td width="10%">' +
                '<input id="txtUser" name="txtUser" type="text" size="30" aria-required="true" class="error"/>' +
                '<label id="errorUser" for="txtUser" class="error" style="display:none">Username field is required</label>' +
                '<input id="txtOrgUser" name="txtOrgUser" type="text" size="30" aria-required="true" style="display:none"/>' +
                '</td> ' +
                '<td>&nbsp;</td> ' +
                '<td>&nbsp;</td>' +
                '</tr>' +
                '<tr>' +
                '<td><span id="labelPassword"></span> </td> ' +
                '<td>' +
                '<input id="txtPassword" name="txtPassword" type="password" size="30" aria-required="true" class="error"/> ' +
                '<label id="errorPassword" for="txtPassword" class="error" style="display:none">Password field is required</label> ' +
                '</td>' +
                '<td>&nbsp;</td> ' +
                '<td>&nbsp;</td>' +
                '</tr>' +
                '<tr>' +
                '<td>Roles </td> ' +
                '<td>' +
                '<div id="selectRole" class="roleBox">' +
                rolehtml() +
                '</div>  ' +
                '<div id="errorRole" class="roleerror" style="display:none">Role cannot be set to empty</div>' +
                '</td> ' +
                '<td>&nbsp;</td>' +
                '<td>&nbsp;</td>' +
                '</tr>' +
                '</tbody>' +
                '</table>';
            $('#addUserWrapper').html(content);


            $("#frmAddUser").validate({
                rules: {
                    txtUser: adminValidationRules.userNameRule,
                    txtPassword: adminValidationRules.passwordRule
                },
                messages: {
                    txtUser: adminValidationRules.userNameMessage,
                    txtPassword: adminValidationRules.passwordMessage
                }
            });
        },
        afterOpen: function () {
            var popup = $(this)[0];
            var roleCheckBox = $("#selectRole > label > input[type='checkbox']");
            $("#errorRole").hide();
            roleCheckBox.iCheck({
                checkboxClass: 'icheckbox_square-aero customCheckbox',
            })
            if (editUserState == -1) {
                $('#labelPassword').html('Password');
                $('#addUserHeader').html('Add User');
            } else {
                $('#labelPassword').html('New Password');
                $('#addUserHeader').html('Edit User');
                $('#txtUser').val($('#addUserInnerPopup').data('username'));
                $('#txtOrgUser').val($('#addUserInnerPopup').data('username'));
                orguser = $('#addUserInnerPopup').data('username');
                var roles = $('#addUserInnerPopup').data('role').split(",");
                UserRoles = roles.map(roles => roles.toLowerCase())
                roles = voltDbRenderer.usersRoles;
                for (var i = 0; i < UserRoles.length; i++) {
                    if (roles.includes(UserRoles[i]), UserRoles[i]) {
                        var selector = "#" + UserRoles[i];
                        $(selector).iCheck("check");
                    }
                }
            }

            $("#btnSaveUser").unbind("click");
            $("#btnSaveUser").on("click", function (e) {
                $("#errorRole").hide();
                if (!$("#frmAddUser").valid()) {
                    if (!roleCheckBox.is(":checked")) $("#errorRole").show();
                    else $("#errorRole").hide();
                    e.preventDefault();
                    e.stopPropagation();
                } else {
                    if (!roleCheckBox.is(":checked")) {
                        e.preventDefault();
                        e.stopPropagation();
                        $("#errorRole").show();
                    } else {
                        $("#errorRole").hide();
                        $("#userSaveDelete").data('status', 'save');
                        $("#userSaveDelete").html("save");
                        $("#addUserControl").hide();
                        $("#deleteSecUser").hide();
                        $("#saveUserControl").show();
                    }
                }
            });

            $("#btnCancelUser").unbind("click");
            $("#btnCancelUser").on("click", function (e) {
                popup.close();
            });

            $(document).off("click", "#btnSaveSecUser");
            $(document).on("click", "#btnSaveSecUser", function () {
                var role = [];
                $("#selectRole > label > div > input[type='checkbox']:checked").each(function () {
                    role.push(this.value)
                });
                var username = $('#txtOrgUser').val();
                var newUsername = $('#txtUser').val();
                var password = encodeURIComponent($('#txtPassword').val());
                var requestType = "POST";
                var requestUser = "";
                popup.close();
                if ($("#userSaveDelete").data('status') == 'save') {
                    var userObject = {
                        "name": newUsername,
                        "roles": role.join(),
                        "password": password,
                        "plaintext": true
                    };
                    if (editUserState == 1) {
                        requestUser = username;
                    } else {
                        requestUser = newUsername;
                        requestType = "PUT";
                    }
                    toggleSecurityEdit(editStates.ShowLoading);
                    voltDbRenderer.UpdateUserConfiguration(userObject, function (result) {
                        if (result.status == "1") {
                            toggleSecurityEdit(editStates.ShowEdit);
                            //Reload Admin configurations for displaying the updated value
                            voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                                VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            });
                        } else {
                            setTimeout(function () {
                                toggleSecurityEdit(editStates.ShowEdit);
                                var errorStatus = '';
                                if (editUserState == 1) {
                                    errorStatus = 'Could not update the user credentials. ';
                                } else {
                                    errorStatus = 'Could not add a new user. ';
                                }
                                var msg = errorStatus;
                                if (result.status == "-1" && result.statusstring == "Query timeout.") {
                                    msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                                }
                                else if (result.statusstring != "") {
                                    msg += " " + result.statusstring;
                                }
                                else {
                                    msg += "Please try again later.";
                                }

                                $('#securityUserErrorFieldMsg').html(msg);
                                $("#sercurityUserPopupLink").trigger("click");
                            }, 3000);
                        }
                    }, requestUser, requestType);
                    popup.close();
                } else if ($("#userSaveDelete").data('status') == 'delete') {
                    toggleSecurityEdit(editStates.ShowLoading);
                    voltDbRenderer.UpdateUserConfiguration(null, function (result) {
                        if (!result.status) { //Handle the condition when the user deletes himself.
                            toggleSecurityEdit(editStates.ShowEdit);
                            return;
                        }

                        if (result.status == "1") {
                            toggleSecurityEdit(editStates.ShowEdit);
                            //Reload Admin configurations for displaying the updated value
                            voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                                VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            });
                        } else {
                            setTimeout(function () {

                                toggleSecurityEdit(editStates.ShowEdit);
                                var errorStatus = 'Could not delete the user.';

                                var msg = errorStatus;
                                if (result.status == "-1" && result.statusstring == "Query timeout.") {
                                    msg += "The Database is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                                } else if (result.statusstring != "") {
                                    msg += " " + result.statusstring;
                                } else {
                                    msg += "Please try again later.";
                                }

                                $('#securityUserErrorFieldMsg').html(msg);
                                $("#sercurityUserPopupLink").trigger("click");
                            }, 3000);
                        }
                    }, username, "DELETE");
                }
            });

            $("#btnCancelSaveSecUser").unbind("click");
            $("#btnCancelSaveSecUser").on("click", function () {
                $("#addUserControl").show();
                $("#deleteSecUser").show();
                $("#saveUserControl").hide();
            });

            $("#deleteSecUser").unbind("click");
            $("#deleteSecUser").on("click", function () {
                $("#userSaveDelete").data('status', 'delete');
                $("#userSaveDelete").html("delete");
                $("#addUserControl").hide();
                $("#deleteSecUser").hide();
                $("#saveUserControl").show();
            });
        },
    });

    $("#updateErrorPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            var popup = $(this)[0];
            $("#btnUpdateErrorOk").unbind("click");
            $("#btnUpdateErrorOk").on("click", function () {

                //Close the popup
                popup.close();
            });
        }
    });

    $("#updateImportErrorPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnImportUpdateErrorOk").unbind("click");
            $("#btnImportUpdateErrorOk").on("click", function () {
                //Close the popup
                popup.close();
            });
        }
    });


    $("#sercurityUserPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            var popup = $(this)[0];
            $("#btnSecurityUserErrorOk").unbind("click");
            $("#btnSecurityUserErrorOk").on("click", function () {

                //Close the popup
                popup.close();
            });
        }
    });

    $("#queryTimeoutUpdateErrorPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            var popup = $(this)[0];
            $("#btnQueryTimeoutUpdateErrorOk").unbind("click");
            $("#btnQueryTimeoutUpdateErrorOk").on("click", function () {

                //Close the popup
                popup.close();
            });
        }
    });

    $("#memorySizeUpdateErrorPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnMemorySizeUpdateErrorOk").unbind("click");
            $("#btnMemorySizeUpdateErrorOk").on("click", function () {
                popup.close();
            });
        }
    });

    $("#updateErrorSnapshotPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnUpdateSnapshotErrorOk").unbind("click");
            $("#btnUpdateSnapshotErrorOk").on("click", function () {
                popup.close();
            });
        }
    });

    //Start DR Master
    var toggleDrMasterEdit = function (state) {

        if (adminEditObjects.chkDrMasterValue) {
            adminEditObjects.chkDrMaster.iCheck('check');
        } else {
            adminEditObjects.chkDrMaster.iCheck('uncheck');
        }
        VoltDbAdminConfig.isDrMasterEditMode = false;

        if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingDrMaster.hide();
            adminEditObjects.LinkDrMasterEdit.hide();
            adminEditObjects.btnEditDrMasterOk.show();
            adminEditObjects.btnEditDrMasterCancel.show();
            adminEditObjects.chkDrMaster.parent().addClass("customCheckbox");
            adminEditObjects.iconDrMasterOption.hide();
            adminEditObjects.txtDrMaster.show();
            VoltDbAdminConfig.isDrMasterEditMode = true;
        } else if (state == editStates.ShowLoading) {
            adminEditObjects.loadingDrMaster.show();
            adminEditObjects.btnEditDrMasterOk.hide();
            adminEditObjects.btnEditDrMasterCancel.hide();
            adminEditObjects.LinkDrMasterEdit.hide();
            adminEditObjects.chkDrMaster.parent().removeClass("customCheckbox");
            adminEditObjects.iconDrMasterOption.hide();
            adminEditObjects.txtDrMaster.hide();
        } else {
            adminEditObjects.loadingDrMaster.hide();
            adminEditObjects.btnEditDrMasterOk.hide();
            adminEditObjects.btnEditDrMasterCancel.hide();
            adminEditObjects.LinkDrMasterEdit.show();
            adminEditObjects.chkDrMaster.parent().removeClass("customCheckbox");
            adminEditObjects.iconDrMasterOption.show();
            adminEditObjects.txtDrMaster.show();
        }

    };

    adminEditObjects.LinkDrMasterEdit.on("click", function () {
        if (adminEditObjects.LinkDrMasterEdit.hasClass("edit"))
            toggleDrMasterEdit(editStates.ShowOkCancel);
    });

    adminEditObjects.btnEditDrMasterCancel.on("click", function () {
        toggleDrMasterEdit(editStates.ShowEdit);
    });

    adminEditObjects.btnEditDrMasterOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnSaveDrMaster").unbind("click");
            $("#btnSaveDrMaster").on("click", function () {
                var adminConfigurations = VoltDbAdminConfig.getLatestRawAdminConfigurations();
                if (!adminConfigurations.hasOwnProperty("dr")) {
                    adminConfigurations.dr = {};
                }
                //Set the new value to be saved.
                adminConfigurations.dr.listen = adminEditObjects.chkDrMaster.is(':checked');
                //Call the loading image only after setting the new value to be saved.
                toggleDrMasterEdit(editStates.ShowLoading);
                voltDbRenderer.updateAdminConfiguration(adminConfigurations, function (result) {
                    if (result.status == "1") {
                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleDrMasterEdit(editStates.ShowEdit);
                        });
                    } else {
                        toggleDrMasterEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.drMasterLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The DB Monitor service is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.updateDrMasterErrorFieldMsg.text(msg);
                        $("#updateErrorDrMasterPopupLink").trigger("click");
                    }
                });
                //Close the popup
                popup.close();
            });

            $("#btnPopupDrMasterCancel").on("click", function () {
                toggleDrMasterEdit(editStates.ShowEdit);
                popup.close();
            });

            $(".popup_back").on("click", function () {
                toggleDrMasterEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleDrMasterEdit(editStates.ShowEdit);
            });
        }
    });

    $("#updateErrorDrMasterPopupLink").popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnUpdateDrMasterOk").unbind("click");
            $("#btnUpdateDrMasterOk").on("click", function () {
                popup.close();
            });
        }
    });

    adminEditObjects.chkDrMaster.on('ifChanged', function () {
        adminEditObjects.txtDrMaster.text(getOnOffText(adminEditObjects.chkDrMaster.is(":checked")));
    });

    adminEditObjects.chkDrMaster.iCheck({
        checkboxClass: 'icheckbox_square-aero',
        increaseArea: '20%' // optional
    });
    //End DR Master

    // Filters servers list
    $('#popServerSearchAdmin').keyup(function () {
        var that = this;
        $.each($('.tblshutdown tbody tr'),
            function (i, val) {
                if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                    $('.tblshutdown tbody tr').eq(i).hide();
                } else {
                    $('.tblshutdown tbody tr').eq(i).show();
                }
            });
    });

    // Hides opened serverlist
    $(document).on('click', function (e) {
        if (!$(e.target).hasClass('adminIcons') && !$(e.target).hasClass('serverName')) {
            if ($(e.target).closest("#serverConfigAdmin").length === 0) {
                $("#serverConfigAdmin").hide();
            }
        }
    });

    // Checkbox style
    $('input.snapshot').iCheck({
        checkboxClass: 'icheckbox_square-aero',
        increaseArea: '20%' // optional
    });

    $('#chkSecurity').iCheck({
        checkboxClass: 'icheckbox_square-aero',
        increaseArea: '20%' // optional
    });

    $.validator.addMethod(
        "regex",
        function (value, element, regexp) {
            var re = new RegExp(regexp);
            return this.optional(element) || re.test(value);
        },
        "Please enter only valid characters."
    );

    $.validator.addMethod(
        "checkDuplicate",
        function (value) {
            var arr = VoltDbAdminConfig.orgUserList;
            if (editUserState == 1) {
                if ($.inArray(value, arr) != -1) {
                    if (value == orguser)
                        return true;
                    return false;
                } else {
                    return true;
                }
            } else {
                if ($.inArray(value, arr) != -1) {
                    return false;
                } else {
                    return true;
                }
            }

        },
        "Username already exists."
    );

    $.validator.addMethod(
        "portRegex",
        function (value, element, regexp) {
            var result = true
            var values = value.split(':');
            var re = new RegExp(regexp);
            if (values.length == 2) {
                if (!$.isNumeric(values[1]) || !(values[1] > 1 && values[1] < 65536))
                    result = false;
                else {
                    if (values[1].split('.').length > 1)
                        result = false;
                }
                if (!re.test(values[0]))
                    result = false;
            } else {
                result = false;
            }

            return this.optional(element) || result;
        },
        "Please enter only valid character."
    );

    showHideIntSnapshotMsg = function (isProVersion) {
        if (!VoltDbAdminConfig.isCommandLogEnabled && isProVersion) {
            $('#continueShutdownMsg').show()
            $('#shutdownWarningMsg').show()
        } else {
            $('#shutdownWarningMsg').hide()
            $('#continueShutdownMsg').hide()
        }
    }
}

(function (window) {
    var iVoltDbAdminConfig = (function () {

        var currentRawAdminConfigurations;
        this.isExportLoading = false;
        this.isCommandLogEnabled = false;
        this.isAdmin = false;
        this.isSecurityEnabled = false;
        this.isRoleChanged = false;
        this.isReloadRequired = false;
        this.registeredElements = [];
        this.servers = [];
        this.stoppedServer = "";
        this.stoppedServers = [];
        this.runningServerIds = "";
        this.firstResponseReceived = false;
        this.adminPort = -1;
        this.isDbPaused = false;
        this.toggleStates = {};
        this.orgUserList = [];
        this.drReplicaEnabled = true;
        this.isDrMasterEditMode = false;
        this.isSnapshotEditMode = false;
        this.isSnmpEditMode = false;
        this.isMemoryLimitEditMode = false;
        this.newStreamMinmPropertyName = {
            "outdir": "#txtOutdirValue",
            "nonce": "#txtnonceValue",
            "type": "#txtFileTypeValue",
            "endpoint_HTTP": "#txtEndpointValue",
            "bootstrap.servers": "#txtBootstrapServersListValue",
            "jdbcurl": "#txtJdbcUrlValue",
            "jdbcdriver": "#txtJdbcDriverValue",
            "endpoint_ELASTICSEARCH": "#txtEndpointESValue"
        };

        this.newImportStreamMinPropertyName = {
            "metadata.broker.list": "#txtImportMetadataBrokerListValue",
            "brokers": "#txtBrokersValue",
            "procedure_KAFKA": "#txtProcedureValue",
            "topics": "#txtTopicsValue",
            "app.name": "#txtAppNameValue",
            "procedure_KINESIS": "#txtProcedureKiValue",
            "region": "#txtRegionValue",
            "stream.name": "#txtStreamNameValue",
            "access.key": "#txtAccessKeyValue",
            "secret.key": "#txtSecretKeyValue",
        }
        this.isImportConfigLoading = false;
        this.orgTypeValue = "";
        this.exportTypes = [];
        this.isSaveSnapshot = false;

        this.server = function (hostIdvalue, serverNameValue, serverStateValue, ipAddress, HTTPPORT, ClientPort) {
            this.hostId = hostIdvalue;
            this.serverName = serverNameValue;
            this.serverState = serverStateValue;
            this.ipAddress = ipAddress;
            this.httpPort = HTTPPORT;
            this.clientPort = ClientPort;
        };

        this.stoppedServer = function (hostIdvalue, serverNameValue) {
            this.HOSTID = hostIdvalue;
            this.HOSTNAME = serverNameValue;
            this.CLUSTERSTATE = "MISSING";

        };

        this.displayAdminConfiguration = function (adminConfigValues, rawConfigValues) {
            if (!VoltDbAdminConfig.firstResponseReceived)
                VoltDbAdminConfig.firstResponseReceived = true;
            if (adminConfigValues != undefined && VoltDbAdminConfig.isAdmin) {
                configureAdminValues(adminConfigValues);
                configureDirectoryValues(adminConfigValues);
                currentRawAdminConfigurations = rawConfigValues;
            }
        };

        this.getLatestRawAdminConfigurations = function () {
            return currentRawAdminConfigurations;
        };

        this.displayPortAndRefreshClusterState = function (portAndClusterValues, serverSettings) {
            if (portAndClusterValues != undefined && VoltDbAdminConfig.isAdmin) {
                configurePortAndOverviewValues(portAndClusterValues, serverSettings);
                client_port = portAndClusterValues.clientPort;
                refreshClusterValues(portAndClusterValues);
                configurePromoteAction(portAndClusterValues);
            }
        };

        this.refreshServerList = function (serverList, serverCount) {
            adminDOMObjects.adminServerList !== undefined && adminDOMObjects.adminServerList.html(serverList);
        };

        this.escapeHtml = function (value) {
            if (!value)
                return "";

            return $('<div/>').text(value).html();
        };

        var configureAdminValues = function (adminConfigValues) {
            adminDOMObjects.siteNumberHeader.text(adminConfigValues.sitesperhost);
            adminDOMObjects.kSafety.text(adminConfigValues.kSafety);
            adminDOMObjects.partitionDetection.removeClass().addClass(getOnOffClass(adminConfigValues.partitionDetection));
            adminDOMObjects.partitionDetectionLabel.text(getOnOffText(adminConfigValues.partitionDetection));
            configureSecurity(adminConfigValues);
            adminDOMObjects.httpAccess.removeClass().addClass(getOnOffClass(adminConfigValues.httpEnabled));
            adminDOMObjects.httpAccessLabel.text(getOnOffText(adminConfigValues.httpEnabled));
            adminDOMObjects.jsonAPI.removeClass().addClass(getOnOffClass(adminConfigValues.jsonEnabled));
            adminDOMObjects.jsonAPILabel.text(getOnOffText(adminConfigValues.jsonEnabled));
            adminDOMObjects.autoSnapshot.removeClass().addClass(getOnOffClass(adminConfigValues.snapshotEnabled));
            if (!VoltDbAdminConfig.isSnapshotEditMode)
                adminDOMObjects.autoSnapshotLabel.text(getOnOffText(adminConfigValues.snapshotEnabled));
            adminDOMObjects.filePrefix.text(adminConfigValues.filePrefix != null ? adminConfigValues.filePrefix : "");
            adminDOMObjects.frequency.text(adminConfigValues.frequency != null ? adminConfigValues.frequency : "");
            adminDOMObjects.frequencyLabel.text(adminConfigValues.frequency != null ? "Hrs" : "");
            adminDOMObjects.retained.text(adminConfigValues.retained != null ? adminConfigValues.retained : "");
            adminDOMObjects.retainedLabel.text(adminConfigValues.retained != null ? "Copies" : "");
            adminEditObjects.tBoxAutoSnapshotRetainedValue = adminConfigValues.retained;
            adminEditObjects.tBoxFilePrefixValue = adminConfigValues.filePrefix;
            VoltDbAdminConfig.isCommandLogEnabled = adminConfigValues.commandLogEnabled;
            adminDOMObjects.commandLog.removeClass().addClass(getOnOffClass(adminConfigValues.commandLogEnabled));
            adminDOMObjects.commandLogLabel.text(adminConfigValues.commandLogEnabled == true ? 'On' : 'Off');
            adminDOMObjects.commandLogFrequencyTime.text(adminConfigValues.commandLogFrequencyTime != null ? adminConfigValues.commandLogFrequencyTime : "");
            adminDOMObjects.commandLogFrequencyTimeLabel.text(adminConfigValues.commandLogFrequencyTime != null ? "ms" : "");
            adminDOMObjects.commandLogFrequencyTransactions.text(adminConfigValues.commandLogFrequencyTransactions != null ? adminConfigValues.commandLogFrequencyTransactions : "");
            adminDOMObjects.commandLogSegmentSize.text(adminConfigValues.logSegmentSize != null ? adminConfigValues.logSegmentSize : "");
            adminDOMObjects.commandLogSegmentSizeLabel.text(adminConfigValues.logSegmentSize != null ? "MB" : "");
            adminDOMObjects.heartBeatTimeout.text(adminConfigValues.heartBeatTimeout != null ? adminConfigValues.heartBeatTimeout : "");


            if (adminConfigValues.heartBeatTimeout != null) {
                adminDOMObjects.heartBeatTimeoutLabel.text("s");

                if (adminEditObjects.editStateHeartbeatTimeout == editStates.ShowEdit)
                    adminEditObjects.LinkHeartbeatEdit.show();
            } else {
                adminDOMObjects.heartBeatTimeoutLabel.text("");
                adminEditObjects.LinkHeartbeatEdit.hide();
            }

            adminDOMObjects.tempTablesMaxSize.text(adminConfigValues.tempTablesMaxSize != null ? adminConfigValues.tempTablesMaxSize : "");
            adminDOMObjects.tempTablesMaxSizeLabel.text(adminConfigValues.tempTablesMaxSize != null ? "MB" : "");
            adminDOMObjects.snapshotPriority.text(adminConfigValues.snapshotPriority);
            var memoryLimitText = adminConfigValues.memorylimit;
            var memoryLimitUnit = "GB";
            var memoryLimitValue = 0;
            if (memoryLimitText != undefined && memoryLimitText.indexOf("%") > -1) {
                memoryLimitUnit = "%";
                memoryLimitValue = memoryLimitText.replace("%", "");
                if (adminEditObjects.btnEditMemorySize.is(":visible")) {
                    adminEditObjects.btnDeleteMemory.show();
                }
            } else if (memoryLimitText != undefined && memoryLimitText.indexOf("%") == -1) {
                memoryLimitValue = memoryLimitText;
                if (adminEditObjects.btnEditMemorySize.is(":visible")) {
                    adminEditObjects.btnDeleteMemory.show();
                }
            } else if (memoryLimitText == undefined) {
                memoryLimitValue = undefined;
                adminEditObjects.btnDeleteMemory.hide();
            }
            adminDOMObjects.memoryLimitSize.text(adminConfigValues.memorylimit != undefined ? memoryLimitValue : "Not Enforced");
            if (!VoltDbAdminConfig.isMemoryLimitEditMode)
                adminDOMObjects.memoryLimitSizeUnit.text(adminConfigValues.memorylimit != undefined ? memoryLimitUnit : "");
            adminEditObjects.spanMemoryLimitSizeValue = memoryLimitValue;
            configureQueryTimeout(adminConfigValues);

            //edit configuration
            adminEditObjects.chkSecurityValue = adminConfigValues.securityEnabled;
            adminEditObjects.chkAutoSnapshotValue = adminConfigValues.snapshotEnabled;
            adminEditObjects.tBoxHeartbeatTimeoutValue = adminConfigValues.heartBeatTimeout;
            var snapshotFrequency = adminConfigValues.frequency != undefined ? parseInt(adminConfigValues.frequency) : '';
            adminEditObjects.tBoxAutoSnapshotFreqValue = snapshotFrequency;
            adminEditObjects.spanAutoSnapshotFreq.text(snapshotFrequency);
            var spanshotUnit = adminConfigValues.frequency != undefined ? adminConfigValues.frequency.slice(-1) : '';
            setSnapShotUnit(spanshotUnit);
            adminClusterObjects.userListObj = adminConfigValues.users;
            getUserList(adminConfigValues.users);
            getExportProperties(adminConfigValues.configuration);
            //dr setting
            getDrMode(adminConfigValues.drListen);

            getImportProperties(adminConfigValues.importConfiguration);

            getDiskLimits(adminConfigValues.disklimit);
            if (VoltDbUI.isDRInfoRequired) {
                adminEditObjects.labelDrId.text(adminConfigValues.drId);
                adminEditObjects.chkDrMasterValue = adminConfigValues.drListen;
                adminEditObjects.iconDrMasterOption.removeClass().addClass(getOnOffClass(adminConfigValues.drListen));
                if (!VoltDbAdminConfig.isDrMasterEditMode)
                    adminEditObjects.txtDrMaster.text(getOnOffText(adminConfigValues.drListen));
                adminEditObjects.labelReplicaSource.text(adminConfigValues.drConnectionSource == "" ? "" : "(source: " + adminConfigValues.drConnectionSource + ")");
                if (VoltDbUI.drReplicationRole.toLowerCase() == "replica") {
                    getDrReplicaStatus(true);
                } else {
                    getDrReplicaStatus(false);
                }
            }



            //snmp setting

            adminEditObjects.chkSnmpValue = adminConfigValues.enabled;
            if (!VoltDbAdminConfig.isSnmpEditMode)
                adminEditObjects.txtSnmp.text(adminConfigValues.enabled == true ? 'On' : 'Off');

            if (adminConfigValues.enabled != null) {
                adminEditObjects.iconSnmpOption.removeClass().addClass(getOnOffClass(adminConfigValues.enabled));
            }

            if (adminConfigValues.target != null) {
                adminEditObjects.targetSpan.text(adminConfigValues.target);
                adminEditObjects.txtTargetValue = adminConfigValues.target;
            }

            if (adminConfigValues.community != null) {
                adminEditObjects.communitySpan.text(adminConfigValues.community);
                adminEditObjects.txtCommunityValue = adminConfigValues.community;
            }

            if (adminConfigValues.username != null) {
                adminEditObjects.usernameSpan.text(adminConfigValues.username);
                adminEditObjects.txtUsernameValue = adminConfigValues.username;
            }

            if (adminConfigValues.authprotocol != null) {
                adminEditObjects.authProtocolSpan.text(adminConfigValues.authprotocol)
                adminEditObjects.ddlAuthProtocolValue = adminConfigValues.authprotocol;
            }
            else {
                adminEditObjects.ddlAuthProtocolValue = "SHA";
            }

            if (adminConfigValues.privacyprotocol != null) {
                adminEditObjects.privProtocolSpan.text(adminConfigValues.privacyprotocol)
                adminEditObjects.ddlPrivProtocolValue = adminConfigValues.privacyprotocol;
            }
            else {
                adminEditObjects.ddlPrivProtocolValue = "AES";
            }

            if (adminConfigValues.authkey != null) {
                adminEditObjects.authKeySpan.text(adminConfigValues.authkey);
                adminEditObjects.txtAuthkeyValue = adminConfigValues.authkey;
            }

            if (adminConfigValues.privacykey != null) {
                adminEditObjects.privKeySpan.text(adminConfigValues.privacykey);
                adminEditObjects.txtPrivkeyValue = adminConfigValues.privacykey;
            }



        };

        var getDrReplicaStatus = function (result) {
            adminEditObjects.chkDrReplicaValue = result;
            adminEditObjects.iconDrReplicaOption.removeClass().addClass(getOnOffClass(result));
            adminEditObjects.txtDrReplica.text(getOnOffText(result));
            if (result) {
                adminEditObjects.txtDrReplica.attr("title", "Use the Promote button to exit replica mode.");
                adminEditObjects.iconDrReplicaOption.attr("title", "Use the Promote button to exit replica mode.");
            } else {
                adminEditObjects.txtDrReplica.removeAttr("title");
                adminEditObjects.iconDrReplicaOption.removeAttr("title");
            }
        };

        var getDrMode = function (drListen) {
            var replicationRole = VoltDbUI.drReplicationRole;
            if (replicationRole.toLowerCase() == "replica") {
                if (VoltDbUI.drMasterState.toUpperCase() == "ACTIVE") {
                    adminEditObjects.labelDrmode.text("Both");
                } else {
                    adminEditObjects.labelDrmode.text("Replica");
                }
            } else {
                adminEditObjects.labelDrmode.text("Master");
            }
            adminEditObjects.LinkDrMasterEdit.removeClass().addClass('edit');
        };

        var getExportProperties = function (data) {
            var result = "";
            if (data != undefined) {

                //Do not update the data in loading condition
                if (adminEditObjects.exportConfiguration.data("status") == "loading") {
                    return;
                }
                for (var i = 0; i < data.length; i++) {
                    var stream = VoltDbAdminConfig.escapeHtml(data[i].target);
                    var type = data[i].type ? (" (" + VoltDbAdminConfig.escapeHtml(data[i].type) + ")") : "";
                    var enabled = data[i].enabled;
                    var streamProperty = data[i].property;
                    var rowId = 'row-4' + i;
                    var style = '';
                    var additionalCss = (VoltDbAdminConfig.toggleStates[rowId] === true) ? 'labelExpanded' : '';

                    if (!VoltDbAdminConfig.toggleStates.hasOwnProperty(rowId) || VoltDbAdminConfig.toggleStates[rowId] === false) {
                        VoltDbAdminConfig.toggleStates[rowId] = false;
                        style = 'style = "display:none;"';
                    }

                    result += '<tr class="child-row-4 subLabelRow parentprop" id="' + rowId + '">' +
                        '   <td class="configLabel expoStream" onclick="toggleProperties(this);" title="Click to expand/collapse">' +
                        '       <a href="javascript:void(0)" class="labelCollapsed ' + additionalCss + '"> ' + stream + type + '</a>' +
                        '   </td>' +
                        '   <td align="right">' +
                        '       <div class="' + getOnOffClass(enabled) + '"></div>' +
                        '   </td>' +
                        '   <td>' + getOnOffText(enabled) + '</td>' +
                        '   <td>' +
                        '       <div class="exportDelete" style="display:none;"></div>' +
                        '       <a href="javascript:void(0)" id="exportEdit' + i + '" class="edit k8s_hidden" onclick="editStream(' + i + ')" title="Edit">&nbsp;</a>' +
                        '   </td>' +
                        '</tr>';

                    if (streamProperty && streamProperty.length > 0) {
                        var isBootstrapServer = false;
                        if (data[i].type.toLowerCase() == 'kafka') {
                            for (var k = 0; k < streamProperty.length; k++) {
                                if (streamProperty[k].name == 'bootstrap.servers') {
                                    isBootstrapServer = true;
                                    break;
                                }
                            }
                        }

                        for (var j = 0; j < streamProperty.length; j++) {
                            if (streamProperty[j].name == 'metadata.broker.list' && !isBootstrapServer) {
                                streamProperty[j].name = 'bootstrap.servers';
                                isBootstrapServer = true;
                            }

                            var name = streamProperty[j].name;
                            var value = streamProperty[j].value;

                            result += '' +
                                '<tr class="childprop-' + rowId + ' subLabelRow" ' + style + '>' +
                                '   <td class="configLabe2">' + name + '</td>' +
                                '   <td class="wordBreak" align="right">' + value + '</td>' +
                                '<td>&nbsp;</td>' +
                                '<td>&nbsp;</td>' +
                                '</tr>';
                        }
                    } else {
                        result += '<tr class="childprop-' + rowId + ' propertyLast subLabelRow" ' + style + '>' +
                            '   <td width="67%" class="configLabe2" colspan="3">No properties available.</td>' +
                            '   <td width="33%">&nbsp</td>' +
                            '</tr>';
                    }
                }
            }

            if (result == "") {
                result += '<tr class="propertyLast subLabelRow">' +
                    '<td width="67%" class="configLabel" colspan="3" id="noConfigExport">No configuration available.</td>' +
                    '<td width="33%">&nbsp</td>' +
                    '</tr>';
            }

            $('#exportConfiguration').html(result);

        };

        var getImportProperties = function (data) {
            var result = "";
            var procedureName = "";
            if (data != undefined) {
                //Do not update the data in loading condition
                if (adminEditObjects.importConfiguration.data("status") == "loading") {
                    return;
                }

                for (var i = 0; i < data.length; i++) {
                    var resultProperty = "";
                    var resultSubProperty = "";
                    var type = data[i].type ? VoltDbAdminConfig.escapeHtml(data[i].type) : "";
                    var enabled = data[i].enabled;
                    var importProperty = data[i].property;
                    var rowId = 'row-5' + i;
                    var style = '';
                    var additionalCss = (VoltDbAdminConfig.toggleStates[rowId] === true) ? 'labelExpanded' : '';

                    if (!VoltDbAdminConfig.toggleStates.hasOwnProperty(rowId) || VoltDbAdminConfig.toggleStates[rowId] === false) {
                        VoltDbAdminConfig.toggleStates[rowId] = false;
                        style = 'style = "display:none;"';
                    }
                    if (importProperty && importProperty.length > 0) {
                        var isFirstProcedureProp = true;
                        for (var j = 0; j < importProperty.length; j++) {
                            var name = importProperty[j].name;
                            var value = importProperty[j].value;

                            resultSubProperty += '' +
                                '<tr class="childprop-' + rowId + ' subLabelRow" ' + style + '>' +
                                '   <td class="configLabe2">' + name + '</td>' +
                                '   <td class="wordBreak" align="right">' + value + '</td>' +
                                '<td>&nbsp;</td>' +
                                '<td>&nbsp;</td>' +
                                '</tr>';

                            if (name == 'procedure' && isFirstProcedureProp) {
                                isFirstProcedureProp = false;
                                procedureName = value;
                            }
                        }
                    } else {
                        resultSubProperty += '<tr class="childprop-' + rowId + ' propertyLast subLabelRow" ' + style + '>' +
                            '   <td width="67%" class="configLabe2" colspan="3">No properties available.</td>' +
                            '   <td width="33%">&nbsp</td>' +
                            '</tr>';
                    }

                    resultProperty += '<tr class="child-row-5 subLabelRow parentprop" id="' + rowId + '">' +
                        '   <td class="configLabel expoStream" onclick="toggleProperties(this);" title="Click to expand/collapse">' +
                        '       <a href="javascript:void(0)" class="labelCollapsed ' + additionalCss + '"> ' + procedureName + ' (' + type + ')</a>' +
                        '   </td>' +
                        '   <td align="right">' +
                        '       <div class="' + getOnOffClass(enabled) + '"></div>' +
                        '   </td>' +
                        '   <td>' + getOnOffText(enabled) + '</td>' +
                        '   <td>' +
                        '       <div class="exportDelete" style="display:none;"></div>' +
                        '       <a href="javascript:void(0)" id="importEdit' + i + '" class="edit k8s_hidden" onclick="editImportStream(' + i + ')" title="Edit">&nbsp;</a>' +
                        '   </td>' +
                        '</tr>';
                    result += resultProperty + resultSubProperty;
                }
            }

            if (result == "") {
                result += '<tr class="propertyLast subLabelRow">' +
                    '<td width="67%" class="configLabel" colspan="3">No configuration available.</td>' +
                    '<td width="33%">&nbsp</td>' +
                    '</tr>';
            }

            $('#importConfiguration').html(result);
        };

        var getDiskLimits = function (data) {
            var result = "";
            var style = '';
            var additionalCss = (VoltDbAdminConfig.toggleStates["row-60"] === true) ? 'labelExpanded' : '';
            if (!VoltDbAdminConfig.toggleStates.hasOwnProperty("row-60") || VoltDbAdminConfig.toggleStates["row-60"] === false) {
                VoltDbAdminConfig.toggleStates["row-60"] = false;
                style = 'style = "display:none;"';
            }
            if (data != undefined) {
                //Do not update the data in loading condition
                if (adminEditObjects.diskLimitConfiguration.data("status") == "loading") {
                    return;
                }

                var content = '';

                content = '<a id="btnEditDiskLimit" href="javascript:void(0)" onclick="editDiskLimit(1)" class="edit k8s_hidden" title="Edit">&nbsp;</a>' +
                        '<div id="loadingDiskLimit" class="loading-small loadExport" style="display: none;"></div>';

                result += '<tr class="child-row-6 subLabelRow parentprop" id="row-60">' +
                    '   <td class="configLabel" id="diskLimit" onclick="toggleProperties(this);" title="Click to expand/collapse" style="cursor: pointer;">' +
                    '   <a href="javascript:void(0)" class="labelCollapsed ' + additionalCss + '">  Disk Limit</a>  ' +
                    '   </td>' +
                    '   <td align="right"></td>' +
                    '<td>&nbsp</td>' +
                    '   <td>' + content + '</td>' +
                    '</tr>';

                var diskfeature = data.feature;

                if (diskfeature && diskfeature.length > 0) {
                    for (var j = 0; j < diskfeature.length; j++) {
                        var name = diskfeature[j].name;
                        var value = diskfeature[j].size;
                        var unit = "";
                        if (value.indexOf("%") == -1) {
                            unit = "GB";
                        }
                        result += '' +
                            '<tr class="childprop-row-60 subLabelRow" ' + style + '>' +
                            '   <td class="configLabe2">' + name + '</td>' +
                            '   <td class="wordBreak" colspan="3">' + value + ' ' + unit + '</td>' +
                            '</tr>';
                    }
                } else {
                    result += '<tr class="childprop-row-60 subLabelRow">' +
                        '<td width="67%" class="configLabel" colspan="3">No features enabled for disk space limit.</td>' +
                        '<td width="33%">&nbsp</td>' +
                        '</tr>';
                }
            }

            if (result == "") {
                result += '<tr class="child-row-6 subLabelRow parentprop" id="row-60">' +
                    '   <td id="diskLimit" class="configLabel" onclick="toggleProperties(this);" title="Click to expand/collapse" style="cursor: pointer">' +
                    '   <a href="javascript:void(0)" class="labelCollapsed ' + additionalCss + '"" ;">Disk Limit</a>  ' +
                    '   </td>' +
                    '   <td align="right">' +
                    '   </td>' +
                    '<td>&nbsp</td>';
                result += '   <td><a id="btnEditDiskLimit" href="javascript:void(0)" onclick="editDiskLimit(1)" class="edit k8s_hidden" title="Edit">&nbsp;</a>' +
                        '<div id="loadingDiskLimit" class="loading-small loadExport" style="display: none;"></div></td>';
                result += '</tr>' +
                    '<tr class="childprop-row-60 subLabelRow" ' + style + '>' +
                    '<td width="67%" class="configLabel" colspan="3">&nbsp &nbsp &nbsp No features available.</td>' +
                    '<td width="33%">&nbsp</td>' +
                    '</tr>';
            }

            $('#diskLimitConfiguration').html(result);

            $("#addDiskLimitLink").on("click", function () {
                adminDOMObjects.addDiskLimitLink.trigger("click");
            });
        };

        var getUserList = function (userData) {
            var result = "";
            VoltDbAdminConfig.orgUserList = [];
            var tableHeader = '<table width="100%" cellpadding="0" cellspacing="0" class="secTbl">' +
                '<tr>' +
                '<th>Username</th>' +
                '<th>Role</th>';
            tableHeader = tableHeader.concat(
                    '<th>&nbsp</th>' +
                    '<th><a href="#addUserPopup" id="addNewUserLink1" onclick="addUser(-1)" class="plusAdd k8s_hidden" title="Add User">&nbsp;</a></th>'
                )
            tableHeader = tableHeader.concat(
                '</tr>'
            )
            var tableFooter = '</table>';
            if (userData != undefined) {
                for (var i = 0; i < userData.length; i++) {
                    var userName = userData[i].name;
                    var role = userData[i].roles;
                    VoltDbAdminConfig.orgUserList.push(userName);
                    result += '<tr>' +
                        '<td>' + userName + '</td>' +
                        '<td>' + formatDisplayName(role) + '</td>' +
                        '<td>&nbsp</td>';
                    result += '<td><a  href="javascript:void(0)" class="edit k8s_hidden" title="Edit" onclick="addUser(1,\'' + userName + '\',\'' + role + '\');">&nbsp;</a></td>';
                    result += '</tr>';
                }
            }
            $('#UsersList').html(tableHeader + result + tableFooter);
        };

        this.getEditUserList = function (userData) {
            var result = "";
            var tableHeader = '<table id="secTbl" width="100%" cellpadding="0" cellspacing="0" class="secTbl">' +
                '<tr>' +
                '<th>Username</th>' +
                '<th>Password</th>' +
                '<th>Role</th>' +
                '<th>Delete</th>' +
                '</tr>';
            var tableFooter = '</table>';
            var userList = [];
            if (userData != undefined) {
                for (var i = 0; i < userData.length; i++) {
                    var userName = userData[i].name;
                    userList.push(userName);
                    var role = userData[i].roles;
                    result += '<tr class="old_row">' +
                        '<td id="latbox" class="username">' +
                        '<input class="usernametxt" size="15" type="text" value=' + userName + ' id="inputUserName' + i + '" name="inputUserName' + i + '">' +
                        '<label id="errorUserName' + i + '" for="inputUserName' + i + '" class="error errorSecurity" style="display:none"></label>' +
                        '<input class="orgUserName" type="text" value=' + userName + ' style="display:none">' +
                        '</td>' +
                        '<td id="latbox' + i + '" class="password">' +
                        '<a class="changePsd" href ="javascript:void(0)" onclick="changePassword(this);" id="anchor' + i + '" >Change Password</a>' +
                        '<input class="passwordtxt" size="15" type="text" style="display:none" id="input' + i + '" name="input' + i + '">' +
                        '<label id="errorUser' + i + '" for="input' + i + '" class="error errorSecurity" style="display:none"></label>' +
                        '</td>' +
                        '<td id="lngbox" class="roleoption">' +
                        '<select class="roleoptiontxt">';

                    if (role.toLowerCase() == 'administrator') {
                        result += '<option selected="selected">Admin</option>' +
                            '<option>User</option>';
                    } else if (role.toLowerCase() == 'user') {
                        result += '<option selected="selected">User</option>' +
                            '<option>Admin</option>';
                    }
                    result += '</select></td>' +
                        '<td>' +
                        '<div class="securityDelete" id="delPOIbutton" onclick="deleteRow(this)"></div>' +
                        '</td>' +
                        '</tr>';
                }
                $('#editUserList').html(tableHeader + result + tableFooter);
            }
            VoltDbAdminConfig.orgUserList = userList;
        };

        var setSnapShotUnit = function (unit) {
            if (unit == 's') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Sec');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Sec';
            } else if (unit == 'm') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Min');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Min';
            } else if (unit == 'h') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Hrs');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Hrs';
            } else {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = '';
            }
        };

        var configureSecurity = function (adminConfigValues) {
            if (adminEditObjects.editStateSecurity == editStates.ShowEdit) {
                adminDOMObjects.securityLabel.text(getOnOffText(adminConfigValues.securityEnabled));

                if (adminConfigValues.users != null && adminConfigValues.users.length > 0) {
                    adminEditObjects.LinkSecurityEdit.removeClass().addClass('edit');
                    adminEditObjects.securityStateOriginal.linkSecurityEdit = true;
                } else {
                    adminEditObjects.LinkSecurityEdit.removeClass().addClass('editDisabled');
                    adminEditObjects.securityStateOriginal.linkSecurityEdit = false;
                }
            }

            adminEditObjects.securityStateOriginal.SecurityStatus = adminConfigValues.securityEnabled;
            adminDOMObjects.security.removeClass().addClass(getOnOffClass(adminConfigValues.securityEnabled));
            adminEditObjects.chkSecurityValue = adminConfigValues.securityEnabled;

        };

        var configureQueryTimeout = function (adminConfigValues) {

            if (adminConfigValues.queryTimeout == null) {
                adminEditObjects.rowQueryTimeout.hide();

                //Remove the class used to expand/collapse all child rows inside 'Admin'
                if (adminEditObjects.rowQueryTimeout.hasClass("child-row-5")) {
                    adminEditObjects.rowQueryTimeout.removeClass("child-row-5");
                }
            }
            //Expand the Querytimeout row to make it visible, only if its sibling 'Heartbeat Timeout' 
            //is also visible. /Otherwise it is in collapsed form.
            else if (adminEditObjects.rowHeartbeatTimeout.is(":visible")) {
                adminEditObjects.rowQueryTimeout.show();

                //Add the class used to expand/collapse all child rows inside 'Admin'
                if (!adminEditObjects.rowQueryTimeout.hasClass("child-row-5")) {
                    adminEditObjects.rowQueryTimeout.addClass("child-row-5");
                }
            }
            adminDOMObjects.queryTimeout.text(adminConfigValues.queryTimeout != null ? adminConfigValues.queryTimeout : "");
            adminDOMObjects.queryTimeoutLabel.text(adminConfigValues.queryTimeout != null ? "ms" : "");
            adminEditObjects.tBoxQueryTimeoutValue = adminConfigValues.queryTimeout;
        };

        var configurePortAndOverviewValues = function (configValues, serverSettings) {
            VoltDbAdminConfig.adminPort = configValues.adminPort;
            adminDOMObjects.clusterAdminPort !== undefined && adminDOMObjects.clusterAdminPort.text(configValues.adminPort);
            adminDOMObjects.clusterHttpPort !== undefined && adminDOMObjects.clusterHttpPort.text(configValues.httpPort);
            adminDOMObjects.clusterInternalPort !== undefined && adminDOMObjects.clusterInternalPort.text(configValues.internalPort);
            adminDOMObjects.clusterZookeeperPort !== undefined && adminDOMObjects.clusterZookeeperPort.text(configValues.zookeeperPort);
            adminDOMObjects.clusterReplicationPort !== undefined && adminDOMObjects.clusterReplicationPort.text(configValues.replicationPort);
            adminDOMObjects.clusterClientPort !== undefined && adminDOMObjects.clusterClientPort.text(configValues.clientPort);
            adminDOMObjects.maxJavaHeap !== undefined && adminDOMObjects.maxJavaHeap.text((configValues.maxJavaHeap != null && configValues.maxJavaHeap != NaN) ? parseFloat(configValues.maxJavaHeap / 1024) : "");
            adminDOMObjects.maxJavaHeapLabel !== undefined && adminDOMObjects.maxJavaHeapLabel.text((configValues.maxJavaHeap != null && configValues.maxJavaHeap != NaN) ? "MB" : "");

            //if clusterwide settings are present
            if (serverSettings) {
                adminDOMObjects.adminPort.text(configValues.adminInterface);
                adminDOMObjects.httpPort.text(configValues.httpInterface);
                adminDOMObjects.clientPort.text(configValues.clientInterface);
                adminDOMObjects.internalPort.text(configValues.internalInterface);
                adminDOMObjects.zookeeperPort.text(configValues.zookeeperInterface);
                adminDOMObjects.replicationPort.text(configValues.replicationInterface);
                adminDOMObjects.serverSettingHeader.text("Server Settings");
            } else {
                adminDOMObjects.adminPort !== undefined && adminDOMObjects.adminPort.text('');
                adminDOMObjects.httpPort !== undefined && adminDOMObjects.httpPort.text('');
                adminDOMObjects.clientPort !== undefined && adminDOMObjects.clientPort.text('');
                adminDOMObjects.internalPort !== undefined && adminDOMObjects.internalPort.text('');
                adminDOMObjects.zookeeperPort !== undefined && adminDOMObjects.zookeeperPort.text('');
                adminDOMObjects.replicationPort !== undefined && adminDOMObjects.replicationPort.text('');
                adminDOMObjects.serverSettingHeader !== undefined && adminDOMObjects.serverSettingHeader.text('');
            }
        };

        var refreshClusterValues = function (clusterValues) {
            if (clusterValues != undefined && clusterValues.hasOwnProperty('clusterState')) {
                if (clusterValues.clusterState.toLowerCase() == "running") {
                    adminClusterObjects.btnClusterPause.show();
                    adminClusterObjects.btnClusterResume.hide();
                    VoltDbAdminConfig.isDbPaused = false;
                } else if (clusterValues.clusterState.toLowerCase() == "paused") {
                    adminClusterObjects.btnClusterPause.hide();
                    adminClusterObjects.btnClusterResume.show();
                    VoltDbAdminConfig.isDbPaused = true;
                }
            }
        };

        var configureDirectoryValues = function (directoryConfigValues) {
            adminDOMObjects.voltdbRoot.text(directoryConfigValues.voltdbRoot);
            adminDOMObjects.snapshotPath.text(directoryConfigValues.snapshotPath);
            adminDOMObjects.exportOverflow.text(directoryConfigValues.exportOverflow);
            adminDOMObjects.commandLogPath.text(directoryConfigValues.commandLogPath);
            adminDOMObjects.commandLogSnapshotPath.text(directoryConfigValues.commandLogSnapshotPath);
            adminDOMObjects.drOverflowPath.text(directoryConfigValues.drOverflowPath);
        };

        var configurePromoteAction = function (adminConfigValues) {
            //Ignore at most 2 requests which might be old.
            if (adminClusterObjects.ignorePromoteUpdateCount > 0) {
                adminClusterObjects.ignorePromoteUpdateCount--;
                return;
            }

            var enable = (adminConfigValues.replicationRole != null && adminConfigValues.replicationRole.toLowerCase() == 'replica');

            if (enable != adminClusterObjects.enablePromote) {
                adminClusterObjects.enablePromote = enable;
                if (adminClusterObjects.enablePromote) {
                    adminClusterObjects.btnClusterPromote !== undefined && adminClusterObjects.btnClusterPromote.removeClass().addClass("promote");
                } else {
                    adminClusterObjects.btnClusterPromote !== undefined && adminClusterObjects.btnClusterPromote.removeClass().addClass("promote-disabled");
                }
            }
        };
    });

    window.VoltDbAdminConfig = VoltDbAdminConfig = new iVoltDbAdminConfig();

})(window);


//common functions
var getOnOffText = function (isChecked) {
    return (isChecked) ? "On" : "Off";
};

var getOnOffClass = function (isOn) {
    return (isOn) ? "onIcon" : "offIcon";
};

//    add/remove table row in security 
function deleteRow(row) {
    var i = row.parentNode.parentNode.rowIndex;
    document.getElementById('secTbl').deleteRow(i);
}
function insRow() {
    var x = document.getElementById('secTbl');
    var new_row = x.rows[1].cloneNode(true);
    var len = x.rows.length;

    var inp1 = new_row.cells[1].getElementsByTagName('input')[0];
    inp1.id = 'input' + (len - 1);
    inp1.name = 'input' + (len - 1);
    inp1.value = '';
    $(inp1).css("display", "inline-block");

    var lbl1 = new_row.cells[1].getElementsByTagName('label')[0];
    lbl1.id = 'errorUser' + (len - 1);
    lbl1.htmlFor = 'input' + (len - 1);
    lbl1.value = '';
    $(lbl1).css("display", "none");

    var anch = new_row.cells[1].getElementsByTagName('a')[0];
    $(anch).css("display", "none");

    var inp0 = new_row.cells[0].getElementsByTagName('input')[0];
    inp0.id = 'inputUserName' + (len - 1);
    inp0.name = 'inputUserName' + (len - 1);
    inp0.value = '';
    $(inp0).css("display", "inline-block");

    var lbl0 = new_row.cells[0].getElementsByTagName('label')[0];
    lbl0.id = 'errorUserName' + (len - 1);
    lbl0.htmlFor = 'inputUserName' + (len - 1);
    lbl0.value = '';
    $(lbl0).css("display", "none");

    var sel = new_row.cells[2].getElementsByTagName('select')[0];
    sel.id += len;
    sel.value = '';
    x.appendChild(new_row);
}

var toggleProperties = function (ele) {
    var parent = $(ele).parent();
    parent.siblings('.childprop-' + parent.attr("id")).toggle();
    parent.find(".labelCollapsed").toggleClass("labelExpanded");

    VoltDbAdminConfig.toggleStates[parent.attr("id")] = parent.find('td:first-child > a').hasClass('labelExpanded');
};

var deleteRow = function (cell) {
    var row = $(cell).parent().parent();
    if (row.length > 0)
        row.remove();
};

var editStream = function (editId) {
    adminDOMObjects.addConfigLink.data("id", editId);
    adminDOMObjects.addConfigLink.trigger("click");
};

var editImportStream = function (editId) {
    adminDOMObjects.addImportConfigPopupLink.data("id", editId);
    adminDOMObjects.addImportConfigPopupLink.trigger("click");
};

var editDiskLimit = function (editId) {
    adminDOMObjects.addDiskLimitLink.trigger("click");
};

var addUser = function (editId, username, role) {
    $('#addUserInnerPopup').data('isupdate', editId);
    if (editId == 1) {
        $('#addUserInnerPopup').data('username', username);
        $('#addUserInnerPopup').data('role', role);
    }
    $("#addNewUserLink").trigger("click");
};

var formatDisplayName = function (displayName) {
    displayName = displayName.toLowerCase();
    return displayName.charAt(0).toUpperCase() + displayName.slice(1);
};








