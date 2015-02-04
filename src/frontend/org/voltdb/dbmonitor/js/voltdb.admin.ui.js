var adminDOMObjects = {};
var adminEditObjects = {};
var adminClusterObjects = {};
var editStates = {
    ShowEdit: 0,
    ShowOkCancel: 1,
    ShowLoading: 2
};
var INT_MAX_VALUE = 2147483647;

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
        errorRestoreMsgContainer: $('#errorRestoreMsgContainer')
    };

    adminDOMObjects = {
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
        exports: $("#exports"),
        exportLabel: $("#txtExportLabel"),
        target: $("#target"),
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

        //ServerList Section
        adminServerList: $("#serverListWrapperAdmin > .tblshutdown > tbody")

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
        securityLabel: $("#securityRow").find("td:first-child").text(),
        editStateSecurity: editStates.ShowEdit,
        securityStateOriginal: { "SecurityStatus": false, "linkSecurityEdit": false },

        //Edit Auto Snapshot objects
        btnEditAutoSnapshotOk: $("#btnEditAutoSnapshotOk"),
        btnEditAutoSnapshotCancel: $("#btnEditAutoSnapshotCancel"),
        LinkAutoSnapshotEdit: $("#autoSnapshotEdit"),
        chkAutoSnapsot: $("#chkAutoSnapshot"),
        chkAutoSnapshotValue: $("#chkAutoSnapshot").is(":checked"),
        iconAutoSnapshotOption: $("#autoSnapshotIcon"),
        txtAutoSnapshot: $("#txtAutoSnapshot"),
        spanAutoSpanEdited: "",
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

        //Update Error
        updateErrorFieldMsg: $("#updateErrorFieldMsg"),
        updateSnapshotErrorFieldMsg: $("#updateSnapshotErrorFieldMsg"),
        heartbeatTimeoutLabel: $("#heartbeatTimeoutRow").find("td:first-child").text(),
        queryTimeoutUpdateErrorFieldMsg: $("#queryTimeoutUpdateErrorFieldMsg"),
        snapshotLabel: $("#row-2").find("td:first-child").text(),
        queryTimeoutFieldLabel: $("#queryTimoutRow").find("td:first-child").text()
    };

    var adminValidationRules = {
        numericRules: {
            required: true,
            min: 0,
            max: INT_MAX_VALUE,
            digits: true,
        },
        numericMessages: {
            required: "Please enter a valid positive number.",
            min: "Please enter a valid positive number.",
            max: "Please enter a positive number between 0 and " + INT_MAX_VALUE + ".",
            digits: "Please enter a positive number without any decimal."
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
        }
    };

    //Admin Page download link
    $('#downloadAdminConfigurations').on('click', function (e) {
        var port = VoltDBConfig.GetPortId() != null ? VoltDBConfig.GetPortId() : '8080';
        var url = window.location.protocol + '//' + VoltDBConfig.GetDefaultServerIP() + ":" + port + '/deployment/download/deployment.xml?' + VoltDBCore.shortApiCredentials;
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
        adminEditObjects.spanAutoSpanEdited = getOnOffText(adminEditObjects.chkAutoSnapsot.is(":checked"));
        adminEditObjects.txtAutoSnapshot.text(getOnOffText(adminEditObjects.chkAutoSnapsot.is(":checked")));
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
        },
        afterOpen: function () {
            var popup = $(this)[0];
            $("#btnShutdownConfirmationOk").unbind("click");
            $("#btnShutdownConfirmationOk").on("click", function () {
                var shutdownTimeout = setTimeout(function () {
                    $('#serverShutdownPopup').click();
                    VoltDBCore.isServerConnected = false;
                    window.clearInterval(VoltDbUI.connectionTimeInterval);
                }, 10000);

                voltDbRenderer.shutdownCluster(function (success) {
                    if (!success) {
                        clearTimeout(shutdownTimeout);
                        alert("Unable to shutdown cluster.");
                    }
                    $("#overlay").hide();
                });
                //Close the popup
                popup.close();
            });

            $("#btnShutdownConfirmationCancel").unbind("click");
            $("#btnShutdownConfirmationCancel").on("click", function () {
                popup.close();
            });
        }
    });

    $("#serverShutdownPopup").popup({
        closeDialog: function () {
            VoltDbUI.isConnectionChecked = false;
            VoltDbUI.refreshConnectionTime('20000');
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

        }
        else if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingSecurity.hide();
            adminEditObjects.iconSecurityOption.hide();
            adminEditObjects.LinkSecurityEdit.hide();
            adminEditObjects.spanSecurity.show();
            adminEditObjects.btnEditSecurityOk.show();
            adminEditObjects.btnEditSecurityCancel.show();
            adminEditObjects.chkSecurity.parent().addClass("customCheckbox");
        }
        else {
            adminEditObjects.loadingSecurity.hide();
            adminEditObjects.spanSecurity.show();
            adminEditObjects.iconSecurityOption.show();
            adminEditObjects.LinkSecurityEdit.show();
            adminEditObjects.btnEditSecurityOk.hide();
            adminEditObjects.btnEditSecurityCancel.hide();
            adminEditObjects.chkSecurity.parent().removeClass("customCheckbox");
        }
    };

    adminEditObjects.LinkSecurityEdit.on("click", function () {
        if (adminEditObjects.securityStateOriginal.linkSecurityEdit == true)
            toggleSecurityEdit(editStates.ShowOkCancel);
    });

    adminEditObjects.btnEditSecurityCancel.on("click", function () {
        adminEditObjects.chkSecurityValue = adminEditObjects.securityStateOriginal.SecurityStatus;
        toggleSecurityEdit(editStates.ShowEdit);
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
                        if (adminConfigurations.security.enabled)
                            location.reload(true);

                        //Reload Admin configurations for displaying the updated value
                        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {
                            VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
                            toggleSecurityEdit(editStates.ShowEdit);
                        });

                    } else {

                        toggleSecurityEdit(editStates.ShowEdit);
                        var msg = '"' + adminEditObjects.securityLabel + '". ';
                        if (result.status == "-1" && result.statusstring == "Query timeout.") {
                            msg += "The DB Monitor service is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += result.statusstring != null ? result.statusstring : "Please try again later.";
                        }

                        adminEditObjects.updateErrorFieldMsg.text(msg);
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
    

    $("#loginWarnPopup").popup({
        afterOpen: function (event, ui, ele) {
            var popup = $(this)[0];
           
            $("#btnLoginWarningOk").unbind("click");
            $("#btnLoginWarningOk").on('click', function () {
                if ($.cookie("username") == undefined || $.cookie("username") == 'null') {
                    location.reload(true);
                }

                if (VoltDbUI.CurrentTab == NavigationTabs.Admin) {
                    $("#navDbmonitor").click();
                }

                $("#navAdmin").hide();
                popup.close();
            });
        },
        closeContent: '',
        modal: true
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
            
            $("#saveSnapshotConfirm").hide();
            $("#saveSnapshot").show();
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

                $("#saveSnapshot").hide();
                $("#saveSnapshotConfirm").show();
            });

            $("#btnSaveSnapshotCancel").unbind("click");
            $("#btnSaveSnapshotCancel").on("click", function () {
                popup.close();
            });
            
            $("#btnSaveSnapshotConfirmCancel").unbind("click");
            $("#btnSaveSnapshotConfirmCancel").on("click", function () {
                $("#saveSnapshotConfirm").hide();
                $("#saveSnapshot").show();
            });
            
            $("#btnSaveSnapshotOk").unbind("click");
            $("#btnSaveSnapshotOk").on("click", function (e) {
                var snapShotDirectory = $('#txtSnapshotDirectory').val();
                var snapShotFileName = $('#txtSnapshotName').val();
                voltDbRenderer.saveSnapshot(snapShotDirectory, snapShotFileName, function (success, snapshotStatus) {
                    if (success) {
                        if (snapshotStatus[getCurrentServer()].RESULT.toLowerCase() == "success") {
                            showUpdateMessage('Snapshot saved successfully.');
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

        var currentDate = new Date().toISOString(); // toISOString() will give you YYYY-MM-DDTHH:mm:ss.sssZ
        return currentDate.substr(0, 19).replace('T', '.').replace(/-/g, '.').replace(/:/g, '.');
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
        $('#tblSearchList').html('<tr style="border:none"><td colspan="3" align="center"><img src="css/resources/images/loader-small.GIF"></td></tr>');
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
                    result += '<tr><td style="color:#c70000" colspan="3"> Error: Failure getting snapshots. ' + snapshot.ERR_MSG + '</td></tr>';
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

            adminEditObjects.loadingSnapshot.show();
            adminEditObjects.loadingSnapshotFrequency.show();
            adminEditObjects.loadingSnapshotPrefix.show();
            adminEditObjects.loadingSnapshotRetained.show();
        }
        else if (state == editStates.ShowOkCancel) {
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
        } else {
            adminEditObjects.chkAutoSnapsot.parent().removeClass("customCheckbox");
            adminEditObjects.btnEditAutoSnapshotOk.hide();
            adminEditObjects.btnEditAutoSnapshotCancel.hide();
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
                            msg += "The DB Monitor service is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
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

    adminEditObjects.LinkAutoSnapshotEdit.click(function () {
        var parent = $(this).parent().parent();
        parent.siblings('.child-' + parent.attr("id")).show();
        parent.find(".labelCollapsed").addClass("labelExpanded");
        toggleAutoSnapshotEdit(editStates.ShowOkCancel);
    });

    $("#formHeartbeatTimeout").validate({
        rules: {
            txtHrtTimeOut: adminValidationRules.numericRules
        },
        messages: {
            txtHrtTimeOut: adminValidationRules.numericMessages
        }
    });

    //Heartbeat time out
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
        }
        else if (state == editStates.ShowOkCancel) {
            adminEditObjects.loadingHeartbeatTimeout.hide();
            adminEditObjects.LinkHeartbeatEdit.hide();
            adminEditObjects.btnEditHeartbeatTimeoutOk.show();
            adminEditObjects.btnEditHeartbeatTimeoutCancel.show();

            adminEditObjects.spanHeartbeatTimeOut.hide();
            adminEditObjects.tBoxHeartbeatTimeout.show();
            adminDOMObjects.heartBeatTimeoutLabel.show();
        }
        else {
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
                            msg += "The DB Monitor service is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
                        } else {
                            msg += "Please try again later.";
                        }

                        adminEditObjects.updateErrorFieldMsg.text(msg);
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

    //Query time out
    var toggleQueryTimeoutEdit = function (state) {

        adminEditObjects.tBoxQueryTimeout.val(adminEditObjects.tBoxQueryTimeoutValue);

        if (state == editStates.ShowLoading) {
            adminDOMObjects.queryTimeoutLabel.hide();
            adminEditObjects.LinkQueryTimeoutEdit.hide();
            adminEditObjects.btnEditQueryTimeoutOk.hide();
            adminEditObjects.btnEditQueryTimeoutCancel.hide();
            adminEditObjects.tBoxQueryTimeout.hide();

            adminEditObjects.loadingQueryTimeout.show();
        }
        else if (state == editStates.ShowOkCancel) {
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
            adminEditObjects.LinkQueryTimeoutEdit.show();

            adminEditObjects.tBoxQueryTimeout.hide();
            adminEditObjects.spanqueryTimeOut.show();
            adminDOMObjects.queryTimeoutLabel.show();
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
                            msg += "The DB Monitor service is either down, very slow to respond or the server refused connection. Please try to edit when the server is back online.";
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
}

(function (window) {
    var iVoltDbAdminConfig = (function () {

        var currentRawAdminConfigurations;
        this.isAdmin = false;
        this.registeredElements = [];
        this.servers = [];
        this.stoppedServer = "";
        this.runningServerIds = "";
        this.firstResponseReceived = false;

        this.server = function (hostIdvalue, serverNameValue, serverStateValue) {
            this.hostId = hostIdvalue;
            this.serverName = serverNameValue;
            this.serverState = serverStateValue;
        };

        this.stoppedServer = function (hostIdvalue, serverNameValue, serverStateValue) {
            this.hostId = hostIdvalue;
            this.serverName = serverNameValue;
            this.serverState = serverStateValue;

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


        this.displayPortAndRefreshClusterState = function (portAndClusterValues) {
            if (portAndClusterValues != undefined && VoltDbAdminConfig.isAdmin) {
                configurePortAndOverviewValues(portAndClusterValues);
                refreshClusterValues(portAndClusterValues);
                configurePromoteAction(portAndClusterValues);
            }
        };

        this.refreshServerList = function (serverList, serverCount) {
            adminDOMObjects.adminServerList.html(serverList);
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
            adminDOMObjects.autoSnapshotLabel.text(adminEditObjects.spanAutoSpanEdited == "" ? getOnOffText(adminConfigValues.snapshotEnabled) : adminConfigValues.spanAutoSpanEdited);
            adminDOMObjects.filePrefix.text(adminConfigValues.filePrefix != null ? adminConfigValues.filePrefix : "");
            adminDOMObjects.frequency.text(adminConfigValues.frequency != null ? adminConfigValues.frequency : "");
            adminDOMObjects.frequencyLabel.text(adminConfigValues.frequency != null ? "Hrs" : "");
            adminDOMObjects.retained.text(adminConfigValues.retained != null ? adminConfigValues.retained : "");
            adminDOMObjects.retainedLabel.text(adminConfigValues.retained != null ? "Copies" : "");
            adminEditObjects.tBoxAutoSnapshotRetainedValue = adminConfigValues.retained;
            adminEditObjects.tBoxFilePrefixValue = adminConfigValues.filePrefix;
            adminDOMObjects.commandLog.removeClass().addClass(getOnOffClass(adminConfigValues.commandLogEnabled));
            adminDOMObjects.commandLogLabel.text(adminConfigValues.commandLogEnabled == true ? 'On' : 'Off');
            adminDOMObjects.commandLogFrequencyTime.text(adminConfigValues.commandLogFrequencyTime != null ? adminConfigValues.commandLogFrequencyTime : "");
            adminDOMObjects.commandLogFrequencyTimeLabel.text(adminConfigValues.commandLogFrequencyTime != null ? "ms" : "");
            adminDOMObjects.commandLogFrequencyTransactions.text(adminConfigValues.commandLogFrequencyTransactions != null ? adminConfigValues.commandLogFrequencyTransactions : "");
            adminDOMObjects.commandLogSegmentSize.text(adminConfigValues.logSegmentSize != null ? adminConfigValues.logSegmentSize : "");
            adminDOMObjects.commandLogSegmentSizeLabel.text(adminConfigValues.logSegmentSize != null ? "MB" : "");
            adminDOMObjects.exports.removeClass().addClass(getOnOffClass(adminConfigValues.export));
            adminDOMObjects.exportLabel.text(getOnOffText(adminConfigValues.export));
            adminDOMObjects.target.text(adminConfigValues.targets);
            adminDOMObjects.heartBeatTimeout.text(adminConfigValues.heartBeatTimeout != null ? adminConfigValues.heartBeatTimeout : "");


            if (adminConfigValues.heartBeatTimeout != null) {
                adminDOMObjects.heartBeatTimeoutLabel.text("ms");

                if (adminEditObjects.editStateHeartbeatTimeout == editStates.ShowEdit)
                    adminEditObjects.LinkHeartbeatEdit.show();
            } else {
                adminDOMObjects.heartBeatTimeoutLabel.text("");
                adminEditObjects.LinkHeartbeatEdit.hide();
            }

            adminDOMObjects.tempTablesMaxSize.text(adminConfigValues.tempTablesMaxSize != null ? adminConfigValues.tempTablesMaxSize : "");
            adminDOMObjects.tempTablesMaxSizeLabel.text(adminConfigValues.tempTablesMaxSize != null ? "MB" : "");
            adminDOMObjects.snapshotPriority.text(adminConfigValues.snapshotPriority);
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
            getExportProperties(adminConfigValues.properties);

        };

        var getExportProperties = function (data) {
            var result = "";
            if (data != undefined) {
                for (var i = 0; i < data.length; i++) {
                    if (i == 0) {
                        result += '<tr>' +
                            '<td width="67%">' + data[i].name + '</td>' +
                            '<td width="33%">' + data[i].value + '</td>' +
                            '</tr>';
                    } else if (i == (data.length - 1)) {
                        result += '<tr class="propertyLast">' +
                            '<td>' + data[i].name + '</td>' +
                            '<td>' + data[i].value + '</td>' +
                            '</tr>';
                    } else {
                        result += '<tr>' +
                            '<td>' + data[i].name + '</td>' +
                            '<td>' + data[i].value + '</td>' +
                            '</tr>';
                    }
                }
            }
            if (result == "") {
                result += '<tr class="propertyLast">' +
                        '<td width="67%">No properties available.</td>' +
                        '<td width="33%">&nbsp</td>' +
                        '</tr>';
            }
            $('#exportProperties').html(result);

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

        //var configureAdminValuesFromSystemInfo = function (adminConfigValues) {
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

        var configurePortAndOverviewValues = function (configValues) {
            adminDOMObjects.adminPort.text(configValues.adminPort);
            adminDOMObjects.httpPort.text(configValues.httpPort);
            adminDOMObjects.internalPort.text(configValues.internalPort);
            adminDOMObjects.zookeeperPort.text(configValues.zookeeperPort);
            adminDOMObjects.replicationPort.text(configValues.replicationPort);
            adminDOMObjects.clientPort.text(configValues.clientPort);
            adminDOMObjects.maxJavaHeap.text((configValues.maxJavaHeap != null && configValues.maxJavaHeap != NaN) ? parseFloat(configValues.maxJavaHeap / 1024) : "");
            adminDOMObjects.maxJavaHeapLabel.text((configValues.maxJavaHeap != null && configValues.maxJavaHeap != NaN) ? "MB" : "");
        };

        var refreshClusterValues = function (clusterValues) {
            if (clusterValues != undefined && clusterValues.hasOwnProperty('clusterState')) {
                if (clusterValues.clusterState.toLowerCase() == "running") {
                    adminClusterObjects.btnClusterPause.show();
                    adminClusterObjects.btnClusterResume.hide();
                } else if (clusterValues.clusterState.toLowerCase() == "paused") {
                    adminClusterObjects.btnClusterPause.hide();
                    adminClusterObjects.btnClusterResume.show();
                }
            }
        };

        var configureDirectoryValues = function (directoryConfigValues) {
            adminDOMObjects.voltdbRoot.text(directoryConfigValues.voltdbRoot);
            adminDOMObjects.snapshotPath.text(directoryConfigValues.snapshotPath);
            adminDOMObjects.exportOverflow.text(directoryConfigValues.exportOverflow);
            adminDOMObjects.commandLogPath.text(directoryConfigValues.commandLogPath);
            adminDOMObjects.commandLogSnapshotPath.text(directoryConfigValues.commandLogSnapshotPath);
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
                    adminClusterObjects.btnClusterPromote.removeClass().addClass("promote");
                } else {
                    adminClusterObjects.btnClusterPromote.removeClass().addClass("promote-disabled");
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

