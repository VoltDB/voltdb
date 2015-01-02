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
        btnErrorClusterPromote: $('#btnErrorPromotePopup'),
        errorPromoteMessage: $('#promoteErrorMessage'),
        updateMessageBar: $('#snapshotBar')
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
        btnEditSecurityOk: $("#btnEditSecurityOk"),
        btnEditSecurityCancel: $("#btnEditSecurityCancel"),
        LinkSecurityEdit: $("#securityEdit"),
        chkSecurity: $("#chkSecurity"),
        chkSecurityValue: $("#chkSecurity").is(":checked"),
        iconSecurityOption: $("#securityOptionIcon"),
        spanSecurity: $("#spanSecurity"),

        //Edit Auto Snapshot objects
        btnEditAutoSnapshotOk: $("#btnEditAutoSnapshotOk"),
        btnEditAutoSnapshotCancel: $("#btnEditAutoSnapshotCancel"),
        LinkAutoSnapshotEdit: $("#autoSnapshotEdit"),
        chkAutoSnapsot: $("#chkAutoSnapshot"),
        chkAutoSnapshotValue: $("#chkAutoSnapshot").is(":checked"),
        iconAutoSnapshotOption: $("#autoSnapshotIcon"),
        txtAutoSnapshot: $("#txtAutoSnapshot"),
        //File Prefix objects
        tBoxFilePrefix: $("#txtPrefix"),
        tBoxFilePrefixValue: $("#txtPrefix").text(),
        spanAutoSnapshotFilePrefix: $("#prefixSpan"),
        //Frequency objects
        tBoxAutoSnapshotFreq: $("#txtFrequency"),
        tBoxAutoSnapshotFreqValue: $("#frequencySpan").text(),
        spanAutoSnapshotFreq: $("#frequencySpan"),
        ddlAutoSnapshotFreqUnit: $("#ddlfrequencyUnit"),
        ddlAutoSnapshotFreqUnitValue: $("#spanFrequencyUnit").text(),
        spanAutoSnapshotFreqUnit: $("#spanFrequencyUnit"),
        //Retained objects
        tBoxAutoSnapshotRetained: $("#txtRetained"),
        tBoxAutoSnapshotRetainedValue: $("#retainedSpan").text(),
        spanAutoSnapshotRetained: $("#retainedSpan"),

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

        //Query Timeout
        rowQueryTimeout: $("#queryTimoutRow"),
        btnEditQueryTimeoutOk: $("#btnEditQueryTimeoutOk"),
        btnEditQueryTimeoutCancel: $("#btnEditQueryTimeoutCancel"),
        LinkQueryTimeoutEdit: $("#btnEditQueryTimeout"),
        tBoxQueryTimeout: $("#txtQueryTimeout"),
        tBoxQueryTimeoutValue: $("#queryTimeOutSpan").text(),
        spanqueryTimeOut: $("#queryTimeOutSpan"),
        
        //Update Error
        updateErrorFieldMsg: $("#updateErrorFieldMsg"),
        heartbeatTimeoutLabel: $("#heartbeatTimeoutRow").find("td:first-child").text()
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
            required: true
        },
        directoryPathMessages: {
            required: "Please enter a valid directory path."
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
        adminEditObjects.spanSecurity.text(getOnOffText(adminEditObjects.chkSecurity.is(":checked")));
    });

    adminEditObjects.chkAutoSnapsot.on('ifChanged', function () {
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
            $("#btnShutdownConfirmationOk").unbind("click");
            $("#btnShutdownConfirmationOk").on("click", function () {
                var shutdownTimeout = setTimeout(function() {
                    $('#serverShutdownPopup').click();
                    VoltDBCore.isServerConnected = false;
                    window.clearInterval(VoltDbUI.connectionTimeInterval);
                },10000);

                voltDbRenderer.shutdownCluster(function (success) {
                    if (!success) {
                        clearTimeout(shutdownTimeout);
                        alert("Unable to shutdown cluster.");
                    } 
                    $("#overlay").hide();
                });
                //Close the popup
                $($(this).siblings()[0]).trigger("click");

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

            $("#btnPauseConfirmationOk").unbind("click");
            $("#btnPauseConfirmationOk").on("click", function () {
                $("#overlay").show();
                voltDbRenderer.GetClusterInformation(function(clusterState) {
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
                $($(this).siblings()[0]).trigger("click");                

            });
        }
    });

    $('#resumeConfirmation').popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

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
                $($(this).siblings()[0]).trigger("click");
            });
        }
    });

    var toggleSecurityEdit = function (showEdit) {
        if (adminEditObjects.chkSecurityValue) {
            adminEditObjects.chkSecurity.iCheck('check');
        } else {
            adminEditObjects.chkSecurity.iCheck('uncheck');
        }
        adminEditObjects.spanSecurity.text(getOnOffText(adminEditObjects.chkSecurityValue));

        if (showEdit) {
            adminEditObjects.chkSecurity.parent().removeClass("customCheckbox");
            adminEditObjects.btnEditSecurityOk.hide();
            adminEditObjects.btnEditSecurityCancel.hide();
            adminEditObjects.LinkSecurityEdit.show();
            adminEditObjects.iconSecurityOption.show();
        } else {
            adminEditObjects.iconSecurityOption.hide();
            adminEditObjects.LinkSecurityEdit.hide();
            adminEditObjects.btnEditSecurityOk.show();
            adminEditObjects.btnEditSecurityCancel.show();
            adminEditObjects.chkSecurity.parent().addClass("customCheckbox");
        }
    };

    adminEditObjects.LinkSecurityEdit.on("click", function () {
        toggleSecurityEdit(false);
    });

    adminEditObjects.btnEditSecurityCancel.on("click", function () {
        toggleSecurityEdit(true);
    });

    adminEditObjects.btnEditSecurityOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            $("#btnSecurityOk").unbind("click");
            $("#btnSecurityOk").on("click", function () {

                if (adminEditObjects.chkSecurity.is(':checked')) {
                    adminEditObjects.iconSecurityOption.removeClass().addClass("onIcon");
                    adminEditObjects.chkSecurityValue = true;
                } else {
                    adminEditObjects.iconSecurityOption.removeClass().addClass("offIcon");
                    adminEditObjects.chkSecurityValue = false;
                }

                //Close the popup
                $($(this).siblings()[0]).trigger("click");
            });

            $("#btnPopupSecurityCancel").on("click", function () {
                toggleSecurityEdit(true);
            });

            $(".popup_back").on("click", function () {
                toggleSecurityEdit(true);
            });

            $(".popup_close").on("click", function () {
                toggleSecurityEdit(true);
            });
        }
    });

    var showUpdateMessage = function(msg) {
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

            $.validator.addMethod(
                "regex",
                function(value, element, regexp) {
                    var re = new RegExp(regexp);
                    return this.optional(element) || re.test(value);
                },
                "Please enter only valid characters."
            );
        },
        afterOpen: function (event) {
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
                        setTimeout(function() {
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

                //var snapShotDirectory = ($('#voltdbroot').text() != "" && $('#voltdbroot').text() != undefined && $('#snapshotpath').text() != "" && $('#snapshotpath').text() != undefined) ? ($('#voltdbroot').text() + '/' + $('#snapshotpath').text()) : '';
                //if (snapShotDirectory == "") {
                //    $($(this).siblings()[0]).trigger("click");
                //    $('#saveSnapshotStatus').html('Failed to save snapshot');
                //    $('#saveSnapshotMessage').html('Could not get Voltdb root directory and Snapshot path');
                //    $('#btnSaveSnapshotPopup').click();
                //} else {
                //    snapShotDirectory = $('#txtSnapshotDirectory').val() != "" ? snapShotDirectory + "/" + $('#txtSnapshotDirectory').val() : snapShotDirectory;
                //}
                var snapShotDirectory = $('#txtSnapshotDirectory').val();
                var snapShotFileName = $('#txtSnapshotName').val();
                voltDbRenderer.saveSnapshot(snapShotDirectory, snapShotFileName, function (success,snapshotStatus) {
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
                $($(this).siblings()[0]).trigger("click");

            });
        }
    });

    adminClusterObjects.btnClusterPromote.on("click", function (e) {
        if (!adminClusterObjects.enablePromote) {
            e.preventDefault();
            e.stopPropagation();
        }
    });

    adminClusterObjects.btnErrorClusterPromote.popup();

    adminClusterObjects.btnClusterPromote.popup({
        open: function(event, ui, ele) {
        },
        afterOpen: function (event) {
            var popup = $(this)[0];
            $("#promoteConfirmOk").unbind("click");
            $("#promoteConfirmOk").on("click", function (e) {
                $("#overlay").show();
                voltDbRenderer.promoteCluster(function (status, statusstring) {
                    $("#overlay").hide();
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
                });
                
                //Close the popup 
                popup.close();
            });
        }
    });

    var getDateTime = function() {
        var currentDate = new Date();
        return (currentDate.getFullYear() + '.' + (currentDate.getMonth() + 1) + '.' + currentDate.getDate() + '.' + currentDate.getHours() + '.' + currentDate.getMinutes() + '.' + currentDate.getSeconds()).toString();
    };

    $('#restoreConfirmation').popup();
    
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

            $("#startConfirmOk").unbind("click");
            $("#startConfirmOk").on("click", function () {

                $("#startConfirmation").hide();
                $("#stopConfirmation").show();

                //Close the popup
                $($(this).siblings()[0]).trigger("click");
            });
        }
    });


    var toggleAutoSnapshotEdit = function (showEdit) {

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

        if (showEdit) {
            adminEditObjects.chkAutoSnapsot.parent().removeClass("customCheckbox");
            adminEditObjects.btnEditAutoSnapshotOk.hide();
            adminEditObjects.btnEditAutoSnapshotCancel.hide();
            adminEditObjects.LinkAutoSnapshotEdit.show();
            adminEditObjects.iconAutoSnapshotOption.show();

            adminEditObjects.tBoxAutoSnapshotFreq.hide();
            adminEditObjects.ddlAutoSnapshotFreqUnit.hide();
            adminEditObjects.tBoxAutoSnapshotRetained.hide();
            adminEditObjects.tBoxFilePrefix.hide();
            adminEditObjects.spanAutoSnapshotFreq.show();
            adminEditObjects.spanAutoSnapshotFreqUnit.show();
            adminEditObjects.spanAutoSnapshotRetained.show();
            adminEditObjects.spanAutoSnapshotFilePrefix.show();
        } else {
            adminEditObjects.iconAutoSnapshotOption.hide();
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
        }
    };

    adminEditObjects.btnEditAutoSnapshotCancel.on("click", function () {
        toggleAutoSnapshotEdit(true);
    });

    adminEditObjects.btnEditAutoSnapshotOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            $("#btnSaveSnapshot").unbind("click");
            $("#btnSaveSnapshot").on("click", function () {
                var regex = new RegExp("^[a-zA-Z0-9_]+$");
                if (!regex.test(adminEditObjects.tBoxFilePrefix.val())) {
                    alert("Invalid input characters.");
                    $($(this).siblings()[0]).trigger("click");
                    return false;
                }

                if (adminEditObjects.chkAutoSnapsot.is(':checked')) {
                    adminEditObjects.iconAutoSnapshotOption.removeClass().addClass("onIcon");
                    adminEditObjects.chkAutoSnapshotValue = true;
                } else {
                    adminEditObjects.iconAutoSnapshotOption.removeClass().addClass("offIcon");
                    adminEditObjects.chkAutoSnapshotValue = false;
                }

                adminEditObjects.tBoxAutoSnapshotFreqValue = adminEditObjects.tBoxAutoSnapshotFreq.val();
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = adminEditObjects.ddlAutoSnapshotFreqUnit.val();
                adminEditObjects.tBoxAutoSnapshotRetainedValue = adminEditObjects.tBoxAutoSnapshotRetained.val();
                adminEditObjects.tBoxFilePrefixValue = adminEditObjects.tBoxFilePrefix.val();

                adminEditObjects.spanAutoSnapshotFreq.html(adminEditObjects.tBoxAutoSnapshotFreqValue);
                adminEditObjects.spanAutoSnapshotFreqUnit.html(adminEditObjects.ddlAutoSnapshotFreqUnitValue);
                adminEditObjects.spanAutoSnapshotRetained.html(adminEditObjects.tBoxAutoSnapshotRetainedValue);
                adminEditObjects.spanAutoSnapshotFilePrefix.html(adminEditObjects.tBoxFilePrefixValue);

                //Close the popup
                $($(this).siblings()[0]).trigger("click");
            });

            $("#btnPopupAutoSnapshotCancel").on("click", function () {
                toggleAutoSnapshotEdit(true);
            });

            $(".popup_back").on("click", function () {
                toggleAutoSnapshotEdit(true);
            });

            $(".popup_close").on("click", function () {
                toggleAutoSnapshotEdit(true);
            });
        }
    });

    adminEditObjects.LinkAutoSnapshotEdit.click(function () {
        var parent = $(this).parent().parent();
        parent.siblings('.child-' + parent.attr("id")).show();
        parent.find(".labelCollapsed").addClass("labelExpanded");
        toggleAutoSnapshotEdit(false);
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

    adminEditObjects.btnEditHeartbeatTimeoutOk.on("click", function(e) {
        $("#formHeartbeatTimeout").valid();

        if (adminEditObjects.errorHeartbeatTimeout.is(":visible")) {
            e.preventDefault();
            e.stopPropagation();
            adminEditObjects.tBoxHeartbeatTimeout.focus();

            adminEditObjects.errorHeartbeatTimeout.css("background-color", "yellow");
            setTimeout(function() {
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
            });

            $(".popup_back").on("click", function () {
                toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
            });

            $(".popup_close").on("click", function () {
                toggleHeartbeatTimeoutEdit(editStates.ShowEdit);
            });
        }
    });

    //Query time out
    var toggleQueryTimeoutEdit = function (showEdit) {

        adminEditObjects.tBoxQueryTimeout.val(adminEditObjects.tBoxQueryTimeoutValue);

        if (showEdit) {
            adminEditObjects.btnEditQueryTimeoutOk.hide();
            adminEditObjects.btnEditQueryTimeoutCancel.hide();
            adminEditObjects.LinkQueryTimeoutEdit.show();

            adminEditObjects.tBoxQueryTimeout.hide();
            adminEditObjects.spanqueryTimeOut.show();
        } else {
            adminEditObjects.LinkQueryTimeoutEdit.hide();
            adminEditObjects.btnEditQueryTimeoutOk.show();
            adminEditObjects.btnEditQueryTimeoutCancel.show();

            adminEditObjects.spanqueryTimeOut.hide();
            adminEditObjects.tBoxQueryTimeout.show();
        }
    };

    adminEditObjects.LinkQueryTimeoutEdit.on("click", function () {
        toggleQueryTimeoutEdit(false);
        $("td.queryTimeOut span").toggleClass("unit");
    });

    adminEditObjects.btnEditQueryTimeoutCancel.on("click", function () {
        toggleQueryTimeoutEdit(true);
    });

    adminEditObjects.btnEditQueryTimeoutOk.popup({
        open: function (event, ui, ele) {
        },
        afterOpen: function () {

            $("#btnPopupQueryTimeoutOk").unbind("click");
            $("#btnPopupQueryTimeoutOk").on("click", function () {

                adminEditObjects.tBoxQueryTimeoutValue = adminEditObjects.tBoxQueryTimeout.val();
                adminEditObjects.spanqueryTimeOut.html(adminEditObjects.tBoxQueryTimeoutValue);

                //Close the popup
                $($(this).siblings()[0]).trigger("click");
            });

            $("#btnPopupQueryTimeoutCancel").on("click", function () {
                toggleQueryTimeoutEdit(true);
            });

            $(".popup_back").on("click", function () {
                toggleQueryTimeoutEdit(true);
            });

            $(".popup_close").on("click", function () {
                toggleQueryTimeoutEdit(true);
            });
        }
    });

    $("#updateErrorPopupLink").popup();

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
}

(function (window) {
    var iVoltDbAdminConfig = (function () {

        var currentRawAdminConfigurations;
        this.isAdmin = false;
        this.registeredElements = [];
        this.servers = [];
        this.stoppedServer="";
        this.runningServerIds = "";
        
        this.server = function(hostIdvalue,serverNameValue,serverStateValue) {
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

        this.refreshServerList = function (serverList,serverCount) {
            adminDOMObjects.adminServerList.html(serverList);
        };

        var configureAdminValues = function (adminConfigValues) {
            adminDOMObjects.siteNumberHeader.text(adminConfigValues.sitesperhost);
            adminDOMObjects.kSafety.text(adminConfigValues.kSafety);
            adminDOMObjects.partitionDetection.removeClass().addClass(getOnOffClass(adminConfigValues.partitionDetection));
            adminDOMObjects.partitionDetectionLabel.text(getOnOffText(adminConfigValues.partitionDetection));
            adminDOMObjects.security.removeClass().addClass(getOnOffClass(adminConfigValues.securityEnabled));
            adminDOMObjects.securityLabel.text(getOnOffText(adminConfigValues.securityEnabled));
            adminDOMObjects.httpAccess.removeClass().addClass(getOnOffClass(adminConfigValues.httpEnabled));
            adminDOMObjects.httpAccessLabel.text(getOnOffText(adminConfigValues.httpEnabled));
            adminDOMObjects.jsonAPI.removeClass().addClass(getOnOffClass(adminConfigValues.jsonEnabled));
            adminDOMObjects.jsonAPILabel.text(getOnOffText(adminConfigValues.jsonEnabled));
            adminDOMObjects.autoSnapshot.removeClass().addClass(getOnOffClass(adminConfigValues.snapshotEnabled));
            adminDOMObjects.autoSnapshotLabel.text(getOnOffText(adminConfigValues.snapshotEnabled));
            adminDOMObjects.filePrefix.text(adminConfigValues.filePrefix != "" ? adminConfigValues.filePrefix : "");
            adminDOMObjects.frequency.text(adminConfigValues.frequency != "" ? adminConfigValues.frequency : "");
            adminDOMObjects.frequencyLabel.text(adminConfigValues.frequency != "" ? "Hrs" : "");
            adminDOMObjects.retained.text(adminConfigValues.retained != "" ? adminConfigValues.retained : "");
            adminDOMObjects.retainedLabel.text(adminConfigValues.retained != "" && adminConfigValues.retained != undefined ? "Copies" : "");
            adminEditObjects.tBoxAutoSnapshotRetainedValue = adminConfigValues.retained;
            adminEditObjects.tBoxFilePrefixValue = adminConfigValues.filePrefix;
            adminDOMObjects.commandLog.removeClass().addClass(getOnOffClass(adminConfigValues.commandLogEnabled));
            adminDOMObjects.commandLogLabel.text(adminConfigValues.commandLogEnabled == true ? 'On' : 'Off');
            adminDOMObjects.commandLogFrequencyTime.text(adminConfigValues.commandLogFrequencyTime != "" ? adminConfigValues.commandLogFrequencyTime : "");
            adminDOMObjects.commandLogFrequencyTimeLabel.text(adminConfigValues.commandLogFrequencyTime != "" && adminConfigValues.commandLogFrequencyTime != undefined ? "ms" : "");
            adminDOMObjects.commandLogFrequencyTransactions.text(adminConfigValues.commandLogFrequencyTransactions != "" ? adminConfigValues.commandLogFrequencyTransactions : "");
            adminDOMObjects.commandLogSegmentSize.text(adminConfigValues.logSegmentSize != "" ? adminConfigValues.logSegmentSize : "");
            adminDOMObjects.commandLogSegmentSizeLabel.text(adminConfigValues.logSegmentSize != "" && adminConfigValues.logSegmentSize != undefined ? "MB" : "");
            adminDOMObjects.exports.removeClass().addClass(getOnOffClass(adminConfigValues.export));
            adminDOMObjects.exportLabel.text(getOnOffText(adminConfigValues.export));
            adminDOMObjects.target.text(adminConfigValues.targets);
            
            adminDOMObjects.heartBeatTimeout.text(adminConfigValues.heartBeatTimeout != "" ? adminConfigValues.heartBeatTimeout : "");

            if (adminConfigValues.heartBeatTimeout != "" && adminConfigValues.heartBeatTimeout != undefined) {
                adminDOMObjects.heartBeatTimeoutLabel.text("ms");
                adminEditObjects.LinkHeartbeatEdit.show();
            } else {
                adminDOMObjects.heartBeatTimeoutLabel.text("");
                adminEditObjects.LinkHeartbeatEdit.hide();
            }

            adminDOMObjects.tempTablesMaxSize.text(adminConfigValues.tempTablesMaxSize != "" ? adminConfigValues.tempTablesMaxSize : "");
            adminDOMObjects.tempTablesMaxSizeLabel.text(adminConfigValues.tempTablesMaxSize != "" && adminConfigValues.tempTablesMaxSize != undefined ? "MB" : "");
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

        var getExportProperties = function(data) {
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

        var setSnapShotUnit = function(unit) {
            if (unit == 's') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Sec');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Sec';
            }else if (unit == 'm') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Min');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Min';
            }else if (unit == 'h') {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('Hrs');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = 'Hrs';
            } else {
                adminEditObjects.spanAutoSnapshotFreqUnit.text('');
                adminEditObjects.ddlAutoSnapshotFreqUnitValue = '';
            }
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
            
            adminDOMObjects.queryTimeout.text(adminConfigValues.queryTimeout != "" ? adminConfigValues.queryTimeout : "");
            adminDOMObjects.queryTimeoutLabel.text(adminConfigValues.queryTimeout != "" ? "ms" : "");
            adminEditObjects.tBoxQueryTimeoutValue = adminConfigValues.queryTimeout;
        };

        var configurePortAndOverviewValues = function (configValues) {
            adminDOMObjects.adminPort.text(configValues.adminPort);
            adminDOMObjects.httpPort.text(configValues.httpPort);
            adminDOMObjects.internalPort.text(configValues.internalPort);
            adminDOMObjects.zookeeperPort.text(configValues.zookeeperPort);
            adminDOMObjects.replicationPort.text(configValues.replicationPort);
            adminDOMObjects.clientPort.text(configValues.clientPort);

            adminDOMObjects.maxJavaHeap.text(configValues.maxJavaHeap != "" ? parseFloat(configValues.maxJavaHeap/1024) : "");
            adminDOMObjects.maxJavaHeapLabel.text(configValues.maxJavaHeap != "" && configValues.maxJavaHeap != undefined ? "MB" : "");
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

