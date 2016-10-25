﻿$(document).ready(function (e) {
    function reposition() {
        var modal = $(this),
            dialog = modal.find('.modal-dialog');
        modal.css('display', 'block');

        // Dividing by two centers the modal exactly, but dividing by three
        // or four works better for larger screens.
        dialog.css("margin-top", Math.max(0, ($(window).height() - dialog.height()) / 2));
    }
    // Reposition when a modal is shown
    $('.modal').on('show.bs.modal', reposition);
    // Reposition when the window is resized
    $(window).on('resize', function() {
        $('.modal:visible').each(reposition);
    });

     $('#overwriteConfirmation').on('shown.bs.modal', function() {
       $("body").removeAttr("style");
 });

    $('#accordion').on('hidden.bs.collapse', function() {
        //do something...
    });

    $('#accordion .accordion-toggle').click(function (e) {
        var chevState = $(e.target).siblings("i.indicator").toggleClass('glyphicon-triangle-right glyphicon-triangle-bottom');
        $("i.indicator").not(chevState).removeClass("glyphicon-triangle-bottom").addClass("glyphicon-triangle-right");
    });

    // Make Expandable Rows.
    $('tr.parent > td:first-child' || 'tr.parent > td:fourth-child')
        .css("cursor", "pointer")
        .attr("title", "Click to expand/collapse")
        .click(function () {
            var parent = $(this).parent();
            parent.siblings('.child-' + parent.attr("id")).toggle();
            parent.find(".glyphicon-triangle-right").toggleClass("glyphicon-triangle-bottom");
			if ($(this).text() == "Export") {
                //If parent is closed, then hide export configuration
                if (parent.find('td:first-child > a').hasClass('glyphicon-triangle-right') && parent.find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                    var parentProp = $('.parentprop');
                    for (var j = 0; j < parentProp.length; j++) {
                        if($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom') ||
                        ($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-right') && $(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom')))
                            //$(".childprop-row-41").show();
                            $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).show();
                        else
                            //$(".childprop-row-41").hide();
                            $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).hide();
                    }

                    //If parent is open, then open the export configuration.
                } else {
                var parentProp = $('.parentprop');
                    for (var j = 0; j < parentProp.length; j++) {
                        $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).hide();
                    }
                }
            }
            if ($(this).text().trim() == "Import") {
                //If parent is closed, then hide export configuration
                if (parent.find('td:first-child > a').hasClass('glyphicon-triangle-right') && parent.find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                    var parentProp = $('.importParentProp');
                    for (var j = 0; j < parentProp.length; j++) {
                        if($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom') ||
                        ($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-right') && $(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom')))
                            //$(".childprop-row-41").show();
                            $(parentProp[j]).siblings(".imp-childprop-" + parentProp[j].id).show();
                        else
                            //$(".childprop-row-41").hide();
                            $(parentProp[j]).siblings(".imp-childprop-" + parentProp[j].id).hide();
                    }

                    //If parent is open, then open the export configuration.
                } else {
                    var parentProp = $('.importParentProp');
                    for (var j = 0; j < parentProp.length; j++) {
                        $(parentProp[j]).siblings(".imp-childprop-" + parentProp[j].id).hide();
                    }
                }
            }
            if ($(this).text() == "Advanced") {
                //If parent is closed, then hide export configuration
                if (parent.find('td:first-child > a').hasClass('glyphicon-triangle-right') && parent.find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                    var parentProp = $('.diskLimitParentProp');
                    for (var j = 0; j < parentProp.length; j++) {
                        if($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom') ||
                        ($(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-right') && $(parentProp[j]).find('td:first-child > a').hasClass('glyphicon-triangle-bottom')))
                            //$(".childprop-row-41").show();
                            $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).show();
                        else
                            //$(".childprop-row-41").hide();
                            $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).hide();
                    }

                    //If parent is open, then open the export configuration.
                } else {
                var parentProp = $('.diskLimitParentProp');
                    for (var j = 0; j < parentProp.length; j++) {
                        $(parentProp[j]).siblings(".childprop-" + parentProp[j].id).hide();
                    }
                }
            }

        });
    $('tr[class^=child-]').hide().children('td');

	// Make Expandable Rows for Export Properties.
    $('tr.parentprop > td:first-child' || 'tr.parentprop > td:fourth-child')
        .css("cursor", "pointer")
        .attr("title", "Click to expand/collapse")
        .click(function () {
            var parent = $(this).parent();
            $('.childprop-' + parent.attr("id")).toggle();
            parent.find(".glyphicon-triangle-right").toggleClass("glyphicon-triangle-bottom");
        });
    $('tr[class^=childprop-]').hide().children('td');


    $("#navbar li").click(function () {
        $("#navbar li").removeClass('active');
        $(this).addClass('active');
        getCurrentTabContent();
        VdmUI.CurrentTab = getCurrentTab();
        saveSessionCookie("current-tab", VdmUI.CurrentTab);
        $("#navbar").removeClass("in");
    });

    $("#btnErrorDialog").click(function(){
        $("#deleteConfirmation").modal("hide");
    })

    //checkbox
    $("input[type=checkbox]").on('ifChanged', function () {
     var onOffText = $(this).is(":checked") ? "On" : "Off";
     $(this).parent().parent().next().text(onOffText);
    });

    //multi-select
    var s = $('#selectServers');
    s.multiSelect({
        selectableHeader: "<div class='custom-header'>All Servers</div>",
        selectionHeader: "<div class='custom-header'>Selected Servers</div>"
    });

    //collapse js

    $('.collapse').on('shown.bs.collapse', function(){
    $(this).parent().find(".glyphicon-triangle-right").removeClass("glyphicon-triangle-right").addClass("glyphicon-triangle-bottom glyphicon-triangle-right");
    }).on('hidden.bs.collapse', function(){
    $(this).parent().find(".glyphicon-triangle-bottom").removeClass("glyphicon-triangle-bottom").addClass("glyphicon-triangle-right");
    });

    loadPage();
});

var editStates = {
    ShowEdit: 0,
    ShowOkCancel: 1,
    ShowLoading: 2
};


var CheckPopupStatus = function(result){
    if(result)
        VdmUI.isErrorPopupShowing = true
    else
        VdmUI.isErrorPopupShowing = false
}



var user = []

var INT_MAX_VALUE = 2147483647;

var saveCookie = function (name, value) {
    $.cookie(name + "_" + VdmConfig.GetPortId(), value, { expires: 365 });
};

var saveSessionCookie = function (name, value) {
    $.cookie(name + "_" + VdmConfig.GetPortId(), value);
};

var NavigationTabs = {
    DBManager: 1,
    ServerSetting: 2
};

var getCurrentTab = function () {
    var activeLinkId = "";
    var activeLink = $("#navbar  li.active a");
    if (activeLink.length > 0) {
        activeLinkId = activeLink.attr("id");
    }
    if (activeLinkId == "serverSetting")
        return NavigationTabs.ServerSetting;

    return NavigationTabs.DBManager;
};

var getCurrentTabContent = function() {
    var activeLinkId = "";
    var activeLink = $("#navbar  li.active a");
    if (activeLink.length > 0) {
        activeLinkId = activeLink.attr("id");
    }
    if (activeLinkId == "serverSetting") {
        $("#divDbManager").addClass("hidden");
        $("#divServerSetting").removeClass("hidden");
    } else {
        $("#divServerSetting").addClass("hidden");
        $("#divDbManager").removeClass("hidden");
    }
};

var saveCurrentServer = function (serverName) {
    saveCookie("currentServer", serverName);
};

var getCurrentServer = function () {
    return VdmUI.getCookie("currentServer");
};

function deleteRow(row) {
    var i = row.parentNode.parentNode.rowIndex;
    document.getElementById('tblAddNewProperty').deleteRow(i);
}

function deleteImportRow(row){
    var i = row.parentNode.parentNode.rowIndex;
    document.getElementById('tblAddNewImportProperty').deleteRow(i);
}

function deleteDiskLimitRow(row){
    var i = row.parentNode.parentNode.rowIndex;
    document.getElementById('tblAddNewFeature').deleteRow(i);
}

var addExportProperties = function () {
        var exportType = $('#txtExportType').val();
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
            if (!$('#txtMetadataBrokerList').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="18" id="txtMetadataBrokerList" name="txtMetadataBrokerList" value="metadata.broker.list" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorMetadataBrokerList" for="txtMetadataBrokerList" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="18" id="txtMetadataBrokerListValue" name="txtMetadataBrokerListValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorMetadataBrokerListValue" for="txtMetadataBrokerListValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtMetadataBrokerList').attr("disabled", "disabled");
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
        } else if (exportType.toUpperCase() == "RABBITMQ") {
            if (!$('#selectRabbitMq').length) {
                exportProperties += '' +
                    '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <select id="selectRabbitMq" name="selectRabbitMq" class="newStreamPropertyName newStreamProperty  requiredProperty"> ' +
                    '           <option>broker.host</option> ' +
                    '           <option>amqp.uri</option> ' +
                    '       </select>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtRabbitMqValue" name="txtRabbitMqValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
                    '       <label id="errorRabbitMqValue" for="txtRabbitMqValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
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
        } else if ($(this).val() == "metadata.broker.list") {
            removeDuplicate(this, "metadata.broker.list");
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
    var exportType = $('#txtExportType').val();
    if (!$(object).hasClass("requiredProperty")) {
        var val = $(':input:eq(' + ($(':input').index(object) + 1) + ')').val();
        if ($(VdmUI.newStreamMinmPropertyName[propertyName]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName]).val(val);
            $(".newStreamMinProperty").addClass("addedProperty");
            var $row = $(object).closest("tr");
            $row.remove();
        } else if ($(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).val(val);
            $(".newStreamMinProperty").addClass("addedProperty");
            var $row1 = $(object).closest("tr");
            $row1.remove();
        }
        if (propertyName == "broker.host" || propertyName == "amqp.uri") {
            $("#selectRabbitMq").val(propertyName);
        }
    }
};

var setDefaultProperty = function () {

    var exportType = $('#txtExportType').val();
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
        setDefaultDisplay($("#txtMetadataBrokerList"));
    } else {
        setNormalDisplay($("#txtMetadataBrokerList"));
    }

    if (exportType.toUpperCase() == "JDBC") {
        setDefaultDisplay($("#txtJdbcUrl"));
        setDefaultDisplay($("#txtJdbcDriver"));
    } else {
        setNormalDisplay($("#txtJdbcUrl"));
        setNormalDisplay($("#txtJdbcDriver"));
    }

    if (exportType.toUpperCase() == "RABBITMQ") {
        setDefaultDisplay($("#selectRabbitMq"));
    } else {
        setNormalDisplay($("#selectRabbitMq"));
    }

    if (exportType.toUpperCase() == "ELASTICSEARCH") {
        setDefaultDisplay($("#txtEndpointES"));
    } else {
        setNormalDisplay($("#txtEndpointES"));
    }

};

var setDefaultDisplay = function (txtbox) {
    if (txtbox.selector != "#selectRabbitMq")
        txtbox.attr('disabled', 'disabled');
    var $row = txtbox.closest("tr");
    $('#tblAddNewProperty tr.headerProperty').after($row);
    var $td = $row.find("td:last-child");
    $td.html('');
};

var setNormalDisplay = function (txtbox) {
    txtbox.removeAttr('disabled');
    var $row = txtbox.closest("tr");
    var $td = $row.find("td:last-child");
    $td.html('<div class="popDelete" onclick="deleteRow(this)"></div>');
};

var addImportProperties = function () {
        var importType = $('#txtImportType').val();
        for (var i = 0; i < $(".newStreamMinImportProperty").length; i++) {
            if (!$($(".newStreamMinImportProperty")[i]).hasClass("orgImportProperty")) {
                $($(".newStreamMinImportProperty")[i]).addClass("importPropertyToRemove");
            }
        }
        $(".importPropertyToRemove").not(".addedImportProperty").remove();

        var importProperties = '';
        if (importType.toUpperCase() == "FILE") {
            if (!$('#txtOutdir').length) {
                importProperties += '' +
                    '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtOutdir" name="txtOutdir" value="outdir" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredImportProperty" type="text">' +
                    '       <label id="errorOutdir" for="txtOutdir" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtOutdirValue" name="txtOutdirValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorOutdirValue" for="txtOutdirValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtOutdir').attr("disabled", "disabled");
            }
            if (!$('#txtnonce').length) {
                importProperties += '' +
                    '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtnonce" name="txtnonce" value="nonce" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredImportProperty" type="text">' +
                    '       <label id="errornonce" for="txtnonce" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtnonceValue" name="txtnonceValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errornonceValue" for="txtnonceValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtnonce').attr("disabled", "disabled");
            }
            if (!$('#txtFileType').length) {
                importProperties += '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtFileType" name="txtFileType" value="type" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredImportProperty" type="text">' +
                    '       <label id="errorFileType" for="txtFileType" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtFileTypeValue" name="txtFileTypeValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorFileTypeValue" for="txtFileTypeValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtFileType').attr("disabled", "disabled");
            }
        } else if (importType.toUpperCase() == "HTTP") {
            if (!$('#txtEndpoint').length) {
                importProperties = '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpoint" name="txtEndpoint" value="endpoint" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredImportProperty" type="text">' +
                    '       <label id="errorEndpoint" for="txtEndpoint" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointValue" name="txtEndpointValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorEndpointValue" for="txtEndpointValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtEndpoint').attr("disabled", "disabled");
            }
        } else if (importType.toUpperCase() == "KAFKA") {
            if (!$('#txtMetadataBrokerList').length) {
                importProperties += '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="18" id="txtMetadataBrokerList" name="txtMetadataBrokerList" value="metadata.broker.list" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredImportProperty" type="text">' +
                    '       <label id="errorMetadataBrokerList" for="txtMetadataBrokerList" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="18 " id="txtMetadataBrokerListValue" name="txtMetadataBrokerListValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorMetadataBrokerListValue" for="txtMetadataBrokerListValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtMetadataBrokerList').attr("disabled", "disabled");
            }
        } else if (importType.toUpperCase() == "JDBC") {
            if (!$('#txtJdbcUrl').length) {
                importProperties += '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcUrl" name="txtJdbcUrl" value="jdbcurl" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredImportProperty" type="text">' +
                    '       <label id="errorJdbcUrl" for="txtJdbcUrl" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcUrlValue" name="txtJdbcUrlValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorJdbcUrlValue" for="txtJdbcUrlValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtJdbcUrl').attr("disabled", "disabled");
            }
            if (!$('#txtJdbcDriver').length) {
                importProperties += '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcDriver" name="txtJdbcDriver" value="jdbcdriver" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredImportProperty" type="text">' +
                    '       <label id="errorJdbcDriver" for="txtJdbcDriver" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtJdbcDriverValue" name="txtJdbcDriverValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorJdbcDriverValue" for="txtJdbcDriverValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtJdbcDriver').attr("disabled", "disabled");
            }
        } else if (importType.toUpperCase() == "RABBITMQ") {
            if (!$('#selectRabbitMq').length) {
                importProperties += '' +
                    '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <select id="selectRabbitMq" name="selectRabbitMq" class="newStreamImportPropertyName newStreamImportProperty  requiredImportProperty"> ' +
                    '           <option>broker.host</option> ' +
                    '           <option>amqp.uri</option> ' +
                    '       </select>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtRabbitMqValue" name="txtRabbitMqValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorRabbitMqValue" for="txtRabbitMqValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            }

        } else if (importType.toUpperCase() == "ELASTICSEARCH") {
            if (!$('#txtEndpointES').length) {
                importProperties = '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointES" name="txtEndpointES" value="endpoint" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredImportProperty" type="text">' +
                    '       <label id="errorEndpoint" for="txtEndpoint" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtEndpointESValue" name="txtEndpointESValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorEndpointESValue" for="txtEndpointESValue" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td></td>' +
                    '</tr>';
            } else {
                $('#txtEndpointES').attr("disabled", "disabled");
            }
        }
        $('#tblAddNewImportProperty tr.headerProperty').after(importProperties);

        removeDuplicateImportProperty();
        setDefaultImportProperty();
    };

var removeDuplicateImportProperty = function () {
    $('#tblAddNewImportProperty :input').each(function () {
        if ($(this).val() == "outdir") {
            removeDuplicateImport(this, "outdir");
        } else if ($(this).val() == "nonce") {
            removeDuplicateImport(this, "nonce");
        } else if ($(this).val() == "type") {
            removeDuplicateImport(this, "type");
        } else if ($(this).val() == "endpoint") {
            removeDuplicateImport(this, "endpoint");
        } else if ($(this).val() == "metadata.broker.list") {
            removeDuplicateImport(this, "metadata.broker.list");
        } else if ($(this).val() == "jdbcurl") {
            removeDuplicateImport(this, "jdbcurl");
        } else if ($(this).val() == "jdbcdriver") {
            removeDuplicateImport(this, "jdbcdriver");
        } else if ($(this).val() == "broker.host") {
            removeDuplicateImport(this, "broker.host");
        } else if ($(this).val() == "amqp.uri") {
            removeDuplicateImport(this, "amqp.uri");
        }
    });
};

var removeDuplicateImport = function (object, propertyName) {
    var exportType = $('#txtImportType').val();
    if (!$(object).hasClass("requiredImportProperty")) {
        var val = $(':input:eq(' + ($(':input').index(object) + 1) + ')').val();
        if ($(VdmUI.newStreamMinmPropertyName[propertyName]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName]).val(val);
            $(".newStreamMinImportProperty").addClass("addedImportProperty");
            var $row = $(object).closest("tr");
            $row.remove();
        } else if ($(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).val(val);
            $(".newStreamMinImportProperty").addClass("addedImportProperty");
            var $row1 = $(object).closest("tr");
            $row1.remove();
        }
        if (propertyName == "broker.host" || propertyName == "amqp.uri") {
            $("#selectRabbitMq").val(propertyName);
        }
    }
};

var setDefaultImportProperty = function () {

    var exportType = $('#txtImportType').val();
    if (exportType.toUpperCase() == "FILE") {
        setDefaultImportDisplay($("#txtOutdir"));
        setDefaultImportDisplay($("#txtnonce"));
        setDefaultImportDisplay($("#txtFileType"));
    } else {
        setNormalImportDisplay($("#txtOutdir"));
        setNormalImportDisplay($("#txtnonce"));
        setNormalImportDisplay($("#txtFileType"));
    }

    if (exportType.toUpperCase() == "HTTP") {
        setDefaultImportDisplay($("#txtEndpoint"));
    } else {
        setNormalImportDisplay($("#txtEndpoint"));
    }

    if (exportType.toUpperCase() == "KAFKA") {
        setDefaultImportDisplay($("#txtMetadataBrokerList"));
    } else {
        setNormalImportDisplay($("#txtMetadataBrokerList"));
    }

    if (exportType.toUpperCase() == "JDBC") {
        setDefaultImportDisplay($("#txtJdbcUrl"));
        setDefaultImportDisplay($("#txtJdbcDriver"));
    } else {
        setNormalImportDisplay($("#txtJdbcUrl"));
        setNormalImportDisplay($("#txtJdbcDriver"));
    }

    if (exportType.toUpperCase() == "RABBITMQ") {
        setDefaultImportDisplay($("#selectRabbitMq"));
    } else {
        setNormalImportDisplay($("#selectRabbitMq"));
    }

    if (exportType.toUpperCase() == "ELASTICSEARCH") {
        setDefaultImportDisplay($("#txtEndpointES"));
    } else {
        setNormalImportDisplay($("#txtEndpointES"));
    }

};

var setDefaultImportDisplay = function (txtbox) {
    if (txtbox.selector != "#selectRabbitMq")
        txtbox.attr('disabled', 'disabled');
    var $row = txtbox.closest("tr");
    $('#tblAddNewImportProperty tr.headerProperty').after($row);
    var $td = $row.find("td:last-child");
    $td.html('');
};

var setNormalImportDisplay = function (txtbox) {
    txtbox.removeAttr('disabled');
    var $row = txtbox.closest("tr");
    var $td = $row.find("td:last-child");
    $td.html('<div class="popDelete" onclick="deleteRow(this)"></div>');
};

function showHideConnectorClass() {
    if ($('#txtExportType').val() == "CUSTOM") {
        $("#trExportConnectorClass").show();
    } else {
        $("#trExportConnectorClass").hide();
    }
};

function showHideImportConnectorClass(){
     if ($('#txtImportType').val() == "CUSTOM") {
        $("#trImportStream").show();
    } else {
        $("#trImportStream").hide();
    }
}
var orgUser = '';
var loadPage = function() {
/*
  $('#updateServerList').on('shown.bs.modal', function(e) {
        debugger;
        var pos = $('#tdHostname_14').offset()
        var modal = $(this);

        modal.css('margin-top', pos.top )
             .css('margin-left', pos.left)

        return this;
    });*/
    $(".dropdown").on("show.bs.dropdown", function() {
        var $btnDropDown = $(this).find(".dropdown-toggle");
        var $listHolder = $(this).find(".dropdown-menu");
        //reset position property for DD container
        $(this).css("position", "static");
        $listHolder.css({
          "top": ($btnDropDown.offset().top + $btnDropDown.outerHeight(true)) + "px",
          "left": $btnDropDown.offset().left + "px"
        });
        $listHolder.data("open", true);
      });
      //add BT DD hide event
    $(".dropdown").on("hidden.bs.dropdown", function() {
      var $listHolder = $(this).find(".dropdown-menu");
      $listHolder.data("open", false);
    });

    $(".ContentServerBody").scroll(function() {
      var $ddHolder = $(this).find(".dropdown")
      var $btnDropDown = $(this).find(".dropdown-toggle");
      var $listHolder = $(this).find(".dropdown-menu");
      if ($listHolder.data("open")) {
        $listHolder.css({
          "top": ($btnDropDown.offset().top + $btnDropDown.outerHeight(true)) + "px",
          "left": $btnDropDown.offset().left + "px"
        });
        $ddHolder.toggleClass("open", ($btnDropDown.offset().left > $(this).offset().left))
      }
    })
    //Retains the current tab while page refreshing.
    var retainCurrentTab = function () {
        var curTab = VdmUI.getCookie("current-tab");
        if (curTab != undefined) {
            curTab = curTab * 1;
            if (curTab == NavigationTabs.ServerSetting) {
                $("#overlay").show();
                setTimeout(function () { $("#serverSetting").trigger("click"); }, 100);
            }
        }
    };
    retainCurrentTab();

    VdmUI.showHideUploadConfiguration(true);

    var validationRules = {
        ServerNameRule: {
            //checkDuplicateServer: [],
            regex: /^[a-zA-Z0-9_.-]+$/
        },
        ServerNameMessage: {
            //checkDuplicateServer: 'This server name already exists.',
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        HostNameRule:{
            required: true,
            //checkDuplicateHost: [],
            regex: /^[a-zA-Z0-9_.-]+$/
        },
        HostNameMessage: {
            required: "This field is required",
            //checkDuplicateHost:'This host name already exists.',
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        PortRule:{
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        PortMessage:{
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        HttpPortRule:{
            checkDuplicateHttpListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        HttpPortMessage:{
//            checkDuplicateHttpListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        AdminPortRule:{
            checkDuplicateAdminListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        AdminPortMessage:{
//            checkDuplicateAdminListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        ClientPortRule:{
            checkDuplicateClientListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        ClientPortMessage:{
//            checkDuplicateClientListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        InternalPortRule:{
            checkDuplicateInternalListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        InternalPortMessage:{
//            checkDuplicateInternalListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        ZookeeperPortRule:{
            checkDuplicateZookeeperListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        ZookeeperPortMessage:{
//            checkDuplicateZookeeperListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        ReplicationPortRule:{
            checkDuplicateReplicationListener: [],
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        ReplicationPortMessage:{
//            checkDuplicateReplicationListener: 'This port already exists',
            portRegex : "Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))"
        },
        IpRule:{
            regex: /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        IpMessage:{
            regex: 'Please enter a valid IP address.'
        },
        DatabaseNameRule:{
            required: true,
            checkDuplicateDb: [],
            regex: /^[a-zA-Z0-9_.]+$/
        },
        DatabaseNameMessage:{
            required: "This field is required.",
            checkDuplicateDb: 'This database already exists.',
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        NumericRules: {
            required: true,
            min: 1,
            max: INT_MAX_VALUE,
            digits: true,
        },
        NumericMessages: {
            required: "Please enter a valid positive number.",
            min: "Please enter a positive number. Its minimum value should be 1.",
            max: "Please enter a positive number between 1 and " + INT_MAX_VALUE + ".",
            digits: "Please enter a positive number without any decimal."
        },
        FileNameRules: {
            required: true,
            minlength: 2,
            regex: /^[a-zA-Z0-9_.]+$/
        },
        FileNameMessages: {
            required: "Please enter a valid file name.",
            minlength: "Please enter at least 2 characters.",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },

        DirectoryPathRules: {
            required: true,
            minlength: 2,
        },
        DirectoryPathMessages: {
            required: "Please enter a valid directory path.",
            minlength: "Please enter at least 2 characters.",
        },

        RestoreSnapshotRules: {
            required: true
        },
        RestoreSnapshotMessages: {
            required: "Please select a snapshot to restore."
        },

        StreamNameRules: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/
        },
        StreamNameMessages: {
            required: "This field is required",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },

        UserNameRule: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/,
            checkDuplicate: []
        },
        UserNameMessage: {
            required: "This field is required",
            regex: 'Only alphabets, numbers, _ and . are allowed.',
            checkDuplicate: 'This username already exists.'
        },
        PasswordRule: {
            required: true

        },
        PasswordMessage: {
            required: "This field is required",
        },

        QueryTimeoutRules: {
            required: true,
            min: 0,
            max: INT_MAX_VALUE,
            digits: true,
        },
        QueryTimeoutMessages: {
            required: "Please enter a valid positive number.",
            min: "Please enter a positive number.",
            max: "Please enter a positive number between 0 and " + INT_MAX_VALUE + ".",
            digits: "Please enter a positive number without any decimal."
        },
        streamNameRules: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/
        },
        streamNameMessages: {
            required: "This field is required",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        checkBoxRules: {
            required: true,
            minlength: 1
        },
        checkBoxMessages: {
            required: 'No servers selected.',
            minlength: 'No servers selected.'
        },
        SitesperhostRules: {
            required: true,
             min: 0,
             max: 15,
             digits: true
        },
        SitesperhostMessages: {
            required: "This field is required",
            min: "Please enter a positive number.",
            max: "Please enter a positive number between 0 and 15.",
            digits: "Please enter a positive number without any decimal."
        },
         KSafetyRules: {
            required: true,
             min: 0,
             max: 2,
             digits: true
        },
        KSafetyMessages: {
            required: "This field is required",
            min: "Please enter a positive number.",
            max: "Please enter a positive number between 0 and 2.",
            digits: "Please enter a positive number without any decimal."
        },
        FrequencyRules:{
            required: true,
            min: 0,
            max: 1000,
            digits: true
        },
        FrequencyMessages:{
            required: "This field is required",
            min: "Please enter a positive number.",
            max: "Please enter a positive number between 0 and 1000.",
            digits: "Please enter a positive number without any decimal."
        },
        LogSegmentRules:{
         required: true,
            min: 3,
            max: 3000,
            digits: true
        },
        LogSegmentMessages:{
            required: "This field is required",
            min: "Please enter a number greater than 2",
            max: "Please enter a positive number between 3 and 3000.",
            digits: "Please enter a positive number without any decimal."
        },
        SnapshotPriorityRules:{
         required: true,
            min: 0,
            max: 10,
            digits: true
        },
        SnapshotPriorityMessages:{
            required: "This field is required",
            min: "Please enter a number greater than 1",
            max: "Please enter a positive number between 1 and 10.",
            digits: "Please enter a positive number without any decimal."
        },
        ConnectionSourceServerRules:{
             required: true,
        },
        ConnectionSourceServerMessages:{
            required: "This field is required when connection source is added.",
        },
        DrPortRules: {
            min: 0,
            max: 65535,
            digits: true,
        },
        DrPortMessages:{
            min: "Please enter a number greater than 1",
            max: "Please enter a positive number between 1 and 65535.",
            digits: "Please enter a positive number without any decimal."
        },
        RoleRules: {
            required: true,
            regex: /^[a-zA-Z0-9_.,-\s]+$/
        },
        RoleMessages:{
            required: "This field is required.",
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        }
    }

    $.validator.addMethod(
        "checkDuplicate",
        function (value) {
            var arr = VdmUI.orgUserList;
            if (VdmUI.userEditId != -1) {
                if ($.inArray(value, arr) != -1) {
                    if (value == orgUser)
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

    clusterDirectoryObjects = {
        txtVoltdbRootDir: $('#txtVoltdbRootDir'),
        txtSnapshotDir: $('#txtSnapshotDir'),
        txtExportOverflowDir: $('#txtExportOverflowDir'),
        txtCommandLogDir: $('#txtCommandLogDir'),
        txtCommandLogSnapDir: $('#txtCommandLogSnapDir'),
        txtDrOverflow: $('#txtDrOverflow')
    }
    clusterObjects = {
        txtSitePerHost: $('#txtSitePerHost'),

        txtKSafety: $('#txtKSafety'),

        //Partition Detection
        chkPartitionDetection: $('#chkPartitionDetection'),
        txtPartitionDetection: $('#txtPartitionDetection'),

        //Security
        chkSecurity: $('#chkSecurity'),
        txtSecurity: $('#txtSecurity'),

        //Http Access
        chkHttpAccess: $('#chkHttpAccess'),
        txtHttpAccess: $('#txtHttpAccess'),

        chkJsonApi: $('#chkJsonApi'),
        txtJsonApi: $('#txtJsonApi'),

        //Auto Snapshot
        chkAutoSnapshot: $('#chkAutoSnapshot'),
        txtAutoSnapshot: $('#txtAutoSnapshot'),
        txtFilePrefix: $('#txtFilePrefix'),
        txtFrequency: $('#txtFrequency'),
        txtFrequencyUnit: $('#txtFrequencyUnit'),
        txtRetained: $('#txtRetained'),

        //Command Log
        chkCommandLog: $('#chkCommandLog'),
        txtCommandLog: $('#txtCommandLog'),
        txtLogFrequencyTime: $('#txtLogFrequencyTime'),
        txtLogFreqTransaction: $('#txtLogFreqTransaction'),
        txtLogSegmentSize: $('#txtLogSegmentSize'),
        txtHeartbeatTimeout: $('#txtHeartbeatTimeout'),
        txtQueryTimeout: $('#txtQueryTimeout'),
        txtMaxTempTableMemory: $('#txtMaxTempTableMemory'),
        txtSnapshotPriority: $('#txtSnapshotPriority'),
        txtMemoryLimit: $('#txtMemoryLimit'),

        //: $('#'),

        //DR
        chkDrOnOff: $('#chkDrOnOff'),
        txtDrId: $('#txtDrId'),
        lblDrId: $('#lblDrId'),
        lblDrType: $('#lblDrType'),
        chkDrOnOffVal: $('#chkDrOnOffVal'),
        txtDatabase: $('#txtDatabase'),
        txtDrPort: $('#txtDrPort'),
        errorDrPort: $('#errorDrPort'),
        lblDrPort: $('#lblDrPort'),
        rowDrPort: $('#rowDrPort'),
    }

    $.validator.addMethod(
        "checkDuplicateServer",
        function (value) {
            var arr = VdmUI.CurrentServerList;
            if (VdmUI.isServerCreate == false) {
                if ($.inArray(value, arr) != -1) {
                    if (value == VdmUI.serverToUpdate || value == '')
                        return true;
                    return false;
                } else {
                    return true;
                }
            } else {
                if ($.inArray(value, arr) != -1 && value.trim() != '') {
                    return false;
                } else {
                    return true;
                }
            }
        },
        "Server name already exists."
    );

    $.validator.addMethod(
        "checkDuplicateHost",
        function (value) {
            var arr = VdmUI.CurrentHostList;
            if (VdmUI.isServerCreate == false) {
                if ($.inArray(value, arr) != -1) {
                    if (value == VdmUI.hostToUpdate)
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
        "Host name already exists."
    );

    $.validator.addMethod(
        "checkDuplicateHttpListener",
        function (value) {
           return CheckPort(value, "httplistener", VdmUI.httplistenerToUpdate, VdmUI.defaultPorts.httplistener, "checkDuplicateHttpListener")
        },
        "Port already exists."
    );

    $.validator.addMethod(
        "checkDuplicateAdminListener",
        function (value) {
           return CheckPort(value, "adminlistener", VdmUI.adminlistenerToUpdate, VdmUI.defaultPorts.adminlistener, "checkDuplicateAdminListener")
        },
        "Port already exists."
    );

    $.validator.addMethod(
        "checkDuplicateInternalListener",
        function (value) {
           return CheckPort(value, "internallistener", VdmUI.internallistenerToUpdate, VdmUI.defaultPorts.internallistener, "checkDuplicateInternalListener")
        },
        "Port already exists."
    );

    $.validator.addMethod(
        "checkDuplicateZookeeperListener",
        function (value) {
           return CheckPort(value, "zookeeperlistener",VdmUI.zookeeperlistenerToUpdate, VdmUI.defaultPorts.zookeeperlistener, "checkDuplicateZookeeperListener")
        },
        "Port already exists."
    );

    $.validator.addMethod(
        "checkDuplicateClientListener",
        function (value) {
           return CheckPort(value, "clientlistener", VdmUI.clientlistenerToUpdate, VdmUI.defaultPorts.clientlistener,"checkDuplicateClientListener")
        },
        "Port already exists."
    );

    $.validator.addMethod(
        "checkDuplicateReplicationListener",
        function (value) {
           return CheckPort(value, "replicationlistener",VdmUI.replicationlistenerToUpdate, VdmUI.defaultPorts.replicationlistener, "checkDuplicateReplicationListener")
        },
        "Port already exists."
    );

    function AddSpecifiedPorts(option){
            var clientlistener = GetPort($("#txtClientPort").val());
            var adminlistener = GetPort($("#txtAdminPort").val());
            var internallistener = GetPort($("#txtInternalPort").val());
            var httplistener = GetPort($("#txtHttpPort").val());
            var zookeeperlistener = GetPort($("#txtZookeeper").val());
            var replicationlistener = GetPort($("#txtReplicationPort").val());

        if (option == "adminlistener"){
                    if (clientlistener != "" )
                    {
                        VdmUI.portList.push(clientlistener);
                    }
                     if (internallistener != "")
                    {
                        VdmUI.portList.push(internallistener);
                    }
                    if (httplistener != "")
                    {
                        VdmUI.portList.push(httplistener);
                    }
                     if (zookeeperlistener != "")
                    {
                        VdmUI.portList.push(zookeeperlistener);
                    }
                     if (replicationlistener != "")
                    {
                        VdmUI.portList.push(replicationlistener);
                    }
            }
        if (option == "clientlistener"){
                if (adminlistener != "" )
                {
                    VdmUI.portList.push(adminlistener);
                }
                 if (internallistener != "")
                {
                    VdmUI.portList.push(internallistener);
                }
                if (httplistener != "")
                {
                    VdmUI.portList.push(httplistener);
                }
                 if (zookeeperlistener != "")
                {
                    VdmUI.portList.push(zookeeperlistener);
                }
                 if (replicationlistener != "")
                {
                    VdmUI.portList.push(replicationlistener);
                }
        }
        if (option == "internallistener"){
                if (adminlistener != "" )
                {
                    VdmUI.portList.push(adminlistener);
                }
                 if (clientlistener != "")
                {
                    VdmUI.portList.push(clientlistener);
                }
                if (httplistener != "")
                {
                    VdmUI.portList.push(httplistener);
                }
                 if (zookeeperlistener != "")
                {
                    VdmUI.portList.push(zookeeperlistener);
                }
                 if (replicationlistener != "")
                {
                    VdmUI.portList.push(replicationlistener);
                }
        }
        if (option == "httplistener"){
                if (adminlistener != "" )
                {
                    VdmUI.portList.push(adminlistener);
                }
                 if (clientlistener != "")
                {
                    VdmUI.portList.push(clientlistener);
                }
                if (internallistener != "")
                {
                    VdmUI.portList.push(internallistener);
                }
                 if (zookeeperlistener != "")
                {
                    VdmUI.portList.push(zookeeperlistener);
                }
                 if (replicationlistener != "")
                {
                    VdmUI.portList.push(replicationlistener);
                }
        }
        if (option == "zookeeperlistener")  {
                if (adminlistener != "" )
                {
                    VdmUI.portList.push(adminlistener);
                }
                 if (clientlistener != "")
                {
                    VdmUI.portList.push(clientlistener);
                }
                if (internallistener != "")
                {
                    VdmUI.portList.push(internallistener);
                }
                 if (httplistener != "")
                {
                    VdmUI.portList.push(httplistener);
                }
                 if (replicationlistener != "")
                {
                    VdmUI.portList.push(replicationlistener);
                }
        }
        if (option == "replicationlistener"){
                if (adminlistener != "" )
                {
                    VdmUI.portList.push(adminlistener);
                }
                 if (clientlistener != "")
                {
                    VdmUI.portList.push(clientlistener);
                }
                if (internallistener != "")
                {
                    VdmUI.portList.push(internallistener);
                }
                 if (httplistener != "")
                {
                    VdmUI.portList.push(httplistener);
                }
                 if (zookeeperlistener != "")
                {
                    VdmUI.portList.push(zookeeperlistener);
                }
        }
    }


    function GetPort(value){
        if (value!="")
        {
            if(value.indexOf(":") > 0){
                var arr = value.split(":")
                return arr[1]
            }
            else{
                return value;
            }
        }
        return value;
    }


    function CheckPort(value, option, valueToUpdate, defaultPort, optionmessage){
            value = GetPort(value);

            server = VdmUI.ServerList;
            VdmUI.portList = [];
            var hostname = $("#txtHostName").val();

            AddSpecifiedPorts(option);


            for(var i=0; i<= server.length -1; i++)
            {
                if (server[i][option] == "" )
                {
                    server[i][option] = defaultPort;
                }

                if(hostname == server[i].hostname)
                {
                    if (VdmUI.serverIdToUpdate != server[i].id){
                        $.each(VdmUI.defaultPorts, function (key, data) {
                            VdmUI.portList.push(GetPort(server[i][key]));
                        })

                    }

                 }

            }

            if (VdmUI.isServerCreate == false) {
                    if ($.inArray(value, VdmUI.portList) != -1) {
                        if (value == valueToUpdate)
                            return true;

                        if (value == defaultPort || value== ""){
                            $.validator.messages[optionmessage] = "Default port already exists";
                        }
                        else{
                            $.validator.messages[optionmessage] = "This port already exists.";
                        }

                        return false;
                    } else {
                        return true;
                    }
            } else {
                if (value == ""){
                    value = defaultPort;
                }
                if ($.inArray(value, VdmUI.portList) != -1) {

                      if (value == defaultPort || value== ""){
                            $.validator.messages[optionmessage] = "Default port already exists";
                        }
                        else{
                            $.validator.messages[optionmessage] = "This port already exists.";
                        }
                    return false;
                } else {
                    return true;
                }
            }

    }

    $.validator.addMethod(
        "checkDuplicateDb",
        function (value) {
            var arr = VdmUI.CurrentDbList;
            if (VdmUI.isDbCreate == false) {
                if ($.inArray(value, arr) != -1) {
                    if (value == VdmUI.dbToUpdate)
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
        "Database already exists."
    );

    $.validator.addMethod(
        "regex",
        function (value, element, regexp) {
            var re = new RegExp(regexp);
            return this.optional(element) || re.test(value);
        },
        "Please enter only valid characters."
    );

    $.validator.addMethod(
        "portRegex",
        function(value, element, regexp){
            var result = true
            var values = value.split(':');
            var re = new RegExp(regexp);
            if(values.length == 1){
                if(!$.isNumeric(values[0]) || !(values[0] > 1 && values[0] < 65536))
                    result = false;
                else{
                    if(values[0].split('.').length > 1)
                        result = false;
                }
            } else if(values.length == 2){
                if(!$.isNumeric(values[1]) || !(values[1] > 1 && values[1] < 65536))
                    result = false;
                else{
                    if(values[1].split('.').length > 1)
                        result = false;
                }
                if(!re.test(values[0]))
                    result = false;
            } else {
                result = false;
            }

            return this.optional(element) || result;
        },
        "Please enter only valid character."
    );


    var dbData = {
        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
    }
    VdmService.GetMemberList(function(connection){
        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
    }, dbData);

    setInterval(function () {
        var dbData = {
            id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
        }
        VdmService.GetMemberList(function(connection){
            VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
            VdmUI.Members = connection.Metadata['MEMBER_LISTING']
        }, dbData);
    }, 5000);

    VdmService.GetDatabaseList(function(connection){
        VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
    });

    RefreshDirectories();

    setInterval(function(){
        RefreshDirectories();
    },5000)

    var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }
    setInterval(function () {
        VdmService.GetDatabaseList(function(connection){
            VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
        });
    }, 5000);


    VdmService.GetDeploymentUsers(function(connection){
        VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']
         VdmService.GetDeployment(function(connection){
            VdmUI.Deployment = connection.Metadata['DEPLOYMENT'];
            VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
         },dbData);
      },dbData)

    setInterval(function () {
        var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }

        VdmService.GetDeploymentUsers(function(connection){
        VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']

         VdmService.GetDeployment(function(connection){
            VdmUI.Deployment = connection.Metadata['DEPLOYMENT'];
            VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
         },dbData);
      },dbData)
    }, 5000);

    function StartCluster(){

        var isValidClusterParameters = VdmUI.CheckValidClusterParameters();

        if(!isValidClusterParameters){
            $('#errorMsg').html("Cluster parameters failed validation: The cluster has more hosts and sites per hosts than required for the requested k-safety value. The number of total sites (sitesPerHost * hostCount) must be a whole multiple of the number of copies of the database (k-safety + 1).")
            $('#errorDialog').modal('show');
            return false;
        }


        localStorage.isStarting = 'true'
        localStorage.shutdownDialogState = 'false'
        VdmUI.haltRequest = true;
        VdmUI.isRunning = false;
        VdmUI.isStartServer = false;

        $(".clusterBtnWrap").hide();
        $(".loadingStart").show();

        var pause = false;
        if($("#pauseOption").prop('checked') == true){
            pause = true
        }else{
            pause = false
        }

        var dbInfo = {
                        data:{},
                        id: VdmUI.getCurrentDbCookie(),
                        pause: pause,
                     }
        if(VdmUI.isFreshStart){
            dbInfo.force = true
        } else {
            dbInfo.force = false
        }
        VdmService.StartCluster(function(connection){
            if(connection.Metadata['VDM_START_CLUSTER'].statusString.indexOf('Start request sent successfully') >=0)
            {
                VdmUI.isRunning = true;
                VdmUI.haltRequest = false;
                VdmUI.GetDatabaseStatus(true);
            }
            else {
                startStatus = JSON.parse(connection.Metadata['VDM_START_CLUSTER']['statusString'])
                if(startStatus.dbStatus.status == "stopped"){  //Failed Cluster
                    var msg = '<p>Cluster startup did not succeed on all servers.</p>';
                    $.each(startStatus.dbStatus.serverStatus, function(k, v) {
                        //display the key and value pair
                        if(v[Object.keys(v)[0]].status=="stopped"){
                            var ip = Object.keys(v)[0];
                            msg = msg + '<ul class="listStalled">' +
                            '<li class="pullback"><span class="redC"><strong>X</strong></span> '+ ip +'<span class="redC left-space"> <a href="javascript:void(0);" data-toggle="modal" data-target="#plusError'+ k +'" >error</a></span><div id="plusError'+ k +'" class="modal fade in" role="dialog">' +
                            '<div class="modal-dialog modal-dialog-center modal-add-user"><div class="modal-content"><div class="modal-header">' +
                            '<h4 class="modal-title" id="errorHeader">Error Detail</h4></div><div class="modal-body"><div class="errorWrapper">' +
                            '"'+v[Object.keys(v)[0]].details+'"</div></div>      <div class="modal-footer"><button id="test" type="button" class="btnPopup btn" data-dismiss="modal">OK</button>' +
                            '</div></div></div></div></li></ul>'
                        }
                        else
                        {
                            msg = msg + '<ul class="listStalled">' +
                            '<li class="pullback"><span class="cross-spacer"></span>'+  Object.keys(v)[0]+'</li></ul>'
                        }
                    });
                    ShowHideDatabaseStatus("failed")
                    if(VdmUI.isErrorPopupShowing==false)
                        $(".containerFailed").html(msg);

                }
                else if(startStatus.dbStatus.status == "stalled"){
                    var msg = '<p>Cluster startup did not succeed on all servers.</p>';
                    $.each(startStatus.dbStatus.serverStatus, function(k, v) {
                        //display the key and value pair
                        if(v[Object.keys(v)[0]].status!="running"){
                            var ip = Object.keys(v)[0];
                                msg = msg + '<ul class="listStalled">' +
                            '<li class="pullback"><span class="redC"><strong>X</strong></span> '+ ip +'<span class="redC left-space"> <a href="javascript:void(0);" data-toggle="modal" data-target="#plusError'+ k +'" >error</a></span><div id="plusError'+ k +'" class="modal fade in" role="dialog">' +
                            '<div class="modal-dialog modal-dialog-center modal-add-user"><div class="modal-content"><div class="modal-header">' +
                            '<h4 class="modal-title" id="errorHeader">Error Detail</h4></div><div class="modal-body"><div class="errorWrapper">' +
                            '"'+v[Object.keys(v)[0]].details+'"</div></div>      <div class="modal-footer"><button id="test" type="button" class="btn" data-dismiss="modal">OK</button>' +
                          '</div></div></div></div></li></ul>'
                        } else {
                            msg = msg + '<ul class="listStalled">' +
                            '<li class="pullback"><span class="cross-spacer"></span>'+  Object.keys(v)[0]+'</li></ul>'
                        }

                    });
                    if(VdmUI.isErrorPopupShowing==false)
                        $(".containerStalled").html(msg);
                    ShowHideDatabaseStatus('stalled')

                }
                else{
                    ShowHideDatabaseStatus("running")
                }
            }
            localStorage.isStarting = 'false'
        }, dbInfo, true);
    }

    function RefreshDirectories(){
        var serverData = {
                        id:VdmUI.getCurrentDbCookie(),
                        server_id: VdmUI.getCurrentServerCookie()
                    }

        if (VdmUI.getCurrentServerCookie() != -1){
            VdmService.GetServer(function(connection){

                var serverInfo = connection.Metadata['GET_SERVER']
                if(serverInfo['server'] == undefined){
                    saveCookie("selected-serverId", 0);
                    VdmUI.displayDefaultPaths();
                }
                else
                {
                    VdmUI.CurrentServerInfo = serverInfo['server'];
                    if(!$('#txtVoltdbRootDir').is(":focus")){
                        $("#txtVoltdbRootDir").val(serverInfo['server']['voltdbroot'])
                    }
                    if(!$('#txtSnapshotDir').is(":focus")){
                        $("#txtSnapshotDir").val(serverInfo['server']['snapshots'])
                    }
                    if(!$('#txtExportOverflowDir').is(":focus")){
                        $("#txtExportOverflowDir").val(serverInfo['server']['exportoverflow'])
                    }
                    if(!$('#txtCommandLogDir').is(":focus")){
                        $("#txtCommandLogDir").val(serverInfo['server']['commandlog'])
                    }
                    if(!$('#txtCommandLogSnapDir').is(":focus")){
                        $("#txtCommandLogSnapDir").val(serverInfo['server']['commandlogsnapshot'])
                    }
                    if(!$('#txtDrOverflow').is(":focus")){
                        $("#txtDrOverflow").val(serverInfo['server']['droverflow'])
                    }
                }

            },serverData)
        }
        else{
            saveCookie("selected-serverId", 0);
            VdmUI.displayDefaultPaths()
        }
    }

    $('#btnStartCluster').on('click', function(){

        if($("#startOption").prop('checked') == true){
            $("#overwriteConfirmation").modal('show')
        }
        else{
            StartCluster()
        }

    })

    $('#overwriteConfirmation').on('focusout', function(){
        $("#startOption").attr('checked',false)
    });

    $("#btnOverwriteYes").on('click', function(){
        VdmUI.isFreshStart = true;
        StartCluster()
        $("#startOption").attr('checked',false)

    })

    $("#btnOverwriteNo").on('click', function(){
        $("#startOption").attr('checked',false)
    })

    $('#overwriteConfirmation').on('hidden.bs.modal', function () {
  $("#startOption").attr('checked',false)
})

    var ShowHideDatabaseStatus = function(status){

        if(status == "stopped"){
            $("#startSuccess").hide();
            $(".loadingStart").hide();
            $("#startFailed").hide();
            $("#stallFailed").hide();
            $(".clusterBtnWrap").show();
            $(".connectionFail").hide();
        }
        else if (status== "running"){
            $("#startSuccess").show();
            $(".loadingStart").hide();
            $("#startFailed").hide();
            $("#stallFailed").hide();
            $(".clusterBtnWrap").hide()
            $(".connectionFail").hide();
        }
        else if (status== "stalled"){
           $("#startSuccess").hide();
            $(".loadingStart").hide();
            $("#startFailed").hide();
            $("#stallFailed").show();
            $(".clusterBtnWrap").hide();
            $(".connectionFail").hide();
        }
        else if (status== "failed"){
            $("#startSuccess").hide();
            $(".loadingStart").hide();
            $("#startFailed").show();
            $("#stallFailed").hide();
            $(".clusterBtnWrap").hide()
            $(".connectionFail").hide();
        }
    }

    VdmUI.GetDatabaseStatus();
    setInterval(function () {

            if(!VdmUI.haltRequest){
                if(VdmUI.isErrorPopupShowing==false)
                {
                    VdmUI.GetDatabaseStatus();
                }
            }
        }, 5000);

    $("#frmCreateServer").validate({
        ignore: [],
        rules: {
            serverName: validationRules.ServerNameRule,
            txtHostName: validationRules.HostNameRule,
            txtClientPort:validationRules.ClientPortRule,
            txtAdminPort:validationRules.AdminPortRule,
            txtZookeeper:validationRules.ZookeeperPortRule,
            txtReplicationPort:validationRules.ReplicationPortRule,
            txtInternalInterface:validationRules.IpRule,
            txtExternalInterface:validationRules.IpRule,
            txtPublicInterface:validationRules.IpRule,
            txtInternalPort:validationRules.InternalPortRule,
            txtHttpPort:validationRules.HttpPortRule,
        },
        messages: {
            serverName: validationRules.ServerNameMessage,
            txtHostName: validationRules.HostNameMessage,
            txtClientPort:validationRules.ClientPortMessage,
            txtAdminPort:validationRules.AdminPortMessage,
            txtZookeeper:validationRules.ZookeeperPortMessage,
            txtReplicationPort:validationRules.ReplicationPortMessage,
            txtInternalInterface:validationRules.IpMessage,
            txtExternalInterface:validationRules.IpMessage,
            txtPublicInterface:validationRules.IpMessage,
            txtInternalPort:validationRules.InternalPortMessage,
            txtHttpPort:validationRules.HttpPortMessage,
        }
    });

    $("#frmSitePerHost").validate({
        rules:{
        txtSitePerHost: validationRules.SitesperhostRules
        },
        messages:{
        txtSitePerHost: validationRules.SitesperhostMessages
        }
    });

     $("#frmKSafety").validate({
        rules:{
        txtKSafety: validationRules.KSafetyRules
        },
        messages:{
        txtKSafety: validationRules.KSafetyMessages
        }
    });

      $("#frmFilePrefix").validate({
        rules:{
        txtFilePrefix: validationRules.FileNameRules
        },
        messages:{
        txtFilePrefix: validationRules.FileNameMessages
        }
    });

     $("#frmFrequency").validate({
        rules:{
        txtFrequency: validationRules.NumericRules
        },
        messages:{
        txtFrequency: validationRules.NumericMessages
        }
    });

     $("#frmRetained").validate({
        rules:{
        txtRetained: validationRules.NumericRules
        },
        messages:{
        txtRetained: validationRules.NumericMessages
        }
    });

      $("#frmLogFrequencyTime").validate({
        rules:{
        txtLogFrequencyTime: validationRules.FrequencyRules
        },
        messages:{
        txtLogFrequencyTime: validationRules.FrequencyMessages
        }
    });


    $("#frmLogFreqTransaction").validate({
        rules:{
        txtLogFreqTransaction: validationRules.NumericRules
        },
        messages:{
        txtLogFreqTransaction: validationRules.NumericMessages
        }
    });

    $("#frmLogFreqTransaction").validate({
        rules:{
        txtLogFreqTransaction: validationRules.NumericRules
        },
        messages:{
        txtLogFreqTransaction: validationRules.NumericMessages
        }
    });

    $("#frmLogSegmentSize").validate({
        rules:{
        txtLogSegmentSize: validationRules.LogSegmentRules
        },
        messages:{
        txtLogSegmentSize: validationRules.LogSegmentMessages
        }
    });

    $("#frmHeartBeatTimeout").validate({
        rules:{
        txtHeartbeatTimeout: validationRules.NumericRules
        },
        messages:{
        txtHeartbeatTimeout: validationRules.NumericMessages
        }
    });

     $("#frmQueryTimeout").validate({
        rules:{
        txtQueryTimeout: validationRules.NumericRules
        },
        messages:{
        txtQueryTimeout: validationRules.NumericMessages
        }
    });

    $("#frmMaxTempTableMemory").validate({
        rules:{
        txtMaxTempTableMemory: validationRules.NumericRules
        },
        messages:{
        txtMaxTempTableMemory: validationRules.NumericMessages
        }
    });

     $("#frmSnapshotPriority").validate({
        rules:{
        txtSnapshotPriority: validationRules.SnapshotPriorityRules
        },
        messages:{
        txtSnapshotPriority: validationRules.SnapshotPriorityMessages
        }
    });

     $("#frmMemoryLimit").validate({
        rules:{
        txtMemoryLimit: validationRules.MemoryLimitRules
        },
        messages:{
        txtMemoryLimit: validationRules.MemoryLimitMessages
        }
    });

    $("#frmAddUser").validate({
                rules: {
                    txtUser: validationRules.UserNameRule,
                    txtPassword: validationRules.PasswordRule,
                    txtUserRole: validationRules.RoleRules,
                },
                messages: {
                    txtUser: validationRules.UserNameMessage,
                    txtPassword: validationRules.PasswordMessage,
                    txtUserRole: validationRules.RoleMessages
                }
            });

    $('#frmDatabaseReplication').validate({
        rules: {
            txtDrId: validationRules.NumericRules,
            //txtDatabase: validationRules.ConnectionSourceServerRules,
            txtDrPort: validationRules.DrPortRules,
        },
        messages: {
            txtDrId: validationRules.NumericMessages,
            //txtDatabase: validationRules.ConnectionSourceServerMessages,
            txtDrPort: validationRules.DrPortMessages,
        }
    })

    $('#frmDrId').validate({
        rules:{
        txtDrId: validationRules.NumericRules
        },
        messages:{
        txtDrId: validationRules.NumericMessages
        }
    })

    $('#btnCreateServerOk').on('click', function(e){
        if (!$("#frmCreateServer").valid()) {
            e.preventDefault();
            e.stopPropagation();
            if($('#errorServerName').is(':hidden') && $('#errorServerName').is(':hidden')){
                $('#colExpIcon').removeClass('glyphicon-triangle-right').addClass('glyphicon-triangle-bottom glyphicon-triangle-right');
                $('#collapseOne').removeClass('in').addClass('in');
                $('#collapseOne').removeAttr('style')
            }
            return;
        }
        var hostName= $('#txtHostName').val()
        var vdmData = {'serverIp': $('#txtHostName').val()}
        VdmUI.showHideLoader(true);
        if(!VdmUI.isTestServer){
            VdmService.GetVdmStatus(function(connection){
                if(connection['Metadata']['VDM_STATUS'].hasOwnProperty('voltdeploy')){
                    result = connection['Metadata']['VDM_STATUS']['voltdeploy'].running;
                    if(Boolean(result)){
                        var serverInfo ={
                            data:{
                                "name" : $('#serverName').val(),
                                "hostname" : $('#txtHostName').val(),
                                "description" : $('#txtDescription').val(),
                                "client-listener" : $('#txtClientPort').val(),
                                "admin-listener" : $('#txtAdminPort').val(),
                                "internal-listener" : $('#txtInternalPort').val(),
                                "http-listener" : $('#txtHttpPort').val(),
                                "zookeeper-listener" : $('#txtZookeeper').val(),
                                "replication-listener" : $('#txtReplicationPort').val(),
                                "internal-interface" : $('#txtInternalInterface').val(),
                                "external-interface" : $('#txtExternalInterface').val(),
                                "public-interface" : $('#txtPublicInterface').val(),
                                "placement-group" : $('#txtPlacementGroup').val(),
                            },
                            id:VdmUI.getCurrentDbCookie(),
                            server_id: $('#addServer').data('serverid')
                        }
                        var hostId = $('#addServer').data('serverid')
                        if(VdmUI.isServerCreate){
                            VdmService.CreateServer(function(connection){
                                VdmUI.showHideLoader(false);
                                if(connection.Metadata['SERVER_CREATE'].status == 201){

                                    var vdmData1 = {'serverIp': hostName, 'data':{'ip_address':document.location.hostname}}
                                    var dbData = {
                                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                                    }
                                    VdmService.GetMemberList(function(connection){
                                        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                                    }, dbData);
                                } else{
                                    errorMsg = 'Unable to create server. '
                                    if(connection.Metadata['SERVER_CREATE'].hasOwnProperty('errors'))
                                        errorMsg += 'Following error was received. <ul><li style="list-style-type: disc;margin-left: 16px" >' + connection.Metadata['SERVER_CREATE']['errors'] + ' </li></ul>'
                                    else if(connection.Metadata['SERVER_CREATE'].hasOwnProperty('statusString'))
                                        errorMsg += 'Following error was received. <ul><li style="list-style-type: disc;margin-left: 16px" >' + connection.Metadata['SERVER_CREATE']['statusString'] + ' </li></ul>'
                                    $('#errorMsg').html(errorMsg)
                                    $('#errorDialog').modal('show');
                                }
                            },serverInfo);
                        } else {
                            toggleServer(editStates.ShowLoading,hostId)
                            VdmService.UpdateServer(function(connection){
                                VdmUI.showHideLoader(false);
                                if(connection.Metadata['SERVER_UPDATE'].status == 200){
                                    var dbData = {
                                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                                    }
                                    VdmService.GetMemberList(function(connection){
                                        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                                    }, dbData);
                                } else{
                                    errorMsg = 'Unable to update server. '
                                    if(connection.Metadata['SERVER_UPDATE'].hasOwnProperty('errors'))
                                        errorMsg += 'Following error was received. <ul><li style="list-style-type: disc;margin-left: 16px" >' + connection.Metadata['SERVER_UPDATE']['errors'] + ' </li></ul>'
                                    else if(connection.Metadata['SERVER_UPDATE'].hasOwnProperty('statusString'))
                                        errorMsg += 'Following error was received. <ul><li style="list-style-type: disc;margin-left: 16px" >' + connection.Metadata['SERVER_UPDATE']['statusString'] + ' </li></ul>'
                                    $('#errorMsg').html(errorMsg)
                                    $('#errorDialog').modal('show');
                                    toggleServer(editStates.ShowEdit,hostId);
                                }
                            },serverInfo);

                        }
                        VdmUI.resetTextBox();
                    }
                    else{
                        VdmUI.showHideLoader(false);
                        if ($('#serverName').val() == ''){
                            $('#errorMsg').html('Unable to add server \''+ $('#txtHostName').val() +'\' because the VDM is not running in it.')
                        }else{
                            $('#errorMsg').html('Unable to add server \''+ $('#serverName').val() +'\' because the VDM is not running in it.')
                        }
                        $('#errorDialog').modal('show');
                    }
                }
                else{
                    VdmUI.showHideLoader(false);
                      if ($('#serverName').val() == ''){
                            $('#errorMsg').html('Unable to add server \''+ $('#txtHostName').val() +'\' because the VDM is not running in it.')
                        }else{
                            $('#errorMsg').html('Unable to add server \''+ $('#serverName').val() +'\' because the VDM is not running in it.')
                        }
                    $('#errorDialog').modal('show');
                }

            },vdmData);
        } else {
            var serverInfo ={
                data:{
                    "name" : $('#serverName').val(),
                    "hostname" : $('#txtHostName').val(),
                    "description" : $('#txtDescription').val(),
                    "client-listener" : $('#txtClientPort').val(),
                    "admin-listener" : $('#txtAdminPort').val(),
                    "internal-listener" : $('#txtInternalPort').val(),
                    "http-listener" : $('#txtHttpPort').val(),
                    "zookeeper-listener" : $('#txtZookeeper').val(),
                    "replication-listener" : $('#txtReplicationPort').val(),
                    "internal-interface" : $('#txtInternalInterface').val(),
                    "external-interface" : $('#txtExternalInterface').val(),
                    "public-interface" : $('#txtPublicInterface').val(),
                    "placement-group" : $('#txtPlacementGroup').val(),
                },
                id:VdmUI.getCurrentDbCookie(),
                server_id: $('#addServer').data('serverid')
            }
            var hostId = $('#addServer').data('serverid')
            if(VdmUI.isServerCreate){
                VdmService.CreateServer(function(connection){
                    VdmUI.showHideLoader(false);
                    if(connection.Metadata['SERVER_CREATE'].status == 1){

                        var vdmData1 = {'serverIp': hostName, 'data':{'ip_address':document.location.hostname}}
                        var dbData = {
                                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                                    }
                        VdmService.GetMemberList(function(connection){
                            VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                        }, dbData);
                    } else{
                        $('#errorMsg').html('Unable to create server.')
                        $('#errorDialog').modal('show');
                    }
                },serverInfo);
            } else {
                toggleServer(editStates.ShowLoading,hostId)
                VdmService.UpdateServer(function(connection){
                    VdmUI.showHideLoader(false);
                    if(connection.Metadata['SERVER_UPDATE'].status == 1){
                        var dbData = {
                                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                                    }
                        VdmService.GetMemberList(function(connection){
                            VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                        }, dbData);
                    } else{
                        $('#errorMsg').html('Unable to update server.')
                        $('#errorDialog').modal('show');
                        toggleServer(editStates.ShowEdit,hostId);
                    }
                },serverInfo);

            }
            VdmUI.resetTextBox();
        }
    });

    $('#frmCreateDB').validate({
        rules: {
            txtDbName: validationRules.DatabaseNameRule
        },
        messages: {
            txtDbName: validationRules.DatabaseNameMessage
        }

    });

    $('#btnAddDatabaseOk').on('click', function(e){
        if (!$("#frmCreateDB").valid()) {
            e.preventDefault();
            e.stopPropagation();
            return;
        }

        var dbName = $('#txtDbName').val();

        var dbInfo = {
            data:{
                name: dbName,
            },
            id:$('#addDatabase').data('id')
        }
        if(VdmUI.isDbCreate){
            VdmService.CreateDatabase(function(connection){
                if(connection.Metadata['DATABASE_CREATE'].status == 201){
                    VdmService.GetDatabaseList(function(connection){
                        VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
                    })
                } else{
                    $('#errorMsg').html('Unable to create database.')
                    $('#errorDialog').modal('show');
                }
            }, dbInfo);
        } else {
            toggleDatabase(editStates.ShowLoading, $('#addDatabase').data('id'))
            VdmService.UpdateDatabase(function(connection){
                if(connection.Metadata['DATABASE_UPDATE'].status == 200){
                    VdmService.GetDatabaseList(function(connection){
                        VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
                    })
                } else {
                    toggleDatabase(editStates.ShowEdit, $('#addDatabase').data('id'))
                    if(connection.Metadata['DATABASE_UPDATE'].hasOwnProperty('error'))
                        $('#errorMsg').html(connection.Metadata['DATABASE_UPDATE']['error'])
                    else
                        $('#errorMsg').html('Unable to update database.')
                    $('#errorDialog').modal('show');
                }
            }, dbInfo);
        }

    });

    var toggleServer = function (state,hostId){
        if(state == editStates.ShowLoading)
        {
            $("#editServer_"+hostId).hide()
            $("#deleteServer_"+hostId).hide()
            $("#editServerTxt_"+hostId).hide()
            $("#deleteServerTxt_"+hostId).hide()
            $("#loadingServer_"+hostId).show()

        }
        else if (state == editStates.ShowOkCancel) {
            $("#editServer_"+hostId).show()
            $("#deleteServer_"+hostId).show()
            $("#editServerTxt_"+hostId).show()
            $("#deleteServerTxt_"+hostId).show()
            $("#loadingServer_"+hostId).hide()
        }
        else{
            $("#editServer_"+hostId).show()
            $("#deleteServer_"+hostId).show()
            $("#editServerTxt_"+hostId).show()
            $("#deleteServerTxt_"+hostId).show()
            $("#loadingServer_"+hostId).hide()
        }
    }


    var toggleDatabase = function (state,dbId){
        if(state == editStates.ShowLoading){
            $("#editDatabase_" + dbId).hide()
            $("#deleteDatabase_" + dbId).hide()
            $("#editDatabaseTxt_" + dbId).hide()
            $("#deleteDatabaseTxt_" + dbId).hide()
            $("#loadingDatabase_" + dbId).show()
        }
        else if (state == editStates.ShowOkCancel) {
            $("#editDatabase_" + dbId).show()
            $("#deleteDatabase_" + dbId).show()
            $("#editDatabaseTxt_" + dbId).show()
            $("#deleteDatabaseTxt_" + dbId).show()
            $("#loadingDatabase_" + dbId).hide()
        }
        else{
            $("#editDatabase_" + dbId).show()
            $("#deleteDatabase_" + dbId).show()
            $("#editDatabaseTxt_" + dbId).show()
            $("#deleteDatabaseTxt_" + dbId).show()
            $("#loadingDatabase_" + dbId).hide()
        }
    }

    $('#deleteServerOk').on('click',function(){
        $('#errorDialog').modal('hide');
        var serverId = $('#deleteConfirmation').data('serverid');
        var serverData = {
           "id": VdmUI.getCurrentDbCookie(),
           "server_id": serverId
        }
        toggleServer(editStates.ShowLoading,serverId)

        VdmService.GetDatabaseServerStatus(function(connection){

        $('#errorMsg').html('');
        if (connection.Metadata['DATABASE_SERVER_STATUS'].status != "running"){

           VdmService.DeleteServer(function(connection){
                if(connection.Metadata['SERVER_DELETE'].result == true){
                    $("#deleteConfirmation").modal('hide');
                    $('#errorDialog').modal('hide');
                    toggleServer(editStates.ShowEdit, serverId);
                } else if(connection.Metadata['SERVER_DELETE'].hasOwnProperty('statusstring')){
                    $('#errorMsg').html(connection.Metadata['SERVER_DELETE']['statusstring'])
                    $('#errorDialog').modal('show');
                    toggleServer(editStates.ShowEdit, serverId);
                }
                var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }
                VdmService.GetMemberList(function(connection){
                    VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                }, dbData);
            }, serverData);
        }
        else{
            $('#errorMsg').html("Cannot delete a running server.")
            $('#errorDialog').modal('show');
            toggleServer(editStates.ShowEdit, serverId);
        }


        },serverData)


    })

    $('#btnAddDatabase').on('click', function(){
        VdmUI.resetDbForm();
        VdmUI.isDbCreate = true;
        VdmUI.dbToUpdate = '';
        $('#dbTitle').html('Add Database');
    });

    $('#btnDeleteDatabaseOk').on('click', function(){
        var dbId = $('#deleteDatabase').data('id');
        var dbData = {
            id: dbId
        }
        toggleDatabase(editStates.ShowLoading, dbId)
        VdmService.DeleteDatabase(function(connection){
            if(!connection.Metadata['DATABASE_DELETE'].hasOwnProperty('error')){
                VdmService.GetDatabaseList(function(connection){
                    VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
                })
            }else{
                toggleDatabase(editStates.ShowEdit, dbId)
                $('#errorMsg').html('Unable to delete given database.')
                $('#errorDialog').modal('show');
            }
        }, dbData);
    });

    $('#btnSelectDbOk').on('click', function(){
        var values = $('#selectServers').val();
        var mem = []
        for(var i = 0; i < values.length; i++){
            mem.push(parseInt(values[i]));
        }
        var memberInfo = {
            data: {
                members: mem,
            },
            id: VdmUI.getCurrentDbCookie()
        }
        VdmService.UpdateMembers(function(connection){
            var dbData = {
                id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
            }
            VdmService.GetMemberList(function(connection){
                VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
            }, dbData);
        }, memberInfo);
    });

    $("#txtSitePerHost").on('change',function(e){
        UpdateTextBox(this,"sitePerHost","frmSitePerHost",e);
    })

    $("#txtKSafety").on('change',function(e){
        UpdateTextBox(this,'kSafety','frmKSafety',e)
    })

    $("#txtFilePrefix").on('change',function(e){
        UpdateTextBox(this,'filePrefix','frmFilePrefix',e);
    })

    $("#txtFrequency").on('change',function(e){
        UpdateTextBox(this,'frequency','frmFrequency',e);
    })

    $("#selFreqUnit").on('change',function(e){
       if (!$("#frmFrequency").valid()) {
        e.preventDefault();
        e.stopPropagation();
        return;
    }
    else{
                ShowSavingStatus();
                var deploymentInfo = []
                var frequencyUnit = "";
                if($("#selFreqUnit option:selected").val()=="1")
                {
                    frequencyUnit = "h";
                }
                else if($("#selFreqUnit option:selected").val()=="2")
                {
                    frequencyUnit = "m";
                }
                else if($("#selFreqUnit option:selected").val()=="3")
                {
                    frequencyUnit = "s";
                }

                deploymentInfo = {
                        data:{
                            snapshot:{
                                frequency: clusterObjects.txtFrequency.val() + frequencyUnit
                            }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }


             VdmService.SaveDeployment(function(connection){
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                        showHideShutdownDialog(true)
                    },deploymentInfo)
                }, deploymentInfo);

                ShowSavedStatus();

       }
    })

    $("#txtRetained").on('change',function(e){
        UpdateTextBox(this,'retained','frmRetained',e);
    })

    $("#txtLogFrequencyTime").on('change',function(e){
        UpdateTextBox(this,'logFrequencyTime','frmLogFrequencyTime',e);
    })

    $("#txtLogFreqTransaction").on('change',function(e){
        UpdateTextBox(this,'logFrequencyTrans','frmLogFreqTransaction',e);
    })

    $("#txtLogSegmentSize").on('change',function(e){
        UpdateTextBox(this,'logSegmentSize','frmLogSegmentSize',e);
    })

    $("#txtHeartbeatTimeout").on('change',function(e){
        UpdateTextBox(this,'heartBeatTimeout','frmHeartBeatTimeout',e);
    })

     $("#txtQueryTimeout").on('change',function(e){
        UpdateTextBox(this,'queryTimeout','frmQueryTimeout',e);
    })

    $("#txtMaxTempTableMemory").on('change',function(e){
        UpdateTextBox(this,'tempTables','frmMaxTempTableMemory',e);
    })

     $("#txtSnapshotPriority").on('change',function(e){
        UpdateTextBox(this,'snapshotPriority','frmSnapshotPriority',e);
    })

    $("#txtMemoryLimit").on('change',function(e){
        setMemoryLimitValidation();
        UpdateTextBox(this,'memoryLimit','frmMemoryLimit',e);
    })

     $("#selMemoryLimitUnit").on('change',function(e){
            setMemoryLimitValidation();
          if (!$("#frmMemoryLimit").valid()) {
        e.preventDefault();
        e.stopPropagation();
        return;
    }
    else{
        ShowSavingStatus();
        var deploymentInfo = []
            var memorySizeUnit = "";
                if($("#selMemoryLimitUnit option:selected").val()=="GB")
                {
                    memorySizeUnit = "";
                }
                else if($("#selMemoryLimitUnit option:selected").val()=="%")
                {
                    memorySizeUnit = "%";
                }

               var memoryLimit = '';

               if ($("#txtMemoryLimit").val() != "")
               {
                memoryLimit = $("#txtMemoryLimit").val() + memorySizeUnit;
               }

               var memorySize = "";
                    if(memoryLimit!="")
                    {
                        if (memoryLimit.indexOf("%")>-1) {
                            memorySize = parseInt(memoryLimit.replace("%", ""));
                            memoryLimit = memorySize + memorySizeUnit;
                        } else {
                            memoryLimit = encodeURIComponent(parseInt(memoryLimit));
                        }
                    }

                 deploymentInfo = {
                data:{
                    systemsettings:{
                    resourcemonitor:{
                        memorylimit: {
                            "size": memoryLimit
                        }
                    }
                    }

                },
                id: VdmUI.getCurrentDbCookie()
            }


             VdmService.SaveDeployment(function(connection){
                    showHideShutdownDialog(true)
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                    },deploymentInfo)
                }, deploymentInfo);

                ShowSavedStatus();
    }
    })

    $("#txtVoltdbRootDir").on('change',function(e){
        UpdatePathInServerInfo(this,'voltdbrootDir','frmVoltdbRootDir',e);
    })


    $("#txtSnapshotDir").on('change',function(e){
        UpdatePathInServerInfo(this,'snapshotDir','frmSnapshotDir',e);
    })

     $("#txtExportOverflowDir").on('change',function(e){
        UpdatePathInServerInfo(this,'exportOverflowDir','frmExportOverflowDir',e);
    })

    $("#txtCommandLogDir").on('change',function(e){
        UpdatePathInServerInfo(this,'commandlogDir','frmCommandLogDir',e);
    })

    $("#txtCommandLogSnapDir").on('change',function(e){
        UpdatePathInServerInfo(this,'commandlogSnapDir','frmCommandLogSnapDir',e);
    })

    $("#txtDrOverflow").on('change',function(e){
        UpdatePathInServerInfo(this,'drOverflow','frmDrOverflow',e);
    })

//    $('#txtDrId').on('change', function(e){
//        UpdateTextBox(this, 'drId', 'frmDrId', e);
//    });

    $('#chkPartitionDetection').on('ifClicked', function(event){
        UpdateCheckBox(clusterObjects.txtPartitionDetection,'partitionDetection');
    });


    $('#chkHttpAccess').on('ifClicked', function(event){
        UpdateCheckBox(clusterObjects.txtHttpAccess,'httpAccess');
    });


    $('#chkAutoSnapshot').on('ifClicked', function(event){
        UpdateCheckBox(clusterObjects.txtAutoSnapshot,'autoSnapshots');
    });

    $('#chkCommandLog').on('ifClicked', function(event){
        UpdateCheckBox(clusterObjects.txtCommandLog,'commandLog');
    });

    $('#chkJsonApi').on('ifClicked', function(event){
        UpdateCheckBox(clusterObjects.txtJsonApi,'jsonApi');
    });

//    $('#chkDrOnOff').on('ifClicked', function(event){
//        UpdateCheckBox(clusterObjects.chkDrOnOffValue, 'dr')
//    });

    UpdateCheckBox = function(element,item){

        if(element.text()=="Off"){
            element.text("On")
        }
        else if (element.text()=="On"){
            element.text("Off")
        }

            ShowSavingStatus();
            var deploymentInfo = [];
             if(item == "security"){
                 deploymentInfo = {
                    data:{
                        security:{
                            enabled: element.text()=="Off"?false:true
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "partitionDetection"){
                  deploymentInfo = {
                    data:{
                        "partition-detection":{
                            enabled: clusterObjects.txtPartitionDetection.text()=="Off"?false:true
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "httpAccess"){
                deploymentInfo = {
                    data:{
                        httpd:{
                            enabled: clusterObjects.txtHttpAccess.text()=="Off"?false:true
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "autoSnapshots"){
                deploymentInfo = {
                    data:{
                        snapshot:{
                            enabled: clusterObjects.txtAutoSnapshot.text()=="Off"?false:true
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "commandLog"){
                deploymentInfo = {
                    data:{
                        commandlog:{
                            enabled: clusterObjects.txtCommandLog.text()=="Off"?false:true
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "jsonApi"){
                deploymentInfo = {
                    data:{
                        httpd:{
                            jsonapi:{
                                enabled: clusterObjects.txtJsonApi.text()=="Off"?false:true
                            }
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
//             else if (item == "dr"){
//                deploymentInfo = {
//                    data: {
//                            dr:{
//                                enabled: clusterObjects.chkDrOnOffValue.text() == "Off" ? false : true
//                               }
//                    },
//                    id: VdmUI.getCurrentDbCookie()
//                }
//             }

             VdmService.SaveDeployment(function(connection){
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                        showHideShutdownDialog(true);
                    },deploymentInfo)
                }, deploymentInfo);

                ShowSavedStatus();
    }

    UpdateTextBox = function(element,item, form, e){
       var serverId = 0;
       if (!$("#"+form).valid()) {
            e.preventDefault();
            e.stopPropagation();
            return;
        }
        else{

              ShowSavingStatus();
              var deploymentInfo = []
              if(item == "sitePerHost"){
                deploymentInfo = {
                    data:{
                        cluster:{
                            sitesperhost: parseInt(clusterObjects.txtSitePerHost.val())
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
              }
              else if (item == "kSafety")
              {
                 deploymentInfo = {
                        data:{
                            cluster:{
                                kfactor: parseInt(clusterObjects.txtKSafety.val())
                            }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "filePrefix")
              {
                deploymentInfo = {
                        data:{
                            snapshot:{
                                prefix: clusterObjects.txtFilePrefix.val()
                            }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "frequency")
              {
                var frequencyUnit = "";
                if($("#selFreqUnit option:selected").val()=="1")
                {
                    frequencyUnit = "h";
                }
                else if($("#selFreqUnit option:selected").val()=="2")
                {
                    frequencyUnit = "m";
                }
                else if($("#selFreqUnit option:selected").val()=="3")
                {
                    frequencyUnit = "s";
                }

                deploymentInfo = {
                        data:{
                            snapshot:{
                                frequency: clusterObjects.txtFrequency.val() + frequencyUnit
                            }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "retained")
              {
               deploymentInfo = {
                        data:{
                            snapshot:{
                                retain: parseInt(clusterObjects.txtRetained.val())
                            }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
               else if (item == "logFrequencyTime")
              {
               deploymentInfo = {
                        data:{
                           commandlog:{
                                frequency:{
                                    time: parseInt(clusterObjects.txtLogFrequencyTime.val())
                                }
                               }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "logFrequencyTrans")
              {
               deploymentInfo = {
                        data:{
                           commandlog:{
                                frequency:{
                                    transactions: parseInt(clusterObjects.txtLogFreqTransaction.val())
                                }
                               }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "logSegmentSize")
              {
               deploymentInfo = {
                        data:{
                           commandlog:{
                            logsize: parseInt(clusterObjects.txtLogSegmentSize.val())
                        }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "heartBeatTimeout")
              {
               deploymentInfo = {
                        data:{
                          heartbeat:{
                    timeout: parseInt(clusterObjects.txtHeartbeatTimeout.val())
                }
                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
              else if (item == "queryTimeout")
              {
               deploymentInfo = {
                        data:{
                         systemsettings:{
                    query: {
                        timeout: parseInt(clusterObjects.txtQueryTimeout.val())
                    }
                }
                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
               else if (item == "tempTables")
              {
               deploymentInfo = {
                        data:{
                        systemsettings:{
                    temptables:{
                        maxsize: parseInt(clusterObjects.txtMaxTempTableMemory.val())
                    }
                }
                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
               else if (item == "snapshotPriority")
              {
               deploymentInfo = {
                        data:{
                        systemsettings:{

                    snapshot:{
                        priority: parseInt(clusterObjects.txtSnapshotPriority.val())
                    },

                }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
                else if (item == "memoryLimit")
              {
                    var memorySizeUnit = "";
                    if($("#selMemoryLimitUnit option:selected").val()=="GB")
                    {
                        memorySizeUnit = "";
                    }
                    else if($("#selMemoryLimitUnit option:selected").val()=="%")
                    {
                        memorySizeUnit = "%";
                    }

                   var memoryLimit = '';

                   if ($("#txtMemoryLimit").val() != "")
                   {
                    memoryLimit = $("#txtMemoryLimit").val() + memorySizeUnit;
                   }

                   var memorySize = "";
                        if(memoryLimit!="")
                        {
                            if (memoryLimit.indexOf("%")>-1) {
                                memorySize = parseInt(memoryLimit.replace("%", ""));
                                memoryLimit = memorySize + memorySizeUnit;
                            } else {
                                memoryLimit = encodeURIComponent(parseInt(memoryLimit));
                            }
                        }

                     deploymentInfo = {
                    data:{
                        systemsettings:{
                        resourcemonitor:{
                            memorylimit: {
                                "size": memoryLimit
                            }
                        }
                        }

                    },
                    id: VdmUI.getCurrentDbCookie()
                }
              }
            else if (item == "drId"){
                deploymentInfo = {
                    data: {
                        dr: {
                            id: parseInt(clusterObjects.txtDrId.val())
                        },
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
            }


             VdmService.SaveDeployment(function(connection){
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                        showHideShutdownDialog(true)
                    },deploymentInfo)
                }, deploymentInfo);

               }
              ShowSavedStatus()
    }

    UpdatePathInServerInfo = function(element,item,form,e){
       var serverId = VdmUI.getCurrentServerCookie();;
       if (!$("#"+form).valid()) {
            e.preventDefault();
            e.stopPropagation();
            return;
        }
        else{
            var serverInfo = []
            ShowSavingStatus();
            if (serverId == 0){ // Updating default Settings
                if(item == "voltdbrootDir"){
                        serverInfo = {
                            data:{
                               paths:{
                                    voltdbroot:{path: clusterDirectoryObjects.txtVoltdbRootDir.val()}
                                }
                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
                }
                else if(item == "snapshotDir"){
                    serverInfo = {
                       data: {
                                paths:{
                                    snapshots: {path: clusterDirectoryObjects.txtSnapshotDir.val()}
                                }
                             },
                             id: VdmUI.getCurrentDbCookie()
                    }
                }
                else if(item == "exportOverflowDir"){
                    serverInfo = {
                        data: {
                                paths:{
                                    exportoverflow: {path: clusterDirectoryObjects.txtExportOverflowDir.val()}
                                }
                              },
                              id: VdmUI.getCurrentDbCookie()
                    }
                }
                else if(item == "commandlogDir"){
                    serverInfo = {
                        data: {
                                paths:{
                                    commandlog: {path: clusterDirectoryObjects.txtCommandLogDir.val()}
                                }
                              },
                              id: VdmUI.getCurrentDbCookie()
                    }
                }
                else if(item == "commandlogSnapDir"){
                    serverInfo = {
                        data: {
                                 paths:{
                                    commandlogsnapshot: {path: clusterDirectoryObjects.txtCommandLogSnapDir.val()}
                                }
                              },
                              id: VdmUI.getCurrentDbCookie()
                    }
                }
                 else if(item == "drOverflow"){
                    serverInfo = {
                        data: {
                                 paths:{
                                    droverflow: {path: clusterDirectoryObjects.txtDrOverflow.val()}
                                }
                              },
                              id: VdmUI.getCurrentDbCookie()
                    }
                }

                VdmService.SaveDeployment(function(connection){
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                        showHideShutdownDialog(true)
                    },serverInfo)
                }, serverInfo);
            }
            else{
                var server = VdmUI.CurrentServerInfo;
                if(item == "voltdbrootDir"){
                    server['voltdbroot'] = clusterDirectoryObjects.txtVoltdbRootDir.val();
                }
                else if(item == "snapshotDir"){
                    server["snapshots"] = clusterDirectoryObjects.txtSnapshotDir.val();
                }
                else if(item == "exportOverflowDir"){
                    server["exportoverflow"] = clusterDirectoryObjects.txtExportOverflowDir.val();
                }
                else if(item == "commandlogDir"){
                    server["commandlog"] = clusterDirectoryObjects.txtCommandLogDir.val();
                }
                 else if(item == "commandlogSnapDir"){
                    server["commandlogsnapshot"] = clusterDirectoryObjects.txtCommandLogSnapDir.val();
                }
                 else if(item == "drOverflow"){
                    server["droverflow"] = clusterDirectoryObjects.txtDrOverflow.val();
                }
                var serverInfo ={
                        data:{
                            "name" : server['name'],
                            "hostname" : server['hostname'],
                            "description" : server['description'],
                            "client-listener" : server['client-listener'],
                            "admin-listener" : server['admin-listener'],
                            "internal-listener" : server['internal-listener'],
                            "http-listener" : server['http-listener'],
                            "zookeeper-listener" : server['zookeeper-listener'],
                            "replication-listener" : server['replication-listener'],
                            "internal-interface" : server['internal-interface'],
                            "external-interface" : server['external-interface'],
                            "public-interface" : server['public-interface'],
                            "placement-group" : server['placement-group'],
                            "voltdbroot" : server['voltdbroot'],
                            "commandlogsnapshot": server['commandlogsnapshot'],
                            "snapshots": server['snapshots'],
                            "droverflow": server['droverflow'],
                            "exportoverflow": server['exportoverflow'],
                            "commandlog": server['commandlog']
                        },
                        id:VdmUI.getCurrentDbCookie(),
                        server_id: serverId
                    }



                    VdmService.UpdateServer(function(connection){

                            var serverData = {
                                id:VdmUI.getCurrentDbCookie(),
                                server_id: serverId
                            }
                            VdmService.GetServer(function(connection){
                                   var serverInfo = connection.Metadata['GET_SERVER']
                                    $("#txtVoltdbRootDir").val(serverInfo['server']['voltdbroot'])
                                    $("#txtSnapshotDir").val(serverInfo['server']['snapshots'])
                                    $("#txtExportOverflowDir").val(serverInfo['server']['exportoverflow'])
                                    $("#txtCommandLogDir").val(serverInfo['server']['commandlog'])
                                    $("#txtCommandLogSnapDir").val(serverInfo['server']['commandlogsnapshot'])
                                    $("#txtDrOverflow").val(serverInfo['server']['droverflow'])
                            },serverData)
                    }, serverInfo);


            }
        }
        ShowSavedStatus();
    }



    var showHideShutdownDialog = function(state){
        if(state && ($('#startSuccess').is(":visible") || localStorage.isStarting ==  "true")){
            $('#divShutdownCluster').show();
            $('#startSuccess').addClass('databaseRg1')
            localStorage.shutdownDialogState = "true"
        } else {
            $('#divShutdownCluster').hide();
            $('#startSuccess').removeClass('databaseRg1')
            localStorage.shutdownDialogState = "false"
            localStorage.isStarting = "false"
        }
    }

    $('#btnShutdownCluster').on('click', function(){
        $('#divClusterShutdown').modal('show');
    })

    $('#btnClusterShutdownOk').on('click', function(){
        var dbInfo = {
                    data:{},
                    id: VdmUI.getCurrentDbCookie()
                 }
        VdmUI.showHideLoader(true);
        VdmService.StopCluster(function(connection){
            if(connection.Metadata['VDM_STOP_CLUSTER'].statusString.indexOf('Stop request sent successfully to servers') < 0){
                //try killing cluster
                VdmService.KillCluster(function(connection){
                     if(connection.Metadata['VDM_KILL_CLUSTER'].statusString.toLowerCase().indexOf('success') >= 0){
                        ShowHideDatabaseStatus('stopped')
                        showHideShutdownDialog(false)
                        $("#btnFailedOk").trigger('click')
                     } else {
                        $('#errorMsg').html('Unable to stop the cluster.')
                        $('#errorDialog').modal('show');
                     }
                     VdmUI.showHideLoader(false);
                },dbInfo);

            } else {
                ShowHideDatabaseStatus('stopped')
                showHideShutdownDialog(false)
                $("#btnFailedOk").trigger('click')
                VdmUI.showHideLoader(false);
            }
        }, dbInfo);
    })

    var setMemoryLimitValidation = function () {
        $("#errorMemoryLimit").val("");
        $("#errorMemoryLimit").hide();
        $("#txtMemoryLimit").rules("remove");
        var unit = $('#selMemoryLimitUnit').val();
        if (unit == "%") {
            $("#txtMemoryLimit").rules("add", {
                min: 1,
                max: 99,
                digits: true,
                messages: {
                    min: "Please enter a positive number.",
                    max: "Maximum value of percentage cannot be greater than 99.",
                    digits: "Please enter a positive number without any decimal."
                }
            });
        } else if (unit == "GB") {
            $("#txtMemoryLimit").rules("add", {
                min: 1,
                max: 2147483647,
                digits: true,
                messages: {
                    min: "Please enter a positive number.",
                    max: "Maximum value of GB cannot be greater than 2147483647.",
                    digits: "Please enter a positive number without any decimal."
                }
            });
        }
    };

    ShowSavedStatus= function(){
         setTimeout(function() {
                $("#changeSaveStatus").html("Changes have been saved.");
                $( "#changeSaveStatus" ).show().delay(1000).fadeOut(1500);
                }, 2000);

    }

    ShowSavingStatus= function(){
        $("#changeSaveStatus").hide();
        $("#changeSaveStatus").html("Saving...");
        $("#changeSaveStatus").show();
    }

    $('#btnAddSecurity').on('click', function(){
        VdmUI.userEditId = -1;
        VdmUI.openSecurityPopup();
        VdmUI.afterOpenSecurityPopup();
    });

    $('#btnAddExportProperty').on('click', function(){
        $('#addImportConfigWrapper').html('');
        VdmUI.exportEditId = -1;
        VdmUI.openExportPopup();
        VdmUI.afterOpenExportPopup();
    });

    $('#btnEditDiskLimit').on('click',function(){
        VdmUI.openDiskLimitPopup();
        VdmUI.afterOpenDiskLimitPopup();
    });

    $("#formAddExportConfiguration").validate({
        rules: {
            txtExportStream: validationRules.streamNameRules,
            txtExportType: validationRules.streamNameRules,
            txtExportConnectorClass: validationRules.streamNameRules
        },
        messages: {
            txtExportStream: validationRules.streamNameMessages,
            txtExportType: validationRules.streamNameMessages,
            txtExportConnectorClass: validationRules.streamNameMessages
        }
    });

    $('#formAddImportConfiguration').validate({
        rules: {
            txtImportStream: validationRules.streamNameRules,
            txtImportType: validationRules.streamNameRules,
            txtImportConnectorClass: validationRules.streamNameRules
        },
        messages: {
            txtImportStream: validationRules.streamNameMessages,
            txtImportType: validationRules.streamNameMessages,
            txtImportConnectorClass: validationRules.streamNameMessages
        }
    });


    $('#btnSaveDiskLimitOk').on('click',function(e){
        $(".duplicateError").hide();
                var featuresNames = [];

                var newFeatureNames = $(".newFeatureName");
                for (var i = 0; i < newFeatureNames.length; i++) {
                    featuresNames.push(newFeatureNames[i].value);
                    $(newFeatureNames[i]).rules("add", {
                        required: true,
                        //regex:/^[a-zA-Z0-9_\-.]+$/,
                        messages: {
                            required: "This field is required",
                            //regex: 'Only alphabets, numbers, <br/> _, - and . are allowed.'
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
                    return;
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
                        return;
                    } else {
                        $("#addDiskLimitControls").hide();
                        $("#saveDiskLimitConfirmation").show();
                        ShowSavingStatus();
                    }

                }


                var newConfig = {};
                newConfig["feature"] = [];

                var newFeatures = $(".newFeature");
                for (var i = 0; i < newFeatures.length; i += 2) {
                    newConfig["feature"].push({
                        "name": encodeURIComponent($(newFeatures[i]).val()),
                        "value": encodeURIComponent($(newFeatures[i + 1]).val()),
                    });
                }

                VdmUI.diskLimitList.push(newConfig);

                SaveDiskLimit();


    });

    $('#btnSaveExportOk').on('click', function(e){
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

        if(!$("#formAddExportConfiguration").valid()){
            e.preventDefault()
            e.stopPropagation()
            return;
        }

        ShowSavingStatus();

        var newConfig = {};
        newConfig["property"] = [];

        var newStreamProperties = $(".newStreamProperty");
        for (var i = 0; i < newStreamProperties.length; i += 2) {
            newConfig["property"].push({
                "name": $(newStreamProperties[i]).val(),
                "value": $(newStreamProperties[i + 1]).val(),
            });
        }
        newConfig["stream"] = $("#txtExportStream").val();
        newConfig["type"] = $("#txtExportType").val().toLowerCase();
        newConfig["enabled"] = $("#chkExportStream").is(':checked');
        if ($("#txtExportType").val().trim().toUpperCase() == "CUSTOM") {
            newConfig["exportconnectorclass"] = $("#txtExportConnectorClass").val();
        } else {
            newConfig["exportconnectorclass"] = '';
        }
        if(VdmUI.exportEditId == -1){
            if(VdmUI.exportPropertyList.length != 0)
                newConfig["id"] = VdmUI.exportPropertyList[VdmUI.exportPropertyList.length -1].id + 1;
            else
                newConfig["id"] = 1;
            VdmUI.exportPropertyList.push(newConfig);
        } else {
            var exportObject = findById(VdmUI.exportPropertyList, VdmUI.exportEditId)
            exportObject.stream = newConfig.stream;
            exportObject.type = newConfig.type;
            exportObject.enabled = newConfig.enabled;
            exportObject.property = newConfig.property;
            exportObject.exportconnectorclass = newConfig.exportconnectorclass
        }

        SaveExport();

    });

    $("#btnSaveUserOk").on('click',function(e){
        if(!$("#frmAddUser").valid()){
            e.preventDefault()
            e.stopPropagation()
            return;
        }
        var role_with_space = $('#txtUserRole').val().split(',');
        var roles = []

        for(var i = 0; i < role_with_space.length; i++){
            role_trim = role_with_space[i].trim()
            if(role_trim.indexOf(' ') > -1){
                $('#errorRole').show()
                $('#errorRole').html('Only alphabets, numbers, _ and . are allowed.')
                e.preventDefault()
                e.stopPropagation()
                return;
            }
            roles.push(role_trim)
        }

        role = roles.join()
        ShowSavingStatus();

        var username = $('#txtOrgUser').val();
        var newUsername = $('#txtUser').val();
        var password = encodeURIComponent($('#txtPassword').val());
        var requestType = "POST";
        var requestUser = "";
        var database_id = 0;


        var deploymentUserInfo = {
                         data:{},
                         id: VdmUI.getCurrentDbCookie()
                        }
        if (VdmUI.userEditId == 1) {

                        requestUser = VdmUI.userId;
                        requestType = "PUT";

                    } else {
//                        requestUser = newUsername;
                        requestType = "POST";
                    }



        deploymentUserInfo['data']['name'] = newUsername;
        deploymentUserInfo['data']['password'] = password;
        deploymentUserInfo['data']['roles'] = role;
        deploymentUserInfo['data']['plaintext'] = true
        deploymentUserInfo['data']['databaseid'] = VdmUI.getCurrentDbCookie()

         VdmService.SaveDeploymentUser(function(connection){
                 showHideShutdownDialog(true);
                 VdmService.GetDeploymentUsers(function(connection){
                    VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']
                     VdmService.GetDeployment(function(connection){
                        VdmUI.Deployment = connection.Metadata['DEPLOYMENT'];
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                     },dbData);
                },dbData);
            }, deploymentUserInfo,requestType, requestUser);


         ShowSavedStatus();


    })
    var userLength = 0;
    $("#deleteUser").on('click',function(e){
        var username = $('#txtOrgUser').val();
        var requestUser = $('#addUserInnerPopup').data('userid')

         var deploymentUserInfo = {
         data:{},
         id: VdmUI.getCurrentDbCookie()
        }

            deploymentUserInfo['data']['name'] = $('#txtUser').val()
            deploymentUserInfo['data']['password'] = encodeURIComponent($('#txtPassword').val())
            deploymentUserInfo['data']['roles'] = $("#selectRole").val()
            deploymentUserInfo['data']['plaintext'] = true


         VdmService.SaveDeploymentUser(function(connection){
                showHideShutdownDialog(true);
                VdmService.GetDeployment(function(connection){
                    VdmService.GetDeploymentUsers(function(connection){
                        VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                    if(connection.Metadata['DEPLOYMENT']['deployment']['users']['user'].length == 0)
                    {
                          deploymentInfo = {
                                        data:{
                                            security:{
                                                enabled: false
                                            }

                                        },
                                        id: VdmUI.getCurrentDbCookie()
                                        }

                               VdmService.SaveDeployment(function(connection){
                                        VdmService.GetDeployment(function(connection){
                                            VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                                        },deploymentInfo)
                                    }, deploymentInfo);
                      }
                    },dbData)
                },deploymentUserInfo)
            }, deploymentUserInfo,"DELETE", requestUser);

         ShowSavedStatus();

         $('#plusSecurity').modal('hide');
    })

    SaveExport = function(){

         var deploymentInfo = {
                data:{
                    export: { configuration: []},
                },
                id: VdmUI.getCurrentDbCookie()
            }
        deploymentInfo['data']['export']['configuration'] = [];
        for(var i=0;i<= VdmUI.exportPropertyList.length -1;i++)
        {
            deploymentInfo['data']['export']['configuration'].push({
                stream: VdmUI.exportPropertyList[i].stream,
                enabled: VdmUI.exportPropertyList[i].enabled,
                type: VdmUI.exportPropertyList[i].type.toLowerCase(),
                exportconnectorclass: VdmUI.exportPropertyList[i].exportconnectorclass==undefined?"":VdmUI.exportPropertyList[i].exportconnectorclass,
                property: []
            });

            deploymentInfo['data']['export']['configuration'][i]['property'] = [];

            for(var j=0;j<=VdmUI.exportPropertyList[i].property.length -1;j++)
            {
                deploymentInfo['data']['export']['configuration'][i]['property'].push({
                    name: VdmUI.exportPropertyList[i].property[j].name,
                    value:  VdmUI.exportPropertyList[i].property[j].value,
                });
            }
        }

         VdmService.SaveDeployment(function(connection){
                showHideShutdownDialog(true)
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },deploymentInfo)
            }, deploymentInfo);

           ShowSavedStatus()
    }

    SaveImport = function(){

        var deploymentInfo = {
                data:{
                    import: { configuration: []},
                },
                id: VdmUI.getCurrentDbCookie()
            }

        deploymentInfo['data']['import']['configuration'] = [];
        for(var i=0;i<= VdmUI.importPropertyList.length -1;i++)
        {
            deploymentInfo['data']['import']['configuration'].push({
                module: VdmUI.importPropertyList[i].module==undefined?"":VdmUI.importPropertyList[i].module,
                enabled: VdmUI.importPropertyList[i].enabled,
                type: VdmUI.importPropertyList[i].type,
                format: VdmUI.importPropertyList[i].format==undefined?"":VdmUI.importPropertyList[i].format,
                property: []
            });

            deploymentInfo['data']['import']['configuration'][i]['property'] = [];

            for(var j=0;j<=VdmUI.importPropertyList[i].property.length -1;j++)
            {
                deploymentInfo['data']['import']['configuration'][i]['property'].push({
                    name: VdmUI.importPropertyList[i].property[j].name,
                    value: VdmUI.importPropertyList[i].property[j].value,
                });
            }
        }

            VdmService.SaveDeployment(function(connection){
                showHideShutdownDialog(true)
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },deploymentInfo)
            }, deploymentInfo);

             ShowSavedStatus();
    }

    SaveDiskLimit = function(){

        var deploymentInfo = {
                    data:{
                        systemsettings: {
                            resourcemonitor: {
                            disklimit: {
                            feature: []}}},
                    },
                    id: VdmUI.getCurrentDbCookie()
                }

                var feature = [];

                var newFeatureNames = $(".newFeatureName");
                var newFeatureValues = $(".newFeatureValue");
                var newFeatureUnits = $(".newFeatureUnit");
                for (var i = 0; i < newFeatureNames.length; i++) {
                    if (newFeatureUnits[i].value == "GB") {
                        feature.push(
                            {
                                name: newFeatureNames[i].value.toLowerCase(),
                                size: parseInt(newFeatureValues[i].value)
                            }
                        );
                    } else {
                        feature.push(
                          {
                              name: newFeatureNames[i].value.toLowerCase(),
                              size: parseInt(newFeatureValues[i].value) + newFeatureUnits[i].value
                          }
                      );

                    }
                }

//                if (feature.length == 0) {
//                    adminConfigurations.systemsettings.resourcemonitor.disklimit = null;
//
//                } else {
//                    adminConfigurations.systemsettings.resourcemonitor.disklimit.feature = feature;
//                }


        deploymentInfo['data']['systemsettings']['resourcemonitor']['disklimit'] = {};
        deploymentInfo['data']['systemsettings']['resourcemonitor']['disklimit']['feature'] = [];


        for(var j=0;j<=feature.length -1;j++)
        {
            deploymentInfo['data']['systemsettings']['resourcemonitor']['disklimit']['feature'].push({
                name: feature[j].name,
                size: feature[j].size.toString(),
            });
        }


         VdmService.SaveDeployment(function(connection){
                showHideShutdownDialog(true)
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },deploymentInfo)
            }, deploymentInfo);

           ShowSavedStatus()
    }

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

    $('#deleteExportConfig').on('click',function(){
        ShowSavingStatus();
        var id = $('#plusExport').data('id');
        var obj = findById(VdmUI.exportPropertyList, id);
        removeProperty(VdmUI.exportPropertyList, id);
        SaveExport();
        $('#plusExport').modal('hide');
    })

    $('#deleteImportConfig').on('click',function(){
        ShowSavingStatus();
        var id = $('#plusImport').data('id');
        var obj = findById(VdmUI.importPropertyList, id);
        removeProperty(VdmUI.importPropertyList, id);
        SaveImport();
        $('#plusImport').modal('hide');
    })

    function findById(source, id) {
      for (var i = 0; i < source.length; i++) {
        if (source[i].id === id) {
          return source[i];
        }
      }
      throw "Couldn't find object with id: " + id;
    }

    function removeProperty(source, id){
        $.each(source, function(i){
            if(source[i].id == id) {
                source.splice(i,1);
                return false;
                }
            });
    }

    function formatExportData(){
        var exportData = []
        for(var i = 0; i < VdmUI.exportPropertyList.length; i++){
            var data = {
                stream: VdmUI.exportPropertyList[i].stream,
                type: VdmUI.exportPropertyList[i].type,
                property: VdmUI.exportPropertyList[i].property,
                enabled: VdmUI.exportPropertyList[i].enabled
            }
            exportData.push(data);
        }
        return exportData;
    }

    $('#btnAddImportProperty').on('click', function(){ //btnAddImportProperty
        $('#addExportConfigWrapper').html('');
        VdmUI.importEditId = -1;
        VdmUI.openImportPopup();
        VdmUI.afterOpenImportPopup();
    });

    $('#btnSaveImportOk').on('click', function(e){
        var newStreamPropertyNames = $(".newStreamImportPropertyName");
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

        var newStreamPropertyValues = $(".newStreamImportPropertyValue");
        for (var j = 0; j < newStreamPropertyValues.length; j++) {
            $(newStreamPropertyValues[j]).rules("add", {
                required: true,
                messages: {
                    required: "This field is required"
                }
            });
        }

        if(!$("#formAddImportConfiguration").valid()){
            e.preventDefault()
            e.stopPropagation()
            return;
        }

        ShowSavingStatus();

        var newConfig = {};
        newConfig["property"] = [];

        var newStreamProperties = $(".newStreamImportProperty");
        for (var i = 0; i < newStreamProperties.length; i += 2) {
            newConfig["property"].push({
                "name": $(newStreamProperties[i]).val(),
                "value": $(newStreamProperties[i + 1]).val(),
            });
        }
        newConfig["format"] = $("#txtImportConnectorClass").val();
        newConfig["type"] = $("#txtImportType").val().toLowerCase();
        newConfig["enabled"] = $("#chkImportStream").is(':checked');
        if ($("#txtImportType").val().trim().toUpperCase() == "CUSTOM") {
            newConfig["module"] = $("#txtImportStream").val();
        } else {
            newConfig["module"] = '';
        }
        if(VdmUI.importEditId == -1){
            if(VdmUI.importPropertyList.length != 0)
                newConfig["id"] = VdmUI.importPropertyList[VdmUI.importPropertyList.length -1].id + 1;
            else
                newConfig["id"] = 1;
            VdmUI.importPropertyList.push(newConfig);
        } else {
            var importObject = findById(VdmUI.importPropertyList, VdmUI.importEditId)
            importObject.module = newConfig.module;
            importObject.type = newConfig.type;
            importObject.enabled = newConfig.enabled;
            importObject.property = newConfig.property;
            importObject.format = newConfig.format
        }

        SaveImport();

    });

    $('.modal')
    .on('shown', function(){
      console.log('show');
      $('body').css({overflow: 'hidden'});
    })
    .on('hidden', function(){
      $('body').css({overflow: ''});
    });

    $('#btnUpdateSource').on('click',function(){
        $('#drServers-error').hide()
        $('#errorDrId').hide()
        $('#errorServers').hide()
        VdmUI.showHideConnectionSource()
        VdmUI.populateDrPopup();
    });

    $('#addConnectionSource').on('click', function(){
        VdmUI.showHideConnectionSource();
    })

    $('#selectDrDatabase').change(function () {
        $('#drServers-error').hide();
    });


    $('#btnSaveReplication').on('click', function(e){
        if(!$('#frmDatabaseReplication').valid()){
            e.preventDefault();
            e.stopPropagation();
            return;
        }

        SaveDrInfo();
    });

    var SaveDrInfo = function(){
        ShowSavingStatus();
        var dr_value = {}
        var masterCluster = clusterObjects.txtDatabase.val();
        var connection_source = {}

        if($('.trConnectionSource').is(':visible')){
            if(clusterObjects.txtDatabase.val().trim() != ''){
                connection_source = {
                    source: masterCluster,
                }
            }
        }
        if(clusterObjects.txtDrPort.val() != ''){
            dr_value = {
                id: parseInt(clusterObjects.txtDrId.val()),
                listen: true,
                connection: connection_source,
                port: parseInt(clusterObjects.txtDrPort.val())
            }
        } else {
            dr_value = {
                id: parseInt(clusterObjects.txtDrId.val()),
                listen: true,
                connection: connection_source,
            }
        }

        deploymentInfo = {
                            data: {
                                dr: dr_value
                            },
                            id: VdmUI.getCurrentDbCookie()
                         }
        VdmService.SaveDeployment(function(connection){
            showHideShutdownDialog(true)
            VdmService.GetDeployment(function(connection){
                $('#connectionSource').modal('hide');
                VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                ShowSavedStatus();
            },deploymentInfo)
        }, deploymentInfo);
    }

    $('#btnDeleteConnection').on('click', function(){
        $('#connectionSource').modal('hide');
        deploymentInfo = {
                            data: {
                                dr: {
                                }
                            },
                            id: VdmUI.getCurrentDbCookie()
                         }

        VdmService.SaveDeployment(function(connection){
            VdmService.GetDeployment(function(connection){
                VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
            },deploymentInfo)
        }, deploymentInfo);
    })


    $('#btnStopCluster').on('click', function(){
        $(".stalledState").hide();
        var dbInfo = {
                        data:{},
                        id: VdmUI.getCurrentDbCookie()
                     }
        VdmService.StopCluster(function(connection){
            if(connection.Metadata['VDM_STOP_CLUSTER'].statusString.indexOf('Stop request sent successfully to servers') < 0){
                //try killing cluster
                VdmService.KillCluster(function(connection){
                 if(connection.Metadata['VDM_KILL_CLUSTER'].statusString.toLowerCase().indexOf('success') >= 0){
                    ShowHideDatabaseStatus('stopped')
                 }
                 else//error killing cluster
                 {}

                },dbInfo);

            }
        }, dbInfo);
    })


    $('#vmcLink').on('click',function(){
        var url = ''
        if(VdmUI.firstRunningHostName == "")
        {
            url = VdmConfig.GetProtocol()+"//"+VdmConfig.GetDefaultServerIP()+":8080";
        }
        else
        {
            var port = "8080";
            for(var i=0;i< VdmUI.members.length;i++)
            {
                if(VdmUI.members[i]['hostname'] == VdmUI.firstRunningHostName)
                {
                    if(VdmUI.members[i]['http-listener'].indexOf(":")<= 0)
                    {
                        port = VdmUI.members[i]['http-listener'] != "" ? VdmUI.members[i]['http-listener']:"8080"
                        url = VdmConfig.GetProtocol()+"//"+VdmUI.firstRunningHostName+":"+port;
                    }
                    else
                    {
                        port = VdmUI.members[i]['http-listener'];
                        url = VdmConfig.GetProtocol()+"//"+ port;
                    }

                }
            }


        }

        var win = window.open(url, '_blank');
        win.focus();
    })

    $("#btnFailedOk").on('click',function(){
        VdmUI.isStartServer = true;
        ShowHideDatabaseStatus('stopped');
    })

    $("#upload-file-btn").on('click', function(){
        VdmUI.showHideUploadConfiguration(false)
    });

    $('#upload-file-btn-Cancel').on('click', function(){
        VdmUI.showHideUploadConfiguration(true)
    });

    $("#upload-file-btn-ok").on('click', function(){
        VdmUI.showHideLoader(true);
        if($('#file').val() == ''){
            $('#errorMsg').html('Please select the file to upload.')
            $('#errorDialog').modal('show');
            VdmUI.showHideLoader(false);
            return;
        }
        var form_data = new FormData($('#upload-file')[0]);

        var configInfo = {
            data:{
                deploy_file:form_data
            },
            id:VdmUI.getCurrentDbCookie()
        }

        VdmService.UploadDeploymentXml(function(connection){
            if(connection.Metadata['UPLOAD_DEPLOYMENT_CONFIG'].hasOwnProperty('status')){
                if(connection.Metadata['UPLOAD_DEPLOYMENT_CONFIG']['status'].toString().toLowerCase() == '201'){
                    showHideShutdownDialog(true);
                    $('#file').val('')
                    $('#successMsg').html('File uploaded successfully.')
                    $('#successDialog').modal('show');
                    VdmUI.showHideUploadConfiguration(true)
                } else{
                    if (connection.Metadata['UPLOAD_DEPLOYMENT_CONFIG'].hasOwnProperty('statusString')){
                        showFileUploadErrorMsg(connection.Metadata['UPLOAD_DEPLOYMENT_CONFIG']['statusString'])
                    } else {
                        $('#errorMsg').html('Unable to upload file to server.')
                        $('#errorDialog').modal('show');
                    }
                    VdmUI.showHideUploadConfiguration(false)
                }

            }
            VdmUI.showHideLoader(false);
        }, configInfo);
    });

    function showFileUploadErrorMsg(errMsg){
        $('#errorMsg').html('Unable to upload the file. Following error was received. <ul><li style="list-style-type: disc;margin-left: 16px" >' + errMsg + ' </li></ul>')
        $('#errorDialog').modal('show');
    }

    $('#deleteConnectionSource').on('click', function(){
        $('.trConnectionSource').hide();
        clusterObjects.txtDatabase.val('')
    })

    $('#downloadConfigurations').on('click', function (e) {
        var host = window.location.protocol + '//' + window.location.host;
        var database_id = VdmUI.getCurrentDbCookie()
        var url = host + '/api/1.0/databases/' + database_id + '/deployment/'
        $(this).attr("href", url);
        setTimeout(function () {
            $('#downloadConfigurations').attr("href", "#");
        }, 100);
    });

    $('#btnConfirmStopServerOk').on('click', function(){
        VdmUI.showHideLoader(true)
        var serverData = {
           "id": VdmUI.getCurrentDbCookie(),
           "server_id": $('#divStopServerPopup').data('serverid'),
           "serverIp": $('#divStopServerPopup').data('ip')
        }
        VdmService.StopServer(function(connection){
            var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }
            if(connection.Metadata['VDM_STOP_SERVER'].statusString.indexOf('success') < 0){
                errorMsg = 'Server couldn\'t be stopped.'
                $('#errorMsg').html(errorMsg)
                $('#errorDialog').modal('show');
            } else {
                VdmService.GetMemberList(function(connection){
                    VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'], true)
                }, dbData);
            }
            VdmUI.showHideLoader(false)
        }, serverData);
    })

//    $('#code').on('shown.bs.modal', function (e) {
//
//    })

};

(function (window) {
    var iVdmUi = (function () {
        this.CurrentTab = NavigationTabs.DBMonitor;
        this.CurrentServerList = [];
        this.CurrentHostList = [];
        this.CurrentDbList = [];
        this.CurrentDbIdList = [];
        this.CurrentDbName = '';
        this.ConnectionSourceStatus = false;
        this.showHideXDCRMessage = false;
        this.showHideReplicaMessage = false;
        this.isServerCreate = true;
        this.isDbCreate = true;
        this.serverToUpdate = '';
        this.hostToUpdate = '';
        this.dbToUpdate = '';
        this.members = [];
        this.exportEditId = -1;
        this.userEditId = -1;
        this.orgUserList = [];
        this.importEditId = -1;
        this.masterCluster = '';
        this.isTestServer = false;
        this.isConnectionSourceDefined = false;
        this.isErrorPopupShowing = false;
        this.isStartServer = false;
        this.haltRequest = false;
        this.isRunning = false;
        this.isServerStarting = false;
        this.isServersStarting = []
        this.firstRunningHostName = '';
        this.ServerList = {};
        this.httplistenerToUpdate = '';
        this.internallistenerToUpdate = '';
        this.zookeeperlistenerToUpdate = '';
        this.clientlistenerToUpdate = '';
        this.replicationlistenerToUpdate = '';
        this.adminlistenerToUpdate = '';
        this.serverIdToUpdate = '';
        this.defaultPorts = {
            "httplistener" : "8080",
            "clientlistener" : "21212",
            "adminlistener" : "21211",
            "internallistener" : "3021",
            "replicationlistener": "5555",
            "zookeeperlistener": "7181"
        };
        this.CurrentServerInfo = {}
        this.portList = [];
        this.DeploymentUsers = [];
        this.userId = 0;
        this.editServerId = -1
        this.isFreshStart = false;
        this.getCookie = function(name) {
            return $.cookie(name + "_" + VdmConfig.GetPortId());
        };

        this.Deployment = [];
        this.exportPropertyList = [];
        this.userSecurityList = [];
        this.importPropertyList = [];
        this.diskLimitList = []
        this.newStreamMinmPropertyName = {
            "outdir": "#txtOutdirValue",
            "nonce": "#txtnonceValue",
            "type": "#txtFileTypeValue",
            "endpoint_http": "#txtEndpointValue",
            "metadata.broker.list": "#txtMetadataBrokerListValue",
            "jdbcurl": "#txtJdbcUrlValue",
            "jdbcdriver": "#txtJdbcDriverValue",
            "broker.host": "#txtRabbitMqValue",
            "amqp.uri": "#txtRabbitMqValue",
            "endpoint_elasticsearch": "#txtEndpointESValue"
        };

        this.displayServers = function(memberList){
            if(memberList == undefined){
                return;
            }
            var dbId = VdmUI.getCurrentDbCookie();
            VdmService.GetDatabaseList(function(connection){
                var dbIdList = []
                databaseList = connection.Metadata['DATABASE_LISTING']
                if(databaseList != undefined && databaseList['statusstring'] != undefined && databaseList['statusstring'] == 'Query timeout.'){
                    $('#warningMsg').html('Deployment manager cannot be reached.')
                    VdmUI.showHideOverlay(false);
                    return
                }
                databaseList.databases.forEach(function (info) {
                    var dbId = info['id']
                    dbIdList.push(dbId);
                });
                if(dbIdList.length != 0 ){
                    VdmUI.showHideOverlay(true);
                    if($.inArray(dbId, dbIdList) == -1){
                        if(dbId != -1) {
                            $('#errorMsg').html('The selected database has been deleted.')
                            $('#errorDialog').modal('show');
                        }
                        dbId = connection.Metadata['DATABASE_LISTING'].databases[0]['id']
                        VdmUI.saveCurrentDbCookie(dbId);

                    }
                    VdmUI.members = []
                    VdmUI.members = memberList.members;
                    var htmlList = "";
                    var serverList = "";
                    VdmUI.CurrentServerList = [];
                    VdmUI.CurrentHostList = [];
                    VdmUI.ServerList = []
                    var dbData = {id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()}
                    var serverStatusArray = []
                    var serverStateArray = []
                    database_status = ''
                    memberCount = 0
                    VdmService.GetDatabaseStatus(function(connection){
                        if(connection.Metadata['DATABASE_STATUS'].hasOwnProperty('dbStatus')){
                            database_server_status = connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus;
                            database_status = connection.Metadata['DATABASE_STATUS'].dbStatus.status;
                            for(var i =0; i< database_server_status.length; i++){
                                var key = Object.keys(database_server_status[i])[0]
                                serverStatusArray[key] = database_server_status[i][key].details
                                serverStateArray[key] = database_server_status[i][key].status
                            }
                            serverList += "<option value='0'>Default Setting</option>"
                            if(memberList.members != undefined){
                                memberCount = memberList.members.len
                                memberList.members.forEach(function (info) {
                                    var id = info["id"]
                                    var hostName = info["hostname"];
                                    var name = info["name"]
                                    var displayName =  name != '' ? name : hostName;
                                    var hostId = info["id"];
                                    var isAdded = info["isAdded"]
                                    var infos = JSON.stringify(info)
                                    var serverData = {
                                       "id": VdmUI.getCurrentDbCookie(),
                                       "server_id": hostId,
                                       "serverIp": hostName
                                    }
                                    var server_state = serverStateArray[hostName]
                                    logMsg = serverStatusArray[hostName] == '' ? 'No logs available.' : serverStatusArray[hostName]


                                    if (server_state.toLowerCase() == 'running'){
                                        htmlList += '<tr> ' +
                                                    '<td id="tdHostname_' +hostId+ '"> ' +
                                                    '<span class="greenDot serverListIcons" title="Server is running "></span> ' +
                                                    '<div id="divServer_' + id + '" class="dropdown serverListDropdown"> ' +
                                                    '<div class="dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true"> ' +
                                                    '<a><span class="clsServerList">' + displayName + '</span> ' +
                                                    '<span class="glyphicon glyphicon-triangle-bottom iconDD"></span></a> ' +
                                                    '</div> ' +
                                                    '<ul id="ser_menu_' + id + '" data-id="' + hostId + '" data-status="running" data-info=\''+ infos +'\' class="dropdown-menu" role="menu" aria-labelledby="dropdownMenu1" > ' +
                                                    '<li class="btnViewServerDetails"><a data-toggle="modal" data-target="#viewServerDetails"><div class="glyphicon glyphicon-search editClass"></div>View</a></li> ' +
                                                    '</ul></div> ' +
                                                    '</td> ' +
                                                    '<td> ' +
                                                    '<a  class="btnServerLog" href="javascript:void(0);"data-toggle="modal" data-target="#divServerLogPopup" >' +
                                                    '<div class="" id="editServer_'+hostId+'"><span class="glyphicon glyphicon-menu-hamburger" aria-hidden="true"></span></div> ' +
                                                    '<span class="editServerTxt" id="editServerTxt_'+hostId+'">Logs</span></a>' +
                                                    '<div class="loading-small" id="loadingServer_'+hostId+'" style="display:none"></div>' +
                                                    '<label style="display:none">' + logMsg + '</label> ' +
                                                    '</td>' +
                                                    '<td data-id="' + hostId + '" data-ip="' + hostName + '"> ' +
                                                    '<a class="btnStopServer" data-toggle="modal" data-target="#" >' +
                                                    '<div class="" id="deleteServer_'+hostId+'"><span class="glyphicon glyphicon-off serverListRemove" aria-hidden="true"></span></div> ' +
                                                    '<span class="deleteServerListTxt" id="deleteServerTxt_'+hostId+'">Stop</span></a>' +
                                                    '</td>' +
                                                '</tr>';
                                        VdmUI.isServersStarting = jQuery.grep(VdmUI.isServersStarting, function(value) {
                                          return value != hostName;
                                        });
                                    } else if(server_state.toLowerCase() == 'unreachable'){
                                        htmlList += '<tr> ' +
                                                    '<td id="tdHostname_' +hostId+ '" class="redDotDropDown"> ' +
                                                    '<span class="grayDot serverListIcons" title="Server is unreachable "></span> ' +
                                                    '<div id="divServer_' + id + '" class="dropdown serverListDropdown"> ' +
                                                    '<div class="dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true"> ' +
                                                    '<a><span class="clsServerList">' + displayName + '</span> ' +
                                                    '<span class="glyphicon glyphicon-triangle-bottom iconDD"></span></a> ' +
                                                    '</div> ' +
                                                    '<ul id="ser_menu_' + id + '" data-id="' + hostId + '"  data-status="unreachable" data-info=\''+ infos +'\' class="dropdown-menu" role="menu" aria-labelledby="dropdownMenu1" > ' +
                                                    '<li class="btnViewServerDetails"><a data-toggle="modal" data-target="#viewServerDetails"><div class="glyphicon glyphicon-search editClass"></div>View</a></li> ' +
                                                    '</ul></div> ' +
                                                    '</td> ' +
                                                    '<td colspan="2"> ' +
                                                    '&nbsp; &nbsp;' +
                                                    '</td>' +
                                                '</tr>';
                                    } else if (server_state.toLowerCase() == 'stalled' && $.inArray(hostName, VdmUI.isServersStarting) > -1){
                                        htmlList += '<tr> ' +
                                                    '<td id="tdHostname_' +hostId+ '"> ' +
                                                    '<span class="redDot serverListIcons" title="Server is down "></span> ' +
                                                    '<span class="clsServerListGreen">' + displayName + '</span> ' +
                                                    '</td> ' +
                                                    '<td> ' +
                                                    '<a style="display:none" class="btnServerLog" href="javascript:void(0);"data-toggle="modal" data-target="#divServerLogPopup" >' +
                                                    '<div class="" id="editServer_'+hostId+'"><span class="glyphicon glyphicon-menu-hamburger" aria-hidden="true"></span></div> ' +
                                                    '<span class="editServerTxt" id="editServerTxt_'+hostId+'">Logs</span></a>' +
                                                    '<div class="loading-small" id="loadingServer_'+hostId+'" style="display:block"></div>' +
                                                    '<label style="display:none">' + logMsg + '</label> ' +
                                                    '</td>' +
                                                    '<td data-id="' + hostId + '"> ' +
                                                    '<a style="display:none" class="btnStopServer" data-toggle="modal" data-target="#" >' +
                                                    '<div class="" id="deleteServer_'+hostId+'"><span class="glyphicon glyphicon-off serverListRemove" aria-hidden="true"></span></div> ' +
                                                    '<span class="deleteServerListTxt" id="deleteServerTxt_'+hostId+'">Stop</span></a>' +
                                                    '</td>' +
                                                '</tr>';
                                    }
                                    else{
                                        classEnableDisable = database_status == 'running' ? 'startServerEnabled' : 'startServerDisabled'
                                        tooltipMsg = database_status == 'running' ? '' : 'Database is currently down.'
                                        htmlList += '<tr> ' +
                                                    '<td id="tdHostname_' +hostId+ '" class="redDotDropDown"> ' +
                                                    '<span class="redDot serverListIcons" title="Server is down "></span> ' +
                                                    '<div id="divServer_' + id + '" class="dropdown serverListDropdown"> ' +
                                                    '<div class="dropdown-toggle" id="dropdownMenu1" data-toggle="dropdown" aria-haspopup="true" aria-expanded="true"> ' +
                                                    '<a><span class="clsServerList">' + displayName + '</span> ' +
                                                    '<span class="glyphicon glyphicon-triangle-bottom iconDD"></span></a> ' +
                                                    '</div> ' +
                                                    '<ul id="ser_menu_' + id + '" data-id="' + hostId + '" data-info=\''+ infos +'\' class="dropdown-menu" role="menu" aria-labelledby="dropdownMenu1" > ' +
                                                    '<li id="btnUpdateServer" class="btnUpdateServer"><a data-toggle="modal" data-target="#addServer"><div class="glyphicon glyphicon-pencil editClass"></div>Edit</a></li> ' +
                                                    '<li id="btnDeleteServer" class="btnDeleteServer"><a data-toggle="modal" data-target="#deleteConfirmation"><div class="glyphicon glyphicon-trash redC"></div>Delete</a></li> ' +
                                                    '</ul></div> ' +
                                                    '</td> ' +
                                                    '<td> ' +
                                                    '<a class="btnServerLog" href="javascript:void(0);"data-toggle="modal" data-target="#divServerLogPopup" >' +
                                                    '<div class="" id="editServer_'+hostId+'"><span class="glyphicon glyphicon-menu-hamburger" aria-hidden="true"></span></div> ' +
                                                    '<span class="editServerTxt" id="editServerTxt_'+hostId+'">Logs</span></a>' +
                                                    '<div class="loading-small" id="loadingServer_'+hostId+'" style="display:none"></div>' +
                                                    '<label style="display:none">' + logMsg + '</label> ' +
                                                    '</td>' +
                                                    '<td data-id="' + hostId + '" data-isadded="'+ isAdded +'" data-hostcount= "'+ memberCount +'" ' +
                                                    'data-ip="' + hostName + '"> ' +
                                                    '<a class="btnStartServer ' + classEnableDisable  + '" data-toggle="modal" data-target="#" title="' + tooltipMsg + '">' +
                                                    '<div class="" id="deleteServer_'+hostId+'"><span class="glyphicon glyphicon-play" aria-hidden="true"></span></div> ' +
                                                    '<span class="deleteServerTxt" id="deleteServerTxt_'+hostId+'">Start</span></a>' +
                                                    '</td>' +
                                                '</tr>';
                                        VdmUI.isServersStarting = jQuery.grep(VdmUI.isServersStarting, function(value) {
                                          return value != hostName;
                                        });
                                    }

                                    VdmUI.CurrentServerList.push(name);
                                    VdmUI.CurrentHostList.push(hostName);
                                    VdmUI.ServerList.push(
                                    {
                                        "name": name,
                                        "hostname": hostName,
                                        "httplistener": info["http-listener"],
                                        "clientlistener": info["client-listener"],
                                        "adminlistener": info["admin-listener"],
                                        "zookeeperlistener": info["zookeeper-listener"],
                                        "replicationlistener": info["replication-listener"],
                                        "internallistener": info["internal-listener"],
                                        "id": hostId
                                    })

                                     serverList += "<option data-info='"+ infos +"' value='"+id+"' >"+displayName+"</option>"
                                });
                            }

                            if(serverList != ""){
                                $('.defaultServ').html(serverList)
                            }

                            $('.defaultServ').val(VdmUI.getCookie("selected-serverId"));

                            var dbId = $.parseJSON(VdmUI.getCookie("current-db"));

                            if(htmlList == ""){
                                $('#serverList').html('<tr><td style="top:-1px !important">No servers available.</td><td></td></tr>')
                            }else{
                                $('#serverList').html(htmlList)
                            }
                            if(VdmUI.editServerId != -1 && (database_status != 'running' || database_status != 'unreachable')){
                                $('#divServer_' + VdmUI.editServerId).addClass('open')
                            } else {
                                VdmUI.editServerId = -1
                                $('#divServer_' + VdmUI.editServerId).removeClass('open')
                            }

                            if(database_status == 'stalled'){
                                if(!VdmUI.isServerStarting){
                                    VdmUI.showHideLoader(false)
                                }
                            } else {
                                VdmUI.showHideLoader(false)
                                VdmUI.isServerStarting = false;
                            }

                            $('.btnDeleteServer').on('click', function(){
                                var serverId = $(this.parentElement).data('id');
                                $('#deleteConfirmation').data('serverid',serverId);
                            });


                            $('.btnUpdateServer').on('click', function(){
                                VdmUI.resetTextBox();
                                VdmUI.isServerCreate = false;
                                var serverInfo = $(this.parentElement).data('info');
                                VdmUI.serverToUpdate = serverInfo['name'];
                                VdmUI.hostToUpdate = serverInfo['hostname'];
                                VdmUI.httplistenerToUpdate = serverInfo['http-listener']
                                VdmUI.adminlistenerToUpdate = serverInfo['admin-listener']
                                VdmUI.replicationlistenerToUpdate = serverInfo['replication-listener']
                                VdmUI.zookeeperlistenerToUpdate = serverInfo['zookeeper-listener']
                                VdmUI.internallistenerToUpdate = serverInfo['internal-listener']
                                VdmUI.clientlistenerToUpdate = serverInfo['client-listener']
                                VdmUI.serverIdToUpdate = serverInfo['id']

                                $('#addServer').data('serverid',serverInfo['id']);
                                $('#serverName').val(serverInfo['name']);
                                $('#txtHostName').val(serverInfo['hostname']);
                                $('#txtDescription').val(serverInfo['description']);
                                $('#txtClientPort').val(serverInfo['client-listener']);
                                $('#txtAdminPort').val(serverInfo['admin-listener']);
                                $('#txtInternalPort').val(serverInfo['internal-listener']);
                                $('#txtHttpPort').val(serverInfo['http-listener']);
                                $('#txtZookeeper').val(serverInfo['zookeeper-listener']);
                                $('#txtReplicationPort').val(serverInfo['replication-listener']);
                                $('#txtInternalInterface').val(serverInfo['internal-interface'])
                                $('#txtExternalInterface').val(serverInfo['external-interface'])
                                $('#txtPublicInterface').val(serverInfo['public-interface']);
                                $('#txtPlacementGroup').val(serverInfo['placement-group']);
                                $('#addServerTitle').html('Update Server');
                            });

                            $('#btnAddServerOption').on('click', function(){
                                VdmUI.isServerCreate = true;
                                VdmUI.serverToUpdate = '';
                                VdmUI.hostToUpdate = '';
                                VdmUI.serverIdToUpdate = '';
                                $('#addServerTitle').html('Add Server');
                                VdmUI.resetTextBox();
                                $('#colExpIcon').removeClass('glyphicon-triangle-bottom').addClass('glyphicon-triangle-right');
                                $('#collapseOne').removeClass('in');
                                $('#collapseOne').css({'height': '0px'});
                            });

                            $('.btnServerLog').on('click', function(){
                                var logMsg = $(this.parentElement).find('label').text()
                                $('#logMessage').html(logMsg)
                            })

                            $('.btnStopServer').on('click', function(){
                                $('#divStopServerPopup').data('ip', $(this.parentElement).data('ip'))
                                $('#divStopServerPopup').data('serverid', $(this.parentElement).data('id'))
                                ip_add = VdmUI.GetRunningServerIp(serverStateArray)
                                messages = 'Are you sure you want to stop the server?'
                                missing_rep_msg = ''
                                if(ip_add != ''){
                                    VdmService.GetSystemInformation(function(connection){
                                        var systemInfo = connection.Metadata['SYSTEM_INFORMATION'];
                                        var kSafety = VdmUI.GetKSafetyValue(systemInfo)
                                        VdmService.GetStatisticsInformation(function(connection){
                                            connection_data = connection.Metadata['STATISTIC_INFORMATION']
                                            if(connection_data != null && connection_data.hasOwnProperty('results')
                                               && connection_data['results'][0].data.length != 0){
                                                data_status = connection.Metadata['STATISTIC_INFORMATION']['results'][0].data
                                                missing_replica = data_status[data_status.length - 1][2]
                                                if(missing_replica != undefined && missing_replica > 0)
                                                    missing_rep_msg += 'You have ' + missing_replica + ' missing replicas.'
                                            }
                                            if(kSafety.toString() == '0')
                                                messages = 'Stopping this server will shutdown the whole cluster. Are you' +
                                                           ' sure you want to proceed?'
                                            else {
                                                serverCount = VdmUI.GetRunningServerCount(serverStateArray)
                                                if(serverCount == 1)
                                                   messages = 'Stopping this server will shutdown the whole cluster. ' +
                                                              'Are you sure you want to proceed?'
                                            }
                                            $('#stopMessage').html((missing_rep_msg == '' ? missing_rep_msg : missing_rep_msg + ' ') + messages)
                                            $('#divStopServerPopup').modal('show');
                                        }, {'serverIp': ip_add});

                                    }, {'serverIp': ip_add});
                                }
                            })

                            $('.btnStartServer').on('click', function(e){
                                var shutdownDialogState = VdmUI.getCookie('shutdownDialogState')
                                if(shutdownDialogState == "true"){
                                    $('#errorMsg').html("New changes have been made to the configuration. You need to shutdown the cluster and start it again for the changes to take effect.")
                                    $('#errorDialog').modal('show');
                                    return false;
                                }
                                else{
                                    var isValidClusterParameters = VdmUI.CheckValidClusterParameters();

                                    if(!isValidClusterParameters){
                                        $('#errorMsg').html("Cluster parameters failed validation: The cluster has more hosts and sites per hosts than required for the requested k-safety value. The number of total sites (sitesPerHost * hostCount) must be a whole multiple of the number of copies of the database (k-safety + 1).")
                                        $('#errorDialog').modal('show');
                                        return false;
                                    }
                                }

                                var server_ip_add = $(this.parentElement).data('ip')
                                var isAdded = $(this.parentElement).data('isadded')
                                var server_id = $(this.parentElement).data('id');
                                var serverData = {
                                   "id": VdmUI.getCurrentDbCookie(),
                                   "server_id": parseInt(server_id)
                                }
                                if($(this).hasClass('startServerEnabled')){
                                    VdmUI.showHideLoader(true);
                                    if(isAdded){
                                        VdmUI.StartDbServer(serverData, server_ip_add)
                                    } else {
                                        VdmUI.AddDbServer(serverData, server_ip_add)
                                    }
                                } else {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    return;
                                }
                            })

                            $('.btnViewServerDetails').on('click', function(){
                                if ($(this.parentElement).data('status') == 'running')
                                    $('#serverStatus').html('(Status: Running)')
                                else if ($(this.parentElement).data('status') == 'unreachable')
                                    $('#serverStatus').html('(Status: Unreachable)')
                                else
                                    $('#serverStatus').html('')
                                var serverInfo = $(this.parentElement).data('info')
                                $('#lblHostName').text(serverInfo.hostname);
                                $('#lblServerName').text(serverInfo['name'])
                                $('#lblDescription').text(serverInfo['description'])
                                $('#lblClientListener').text(serverInfo['client-listener'].trim() == '' ? '21212' : serverInfo['client-listener']);
                                $('#lblAdminListener').text(serverInfo['admin-listener'].trim() == '' ? '21211' : serverInfo['admin-listener']);
                                $('#lblInternalListener').text(serverInfo['internal-listener'].trim() == '' ? '3021' : serverInfo['internal-listener'])
                                $('#lblHttpListener').text(serverInfo['http-listener'].trim() == '' ? '8080' : serverInfo['http-listener'])
                                $('#lblZookeeperListener').text(serverInfo['zookeeper-listener'].trim() == '' ? '7181' : serverInfo['zookeeper-listener'])
                                $('#lblReplicationListener').text(serverInfo['replication-listener'].trim() == '' ? '5555' : serverInfo['replication-listener'])
                                $('#lblInternalInterface').text(serverInfo['internal-interface'])
                                $('#lblExternalInterface').text(serverInfo['external-interface'])
                                $('#lblPublicInterface').text(serverInfo['public-interface'])
                                $('#lblPlacementGroup').text(serverInfo['placement-group'])
                                //$('#').text(serverInfo[''])

                            });

                            $(".dropdown").on("show.bs.dropdown", function() {
                              clicked_id = $(this).find('ul').data('id')
                              VdmUI.editServerId = clicked_id;
                            });

                            $(".dropdown").on("hide.bs.dropdown", function() {
                                VdmUI.editServerId = -1
                            });
                        } else{
                            $('.defaultServ').html('');
                            $('.defaultServ').html("<option value='0'>Default Setting</option>")
                            $('#serverList').html('<tr><td style="top:-1px !important">No servers available.</td><td></td></tr>')
                        }


                    }, dbData);
                } else {
                    $('.defaultServ').html('');
                    $('.defaultServ').html("<option value='0'>Default Setting</option>")
                    $('#serverList').html('<tr><td style="top:-1px !important">No server available.</td><td></td></tr>')
                    $('#warningMsg').html('No database has been added. Please click the down icon besides the Database to add a new database.')
                    VdmUI.showHideOverlay(false);
                }
            });
        };

        this.AddDbServer = function(serverData, server_ip){
            VdmService.AddServer(function(connection){
                var result = connection.Metadata['VDM_ADD_SERVER'].statusString
                if(result != 'Success'){
                    errorMsg = 'Server (' + server_ip + ') couldn\'t be started. ' + result + '.'
                    $('#errorMsg').html(errorMsg)
                    $('#errorDialog').modal('show');
                    VdmUI.showHideLoader(false)
                } else {
                    if($.inArray(server_ip, VdmUI.isServersStarting) == -1){
                        VdmUI.isServersStarting.push(server_ip)
                    }
                    var dbData = {
                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                    }
                    VdmService.GetMemberList(function(connection){
                        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                    }, dbData);
                }

            }, serverData);
        }

        this.StartDbServer = function(serverData, server_ip){
            VdmService.StartServer(function(connection){
                var result = connection.Metadata['VDM_START_SERVER'].statusString
                if(result != 'Success'){
                    errorMsg = 'Server (' + server_ip + ') couldn\'t be started. ' + result + '.'
                    $('#errorMsg').html(errorMsg)
                    $('#errorDialog').modal('show');
                    VdmUI.showHideLoader(false)
                } else {
                    if($.inArray(server_ip, VdmUI.isServersStarting) == -1){
                        VdmUI.isServersStarting.push(server_ip)
                    }
                    VdmUI.isServerStarting = true
                    var dbData = {
                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                    }
                    VdmService.GetMemberList(function(connection){
                        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                    }, dbData);
                }

            }, serverData);
        }

        this.CheckHostCount = function(systemInfo, serverInitStateArray){
            result = 'create'
            serverProperty = systemInfo.results[0].data
            var hostCount = 0
            for(var j=0; j < serverProperty.length; j++){
                propertyObject = serverProperty[j]
                if($.inArray('hostcount', propertyObject) > -1){
                    hostCount =  propertyObject[1]
                    break;
                }
            }
            server_keys = Object.keys(serverInitStateArray);
            initMemberCount = 0
            memberCount = server_keys.length

            for (var k = 0; k < server_keys.length; k++){
                if(serverInitStateArray[server_keys[k]] == true){
                    initMemberCount++;
                }
            }

            return hostCount == initMemberCount ? true : false
        }

        this.GetKSafetyValue = function(systemInfo){
            var kSafety = 0
            if(systemInfo != undefined){
                serverProperty = systemInfo.results[0].data
                for(var j=0; j < serverProperty.length; j++){
                    propertyObject = serverProperty[j]
                    if($.inArray('kfactor', propertyObject) > -1){
                        kSafety =  propertyObject[1]
                        break;
                    }
                }
            }
            return kSafety
        }

        this.GetRunningServerIp = function(serverStateArray){
            server_ip = ''
            server_keys = Object.keys(serverStateArray);
            for (var k = 0; k < server_keys.length; k++){
                if(serverStateArray[server_keys[k]] == 'running'){
                    server_ip = server_keys[k]
                    break;
                }
            }
            return server_ip;
        }

        this.GetRunningServerCount = function(serverStateArray){
            count = 0
            server_keys = Object.keys(serverStateArray);
            for (var k = 0; k < server_keys.length; k++){
                if(serverStateArray[server_keys[k]] == 'running'){
                    count++
                }
            }
            return count;
        }

        this.DisplayError = function(errorMsg){
            $('#errorMsg').html(errorMsg)
            $('#errorDialog').modal('show');
            VdmUI.showHideLoader(false)
        }

        this.resetTextBox = function(){
                $('#serverName').val('');
                $('#txtHostName').val('');
                $('#txtDescription').val('');
                $('#txtClientPort').val('');
                $('#errorClientPort').hide();
                $('#txtAdminPort').val('');
                $('#errorAdminPort').hide();
                $('#txtZookeeper').val('');
                $('#errorZookeeper').hide();
                $('#txtReplicationPort').val('');
                $('#errorReplicationPort').hide();
                $('#txtInternalInterface').val('');
                $('#txtExternalInterface').val('');
                $('#txtPublicInterface').val('');
                $('#txtInternalPort').val('');
                $('#txtHttpPort').val('');
                $('#txtPlacementGroup').val('');
                $('#errorServerName').hide();
                $('#errorHostName').hide();
                $('#errorDescription').hide();
                $('#errorInternalInterface').hide();
                $('#errorExternalInterface').hide();
                $('#errorPublicInterface').hide();
                $('#errorInternalPort').hide();
                $('#errorHttpPort').hide();
            }

        this.resetDbForm = function() {
                $('#txtDbName').val('');
                $('#errorDbName').hide();
            };

        this.displayDatabases = function(databaseList){
            if(databaseList == undefined)
                return;
            if(databaseList != undefined && databaseList['statusstring'] != undefined && databaseList['statusstring'] == 'Query timeout.'){
                $('#warningMsg').html('Deployment manager cannot be reached.')
                VdmUI.showHideOverlay(false);
                return
            }
            var htmlList = '';
            var isActiveDbFound = false;
            var isDbCookieDefined = true;
            var currentDbId = VdmUI.getCurrentDbCookie();
            if(currentDbId == -1){
                isDbCookieDefined = false
                currentDbId = databaseList.databases[0]['id'];
                VdmUI.saveCurrentDbCookie(currentDbId);
            }
            VdmUI.CurrentDbList = [];
            VdmUI.CurrentDbIdList = [];

            if(databaseList.databases ==  undefined || databaseList.databases.length == 0){
                $('#clusterName').html('');
                $('#warningMsg').html('No database has been added. Please click the down icon besides the Database to add a new database.')
                VdmUI.showHideOverlay(false);
            }else{
                VdmUI.showHideOverlay(true);
                databaseList.databases.forEach(function (info) {
                var dbName = info['name'];
                var dbId = info['id']
                var dbInfo = JSON.stringify(info);
                if(dbId == currentDbId){

                    $('#clusterName').html(dbName);
                    isActiveDbFound = true;
                    htmlList += '<tr>' +
                                '<td data-id="' + dbId + '"><a id="dbInfo_' + dbId + '" class="btnDbList selected" >' + dbName + '</a></td>' +
                                '<td data-id="' + dbId + '" data-info=\''+ dbInfo +'\'>' +
                                '<a id="btnUpdataDb_' + dbId + '" class="btnUpdateDatabase" data-toggle="modal" data-target="#addDatabase" >' +
                                '<div id="editDatabase_'+ dbId +'" class="editServerDatabase"></div>' +
                                '<span class="editServerDatabaseTxt" id="editDatabaseTxt_'+ dbId +'">Edit</span></a>' +
                                '<div class="loading-small-DB" id="loadingDatabase_'+ dbId +'" style="display:none"></div>' +
                                '</td>' +
                                '<td data-id="' + dbId + '" width="2%">' +
                                '<a id="btnDeleteDb_' + dbId + '" class="btnDeleteDatabase" data-toggle="modal" data-target="#deleteDatabase" >' +
                                '<div class="deleteDisableDS" id="deleteDatabase_'+ dbId +'"></div>' +
                                '<span class="deleteDisableDSTxt" id="deleteDatabaseTxt_'+ dbId +'">Delete</span></a></td>' +
                                '</tr>';
                                VdmUI.CurrentDbName = dbName;
                } else {
                    htmlList += '<tr>' +

                                '<td data-id="' + dbId + '"><a id="dbInfo_' + dbId + '" class="btnDbList" >' + dbName + '</a></td>' +
                                '<td data-id="' + dbId + '" data-info=\''+ dbInfo +'\' >' +
                                '<a id="btnUpdataDb_' + dbId + '" class="btnUpdateDatabase" data-toggle="modal" data-target="#addDatabase" >' +
                                '<div id="editDatabase_'+ dbId +'" class="editServerDatabase"></div>' +
                                '<span class="editServerDatabaseTxt" id="editDatabaseTxt_'+ dbId +'">Edit</span></a>' +
                                '<div class="loading-small-DB" id="loadingDatabase_'+ dbId +'" style="display:none"></div>' +
                                '</td>' +
                                '<td data-id="' + dbId + '" width="2%">' +
                                '<a id="btnDeleteDb_' + dbId + '" class="btnDeleteDatabase" href="javascript:void(0);"data-toggle="modal" data-target="#deleteDatabase" >' +
                                '<div class="deleteDatabaseServer" id="deleteDatabase_'+ dbId +'"></div>' +
                                '<span class="deleteServerDatabaseTxt" id="deleteDatabaseTxt_'+ dbId +'">Delete</span></a></td>' +
                                '</tr>';
                }
                VdmUI.CurrentDbList.push(dbName);
                VdmUI.CurrentDbIdList.push(dbId);
            });
                if(!isActiveDbFound){
                    currentDbId = databaseList.databases[0]['id'];
                    VdmUI.saveCurrentDbCookie(currentDbId);
                    if(!isDbCookieDefined) {
                        $('#errorMsg').html('The selected database has been deleted.')
                        $('#errorDialog').modal('show');
                    }
                }
            }
            if(htmlList == ""){
                $('#tblDatabaseList').html('<tr><td style="top:-1px !important">No database available.</td><td></td></tr>')
            }else{
                $('#tblDatabaseList').html(htmlList)
            }

            $('.btnUpdateDatabase').on('click', function(){
                VdmUI.resetDbForm();
                VdmUI.isDbCreate = false;
                var dbInfo = $(this.parentElement).data('info');
                VdmUI.dbToUpdate = dbInfo['name'];
                $('#addDatabase').data('id',dbInfo['id']);
                $('#txtDbName').val(dbInfo['name']);
                $('#dbTitle').html('Update Database');
            });

            $('.btnDeleteDatabase').on('click', function(e){
                if(!$(this.children).hasClass('deleteDisableDS')){
                    var dbId = $(this.parentElement).data('id');
                    $('#deleteDatabase').data('id', dbId);
                } else {
                    e.preventDefault();
                    e.stopPropagation();
                }
            })

            $('.btnDbList').on('click', function(){
                VdmUI.showHideLoader(true);
                var dbId = $(this.parentElement).data('id');
                VdmUI.saveCurrentDbCookie(dbId);
                $(this).addClass('selected');
                VdmService.GetDatabaseList(function(connection){
                    VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
                    var dbData = {
                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                    }
                    VdmService.GetMemberList(function(connection){
                        VdmUI.displayServers(connection.Metadata['MEMBER_LISTING'])
                        VdmUI.ChangeDirectories();
                        VdmUI.showHideLoader(false);
                    }, dbData);
                })
                 var dbData = {
                    id: VdmUI.getCurrentDbCookie()
                }

                VdmService.GetDeploymentUsers(function(connection){
                    VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']

                     VdmService.GetDeployment(function(connection){
                        VdmUI.Deployment = connection.Metadata['DEPLOYMENT'];
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                     },dbData);
                },dbData);

                $('#createDatabase').modal('hide');

                VdmUI.GetDatabaseStatus();


            })

        };

        this.saveCurrentDbCookie = function(dbId){
             saveCookie("current-db", dbId);
        };


        this.getCurrentDbCookie = function(){
            var dbId = -1;
            var count = 0;
            try {
                var dbId = $.parseJSON(VdmUI.getCookie("current-db"));
            } catch (e) {
                //do nothing
            }
            return dbId;
        };

        this.getCurrentServerCookie = function(){
           var dbId = -1;
            var count = 0;
            try {
                var dbId = $.parseJSON(VdmUI.getCookie("selected-serverId"));
            } catch (e) {
                //do nothing
            }
            return dbId;
        }

        this.showHideLoader = function(state){
            if(state){
                $('.loader').show();
                $('body').css({overflow: 'hidden'});
            } else{
                $('.loader').fadeOut(1000);
                $('body').css({overflow: ''});
            }
        };


        this.displayDeployment = function(deployment){

            if(deployment == undefined)
                return;

            if(!$('#txtSitePerHost').is(":focus")){
                clusterObjects.txtSitePerHost.val(deployment['deployment']['cluster']['sitesperhost']);
            }

            if(!$('#txtKSafety').is(":focus")){
            clusterObjects.txtKSafety.val(deployment['deployment']['cluster']['kfactor']);
            }

            if(!$('#txtHeartbeatTimeout').is(":focus")){
            clusterObjects.txtHeartbeatTimeout.val(deployment['deployment']['heartbeat']['timeout'])
            }

            if(!$('#txtQueryTimeout').is(":focus")){
            clusterObjects.txtQueryTimeout.val(deployment['deployment']['systemsettings']['query']['timeout'])
            }
            if(!$('#txtMaxTempTableMemory').is(":focus")){
            clusterObjects.txtMaxTempTableMemory.val(deployment['deployment']['systemsettings']['temptables']['maxsize'])
            }
            if(!$('#txtSnapshotPriority').is(":focus")){
            clusterObjects.txtSnapshotPriority.val(deployment['deployment']['systemsettings']['snapshot']['priority'])
            }
            if(!$('#txtMemoryLimit').is(":focus")){
                if(deployment['deployment']['systemsettings']['resourcemonitor']!= null)
                {
                    if(deployment['deployment']['systemsettings']['resourcemonitor']['memorylimit']!= null)
                    {
                        var memoryLimitText = deployment['deployment']['systemsettings']['resourcemonitor']['memorylimit']['size'];
                        var memoryLimitUnit = "GB";
                        var memoryLimitValue = 0;
                        if (memoryLimitText != undefined && memoryLimitText.indexOf("%") > -1) {
                            memoryLimitUnit = "%";
                            memoryLimitValue = memoryLimitText.replace("%", "");
                        } else if (memoryLimitText != undefined && memoryLimitText.indexOf("%") == -1) {
                            memoryLimitValue = memoryLimitText;
                        } else if (memoryLimitText == undefined) {
                            memoryLimitValue = undefined;
                    }

                    clusterObjects.txtMemoryLimit.val(memoryLimitValue);
                }
            }
            }
            if(!$("#selMemoryLimitUnit").is(":focus"))
            {
                if(deployment['deployment']['systemsettings']['resourcemonitor']!= null)
                    {
                        if(deployment['deployment']['systemsettings']['resourcemonitor']['memorylimit']!= null)
                        {
                                var memoryLimitText = deployment['deployment']['systemsettings']['resourcemonitor']['memorylimit']['size'];
                                var memoryLimitUnit = "GB";
                                var memoryLimitValue = 0;
                                if (memoryLimitText != undefined && memoryLimitText.indexOf("%") > -1) {
                                    memoryLimitUnit = "%";
                                    memoryLimitValue = memoryLimitText.replace("%", "");
                                } else if (memoryLimitText != undefined && memoryLimitText.indexOf("%") == -1) {
                                    memoryLimitValue = memoryLimitText;
                                } else if (memoryLimitText == undefined) {
                                    memoryLimitValue = undefined;
                            }
                            $("#selMemoryLimitUnit").val(memoryLimitUnit);
                        }
                    }
            }
            if(!$('#txtLogFrequencyTime').is(":focus")){
            clusterObjects.txtLogFrequencyTime.val(deployment['deployment']['commandlog']['frequency']['time'])
            }
            if(!$('#txtLogFreqTransaction').is(":focus")){
            clusterObjects.txtLogFreqTransaction.val(deployment['deployment']['commandlog']['frequency']['transactions'])
            }
            if(!$('#txtLogSegmentSize').is(":focus")){
            clusterObjects.txtLogSegmentSize.val(deployment['deployment']['commandlog']['logsize'])
            }
            if(!$('#txtFilePrefix').is(":focus")){
            clusterObjects.txtFilePrefix.val(deployment['deployment']['snapshot']['prefix'])
            }
            if(!$("#selFreqUnit").is(":focus"))
            {
            var frequency = deployment['deployment']['snapshot']['frequency'];
                var spanshotUnit = frequency != undefined ? frequency.slice(-1) : '';

                if (spanshotUnit == 's') {
                    $("#selFreqUnit").val('3');
                } else if (spanshotUnit == 'm') {
                    $("#selFreqUnit").val('2');
                } else if (spanshotUnit == 'h') {
                    $("#selFreqUnit").val('1');
                } else {
                    $("#selFreqUnit").val('');
                }
            }
            if(!$('#txtFrequency').is(":focus")){
                clusterObjects.txtFrequency.val(parseInt(deployment['deployment']['snapshot']['frequency']))
            }
            if(!$('#txtRetained').is(":focus")){
            clusterObjects.txtRetained.val(deployment['deployment']['snapshot']['retain'])
            }

            if(!$('#txtPartitionDetection').is(":focus")){
            clusterObjects.txtPartitionDetection.text(getOnOffText(deployment['deployment']['partition-detection']['enabled']));
            }

            if(deployment['deployment']['partition-detection']['enabled']==true)
            {
                clusterObjects.chkPartitionDetection.iCheck('check')
            }
            else
            {
                clusterObjects.chkPartitionDetection.iCheck('uncheck')
            }



            if(deployment['deployment']['users'] != null){
                if(deployment['deployment']['users']['user'].length!=0)
                {
                    $("#row-6 td:nth-child(2)").html('<input id="chkSecurity" tabindex="1" type="checkbox">')
                    $('#chkSecurity').iCheck({
                        checkboxClass: 'icheckbox_square-aero customCheckbox',
                        increaseArea: '20%'
                    });


                    clusterObjects.txtSecurity.text(getOnOffText(deployment['deployment']['security']['enabled']))
                    if(deployment['deployment']['security']['enabled']==true)
                    {
                        $('#chkSecurity').iCheck('check')
                    }
                    else
                    {
                        $('#chkSecurity').iCheck('uncheck')
                    }
                    clusterObjects.chkSecurity.next().attr('title','')

                }
                else
                {
                    $("#row-6 td:nth-child(2)").html('<div id="onOff" title="Security cannot be enabled when there are no users. You need to add at least one user to enable security." class="offIconSecurity"></div>')
                    clusterObjects.txtSecurity.text("Off")
                }
            }


            clusterObjects.txtHttpAccess.text(getOnOffText(deployment['deployment']['httpd']['enabled']))

            if(deployment['deployment']['httpd']['enabled']==true)
            {
                clusterObjects.chkHttpAccess.iCheck('check')
            }
            else
            {
                clusterObjects.chkHttpAccess.iCheck('uncheck')
            }

             clusterObjects.txtJsonApi.text(getOnOffText(deployment['deployment']['httpd']['jsonapi']['enabled']))

            if(deployment['deployment']['httpd']['jsonapi']['enabled']==true)
            {
                clusterObjects.chkJsonApi.iCheck('check')
            }
            else
            {
                clusterObjects.chkJsonApi.iCheck('uncheck')
            }

            clusterObjects.txtAutoSnapshot.text(getOnOffText(deployment['deployment']['snapshot']['enabled']))

            if(deployment['deployment']['snapshot']['enabled']==true)
            {
                clusterObjects.chkAutoSnapshot.iCheck('check')
            }
            else
            {
                clusterObjects.chkAutoSnapshot.iCheck('uncheck')
            }

            clusterObjects.txtCommandLog.text(getOnOffText(deployment['deployment']['commandlog']['enabled']))

            if(deployment['deployment']['commandlog']['enabled']==true)
            {
                clusterObjects.chkCommandLog.iCheck('check')
            }
            else
            {
                clusterObjects.chkCommandLog.iCheck('uncheck')
            }

            VdmUI.exportPropertyList = [];
            //Export property
            if(deployment['deployment']['export'] != null){
                if(deployment['deployment']['export']['configuration'] != null){
                    var config = deployment['deployment']['export']['configuration'];
                    for(var i = 0; i < config.length; i++){
                        config[i]['id'] = i;
                        VdmUI.exportPropertyList.push(config[i])
                    }
                }
            }
            VdmUI.displayExportProperties()

            VdmUI.displaySecurity()


            //Import property
            VdmUI.importPropertyList = [];
            if(deployment['deployment']['import'] != null){
                if(deployment['deployment']['import']['configuration'] != null){
                    var config = deployment['deployment']['import']['configuration'];
                    for(var i = 0; i < config.length; i++){
                        config[i]['id'] = i;
                        VdmUI.importPropertyList.push(config[i])
                    }
                }
            }
            VdmUI.displayImportProperties()


            //Disk Limit
            VdmUI.diskLimitList = [];
            if(deployment['deployment']['systemsettings']['resourcemonitor'] != null){
                if(deployment['deployment']['systemsettings']['resourcemonitor']['disklimit'] != null &&
                deployment['deployment']['systemsettings']['resourcemonitor']['disklimit'].hasOwnProperty('feature')){
                    var config = deployment['deployment']['systemsettings']['resourcemonitor']['disklimit']['feature'];
                    for(var i = 0; i < config.length; i++){
                        config[i]['id'] = i;
                        VdmUI.diskLimitList.push(config[i])
                    }
                }
            }
            VdmUI.displayDiskLimitProperties();

            //DR Configuration
            if(deployment['deployment']['dr'] != null && !$.isEmptyObject(deployment['deployment']['dr'])){
                $('.clsDrType').show();
                $('#onOffIcon').removeClass('checked').addClass('checked')
                clusterObjects.chkDrOnOffVal.text('On')
                clusterObjects.lblDrId.text(deployment['deployment']['dr'].id);
                if(deployment['deployment']['dr'].port != undefined){
                    clusterObjects.rowDrPort.show();
                    clusterObjects.lblDrPort.text(deployment['deployment']['dr'].port);
                } else {
                    clusterObjects.rowDrPort.hide();
                    clusterObjects.lblDrPort.text('');
                }
                var htmlConSource = '';
                var htmlServers = '';
                var htmlServerList = '';
                if(deployment['deployment']['dr'].connection != null && ! $.isEmptyObject(deployment['deployment']['dr'].connection)){
                    VdmUI.isConnectionSourceDefined = true;
                    $('#trConnectionSource').show()
                    VdmUI.ConnectionSourceStatus = true;
                    var source = deployment['deployment']['dr']['connection'].source;
                    VdmUI.masterCluster = source;
                    $('#master-cluster-name').html(source);
                } else {
                    VdmUI.isConnectionSourceDefined = false;
                    $('#trConnectionSource').hide()
                    VdmUI.ConnectionSourceStatus = false;
                    $('#row-master-cluster').hide();
                    $('#row-server').hide()
                }
            } else {
                $('.clsDrType').hide();
                clusterObjects.rowDrPort.hide();
                clusterObjects.lblDrPort.text('');
                $('#onOffIcon').removeClass('checked')
                clusterObjects.chkDrOnOffVal.text('Off')
            }

            $('#chkSecurity').on('ifClicked', function(event){
                UpdateCheckBox(clusterObjects.txtSecurity,'security');
            });
        };

        this.displayDefaultPaths = function(){
            dbData = {
               id: VdmUI.getCurrentDbCookie()
            }

            VdmService.GetDeployment(function(connection){
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                if(!$('#txtVoltdbRootDir').is(":focus")){
                    $("#txtVoltdbRootDir").val(deploymentInfo['deployment']['paths']['voltdbroot']['path'])
                }
                if(!$('#txtSnapshotDir').is(":focus")){
                    $("#txtSnapshotDir").val(deploymentInfo['deployment']['paths']['snapshots']['path'])
                }
                if(!$('#txtExportOverflowDir').is(":focus")){
                    $("#txtExportOverflowDir").val(deploymentInfo['deployment']['paths']['exportoverflow']['path'])
                }
                if(!$('#txtCommandLogDir').is(":focus")){
                    $("#txtCommandLogDir").val(deploymentInfo['deployment']['paths']['commandlog']['path'])
                }
                if(!$('#txtCommandLogSnapDir').is(":focus")){
                    $("#txtCommandLogSnapDir").val(deploymentInfo['deployment']['paths']['commandlogsnapshot']['path'])
                }
                if(!$('#txtDrOverflow').is(":focus")){
                    $("#txtDrOverflow").val(deploymentInfo['deployment']['paths']['droverflow']['path'])
                }

            },dbData);
        }

        var getOnOffText = function (isChecked) {
            return (isChecked) ? "On" : "Off";
        };

        this.displayExportProperties = function(){
            var exportList = '';
            var showHide = 'none';
            if ($('#row-4').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-4').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                showHide = ''
            }
            for(var i = 0; i < VdmUI.exportPropertyList.length; i++){
                var name = VdmUI.exportPropertyList[i].stream;
                var type = VdmUI.exportPropertyList[i].type.toUpperCase();
                var property = VdmUI.exportPropertyList[i].property;
                var enabled = VdmUI.exportPropertyList[i].enabled;
                var showHideSub = 'none';
                var subClass = 'glyphicon-triangle-right'
                if ($('#row-41-'+ i).find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
                $('#row-41-'+ i).find('td:first-child > a').hasClass('glyphicon-triangle-bottom') && $('#row-4').find('td:first-child > a').hasClass('glyphicon-triangle-bottom') ) {
                    showHideSub = ''
                    subClass = 'glyphicon-triangle-right glyphicon-triangle-bottom'
                }
                exportList += '<tr data-property=\''+ VdmUI.exportPropertyList[i].id +'\' class="child-row-4 subLabelRow parentprop exportPropertyClass" id="row-41-' + i + '" style="display:'+ showHide +'">' +
                       '<td class="configLabel"><a href="javascript:void(0)" class="'+subClass+'">' +
                       '<span id="exportList_'+ i +'" class="fontFamily">' + name + '(' + type + ')' + '</span></a></td>';
                       if(enabled){
                           exportList += '<td align="right"><div class="onIcon"></div></td>' +
                                        '<td class="onOff" id="txtAutoSnapshot">On</td>';
                       } else{
                           exportList +=  '<td align="right"><div class="offIcon"></div></td>' +
                                        '<td class="onOff" id="txtAutoSnapshot">Off</td>' ;
                                        }

                       exportList += '<td><a id="btnUpdateExport_'+ i +'" class="btnUpdateExport editIcon" href="javascript:void(0);"data-toggle="modal" data-target="#plusExport"></a></td>' +
                                    '</tr>';
                if(property != undefined)
                {
                    if(property.length > 0){
                        for(var j = 0; j < property.length; j++){
                           var propertyName = property[j].name;
                           var propertyValue = property[j].value;
                           exportList += '<tr class="childprop-row-41-' + i + ' subLabelRow exportPropertyClass" style="display:'+showHideSub+'">' +
                           '<td class="configLabel-sub">' + propertyName + '</td>' +
                           '<td align="right" >' + propertyValue + '</td>' +
                           '<td>&nbsp;</td>' +
                           '<td>&nbsp;</td>' +
                           '</tr>';
                        }
                     }else{
                        exportList += '<tr class="childprop-row-41-' + i + ' subLabelRow exportPropertyClass" style="display:'+showHideSub+'">' +
                            '<td class="configLabel-sub">No property available.</td>' +
                            '<td>&nbsp;</td>' +
                            '<td>&nbsp;</td>' +
                            '<td>&nbsp;</td>' +
                           '</tr>';
                     }
                }
            }
            $('.exportPropertyClass').remove()
            if(exportList == ''){
                exportList = '<tr class="child-row-4 subLabelRow exportPropertyClass parentprop" id="trExport"  style="display:'+ showHide +'">' +
                    '<td class="configLabel">Click on the plus sign to add new export configurations.</td>' +
                    '<td>&nbsp;</td>' +
                    '<td>&nbsp;</td>' +
                    '<td>&nbsp;</td>' +
                    '</tr>';
                $('#row-4').after(exportList)
            } else {
                $('#row-4').after(exportList)
            }
            // Make Expandable Rows for Export Properties.
            $('tr.parentprop > td:first-child' || 'tr.parentprop > td:fourth-child')
                .css("cursor", "pointer")
                .attr("title", "Click to expand/collapse")
                .click(function () {
                    var parent = $(this).parent();
                    $('.childprop-' + parent.attr("id")).toggle();
                    parent.find(".glyphicon-triangle-right").toggleClass("glyphicon-triangle-bottom");
                });


            $('.btnUpdateExport').on('click', function(){
                var id = $(this.parentElement.parentElement).data('property');
                var exportInfo = VdmUI.exportPropertyList[id];

                $('#plusExport').data('id', id);
                VdmUI.exportEditId = id;
                VdmUI.openExportPopup();
                VdmUI.afterOpenExportPopup(exportInfo);
            })



        }

        this.displayImportProperties = function(){
            var importList = '';
            var showHide = 'none';
            if ($('#row-7').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-7').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                showHide = ''
            }
            for(var i = 0; i < VdmUI.importPropertyList.length; i++){
                var name = VdmUI.importPropertyList[i].module;
                var type = VdmUI.importPropertyList[i].type.toUpperCase();
                var property = VdmUI.importPropertyList[i].property;
                var enabled = VdmUI.importPropertyList[i].enabled;
                var format = VdmUI.importPropertyList[i].format;
                var showHideSub = 'none';
                var subClass = 'glyphicon-triangle-right'
                 if ($('#row-71-'+ i).find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
                $('#row-71-'+ i).find('td:first-child > a').hasClass('glyphicon-triangle-bottom') && $('#row-7').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                    showHideSub = ''
                    subClass = 'glyphicon-triangle-right glyphicon-triangle-bottom'
                }

                importList += '<tr data-property=\''+ VdmUI.importPropertyList[i].id +'\' class="child-row-7 subLabelRow importParentProp  importPropertyClass" id="row-71-' + i +'" style="display:'+ showHide +'">' +
                       '<td class="configLabel"><a href="javascript:void(0)" class="'+subClass+'">' +
                       '<span class="fontFamily">' + format + '(' + type + ')' + '</span></a></td>';
                       if(enabled){
                           importList += '<td align="right"><div class="onIcon"></div></td>' +
                                        '<td class="onOff" id="txtImportOnOff">On</td>';
                       } else{
                           importList +=  '<td align="right"><div class="offIcon"></div></td>' +
                                        '<td class="onOff" id="txtImportOnOff">Off</td>' ;
                       }

                       importList += '<td><a class="btnUpdateImport editIcon" href="javascript:void(0);"data-toggle="modal" data-target="#plusImport"></a></td>' +
                                    '</tr>';
                if(property != undefined)
                {
                    if(property.length > 0){
                        for(var j = 0; j < property.length; j++){
                           var propertyName = property[j].name;
                           var propertyValue = property[j].value;
                           importList += '<tr class="imp-childprop-row-71-' + i + ' subLabelRow importPropertyClass" style="display:'+showHideSub+'">' +
                           '<td class="configLabel-sub">' + propertyName + '</td>' +
                           '<td align="right" >' + propertyValue + '</td>' +
                           '<td>&nbsp;</td>' +
                           '<td>&nbsp;</td>' +
                           '</tr>';
                        }
                     } else {
                        importList += '<tr class="imp-childprop-row-71-' + i + ' subLabelRow importPropertyClass" style="display:'+showHideSub+'">' +
                           '<td class="configLabel-sub">No property available.</td>' +
                            '<td>&nbsp;</td>' +
                            '<td>&nbsp;</td>' +
                            '<td>&nbsp;</td>' +
                           '</tr>';
                     }
                 }
            }
            $('.importPropertyClass').remove()
            if(importList == ''){
                importList = '<tr class="child-row-7 subLabelRow importPropertyClass importParentProp" id="trExport"  style="display:'+ showHide +'">' +
                    '<td class="configLabel">Click on the plus sign to add new import configurations.</td>' +
                    '<td>&nbsp;</td>' +
                    '<td>&nbsp;</td>' +
                    '<td>&nbsp;</td>' +
                    '</tr>';
                $('#row-7').after(importList)
            } else {
                $('#row-7').after(importList)
            }

            // Make Expandable Rows for Export Properties.
            $('tr.importParentProp > td:first-child' || 'tr.importParentProp > td:fourth-child')
                .css("cursor", "pointer")
                .attr("title", "Click to expand/collapse")
                .click(function () {
                    var parent = $(this).parent();
                    $('.imp-childprop-' + parent.attr("id")).toggle();
                    parent.find(".glyphicon-triangle-right").toggleClass("glyphicon-triangle-bottom");
                });

            $('.btnUpdateImport').on('click', function(){
                var id = $(this.parentElement.parentElement).data('property');
                importInfo = VdmUI.importPropertyList[id];
                $('#plusImport').data('id', importInfo.id);
                VdmUI.importEditId = id;
                VdmUI.openImportPopup();
                VdmUI.afterOpenImportPopup(importInfo);
            })



        }

        this.openExportPopup = function(){
            //For adding a new configuration
            if (VdmUI.exportEditId == "-1") {
                $("#btnAddExportConfigTitle").text("Add Configuration");
            }//For editing an existing configuration
            else {
                $("#btnAddExportConfigTitle").text("Edit Configuration");
            }
            var contents = '' +
                '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tbody>' +
                '<tr id="trExportStream">' +
                '    <td>Stream</td>' +
                '    <td width="15%">' +
                '       <input id="txtExportStream" name="txtExportStream" type="text" size="38">' +
                '       <label id="errorExportStream" for="txtExportStream" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td width="8%" align="right">' +
                '        <input tabindex="10" type="checkbox" id="chkExportStream" name="chkExportStream" checked="oncheck"></td>' +
                '    <td id="chkExportStreamValue" class="icheck-label">On</td>' +
                '</tr>' +
                '<tr>' +
                '    <td>Type </td>' +
                '    <td>' +
                '       <select id="txtExportType" name="txtExportType">' +
                '           <option>ELASTICSEARCH</option>' +
                '           <option>FILE</option>' +
                '           <option>HTTP</option>' +
                '           <option>JDBC</option>' +
                '           <option>KAFKA</option>' +
                '           <option>RABBITMQ</option>' +
                '           <option>CUSTOM</option>' +
                '       </select></td> ' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '<tr id="trExportConnectorClass" style="display:none">' +
                '    <td>Custom connector class</td>' +
                '    <td width="15%" id="tdExportConnectorClass">' +
                '        <input id="txtExportConnectorClass" name="txtExportConnectorClass" type="text" size="38">' +
                '        <label id="errorExportConnectorClass" for="txtExportConnectorClass" class="error" style="display: none;"></label></td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '  </tr>' +
                '</tbody>' +
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
            $('#addExportConfigWrapper').html(contents)

            $('#chkExportStream').iCheck({
                checkboxClass: 'icheckbox_square-aero customCheckbox',
                increaseArea: '20%'
            });

            $('#chkExportStream').on('ifChanged', function () {
                $("#chkExportStreamValue").text(getOnOffText($('#chkExportStream').is(":checked")));
            });

            $('#txtCustomType').focusout(function () {
                // Uppercase-ize contents
                this.value = this.value.toUpperCase();
            });

            $('#txtExportType').change(function () {
                showHideConnectorClass();
                if (typeof type === "undefined") {
                    addExportProperties();
                }
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
                    '   <td><div class="popDelete" id="deleteFirstProperty" onclick="deleteRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewProperty").append(newRow);
            });

//            $("#formAddConfiguration").validate({
//                rules: {
//                    txtExportStream: validationRules.streamNameRules,
//                    txtExportType: validationRules.streamNameRules,
//                    txtExportConnectorClass: validationRules.streamNameRules
//                },
//                messages: {
//                    txtExportStream: validationRules.streamNameMessages,
//                    txtExportType: validationRules.streamNameMessages,
//                    txtExportConnectorClass: validationRules.streamNameMessages
//                }
//            });

            if (VdmUI.exportEditId == "-1") {
                $("#deleteExportConfig").hide();
            }//For editing an existing configuration
            else {
                $("#deleteExportConfig").show();
            }

        };

        this.afterOpenExportPopup = function(exportValue){
            //For editing an existing configuration
            if (VdmUI.exportEditId != -1) {
                $('#txtExportStream').val(exportValue.stream);

                $("#txtExportType").val(exportValue.type.toUpperCase())

                $('#txtExportConnectorClass').val(exportValue.exportconnectorclass)
                showHideConnectorClass();
                if (typeof type === "undefined") {
                    addExportProperties();
                }


                if(exportValue.enabled){
                    $('#chkExportStream').iCheck('check');
                    $('#chkExportStreamValue').html('On');
                } else{
                    $('#chkExportStream').iCheck('uncheck');
                     $('#chkExportStreamValue').html('Off');
                }

                var properties = exportValue.property;

                if (properties.length == 0) {
                    $("#deleteFirstProperty").trigger("click");
                }

                var count = 1;
                var multiPropertyCount = 0;
                for (var i = 0; i < properties.length; i++) {
                    if (VdmUI.newStreamMinmPropertyName.hasOwnProperty(properties[i].name) || VdmUI.newStreamMinmPropertyName.hasOwnProperty(properties[i].name + '_' + exportValue.type)) {
                        if (properties[i].name == "broker.host" || properties[i].name == "amqp.uri") {
                            $("#selectRabbitMq").val(properties[i].name);
                        }
                        if ($(VdmUI.newStreamMinmPropertyName[properties[i].name]).length) {
                            $(VdmUI.newStreamMinmPropertyName[properties[i].name]).val(properties[i].value);
                            $(".newStreamMinProperty").addClass("orgProperty");
                        } else if ($(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + exportValue.type]).length && multiPropertyCount == 0) {
                            $(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + exportValue.type]).val(properties[i].value);
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
            }else{
//                $("#txtMetadataBrokerListValue").val("");
//                $("#txtEndpointESValue").val("");
//                $("#txtEndpointValue").val("");
//                $("#txtFileTypeValue").val("");
//                $("#txtnonceValue").val("");
//                $("#txtOutdirValue").val("");
//                $("#txtRabbitMqValue").val("");
//                $("#txtJdbcDriverValue").val("");
//                $("#txtJdbcUrlValue").val("")
                showHideConnectorClass();
                if (typeof type === "undefined") {
                    addExportProperties();
                }
            }


        }

        this.displayDiskLimitProperties = function(){
            var diskLimitList = '';
            var showHide = 'none';

            if ($('#row-51').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-51').find('td:first-child > a').hasClass('glyphicon-triangle-bottom') &&
            $('#row-5').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-5').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                showHide = ''
            }

            if(VdmUI.diskLimitList.length > 0){
                for(var j = 0; j < VdmUI.diskLimitList.length; j++){
                   var propertyName = VdmUI.diskLimitList[j].name;
                   var propertyValue = VdmUI.diskLimitList[j].size;
                   var unit = "";
                        if (propertyValue.indexOf("%") == -1) {
                            unit = "GB";
                        }
                   diskLimitList += '<tr class="childprop-row-51 subLabelRow featureClass" style="display:'+showHide+'">' +
                   '<td class="configLabel-sub">' + propertyName.toUpperCase() + '</td>' +
                   '<td align="right" >' + propertyValue +' '+ unit + '</td>' +
                   '<td>&nbsp;</td>' +
                   '<td>&nbsp;</td>' +
                   '</tr>';
                }
             }else{
                diskLimitList += '<tr class="childprop-row-51 subLabelRow featureClass" style="display:'+showHide+'">' +
                    '<td class="configLabel-sub" colspan="4">No features available.</td>' +
                    '</tr>';
             }

            childRowClass = $('.child-row-5').find('td:first-child > a').attr('class')
            $('.featureClass').remove()

            $('#row-51').after(diskLimitList)

        }

         // Make Expandable Rows for Export Properties.
            $('tr.diskLimitParentProp > td:first-child' || 'tr.diskLimitParentProp > td:fourth-child')
                .css("cursor", "pointer")
                .attr("title", "Click to expand/collapse")
                .on('click',function () {
                    var parent = $(this).parent();
                    $('.childprop-' + parent.attr("id")).toggle();
                    parent.find(".glyphicon-triangle-right").toggleClass("glyphicon-triangle-bottom");
                });

             $('.defaultServ').on('change', function(){
                VdmUI.ChangeDirectories();
             });

        this.openDiskLimitPopup = function(){

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
                 '                    <th width="53%">Name</th>' +
                 '                    <th align="right" width="37%">Value</th>' +
                 '                    <th align="right" width="14%">Unit</th>' +
                 '                    <th width="5%">Delete</th>' +
                 '                </tr>' +

                 '            </table>' +
                 '        </div>' +
                 '    </td>' +
                 '</tr>' +
                 '</table>';

            $("#addDiskLimitWrapper").html(contents);

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
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newFeatureValue newFeature" type="text">' +
                    '       <label id="errorValueDL' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                     '   <td><select id="' + unitId + '" name="' + unitId + '" class="newFeatureUnit newFeature"><option>GB</option><option>%</option></select>' +
                    '       <label id="errorValueUnit' + count + '" for="' + unitId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="deleteDiskLimit" id="deleteFirstFeature" onclick="deleteDiskLimitRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewFeature").append(newRow);
            });

            $("#formAddDiskLimit").validate();
        }

        this.afterOpenDiskLimitPopup = function(){

            var features = VdmUI.diskLimitList;

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
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newFeatureValue newFeature" type="text">' +
                    '       <label id="errorValueDL' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><select id="' + unitId + '" name="' + unitId + '" class="newFeatureUnit newFeature"><option>GB</option><option>%</option></select>' +
                    '       <label id="errorValueUnit' + count + '" for="' + unitId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="deleteDiskLimit" id="deleteFirstFeature" onclick="deleteDiskLimitRow(this)"></div></td>' +
                    '</tr>';

                $("#tblAddNewFeature").append(newRow);

                $("#" + nameId).val(features[i].name.toUpperCase());


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

        this.displaySecurity = function(){
            var securityList = '';
            var showHide = 'none';

            if ($('#row-6').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-6').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                showHide = ''
            }

            VdmUI.orgUserList = [];
            var dbData = {id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()}
            VdmService.GetDeploymentUsers(function(connection){
                VdmUI.DeploymentUsers = connection.Metadata['DEPLOYMENT_USER']
                for(var i = 0; i < VdmUI.DeploymentUsers.deployment.length; i++){

                    var username = VdmUI.DeploymentUsers.deployment[i].name;
                    VdmUI.orgUserList.push(username);
                    VdmUI.userId = VdmUI.DeploymentUsers.deployment[i].userid;
                    var roles = VdmUI.DeploymentUsers.deployment[i].roles;
                        securityList += '<tr  style="display:'+showHide+'" class="child-row-6 subLabelRow securityList" data-id=\''+ VdmUI.userId +'\' data-property=\''+ JSON.stringify(VdmUI.DeploymentUsers.deployment[i]) +'\'><td class="configLabel">'+ username + '</td><td>'+ roles +'</td><td>&nbsp;</td><td>' +
                        '<a class="btnUpdateSecurity editIcon" href="javascript:void(0);" data-toggle="modal" data-target="#plusSecurity"></a><td></tr>'
                    }
                $('.securityList').remove();
                if(securityList == ''){
                    securityList = '<tr class="child-row-6 subLabelRow securityList" id="trSecurity" style="display:'+showHide+'">' +
                        '<td class="configLabel">No security available.</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                        '</tr>';

                        $('.secTbl1').after(securityList)
                } else {
                        $('.secTbl1').after(securityList)
                }

                $('.btnUpdateSecurity').on('click', function(){
                    VdmUI.userEditId = 1;
                    $('#addUserInnerPopup').data('isupdate', 1);
                    var securityInfo = $(this.parentElement.parentElement).data('property');
                    $('#plusSecurity').data('id', securityInfo.id);
                    $('#addUserInnerPopup').data('userid',securityInfo.userid);
                    orgUser = securityInfo.name;
                    VdmUI.openSecurityPopup();
                    VdmUI.afterOpenSecurityPopup(securityInfo);
                })
            }, dbData);
        }

        this.openSecurityPopup = function(){
            //For adding a new configuration
            if (VdmUI.userEditId == "-1") {
                $("#btnAddSecurityTitle").text("Add User");
                $("#deleteUser").css('display', 'none');
            }//For editing an existing configuration
            else {
                $("#btnAddSecurityTitle").text("Edit User");
                $("#deleteUser").css('display', 'block');
            }
            var contents = '' +
                '<table width="100%" cellpadding="0" cellspacing="0" class="modalTbl">' +
                '<tbody><tr><td width="30%">Username</td><td width="10%" class="tdUserName">' +
                '<input id="txtUser" name="txtUser" type="text" size="30" aria-required="true" class="error">' +
                '<label id="errorUser" for="txtUser" class="error" style="display:none">This field is required</label>' +
                '<input id="txtOrgUser" name="txtOrgUser" type="text" size="30" aria-required="true" style="display:none"></td>' +
                 '<td>&nbsp;</td> <td>&nbsp;</td></tr><tr><td><span id="labelPassword">Password</span> </td>' +
                  '<td><input id="txtPassword" name="txtPassword" type="password" size="30" aria-required="true" class="error">' +
                  '<label id="errorPassword" for="txtPassword" class="error" style="display:none">This field is required</label> </td>' +
                  '<td>&nbsp;</td> <td>&nbsp;</td></tr>' +
                  '<tr>' +
                  '<td>Roles </td> <td class="tdROle">' +
                  '<input type="text" class="form-control" id="txtUserRole"  name="txtUserRole" value="" />' +
                  '<label id="errorRole" for="txtUserRole" class="error" style="display:none">This field is required</label>' +
                  '<span class="tokenNote"> <i>Note: Please type role and hit <b>"Enter"</b> or <b>"Tab"</b></i></span>' +
                  '</td> ' +
                  '<td>&nbsp;</td>' +
                  '<td>&nbsp;</td>' +
                  '</tr></tbody></table>'

            $('#addSecurityConfigWrapper').html(contents)

            $('#txtUserRole').tokenfield({
              autocomplete: {
                source: VdmUI.autocompleteSource,
                delay: 100
              },
              showAutocompleteOnFocus: true
            })
            $('#txtUserRole').on('tokenfield:createtoken', function (e) {
                var data = e.attrs.value.split('|')
                value = data[1] || data[0]
                var roles = $('#txtUserRole').val().split(',')
                for(var i =0; i < roles.length; i++){
                    roles[i] = roles[i].trim().toLowerCase()
                }
                if($.inArray(value.toLowerCase(), roles) > -1){
                    $('#txtUserRole-tokenfield').val('')
                    e.preventDefault()
                    e.stopPropagation()
                    return
                }
            })
            $('#txtUserRole').on('tokenfield:createdtoken', function (e) {
                var re = /^[a-zA-Z0-9_.-]+$/
                var valid = re.test(e.attrs.value)
                if (!valid) {
                  $(e.relatedTarget).addClass('invalid')
                }
            })
            $('#txtUserRole').on('tokenfield:removedtoken', function (e) {
                $("#frmAddUser").valid()
            })
            $("#txtUserRole" ).change(function() {
                if($('#errorRole').is(":visible"))
                    $("#frmAddUser").valid()
            });
        }

        this.autocompleteSource = function(request, response){
            sources = ['administrator', 'user']
            new_source = []
            values = $('#txtUserRole').val().split(',')
            for(var i =0; i < values.length; i++){
                values[i] = values[i].trim().toLowerCase()
            }
            for(var i =0; i < sources.length; i++){
                if($.inArray(sources[i], values) == -1 && (request.term == '' || sources[i].toLowerCase().indexOf(request.term.toLowerCase()) == 0)){
                    new_source.push(VdmUI.capitalizeFirstLetter(sources[i]))
                }
            }
            response(new_source)

        }

        this.capitalizeFirstLetter = function(string) {
            return string.charAt(0).toUpperCase() + string.slice(1);
        }

        this.afterOpenSecurityPopup = function(securityValue){
             if (VdmUI.userEditId == -1) {
                    $('#labelPassword').html('Password');
                    $('#addUserHeader').html('Add User');
                } else {
                    $('#labelPassword').html('New Password');
                    $('#addUserHeader').html('Edit User');
                    $('#txtUser').val(securityValue.name);
                    $("#txtPassword").val(securityValue.password);
                    $('#txtUserRole').tokenfield('setTokens', securityValue.roles);
                    $('#txtOrgUser').val(securityValue.name);
                    orgUser = securityValue.name;
                }
        }

        this.openImportPopup = function(){
            //For adding a new configuration
            if (VdmUI.importEditId == "-1") {
                $("#addImportConfigTitle").text("Add Configuration");
            }//For editing an existing configuration
            else {
                $("#addImportConfigTitle").text("Edit Configuration");
            }
            var contents = '' +
                '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
                '<tbody>' +
                '<tr id="trImportType">' +
                '    <td width="30%">Type </td>' +
                '    <td width="50%">' +
                '       <select id="txtImportType" name="txtImportType">' +
                '           <option>KAFKA</option>' +
                '           <option>CUSTOM</option>' +
                '       </select></td> ' +
                '    </td>' +
                '    <td width="10%" align="right">' +
                '        <input tabindex="10" type="checkbox" id="chkImportStream" name="chkImportStream" checked="oncheck"></td>' +
                '    <td id="chkImportStreamValue" class="icheck-label">On</td>' +
                '</tr>' +
                '<tr id="trImportConnectorClass" >' +
                '    <td>Format</td>' +
                '    <td id="tdImportConnectorClass">' +
                '<div class="form-group formatImport">' +
                '<div class="input-group"><input type="text" class="formatHeight" id="txtImportConnectorClass" name="txtImportConnectorClass">' +
                '<div class="input-group-addon drop-down-trigger"><span class="caret"></span></div>' +
                '</div>' +
                '   <ul id="ddlFormat" class="drop-down-list">' +
                '       <li>csv</li>' +
                '       <li>tsv</li>' +
                '   </ul>' +
                '</div> ' +
                '        <label id="errorImportConnectorClass" for="txtImportConnectorClass" class="error" style="display: none;"></label></td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '  </tr>' +
                '<tr id="trImportStream" style="display:none">' +
                '    <td>Module</td>' +
                '    <td>' +
                '       <input id="txtImportStream" name="txtImportStream" type="text" size="38">' +
                '       <label id="errorImportStream" for="txtImportStream" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '</tbody>' +
                '</table>' +

                '<table width="100%" cellpadding="0" cellspacing="0" class="configurTbl">' +
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
            $('#addImportConfigWrapper').html(contents)

            $('ul.drop-down-list li').click(function(e){
                $('#txtImportConnectorClass').val($(this).text());
                $('ul.drop-down-list').hide()
            });

            $('#txtImportFormat').on('input', function(){
                $('ul.drop-down-list').hide()
            })

            $('#chkImportStream').iCheck({
                checkboxClass: 'icheckbox_square-aero customCheckbox',
                increaseArea: '20%'
            });

            $('#chkImportStream').on('ifChanged', function () {
                $("#chkImportStreamValue").text(getOnOffText($('#chkImportStream').is(":checked")));
            });

            $('#txtImportType').focusout(function () {
                // Uppercase-ize contents
                this.value = this.value.toUpperCase();
            });

            $('#txtImportType').change(function () {
                showHideImportConnectorClass();
                if (typeof type === "undefined") {
                    addImportProperties();
                }
            });


            var count = 0;

            $("#lnkAddNewImportProperty").on("click", function () {
                count++;
                var nameId = 'txtName' + count;
                var valueId = 'txtValue' + count;

                var newRow = '<tr>' +
                    '   <td>' +
                    '       <input size="15" id="' + nameId + '" name="' + nameId + '" class="newStreamImportPropertyName newStreamImportProperty" type="text">' +
                    '       <label id="errorName' + count + '" for="' + nameId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="' + valueId + '" name="' + valueId + '" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
                    '       <label id="errorValue' + count + '" for="' + valueId + '" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td><div class="popDelete" id="deleteFirstProperty" onclick="deleteImportRow(this)"></div></td>' +
                    '</tr>';
                $("#tblAddNewImportProperty").append(newRow);
            });

            if (VdmUI.importEditId == "-1") {
                $("#deleteImportConfig").hide();
            }//For editing an existing configuration
            else {
                $("#deleteImportConfig").show();
            }

            $('.drop-down-trigger').on('click', function(){
                $('ul.drop-down-list').toggle();
            })
        };

        this.afterOpenImportPopup = function(importValue){
            //For editing an existing configuration
            if (VdmUI.importEditId != -1) {
                $('#txtImportStream').val(importValue.module);

                $("#txtImportType").val(importValue.type.toUpperCase())

                $('#txtImportConnectorClass').val(importValue.format)

                showHideImportConnectorClass();
                if (typeof type === "undefined") {
                    addImportProperties();
                }


                if(importValue.enabled){
                    $('#chkImportStream').iCheck('check');
                    $('#chkImportStreamValue').html('On');
                } else{
                    $('#chkImportStream').iCheck('uncheck');
                     $('#chkImportStreamValue').html('Off');
                }

                var properties = importValue.property;

                if (properties.length == 0) {
                    $("#deleteFirstProperty").trigger("click");
                }

                var count = 1;
                var multiPropertyCount = 0;
                for (var i = 0; i < properties.length; i++) {
                    if (VdmUI.newStreamMinmPropertyName.hasOwnProperty(properties[i].name) || VdmUI.newStreamMinmPropertyName.hasOwnProperty(properties[i].name + '_' + importValue.type)) {
                        if (properties[i].name == "broker.host" || properties[i].name == "amqp.uri") {
                            $("#selectRabbitMq").val(properties[i].name);
                        }
                        if ($(VdmUI.newStreamMinmPropertyName[properties[i].name]).length) {
                            $(VdmUI.newStreamMinmPropertyName[properties[i].name]).val(properties[i].value);
                            $(".newStreamMinImportProperty").addClass("orgImportProperty");
                        } else if ($(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + importValue.type]).length && multiPropertyCount == 0) {
                            $(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + importValue.type]).val(properties[i].value);
                            $(".newStreamMinImportProperty").addClass("orgImportProperty");
                            multiPropertyCount++;
                        } else {
                            $("#lnkAddNewImportProperty").trigger("click");
                            $("#txtName" + count).val(properties[i].name);
                            $("#txtValue" + count).val(properties[i].value);
                            count++;
                        }
                    } else {
                        $("#lnkAddNewImportProperty").trigger("click");
                        $("#txtName" + count).val(properties[i].name);
                        $("#txtValue" + count).val(properties[i].value);
                        count++;
                    }
                }
            }else{

                showHideImportConnectorClass();
                if (typeof type === "undefined") {
                    addImportProperties();
                }
            }
        }

        this.showHideOverlay = function(state){
            if(state){
                $('#dbWarningText').hide()
                $('#divOverlayWrapper').hide()
                $('body').css({overflow: ''});
            } else {
                $(window).scrollTop(0);
                $('body').css({overflow: 'hidden'});
                $('#dbWarningText').show()
                $('#divOverlayWrapper').show()
            }
        }

        this.showHideConnectionSource = function(){
            if($('.trConnectionSource').is(':hidden')){
                $('.trConnectionSource').show();
            }
        }

        this.showHideDrConfigs = function(){
            $('.showHideDrConfig').show();
            VdmUI.showHideConnectionSource()

        }

        this.populateDrPopup = function(){
            dbData = {
               id: VdmUI.getCurrentDbCookie()
            }
            VdmService.GetDeployment(function(connection){
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                if(deploymentInfo['deployment']['dr'] != undefined && !$.isEmptyObject(deploymentInfo['deployment']['dr']))
                {
                    clusterObjects.txtDrId.val(deploymentInfo['deployment']['dr'].id);
                    clusterObjects.txtDrPort.val(deploymentInfo['deployment']['dr'].port != undefined ? deploymentInfo['deployment']['dr'].port : '')
                    if(deploymentInfo['deployment']['dr'].hasOwnProperty('connection') && deploymentInfo['deployment']['dr']['connection'] != undefined &&
                    deploymentInfo['deployment']['dr']['connection'].hasOwnProperty('source')){
                        $('.trConnectionSource').show()
                        clusterObjects.txtDatabase.val(deploymentInfo['deployment']['dr']['connection'].source)
                    } else {
                        $('.trConnectionSource').hide()
                    }
                } else {
                    $('.trConnectionSource').hide()
                    clusterObjects.txtDrId.val('');
                    clusterObjects.txtDatabase.val('');
                    clusterObjects.txtDrPort.val('');
                    clusterObjects.txtDatabase.val('');
                }
            },dbData);
        }

        this.GetDatabaseStatus = function(isStarting){
            $('#btnStopCluster').show()
            var failedOrStalled = false
            var dbData = {
                        id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                    }
            VdmService.GetDatabaseStatus(function(connection){
                if (!VdmUI.haltRequest){
                   if(VdmUI.isFirstStatusDisplay){
                       if (connection.Metadata['DATABASE_STATUS'].dbStatus != undefined){
                           if (connection.Metadata['DATABASE_STATUS'].dbStatus.isFreshStart == true){
                                $("#startOption").val("1")
                           }
                           else{
                                $("#startOption").val("2")
                           }
                       }
                   }

                    VdmUI.isFirstStatusDisplay = false;

                if(!(connection.Metadata['DATABASE_STATUS'].dbStatus.status=="stalled" && VdmUI.isRunning) && isStarting == "true")
                  localStorage.isStarting = "false"
                  if (connection.Metadata['DATABASE_STATUS'].statusString == "No Members"){
                        $(".connectionFail").show();
                        $(".containerFail").html('');
                        $(".containerFail").html('<p>No server is available. Please add a server to start.</p>');
                        $(".loadingStart").hide();
                        $(".clusterBtnWrap").hide();
                        $(".databaseRg").hide();
                        $(".containerStalled").hide();
                  }
                  else if (connection.Metadata['DATABASE_STATUS'].statusString == "error" || connection.Metadata['DATABASE_STATUS'].dbStatus.status == "unreachable"){
                        $(".connectionFail").show()
                        $(".containerFail").html('');
                        $(".containerFail").html('<p>Could not connect to the server(s). Please ensure that all servers are reachable.</p>');
                        $(".loadingStart").hide();
                        $(".clusterBtnWrap").hide();
                        $(".databaseRg").hide();
                        $(".containerStalled").hide();
                  }
                 else if (connection.Metadata['DATABASE_STATUS'].dbStatus.status=="stopped")
                    {
                        $.each(connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus, function(k, v) {
                                        if(v[Object.keys(v)[0]].hasOwnProperty('details') && v[Object.keys(v)[0]].details != "")
                                          {
                                           if((v[Object.keys(v)[0]].details.indexOf('Connection broken') < 0) && (v[Object.keys(v)[0]].details.indexOf('Connection refused') < 0) && (v[Object.keys(v)[0]].details.indexOf('@Shutdown') < 0))
                                                failedOrStalled = true
                                          }

                            });
                        if (failedOrStalled && !VdmUI.isStartServer)
                        {
                            var msg = '<p>Cluster startup did not succeed on all servers.</p>';
                            $.each(connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus, function(k, v) {
                                //display the key and value pair
                                    if(v[Object.keys(v)[0]].status=="stopped"){
                                        var ip = Object.keys(v)[0];
                                            msg = msg + '<ul class="listStalled">' +
                                        '<li class="pullback"><span class="redC"><strong>X</strong></span> '+ ip +'<span class="redC left-space"> <a href="javascript:void(0);" onClick="CheckPopupStatus(true)"  data-toggle="modal" data-target="#plusError'+ k +'" >error</a></span><div id="plusError'+ k +'" class="errorLogPopup modal fadein " role="dialog">' +
                                        '<div class="modal-dialog modal-dialog-center modal-add-user"><div class="modal-content"><div class="modal-header">' +
                                    '<h4 class="modal-title" id="errorHeader">Error Detail</h4></div><div class="modal-body"><div class="errorWrapper">' +
                                    '"'+v[Object.keys(v)[0]].details+'"</div></div>      <div class="modal-footer"><button id="" type="button" class="btnPopup btn" data-dismiss="modal">OK</button>' +
                                  '</div></div></div></div></li></ul>'
                                    }
                                    else
                                    {
                                        msg = msg + '<ul class="listStalled">' +
                                        '<li class="pullback"><span class="cross-spacer"></span>'+  Object.keys(v)[0]+'</li></ul>'
                                    }
                            });

                            VdmUI.ShowHideDatabaseStatus("failed")
                            if(VdmUI.isErrorPopupShowing==false)
                                $(".containerFailed").html(msg);

                            $('.errorLogPopup').on('hidden.bs.modal', function () {
                                  VdmUI.CheckPopupStatus(false)
                            })
                        }
                        else
                        {
                            VdmUI.ShowHideDatabaseStatus("stopped");
                        }

                    }
                 else if (connection.Metadata['DATABASE_STATUS'].dbStatus.status=="running")
                    {
                        var isFirstRunning = false;
                        $.each(connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus, function(k, v) {
                            if(!isFirstRunning)
                            {
                                if(v[Object.keys(v)[0]].status=="running"){
                                    isFirstRunning = true;
                                    VdmUI.firstRunningHostName = Object.keys(v)[0];

                                }
                            }

                        });
                        VdmUI.ShowHideDatabaseStatus("running")

                        var shutdownDialogState = localStorage.shutdownDialogState
                        if(shutdownDialogState == "true"){
                            $('#divShutdownCluster').show();
                            $('#startSuccess').addClass('databaseRg1')
                        }
                        else {
                            $('#divShutdownCluster').hide();
                            $('#startSuccess').removeClass('databaseRg1')
                        }
                    }
                 else if (connection.Metadata['DATABASE_STATUS'].dbStatus.status=="stalled")
                    {
                        if(!VdmUI.isRunning){
                            $.each(connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus, function(k, v) {
                                        if(v[Object.keys(v)[0]].hasOwnProperty('details'))
                                          {
                                            failedOrStalled = true
                                          }
                            });

                            if (failedOrStalled && !VdmUI.isStartServer){
                                var msg = '<p>Cluster startup did not succeed on all servers.</p>';
                                $.each(connection.Metadata['DATABASE_STATUS'].dbStatus.serverStatus, function(k, v) {
                                    //display the key and value pair
                                        if(v[Object.keys(v)[0]].status!="running"){
                                            var ip = Object.keys(v)[0];
                                                msg = msg + '<ul class="listStalled">' +
                                            '<li class="pullback"><span class="redC"><strong>X</strong></span> '+ ip +'<span class="redC left-space"> <a href="javascript:void(0);" onClick="CheckPopupStatus(true)" data-toggle="modal" data-target="#plusError'+ k +'" >error</a></span><div id="plusError'+ k +'" class="errorLogPopup modal fade in" role="dialog">' +
                                            '<div class="modal-dialog modal-dialog-center modal-add-user"><div class="modal-content"><div class="modal-header">' +
                                            '<h4 class="modal-title" id="errorHeader">Error Detail</h4></div><div class="modal-body"><div class="errorWrapper">' +
                                            '"'+v[Object.keys(v)[0]].details+'"</div></div>      <div class="modal-footer"><button type="button" id="btnRecoverOk" class="btnPopup btn" data-dismiss="modal">OK</button>' +
                                          '</div></div></div></div></li></ul>'
                                        }
                                        else
                                        {
                                            msg = msg + '<ul class="listStalled">' +
                                            '<li class="pullback"><span class="cross-spacer"></span>'+  Object.keys(v)[0]+'</li></ul>'
                                        }

                                });

                                if(VdmUI.isErrorPopupShowing==false)
                                    $(".containerStalled").html(msg);
                                    VdmUI.ShowHideDatabaseStatus('stalled')

                                $('.errorLogPopup').on('hidden.bs.modal', function () {
                                      VdmUI.CheckPopupStatus(false)
                                })
                            }
                        }

                    }
                }
                }, dbData);
        }

        this.ChangeDirectories = function(){

            var value = $('.defaultServ').val();
            saveCookie("selected-serverId", value);

            if (value == 0){ //Default settings
                VdmUI.displayDefaultPaths();
            }
            else{
                 var serverData = {
                                id:VdmUI.getCurrentDbCookie(),
                                server_id: VdmUI.getCurrentServerCookie()
                            }
                        VdmService.GetServer(function(connection){

                            var serverInfo = connection.Metadata['GET_SERVER']
                            VdmUI.CurrentServerInfo = serverInfo['server'];
                            $("#txtVoltdbRootDir").val(serverInfo['server']['voltdbroot'])
                            $("#txtSnapshotDir").val(serverInfo['server']['snapshots'])
                            $("#txtExportOverflowDir").val(serverInfo['server']['exportoverflow'])
                            $("#txtCommandLogDir").val(serverInfo['server']['commandlog'])
                            $("#txtCommandLogSnapDir").val(serverInfo['server']['commandlogsnapshot'])
                            $("#txtDrOverflow").val(serverInfo['server']['droverflow'])

                       },serverData)

            }
        }

        this.CheckPopupStatus = function(result){
            if(result)
                VdmUI.isErrorPopupShowing = true
            else
                VdmUI.isErrorPopupShowing = false
        }

        this.ShowHideDatabaseStatus = function(status){
            if(status == "stopped"){
                $("#startSuccess").hide();
                $(".loadingStart").hide();
                $("#startFailed").hide();
                $("#stallFailed").hide();
                $(".clusterBtnWrap").show();
                $(".connectionFail").hide();
            }
            else if (status== "running"){
                $("#startSuccess").show();
                $(".loadingStart").hide();
                $("#startFailed").hide();
                $("#stallFailed").hide();
                $(".clusterBtnWrap").hide()
                $(".connectionFail").hide();
            }
            else if (status== "stalled"){
               $("#startSuccess").hide();
                $(".loadingStart").hide();
                $("#startFailed").hide();
                $("#stallFailed").show();
                $(".clusterBtnWrap").hide();
                $(".connectionFail").hide();
            }
            else if (status== "failed"){
                $("#startSuccess").hide();
                $(".loadingStart").hide();
                $("#startFailed").show();
                $("#stallFailed").hide();
                $(".clusterBtnWrap").hide()
                $(".connectionFail").hide();
            }
        }

        this.CheckValidClusterParameters = function(){
        var sitesperhost = parseInt($("#txtSitePerHost").val())
        var ksafety = parseInt($("#txtKSafety").val())
        var hostcount = VdmUI.ServerList.length;

        var numerator = (sitesperhost * hostcount)
        var denominator = (ksafety + 1)

        if (numerator % denominator === 0){
            return true
        }
        else{
            return false;
        }
    }

        this.showHideUploadConfiguration = function(status){
            if(!status){
                $('#upload-file-btn').hide()
                $('#upload-file-btn-ok').show()
                $('.chooseFile').show()
                $('#upload-file-btn-Cancel').show()
            } else {
                $('#upload-file-btn').show()
                $('#upload-file-btn-ok').hide()
                $('.chooseFile').hide()
                $('#upload-file-btn-Cancel').hide()
            }
        }
    });
    window.VdmUI = VdmUI = new iVdmUi();
})(window);



