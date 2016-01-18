﻿﻿$(document).ready(function (e) {
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

    loadPage();
});


var editStates = {
    ShowEdit: 0,
    ShowOkCancel: 1,
    ShowLoading: 2
};

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
var editId = -1;
var addExportProperties = function () {
        var exportType = $('#txtExportType').val();
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
            if (!$('#txtMetadataBrokerList').length) {
                exportProperties += '<tr class="newStreamMinProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtMetadataBrokerList" name="txtMetadataBrokerList" value="metadata.broker.list" disabled="disabled" class="newStreamPropertyName newStreamProperty requiredProperty" type="text">' +
                    '       <label id="errorMetadataBrokerList" for="txtMetadataBrokerList" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtMetadataBrokerListValue" name="txtMetadataBrokerListValue" class="newStreamPropertyValue newStreamProperty" type="text">' +
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
        if (editId == 1)
            VoltDbAdminConfig.orgTypeValue = "";
        for (var i = 0; i < $(".newStreamMinImportProperty").length; i++) {
            if (!$($(".newStreamMinImportProperty")[i]).hasClass("orgProperty")) {
                $($(".newStreamMinImportProperty")[i]).addClass("propertyToRemove");
            }
        }
        $(".propertyToRemove").not(".addedProperty").remove();

        var importProperties = '';
        if (importType.toUpperCase() == "FILE") {
            if (!$('#txtOutdir').length) {
                exportProperties += '' +
                    '<tr class="newStreamMinImportProperty">' +
                    '   <td>' +
                    '       <input size="15" id="txtOutdir" name="txtOutdir" value="outdir" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredProperty" type="text">' +
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
                    '       <input size="15" id="txtnonce" name="txtnonce" value="nonce" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredProperty" type="text">' +
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
                    '       <input size="15" id="txtFileType" name="txtFileType" value="type" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredProperty" type="text">' +
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
                    '       <input size="15" id="txtEndpoint" name="txtEndpoint" value="endpoint" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredProperty" type="text">' +
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
                    '       <input size="15" id="txtMetadataBrokerList" name="txtMetadataBrokerList" value="metadata.broker.list" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredProperty" type="text">' +
                    '       <label id="errorMetadataBrokerList" for="txtMetadataBrokerList" class="error" style="display: none;"></label>' +
                    '   </td>' +
                    '   <td>' +
                    '       <input size="15" id="txtMetadataBrokerListValue" name="txtMetadataBrokerListValue" class="newStreamImportPropertyValue newStreamImportProperty" type="text">' +
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
                    '       <input size="15" id="txtJdbcUrl" name="txtJdbcUrl" value="jdbcurl" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredProperty" type="text">' +
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
                    '       <input size="15" id="txtJdbcDriver" name="txtJdbcDriver" value="jdbcdriver" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty requiredProperty" type="text">' +
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
                    '       <select id="selectRabbitMq" name="selectRabbitMq" class="newStreamImportPropertyName newStreamImportProperty  requiredProperty"> ' +
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
                    '       <input size="15" id="txtEndpointES" name="txtEndpointES" value="endpoint" disabled="disabled" class="newStreamImportPropertyName newStreamImportProperty  requiredProperty" type="text">' +
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
    if (!$(object).hasClass("requiredProperty")) {
        var val = $(':input:eq(' + ($(':input').index(object) + 1) + ')').val();
        if ($(VdmUI.newStreamMinmPropertyName[propertyName]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName]).val(val);
            $(".newStreamMinImportProperty").addClass("addedProperty");
            var $row = $(object).closest("tr");
            $row.remove();
        } else if ($(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).length) {
            $(VdmUI.newStreamMinmPropertyName[propertyName + '_' + exportType]).val(val);
            $(".newStreamMinImportProperty").addClass("addedProperty");
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
        $("#trImportConnectorClass").show();
    } else {
        $("#trImportConnectorClass").hide();
    }
}
var orgUser = '';
var loadPage = function() {
    VdmUI.checkConditionsForDr()
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

    var validationRules = {
        ServerNameRule: {
            required: true,
            checkDuplicateServer: [],
            regex: /^[a-zA-Z0-9_.]+$/
        },
        ServerNameMessage: {
            required: "This field is required",
            checkDuplicateServer: 'This server name already exists.',
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        HostNameRule:{
            required: true,
            checkDuplicateHost: [],
            regex: /^[a-zA-Z0-9_.]+$/
        },
        HostNameMessage: {
            required: "This field is required",
            checkDuplicateHost:'This host name already exists.',
            regex: 'Only alphabets, numbers, _ and . are allowed.'
        },
        PortRule:{
            portRegex : /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        },
        PortMessage:{
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
        MemoryLimitRules: {
            required: true,
            min: 1,
            max: 99,
            digits: true
            //regex: /[^0-9\.]/ // "^[0-9]+(\.[0-9]{0,4})?$"
        },
        MemoryLimitMessages: {
            required: "This field is required",
            min: "Please enter a positive number.",
            max: "Please enter a positive number less than 100.",
            digits: "Please enter a positive number without any decimal." //"Only four digits are allowed after decimal."
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
            min: "Please enter a number greater than 3",
            max: "Please enter a positive number between 3 and 3000.",
            digits: "Please enter a positive number without any decimal."
        },
         MaxJavaHeapRules:{
         required: true,
            min: 1,
            max: 32,
            digits: true
        },
        MaxJavaHeapMessages:{
            required: "This field is required",
            min: "Please enter a number greater than 1",
            max: "Please enter a positive number between 1 and 32.",
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
            max: "Please enter a positive number between 1 and 32.",
            digits: "Please enter a positive number without any decimal."
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
        txtMaxJavaHeap: $('#txtMaxJavaHeap'),
        txtHeartbeatTimeout: $('#txtHeartbeatTimeout'),
        txtQueryTimeout: $('#txtQueryTimeout'),
        txtMaxTempTableMemory: $('#txtMaxTempTableMemory'),
        txtSnapshotPriority: $('#txtSnapshotPriority'),
        txtMemoryLimit: $('#txtMemoryLimit'),

        txtMaxJavaHeap: $('#txtMaxJavaHeap'),
        //: $('#'),

        //DR
        chkDrOnOff: $('#chkDrOnOff'),
        chkDrOnOffValue: $('#chkDrOnOffValue'),
        selectDrType: $('#selectDrType'),
        txtDrId: $('#txtDrId'),
        lblDrId: $('#lblDrId'),
        lblDrType: $('#lblDrType'),
        chkDrOnOffStatus: $('#chkDrOnOffStatus'),
        chkDrOnOffVal: $('#chkDrOnOffVal')
    }

    $.validator.addMethod(
        "checkDuplicateServer",
        function (value) {
            var arr = VdmUI.CurrentServerList;
            if (VdmUI.isServerCreate == false) {
                if ($.inArray(value, arr) != -1) {
                    if (value == VdmUI.serverToUpdate)
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

    VdmService.GetServerList(function(connection){
        VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
    })

    VdmService.GetDatabaseList(function(connection){
        VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
    });

    setInterval(function () {
        VdmService.GetServerList(function(connection){
            VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
        })
    }, 5000);

 var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }
    setInterval(function () {
        VdmService.GetDatabaseList(function(connection){
            VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
        });
    }, 5000);

    VdmService.GetDeployment(function(connection){
        VdmUI.Deployment = connection.Metadata['DEPLOYMENT'];
        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
    },dbData);


    setInterval(function () {
        var dbData = {
                    id: VdmUI.getCurrentDbCookie() == -1 ? 1 : VdmUI.getCurrentDbCookie()
                }
        VdmService.GetDeployment(function(connection){
                 VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },dbData);
    }, 5000);

    $("#frmCreateServer").validate({
        rules: {
            serverName: validationRules.ServerNameRule,
            txtHostName: validationRules.HostNameRule,
            txtClientPort:validationRules.PortRule,
            txtAdminPort:validationRules.PortRule,
            txtZookeeper:validationRules.PortRule,
            txtReplicationPort:validationRules.PortRule,
            txtInternalInterface:validationRules.IpRule,
            txtExternalInterface:validationRules.IpRule,
            txtPublicInterface:validationRules.IpRule,
            txtInternalPort:validationRules.PortRule,
            txtHttpPort:validationRules.PortRule,
        },
        messages: {
            serverName: validationRules.ServerNameMessage,
            txtHostName: validationRules.HostNameMessage,
            txtClientPort:validationRules.PortMessage,
            txtAdminPort:validationRules.PortMessage,
            txtZookeeper:validationRules.PortMessage,
            txtReplicationPort:validationRules.PortMessage,
            txtInternalInterface:validationRules.IpMessage,
            txtExternalInterface:validationRules.IpMessage,
            txtPublicInterface:validationRules.IpMessage,
            txtInternalPort:validationRules.PortMessage,
            txtHttpPort:validationRules.PortMessage,
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

     $("#frmMaxJavaHeap").validate({
        rules:{
        txtMaxJavaHeap: validationRules.MaxJavaHeapRules
        },
        messages:{
        txtMaxJavaHeap: validationRules.MaxJavaHeapMessages
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
                    txtPassword: validationRules.PasswordRule
                },
                messages: {
                    txtUser: validationRules.UserNameMessage,
                    txtPassword: validationRules.PasswordMessage
                }
            });

    $('#frmDatabaseReplication').validate({
        rules: {
            drServers: validationRules.checkBoxRules,
            txtDrId : validationRules.NumericRules
        },
        messages: {
            drServers: validationRules.checkBoxMessages,
            txtDrId : validationRules.NumericMessages
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
            return;
        }

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
            id:(VdmUI.isServerCreate?VdmUI.getCurrentDbCookie():$('#addServer').data('serverid'))
        }
        var hostId = $('#addServer').data('serverid')
        if(VdmUI.isServerCreate){
            VdmService.CreateServer(function(connection){
                if(connection.Metadata['SERVER_CREATE'].status == 1){
                    VdmService.GetServerList(function(connection){
                        VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
                    })
                } else{
                    $('#errorMsg').html('Unable to create server.')
                    $('#errorDialog').modal('show');
                }
            },serverInfo);
        } else {
            toggleServer(editStates.ShowLoading,hostId)
            VdmService.UpdateServer(function(connection){
                if(connection.Metadata['SERVER_UPDATE'].status == 1){
                    VdmService.GetServerList(function(connection){
                        VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
                    })
                } else{
                    $('#errorMsg').html('Unable to update server.')
                    $('#errorDialog').modal('show');
                    toggleServer(editStates.ShowEdit,hostId);
                }
            },serverInfo);

        }
        VdmUI.resetTextBox();
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
        var deployment = $('#txtDeployment').val();

        if(deployment=="")
        {
            deployment = "default";
        }

        var dbInfo = {
            data:{
                name: dbName,
                deployment: deployment
            },
            id:$('#addDatabase').data('id')
        }
        if(VdmUI.isDbCreate){
            VdmService.CreateDatabase(function(connection){
                if(connection.Metadata['DATABASE_CREATE'].status == 1){
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
                if(connection.Metadata['DATABASE_UPDATE'].status == 1){
                    VdmService.GetDatabaseList(function(connection){
                        VdmUI.displayDatabases(connection.Metadata['DATABASE_LISTING'])
                    })
                } else {
                    toggleDatabase(editStates.ShowEdit, $('#addDatabase').data('id'))
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
        var serverId = $('#deleteConfirmation').data('serverid');
        var serverData = {
           data:{
               dbId: VdmUI.getCurrentDbCookie()
           },
           "id": serverId
        }
        toggleServer(editStates.ShowLoading,serverId)

        VdmService.DeleteServer(function(connection){
            if(connection.Metadata['SERVER_DELETE'].hasOwnProperty('success')){
                $('#successMsg').html(connection.Metadata['SERVER_DELETE']['success'])
                $('#successDialog').modal('show');
                toggleServer(editStates.ShowEdit,hostId);
            } else if(connection.Metadata['SERVER_DELETE'].hasOwnProperty('error')){
                $('#errorMsg').html(connection.Metadata['SERVER_DELETE']['error'])
                $('#errorDialog').modal('show');
                toggleServer(editStates.ShowEdit,hostId);
            }
            VdmService.GetServerList(function(connection){
                VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
            })
        }, serverData);
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
                $('#errorMsg').html('Unable to add existing servers.')
                $('#errorDialog').modal('show');
            }
        }, dbData);
    });

    $('#btnAddExistingServer').on('click', function(){
        VdmUI.getNonMemberServerList();
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
            VdmService.GetServerList(function(connection){
                VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
            })
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

    $("#txtMaxJavaHeap").on('change',function(e){
        UpdateTextBox(this,'maxJavaHeap','frmMaxJavaHeap',e);
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
        UpdateTextBox(this,'memoryLimit','frmMemoryLimit',e);
    })

    $("#txtVoltdbRootDir").on('change',function(e){
        UpdateTextBox(this,'voltdbrootDir','frmVoltdbRootDir',e);
    })

    $("#txtSnapshotDir").on('change',function(e){
        UpdateTextBox(this,'snapshotDir','frmSnapshotDir',e);
    })

     $("#txtExportOverflowDir").on('change',function(e){
        UpdateTextBox(this,'exportOverflowDir','frmExportOverflowDir',e);
    })

    $("#txtCommandLogDir").on('change',function(e){
        UpdateTextBox(this,'commandlogDir','frmCommandLogDir',e);
    })

    $("#txtCommandLogSnapDir").on('change',function(e){
        UpdateTextBox(this,'commandlogSnapDir','frmCommandLogSnapDir',e);
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
                        partitionDetection:{
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
             else if (item == "dr"){
                deploymentInfo = {
                    data: {
                            dr:{
                                enabled: clusterObjects.chkDrOnOffValue.text() == "Off" ? false : true
                               }
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }

             VdmService.SaveDeployment(function(connection){
                    VdmService.GetDeployment(function(connection){
                        VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                    },deploymentInfo)
                }, deploymentInfo);

                ShowSavedStatus();
    }

    UpdateTextBox = function(element,item, form, e){
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
              else if (item == "maxJavaHeap")
              {
               deploymentInfo = {
                        data:{
                           heap:{
                    maxjavaheap: parseInt(clusterObjects.txtMaxJavaHeap.val())
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
               deploymentInfo = {
                        data:{
                         systemsettings:{
                    resourcemonitor:{
                        memorylimit:{
                            size: clusterObjects.txtMemoryLimit.val()
                        }
                    }
                }

                        },
                        id: VdmUI.getCurrentDbCookie()
                    }
              }
               else if (item == "voltdbrootDir"){
                deploymentInfo = {
                    data:{
                       paths:{
                    voltdbroot: {path:clusterDirectoryObjects.txtVoltdbRootDir.val()}
                }
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
               else if (item == "snapshotDir"){
                deploymentInfo = {
                    data:{
                       paths:{
                    snapshots: {path:clusterDirectoryObjects.txtSnapshotDir.val()}
                }
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
             else if (item == "exportOverflowDir"){
                deploymentInfo = {
                    data:{
                       paths:{
                    exportoverflow: {path:clusterDirectoryObjects.txtExportOverflowDir.val()}
                }
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
              else if (item == "commandlogDir"){
                deploymentInfo = {
                    data:{
                       paths:{
                    commandlog: {path:clusterDirectoryObjects.txtCommandLogDir.val()},
                }
                    },
                    id: VdmUI.getCurrentDbCookie()
                }
             }
              else if (item == "commandlogSnapDir"){
                deploymentInfo = {
                    data:{
                       paths:{
                            commandlogsnapshot:{path: clusterDirectoryObjects.txtCommandLogSnapDir.val()},
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
                    },deploymentInfo)
                }, deploymentInfo);

               }
                ShowSavedStatus()
    }

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
        VdmUI.exportEditId = -1;
        VdmUI.openExportPopup();
        VdmUI.afterOpenExportPopup();
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
                "name": encodeURIComponent($(newStreamProperties[i]).val()),
                "value": encodeURIComponent($(newStreamProperties[i + 1]).val()),
            });
        }
        newConfig["stream"] = $("#txtExportStream").val();
        newConfig["type"] = $("#txtExportType").val().trim();
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

        ShowSavingStatus();

        var username = $('#txtOrgUser').val();
        var newUsername = $('#txtUser').val();
        var password = encodeURIComponent($('#txtPassword').val());
        var role = $('#selectRole').val();
        var requestType = "POST";
        var requestUser = "";
        var database_id = 0;


        var deploymentUserInfo = {
                         data:{},
                         id: VdmUI.getCurrentDbCookie()
                        }
        if (VdmUI.userEditId == 1) {

                        requestUser = username;

                    } else {
                        requestUser = newUsername;
                        requestType = "PUT";
                    }



        deploymentUserInfo['data']['name'] = newUsername;
        deploymentUserInfo['data']['password'] = password;
        deploymentUserInfo['data']['roles'] = role;
        deploymentUserInfo['data']['plaintext'] = true
        deploymentUserInfo['data']['databaseid'] = VdmUI.getCurrentDbCookie()

         VdmService.SaveDeploymentUser(function(connection){
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },deploymentUserInfo)
            }, deploymentUserInfo,requestType, requestUser);


         ShowSavedStatus();


    })

    $("#deleteUser").on('click',function(e){
        var username = $('#txtOrgUser').val();
        var requestUser = username;

         var deploymentUserInfo = {
         data:{},
         id: VdmUI.getCurrentDbCookie()
        }

            deploymentUserInfo['data']['name'] = $('#txtUser').val()
            deploymentUserInfo['data']['password'] = encodeURIComponent($('#txtPassword').val())
            deploymentUserInfo['data']['roles'] = $("#selectRole").val()
            deploymentUserInfo['data']['plaintext'] = true


         VdmService.SaveDeploymentUser(function(connection){
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
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
                type: VdmUI.exportPropertyList[i].type,
                exportconnectorclass: VdmUI.exportPropertyList[i].exportConnectorClass==undefined?"":VdmUI.exportPropertyList[i].exportConnectorClass,
                property: []
            });

            deploymentInfo['data']['export']['configuration'][i]['property'] = [];

            for(var j=0;j<=VdmUI.exportPropertyList[i].property.length -1;j++)
            {
                deploymentInfo['data']['export']['configuration'][i]['property'].push({
                    name: VdmUI.exportPropertyList[i].property[j].name,
                    value: VdmUI.exportPropertyList[i].property[j].value,
                });
            }
        }

         VdmService.SaveDeployment(function(connection){
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
                module: VdmUI.importPropertyList[i].module,
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
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },deploymentInfo)
            }, deploymentInfo);

             ShowSavedStatus();
    }

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
                "name": encodeURIComponent($(newStreamProperties[i]).val()),
                "value": encodeURIComponent($(newStreamProperties[i + 1]).val()),
            });
        }
        newConfig["module"] = $("#txtImportStream").val();
        newConfig["type"] = $("#txtImportType").val().trim();
        newConfig["enabled"] = $("#chkImportStream").is(':checked');
        if ($("#txtImportType").val().trim().toUpperCase() == "CUSTOM") {
            newConfig["importconnectorclass"] = $("#txtImportConnectorClass").val();
        } else {
            newConfig["importconnectorclass"] = '';
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
            importObject.importconnectorclass = newConfig.importconnectorclass
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

    VdmUI.showHideDrProperties();

    $('#chkDrOnOff').on('ifChanged', function () {
        VdmUI.showHideDrProperties();
    });

    $('#selectDrType').change(function () {
        $('#drServers-error').hide();
        VdmUI.showHideConnectionSource()
    });

    $('#btnUpdateSource').on('click',function(){
        $('#drServers-error').hide()
        $('#errorDrId').hide()
        VdmUI.showHideConnectionSource()
        VdmUI.populateDrPopup();
    });

    $('#chkDrOnOffStatus').iCheck({
            checkboxClass: 'icheckbox_square-aero customCheckbox',
            increaseArea: '20%'
        });

    $('#chkDrOnOffStatus').on('ifChanged', function () {
        VdmUI.showHideDrConfigs();
    });



    $('#selectDrDatabase').change(function () {
        $('#drServers-error').hide();
        VdmUI.getServersForDb();
    });


    $('#btnSaveReplication').on('click', function(e){
        if($("select[id='tdDbSelect']").length < 1 && clusterObjects.selectDrType.val().toLowerCase() != 'master'){
            $('#drServers-error').show();
            $('#drServers-error').html('No database Available.')
            e.preventDefault();
            e.stopPropagation();
            return;
        }

        if(!$('#frmDatabaseReplication').valid()){
            e.preventDefault();
            e.stopPropagation();
            return;
        }
        if($("input[name='drServers']").length < 1 && clusterObjects.selectDrType.val().toLowerCase() != 'master'){
            $('#drServers-error').show();
            $('#drServers-error').html('No servers Available.')
            e.preventDefault();
            e.stopPropagation();
            return;
        }

//        var result = VdmUI.checkConditionForDbChange()
//        if(!result){
//            e.preventDefault();
//            e.stopPropagation();
//            return;
//        }


        ///////////////////////////////////
        var result = true;
        var selectedDb = $('#tdDbSelect').val()
        dbData = {
           id: selectedDb
        }
        VdmService.GetDeployment(function(connection){

            var deploymentInfo = connection.Metadata['DEPLOYMENT'];
            if(clusterObjects.selectDrType.val() == 'Replica'){
                if(deploymentInfo['deployment'].dr != null){
                    var deploymentStatus = deploymentInfo['deployment'].dr.type != undefined ? deploymentInfo['deployment'].dr.type : '';
                    //dr type must be master
                    if(deploymentStatus.toLowerCase() != 'master'){
                        setTimeout(
                          function()
                          {
                            $('#connectionSource').modal('show');
                          }, 500);
                        result = false;
                        $('#drServers-error').show();
                        $('#drServers-error').html('The selected database must be of type Master.')
                        e.preventDefault();
                        e.stopPropagation();
                        return;
                    } else {
                        SaveDrInfo();
                    }
                } else {
                    //display alert message that dr is not set
                    setTimeout(
                          function()
                          {
                            $('#connectionSource').modal('show');
                          }, 500);
                    result = false;
                    $('#drServers-error').show();
                    $('#drServers-error').html('The selected database must have database enabled.')
                    e.preventDefault();
                    e.stopPropagation();
                    return;
                }
            } else if (clusterObjects.selectDrType.val() == 'XDCR'){
                if(deploymentInfo['deployment'].dr != null || deploymentInfo['deployment'].dr.enabled == false){
                    //allow creating
                    if(deploymentInfo['deployment'].dr.connection != undefined &&
                    deploymentInfo['deployment'].dr.connection.source !=  VdmUI.CurrentDbName){
                        //allow creating
                        setTimeout(
                          function()
                          {
                            $('#connectionSource').modal('show');
                          }, 500);
                        result = false;
                        $('#drServers-error').show();
                        if(deploymentInfo['deployment'].dr.type.toLowerCase() == 'xdcr')
                            $('#drServers-error').html('The selected database is configured to use XDCR with another database. Please use another database.')
                        else if(deploymentInfo['deployment'].dr.type.toLowerCase() == 'master' ||
                            deploymentInfo['deployment'].dr.type.toLowerCase() == 'replica'){
                            $('#drServers-error').html('The selected database should be of DR type "XDCR".')
                            }
                        e.preventDefault();
                        e.stopPropagation();
                        return;
                    } else {
                        SaveDrInfo()
                    }
                } else {
                    SaveDrInfo()
                }
            }
            else{
                SaveDrInfo()
            }

        }, dbData)
        /////////////////////////////////////

    });

    var SaveDrInfo = function(){
        ShowSavingStatus();
        var masterCluster = $("#selectDrDatabase option:selected").text();
        var servers = []
        $('input[name="drServers"]:checked').each(function() {
           servers.push(parseInt($(this).val()))
        });
        var drEnabled = clusterObjects.chkDrOnOffValue.text()=="Off"?false:true
        if(drEnabled){
            deploymentInfo = {
                                data: {
                                    dr: {
                                        enabled: drEnabled,
                                        id: parseInt(clusterObjects.txtDrId.val()),
                                        type: clusterObjects.selectDrType.val(),
                                        connection: {
                                            source: masterCluster,
                                            servers: servers
                                        },

                                    }
                                },
                                id: VdmUI.getCurrentDbCookie()
                             }
        } else {
            deploymentInfo = {
                                data: {
                                    dr: {

                                    }
                                },
                                id: VdmUI.getCurrentDbCookie()
                             }
        }
        VdmService.SaveDeployment(function(connection){
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
        this.getCookie = function(name) {
            return $.cookie(name + "_" + VdmConfig.GetPortId());
        };

        this.Deployment = [];
        this.exportPropertyList = [];
        this.userSecurityList = [];
        this.importPropertyList = [];
        this.newStreamMinmPropertyName = {
            "outdir": "#txtOutdirValue",
            "nonce": "#txtnonceValue",
            "type": "#txtFileTypeValue",
            "endpoint_HTTP": "#txtEndpointValue",
            "metadata.broker.list": "#txtMetadataBrokerListValue",
            "jdbcurl": "#txtJdbcUrlValue",
            "jdbcdriver": "#txtJdbcDriverValue",
            "broker.host": "#txtRabbitMqValue",
            "amqp.uri": "#txtRabbitMqValue",
            "endpoint_ELASTICSEARCH": "#txtEndpointESValue"
        };

        this.displayServers = function(serverList){
            if (serverList == undefined) {
                return;
            }
            var dbId = VdmUI.getCurrentDbCookie();
            VdmService.GetDatabaseList(function(connection){
                var dbIdList = []
                connection.Metadata['DATABASE_LISTING'].databases.forEach(function (info) {
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
                    var dbData = {
                        id: dbId
                    }

                    VdmService.GetMemberList(function(connection){
                        VdmUI.members = connection['Metadata']['MEMBER_LISTING'].members;
                        var htmlList = "";
                        VdmUI.CurrentServerList = [];
                        VdmUI.CurrentHostList = [];
                        serverList.servers.forEach(function (info) {
                            var hostName = info["hostname"];
                            var serverName = info["name"]
                            var hostId = info["id"];
                            var infos = JSON.stringify(info)
                            if($.inArray(hostId, VdmUI.members) != -1){
                                htmlList += '<tr>' +
                                            '<td id="tdHostname_' +hostId+ '">' + hostName + '</td>' +
                                            '<td data-id="' + hostId + '" data-info=\''+ infos +'\'><a class="btnUpdateServer" href="javascript:void(0);"data-toggle="modal" data-target="#addServer" >' +
                                            '<div class="editServer" id="editServer_'+hostId+'"></div></a>' +
                                            '<div class="loading-small" id="loadingServer_'+hostId+'" style="display:none"></div>' +
                                            '<span class="editServerTxt" id="editServerTxt_'+hostId+'">Edit</span></td>' +
                                            '<td data-id="' + hostId + '" ><a class="btnDeleteServer" href="javascript:void(0);"data-toggle="modal" data-target="#deleteConfirmation" >' +
                                            '<div class="deleteServer" id="deleteServer_'+hostId+'"></div></a>' +
                                            '<span class="deleteServerTxt" id="deleteServerTxt_'+hostId+'">Delete</span></td>' +
                                        '</tr>';
                            }
                            VdmUI.CurrentServerList.push(serverName);
                            VdmUI.CurrentHostList.push(hostName)
                        });
                        if(htmlList == ""){
                            $('#serverList').html('<tr><td style="top:-1px !important">No servers available.</td><td></td></tr>')
                        }else{
                            $('#serverList').html(htmlList)
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

                        $('#btnAddServer').on('click', function(){
                            VdmUI.isServerCreate = true;
                            VdmUI.serverToUpdate = '';
                            VdmUI.hostToUpdate = '';
                            $('#addServerTitle').html('Add Server');
                            VdmUI.resetTextBox();
                        });
                    }, dbData);
                } else {
                    $('#serverList').html('<tr><td style="top:-1px !important">No server available.</td><td></td></tr>')
                    VdmUI.showHideOverlay(false);
                }
            });
        };

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
                $('#txtDeployment').val('');
                $('#errorDbName').hide();
            };

        this.displayDatabases = function(databaseList){
            if(databaseList == undefined)
                return;
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
                                '<td data-id="' + dbId + '"><a id="dbInfo_' + dbId + '" class="btnDbList selected" href="javascript:void(0)">' + dbName + '</a></td>' +
                                '<td data-id="' + dbId + '" data-info=\''+ dbInfo +'\' width="2%">' +
                                '<a id="btnUpdataDb_' + dbId + '" class="btnUpdateDatabase" href="javascript:void(0);"data-toggle="modal" data-target="#addDatabase" >' +
                                '<div id="editDatabase_'+ dbId +'" class="editServerDatabase"></div></a>' +
                                '<div class="loading-small-DB" id="loadingDatabase_'+ dbId +'" style="display:none"></div>' +
                                '<span class="editServerDatabaseTxt" id="editDatabaseTxt_'+ dbId +'">Edit</span></td>' +
                                '<td data-id="' + dbId + '" width="2%">' +
                                '<a id="btnDeleteDb_' + dbId + '" class="btnDeleteDatabase" href="javascript:void(0);"data-toggle="modal" data-target="#deleteDatabase" >' +
                                '<div class="deleteDisableDS" id="deleteDatabase_'+ dbId +'"></div>' +
                                '</a> <span class="deleteDisableDSTxt" id="deleteDatabaseTxt_'+ dbId +'">Delete</span></td>' +
                                '</tr>';
                                VdmUI.CurrentDbName = dbName;
                } else {
                    htmlList += '<tr>' +

                                '<td data-id="' + dbId + '"><a id="dbInfo_' + dbId + '" class="btnDbList" href="javascript:void(0)">' + dbName + '</a></td>' +
                                '<td data-id="' + dbId + '" data-info=\''+ dbInfo +'\' width="2%">' +
                                '<a id="btnUpdataDb_' + dbId + '" class="btnUpdateDatabase" href="javascript:void(0);"data-toggle="modal" data-target="#addDatabase" >' +
                                '<div id="editDatabase_'+ dbId +'" class="editServerDatabase"></div></a>' +
                                '<div class="loading-small-DB" id="loadingDatabase_'+ dbId +'" style="display:none"></div>' +
                                '<span class="editServerDatabaseTxt" id="editDatabaseTxt_'+ dbId +'">Edit</span></td>' +
                                '<td data-id="' + dbId + '" width="2%">' +
                                '<a id="btnDeleteDb_' + dbId + '" class="btnDeleteDatabase" href="javascript:void(0);"data-toggle="modal" data-target="#deleteDatabase" >' +
                                '<div class="deleteDatabaseServer" id="deleteDatabase_'+ dbId +'"></div>' +
                                '</a> <span class="deleteServerDatabaseTxt" id="deleteDatabaseTxt_'+ dbId +'">Delete</span></td>' +
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
                $('#txtDeployment').val(dbInfo['deployment']);
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
                    VdmService.GetServerList(function(connection){
                        VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
                        VdmUI.showHideLoader(false);
                    })
                })
                 var dbData = {
                    id: VdmUI.getCurrentDbCookie()
                }
                VdmService.GetDeployment(function(connection){
                    VdmUI.displayDeployment(connection.Metadata['DEPLOYMENT'])
                },dbData);

                $('#createDatabase').modal('hide');
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

        this.getNonMemberServerList = function(){
            VdmService.GetServerList(function(connection){
                //VdmUI.displayServers(connection.Metadata['SERVER_LISTING'])
                var htmlList = '';
                connection.Metadata['SERVER_LISTING'].servers.forEach(function (info) {
                    var serverId = info['id'];
                    var hostName = info['hostname'];
                    if($.inArray(serverId, VdmUI.members) == -1){
                        htmlList += '<option value='+ parseInt(serverId) +'>' + hostName + '</option>';
                    }
                });
                $('#selectServers').html(htmlList);
                $('#selectServers').multiSelect( 'refresh' );
            })
        };

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
            clusterObjects.txtMemoryLimit.val(deployment['deployment']['systemsettings']['resourcemonitor']['memorylimit']['size'])
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
            if(!$('#txtVoltdbRootDir').is(":focus")){
            clusterDirectoryObjects.txtVoltdbRootDir.val(deployment['deployment']['paths']['voltdbroot']['path'])
            }
            if(!$('#txtSnapshotDir').is(":focus")){
            clusterDirectoryObjects.txtSnapshotDir.val(deployment['deployment']['paths']['snapshots']['path'])
            }
            if(!$('#txtExportOverflowDir').is(":focus")){
            clusterDirectoryObjects.txtExportOverflowDir.val(deployment['deployment']['paths']['exportoverflow']['path'])
            }

            if(!$('#txtCommandLogDir').is(":focus")){
            clusterDirectoryObjects.txtCommandLogDir.val(deployment['deployment']['paths']['commandlog']['path'])
            }
            if(!$('#txtCommandLogSnapDir').is(":focus")){
            clusterDirectoryObjects.txtCommandLogSnapDir.val(deployment['deployment']['paths']['commandlogsnapshot']['path'])
            }
            if(!$('#txtPartitionDetection').is(":focus")){
            clusterObjects.txtPartitionDetection.text(getOnOffText(deployment['deployment']['partitionDetection']['enabled']));
            }
            if(!$('#txtMaxJavaHeap').is(":focus")){
            clusterObjects.txtMaxJavaHeap.val(deployment['deployment']['heap']['maxjavaheap'])
            }

            if(deployment['deployment']['partitionDetection']['enabled']==true)
            {
                clusterObjects.chkPartitionDetection.iCheck('check')
            }
            else
            {
                clusterObjects.chkPartitionDetection.iCheck('uncheck')
            }



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
            VdmUI.exportPropertyList = []
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

            VdmUI.userSecurityList = [];
            if(deployment['deployment']['users']!=null)
            {
                for(var j=0; j< deployment['deployment']['users']['user'].length;j++)
                {
                VdmUI.userSecurityList.push({
                    id : j+1,
                    name: deployment['deployment']['users']['user'][j].name,
                    roles: deployment['deployment']['users']['user'][j].roles,
                    plaintext: deployment['deployment']['users']['user'][j].plaintext
                })
                }
            }

            VdmUI.displaySecurity()

            VdmUI.importPropertyList = [];
            //Import property
            VdmUI.importPropertyList = []
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

            //DR Configuration
            if(deployment['deployment']['dr'] != null){
                if(deployment['deployment']['dr'].enabled == true){
                    //clusterObjects.chkDrOnOff.iCheck('check');
                    $('#onOffIcon').removeClass('offIcon')
                    $('#onOffIcon').addClass('onIcon')
                    clusterObjects.chkDrOnOffVal.text('On')
                } else {
                    //clusterObjects.chkDrOnOff.iCheck('uncheck');
                    $('#onOffIcon').removeClass('onIcon')
                    $('#onOffIcon').addClass('offIcon')
                    clusterObjects.chkDrOnOffVal.text('Off')
                }
                clusterObjects.lblDrId.text(deployment['deployment']['dr'].id);

                clusterObjects.lblDrType.text(deployment['deployment']['dr'].type);

                var htmlConSource = '';
                var htmlServers = '';
                var htmlServerList = '';
                if(deployment['deployment']['dr'].connection != null && ! $.isEmptyObject(deployment['deployment']['dr'].connection)){
                    VdmUI.ConnectionSourceStatus = true;
                    var source = deployment['deployment']['dr']['connection'].source;
                    VdmUI.masterCluster = source;
                    //$('#row-master-cluster').show();
                    $('#master-cluster-name').html(source);
                    //$('#row-server').show();
                    var servers = deployment['deployment']['dr']['connection'].servers;
                    VdmService.GetServerList(function(connection){
                        serverList = connection.Metadata['SERVER_LISTING'];
                        serverList.servers.forEach(function (info) {
                            var hostName = info["hostname"];
                            var serverName = info["name"]
                            var hostId = info["id"];
                            if($.inArray(hostId, servers) != -1){
                                htmlServerList += '<tr class="child-row-server subLabelRowSub clsServerList drTrToRemove" style="display:none">' +
                                                '<td class="configLabel" colspan="3">'+ hostName +'</td><td>&nbsp;</td></tr>' ;
                            }
                        });
                        $('.drTrToRemove').remove();
                        $('#row-server').after(htmlServerList)

                        VdmUI.showHideDrProperties()
                    });
                } else {
                    VdmUI.ConnectionSourceStatus = false;
                    htmlServerList = '<tr class="rowDr subLabelRow clsDrEnabled drTrToRemove">' +
                                    '<td colspan="2" class="configLabel label-master-cluster">No source available.</td>' +
                                    '<td>&nbsp;</td>' +
                                    '<td>&nbsp;</td>' +
                                    '</tr>';

                    $('.drTrToRemove').remove();
                    $('#row-server').after(htmlServerList)
                    VdmUI.showHideDrProperties()
                    $('#row-master-cluster').hide();
                    $('#row-server').hide()
                }
            } else {
                //$('#divDrWrapperAdmin').hide()
                deploymentInfo = {
                    data: {
                            dr:{
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

            $('#chkSecurity').on('ifClicked', function(event){
                UpdateCheckBox(clusterObjects.txtSecurity,'security');
            });



        };

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
                var type = VdmUI.exportPropertyList[i].type;
                var property = VdmUI.exportPropertyList[i].property;
                var enabled = VdmUI.exportPropertyList[i].enabled;

                exportList += '<tr data-property=\''+ JSON.stringify(VdmUI.exportPropertyList[i]) +'\' class="child-row-4 subLabelRow parentprop exportPropertyClass" id="row-41-' + i + ' " style="display:'+ showHide +'">' +
                       '<td class="configLabel"><a href="javascript:void(0)" class="glyphicon-triangle-right">' +
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
                if(property.length > 0){
                    for(var j = 0; j < property.length; j++){
                       var propertyName = property[j].name;
                       var propertyValue = property[j].value;
                       exportList += '<tr class="childprop-row-41-' + i + ' subLabelRow exportPropertyClass" style="display:'+showHide+'">' +
                       '<td class="configLabel-sub">' + propertyName + '</td>' +
                       '<td align="right" >' + propertyValue + '</td>' +
                       '<td>&nbsp;</td>' +
                       '<td>&nbsp;</td>' +
                       '</tr>';
                    }
                 }else{
                    exportList += '<tr class="childprop-row-41-' + i + ' subLabelRow exportPropertyClass" style="display:'+showHide+'">' +
                        '<td class="configLabel-sub">No property available.</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                       '</tr>';
                 }
            }
            childRowClass = $('.child-row-4').find('td:first-child > a').attr('class')
            $('.exportPropertyClass').remove()
            if(exportList == ''){
                exportList = '<tr class="child-row-4 subLabelRow exportPropertyClass parentprop" id="trExport"  style="display:'+ showHide +'">' +
                    '<td class="configLabel">No configuration available.</td>' +
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

            $('.child-row-4').find('td:first-child > a').attr('class',childRowClass)

            if (!$('.child-row-4').find('td:first-child > a').hasClass('glyphicon-triangle-bottom'))
            {
                $('tr[class^=childprop-row-41]').hide().children('td');
            }

            $('.btnUpdateExport').on('click', function(){
                var exportInfo = $(this.parentElement.parentElement).data('property');
                $('#plusExport').data('id', exportInfo.id);
                VdmUI.exportEditId = exportInfo.id;
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
                var type = VdmUI.importPropertyList[i].type;
                var property = VdmUI.importPropertyList[i].property;
                var enabled = VdmUI.importPropertyList[i].enabled;

                importList += '<tr data-property=\''+ JSON.stringify(VdmUI.importPropertyList[i]) +'\' class="child-row-7 subLabelRow importParentProp  importPropertyClass" id="row-71-' + i + ' " style="display:'+ showHide +'">' +
                       '<td class="configLabel"><a href="javascript:void(0)" class="glyphicon-triangle-right">' +
                       '<span class="fontFamily">' + name + '(' + type + ')' + '</span></a></td>';
                       if(enabled){
                           importList += '<td align="right"><div class="onIcon"></div></td>' +
                                        '<td class="onOff" id="txtImportOnOff">On</td>';
                       } else{
                           importList +=  '<td align="right"><div class="offIcon"></div></td>' +
                                        '<td class="onOff" id="txtImportOnOff">Off</td>' ;
                       }

                       importList += '<td><a class="btnUpdateImport editIcon" href="javascript:void(0);"data-toggle="modal" data-target="#plusImport"></a></td>' +
                                    '</tr>';
                if(property.length > 0){
                    for(var j = 0; j < property.length; j++){
                       var propertyName = property[j].name;
                       var propertyValue = property[j].value;
                       importList += '<tr class="imp-childprop-row-71-' + i + ' subLabelRow importPropertyClass" style="display:'+showHide+'">' +
                       '<td class="configLabel-sub">' + propertyName + '</td>' +
                       '<td align="right" >' + propertyValue + '</td>' +
                       '<td>&nbsp;</td>' +
                       '<td>&nbsp;</td>' +
                       '</tr>';
                    }
                 } else {
                    importList += '<tr class="imp-childprop-row-71-' + i + ' subLabelRow importPropertyClass" style="display:'+showHide+'">' +
                       '<td class="configLabel-sub">No property available.</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                        '<td>&nbsp;</td>' +
                       '</tr>';
                 }
            }
            childRowClass = $('.child-row-7').find('td:first-child > a').attr('class')
            $('.importPropertyClass').remove()
            if(importList == ''){
                importList = '<tr class="child-row-7 subLabelRow importPropertyClass importParentProp" id="trExport"  style="display:'+ showHide +'">' +
                    '<td class="configLabel">No configuration available.</td>' +
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

            $('.child-row-7').find('td:first-child > a').attr('class',childRowClass)

            if (!$('.child-row-7').find('td:first-child > a').hasClass('glyphicon-triangle-bottom'))
            {
                $('tr[class^=imp-childprop-row-71]').hide().children('td');
            }

            $('.btnUpdateImport').on('click', function(){
                var importInfo = $(this.parentElement.parentElement).data('property');
                $('#plusImport').data('id', importInfo.id);
                VdmUI.importEditId = importInfo.id;
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
                '           <option>KAFKA</option>' +
                '           <option>ELASTICSEARCH</option>' +
                '           <option>HTTP</option>' +
                '           <option>FILE</option>' +
                '           <option>RABBITMQ</option>' +
                '           <option>JDBC</option>' +
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

                $("#txtExportType").val(exportValue.type)

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
                showHideConnectorClass();
                if (typeof type === "undefined") {
                    addExportProperties();
                }
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

            for(var i = 0; i < VdmUI.userSecurityList.length; i++){

                var username = VdmUI.userSecurityList[i].name;
                VdmUI.orgUserList.push(username);
                var roles = VdmUI.userSecurityList[i].roles;
                    securityList += '<tr  style="display:'+showHide+'" class="child-row-6 subLabelRow securityList" data-property=\''+ JSON.stringify(VdmUI.userSecurityList[i]) +'\'><td class="configLabel">'+ username + '</td><td>'+ roles +'</td><td>&nbsp;</td><td>' +
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
                orgUser = securityInfo.name;
                VdmUI.openSecurityPopup();
                VdmUI.afterOpenSecurityPopup(securityInfo);
            })
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
                '<tbody><tr><td width="30%">Username</td><td width="10%">' +
                '<input id="txtUser" name="txtUser" type="text" size="30" aria-required="true" class="error">' +
                '<label id="errorUser" for="txtUser" class="error" style="display:none">This field is required</label>' +
                '<input id="txtOrgUser" name="txtOrgUser" type="text" size="30" aria-required="true" style="display:none"></td>' +
                 '<td>&nbsp;</td> <td>&nbsp;</td></tr><tr><td><span id="labelPassword">Password</span> </td>' +
                  '<td><input id="txtPassword" name="txtPassword" type="password" size="30" aria-required="true" class="error">' +
                  '<label id="errorPassword" for="txtPassword" class="error" style="display:none">This field is required</label> </td>' +
                  '<td>&nbsp;</td> <td>&nbsp;</td></tr><tr><td>Roles </td> <td><select id="selectRole">' +
                  '<option value="Administrator" selected="selected">Administrator</option><option value="User">User</option></select>  </td> ' +
                  '</tr></tbody></table>'

            $('#addSecurityConfigWrapper').html(contents)


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
                    $("#selectRole").val(securityValue.roles);
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
                '<tr id="trImportStream">' +
                '    <td>Module</td>' +
                '    <td width="15%">' +
                '       <input id="txtImportStream" name="txtImportStream" type="text" size="38">' +
                '       <label id="errorImportStream" for="txtImportStream" class="error" style="display: none;"></label>' +
                '    </td>' +
                '    <td width="8%" align="right">' +
                '        <input tabindex="10" type="checkbox" id="chkImportStream" name="chkImportStream" checked="oncheck"></td>' +
                '    <td id="chkImportStreamValue" class="icheck-label">On</td>' +
                '</tr>' +
                '<tr>' +
                '    <td>Type </td>' +
                '    <td>' +
                '       <select id="txtImportType" name="txtImportType">' +
                '           <option>KAFKA</option>' +
                '           <option>ELASTICSEARCH</option>' +
                '           <option>HTTP</option>' +
                '           <option>FILE</option>' +
                '           <option>RABBITMQ</option>' +
                '           <option>JDBC</option>' +
                '           <option>CUSTOM</option>' +
                '       </select></td> ' +
                '    </td>' +
                '    <td>&nbsp;</td>' +
                '    <td>&nbsp;</td>' +
                '</tr>' +
                '<tr id="trImportConnectorClass" style="display:none">' +
                '    <td>Format</td>' +
                '    <td width="15%" id="tdImportConnectorClass">' +
                '        <input id="txtImportConnectorClass" name="txtImportConnectorClass" type="text" size="38">' +
                '        <label id="errorImportConnectorClass" for="txtImportConnectorClass" class="error" style="display: none;"></label></td>' +
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

        };

        this.afterOpenImportPopup = function(importValue){
            //For editing an existing configuration
            if (VdmUI.importEditId != -1) {
                $('#txtImportStream').val(importValue.module);

                $("#txtImportType").val(importValue.type)

                $('#txtImportConnectorClass').val(importValue.importconnectorclass)

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
                            $(".newStreamMinImportProperty").addClass("orgProperty");
                        } else if ($(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + importValue.type]).length && multiPropertyCount == 0) {
                            $(VdmUI.newStreamMinmPropertyName[properties[i].name + '_' + importValue.type]).val(properties[i].value);
                            $(".newStreamMinImportProperty").addClass("orgProperty");
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

        this.showHideDrServers = function(){
            if ($('#row-server').find('td:first-child > a').hasClass('glyphicon-triangle-right') &&
            $('#row-server').find('td:first-child > a').hasClass('glyphicon-triangle-bottom')) {
                $('.clsServerList').show()
            } else {
                $('.clsServerList').hide()
            }
        }

        this.showHideDrProperties = function(){
            if($('#chkDrOnOffVal').text().toLowerCase().trim() == 'on'){
                if($('#lblDrType').text() == undefined || $('#lblDrType').text().toLowerCase() == 'master'){
                    $('.clsDrEnabled').hide();
                    $('.clsServerList').hide()
                }else{
                    $('.clsDrEnabled').show();
                    VdmUI.showHideDrServers();
                }
                $('.clsDrType').show();
            } else {
                $('.clsDrType').hide();
                $('.clsDrEnabled').hide();
                $('.clsServerList').hide()
            }
        }

        this.showHideConnectionSource = function(){
            if(true){
                if($('#selectDrType').val() == undefined || $('#selectDrType').val().toLowerCase() == 'master'){
                    $('.trConnectionSource').hide();
                }else{
                    $('.trConnectionSource').show()
                }
            } else {
                $('.trConnectionSource').hide()
            }
        }

       this.showHideDrConfigs = function(){
            if($('#chkDrOnOffValue').text().toLowerCase() == 'on'){
                $('.showHideDrConfig').show();
                VdmUI.showHideConnectionSource()
                if(VdmUI.showHideXDCRMessage){
                    $('.showHideXDCRMessagge').show()
                } else {
                    $('.showHideXDCRMessagge').hide()
                }
//                if(VdmUI.showHideReplicaMessage){
//                    $('#drReplicaMessage').show();
//                } else {
//                    $('#drReplicaMessage').hide();
//                }
            } else {
                $('.showHideDrConfig').hide();
                $('.showHideXDCRMessagge').hide()
            }
        }

        this.populateDrPopup = function(){
            dbData = {
               id: VdmUI.getCurrentDbCookie()
            }
            VdmService.GetDeployment(function(connection){
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                if(deploymentInfo['deployment']['dr'] != undefined ){
                    if(deploymentInfo['deployment']['dr'].enabled == true){
                        clusterObjects.chkDrOnOffStatus.iCheck('check');
                    } else {
                        clusterObjects.chkDrOnOffStatus.iCheck('uncheck');
                    }
                    clusterObjects.txtDrId.val(deploymentInfo['deployment']['dr'].id);
                    clusterObjects.selectDrType.val(deploymentInfo['deployment']['dr'].type == undefined ||
                    deploymentInfo['deployment']['dr'].type == '' ? 'Master' : deploymentInfo['deployment']['dr'].type);
                    VdmService.GetDatabaseList(function(connection){
                         var dbIdList = []
                         var htmlContent = '<select id="tdDbSelect">';
                         connection.Metadata['DATABASE_LISTING'].databases.forEach(function (info) {
                             var dbId = info['id'];
                             if(dbId != VdmUI.getCurrentDbCookie()){
                                 var dbName = info['name'];
                                 if(VdmUI.ConnectionSourceStatus && dbName == VdmUI.masterCluster){
                                    htmlContent += '<option value="'+ dbId + '" selected>' + dbName + '</option>';
                                 } else {
                                    htmlContent += '<option value="'+ dbId + '">' + dbName + '</option>';
                                 }
                             }
                         });
                         if(htmlContent == '<select id="tdDbSelect">'){
                            htmlContent = 'No database available'
                            $('#selectDrDatabase').html(htmlContent);
                            $('#divDrServerList').html('<ul><li>No servers available.</li></ul>' );
                         } else {
                            $('#trServerList').show();
                            htmlContent += '</select>';
                            $('#selectDrDatabase').html(htmlContent);
                            VdmUI.getServersForDb();
                            VdmUI.checkConditionsForDr()
                         }
                         VdmUI.showHideConnectionSource()
                         VdmUI.showHideDrConfigs();
                    });
                }
            },dbData);
        }

        this.getServersForDb = function(){
            var dbId = $('#tdDbSelect').val()
            if(dbId == ""){
                $("#tdDbSelect option:first").attr('selected','selected');
                dbId = $('#tdDbSelect').val()
            }
            var dbData = {
                id: dbId
            }
            var deployData = {
                id: VdmUI.getCurrentDbCookie()
            }
            VdmService.GetDeployment(function(connection){
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                var selectedMembers = null;
                if(deploymentInfo['deployment']['dr'] != undefined && deploymentInfo['deployment']['dr']['connection'] != undefined
                && deploymentInfo['deployment']['dr']['connection']['source'] != undefined){
                    selectedMembers = deploymentInfo['deployment']['dr']['connection']['servers'];
                }
                VdmService.GetServerList(function(connection){
                    serverList = connection.Metadata['SERVER_LISTING'];
                    var dbData = {id: dbId}
                    VdmService.GetMemberList(function(connection){
                        VdmUI.members = connection['Metadata']['MEMBER_LISTING'].members;
                        var htmlList = '<ul class="server-lists" id="ulDrServerList">';
                        VdmUI.CurrentServerList = [];
                        VdmUI.CurrentHostList = [];
                        serverList.servers.forEach(function (info) {
                            var hostName = info["hostname"];
                            var serverName = info["name"]
                            var hostId = info["id"];
                            if($.inArray(hostId, VdmUI.members) != -1){
                                if(selectedMembers != null){
                                    if($.inArray(hostId, selectedMembers) != -1){
                                        htmlList += '<li class="checked-server"><label><input name="drServers" class="chkServerList" type="checkbox" checked value="' + hostId + '"/>' + hostName + '</label></li>';
                                    }else{
                                        htmlList += '<li><label><input name="drServers" class="chkServerList" type="checkbox" value="' + hostId + '"/>' + hostName + '</label></li>' ;
                                    }
                                } else {
                                     htmlList += '<li><label><input name="drServers" class="chkServerList" type="checkbox" value="' + hostId + '"/>' + hostName + '</label></li>' ;
                                }
                            }
                            if(htmlList == '<ul class="server-lists" id="ulDrServerList">'){
                                $('#divDrServerList').html('<ul><li>No servers available.</li></ul>' );
                            } else{
                                $('#divDrServerList').html(htmlList + '</ul>');
                            }
                        });

                        $('.chkServerList').change(function(){
                            if(this.checked){
                                $(this.parentElement.parentElement).addClass('checked-server')
                            } else {
                                $(this.parentElement.parentElement).removeClass('checked-server');
                            }
                        })
                    },dbData);
                });
            },deployData);
        }

        this.checkConditionsForDr = function(){
            VdmService.GetDeployment(function(connection){
                var currentDbId = VdmUI.getCurrentDbCookie();
                var selectedDbId = 2;
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                //Check current database status
                clusterObjects.selectDrType.prop('disabled', false);
                clusterObjects.selectDrType.removeClass('disableSelect')
                $('#tdDbSelect').prop('disabled', false);
                $('#tdDbSelect').removeClass('disableSelect')
                $('#drMessage').hide()
                $('#drReplicaMessage').hide()
                VdmUI.showHideXDCRMessage = false;
                VdmUI.showHideReplicaMessage = false;
                for(var i = 0;i < deploymentInfo.deployment.length; i++){
                    var drInfo = deploymentInfo.deployment[i].dr;
                    if(VdmUI.CurrentDbName != '' && deploymentInfo.deployment[i].databaseid != VdmUI.getCurrentDbCookie()
                    && drInfo != undefined && drInfo.connection != undefined && drInfo.connection.source != undefined && drInfo.connection.source == VdmUI.CurrentDbName){
                        if(drInfo.type.toLowerCase() == 'xdcr'){
                            var selectedDbId = deploymentInfo.deployment[i].databaseid;
                            var selectedDbType = 'XDCR'
                            clusterObjects.selectDrType.val(deploymentInfo.deployment[i].dr.type);
                            $('#tdDbSelect').val(selectedDbId);
                            clusterObjects.selectDrType.prop('disabled', true);
                            clusterObjects.selectDrType.addClass('disableSelect')
                            $('#tdDbSelect').prop('disabled', true);
                            $('#tdDbSelect').addClass('disableSelect')
                            VdmUI.showHideXDCRMessage = true;
                            $('#spanCurrentDb').html($("#tdDbSelect option:selected").text());
                            $('#spanSelectedDb').html($("#tdDbSelect option:selected").text());
                            VdmUI.getServersForDb()

                        } else if (drInfo.type.toLowerCase() == 'replica') {
//                            clusterObjects.selectDrType.prop('disabled', true);
//                            clusterObjects.selectDrType.addClass('disableSelect')
//                            VdmUI.showHideReplicaMessage = true;
//                            $('#tdDbSelect').prop('disabled', true);
//                            $('#tdDbSelect').addClass('disableSelect')

                        } else {
                            clusterObjects.selectDrType.prop('disabled', false);
                            clusterObjects.selectDrType.removeClass('disableSelect')
                            $('#tdDbSelect').prop('disabled', false);
                            $('#tdDbSelect').removeClass('disableSelect')
                            $('#drMessage').hide()
                        }
                    }
//                    else {
//                        clusterObjects.selectDrType.prop('disabled', false);
//                        $('#tdDbSelect').prop('disabled', false);
//                    }

                }
                VdmUI.showHideDrConfigs()
            });
        }

        this.checkConditionForDbChange = function() {
            var result = true;
            var selectedDb = $('#tdDbSelect').val()
            dbData = {
               id: selectedDb
            }
            VdmService.GetDeployment(function(connection){
                var deploymentInfo = connection.Metadata['DEPLOYMENT'];
                if(clusterObjects.selectDrType.val() == 'Replica'){
                    if(deploymentInfo['deployment'].dr != null){
                        var deploymentStatus = deploymentInfo['deployment'].dr.type;
                        //dr type must be master
                        if(deploymentStatus.toLowerCase() != 'master'){
                            result = false;
                            $('#drServers-error').show();
                            $('#drServers-error').html('The selected database must be master.')
                        }
                    } else {
                        //display alert message that dr is not set
                        result = false;
                        $('#drServers-error').show();
                        $('#drServers-error').html('The selected database must have DR enabled.')
                    }
                } else if (clusterObjects.selectDrType.val() == 'XDRC'){
                    if(deploymentInfo['deployment'].dr != null){
                        //allow creating
                        if(deploymentInfo['deployment'].dr.connection.source !=  VdmUI.CurrentDbName){
                            //allow creating
                            result = false;
                            $('#drServers-error').show();
                            $('#drServers-error').html('The selected database is configured to use XDCR with another database. Please use another database.')
                        }
                    }
                }

            }, dbData)
        }
    });
    window.VdmUI = VdmUI = new iVdmUi();
})(window);



