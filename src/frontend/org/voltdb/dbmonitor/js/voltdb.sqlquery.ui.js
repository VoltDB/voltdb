$(document).ready(function () {

    //Default Action
    $(".tab_content").hide(); //Hide all content
    $("ul.tabs li:first").addClass("active").show(); //Activate first tab
    $(".tab_content:first").show(); //Show first tab content
   
    //On Click Event
    $("ul.tabs li").click(function () {
        $("ul.tabs li").removeClass("active"); //Remove any "active" class
        $(this).addClass("active"); //Add "active" class to selected tab
        $(".tab_content").hide(); //Hide all tab content
        var activeTab = $(this).find("a").attr("href"); //Find the rel attribute value to identify the active tab + content
        $(activeTab).fadeIn(); //Fade in the active content
        return false;
    });
    

    // Table Accordion	
    $('#accordionTable').accordion({
        collapsible: true,
        active: false,
        beforeActivate: function (event, ui) {
            // The accordion believes a panel is being opened
            if (ui.newHeader[0]) {
                var currHeader = ui.newHeader;
                var currContent = currHeader.next('.ui-accordion-content');
                // The accordion believes a panel is being closed
            } else {
                var currHeader = ui.oldHeader;
                var currContent = currHeader.next('.ui-accordion-content');
            }
            // Since we've changed the default behavior, this detects the actual status
            var isPanelSelected = currHeader.attr('aria-selected') == 'true';

            // Toggle the panel's header
            currHeader.toggleClass('ui-corner-all', isPanelSelected).toggleClass('accordion-header-active ui-state-active ui-corner-top', !isPanelSelected).attr('aria-selected', ((!isPanelSelected).toString()));

            // Toggle the panel's icon
            currHeader.children('.ui-icon').toggleClass('ui-icon-triangle-1-e', isPanelSelected).toggleClass('ui-icon-triangle-1-s', !isPanelSelected);

            // Toggle the panel's content
            currContent.toggleClass('accordion-content-active', !isPanelSelected)
            if (isPanelSelected) { currContent.slideUp(50); } else { currContent.slideDown(50); }

            return false; // Cancels the default action
        }
    });


    // Views Accordion	
    $('#accordionViews').accordion({
        collapsible: true,
        active: false,
        beforeActivate: function (event, ui) {
            // The accordion believes a panel is being opened
            if (ui.newHeader[0]) {
                var currHeader = ui.newHeader;
                var currContent = currHeader.next('.ui-accordion-content');
                // The accordion believes a panel is being closed
            } else {
                var currHeader = ui.oldHeader;
                var currContent = currHeader.next('.ui-accordion-content');
            }
            // Since we've changed the default behavior, this detects the actual status
            var isPanelSelected = currHeader.attr('aria-selected') == 'true';

            // Toggle the panel's header
            currHeader.toggleClass('ui-corner-all', isPanelSelected).toggleClass('accordion-header-active ui-state-active ui-corner-top', !isPanelSelected).attr('aria-selected', ((!isPanelSelected).toString()));

            // Toggle the panel's icon
            currHeader.children('.ui-icon').toggleClass('ui-icon-triangle-1-e', isPanelSelected).toggleClass('ui-icon-triangle-1-s', !isPanelSelected);

            // Toggle the panel's content
            currContent.toggleClass('accordion-content-active', !isPanelSelected)
            if (isPanelSelected) { currContent.slideUp(50); } else { currContent.slideDown(50); }

            return false; // Cancels the default action
        }
    });

    // Procedures Accordion
    $('#accordionProcedures').accordion({
        collapsible: true,
        active: false,
        beforeActivate: function (event, ui) {
            // The accordion believes a panel is being opened
            if (ui.newHeader[0]) {
                var currHeader = ui.newHeader;
                var currContent = currHeader.next('.ui-accordion-content');
                // The accordion believes a panel is being closed
            } else {
                var currHeader = ui.oldHeader;
                var currContent = currHeader.next('.ui-accordion-content');
            }
            // Since we've changed the default behavior, this detects the actual status
            var isPanelSelected = currHeader.attr('aria-selected') == 'true';

            // Toggle the panel's header
            currHeader.toggleClass('ui-corner-all', isPanelSelected).toggleClass('accordion-header-active ui-state-active ui-corner-top', !isPanelSelected).attr('aria-selected', ((!isPanelSelected).toString()));

            // Toggle the panel's icon
            currHeader.children('.ui-icon').toggleClass('ui-icon-triangle-1-e', isPanelSelected).toggleClass('ui-icon-triangle-1-s', !isPanelSelected);

            // Toggle the panel's content
            currContent.toggleClass('accordion-content-active', !isPanelSelected)
            if (isPanelSelected) { currContent.slideUp(50); } else { currContent.slideDown(50); }

            return false; // Cancels the default action
        }
    });

    // Implements Scroll in Server List div
    $('#tabScroller').slimscroll({
        disableFadeOut: true,
        height: '225px'
    });
    
});

function loadSQLQueryPage(serverName, portid, userName, password, admin) {
    
    function saveConnectionKey() {
        var server = serverName == null ? VoltDBConfig.GetDefaultServerNameForKey() : $.trim(serverName);
        var port = portid == null ? '8080' : $.trim(portid);
        var user = userName == '' ? null : userName;
        var processName = 'TABLE_INFORMATION';
        var key = (server + '_' + port + '_' + (user == '' ? '' : user) + '_' +
            (admin == true ? 'Admin' : '') + "_" + processName).replace(/[^_a-zA-Z0-9]/g, "_");
        saveCookie('connectionkey', key);
    }

    saveConnectionKey();

    var tablesArray = [];

    // Export Type Change	 
    $('#exportType').change(function () {
        if ($('#exportType').val() == 'HTML') {
            $('#resultHtml').css('display', 'block');
            $('#resultCsv').css('display', 'none');
            $('#resultMonospace').css('display', 'none');

        } else if ($('#exportType').val() == 'CSV') {
            $('#resultCsv').css('display', 'block');
            $('#resultHtml').css('display', 'none');
            $('#resultMonospace').css('display', 'none');

        } else if ($('#exportType').val() == 'Monospace') {
            $('#resultMonospace').css('display', 'block');
            $('#resultHtml').css('display', 'none');
            $('#resultCsv').css('display', 'none');
        }
    });
    // Clears Query
    $('#clearQuery').click(function () {
        $('#theQueryText').val('');
    });

    // Tooltip

    $('.tooltip').tooltipster();

    var populateTableData = function (tables) {
        var count = 0;
        var src = "";
        for (var k in tables) {
            tablesArray.push(k);
            src += '<h3>' + k + '</h3>';
            var item = tables[k];
            src += '<div id="column_' + count + '" class="listView">';
            if (item.columns != null) {
                src += '<ul>';
                for (var c in item.columns)
                    src += '<li>' + item.columns[c] + '</li>';
                src += '</ul>';
            } else
                src += '<ul><li>Schema could not be read...</li></ul>';
            src += '</div>';
            count++;
        }
        if (src == "") {
            src += '<h3>No tables found.</h3>';
            $('#accordionTable').html(src);
        } else {
            $('#accordionTable').html(src);
            $("#accordionTable").accordion("refresh");
        }
    };

    var populateViewData = function (views) {
        var count = 0;
        var src = "";
        for (var k in views) {
            src += '<h3>' + k + '</h3>';
            var item = views[k];
            src += '<div id="view_' + count + '" class="listView">';
            if (item.columns != null) {
                src += '<ul>';
                for (var c in item.columns)
                    src += '<li>' + item.columns[c] + '</li>';
                src += '</ul>';
            } else
                src += '<ul><li>Schema could not be read...</li></ul>';
            src += '</div>';
            count++;
        }
        if (src == "") {
            src += '<h3>No views found.</h3>';
            $('#accordionViews').html(src);
        } else {
            $('#accordionViews').html(src);
            $("#accordionViews").accordion("refresh");
        }
    };

    var populateStoredProcedure = function (proceduresData, procedureColumnData, sysProcedure) {
        // Stored Procedures
        var sysScr = "";
        var src = "";
        var defSrc = "";
        sysScr += '<h3 class="systemHeader">System Stored Procedures</h3>';
        sysScr += '<div id="systemProcedure" class="listView">';
        for (var k in sysProcedure) {
            for (var paramCount in sysProcedure[k]) {
                sysScr += '<h3>' + k + '(' + paramCount + ')</h3>';
                sysScr += '<div class="listView">';
                sysScr += '<ul>';
                for (var i = 0; i < sysProcedure[k][paramCount].length - 1; i++) {
                    sysScr += '<li class="parameterValue">' + sysProcedure[k][paramCount][i] + '</li>';
                }
                sysScr += '<li class="returnValue">' + sysProcedure[k][paramCount][i] + '</li>';
                sysScr += '</ul>';
                sysScr += '</div>';
            }
        }

        sysScr += '</div>';
        for (var i = 0; i < proceduresData.length; ++i) {
            var connTypeParams = [];
            var procParams = [];
            var procName = proceduresData[i][2];
            for (var p = 0; p < procedureColumnData.length; ++p) {
                if (procedureColumnData[p][2] == procName) {
                    paramType = procedureColumnData[p][6];
                    paramName = procedureColumnData[p][3];
                    paramOrder = procedureColumnData[p][17] - 1;
                    if (procedureColumnData[p][12] == "ARRAY_PARAMETER") {
                        if (paramType.toLowerCase() == "tinyint") // ENG-2040 and ENG-3101, identify it as an array (byte[])
                            paramType = "byte[]";
                        else
                            paramType += "_array";
                    }
                    procParams[paramOrder] = { 'name': paramName, 'type': paramType.toLowerCase() };
                }
            }

            var procArray = procName.split('.');
            if (procArray.length > 1 && jQuery.inArray(procArray[0], tablesArray) != -1) {
                defSrc += '<h3>' + procName + '</h3>';
                defSrc += '<div class="listView">';
                defSrc += '<ul>';
                for (var p = 0; p < procParams.length; ++p) {
                    defSrc += '<li class="parameterValue">Param' + (p) + ' (' + procParams[p].type + ')</li>';
                }
                defSrc += '<li class="returnValue">Return Table[]</li>';
                defSrc += '</ul>';
                defSrc += '</div>';
            } else {
                src += '<h3>' + procName + '</h3>';
                src += '<div class="listView">';
                src += '<ul>';
                for (var p = 0; p < procParams.length; ++p) {
                    src += '<li class="parameterValue">Param' + (p) + ' (' + procParams[p].type + ')</li>';
                }
                src += '<li class="returnValue">Return Table[]</li>';
                src += '</ul>';
                src += '</div>';

            }

        }
        var defSrcHeader = "";
        defSrcHeader += '<h3 class="systemHeader">Default Stored Procedures</h3>';
        defSrcHeader += '<div id="defaultProcedure" class="listView">';
        var defSrcFooter = '</div>';
        defSrc = defSrcHeader + defSrc + defSrcFooter;
        $('#accordionProcedures').html(sysScr + defSrc + src);
        $('#accordionProcedures').accordion("refresh");
        $('#systemProcedure').accordion({
            collapsible: true,
            active: false,
            beforeActivate: function (event, ui) {
                // The accordion believes a panel is being opened
                if (ui.newHeader[0]) {
                    var currHeader = ui.newHeader;
                    var currContent = currHeader.next('.ui-accordion-content');
                    // The accordion believes a panel is being closed
                } else {
                    var currHeader = ui.oldHeader;
                    var currContent = currHeader.next('.ui-accordion-content');
                }
                // Since we've changed the default behavior, this detects the actual status
                var isPanelSelected = currHeader.attr('aria-selected') == 'true';

                // Toggle the panel's header
                currHeader.toggleClass('ui-corner-all', isPanelSelected).toggleClass('accordion-header-active ui-state-active ui-corner-top', !isPanelSelected).attr('aria-selected', ((!isPanelSelected).toString()));

                // Toggle the panel's icon
                currHeader.children('.ui-icon').toggleClass('ui-icon-triangle-1-e', isPanelSelected).toggleClass('ui-icon-triangle-1-s', !isPanelSelected);

                // Toggle the panel's content
                currContent.toggleClass('accordion-content-active', !isPanelSelected)
                if (isPanelSelected) { currContent.slideUp(50); } else { currContent.slideDown(50); }

                return false; // Cancels the default action
            }
        });
        $('#defaultProcedure').accordion({
            collapsible: true,
            active: false,
            beforeActivate: function (event, ui) {
                // The accordion believes a panel is being opened
                if (ui.newHeader[0]) {
                    var currHeader = ui.newHeader;
                    var currContent = currHeader.next('.ui-accordion-content');
                    // The accordion believes a panel is being closed
                } else {
                    var currHeader = ui.oldHeader;
                    var currContent = currHeader.next('.ui-accordion-content');
                }
                // Since we've changed the default behavior, this detects the actual status
                var isPanelSelected = currHeader.attr('aria-selected') == 'true';

                // Toggle the panel's header
                currHeader.toggleClass('ui-corner-all', isPanelSelected).toggleClass('accordion-header-active ui-state-active ui-corner-top', !isPanelSelected).attr('aria-selected', ((!isPanelSelected).toString()));

                // Toggle the panel's icon
                currHeader.children('.ui-icon').toggleClass('ui-icon-triangle-1-e', isPanelSelected).toggleClass('ui-icon-triangle-1-s', !isPanelSelected);

                // Toggle the panel's content
                currContent.toggleClass('accordion-content-active', !isPanelSelected)
                if (isPanelSelected) { currContent.slideUp(50); } else { currContent.slideDown(50); }

                return false; // Cancels the default action
            }
        });

    };

    var populateTablesAndViews = function () {
        voltDbRenderer.GetTableInformation(function (tablesData, viewsData, proceduresData, procedureColumnsData,sysProcedureData) {
            var tables = tablesData['tables'];
            populateTableData(tables);
            var views = viewsData['views'];
            populateViewData(views);
            var procedures = proceduresData['procedures'];
            var procedureColumns = procedureColumnsData['procedureColumns'];
            var sysProcedure = sysProcedureData['sysProcedures'];
            populateStoredProcedure(procedures, procedureColumns,sysProcedure);
        });
    };
    populateTablesAndViews();

    $('#runBTn').click(function () {
        var queryString = $('#theQueryText').getSelectedText();
        if (queryString != null) {
            queryString = queryString.replace(/^\s+|\s+$/g, '');
            if (queryString == '')
                queryString = $('#theQueryText').val();
        }
        else
            queryString = $('#theQueryText').val();

        new QueryUI(queryString).execute();

    });
    $('#clearQuery').click(function () {
        $('#theQueryText').val('');
    });

    $("#overlay").hide();
}

