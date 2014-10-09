$(document).ready(function () {
    if (navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1) {
        shortcut.add("f6", function () {
            $("#runBTn").button().click();
        });
    } else {
        shortcut.add("f5", function () {
            $("#runBTn").button().click();
        });
    }
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

    // Implements Scroll in Server List div
    $('#tabScroller').slimscroll({
        disableFadeOut: true,
        height: '225px'
    });
    
    //Attach the login popup to the page.
    $("body").append(voltDbRenderer.GetLoginPopup());

    var serverName = window.location.hostname == "localhost" ? null : window.location.hostname;
    var portid = window.location.hostname == "localhost" ? null : window.location.port;
    
    //If security is enabled, then it displays login popup. After user is verified, it calls loadPage().
    //If security is not enabled, then it simply calls loadPage().
    voltDbRenderer.HandleLogin(serverName, portid, function() { loadPage(serverName, portid); });
});

var saveCookie = function (name, value) {
    $.cookie(name, value, { expires: 365 });
};

var saveSessionCookie = function (name, value) {
    $.cookie(name, value, { path: '/', domain: window.location.hostname });
};

function loadPage(serverName, portid) {
    
    var userName = $.cookie('username') != undefined ? $.cookie('username') : "";
    var password = $.cookie('password') != undefined ? $.cookie('password') : "";
    var admin = $.cookie('admin') != undefined ? $.cookie('admin') : true;
    voltDbRenderer.ChangeServerConfiguration(serverName, portid, userName, password, true, admin);
    voltDbRenderer.ShowUsername(userName);

    function saveConnectionKey() {
        var server = serverName == null ? '184_73_30_156' : $.trim(serverName);
        var port = portid == null ? '8080' : $.trim(portid);
        var user = userName == '' ? null : userName;
        var processName = 'TABLE_INFORMATION';
        var key = (server + '_' + port + '_' + (user == '' ? '' : user) + '_' +
            (admin == true ? 'Admin' : '') + "_" + processName).replace(/[^_a-zA-Z0-9]/g, "_");
        saveCookie('connectionkey', key);
    }

    saveConnectionKey();

    var queryString = '';

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
        var query = new QueryUI(queryString).execute();
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

    var populateTablesAndViews = function () {
        voltDbRenderer.GetTableInformation(function (tablesData, viewsData) {
            var tables = tablesData['tables'];
            populateTableData(tables);
            var views = viewsData['views'];
            populateViewData(views);
        });
    };
    populateTablesAndViews();

    $('#runBTn').click(function () {
        queryString = $('#theQueryText').val();
        new QueryUI(queryString).execute();

    });
    $('#clearQuery').click(function () {
        $('#theQueryText').val('');
    });
}
