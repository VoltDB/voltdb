(function( window, undefined ){

var IMainUI = (function(){

var tab_counter = 1;
var $tabs = null;
var $volt_version = '4.6.tp';

this.InitWorkspace = function()
{
    var autoConnect = false;

    $(".sp_track").slider({ value:0, min:0, max: 205, slide: function(event,ui) {
            $('#sidebar').width(Math.floor(250+parseInt(ui.value)));
            $('#workspace').css("left", (5+Math.floor(250+parseInt(ui.value))) + "px");
            MonitorUI.Redraw();
        }
    });

    $tabs = $("#worktabs").tabs(
    {
        tabTemplate: "<li><a href='#{href}'>#{label}</a> <span class='ui-icon ui-icon-close close-tab' title='Close Tab'>Close</span></li>",
        panelTemplate: '<div id="#{href}"></div>',
        add: function(event, ui)
        {
            $tabs.tabs('select', '#' + ui.panel.id);
            $('.close-tab').click(function() {
                var index = $( "li", $tabs ).index( $( this ).parent() );
                $tabs.tabs( "remove", index );
            });
            $('.close-tab').removeClass('close-tab');

            var tab = $($('#worktabs div.ui-tabs-panel')[$tabs.tabs('option','selected')]);
            if (ui.panel.id.substr(0,1) == 'q')
            {
                tab.addClass('query');
                tab.html('<div class="querybar"><div class="wrapper"><textarea class="querybox" wrap="off"></textarea></div></div><div class="workspacestatusbar ui-widget-header noborder"><select class="datasource" title="Select Connection"></select><span class="status"></span></div><div class="resultbar"></div><div class="sp_track2"><div class="sp_track_h2 ui-slider-handle">&nbsp;</div></div>');
                $('#' + ui.panel.id + ' .sp_track2').slider({ orientation:"vertical", value:150, min:0, max: 405, slide: function(event,ui) {
                        var tabId = this.parentNode.id;
                        $('#' + tabId + ' div.resultbar').height(50+Math.floor(parseInt(ui.value)-0));
                        $('#' + tabId + ' div.workspacestatusbar').css("bottom", (50+Math.floor(5+parseInt(ui.value))) + "px");
                        $('#' + tabId + ' div.querybar').css("bottom", (50+18+Math.floor(5+parseInt(ui.value))) + "px");
                    }
                });
                MainUI.RefreshDataSources(tab.find('select.datasource'));
                tab.find('select.datasource').val($('.treeview li.database.selected').attr('id'));
            }
            else
            {
                tab.addClass('monitor');
                tab.html('<div class="monitortab"><div class="chartbar"><div class="top"><div class="leftchart"><div class="chartitemselector"><select title="Choose Chart Metric" id="leftitem-' + ui.panel.id + '" class="chartitem"><option value="mem">Memory (GB)</option><option value="lat" selected>Latency (ms)</option><option value="tps">Transactions/s</option><option value="str">Site Starvation %</option><option value="tb">Transaction Breakdown</option></select></div><div id="leftchart-' + ui.panel.id + '" class="graph"></div></div><div class="rightchart"><div class="chartitemselector"><select title="Choose Chart Metric" id="rightitem-' + ui.panel.id + '" class="chartitem"><option value="mem">Memory (GB)</option><option value="lat">Latency (ms)</option><option value="tps" selected>Transactions/s</option><option value="str">Site Starvation %</option><option value="tb">Transaction Breakdown</option></select></div><div id="rightchart-' + ui.panel.id + '" class="graph"></div></div></div></div><div class="workspacestatusbar ui-widget-header noborder"><select title="Choose Quick Monitor Metric" class="monitoritemselection"><option value="m">Memory (GB)</option><option value="l">Latency (ms)</option><option value="t">Transactions/s</option></select><span class="monitoritemlabel">Last</span><span class="ui-corner-all monitoritem">10</span><span class="monitoritemlabel">Min</span><span class="ui-corner-all monitoritem">10</span><span class="monitoritemlabel">Max</span><span class="ui-corner-all monitoritem">10</span><span class="monitoritemlabel">Avg</span><span class="ui-corner-all monitoritem">10</span><span id="rd-' + ui.panel.id + '" class="link" title="Update Monitor Now">Update Now</span></div><div class="tablebar"></div><div class="sp_track3"><div class="sp_track_h3 ui-slider-handle">&nbsp;</div></div></div>');
                $('#' + ui.panel.id + ' .sp_track3').slider({ orientation:"vertical", value:150, min: 0, max: 405, slide: function(event,ui) {
                        var tabId = this.parentNode.parentNode.id;
                        $('#' + tabId + ' div.tablebar').height(50+Math.floor(parseInt(ui.value)-0));
                        $('#' + tabId + ' div.workspacestatusbar').css("bottom", (50+Math.floor(5+parseInt(ui.value))) + "px");
                        $('#' + tabId + ' div.chartbar').css("bottom", (50+18+Math.floor(5+parseInt(ui.value))) + "px");
                        MonitorUI.Redraw();
                    }
                });
                MonitorUI.AddMonitor(tab);
                var cbw1 = function(tid){ var id = tid; this.Callback = function() {MonitorUI.RefreshMonitorData(id);}};
                $('#rd-' + ui.panel.id).click((new cbw1(ui.panel.id)).Callback);

                var cbw2 = function(tid){ var id = tid; this.Callback = function() {MonitorUI.UpdateMonitorItem(id);}};
                $('#' + ui.panel.id + ' select.monitoritemselection').change((new cbw2(ui.panel.id)).Callback);

                var cbw3 = function(tid, tchart){ var id = tid; var chart = tchart; this.Callback = function() {MonitorUI.ChangeChartMetric(id, chart, $('#' + chart + 'item-'+id).val());}};
                $('#leftitem-' + ui.panel.id).change((new cbw3(ui.panel.id, 'left')).Callback);
                $('#rightitem-' + ui.panel.id).change((new cbw3(ui.panel.id, 'right')).Callback);

            }
            SetExecuteButtonState();
        },
        remove: function(event, ui)
        {
            OnTabRemoved(ui.panel.id);
        },
        show: function(event, ui)
        {
            SetExecuteButtonState();
        }
    });

    $("#new-query").button().click(function() {
        if (Object.size(VoltDB.Connections) == 0)
        {
            MainUI.OnAddConnectionCompleted = function(connection) {$tabs.tabs( "add", "#q-" + tab_counter, 'SQLQuery' + tab_counter + '.sql' ); tab_counter++;};
            $("#connect-dialog").dialog("open");
        }
        else
        {
            $tabs.tabs( "add", "#q-" + tab_counter, 'SQLQuery' + tab_counter + '.sql' );
            tab_counter++;
        }
    });

    $("#new-monitor").button().click(function() {
        if (Object.size(VoltDB.Connections) == 0)
        {
            MainUI.OnAddConnectionCompleted = function(connection) {
                if($('#m-' + connection.Key).length > 0)
                    $tabs.tabs('select','#m-' + connection.Key);
                else
                    $tabs.tabs( 'add', '#m-' + connection.Key, 'Monitor: ' + connection.Display );
            };
            $("#connect-dialog").dialog("open");
        }
        else
        {
            if($('#m-' + MainUI.ActiveConnection().Key).length > 0)
                $tabs.tabs('select','#m-' + MainUI.ActiveConnection().Key);
            else
                $tabs.tabs( 'add', '#m-' + MainUI.ActiveConnection().Key, 'Monitor: ' + MainUI.ActiveConnection().Display );
        }
    });

    $("#execute-query").button().click(function() {
        var queryTab = $($('#worktabs div.ui-tabs-panel')[$tabs.tabs('option','selected')]);
        if (queryTab.length == 1)
            new QueryUI($($('#worktabs div.ui-tabs-panel')[$tabs.tabs('option','selected')])).execute();
    });
    $("#execute-query").button('disable');

    $('#result-format').buttonset();
    $('#monitor-speed').buttonset();
    $('input[name=mspeed]').click(function() { MonitorUI.SetRefreshSpeed(); });

    $('#dbbrowser').treeview();

    $(function() {
        var server = $( "#server" ),
            port = $( "#port" ),
            user = $( "#user" ),
            password = $( "#password" ),
            admin = $('#admin'),
            allFields = $( [] ).add( server ).add( port ).add( user ).add( password ).add( admin ),
            tips = $( ".validateTips" );

        function updateTips( t ) {
            tips
                .text( t )
                .addClass( "ui-state-highlight" );
            setTimeout(function() {
                tips.removeClass( "ui-state-highlight", 1500 );
            }, 500 );
        }

        function checkLength( o, n, min, max ) {
            if ( o.val().length > max || o.val().length < min ) {
                o.addClass( "ui-state-error" );
                updateTips( "Length of field '" + n + "' must be between " + min + " and " + max + "." );
                return false;
            } else {
                return true;
            }
        }

        function checkRegexp( o, regexp, n ) {
            if ( !( regexp.test( o.val() ) ) ) {
                o.addClass( "ui-state-error" );
                updateTips( n );
                return false;
            } else {
                return true;
            }
        }
        $('#connect-dialog').dialog({
            autoOpen: false,
            height: 210,
            width: 450,
            modal: true,
            resizable: false,
            buttons: {
                "Test Connection": function() {
                    var dialog = $(this);
                    var bValid = true;
                    allFields.removeClass( "ui-state-error" );

                    bValid = bValid && checkLength( server, "server", 3, 64 );
                    bValid = bValid && checkLength( port, "port", 1, 5 );
                    bValid = bValid && checkRegexp( port, /^([0-9])+$/, "Please enter a valid port number." );

                    if ( bValid )
                    {
                        if (VoltDB.HasConnection(server.val(), port.val(), false, user.val(), password.val()))
                        {
                            updateTips("Connection already exists.");
                        }
                        else
                        {
                            VoltDB.TestConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val(), false, function(result) {if (result == true) { updateTips("Connection successful."); } else updateTips("Unable to connect.");});
                        }
                    }
                },
                Cancel: function() {
                    $(this).dialog("close");
                },
                "OK": function() {
                    var dialog = $(this);
                    var bValid = true;
                    allFields.removeClass( "ui-state-error" );

                    bValid = bValid && checkLength( server, "server", 3, 64 );
                    bValid = bValid && checkLength( port, "port", 1, 5 );
                    bValid = bValid && checkRegexp( port, /^([0-9])+$/, "Please enter a valid port number." );

                    if ( bValid )
                    {
                        if (VoltDB.HasConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val()))
                        {
                            updateTips("Connection already exists.");
                            dialog.dialog("close");
                        }
                        else
                        {
                            VoltDB.TestConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val(), false, function(result) {if (result == true) { updateTips("Connection successful.");VoltDB.AddConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val(), false, MainUI.AddConnection);dialog.dialog("close"); } else updateTips("Unable to connect."); });
                        }
                    }
                }
            },
            close: function() {
                allFields.val( "" ).removeClass( "ui-state-error" );
            },
            open: function() {
                if (server.val() == '') server.val(hostnameDefault);
                if (port.val() == '') port.val(portDefault);
                tips.text('');
                if (autoConnect)
                {
                    VoltDB.TestConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val(), false, function(result) {if (result == true) { updateTips("Connection successful.");VoltDB.AddConnection(server.val(), port.val(), admin.is(':checked'), user.val(), password.val(), false, MainUI.AddConnection);$('#connect-dialog').dialog("close"); } else updateTips("Unable to connect."); });
                    autoConnect = false;
                }
            }
        });
        $('#connect-dialog').parent().find('.ui-dialog-buttonset').addClass('connect-dialog');
        $('#connect-dialog').parent().find('.ui-dialog-buttonset').find('button:first').addClass('fl');
    });

    $('.sidebar .header span.connect').click(function() { MainUI.OnAddConnectionCompleted = null; $('#connect-dialog').dialog('open'); });
    $("#object-explorer").button().click(MainUI.ToggleSidebar);
    $('#sidebar span.close').click(function(){$("#object-explorer").button().click();$("#object-explorer").button('refresh');});
    $('#about').button().click(function(){$('#about-dialog').dialog('open');});
        $('#about-dialog').dialog({
            autoOpen: false,
            height: 400,
            width: 580,
            modal: true,
            resizable: false,
            buttons: {
                Close: function() {
                    $(this).dialog("close");
                }
            }
        });

    shortcut.add("F5", function() {$("#execute-query").button().click();});
    shortcut.add("Ctrl+N", function() {$("#new-query").button().click();})
    shortcut.add("F3", function() {$("#new-monitor").button().click();})
    shortcut.add("F2", function() {$("#object-explorer").button().click();$("#object-explorer").button('refresh');})
    shortcut.add("Ctrl+D", function() {$("#rf-grd").button().click();$("#rf-grd").button('refresh');})
    shortcut.add("Ctrl+T", function() {$("#rf-fix").button().click();$("#rf-fix").button('refresh');})

    $(document).unload(function() {$('*').unbind(); });
    $(window).resize(function(){MonitorUI.Redraw();});

    if (hasQueryParameters())
    {
        if (getQueryParameter('oe') == 0)
            $('#sidebar span.close').click();
        if (getQueryParameter('rf') != null)
            $('#rf-' + getQueryParameter('rf')).click();
        if (getQueryParameter('ms') != null)
            $('#ms-' + getQueryParameter('ms')).click();
        if (getQueryParameter('startup') != null)
        {
            if (getQueryParameter('startup') == 'monitor')
            {
                autoConnect = true;
                setTimeout(function() {$("#new-monitor").button().click()}, 100);
            }
            else if (getQueryParameter('startup') == 'query')
            {
                autoConnect = true;
                setTimeout(function() {$("#new-query").button().click()}, 100);
            }
        }
    }
    else
    {
        if (Injection[2] != '${requires-authentication}')
        {
            hostnameDefault = Injection[0];
            portDefault = Injection[1];
            if (Injection[2] == 'false')
                autoConnect = true;
            setTimeout(function() {$("#new-query").button().click()}, 100);
        }
        parent.frames['versioncheck'].location.href = 'http://community.voltdb.com/versioncheck?app=webstudio&ver=' + $volt_version + '&productid=ws-ipc';
    }
}
function hasQueryParameters()
{
    return window.location.search.substring(1) != '';
}
function getQueryParameter(key)
{
    var pairs = window.location.search.substring(1).split('&');
    for (i=0;i<pairs.length;i++)
    {
        pair = pairs[i].split("=");
        if (pair[0] == key)
            return decodeURIComponent(pair[1]);
    }
    return null;
}
function OnTabRemoved(panelid)
{
    if (panelid.substr(0,1) == 'm')
        MonitorUI.RemoveMonitor(panelid);
    SetExecuteButtonState();
}
function SetExecuteButtonState()
{
    if ($tabs == null) return;
    var tab = $($('#worktabs div.ui-tabs-panel')[$tabs.tabs('option','selected')]);
    if (tab == null)
        $("#execute-query").button('disable');
    else if (tab.hasClass('query'))
        $("#execute-query").button('enable');
    else
        $("#execute-query").button('disable');
}

var WorkspacePosition = null;
this.ToggleSidebar = function()
{
    if(WorkspacePosition == null)
    {
        WorkspacePosition = $('#workspace').css('left');
        $('#sidebarwrapper').addClass('hidden');
        $('#workspace').css('left', '0px');
    }
    else
    {
        $('#sidebarwrapper').removeClass('hidden');
        $('#workspace').css('left', WorkspacePosition);
        WorkspacePosition = null;
    }
    MonitorUI.Redraw();
}

this.OnAddConnectionCompleted = null;

this.AddConnection = function(connection, success)
{
    if ($('#'+connection.Key).size() > 0)
        return;

    var src = '<li id="' + connection.Key + '" class="database"><span><span>' + connection.Display + '</span></span><ul>';

    // Adding Tables
    src += '<li class="folder closed"><span>Tables</span>';
    if (Object.size(connection.Metadata['tables']) > 0)
    {
        src += '<ul>';
        for(var k in connection.Metadata['tables'])
        {
            src += '<li class="table closed"><span>' + k + '</span>';
            var item = connection.Metadata['tables'][k];
            src += '<ul>';
            src += '<li class="folder closed"><span>Columns</span>';
            if (item.columns != null)
            {
                src += '<ul>';
                for(var c in item.columns)
                    src += '<li class="column"><span>' + item.columns[c] + '</span></li>';
                src += '</ul>';
            }
            else
                src += '<ul><li><span>Schema could not be read...</span></li></ul>';
            src += '</li>';

            src += '<li class="folder closed"><span>Keys</span>';
            if (item.key != null)
                src += '<ul><li class="key"><span>' + item.key + '</span></li></ul>';
            src += '</li>';

            src += '<li class="folder closed"><span>Indexes</span>';
            if (item.indexes != null)
            {
                src += '<ul>';
                for(var i in item.indexes)
                    src += '<li class="index"><span>' + item.indexes[i] + '</span></li>';
                src += '</ul>';
            }
            src += '</ul>';
            src += '</li>';
        }
        src += '</ul>';
    }
    src += '</li>';

    // Adding Views
    src += '<li class="folder closed"><span>Views</span>';
    if (Object.size(connection.Metadata['views']) > 0)
    {
        src += '<ul>';
        for(var k in connection.Metadata['views'])
        {
            src += '<li class="view closed"><span>' + k + '</span>';
            var item = connection.Metadata['views'][k];
            src += '<ul>';
            src += '<li class="folder closed"><span>Columns</span>';
            if (item.columns != null)
            {
                src += '<ul>';
                for(var c in item.columns)
                    src += '<li class="column"><span>' + item.columns[c] + '</span></li>';
                src += '</ul>';
            }
            else
                src += '<ul><li><span>Schema could not be read...</span></li></ul>';
            src += '</li>';

            src += '<li class="folder closed"><span>Keys</span>';
            if (item.key != null)
                src += '<ul><li class="key"><span>' + item.key + '</span></li></ul>';
            src += '</li>';

            src += '<li class="folder closed"><span>Indexes</span>';
            if (item.indexes != null)
            {
                src += '<ul>';
                for(var i in item.indexes)
                    src += '<li class="index"><span>' + item.indexes[i] + '</span></li>';
                src += '</ul>';
            }
            src += '</ul>';
            src += '</li>';
        }
        src += '</ul>';
    }
    src += '</li>';

    // Adding Exports
    src += '<li class="folder closed"><span>Exports</span>';
    if (Object.size(connection.Metadata['exports']) > 0)
    {
        src += '<ul>';
        for(var k in connection.Metadata['exports'])
        {
            src += '<li class="export"><span>' + k + '</span></li>';
        }
        src += '</ul>';
    }
    src += '</li>';

    // Adding Programmability
    src += '<li class="folder closed"><span>Programmability</span>';
    src += '<ul>';

    // Stored Procedures
    src += '<li class="folder closed"><span>Stored Procedures</span>';
    src += '<ul id="' + connection.Key + '_sp">';
    src += '<li class="folder closed"><span>System Stored Procedures</span>';
    src += '<ul>';
    for(var k in connection.Metadata['sysprocs'])
    {
        for (var paramCount in connection.Metadata['sysprocs'][k]) {
            src += '<li class="procedure closed"><span>' + k + '(' + paramCount +')</span>';
            src += '<ul>'
            src += '<li class="folder closed"><span>Parameters</span>';
            src += '<ul>'
            for(var i = 0; i < connection.Metadata['sysprocs'][k][paramCount].length-1; i++)
                src += '<li class="paramin"><span>' + connection.Metadata['sysprocs'][k][paramCount][i] + '</span></li>';
            src += '<li class="paramreturn"><span>' + connection.Metadata['sysprocs'][k][paramCount][i] + '</span></li>';
            src += '</ul>';
            src += '</li>';
            src += '</ul>';
            src += '</li>';
        }
    }
    src += '</ul>';
    src += '</li>'; // System Stored Procedures

    // User Procedures
    for (var i = 0; i < connection.Metadata['procedures'].data.length; ++i)
    {
        var connTypeParams = [];
        var procParams = [];
        var procName = connection.Metadata['procedures'].data[i][2];
        for (var p = 0; p < connection.Metadata['procedurecolumns'].data.length; ++p)
        {
            if (connection.Metadata['procedurecolumns'].data[p][2] == procName)
            {
                paramType = connection.Metadata['procedurecolumns'].data[p][6];
                paramName = connection.Metadata['procedurecolumns'].data[p][3];
                paramOrder = connection.Metadata['procedurecolumns'].data[p][17] - 1;
                if (connection.Metadata['procedurecolumns'].data[p][12] == "ARRAY_PARAMETER")
                {
                    if (paramType.toLowerCase() == "tinyint") // ENG-2040 and ENG-3101, identify it as an array (byte[])
                        paramType = "byte[]";
                    else
                        paramType += "_array";
                }
                procParams[paramOrder] = {'name': paramName, 'type': paramType.toLowerCase()};
            }
        }

        src += '<li class="procedure closed"><span>' + procName + '</span>';
          src += '<ul>'
            src += '<li class="folder closed"><span>Parameters</span>';
              src += '<ul>'
                for (var p = 0; p < procParams.length; ++p) {
                    src += '<li class="paramin"><span>Param' + (p) + ' (' + procParams[p].type + ')</span></li>';
                }
                src += '<li class="paramreturn"><span>Return Table[]</span></li>';
              src += '</ul>'
            src += '</li>'
          src += '</ul>'
        src += '</li>'
    }

    src += '</ul>'; // connection.Key_sp
    src += '</li>'; // Stored Procedures

    // Types
    src += '<li class="folder closed"><span>Types</span><ul>';
    src += '<li class="folder closed"><span>Exact Numerics</span><ul>';
    var list = ['tinyint','smallint','int','bigint','decimal'];
    for(var k in list)
        src += '<li class="type"><span>' +  list[k] + '</span></li>';
    src += '</ul></li>';
    src += '<li class="folder closed"><span>Approximate Numerics</span><ul>';
        src += '<li class="type"><span>float</span></li>';
    src += '</ul></li>';
    src += '<li class="folder closed"><span>Date and Time</span><ul>';
        src += '<li class="type"><span>timestamp</span></li>';
    src += '</ul></li>';
    src += '<li class="folder closed"><span>Character Strings (UTF-8)</span><ul>';
        src += '<li class="type"><span>varchar</span></li>';
    src += '</ul></li>';
    src += '<li class="folder closed"><span>Extended Types</span><ul>';
        src += '<li class="folder closed"><span>Numerics</span><ul>';
            src += '<li class="type"><span>bit</span></li>';
        src += '</ul></li>';
        src += '<li class="folder closed"><span>Character String Enumerations</span><ul>';
            src += '<li class="type closed"><span>CatalogComponent</span><ul>';
                list = ['COLUMNS','INDEXINFO','PRIMARYKEYS','PROCEDURECOLUMNS','PROCEDURES','TABLES'];
                for(var k in list)
                    src += '<li class="empty"><span>' + list[k] + '</span></li>';
            src += '</ul></li>';
            src += '<li class="type closed"><span>StatisticsComponent</span><ul>';
                list = ['INDEX','INITIATOR','IOSTATS','MANAGEMENT','MEMORY','PROCEDURE','TABLE','PARTITIONCOUNT','STARVATION','LIVECLIENTS'];
                for(var k in list)
                    src += '<li class="empty"><span>' + list[k] + '</span></li>';
            src += '</ul></li>';
            src += '<li class="type closed"><span>SysInfoSelector</span><ul>';
                list = ['OVERVIEW','DEPLOYMENT'];
                for(var k in list)
                    src += '<li class="empty"><span>' + list[k] + '</span></li>';
            src += '</ul></li>';
        src += '</ul></li>';
        src += '<li class="folder closed"><span>Character Strings</span><ul>';
            src += '<li class="type"><span>xml</span></li>';
        src += '</ul></li>';
    src += '</ul></li>';

    src += '</ul>';

    src += '</ul></ul>';
    var b = $(src).appendTo('#dbbrowser');
    $('#dbbrowser').treeview({add: b});
    $('#' + connection.Key).click(function(event,ui){$('.treeview li.database.selected').removeClass('selected'); $(this).addClass('selected');});
    $('#' + connection.Key).click();
    $('#' + connection.Key).contextMenu({ menu: 'objectbrowsermenu'},
        function(action, el, pos)
        {
            $('.treeview li.database.selected').removeClass('selected');
            $('#' + connection.Key).addClass('selected');
            switch(action)
            {
                case 'new-query':
                    $('#new-query').click();
                    break;
                case 'new-monitor':
                    $('#new-monitor').click();
                    break;
                case 'disconnect':
                    VoltDB.RemoveConnection(VoltDB.GetConnection($(el).attr('id')), MainUI.RemoveConnection);
                    break;
            }
    });
    MainUI.RefreshDataSources($('#worktabs select.datasource'));
    if (MainUI.OnAddConnectionCompleted != null)
        MainUI.OnAddConnectionCompleted(connection);
}

this.ActiveConnection = function()
{
    return VoltDB.GetConnection($('.treeview li.database.selected').attr('id'));
}

this.RemoveConnection = function(connection)
{
    $('#' + connection.Key).remove();
    MainUI.RefreshDataSources($('#worktabs select.datasource'));
    $('.treeview li.database.selected').removeClass('selected');
    $('.treeview li.database:first').addClass('selected');
}
this.RefreshDataSources = function(datasources)
{
    if (datasources.length == 0)
        return;
    for(var i = 0; i < datasources.length; i++)
    {
        var options = $(datasources[i]).find('option');
        for(var j = 0; j < options.length; j++)
            if ($(options[j]).attr('value') != 'Disconnected' && !VoltDB.HasConnectionKey($(options[j]).attr('value')))
                $(options[j]).remove();

        var present = false;
        for(var j = 0; j < options.length; j++)
        {
            if ($(options[j]).attr('value') == 'Disconnected')
            {
                present = true;
                break;
            }
        }
        if (!present)
            $(datasources[i]).append($('<option></option>').attr('value', 'Disconnected' ).text('Disconnected'));

        for(var key in VoltDB.Connections)
        {
            present = false;
            for(var j = 0; j < options.length; j++)
            {
                if ($(options[j]).attr('value') == key)
                {
                    present = true;
                    break;
                }
            }
            if (!present)
                $(datasources[i]).append($('<option></option>').attr('value', key ).text(VoltDB.Connections[key].Display));
        }
    }
}
this.WindowSize = null;
this.ToggleFullScreen = function()
{
    if (this.WindowSize == null)
    {
        if (isNaN(window.screenLeft))
            this.WindowSize = [window.screenX, window.screenY, window.outerWidth, window.outerHeight];
        else
            this.WindowSize = [window.screenLeft, window.screenTop, window.outerWidth, window.outerHeight];
        window.moveTo(0,0);
        window.resizeTo(screen.availWidth,screen.availHeight);
    }
    else
    {
        window.resizeTo(this.WindowSize[2],this.WindowSize[3]);
        window.moveTo(this.WindowSize[0],this.WindowSize[1]);
        this.WindowSize = null;
    }
}
});
window.MainUI = MainUI = new IMainUI();
})(window);

