﻿
var ispopupRevoked = false;
var table = '';
var isMobile = false; //initiate as false
// device detection
if(/(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|Android|Silk|lge |maemo|midp|mmp|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino/i.test(navigator.userAgent)
    || /1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i.test(navigator.userAgent.substr(0,4))) isMobile = true;

var is_iPad = navigator.userAgent.match(/iPad/i) != null;
$(document).ready(function () {
    $("#helppopup").load("help.htm", function () {
    });

    //clear the localStorage for DataTables in DR Section

    var tmp = [];
    for(var i=0, len=localStorage.length; i<len; i++) {
        tmp.push(localStorage.key(i))
    }

    for(var i=0, len=tmp.length; i<len; i++) {
        var key = tmp[i];
        var value = localStorage[key];
        if(key != 'queries' && key != 'queryNameList' && key != 'key' ){
                if(value != undefined){
                data = $.parseJSON(value);
                }
                if(!data.hasOwnProperty('time')){
                    if($.cookie('sessionCookie') == undefined){
                        localStorage.removeItem(key)
                    }
                } else {
                    if(data['time'] == '0' ){
                        if($.cookie('sessionCookie') == undefined){
                            localStorage.removeItem(key)

                        }
                    } else {
                        var oneDay = 24*60*60*1000; // hours*minutes*seconds*milliseconds
                        var sessionStartTime = new Date(data['time']);
                        var currentTime = new Date();
                        var difference = Math.round(Math.abs((sessionStartTime.getTime() - currentTime.getTime())/(oneDay)));

                    if(difference >= 365){
                        localStorage.removeItem(key)
                    }
                }
            }
        }
    }


    $.cookie('sessionCookie', 'true')
    var rv = -1;
    if (VoltDbUI.getCookie("username") != undefined && VoltDbUI.getCookie("username") != 'null') {
        $("#logOut").css('display', 'block');
    } else {
        $("#logOut").css('display', 'none');
    }

    //search text clean up required for mozilla
    $("#filterDatabaseTable").val('');
    $("#filterStoredProc").val('');

    //Prevent scrolling of page.
    $('#navSchema').on("click", function (e) {
        //Browser validation for IE version less than IE 11
        if (navigator.userAgent.indexOf('MSIE') >= 0) {
            window.location.hash = "#o"; //This is required for IE.
            setTimeout(function () {
                window.scrollTo(0, 0);
            }, 10);
        }

            //IE 11 is just a preview release.
            //Hence validation expressions may differs after the full version is released
            //In such case, below validation has to be updated
        else if (navigator.appName == 'Netscape') {
            var ua = navigator.userAgent;
            var re = new RegExp("Trident/.*rv:([0-9]{1,}[\.0-9]{0,})");

            if (re.exec(ua) != null) {
                rv = parseFloat(RegExp.$1);
                if (rv == 11) {
                    window.location.hash = "#o"; //This is required for IE.
                    setTimeout(function () {
                        window.scrollTo(0, 0);
                    }, 10);
                }
            }
        }
        e.preventDefault();
    });

    try {
        var savedData = getParameterByName("data");

        if (savedData != undefined && savedData != "") {
            var json = jQuery.parseJSON(decodeURIComponent(savedData));

            if (json["DisplayPreferences"] != undefined && json["DisplayPreferences"] != "")
                saveInLocalStorage("user-preferences", json["DisplayPreferences"]);

            if (json["GraphView"] != undefined && json["GraphView"] != "")
                saveInLocalStorage("graph-view", json["GraphView"]);

            if (json["CurrentServer"] != undefined && json["CurrentServer"] != "")
                saveCurrentServer(json["CurrentServer"]);

            if (json["username"] != undefined && json["username"] != "")
                saveSessionCookie("username", json["username"]);

            if (json["password"] != undefined && json["password"] != "")
                saveSessionCookie("password", json["password"]);

            if (json["AlertThreshold"] != undefined && json["AlertThreshold"] != "")
                saveInLocalStorage("alert-threshold", json["AlertThreshold"])

            if (json["tab"] == "admin") {
                saveSessionCookie("current-tab", NavigationTabs.Admin);
            }

            location.href = location.href.split("?")[0];
        }
    } catch (e) {/*Ignore error.*/ }

    // Toogle Server popup
    $('#btnPopServerList').click(function (event) {

        $("#popServerSearch").val('');
        $("#popServerSearch").attr('placeholder', 'Search Server');
        $('#popServerSearch').keyup();
        event.stopPropagation();
        if ($('#popServerList').css('display') == 'none') {
            $(this).removeClass('showServers');
            $(this).addClass('hideServers');
        } else {
            $(this).removeClass('hideServers');
            $(this).addClass('showServers');

        };
        $('#popServerList').toggle('slide', '', 1500);


        $("#wrapper").click(function () {
            if ($('#popServerList').css('display') == 'block') {
                $('#popServerList').hide();
                $('#btnPopServerList').removeClass('hideServers');
                $('#btnPopServerList').addClass('showServers');
            }

        });
    });

    $("#popServerList").on("click", function (event) {
        event.stopPropagation();
    });

    // Pop Slide Server Search
    $('#popServerSearch').keyup(function () {
        var searchText = $(this).val().toLowerCase();
        $('#serversList > tbody > tr.filterClass').each(function () {
            var currentLiText = $(this).text().toLowerCase(),
                showCurrentLi = currentLiText.indexOf(searchText) !== -1;
            $(this).toggle(showCurrentLi);
        });
    });

    // Implements Scroll in Server List div
    $('#serverListWrapper').slimscroll({
        disableFadeOut: true,
        height: '225px'
    });

    // Tabs Default Action
    $(".tab_content").hide(); //Hide all content
    $("ul.tabs li:first").addClass("active").show(); //Activate first tab
    $(".tab_content:first").show(); //Show first tab content

    // Show Hide Graph Block
    $('#showHideGraphBlock').click(function () {
        var userPreferences = getUserPreferences();
        if (userPreferences != null) {
            if (userPreferences['ClusterLatency'] != false || userPreferences['ClusterTransactions'] != false || userPreferences['ServerCPU'] != false || userPreferences['ServerRAM'] != false || userPreferences["PartitionIdleTime"] != false || userPreferences["CommandLogStat"] != false || userPreferences["CommandLogTables"] != false) {
                var graphState = $("#mainGraphBlock").css('display');
                if (graphState == 'none') {
                    $(".showhideIcon").removeClass('collapsed');
                    $(".showhideIcon").addClass('expanded');
                } else {
                    $(".showhideIcon").removeClass('expanded');
                    $(".showhideIcon").addClass('collapsed');
                }
                $('#mainGraphBlock').slideToggle();

                MonitorGraphUI.UpdateCharts();
            }
        }
    });

    //DR Show/hide toggle
    // Show Hide Graph Block
    $('#showHideDrBlock').click(function () {
        $(".drShowHide").toggle();
        var $this = $(this);
        var drState = $(".drShowHide").css('display');
        if (drState == 'none') {
            $this.removeClass('expanded');
            $this.addClass('collapsed');

        } else {
            $this.removeClass('collapsed');
            $this.addClass('expanded');

        }


    });

    // Show Hide Command Log Performance Block
    $('#showHideCLPBlock').click(function () {
        $(".clpShowHide").toggle();
        var $this = $(this);
        var clpState = $(".clpShowHide").css('display');
        if (clpState == 'none') {
            $this.removeClass('expanded');
            $this.addClass('collapsed');

        } else {
            $this.removeClass('collapsed');
            $this.addClass('expanded');

        }


    });

    // Shows memory alerts
    $('#showMemoryAlerts').popup();
    $('.drWarning').popup();

    //Logout popup
    $('#logOut').popup();
    $('#btnlogOut').popup();

    //Shows Save Snapshot status
    $('#btnSaveSnapshotPopup').popup({
        open: function (event, ui, ele) {
            if ($('#saveSnapshotStatus').html().indexOf("Snapshot queued successfully") > -1) {
                $("#imgSaveSnapshotStatus").hide();
            } else {
                $("#imgSaveSnapshotStatus").show();
            }
        }
    });

    // Filters Stored Procedures
    $('#filterStoredProc').keyup(function () {
        var that = this;
        $.each($('.storeTbl tbody tr'),
        function (i, val) {
            if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                $('.storeTbl tbody tr').eq(i).hide();
            } else {
                $('.storeTbl tbody tr').eq(i).show();
            }
        });
    });

    // Filters Database Tables
    $('#filterDatabaseTable').keyup(function () {
        var that = this;
        $.each($('.dbTbl tbody tr'),
        function (i, val) {
            if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                $('.dbTbl tbody tr').eq(i).hide();
            } else {
                $('.dbTbl tbody tr').eq(i).show();
            }
        });
    });

    var refreshCss = function () {
        //Enable Schema specific css only for Schema Tab.
        if (VoltDbUI.CurrentTab == NavigationTabs.Schema) {
            $('#styleBootstrapMin').removeAttr('disabled');
            $('#styleBootstrapResponsive').removeAttr('disabled');
            $('#styleThemeBootstrap').removeAttr('disabled');
            $("#nav").css("left", navLeft - 100);

            $('#styleBootstrapMin').removeProp("disabled");
            $('#styleBootstrapResponsive').removeProp("disabled");
            $('#styleThemeBootstrap').removeProp("disabled");
        } else {
            $("#styleBootstrapMin").attr('disabled', 'disabled');
            $("#styleBootstrapResponsive").attr('disabled', 'disabled');
            $("#styleThemeBootstrap").attr('disabled', 'disabled');
            $("#nav").css("left", navLeft);

            //This is required for firefox
            $('#styleBootstrapMin').prop("disabled", true);
            $('#styleBootstrapResponsive').prop("disabled", true);
            $('#styleThemeBootstrap').prop("disabled", true);
        }
    };



    refreshCss();
    var navLeft = $("#nav").css("left");
    $("#nav li").click(function () {
        $('.contents').hide().eq($(this).index()).show();
        $("#nav li").removeClass('active');
        $(this).addClass('active');
        if(isMobile==false){
               if(!is_iPad){
                   if(VoltDbUI.gutterInstanceHor != undefined)
                        VoltDbUI.gutterInstanceHor.destroy();
                    VoltDbUI.gutterInstanceHor = Split(['#a', '#inputQuery'],{
                        gutterSize:15,
                        minSize:380,
                        sizes:[25, 75]
                    });
               }

                   var queryLength = $("#ulTabList li").length -1;
                   if(VoltDbUI.gutterInstanceVer != undefined)
                        VoltDbUI.gutterInstanceVer.destroy()
                   VoltDbUI.gutterInstanceVer = Split(['#querybox-'+ queryLength, '#blockContainer' + queryLength], {
                       direction: 'vertical',
                       sizes: [30, 70],
                       gutterSize: 15,
                       minSize: [120, 150]
                   })


        }

        VoltDbUI.CurrentTab = getCurrentTab();
        refreshCss();
        saveSessionCookie("current-tab", VoltDbUI.CurrentTab);

        //Activate Shortcut keys only if the current tab is "SQL Query".
        //Also show proper help contents as per the choosen tab.
        if (VoltDbUI.CurrentTab == NavigationTabs.SQLQuery) {

            $("#VDBMonHelp").hide();
            $("#VDBSchHelp").hide();
            $("#VDBQHelp").show();
            $("#VDBAdmHelp").hide();
            $("#showMyHelp").html("SQL Query Help");

            if (navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1) {
                shortcut.add("f6", function () {
                    var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
                    var element_id = element.split('-')[1]
                    var btn_id = "#runBTn-" + element_id

                    if ($(btn_id).attr('disabled') != "disabled")
                        $(btn_id).button().click();
                });


            } else {
                shortcut.add("F5", function () {
                    var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
                    var element_id = element.split('-')[1]
                    var btn_id = "#runBTn-" + element_id

                    if ($(btn_id).attr('disabled') != "disabled")
                        $(btn_id).button().click();
                });
            }
        } else {

            //Refresh the charts if the current tab is "DB Monitor"
            if (VoltDbUI.CurrentTab == NavigationTabs.DBMonitor) {
                $("#showMyHelp").html("DB Monitor Help");
                $("#VDBMonHelp").show();
                $("#VDBSchHelp").hide();
                $("#VDBQHelp").hide();
                $("#VDBAdmHelp").hide();

                MonitorGraphUI.UpdateCharts();
            }
            else if (VoltDbUI.CurrentTab == NavigationTabs.Schema) {
                $("#showMyHelp").html("Schema Help");
                $("#VDBMonHelp").hide();
                $("#VDBSchHelp").show();
                $("#VDBQHelp").hide();
                $("#VDBAdmHelp").hide();
            }
            else if (VoltDbUI.CurrentTab == NavigationTabs.Admin) {
                $("#showMyHelp").html("Admin Help");
                $("#VDBMonHelp").hide();
                $("#VDBSchHelp").hide();
                $("#VDBQHelp").hide();
                $("#VDBAdmHelp").show();
            } else if (VoltDbUI.CurrentTab ==  NavigationTabs.DR){
                $("#showMyHelp").html("DR Help");
                $("#VDBMonHelp").show();
                $("#VDBSchHelp").hide();
                $("#VDBQHelp").hide();
                $("#VDBAdmHelp").hide();

                MonitorGraphUI.UpdateCharts();
            }

            shortcut.remove("f5");
            shortcut.remove("f6");
        }
    });

    //Attach the login popup to the page.
    $("body").append(voltDbRenderer.GetLoginPopup());

    var serverName = VoltDBConfig.GetDefaultServerIP();
    var portid = VoltDBConfig.GetPortId();

    //If security is enabled, then it displays login popup. After user is verified, it calls loadPage().
    //If security is not enabled, then it simply calls loadPage().
    voltDbRenderer.HandleLogin(serverName, portid, function () {
        loadPage(serverName, portid);
    });

    window.onscroll = function () {
        if (VoltDbUI.isSchemaTabLoading && VoltDbUI.CurrentTab == NavigationTabs.Schema)
            window.scrollTo(0, 0);
    };
});


function convertArrayOfObjectsToCSV(args) {
    var result, ctr, keys, columnDelimiter, lineDelimiter, data;

    data = args.data || null;
    if (data == null || !data.length) {
        return null;
    }

    columnDelimiter = args.columnDelimiter || ',';
    lineDelimiter = args.lineDelimiter || '\n';

    keys = Object.keys(data[0]);

    result = '';
    result += keys.join(columnDelimiter);
    result += lineDelimiter;

    data.forEach(function(item) {
        ctr = 0;
        keys.forEach(function(key) {
            if (ctr > 0) result += columnDelimiter;
                if (key == "timestamp"){
                    result += new Date(item[key]).getHours() + ":" + new Date(item[key]).getMinutes() + ":" + new Date(item[key]).getSeconds()
                }
                else
                    result += item[key];

            ctr++;
        });
        result += lineDelimiter;
    });

    return result;
}

function downloadCSV(event,args,whichChart) {
    if (navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1 ||
    navigator.userAgent.indexOf('MSIE') > 0 || navigator.userAgent.indexOf('Trident/') > 0 ) {
        event.preventDefault()
        event.stopPropagation()
        return;
    }

    var data, filename, link;
    var graphView = $("#graphView").val()
    var chartData = {}

    if (whichChart == "cpu"){
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.cpuDetails)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.cpuDetailsMin)
        }
        else if (graphView == "Days"){
            chartData = JSON.parse(localStorage.cpuDetailsDay)
        }

    }
    else if (whichChart == "memory"){
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.memoryDetails)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.memoryDetailsMin)
        }
        else if (graphView == "Days"){
            chartData = JSON.parse(localStorage.memoryDetailsDay)
        }
    }
    else if (whichChart == "transaction"){
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.transDetails)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.transDetailsMin)
        }
        else if (graphView == "Days"){
            chartData = JSON.parse(localStorage.transDetailsDay)
        }

    }
    else if (whichChart == "latency"){
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.latency)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.latencyMin)
        }
        else if (graphView == "Days"){
            chartData = JSON.parse(localStorage.latencyDay)
        }

    }
    else if (whichChart == "partitionIdle"){
        if (graphView == "Seconds"){
            chartData = convertPartitionData(JSON.parse(localStorage.partitionDetails))
        }
        else if (graphView == "Minutes"){
            chartData = convertPartitionData(JSON.parse(localStorage.partitionDetailsMin))
        }
        else if (graphView == "Days"){
            chartData = convertPartitionData(JSON.parse(localStorage.partitionDetailsDay))
        }

    }
    else if (whichChart == "dataReplication"){
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.drDetails)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.drDetailsMin)
        }
        else if (graphView == "Days"){
            chartData = JSON.parse(localStorage.drDetailsDay)
        }
    }
    else if (whichChart == "commandLog"){
        var overLayData = convertOverlayData(JSON.parse(localStorage.SnapshotOverlayData))
        if (graphView == "Seconds"){
            chartData = JSON.parse(localStorage.cmdLog)
        }
        else if (graphView == "Minutes"){
            chartData = JSON.parse(localStorage.cmdLogMin)
        }
        else if (graphView == "Days"){
            chartData =  JSON.parse(localStorage.cmdLogDay)
        }
    }

    var csv = convertArrayOfObjectsToCSV({
        data: chartData
    });
    if (csv == null) return;

    filename = args.filename + "-" + graphView + ".csv";
    data = encodeURI(csv);

    var blob = new Blob([csv], {type: "text/csv;"});
    saveAs(blob, filename );
}

function convertPartitionData(data){
    var chartData = [];
    for (var i=0; i< data.length; i++){
        for(var j=0 ; j< data[i].values.length; j++){
            chartData.push({
            "key": data[i].key.replace(" ", ""),
            "type": getPartitionType(data[i].key, data[i].color),
            "timestamp": data[i].values[j].x,
            "value": data[i].values[j].y
        })
        }
    }
    return chartData;
}

function getPartitionType(key, color){
    var type = "local-partitiion"
    if(color == MonitorGraphUI.enumPartitionColor.maxMinPartition){
        if(key.substr(key.length-4, 3) == "Max"){
            type = "maximum-partition"
        } else {
            type = "minimum-partition"
        }
    } else if(color ==  MonitorGraphUI.enumPartitionColor.multiPartition){
        type = "multi-partition"
    }
    return type
}

function convertOverlayData(data){
     var chartData = [];
     for (var i=0; i< data.length; i++){
        var startTime = new Date(data[i].startTime);
        var endTime = new Date(data[i].endTime);
        var starthours = startTime.getHours();
        var startminutes = "0" + startTime.getMinutes();
        var startseconds = "0" + startTime.getSeconds();
        var startmilliseconds = startTime.getMilliseconds() < 100 ? "0" + startTime.getMilliseconds() : startTime.getMilliseconds();
        var startformattedTime = starthours + ':' + startminutes.substr(-2) + ':' + startseconds.substr(-2) + ':' + startmilliseconds;
        var endhours = endTime.getHours();
        var endminutes = "0" + endTime.getMinutes();
        var endseconds = "0" + endTime.getSeconds();
        var endmilliseconds = endTime.getMilliseconds();
        var endformattedTime = endhours + ':' + endminutes.substr(-2) + ':' + endseconds.substr(-2) + ':' + endmilliseconds;

        chartData.push({
        "startTime": startformattedTime,
        "endTime": endformattedTime
        })
     }

    return chartData

}

function logout() {
    saveSessionCookie("username", null);
    saveSessionCookie("password", null);
    saveSessionCookie("current-tab", NavigationTabs.DBMonitor);
    $('#logOut').prop('title', '');
    location.reload(true);
};

function changePassword(obj) {
    var id = obj.id;
    $(obj).css('display', 'none');
    $(obj.parentElement).children('input').each(function () {
        $(this).css('display', 'inline-block'); // "this" is the current element in the loop
    });
}

var loadPage = function (serverName, portid) {
    $(".drShowHide").show();
    $(".clpShowHide").show();
    $("#showHideDrBlock").removeClass('collapsed');
    $("#showHideDrBlock").addClass('expanded');
    $("#showHideCLPBlock").removeClass('collapsed');
    $("#showHideCLPBlock").addClass('expanded');

    var userName = VoltDbUI.getCookie('username') != undefined ? VoltDbUI.getCookie('username') : "";
    var password = VoltDbUI.getCookie('password') != undefined ? VoltDbUI.getCookie('password') : "";

    voltDbRenderer.ChangeServerConfiguration(serverName, portid, userName, password, true, true);
    voltDbRenderer.ShowUsername(userName);

    loadSQLQueryPage(serverName, portid, userName);
    VoltDbUI.refreshSqlAndSchemaTab();

    VoltDbUI.partitionGraphInterval = setInterval(function () {
        if (getCurrentServer() != undefined) {
            window.clearInterval(VoltDbUI.partitionGraphInterval);
            voltDbRenderer.GetPartitionIdleTimeInformation(function (partitionDetail) {
                MonitorGraphUI.GetPartitionDetailData(partitionDetail);
            });
        }
    }, 5000);

    showEnableDisableDownloadBtn()
    function showEnableDisableDownloadBtn(){
         if ((navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1) ||
         navigator.userAgent.indexOf('MSIE') > 0 || navigator.userAgent.indexOf('Trident/') > 0 ) {
            $(".downloadCls").attr("src","css/resources/images/icon_download_disabled.png");
            $(".downloadCls").attr("title","Download file feature is not supported in this browser.")
            $(".downloadCls").css( 'cursor', 'default' );

         } else {
            $(".downloadCls").attr("src","css/resources/images/downloadBtn.png");
            $(".downloadCls").attr("title","Download data as CSV")
            $(".downloadCls").css( 'cursor', 'pointer' );
         }
    }

    var showAdminPage = function () {
        if (!VoltDbAdminConfig.isAdmin) {
            VoltDbAdminConfig.isAdmin = true;

            if (VoltDbUI.getCookie("sql_port_for_paused_db") == sqlPortForPausedDB.UseAdminPort) {
                VoltDBService.SetConnectionForSQLExecution(true);
                SQLQueryRender.saveConnectionKey(true);
            }

            $("#navAdmin").show();
            loadAdminPage();
        }

    };

    //Retains the current tab while page refreshing.
    var retainCurrentTab = function () {

        if (!(securityChecks.securityChecked && securityChecks.previlegesChecked))
            return;
        var curTab = VoltDbUI.getCookie("current-tab");
        if (curTab != undefined) {
            curTab = curTab * 1;
            if (curTab == NavigationTabs.Schema) {
                $("#overlay").show();
                setTimeout(function () { $("#navSchema > a").trigger("click"); }, 100);
            } else if (curTab == NavigationTabs.SQLQuery) {
                $("#overlay").show();
                setTimeout(function () { $("#navSqlQuery > a").trigger("click"); }, 100);
            } else if (curTab == NavigationTabs.Admin) {
                if (VoltDbAdminConfig.isAdmin) {
                    $("#overlay").show();
                    setTimeout(function () { $("#navAdmin > a").trigger("click"); }, 100);
                } else {
                    saveSessionCookie("current-tab", NavigationTabs.DBMonitor);
                }
            }
        }
    };

    var securityChecks = {
        securityChecked: false,
        previlegesChecked: false
    };

    //Load Admin configurations
    voltDbRenderer.GetAdminDeploymentInformation(true, function (adminConfigValues, rawConfigValues) {
        securityChecks.securityChecked = true;

        //Show admin page if security is turned off.
        if (adminConfigValues != null && adminConfigValues.VMCNoPermission != true && !adminConfigValues.security) {
            showAdminPage();
        } else if (!VoltDbAdminConfig.isAdmin) {
            $("#navAdmin").hide();
        }

        retainCurrentTab();
    });


    voltDbRenderer.CheckAdminPriviledges(function (hasAdminPrivileges) {
        securityChecks.previlegesChecked = true;

        if (hasAdminPrivileges) {
            showAdminPage();
        } else if (!VoltDbAdminConfig.isAdmin) {
            $("#navAdmin").hide();
        }

        retainCurrentTab();
    });

    $("#loginWarnPopup").popup({
        afterOpen: function (event, ui, ele) {
            var popup = $(this)[0];

            $("#btnLoginWarningOk").unbind("click");
            $("#btnLoginWarningOk").on('click', function () {

                if (!VoltDbUI.hasPermissionToView) {
                    location.reload(true);
                } else {
                    if (VoltDbUI.CurrentTab == NavigationTabs.Admin) {
                        setTimeout(function () {
                            $("#navDbmonitor").trigger("click");
                        }, 500);
                    }

                    $("#navAdmin").hide();
                }
                popup.close();
            });
        },
        closeContent: '',
        modal: true
    });

    var defaultSearchTextProcedure = 'Search Stored Procedures';
    var defaultSearchTextTable = 'Search Database Tables';

    var currentProcedureAction = VoltDbUI.ACTION_STATES.NONE;
    var priorProcedureAction = VoltDbUI.ACTION_STATES.NONE;
    var currentTableAction = VoltDbUI.ACTION_STATES.NONE;
    var priorTableAction = VoltDbUI.ACTION_STATES.NONE;
    VoltDbUI.CurrentTab = getCurrentTab();

    RefreshServerUI();

    var version = "";
    var setVersionCheckUrl = function (currentServer) {
        if (version == "") {
            version = voltDbRenderer.getVersion(currentServer);
            $('#versioncheck').attr('src', 'http://community.voltdb.com/versioncheck?app=vmc&ver=' + version);
        }
    };

    var refreshClusterHealth = function () {
        //loads cluster health and other details on the top banner
        var loadClusterHealth = function () {
            voltDbRenderer.GetClusterHealth(function (htmlData, alertHtmlData) {
                $("#clusterHealth").html(htmlData).show();
                $("#memoryAlertsList").html(alertHtmlData);
            });

            voltDbRenderer.mapNodeInformationByStatus(function (htmlData) {

                var currentServer = getCurrentServer();
                if (currentServer == undefined) {
                    saveCurrentServer(htmlData.ServerInformation[1].CurrentServer);
                }

                $(".activeServerName").html(htmlData.ServerInformation[1].CurrentServer).attr('title', htmlData.ServerInformation[1].CurrentServer);
                $("#serversList").html(htmlData.ServerInformation[0].ServersList);
                setVersionCheckUrl(htmlData.ServerInformation[1].CurrentServer);

                //Trigger search on the newly loaded list. This is required to
                //search server since we are refreshing the server list.
                if ($("#popServerSearch").val() != "Search Server")
                    $("#popServerSearch").trigger("keyup");
            });

            //hide loading icon
            $("#overlay").hide();

            $('#serversList >  tbody > tr > td.active > a').click(function () {
                var clickedServer = $(this).html();
                $('.activeServerName').html(clickedServer).attr('title', clickedServer);

                if ($('#popServerList').css('display') == 'none') {
                    $('#btnPopServerList').removeClass('showServers');
                    $('#btnPopServerList').addClass('hideServers');
                } else {
                    $('#btnPopServerList').removeClass('hideServers');
                    $('#btnPopServerList').addClass('showServers');

                };
                $('#popServerList').toggle('slide', '', 1500);
                $(this).parent().prevAll().removeClass('monitoring');
                $(this).parent().nextAll().removeClass('monitoring');
                $(this).parent().addClass('monitoring');


                var serverIp = voltDbRenderer.getServerIP($(this).attr('data-ip'));
                var currentUrl = window.location.href.split('?')[0];
                var urlArray = currentUrl.split('/');
                var newUrl = '';
                if (urlArray != null && urlArray.length > 0) {
                    var urlArray2 = urlArray[2].split(':');
                    urlArray2[0] = serverIp;
                    urlArray[2] = urlArray2.join(':');
                    newUrl = urlArray.join('/');
                }
                var data = {
                    CurrentServer: clickedServer,
                    GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
                    DisplayPreferences: VoltDbUI.getFromLocalStorage("user-preferences"),
                    AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
                    username: VoltDbUI.getCookie("username"),
                    password: VoltDbUI.getCookie("password"),
                };

                var win = window.open(newUrl + '?data=' + encodeURIComponent(JSON.stringify(data)), '_parent');
                win.focus();

            });

            var lUserPreferences = getUserPreferences();
            showHideGraph(lUserPreferences);
        };
        var loadAdminTabPortAndOverviewDetails = function (portAndOverviewValues, serverSettings) {
            VoltDbAdminConfig.displayPortAndRefreshClusterState(portAndOverviewValues, serverSettings);
        };

        var loadAdminServerList = function (serverList) {
            VoltDbAdminConfig.refreshServerList(serverList);
            $(VoltDbAdminConfig.runningServerIds).on('click', function () {
                openPopup($(this));
            });

            $('.tblshutdown  >tbody > tr.activeHost > td:first-child > a').click(function () {
                var clickedServer = $(this).html();
                var serverIp = voltDbRenderer.getServerIP($(this).parent().siblings('td:first').next().find("a").attr('data-hostid'));
                var currentUrl = window.location.href.split('?')[0];
                var urlArray = currentUrl.split('/');
                var newUrl = '';
                if (urlArray != null && urlArray.length > 0) {
                    var urlArray2 = urlArray[2].split(':');
                    urlArray2[0] = serverIp;
                    urlArray[2] = urlArray2.join(':');
                    newUrl = urlArray.join('/');
                }
                var data = {
                    CurrentServer: clickedServer,
                    GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
                    DisplayPreferences: VoltDbUI.getFromLocalStorage("user-preferences"),
                    AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
                    username: VoltDbUI.getCookie("username"),
                    password: VoltDbUI.getCookie("password"),
                    tab: 'admin'
                };

                var win = window.open(newUrl + '?data=' + encodeURIComponent(JSON.stringify(data)), '_parent');
                win.focus();
            });
        };

        var openPopup = function (srcElement) {
            var i = 0;
            var hostName = srcElement.attr('data-HostName');
            var hostId = srcElement.attr('data-HostId');
            var idleServerDetails;

            var popup = new $.Popup({
                content: "#stopConfirmationPop",

                afterOpen: function () {
                    $("#StopConfirmOK").unbind("click");
                    $("#StopConfirmOK").on("click", function () {

                        //API Request
                        try {
                            voltDbRenderer.stopServer(hostId, hostName, function (success, statusString) {
                                if (success) {
                                    adminClusterObjects.ignoreServerListUpdateCount = 2;
                                    updateServers(hostId, hostName, "MISSING");
                                    $("#stopServer_" + hostName).addClass('disableServer');
                                    $("#stopServer_" + hostName + " span").addClass('shutdownServer');
                                    $("#stopServer_" + hostName + " span").addClass('stopDisable');


                                }
                                else {
                                    $('#errorLabel').text(statusString);
                                    popup.open("#divStopServerError", undefined, srcElement);

                                }

                            });

                        }
                        catch (error) {

                        }

                        //Close the popup
                        $($(this).siblings()[0]).trigger("click");

                    });

                    $("#StopConfirmCancel").unbind("click");
                    $("#StopConfirmCancel").on("click", function () {
                        popup.close();
                    });
                }

            });
            popup.open("#stopConfirmationPop", undefined, srcElement);
        };


        voltDbRenderer.GetSystemInformation(loadClusterHealth, loadAdminTabPortAndOverviewDetails, loadAdminServerList);


        //Load Admin configurations
        voltDbRenderer.GetAdminDeploymentInformation(false, function (adminConfigValues, rawConfigValues) {

            if (!VoltDbUI.hasPermissionToView)
                return;

            if (rawConfigValues.status == -3 && VoltDbAdminConfig.isAdmin) {
                VoltDbAdminConfig.isAdmin = false;
                setTimeout(function () {
                    var checkPermission = function () {

                        if (!VoltDbUI.hasPermissionToView)
                            return;
                        else
                            $("#loginWarningPopupMsg").text("Security settings have been changed. You no longer have permission to view Admin Tab.");

                        if (!$("#loginWarningPopup").is(":visible")) {
                            $("#loginWarnPopup").trigger("click");
                        }

                    };
                    voltDbRenderer.GetSystemInformation(checkPermission, function (portAndOverviewValues, serverSettings) { }, function (data) { });
                }, 2000);
            } else
                VoltDbAdminConfig.displayAdminConfiguration(adminConfigValues, rawConfigValues);
        });

        //Get System Overview information
        voltDbRenderer.GetDeploymentInformation(function (deploymentDetails) {
            if (deploymentDetails != undefined) {
                var siteCount = deploymentDetails.DETAILS.SITECOUNT;
                var hostCount = deploymentDetails.DETAILS.HOSTCOUNT;
                //check whether command log is enabled or not
                VoltDbUI.isCommandLogEnabled = deploymentDetails.DETAILS.COMMANDLOGSTATUS;
                var clusterDetails = voltDbRenderer.getClusterDetail(getCurrentServer());
                if (clusterDetails != undefined) {
                    if (clusterDetails.MODE != undefined && clusterDetails.VERSION != undefined && clusterDetails.BUILDSTRING != undefined && clusterDetails.UPTIME != undefined) {
                        $("#mode").html(clusterDetails.MODE);
                        $("#voltdbVersion").html(clusterDetails.VERSION);
                        $("#buildString").html(clusterDetails.BUILDSTRING);
                        $("#clusterComposition").html(hostCount + " hosts with " + (hostCount * siteCount) + " sites (" + siteCount + " per host)");
                        $("#runningSince").html(getRunningTimeInfo(parseInt(clusterDetails.STARTTIME), clusterDetails.UPTIME));
                        getLicenseInformation(clusterDetails.LICENSE);
                    }
                }
            }
        });

        var getRunningTimeInfo = function (startTime, upTime) {
            var strTime = new Date(startTime).toUTCString();
            var upTime1 = upTime.split(' ');
            var upTimeHrMin = upTime1[2].split(':');
            var runningSince = strTime + " (" + parseInt(upTime1[0]) + "d " + parseInt(upTimeHrMin[0]) + "h " + parseInt(upTimeHrMin[1]) + "m)";
            return runningSince;
        };

        var getLicenseInformation = function (licenseInfo) {
            if (licenseInfo != undefined && licenseInfo != "") {
                var licInfo = $.parseJSON(licenseInfo);
                $("#addNewConfigLink").show();
                $(".licenseInfo").show();
                $("#tdLicenseInfo").hide();
                $("#tdLicenseInfo").css("display", "none");
                $("#tdHostCount").html(licInfo.hostcount);
                $("#tdWanReplication").html(licInfo.wanreplication == undefined ? '' : licInfo.wanreplication.toString());
                $("#tdExpiration").html(licInfo.expiration);
                $("#tdCommandLogging").html(licInfo.commandlogging == undefined ? '' : licInfo.commandlogging.toString());
                $("#tdTrial").html(licInfo.trial == undefined ? '' : licInfo.trial.toString());
            } else {
                $("#addNewConfigLink").hide();
                $(".licenseInfo").hide();
                $("#tdLicenseInfo").show();
            }
        };
    };

    var showHideCmdlogDataAndCharts = function (cmdLogStatus, graphView, currentTab) {
        var logTableSpanSelector = $('#liCommandLogTables').find("span");
        var logTableInputSelector = $('#liCommandLogTables').find("input");
        var logStatSpanSelector = $('#liCommandLogStat').find("span");
        var logStatInputSelector = $('#liCommandLogStat').find("input");
        if (cmdLogStatus == 'true') {
            logStatSpanSelector.html('Command Log Statistics');
            logStatSpanSelector.removeClass("notActive");
            logStatInputSelector.removeAttr("disabled");
            logStatInputSelector.removeAttr("title");

            logTableSpanSelector.html('Command Log Data');
            logTableSpanSelector.removeClass("notActive");
            logTableInputSelector.removeAttr("disabled");
            logTableInputSelector.removeAttr("title");
            var userPreference = getUserPreferences();
            if (userPreference["CommandLogStat"]) {

                $("#chartCommandLogging").show();
                MonitorGraphUI.refreshGraphCmdLog();

            }
            if (userPreference["CommandLogTables"]) {
                $("#divCommandLog").show();
            }
            refreshCmdLogSection(graphView, currentTab);
        } else {
            logStatSpanSelector.html('Command Log Statistics (not active)');
            logStatSpanSelector.addClass("notActive");
            logStatInputSelector.attr("disabled", true);
            logStatInputSelector.attr("title", "This feature is not active.");
            logStatInputSelector.attr("checked", false);
            $("#chartCommandLogging").hide();
            $("#divCommandLog").hide();

            logTableSpanSelector.html('Command Log Data (not active)');
            logTableSpanSelector.addClass("notActive");
            logTableInputSelector.attr("disabled", true);
            logTableInputSelector.attr("title", "This feature is not active.");
            logTableInputSelector.attr("checked", false);
        }
    };

    var refreshGraphAndData = function (graphView, currentTab) {

        voltDbRenderer.GetExportProperties(function (rawData) {
            VoltDbAdminConfig.exportTypes = rawData;
        });

        voltDbRenderer.getMemoryGraphInformation(function (memoryDetails) {
            MonitorGraphUI.RefreshMemory(memoryDetails, getCurrentServer(), graphView, currentTab);
        });

        voltDbRenderer.getLatencyGraphInformation(function (latencyDetails) {
            MonitorGraphUI.RefreshLatency(latencyDetails, graphView, currentTab);
        });

        voltDbRenderer.GetTransactionInformation(function (transactionDetails) {
            MonitorGraphUI.RefreshTransaction(transactionDetails, graphView, currentTab);
        });

        voltDbRenderer.getCpuGraphInformation(function (cpuDetails) {
            MonitorGraphUI.RefreshCpu(cpuDetails, getCurrentServer(), graphView, currentTab);
        });

        voltDbRenderer.GetPartitionIdleTimeInformation(function (partitionDetail) {
            if (getCurrentServer() != undefined)
                MonitorGraphUI.RefreshPartitionIdleTime(partitionDetail, getCurrentServer(), graphView, currentTab);
        });

        showHideCmdlogDataAndCharts(VoltDbUI.isCommandLogEnabled, graphView, currentTab);

        voltDbRenderer.GetClusterReplicaInformation(function (replicaDetail) {
            if (getCurrentServer() != undefined) {
                $('#dRHeaderName').html(getCurrentServer())
                var isReplicaDataVisible = false;
                var isMasterDataVisible = false;
                var isDrGraphVisible = false;
                var currentServer = getCurrentServer();
                if (replicaDetail.hasOwnProperty(currentServer))
                    VoltDbUI.drReplicationRole = replicaDetail[currentServer]['status'];
                else
                    return;
                voltDbRenderer.GetDrStatusInformation(function (drDetails) {
                    if (getCurrentServer() != undefined) {
                        var drResult = drDetails["Details"]["STATUS"];
                        if (drResult != -2 && drDetails.hasOwnProperty(currentServer) && drDetails[currentServer].hasOwnProperty('MASTERENABLED')) {
                            VoltDbUI.drMasterEnabled = (drDetails[currentServer]['MASTERENABLED'] != null && drDetails[currentServer]['MASTERENABLED'] != false) ? true : false;
                            VoltDbUI.drMasterState = (drDetails[currentServer]['STATE']);
                            //show master/replica table
                            voltDbRenderer.GetDrConsumerInformation(function(drConsumerDetails){
                                if(drConsumerDetails.hasOwnProperty(currentServer) && drConsumerDetails[currentServer].hasOwnProperty('STATE'))
                                    VoltDbUI.drConsumerState = drConsumerDetails[currentServer]['STATE'];
                                else
                                    VoltDbUI.drConsumerState = 'DISABLE'

                                if(!(drDetails[currentServer]['STATE'] == 'OFF' && VoltDbUI.drConsumerState == 'DISABLE')){
                                    if (!(VoltDbUI.drReplicationRole.toLowerCase() == "none" && !VoltDbUI.drMasterEnabled)) {
                                        var userPreference = getUserPreferences();
                                        VoltDbUI.isDRInfoRequired = true;
                                        VoltDbUI.drStatus = drDetails[currentServer]['SYNCSNAPSHOTSTATE'];
                                        // showHideLastLineClass(true);
                                        $("#divDrWrapperAdmin").show();
                                        if (VoltDbUI.drReplicationRole.toLowerCase() == 'replica') {
                                            if(VoltDbUI.drConsumerState.toLowerCase() != 'disable') {
                                                $("#ChartDrReplicationRate").show();
                                                isDrGraphVisible = true;
                                                MonitorGraphUI.refreshGraphDR();
                                                $('#drReplicaSection').css('display', 'block');
                                                isReplicaDataVisible = true;
                                            }
                                            else {
                                                $('#drReplicaSection').css('display', 'none');
                                                isReplicaDataVisible = false;
                                                $("#ChartDrReplicationRate").hide();
                                                isDrGraphVisible = false;
                                            }
                                            refreshDrReplicaSection(graphView, currentTab);
                                            //to show DR Mode and DR tables
                                            if (VoltDbUI.drMasterState.toUpperCase() == 'ACTIVE') {
                                                $("#dbDrMode").text("Both");
                                                $('#drMasterSection').css('display', 'block');
                                                isMasterDataVisible = true;
                                                refreshDrMasterSection();
                                                $(".replicaWrapper").css('top', '-27px');
                                            } else {
                                                $("#dbDrMode").text("Replica");
                                                $(".replicaWrapper").css('top', '0px');
                                                $('#drMasterSection').css('display', 'none');
                                                isMasterDataVisible = false;
                                            }
                                            if(VoltDbUI.drConsumerState.toLowerCase() != 'disable' || VoltDbUI.drMasterState.toUpperCase() == 'ACTIVE'){
                                                $("#divDrReplication").show();
                                            } else {
                                                $("#divDrReplication").hide();
                                            }
                                        } else {
                                            voltDbRenderer.GetDrInformations(function (clusterInfo) {
                                                $('#clusterId').show();
                                                $('#clusterId').html(" (ID: " + clusterInfo[getCurrentServer()]['CLUSTER_ID'] + ")");
                                            });
                                            //to show DR Mode
                                            if (VoltDbUI.drMasterEnabled) {
                                                $("#dbDrMode").text("Master");
                                                $('#drMasterSection').css('display', 'block');
                                                isMasterDataVisible = true;
                                                refreshDrMasterSection();
                                            }else {
                                                isMasterDataVisible = false;
                                            }
                                            if(VoltDbUI.drMasterEnabled && VoltDbUI.drConsumerState.toLowerCase() != 'disable') {
                                                $("#ChartDrReplicationRate").show();
                                                isDrGraphVisible = true;
                                                MonitorGraphUI.refreshGraphDR();
                                                $('#drReplicaSection').css('display', 'block');
                                                isReplicaDataVisible = true;
                                                refreshDrReplicaSection(graphView, currentTab);
                                            } else {
                                                $("#ChartDrReplicationRate").hide();
                                                isDrGraphVisible = false;
                                                $('#drReplicaSection').css('display', 'none');
                                                isReplicaDataVisible = false;
                                            }

                                            if(VoltDbUI.drMasterEnabled || VoltDbUI.drConsumerState.toLowerCase() != 'disable'){
                                                $("#divDrReplication").show();
                                            }else {
                                                $("#divDrReplication").hide();
                                            }
                                        }
                                    } else {
                                        var userPreference = getUserPreferences();
                                        voltDbRenderer.GetDrInformations(function (clusterInfo) {
                                            $('#clusterId').show();
                                            $('#clusterId').html(" (ID: " + clusterInfo[getCurrentServer()]['CLUSTER_ID'] + ")");
                                        });
                                        VoltDbUI.isDRInfoRequired = true;
                                        $("#divDrReplication").hide();
                                        $("#divDrWrapperAdmin").show();
                                        if(VoltDbUI.drConsumerState.toLowerCase() != 'disable') {
                                            $("#divDrReplication").show();
                                            $('#drReplicaSection').css('display', 'block');
                                            isReplicaDataVisible = true;
                                            $("#ChartDrReplicationRate").show();
                                            isDrGraphVisible = true;
                                            MonitorGraphUI.refreshGraphDR();
                                            refreshDrReplicaSection(graphView, currentTab);
                                        } else {
                                            $("#divDrReplication").hide();
                                            $('#drReplicaSection').css('display', 'none');
                                            isReplicaDataVisible = false;
                                            $("#ChartDrReplicationRate").hide();
                                            isDrGraphVisible = false;
                                        }
                                        $('#drMasterSection').css('display', 'none');
                                        isMasterDataVisible = false;
                                    }
                                    if(isDrGraphVisible || isMasterDataVisible || isReplicaDataVisible){
                                        var curTab = VoltDbUI.getCookie("current-tab");
                                        if (curTab == NavigationTabs.DR){
                                            $("#overlay").show();
                                            setTimeout(function () { $("#navDR > a").trigger("click"); }, 100);
                                        }

                                        $('#navDR').show();
                                    }
                                } else {
                                    hideDrInformation()
                                }
                            })

                        } else {
                            hideDrInformation()
                        }
                    }
                });
            } else {
                hideDrInformation()
            }
        });

        var hideDrInformation =  function(){
            $('#navDR').hide()
            $('#clusterId').hide();
            $('#clusterId').html("");
            VoltDbUI.isDRInfoRequired = false;
            $("#divDrReplication").hide();
            $("#ChartDrReplicationRate").hide();
            $("#divDrWrapperAdmin").hide();
        }

        var loadProcedureInformations = function (procedureMetadata) {
            if ((procedureMetadata != "" && procedureMetadata != undefined)) {
                voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, function (traverse, htmlData) {
                    if (!voltDbRenderer.isProcedureSearch) {
                        if ((currentProcedureAction == VoltDbUI.ACTION_STATES.REFRESH && currentProcedureAction != VoltDbUI.ACTION_STATES.NONE) || (currentProcedureAction != VoltDbUI.ACTION_STATES.REFRESH && currentProcedureAction == VoltDbUI.ACTION_STATES.NONE)) {
                            lblTotalPages.innerHTML = voltDbRenderer.procedureDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureDataSize / voltDbRenderer.maxVisibleRows));

                        }

                        if (htmlData.SystemInformation[0] != "") {
                            if ((currentProcedureAction == VoltDbUI.ACTION_STATES.NONE || currentProcedureAction == VoltDbUI.ACTION_STATES.REFRESH) && priorProcedureAction == VoltDbUI.ACTION_STATES.NONE) //only during initial load
                                lblPrevious.innerHTML = " ".concat(1, ' ');

                            $('#storeProcedureBody').html(htmlData.SystemInformation);

                        } else {
                            lblPrevious.innerHTML = " ".concat(0, ' ');
                            lblTotalPages.innerHTML = " ".concat(0);
                            $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

                        }

                    }

                    if (currentProcedureAction == VoltDbUI.ACTION_STATES.SEARCH) {
                        priorProcedureAction = currentProcedureAction;
                    }

                    currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
                    VoltDbUI.CurrentProcedureDataProgress = VoltDbUI.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;

                });

            } else {
                lblPrevious.innerHTML = " ".concat(0, ' ');
                lblTotalPages.innerHTML = " ".concat(0);
                $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

                if (currentProcedureAction == VoltDbUI.ACTION_STATES.SEARCH) {
                    priorProcedureAction = currentProcedureAction;
                }

                currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
                VoltDbUI.CurrentProcedureDataProgress = VoltDbUI.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;

            }

        };

        voltDbRenderer.GetProceduresInfo(loadProcedureInformations);

        voltDbRenderer.getTablesInformation(function (tableMetadata) {
            if (tableMetadata != "" && tableMetadata != undefined) {
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {

                    if (!voltDbRenderer.isTableSearch) {
                        if ((currentTableAction == VoltDbUI.ACTION_STATES.REFRESH && currentTableAction != VoltDbUI.ACTION_STATES.NONE) || (currentTableAction != VoltDbUI.ACTION_STATES.REFRESH && currentTableAction == VoltDbUI.ACTION_STATES.NONE)) {
                            setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                        }

                        if (htmlData != "") {
                            if (currentTableAction == VoltDbUI.ACTION_STATES.NONE && currentTableAction == VoltDbUI.ACTION_STATES.NONE) //only during initial load
                            {
                                setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                            }
                            $('#tablesBody').html(htmlData);

                        } else {
                            setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                            $('#tablesBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");
                        }

                    }

                    if (currentTableAction == VoltDbUI.ACTION_STATES.SEARCH) {
                        priorTableAction = currentTableAction;
                    }
                    currentTableAction = VoltDbUI.ACTION_STATES.REFRESH;

                });

            } else {
                lblPreviousTable.innerHTML = " ".concat(0, ' ');
                lblTotalPagesofTables.innerHTML = " ".concat(0, ' ');
                $('#tablesBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

            }

        });

        $("#previousProcedures").unbind("click");
        $('#previousProcedures').on('click', function () {
            if (voltDbRenderer.procedureTableIndex >= 0) {
                currentProcedureAction = VoltDbUI.ACTION_STATES.PREVIOUS;
                voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, function (currentAction, htmlData) {
                    setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);

                    if (!voltDbRenderer.isProcedureSearch) {
                        $('#storeProcedureBody').html(htmlData.SystemInformation);

                    } else {
                        $('#storeProcedureBody').html(htmlData);

                    }
                    priorProcedureAction = currentProcedureAction;


                });
                currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
            }
        });

        $("#nextProcedures").unbind("click");
        $('#nextProcedures').on('click', function () {
            var isValidAction = false;

            if (voltDbRenderer.isProcedureSearch) {
                isValidAction = !((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows >= voltDbRenderer.procedureSearchDataSize);

            } else {
                isValidAction = !((voltDbRenderer.procedureTableIndex + 1) * voltDbRenderer.maxVisibleRows >= voltDbRenderer.procedureDataSize);
            }

            if (isValidAction) {
                currentProcedureAction = VoltDbUI.ACTION_STATES.NEXT;
                voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, function (currentAction, htmlData) {
                    setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);
                    if (!voltDbRenderer.isProcedureSearch) {
                        $('#storeProcedureBody').html(htmlData.SystemInformation);

                    } else {
                        $('#storeProcedureBody').html(htmlData);
                    }
                    priorProcedureAction = currentAction;

                });
                currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
            }
        });

        $("#previousTables").unbind("click");
        $('#previousTables').on('click', function () {
            if (voltDbRenderer.tableIndex >= 0) {
                currentTableAction = VoltDbUI.ACTION_STATES.PREVIOUS;
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                    $('#tablesBody').html(htmlData);
                    priorTableAction = currentTableAction;
                    currentTableAction = VoltDbUI.ACTION_STATES.REFRESH;
                });



            }

        });

        $("#nextTables").unbind("click");
        $('#nextTables').on('click', function () {
            var isValidAction = false;
            voltDbRenderer.isNextClicked = true;

            if (voltDbRenderer.isTableSearch) {
                isValidAction = !((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows >= voltDbRenderer.tableSearchDataSize);

            } else {
                isValidAction = !((voltDbRenderer.tableIndex + 1) * voltDbRenderer.maxVisibleRows >= voltDbRenderer.tableDataSize);
            }

            if (isValidAction) {
                currentTableAction = VoltDbUI.ACTION_STATES.NEXT;
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                    if (!voltDbRenderer.isTableSearch && (htmlData != undefined && htmlData != "")) {
                        $('#tablesBody').html(htmlData);
                    }

                    else if (htmlData != undefined && htmlData != "") {
                        $('#tablesBody').html(htmlData);

                    }
                    priorTableAction = currentTableAction;
                    currentTableAction = VoltDbUI.ACTION_STATES.REFRESH;
                    voltDbRenderer.isNextClicked = false;
                });


            }

        });

        $("#filterStoredProc").unbind("click");
        $('#filterStoredProc').on('click', function () {
            if (this.value == defaultSearchTextProcedure)
                this.value = '';
        });

        $("#filterStoredProc").unbind("keyup");
        $('#filterStoredProc').on('keyup', function () {
            if (this.value != '' && this.value != defaultSearchTextProcedure) {
                voltDbRenderer.isProcedureSearch = true;
                currentProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

            }
            else {
                voltDbRenderer.isProcedureSearch = false;
                currentProcedureAction = VoltDbUI.ACTION_STATES.REFRESH;
            }

            if (voltDbRenderer.isProcedureSearch) {
                voltDbRenderer.searchProcedures('', this.value, function (searchResult) {
                    priorProcedureAction = currentProcedureAction;
                    currentProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

                    if (searchResult) {
                        if (voltDbRenderer.isSortProcedures)
                            voltDbRenderer.sortProceduresByColumns(true);

                        voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, function (traverse, htmlSearchData) {
                            if (htmlSearchData != "")
                                $('#storeProcedureBody').html(htmlSearchData);

                            else
                                $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

                            //set pagination
                            setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch, voltDbRenderer.procedureSearchDataSize);

                        });

                    } else {
                        $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");
                        setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch, voltDbRenderer.procedureSearchDataSize);
                    }
                    priorProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

                });
            } else {
                voltDbRenderer.mapProcedureInformation(currentProcedureAction, priorProcedureAction, function (traverse, htmlData) {
                    if (htmlData != "")
                        $('#storeProcedureBody').html(htmlData.SystemInformation);

                    else {
                        $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");
                    }

                    setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);

                });

            }
        });

        $("#filterDatabaseTable").unbind("keyup");
        $('#filterDatabaseTable').on('keyup', function () {
            if ($('#filterDatabaseTable')[0].value != '' && $('#filterDatabaseTable')[0].value != defaultSearchTextTable) {
                voltDbRenderer.isTableSearch = true;
                voltDbRenderer.isSearchTextCleaned = false;
                currentTableAction = VoltDbUI.ACTION_STATES.SEARCH;

            } else {
                voltDbRenderer.isTableSearch = false;
                voltDbRenderer.isSearchTextCleaned = true;
                currentTableAction = VoltDbUI.ACTION_STATES.REFRESH;
            }

            if (voltDbRenderer.isTableSearch) {
                voltDbRenderer.GetDataTablesInformation(function (connection) {
                    voltDbRenderer.searchTables(connection, $('#filterDatabaseTable')[0].value, function (searchResult) {
                        priorTableAction = currentTableAction;
                        currentTableAction = VoltDbUI.ACTION_STATES.SEARCH;
                        if (searchResult) {
                            if (voltDbRenderer.isSortTables)
                                voltDbRenderer.sortTablesByColumns(true);

                            voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlSearchData) {
                                if (htmlSearchData != undefined) {
                                    $('#tablesBody').html(htmlSearchData);
                                }
                                setPaginationIndicesOfTables(voltDbRenderer.isTableSearch, "search");

                            });

                        } else {
                            $('#tablesBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");
                            setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                        }
                        priorTableAction = VoltDbUI.ACTION_STATES.SEARCH;

                    });
                });
            }
            else {
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    if (htmlData != "") {
                        $('#tablesBody').html(htmlData);
                    } else {
                        $('#tablesBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");
                    }
                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);

                });
            }
        });

        $("#filterDatabaseTable").unbind("click");
        $('#filterDatabaseTable').on('click', function () {
            if (this.value == defaultSearchTextTable)
                this.value = '';

        });

    };

    var replicationWarning = function (count) {
        if (count == 0 || count == undefined) {
            $('#drWarning').hide();
            $('#drAlertWarning').hide();
        } else {
            $('#drWarning').show();
            $('#drAlertWarning').show();
            if (count == 1) {
                $('#drPartitionWarningMsg').text(count + ' partition is uncovered.');
            } else {
                $('#drPartitionWarningMsg').text(count + ' partitions are uncovered.');
            }
        }
    };

    var cmdLogTable = '';

    var refreshCmdLogSection = function (graphView, currentTab) {
        voltDbRenderer.GetCommandLogInformation(function (cmdLogDetails) {
            var response = cmdLogDetails;
            var htmlcontent = "";

            voltDbRenderer.GetSnapshotStatus(function (snapshotDetails) {
                cmdLogDetails[getCurrentServer()].SNAPSHOTS = snapshotDetails[getCurrentServer()];
                MonitorGraphUI.RefreshCommandLog(cmdLogDetails, getCurrentServer(), graphView, currentTab);
            });

            for (var key in response) {
                htmlcontent = htmlcontent + "<tr>";
                htmlcontent = htmlcontent + "<td>" + key + "</td>" +
                    "<td>" + response[key].OUTSTANDING_BYTES + "</td>" +
                    "<td>" + response[key].OUTSTANDING_TXNS + "</td>" +
                    "<td>" + response[key].SEGMENT_COUNT + "</td>" +
                    "<td>" + response[key].IN_USE_SEGMENT_COUNT + "</td>" +
                    "<td>" + response[key].FSYNC_INTERVAL + "</td>";
                htmlcontent = htmlcontent + "</tr>";

            }
            var leftScroll = $("#tblCmdLog_wrapper").find(".tblScroll").scrollLeft();
            if ($.fn.dataTable.isDataTable('#tblCmdLog')) {
                $("#tblCmdLog").DataTable().destroy();
            }

            var content = "<table width='100%' border='0' cellspacing='0' id='tblCmdLog' cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblCmdLog_info' role='grid'>" +
                "<thead><tr role='row'><th id='cmdServer' width='25%' data-name='none' class='' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Server: activate to sort column descending'>Server</th>" +
                "<th id='cmdPendingBytes' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Pending (in bytes)</th>" +
                "<th id='cmdPendingTrans' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Pending (in transactions)</th>" +
                "<th id='cmdTotalSegments' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Total segments</th>" +
                "<th id='cmdSegmentsInUse' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Segments in use</th>" +
                "<th id='cmdFsyncInterval' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Fsyncinterval</th>" +
                "</thead><tbody>";
            $("#tblCmdLog_wrapper").find(".cmdLogContainer").html(content + htmlcontent + "</tbody></table>");

            cmdLogTable = $("#tblCmdLog").DataTable({
                stateSave: true,
                pageLength: 5,
                "sPaginationType": "extStyleLF",
                "bAutoWidth": false,
                "language": {
                    "zeroRecords": "No data to be displayed"
                },
                "fnDrawCallback": function () {
                    if ($("#tblCmdLog").find("tbody tr td").first().html() == "No data to be displayed") {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text("0");
                    } else {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text(" " + this.fnPagingInfo().iPage + " ");
                    }

                    $(this).parent().parent().find(".dataTables_paginate .navigationLabel .totalPages").text(this.fnPagingInfo().iTotalPages);

                    if ((screen.width == 1600) && (screen.height == 900)) {
                        var length = $("#tblCmdLog tr").length - 1;
                        if (length >= 5) {
                            $("#clpSection").css("min-height", "280px");
                        } else if (length == 4) {
                            $("#clpSection").css("min-height", "250px");
                        } else if (length == 3) {
                            $("#clpSection").css("min-height", "230px");
                        } else if (length == 2) {
                            $("#clpSection").css("min-height", "200px");
                        } else if (length == 1 || length == 0) {
                            $("#clpSection").css("min-height", "170px");
                        }
                    }

                    else if ((screen.width == 360) && (screen.height == 640)) {
                        $("#clpSection").css("min-height", "380px");
                    }
                    else if ((screen.width == 640) && (screen.height == 960)) {
                        alert("iphone resolution mode");
                        $("#clpSection").css("min-height", "380px");
                    }
                    else if ($(window).width() == '751') {
                        $("#clpSection").css("min-height", "350px");
                    }
                },

                "sDom": 'p<"tblScroll drScroll"t>',
                "aoColumns": [
                    null,
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false }
                ]
            });




            $("#tblCmdLog_wrapper").find(".tblScroll").scrollLeft(leftScroll);

            $("#tblCmdLog_wrapper").find(".paginationDefault").remove();


            //Customizing DataTables to make it as existing pagination
            $(".paginate_disabled_previous").html("Prev");
            $(".paginate_enabled_next").html("Next");
            $(".paginate_disabled_next").html("Next");
            $(".paginate_enabled_previous").html("Prev");

            $(".paginate_disabled_previous").attr("title", "Previous Page");
            $(".paginate_enabled_next").attr("title", "Next Page");
            $(".paginate_disabled_next").attr("title", "Next Page");
            $(".paginate_enabled_previous").attr("title", "Previous Page");
        });

        $('#filterServer').on('keyup', function () {
            cmdLogTable.search(this.value).draw();
        });
    };
    var refreshDrMasterSection = function () {
        $("#drMasterSection").show();
        voltDbRenderer.GetDrDetails(function (drDetails) {
            var response = drDetails;

            var htmlcontent = "";
            var replicaLatencyMs = 0;
            var replicaLatencyTrans = 0;

            for (var key in response) {

                for (var i = 0; i <= response[key].length - 1; i++) {

                    replicaLatencyTrans = response[key][i].LASTQUEUEDDRID - response[key][i].LASTACKDRID;
                    replicaLatencyMs = (response[key][i].LASTQUEUEDTIMESTAMP - response[key][i].LASTACKTIMESTAMP) / 1000;
                    htmlcontent = htmlcontent + "<tr>";
                    htmlcontent = htmlcontent + "<td style='text-align: right;'>" + key + "</td>" +
                        "<td >" + VoltDbUI.drStatus + "</td>" +
                        "<td style='text-align: right;'>" + (response[key][i].TOTALBYTES / 1024 / 1024).toFixed(2) + "</td >" +
                        "<td style='text-align: right;'>" + replicaLatencyMs + "</td >" +
                        "<td style='text-align: right;'>" + replicaLatencyTrans + "</td >";
                    htmlcontent = htmlcontent + "</tr>";
                }

            }
            var leftScroll = $("#tblDrMAster_wrapper").find(".tblScroll").scrollLeft();
            if ($.fn.dataTable.isDataTable('#tblDrMAster')) {
                $("#tblDrMAster").DataTable().destroy();
            }

            var content = "<table width='100%' border='0' cellspacing='0' id='tblDrMAster' cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblDrMAster_info' role='grid'>" +
                "<thead><tr role='row'><th id='partitionID' width='20%' data-name='none' class='' tabindex='0' aria-controls='tblDrMAster' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Partition ID: activate to sort column descending'>Partition ID</th>" +
                "<th id='status' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster' rowspan='1' colspan='1' >Status</th>" +
                "<th id='mbOnDisk' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster' rowspan='1' colspan='1' >MB on disk</th>" +
                "<th id='replicaLatencyMs' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster' rowspan='1' colspan='1' >Replica Latency (ms)</th>" +
                "<th id='replicaLatencyTrans' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster' rowspan='1' colspan='1'>Replica latency (in transactions)</th></tr></thead><tbody>";
            $("#tblMAster_wrapper").find(".drMasterContainer").html(content + htmlcontent + "</tbody></table>");

            table = $("#tblDrMAster").DataTable({
                stateSave: true,
                pageLength: 5,
                "sPaginationType": "extStyleLF",
                "bAutoWidth": false,
                "language": {
                    "zeroRecords": "No data to be displayed"
                },
                "fnDrawCallback": function () {
                    if ($("#tblDrMAster").find("tbody tr td").first().html() == "No data to be displayed") {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text("0");
                    } else {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text(" " + this.fnPagingInfo().iPage + " ");
                    }

                    $(this).parent().parent().find(".dataTables_paginate .navigationLabel .totalPages").text(this.fnPagingInfo().iTotalPages);

                    if ((screen.width == 1600) && (screen.height == 900)) {
                        var length = $("#tblDrMAster tr").length - 1;
                        if (length >= 5) {
                            $("#drMasterSection").css("min-height", "280px");
                        } else if (length == 4) {
                            $("#drMasterSection").css("min-height", "250px");
                        } else if (length == 3) {
                            $("#drMasterSection").css("min-height", "230px");
                        } else if (length == 2) {
                            $("#drMasterSection").css("min-height", "200px");
                        } else if (length == 1 || length == 0) {
                            $("#drMasterSection").css("min-height", "170px");
                        }
                    }

                    else if ((screen.width == 360) && (screen.height == 640)) {
                        $("#drMasterSection").css("min-height", "380px");
                    }
                    else if ((screen.width == 640) && (screen.height == 960)) {
                        alert("iphone resolution mode");
                        $("#drMasterSection").css("min-height", "380px");
                    }
                },

                "sDom": 'p<"tblScroll drScroll"t>',
                "aoColumns": [
                    null,
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false }
                ]
            });




            $("#tblDrMAster_wrapper").find(".tblScroll").scrollLeft(leftScroll);

            $("#tblMAster_wrapper").find(".paginationDefault").remove();


            //Customizing DataTables to make it as existing pagination
            $(".paginate_disabled_previous").html("Prev");
            $(".paginate_enabled_next").html("Next");
            $(".paginate_disabled_next").html("Next");
            $(".paginate_enabled_previous").html("Prev");

            $(".paginate_disabled_previous").attr("title", "Previous Page");
            $(".paginate_enabled_next").attr("title", "Next Page");
            $(".paginate_disabled_next").attr("title", "Next Page");
            $(".paginate_enabled_previous").attr("title", "Previous Page");
        });

        $('#filterPartitionId').on('keyup', function () {
            table.search(this.value).draw();
        });
    };
    var replicaTable = '';
    var refreshDrReplicaSection = function (graphView, currentTab) {
        voltDbRenderer.GetDrReplicationInformation(function (replicationData) {
            $('#clusterId').show();
            $('#clusterId').html(" (ID: " + replicationData["DR_GRAPH"]["CLUSTER_ID"] + ")");

            MonitorGraphUI.RefreshDrReplicationGraph(replicationData, getCurrentServer(), graphView, currentTab);

            replicationWarning(replicationData["DR_GRAPH"]['WARNING_COUNT']);

            var response = replicationData["DR_GRAPH"]["REPLICATION_DATA"];

            var htmlcontent = "";
            for (var key in response) {
                htmlcontent = htmlcontent + "<tr>";
                htmlcontent = htmlcontent + "<td>" + response[key].HOSTNAME + "</td>" +
                    "<td>" + response[key].STATE + "</td>" +
                    "<td>" + response[key].REPLICATION_RATE_1M + "</td >" +
                    "<td>" + response[key].REPLICATION_RATE_5M + "</td >";
                htmlcontent = htmlcontent + "</tr>";

            }

            var leftScroll = $("#tblDrReplica_wrapper").find(".tblScroll").scrollLeft();
            if ($.fn.dataTable.isDataTable('#tblDrReplica')) {
                $("#tblDrReplica").DataTable().destroy();
            }
            var content = " <table width='100%' border='0' cellspacing='0' id='tblDrReplica' cellpadding='0' class='storeTbl drTbl no-footer dataTable'><thead><tr><th id='replicaServer' width='25%' data-name='none'>Server</th><th id='replicaStatus' width='25%' data-name='none'>Status</th><th id='replicationRate1' width='25%' data-name='none'>Replication rate (last 1 minute)</th>" +
                                               "<th id='replicationRate5' width='25%' data-name='none'>Replication rate (last 5 minutes)</th></tr></thead>" +
                                        "<tbody>";
            $("#drReplicaSection").find(".drReplicaContainer").html(content + htmlcontent + "</tbody></table>");

            replicaTable = $("#tblDrReplica").DataTable({
                stateSave: true,
                pageLength: 5,
                "sPaginationType": "extStyleLF",
                "bAutoWidth": false,
                "language": {
                    "zeroRecords": "No data to be displayed"
                },
                "fnDrawCallback": function () {

                    if ($("#tblDrReplica").find("tbody tr td").first().html() == "No data to be displayed") {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text("0");
                    } else {
                        $(this).parent().parent().find(".dataTables_paginate .navigationLabel .pageIndex").text(" " + this.fnPagingInfo().iPage + " ");
                    }


                    $(this).parent().parent().find(".dataTables_paginate .navigationLabel .totalPages").text(this.fnPagingInfo().iTotalPages);
                    var length = $("#tblDrReplica tr").length - 1;
                    if (length >= 5) {
                        $("#drReplicaSection").css("min-height", "280px");
                    } else if (length == 4) {
                        $("#drReplicaSection").css("min-height", "250px");
                    }
                    else if (length == 3) {
                        $("#drReplicaSection").css("min-height", "230px");
                    }
                    else if (length == 2) {
                        $("#drReplicaSection").css("min-height", "200px");
                    }
                    else if (length == 1 || length == 0) {
                        $("#drReplicaSection").css("min-height", "170px");
                    }
                },
                "sDom": 'p<"tblScroll drScroll"t>',
                "aoColumns": [
                    null,
                    { "bSearchable": false },
                    { "bSearchable": false },
                    { "bSearchable": false }
                ]
            });



            $("#tblDrReplica_wrapper").find(".tblScroll").scrollLeft(leftScroll);
            $("#tblReplica_wrapper").find(".paginationDefault").remove();

            //  Customizing DataTables to make it as existing pagination
            $(".paginate_disabled_previous").html("Prev");
            $(".paginate_enabled_next").html("Next");
            $(".paginate_disabled_next").html("Next");
            $(".paginate_enabled_previous").html("Prev");

            $(".paginate_disabled_previous").attr("title", "Previous Page");
            $(".paginate_enabled_next").attr("title", "Next Page");
            $(".paginate_disabled_next").attr("title", "Next Page");
            $(".paginate_enabled_previous").attr("title", "Previous Page");
            $(".paginate_enabled_previous").attr("title", "Previous Page");

        });

        $('#filterHostID').on('keyup', function () {
            replicaTable.search(this.value).draw();
        });
    };

    var setPaginationIndicesOfProcedures = function (isProcedureSearch) {
        if (isProcedureSearch) {
            if (voltDbRenderer.procedureSearchDataSize > 0) {
                if (currentProcedureAction == VoltDbUI.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUI.ACTION_STATES.SEARCH) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                }
                else if ((currentProcedureAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorProcedureAction == VoltDbUI.ACTION_STATES.SEARCH)) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                }

                else if (currentProcedureAction == VoltDbUI.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                }
                else if (currentProcedureAction == VoltDbUI.ACTION_STATES.NEXT && priorProcedureAction == VoltDbUI.ACTION_STATES.NEXT) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                }
                else if (currentProcedureAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorProcedureAction == VoltDbUI.ACTION_STATES.NEXT) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));

                }
                else {
                    lblPrevious.innerHTML = " ".concat(1, ' ');
                    lblTotalPages.innerHTML = voltDbRenderer.procedureSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureSearchDataSize / voltDbRenderer.maxVisibleRows));
                }


            } else {
                lblPrevious.innerHTML = " ".concat(0, ' ');
                lblTotalPages.innerHTML = " ".concat(0);

            }

        } else {
            if (voltDbRenderer.procedureDataSize > 0) {
                if (currentProcedureAction == VoltDbUI.ACTION_STATES.NEXT) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                }
                else if (currentProcedureAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                    lblPrevious.innerHTML = " ".concat(voltDbRenderer.procedureTableIndex + 1, ' ');
                }
                else {
                    lblPrevious.innerHTML = " ".concat(1, ' ');
                }
                lblTotalPages.innerHTML = voltDbRenderer.procedureDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.procedureDataSize / voltDbRenderer.maxVisibleRows));

            } else {
                lblPrevious.innerHTML = " ".concat(0, ' ');
                lblTotalPages.innerHTML = " ".concat(0);

            }
        }

    };

    var setPaginationIndicesOfTables = function (isTableSearch) {
        //set pagination
        if (isTableSearch) {
            if (voltDbRenderer.tableSearchDataSize > 0) {
                if (currentTableAction == VoltDbUI.ACTION_STATES.NEXT &&
                    (priorTableAction == VoltDbUI.ACTION_STATES.SEARCH || priorTableAction == VoltDbUI.ACTION_STATES.PREVIOUS || priorTableAction == VoltDbUI.ACTION_STATES.NEXT)) {
                    lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));


                } else if ((currentTableAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorTableAction == VoltDbUI.ACTION_STATES.SEARCH || priorTableAction == VoltDbUI.ACTION_STATES.PREVIOUS)) {
                    lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));

                } else if (currentTableAction == VoltDbUI.ACTION_STATES.PREVIOUS && priorTableAction == VoltDbUI.ACTION_STATES.NEXT) {
                    lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));

                }

                else if (currentTableAction == VoltDbUI.ACTION_STATES.REFRESH && (priorTableAction == VoltDbUI.ACTION_STATES.PREVIOUS || priorTableAction == VoltDbUI.ACTION_STATES.NEXT)) {
                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));
                }
                else {
                    lblPreviousTable.innerHTML = " ".concat(1, ' ');
                    lblTotalPagesofTables.innerHTML = voltDbRenderer.tableSearchDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableSearchDataSize / voltDbRenderer.maxVisibleRows));
                }

            }
            else {
                lblPreviousTable.innerHTML = " ".concat(0, ' ');
                lblTotalPagesofTables.innerHTML = " ".concat(0, ' ');

            }

        }
        else {
            if (voltDbRenderer.tableDataSize > 0) {
                if (currentTableAction == VoltDbUI.ACTION_STATES.NEXT) {
                    lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');
                }
                else if (currentTableAction == VoltDbUI.ACTION_STATES.PREVIOUS) {
                    lblPreviousTable.innerHTML = " ".concat(voltDbRenderer.tableIndex + 1, ' ');

                } else if ((currentTableAction == VoltDbUI.ACTION_STATES.REFRESH || currentTableAction == VoltDbUI.ACTION_STATES.NONE) &&
                    (voltDbRenderer.isSearchTextCleaned || !(priorTableAction == VoltDbUI.ACTION_STATES.PREVIOUS || priorTableAction == VoltDbUI.ACTION_STATES.NEXT))) {
                    lblPreviousTable.innerHTML = " ".concat(1, ' ');
                    voltDbRenderer.isSearchTextCleaned = false;

                }
                else if (voltDbRenderer.isTableSortClicked) {
                    lblPreviousTable.innerHTML = " ".concat(1, ' ');
                }
                lblTotalPagesofTables.innerHTML = voltDbRenderer.tableDataSize < voltDbRenderer.maxVisibleRows ? " ".concat(1) : " ".concat(Math.ceil(voltDbRenderer.tableDataSize / voltDbRenderer.maxVisibleRows));


            } else {
                lblPreviousTable.innerHTML = " ".concat(0, ' ');
                lblTotalPagesofTables.innerHTML = " ".concat(0, ' ');

            }
        }
    };


    var saveThreshold = function () {

        var defaultThreshold = 70;
        var thresholdInput = $("#threshold");

        if (thresholdInput.val() == "") {
            thresholdInput.val(defaultThreshold);
        }
        if (VoltDbUI.getFromLocalStorage("alert-threshold") == undefined || VoltDbUI.getFromLocalStorage("alert-threshold") == null) {
            saveInLocalStorage("alert-threshold", defaultThreshold);
        }

        $("#saveThreshold").on("click", function () {

            if (thresholdInput.val() == "" || thresholdInput.val() * 1 > 100) {
                alert("The value of \"Alert Threshold\" should be between 0 and 100.");
                return false;
            }

            var thresholdValue = (thresholdInput.val() != "") ? thresholdInput.val() : defaultThreshold;
            saveInLocalStorage("alert-threshold", thresholdValue)
            refreshClusterHealth();
            $("#popServerList").hide();
            $("#overlay").show();
            $('#btnPopServerList').removeClass('hideServers');
            $('#btnPopServerList').addClass('showServers');
            return true;
        });

        $("#threshold").on("keypress", function (e) {
            if (e.which != 8 && e.which != 0 && (e.which < 48 || e.which > 57)) {
                return false;
            }
            return true;

        });

        //Set the value of threshold
        $("#threshold").val(VoltDbUI.getFromLocalStorage("alert-threshold"));

    };

    if (VoltDbUI.getFromLocalStorage("graph-view") == undefined || VoltDbUI.getFromLocalStorage("graph-view") == null)
        saveInLocalStorage("graph-view", $("#graphView").val());

    $("#graphView").val(VoltDbUI.getFromLocalStorage("graph-view"));
    MonitorGraphUI.AddGraph(VoltDbUI.getFromLocalStorage("graph-view"), $('#chartServerCPU'), $('#chartServerRAM'), $('#chartClusterLatency'), $('#chartClusterTransactions'), $('#chartPartitionIdleTime'), $('#ChartDrReplicationRate'), $('#chartCommandLogging'));

    $('#PROCEDURE,#INVOCATIONS,#MIN_LATENCY,#MAX_LATENCY,#AVG_LATENCY,#AVG_LATENCY,#PERC_EXECUTION').unbind('click');
    $('#PROCEDURE,#INVOCATIONS,#MIN_LATENCY,#MAX_LATENCY,#AVG_LATENCY,#PERC_EXECUTION').on('click', function () {
        voltDbRenderer.isProcedureSortClicked = true;
        currentProcedureAction = VoltDbUI.ACTION_STATES.SORT;

        if (this.id != 'PROCEDURE') {
            $("#tblStoredProcedures").find('#PROCEDURE').attr('data-name', 'none');
            $("#tblStoredProcedures").find('#PROCEDURE').removeAttr('class');
        }

        if (VoltDbUI.sortStatus == VoltDbUI.SORT_STATES.NONE || VoltDbUI.sortStatus == VoltDbUI.SORT_STATES.SORTED) {
            if ($(this).data('name') == "none") {
                $(this).data('name', 'ascending');
                voltDbRenderer.sortOrder = "ascending";

            }
            else if ($(this).data('name') == "ascending") {
                voltDbRenderer.sortOrder = "descending";
                $(this).data('name', 'descending');

            } else if ($(this).data('name') == "descending") {
                voltDbRenderer.sortOrder = "ascending";
                $(this).data('name', 'ascending');

            }

            //apply css to display sort arrow image
            if ($(this).hasClass("sorttable_sorted")) {
                if (this.id == voltDbRenderer.sortColumn)
                    $("#" + voltDbRenderer.sortColumn).data('name', 'descending');
                else
                    $("#" + voltDbRenderer.sortColumn).data('name', 'none');


                $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted");
                $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted_reverse");

                $(this).removeClass("sorttable_sorted");
                $(this).addClass("sorttable_sorted_reverse");


            } else if ($(this).hasClass("sorttable_sorted_reverse")) {
                if (this.id == voltDbRenderer.sortColumn)
                    $("#" + voltDbRenderer.sortColumn).data('name', 'ascending');
                else
                    $("#" + voltDbRenderer.sortColumn).data('name', 'none');

                $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted");
                $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted_reverse");

                $(this).removeClass("sorttable_sorted_reverse");
                $(this).addClass("sorttable_sorted");

            } else {
                $(this).addClass("sorttable_sorted");
                if ($("#" + voltDbRenderer.sortColumn) != undefined) {
                    $("#" + voltDbRenderer.sortColumn).data('name', 'none');
                    $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted");
                    $("#" + voltDbRenderer.sortColumn).removeClass("sorttable_sorted_reverse");
                }

            }

            voltDbRenderer.isSortProcedures = true;
            voltDbRenderer.sortColumn = this.id;

            if (voltDbRenderer.isProcedureSearch) {
                VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.SORTING;
                voltDbRenderer.searchProcedures('', $('#filterStoredProc')[0].value, function (searchResult) {
                    currentProcedureAction = VoltDbUI.ACTION_STATES.SORT;
                    voltDbRenderer.formatSearchDataToJsonArray();
                    if (voltDbRenderer.sortProceduresByColumns(false)) {
                        voltDbRenderer.mapProcedureInformationSorting(currentProcedureAction, priorProcedureAction, function (htmlData) {
                            if (htmlData != "")
                                $('#storeProcedureBody').html(htmlData);

                            else
                                $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

                            //set pagination
                            setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);
                            VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.SORTED;

                        });

                    } else {
                        VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.NONE;
                    }
                    priorProcedureAction = VoltDbUI.ACTION_STATES.SEARCH;

                });
            } else {
                VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.SORTING;
                voltDbRenderer.sortProceduresByColumns(false);
                voltDbRenderer.mapProcedureInformationSorting(currentProcedureAction, priorProcedureAction, function (htmlData) {
                    if (htmlData != "")
                        $('#storeProcedureBody').html(htmlData);

                    else
                        $('#storeProcedureBody').html("<tr><td colspan=6> No data to be displayed</td></tr>");

                    setPaginationIndicesOfProcedures(voltDbRenderer.isProcedureSearch);
                    VoltDbUI.sortStatus = VoltDbUI.SORT_STATES.SORTED;

                });

            }

        }
        voltDbRenderer.isProcedureSortClicked = false;
    });

    $('#TABLE_NAME,#TUPLE_COUNT,#MAX_ROWS,#MIN_ROWS,#AVG_ROWS,#TABLE_TYPE').unbind('click');
    $('#TABLE_NAME,#TUPLE_COUNT,#MAX_ROWS,#MIN_ROWS,#AVG_ROWS,#TABLE_TYPE').on('click', function () {
        voltDbRenderer.isTableSortClicked = true;

        if (this.id != 'TABLE_NAME') {
            $("#tblDataTables").find('#TABLE_NAME').attr('data-name', 'none');
            $("#tblDataTables").find('#TABLE_NAME').removeAttr('class');
        }
        if (VoltDbUI.tableSortStatus == VoltDbUI.SORT_STATES.NONE || VoltDbUI.tableSortStatus == VoltDbUI.SORT_STATES.SORTED) {
            if ($(this).data('name') == "none") {
                $(this).data('name', 'ascending');
                voltDbRenderer.sortTableOrder = "ascending";

            }
            else if ($(this).data('name') == "ascending") {
                voltDbRenderer.sortTableOrder = "descending";
                $(this).data('name', 'descending');

            } else if ($(this).data('name') == "descending") {
                voltDbRenderer.sortTableOrder = "ascending";
                $(this).data('name', 'ascending');

            }

            //apply css to display sort arrow image
            if ($(this).hasClass("sorttable_sorted")) {
                if (this.id == voltDbRenderer.tableSortColumn) {
                    $("#" + voltDbRenderer.tableSortColumn).data('name', 'descending');
                } else
                    $("#" + voltDbRenderer.tableSortColumn).data('name', 'none');


                $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted");
                $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted_reverse");

                $(this).removeClass("sorttable_sorted");
                $(this).addClass("sorttable_sorted_reverse");


            } else if ($(this).hasClass("sorttable_sorted_reverse")) {
                if (this.id == voltDbRenderer.tableSortColumn)
                    $("#" + voltDbRenderer.tableSortColumn).data('name', 'ascending');
                else
                    $("#" + voltDbRenderer.tableSortColumn).data('name', 'none');

                $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted");
                $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted_reverse");

                $(this).removeClass("sorttable_sorted_reverse");
                $(this).addClass("sorttable_sorted");

            } else {
                $(this).addClass("sorttable_sorted");
                if ($("#" + voltDbRenderer.tableSortColumn) != undefined) {
                    $("#" + voltDbRenderer.tableSortColumn).data('name', 'none');
                    $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted");
                    $("#" + voltDbRenderer.tableSortColumn).removeClass("sorttable_sorted_reverse");
                }

            }


            voltDbRenderer.isSortTables = true;
            voltDbRenderer.tableSortColumn = this.id;

            if (voltDbRenderer.isTableSearch) {
                VoltDbUI.tableSortStatus = VoltDbUI.SORT_STATES.SORTING;
                priorTableAction = currentTableAction;
                currentTableAction = VoltDbUI.ACTION_STATES.SEARCH;
                voltDbRenderer.sortTablesByColumns();
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    if ((htmlData != undefined && htmlData != "")) {
                        $('#tablesBody').html(htmlData);
                    }

                    //set pagination
                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                    VoltDbUI.tableSortStatus = VoltDbUI.SORT_STATES.SORTED;
                    voltDbRenderer.isTableSortClicked = false;

                });
                priorTableAction = VoltDbUI.ACTION_STATES.SEARCH;

            } else {
                VoltDbUI.tableSortStatus = VoltDbUI.SORT_STATES.SORTING;
                voltDbRenderer.sortTablesByColumns();
                voltDbRenderer.mapTableInformation(currentTableAction, priorTableAction, voltDbRenderer.isTableSearch, function (htmlData) {
                    if ((htmlData != undefined && htmlData != "")) {
                        $('#tablesBody').html(htmlData);
                    }

                    setPaginationIndicesOfTables(voltDbRenderer.isTableSearch);
                    VoltDbUI.tableSortStatus = VoltDbUI.SORT_STATES.SORTED;
                    voltDbRenderer.isTableSortClicked = false;
                });

            }
        }

    });


    $("#graphView").on("change", function () {
        var graphView = $("#graphView").val();
        saveInLocalStorage("graph-view", graphView);
        MonitorGraphUI.RefreshGraph(graphView);
        MonitorGraphUI.UpdateCharts();
    });

    //slides the element with class "menu_body" when paragraph with class "menu_head" is clicked
    $("#firstpane div.menu_head").click(function () {
        var userPreferences = getUserPreferences();
        if (userPreferences != null) {
            if (userPreferences['DatabaseTables'] != false || userPreferences['StoredProcedures'] != false) {
                var headerState = $("#firstpane div.menu_body").css('display');
                if (headerState == 'none') {
                    $(this).removeClass('collapsed');
                    $(this).addClass('expanded');
                } else {
                    $(this).removeClass('expanded');
                    $(this).addClass('collapsed');
                }
                $(this).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");
            }
        } else {

            $('ul.user-preferences > li Input:checkbox').each(function (value) {
                userPreferences[$(this)[0].id] = true;
            });
            saveUserPreferences(userPreferences);

            $(this).removeClass('collapsed');
            $(this).addClass('expanded');
            $(this).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");

        }

    });

    $("#dbPane div.menu_head").click(function () {
        var headerState = $("#dbPane div.menu_body").css('display');
        if (headerState == 'none') {
            $(this).removeClass('collapsedDR');
            $(this).addClass('expandedDR');
        } else {
            $(this).removeClass('expandedDR');
            $(this).addClass('collapsedDR');
        }
        $(this).next("div.menu_body").slideToggle(300).siblings("div.menu_body").slideUp("slow");
    });

    refreshClusterHealth();
    MonitorGraphUI.setStartTime()
    refreshGraphAndData(VoltDbUI.getFromLocalStorage("graph-view"), VoltDbUI.CurrentTab);
    setInterval(refreshClusterHealth, 5000);
    setInterval(function () {
        refreshGraphAndData(VoltDbUI.getFromLocalStorage("graph-view"), VoltDbUI.CurrentTab);
    }, 5000);

    configureUserPreferences();
    adjustGraphSpacing();
    saveThreshold();

    $('#showMyHelp').popup();
    $('#ShowAbout').popup();
    $("#conPopup").popup({
        closeDialog: function () {
            VoltDbUI.isConnectionChecked = false;
            VoltDbUI.refreshConnectionTime('20000');
            $('#connectionPopup').hide();
        }
    });

    VoltDbUI.refreshConnectionTime('20000');
};



/*********************************************************************************************
/Configure My Graph dialog
/*********************************************************************************************/
var configureUserPreferences = function () {
    var userPreference = {};
    userPreference["serverCPU"] = {};
    userPreference["serverRAM"] = {};
    userPreference["clusterLatency"] = {};
    userPreference["clusterTransactions"] = {};
    userPreference["partitionIdleTime"] = {};
    userPreference["storedProcedures "] = {};
    userPreference["databaseTables "] = {};

    $('#showMyPreference').popup(
        {
            open: function (event, ui, ele) {
                userPreference = getUserPreferences();
                $('ul.user-preferences > li Input:checkbox').each(function (value) {
                    $(this)[0].checked = userPreference[$(this)[0].id];
                });
            },
            save: function () {
                $('ul.user-preferences > li Input:checkbox').each(function (value) {
                    userPreference[$(this)[0].id] = $(this)[0].checked;

                });
                saveUserPreferences(userPreference);

                MonitorGraphUI.UpdateCharts();
            }

        });
    };


/*******************************************************************************************
//common methods
/*******************************************************************************************/

var isNodeButtonRegistered = function (elementName) {
    var isRegistered = false;
    var elementCount = 0;
    if (VoltDbAdminConfig.registeredElements != undefined && VoltDbAdminConfig.registeredElements.length > 0) {
        $.each(VoltDbAdminConfig.registeredElements, function (key, value) {
            if (value == elementName) {
                isRegistered = true;
                return false;
            }
            else if (elementCount == VoltDbAdminConfig.registeredElements.length - 1) {
                isRegistered = false;
            }
            elementCount++;
        });

    } else
        isRegistered = false;

    return isRegistered;
};


//Dummy wrapper for console.log for IE9
if (!(window.console && console.log)) {
    console = {
        log: function () { },
        debug: function () { },
        info: function () { },
        warn: function () { },
        error: function () { }
    };
}

var saveCookie = function (name, value) {
    $.cookie(name + "_" + VoltDBConfig.GetPortId(), value, { expires: 365 });
};

var saveSessionCookie = function (name, value) {
    $.cookie(name + "_" + VoltDBConfig.GetPortId(), value);
};

var saveInLocalStorage = function(name, value){
    data = {}
    data.value = value
    data.time = new Date().getTime();
    localStorage.setItem(name + "_" + VoltDBConfig.GetPortId(), JSON.stringify(data))
}

var saveUserPreferences = function (preferences) {
    var lPreferences = preferences;
    saveInLocalStorage("user-preferences", JSON.stringify(preferences));
    showHideGraph(lPreferences);
};

var NavigationTabs = {
    DBMonitor: 1,
    Admin: 2,
    Schema: 3,
    SQLQuery: 4,
    DR: 5
};

var getCurrentTab = function () {
    var activeLinkId = "";
    var activeLink = $("#nav .active");
    if (activeLink.length > 0) {
        activeLinkId = activeLink.attr("id");
    }

    if (activeLinkId == "navSqlQuery") {
        $(".nvtooltip").hide();
        return NavigationTabs.SQLQuery;
    } else if (activeLinkId == "navSchema") {
        $(".nvtooltip").hide();
        return NavigationTabs.Schema;
    } else if (activeLinkId == "navAdmin") {
        $(".nvtooltip").hide();
        return NavigationTabs.Admin;
    } else if (activeLinkId == "navDR"){
        $(".nvtooltip").hide();
        return NavigationTabs.DR;
    }
    $(".nvtooltip").show();
    return NavigationTabs.DBMonitor;
};

var getUserPreferences = function () {
    try {
        voltDbRenderer.userPreferences = $.parseJSON(VoltDbUI.getFromLocalStorage("user-preferences"));
    } catch (e) {

        voltDbRenderer.userPreferences = {};
        var preferencesList = ["ServerCPU", "ServerRAM", "ClusterLatency", "ClusterTransactions", "StoredProcedures", "DatabaseTables", "PartitionIdleTime", "CommandLogStat", "CommandLogTables"];
        for (var i = 0; i < preferencesList.length; i++) {
            if (preferencesList[i] == "ServerCPU" || preferencesList[i] == "ServerRAM" || preferencesList[i] == "ClusterLatency" || preferencesList[i] == "ClusterTransactions" || preferencesList[i] == "DatabaseTables") {
                voltDbRenderer.userPreferences[preferencesList[i]] = true;
            } else {
                voltDbRenderer.userPreferences[preferencesList[i]] = false;
            }
        }
    }
    return voltDbRenderer.userPreferences;
};

var saveCurrentServer = function (serverName) {
    saveInLocalStorage("currentServer", serverName);
};

var getCurrentServer = function () {
    return VoltDbUI.getFromLocalStorage("currentServer");
};

var showHideGraph = function (userpreferences) {
    if (userpreferences["ServerCPU"] == false)
        $("#chartServerCPU").hide();
    else
        $("#chartServerCPU").show();

    if (userpreferences["ServerRAM"] == false)
        $("#chartServerRAM").hide();
    else
        $("#chartServerRAM").show();

    if (userpreferences["ClusterLatency"] == false)
        $("#chartClusterLatency").hide();
    else
        $("#chartClusterLatency").show();

    if (userpreferences["ClusterTransactions"] == false)
        $("#chartClusterTransactions").hide();
    else
        $("#chartClusterTransactions").show();

    if (userpreferences["PartitionIdleTime"] == false)
        $("#chartPartitionIdleTime").hide();
    else
        $("#chartPartitionIdleTime").show();

    if(VoltDbUI.drConsumerState.toLowerCase() != 'disable' && VoltDbUI.drConsumerState != '' ){
        $("#ChartDrReplicationRate").show();
    }else{
        $("#ChartDrReplicationRate").hide();
    }

    if (userpreferences["StoredProcedures"] == false)
        $("#tblStoredProcedures").hide();
    else
        $("#tblStoredProcedures").show();

    if (userpreferences["DatabaseTables"] == false)
        $("#tblDataTables").hide();
    else
        $("#tblDataTables").show();

    if (userpreferences["DatabaseTables"] == false && userpreferences["StoredProcedures"] == false)
        $("#firstpane").hide();
    else
        $("#firstpane").show();

    if (!(VoltDbUI.drReplicationRole.toLowerCase() == "none" && !VoltDbUI.drMasterEnabled)) {
        if (VoltDbUI.drReplicationRole.toLowerCase() == 'replica') {
            if ((VoltDbUI.drConsumerState.toLowerCase() != 'disable' || VoltDbUI.drMasterState.toUpperCase() == 'ACTIVE') && VoltDbUI.drConsumerState.toLowerCase() != '' && VoltDbUI.drMasterState.toUpperCase() != '') {
                $("#divDrReplication").show();
            } else {
                $("#divDrReplication").hide();
            }
        } else {
            if (VoltDbUI.drMasterEnabled || VoltDbUI.drConsumerState.toLowerCase() != 'disable') {
                $("#divDrReplication").show();
            } else {
                $("#divDrReplication").hide();
            }
        }
    } else {
        if (VoltDbUI.drConsumerState.toLowerCase() != 'disable') {
            $("#divDrReplication").show();
        } else {
            $("#divDrReplication").hide();
        }
    }


    if (VoltDbUI.isCommandLogEnabled == 'true') {
        if (userpreferences["CommandLogStat"] == true) {
            $("#chartCommandLogging").show();
        } else {
            $("#chartCommandLogging").hide();
        }
    } else {
        $("#chartCommandLogging").hide();
    }

    if (VoltDbUI.isCommandLogEnabled == 'true') {
        if (userpreferences["CommandLogTables"] == true) {
            $("#divCommandLog").show();
        } else {
            $("#divCommandLog").hide();
        }
    } else {
        $("#divCommandLog").hide();
    }

    adjustGraphSpacing();
    ChangeGraphLabelColor();
    ChangeTableProcedureLabelColor();
};

function ChangeGraphLabelColor() {
    if (VoltDbUI.getFromLocalStorage("user-preferences") != undefined) {
        var userPreferences = $.parseJSON(VoltDbUI.getFromLocalStorage("user-preferences"));

        if (userPreferences['ClusterLatency'] != false || userPreferences['ClusterTransactions'] != false || userPreferences['ServerCPU'] != false || userPreferences['ServerRAM'] != false || userPreferences["PartitionIdleTime"] != false || userPreferences["CommandLogStat"] != false || userPreferences["CommandLogStat"] != false) {
            $('#showHideGraphBlock').css('color', '#000000');
            $("#GraphBlock").removeClass("graphOpacity");
        } else {
            $('#showHideGraphBlock').css('color', 'gray');
            $("#GraphBlock").addClass("graphOpacity");
        }
    }
}

function ChangeTableProcedureLabelColor() {
    if (VoltDbUI.getFromLocalStorage("user-preferences") != undefined) {
        var userPreferences = $.parseJSON(VoltDbUI.getFromLocalStorage("user-preferences"));
        if (userPreferences['DatabaseTables'] != false || userPreferences['StoredProcedures'] != false) {
            $('#ShowHideBlock').css('color', '#000000');
        } else {
            $('#ShowHideBlock').css('color', 'gray');
        }
    }
}

// Graph Spacing adjustment on preference change
var adjustGraphSpacing = function () {
    var graphList = [$("#chartServerCPU"), $("#chartServerRAM"), $("#chartClusterLatency"), $("#chartClusterTransactions"), $("#chartPartitionIdleTime"),$('#chartCommandLogging')];

    var css = "left";

    for (var i = 0; i < graphList.length; i++) {
        if (graphList[i].is(':visible')) {
            graphList[i].removeClass("left right");
            graphList[i].addClass(css);

            if (css == "left")
                css = "right";
            else
                css = "left";
        }
    }
};

(function (window) {
    var iVoltDbUi = (function () {
        this.isSchemaTabLoading = false;
        this.drMasterEnabled = false;
        this.drMasterState = '';
        this.drConsumerState = '';
        this.drStatus = '';
        this.drReplicationRole = "NONE";
        this.isDRInfoRequired = false;
        this.isCommandLogEnabled = false;
        this.ACTION_STATES = {
            NONE: -1,
            NEXT: 0,
            PREVIOUS: 1,
            REFRESH: 2,
            SEARCH: 3,
            SORT: 4
        };

        this.SORT_STATES = {
            NONE: 0,
            SORTING: 1,
            SORTED: 2
        };

        this.DASHBOARD_PROGRESS_STATES = {
            REFRESHMEMORY: 0,
            REFRESHMEMORY_NONE: 1,
            REFRESHCPU: 2,
            REFRESHCPU_NONE: 3,
            REFRESHLATENCY: 4,
            REFRESHLATENCY_NONE: 5,
            REFRESHTRANSACTION: 6,
            REFRESHTRANSACTION_NONE: 7,

            REFRESH_PROCEDUREDATA: 8,
            REFRESH_PROCEDUREDATA_NONE: 9,
            REFRESH_TABLEDATA: 10,
            REFRESH_TABLEDATA_NONE: 11
        };

        this.popups = [];
        this.isPopupRevoked = false;

        this.CurrentTab = NavigationTabs.DBMonitor;

        this.CurrentMemoryProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHMEMORY_NONE;
        this.CurrentCpuProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHCPU_NONE;
        this.CurrentLatencyProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHLATENCY_NONE;
        this.CurrentTransactionProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHTRANSACTION_NONE;
        this.CurrentTableDataProgress = this.DASHBOARD_PROGRESS_STATES.REFRESH_TABLEDATA_NONE;
        this.CurrentProcedureDataProgress = this.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;
        this.sortStatus = this.SORT_STATES.NONE;
        this.tableSortStatus = this.SORT_STATES.NONE;
        this.isConnectionChecked = false;
        this.connectionTimeInterval = null;
        this.partitionGraphInterval = null;
        this.gutterInstanceHor = null;
        this.gutterInstanceVer = null;
        //load schema tab and table and views tabs inside sql query
        this.refreshSqlAndSchemaTab = function () {
            this.loadSchemaTab();
            SQLQueryRender.populateTablesAndViews();
        };
        this.hasPermissionToView = true;
        this.getCookie = function (name) {
            return $.cookie(name + "_" + VoltDBConfig.GetPortId());
        },

        this.getFromLocalStorage = function(name) {
            var value =  undefined
            var data = localStorage.getItem(name + "_" + VoltDBConfig.GetPortId())
            if(data != undefined)
                value =  $.parseJSON(data)
            return value == undefined ? value : value['value']
        },

        this.refreshConnectionTime = function (seconds) {
            if (VoltDbUI.connectionTimeInterval != null)
                window.clearInterval(VoltDbUI.connectionTimeInterval);

            VoltDbUI.connectionTimeInterval = window.setInterval(checkServerConnection, seconds);
        };

        var checkServerConnection = function () {
            if (!VoltDbUI.isConnectionChecked) {
                VoltDbUI.isConnectionChecked = true;
                voltDbRenderer.CheckServerConnection(
                    function (result) {
                        if (result == false) {
                            VoltDBCore.isServerConnected = false;
                            if (!$('#conpop').is(':visible') && !$('#shutdownPop').is(':visible')) {
                                window.clearInterval(VoltDbUI.connectionTimeInterval);
                                $('#conPopup').click();
                            }
                        } else {
                            VoltDbUI.isConnectionChecked = false;
                        }
                    }
                );
            }
        };

        this.loadSchemaTab = function () {
            this.isSchemaTabLoading = true;

            var schemaHtml = '<div id="schemaOverlay" style="display: block;"><div class="loading"></div></div>';
            schemaHtml = schemaHtml + $('#schema').html();
            $('#schema').html(schemaHtml);

            var templateUrl = window.location.protocol + '//' + window.location.host + '/catalog';
            var templateJavascript = "js/template.js";


            $.post(templateUrl, function (result) {
                result = result.replace('<!--##SIZES##>', '');
                var body = $(result).filter("#wrapper").html();
                $("#schema").html(body);
                $("#overlay").hide();
                $("#schemaLinkSqlQuery").on("click", function (e) {
                    $("#navSqlQuery").trigger("click");
                    e.preventDefault();
                });

                $.getScript(templateJavascript, function () {
                    $('.schm').css("display", "block");
                    $('.refreshBtn').unbind("click");
                    $('.refreshBtn.schm').unbind("click");
                    $('.refreshBtn.schm,.refreshBtn').click(function () {
                        VoltDbUI.refreshSqlAndSchemaTab();

                    });
                    VoltDbUI.isSchemaTabLoading = false;
                });

            });

        };

    });
    window.VoltDbUI = VoltDbUi = new iVoltDbUi();

})(window);


function RefreshServerUI() {
    var clickedServer = window.location.hostname;
    if (clickedServer != "") {
        $('.activeServerName').html(clickedServer).attr('title', clickedServer);
        saveCurrentServer(clickedServer);

        $('#popServerList').toggle('slide', '', 1500);
        $(this).parent().prevAll().removeClass('monitoring');
        $(this).parent().nextAll().removeClass('monitoring');
        $(this).parent().addClass('monitoring');

        $('#btnPopServerList').removeClass('hideServers');
        $('#btnPopServerList').addClass('showServers');

        $("#popServerList").hide();

    }

}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
    var regexS = "[\\?&]" + name + "=([^&#]*)";
    var regex = new RegExp(regexS);
    var results = regex.exec(window.location.href);
    if (results == null)
        return "";
    else
        return decodeURIComponent(results[1].replace(/\+/g, " "));
}
