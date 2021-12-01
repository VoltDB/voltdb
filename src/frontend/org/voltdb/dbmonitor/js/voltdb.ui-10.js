var ispopupRevoked = false;
var table = "";
var isMobile = false; //initiate as false
// device detection
if (
  /(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|Android|Silk|lge |maemo|midp|mmp|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino/i.test(
    navigator.userAgent
  ) ||
  /1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i.test(
    navigator.userAgent.substr(0, 4)
  )
)
  isMobile = true;

var is_iPad = navigator.userAgent.match(/iPad/i) != null;

$(document).ready(function () {
  $("#helppopup").load("help.htm", function () { });
  //clear the localStorage for DataTables in DR Section
  var tmp = [];
  for (var i = 0, len = localStorage.length; i < len; i++) {
    tmp.push(localStorage.key(i));
  }

  var errCount = 0;

  for (var i = 0, len = tmp.length; i < len; i++) {
    var key = tmp[i];
    var value = localStorage[key];
    if (key != "queries" && key != "queryNameList" && key != "key") {
      if (value != undefined) {
        try {
          var data = $.parseJSON(value);
        }
        catch (err) {
          console.log('JSON ERROR : ', key, value, err);
          localStorage.removeItem(key);
          errCount++;
          continue;
        }
      }
      if (!data.hasOwnProperty("time")) {
        if ($.cookie("sessionCookie") == undefined) {
          localStorage.removeItem(key);
        }
      } else {
        if (data["time"] == "0") {
          if ($.cookie("sessionCookie") == undefined) {
            localStorage.removeItem(key);
          }
        } else {
          var oneDay = 24 * 60 * 60 * 1000; // hours*minutes*seconds*milliseconds
          var sessionStartTime = new Date(data["time"]);
          var currentTime = new Date();
          var difference = Math.round(
            Math.abs(
              (sessionStartTime.getTime() - currentTime.getTime()) / oneDay
            )
          );

          if (difference >= 365) {
            localStorage.removeItem(key);
          }
        }
      }
      if (
        key.indexOf("DataTables_tblDrMAster") > -1 ||
        key.indexOf("DataTables_tblDrReplica") > -1 ||
        key.indexOf("DataTables_tblCmdLog_") > -1
      ) {
        localStorage.removeItem(key);
      }
    }
  }

  if (errCount > 0) {
    window.location.reload();
  }

  $.cookie("sessionCookie", "true");
  var rv = -1;
  if (
    VoltDbUI.getCookie("username") != undefined &&
    VoltDbUI.getCookie("username") != "null"
  ) {
    $("#logOut").css("display", "block");
  } else {
    $("#logOut").css("display", "none");
  }

  //Prevent scrolling of page.
  $("#navSchema").on("click", function (e) {
    //Browser validation for IE version less than IE 11
    if (navigator.userAgent.indexOf("MSIE") >= 0) {
      window.location.hash = "#o"; //This is required for IE.
      setTimeout(function () {
        window.scrollTo(0, 0);
      }, 10);
    }
    //IE 11 is just a preview release.
    //Hence validation expressions may differs after the full version is released
    //In such case, below validation has to be updated
    else if (navigator.appName == "Netscape") {
      var ua = navigator.userAgent;
      var re = new RegExp("Trident/.*rv:([0-9]{1,}[.0-9]{0,})");

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

      if (
        json["DisplayPreferences"] != undefined &&
        json["DisplayPreferences"] != ""
      )
        saveInLocalStorage("user-preferences", json["DisplayPreferences"]);

      if (json["GraphView"] != undefined && json["GraphView"] != "")
        saveInLocalStorage("graph-view", json["GraphView"]);

      if (json["DrGraphView"] != undefined && json["DrGraphView"] != "")
        saveInLocalStorage("dr-graph-view", json["DrGraphView"]);

      if (
        json["ImporterGraphView"] != undefined &&
        json["ImporterGraphView"] != ""
      )
        saveInLocalStorage("importer-graph-view", json["ImporterGraphView"]);

      if (
        json["ExporterGraphView"] != undefined &&
        json["ExporterGraphView"] != ""
      )
        saveInLocalStorage("exporter-graph-view", json["ExporterGraphView"]);

      if (json["CurrentServer"] != undefined && json["CurrentServer"] != "")
        saveCurrentServer(json["CurrentServer"]);

      if (json["username"] != undefined && json["username"] != "")
        saveSessionCookie("username", json["username"]);

      if (json["AlertThreshold"] != undefined && json["AlertThreshold"] != "")
        saveInLocalStorage("alert-threshold", json["AlertThreshold"]);

      if (json["tab"] == "admin") {
        saveSessionCookie("current-tab", NavigationTabs.Admin);
      }

      location.href = location.href.split("?")[0];
    }
  } catch (e) {
    /*Ignore error.*/
  }

  // Toogle Server popup
  $("#btnPopServerList").click(function (event) {
    $("#popServerSearch").val("");
    $("#popServerSearch").attr("placeholder", "Search Server");
    $("#popServerSearch").keyup();
    event.stopPropagation();
    if ($("#popServerList").css("display") == "none") {
      $(this).removeClass("showServers");
      $(this).addClass("hideServers");
    } else {
      $(this).removeClass("hideServers");
      $(this).addClass("showServers");
    }
    $("#popServerList").toggle("slide", { direction: "right" }, 1500);

    // $("#wrapper").click(function () {
    //     if ($('#popServerList').css('display') == 'block') {
    //         $('#popServerList').hide();
    //         $('#btnPopServerList').removeClass('hideServers');
    //         $('#btnPopServerList').addClass('showServers');
    //     }

    // });
  });

  $("#popServerList").on("click", function (event) {
    event.stopPropagation();
  });

  // Pop Slide Server Search
  $("#popServerSearch").on("keyup", function () {
    var searchText = $(this).val().toLowerCase();
    $("#serversList > tr.filterClass").each(function () {
      var currentLiText = $(this).text().toLowerCase(),
        showCurrentLi = currentLiText.indexOf(searchText) !== -1;
      $(this).toggle(showCurrentLi);
    });
  });

  // Implements Scroll in Server List div
  $("#serverListWrapper").slimscroll({
    disableFadeOut: true,
    height: "225px",
  });

  // Tabs Default Action
  $(".tab_content").hide(); //Hide all content
  $("ul.tabs li:first").addClass("active").show(); //Activate first tab
  $(".tab_content:first").show(); //Show first tab content

  // Show Hide Graph Block for cluster lantency
  $("#showHideGraphBlock").on("click", function () {
    var userPreferences = getUserPreferences();
    if (userPreferences != null) {
      if (
        userPreferences["ClusterLatency"] != false ||
        userPreferences["ClusterTransactions"] != false ||
        userPreferences["ServerCPU"] != false ||
        userPreferences["ServerRAM"] != false ||
        userPreferences["PartitionIdleTime"] != false ||
        userPreferences["CommandLogStat"] != false ||
        userPreferences["CommandLogTables"] != false
      ) {
        var graphState = $("#mainGraphBlock").css("display");
        if (graphState == "none") {
          $(".showhideIcon").removeClass("collapsed");
          $(".showhideIcon").addClass("expanded");
        } else {
          $(".showhideIcon").removeClass("expanded");
          $(".showhideIcon").addClass("collapsed");
        }
        $("#mainGraphBlock").slideToggle();

        MonitorGraphUI.UpdateCharts();
      }
    }
  });

  //Show Hide Graph Block for importer
  $("#showHideImporterGraphBlock").on("click", function () {
    var graphState = $("#mainImporterGraphBlock").css("display");
    if (graphState == "none") {
      $(".showhideImporterIcon").removeClass("collapsed");
      $(".showhideImporterIcon").addClass("expanded");
    } else {
      $(".showhideImporterIcon").removeClass("expanded");
      $(".showhideImporterIcon").addClass("collapsed");
    }
    $("#mainImporterGraphBlock").slideToggle();
  });

  //Show Hide Graph Block for exporter
  $("#showHideExporterGraphBlock").on("click", function () {
    var graphState = $("#mainExporterGraphBlock").css("display");
    if (graphState == "none") {
      $(".showhideExporterIcon").removeClass("collapsed");
      $(".showhideExporterIcon").addClass("expanded");
    } else {
      $(".showhideExporterIcon").removeClass("expanded");
      $(".showhideExporterIcon").addClass("collapsed");
    }
    $("#mainExporterGraphBlock").slideToggle();

    var graphState = $("#queuedGraphBlock").css("display");
    if (graphState == "none") {
      $(".showhideExporterIcon").removeClass("collapsed");
      $(".showhideExporterIcon").addClass("expanded");
    } else {
      $(".showhideExporterIcon").removeClass("expanded");
      $(".showhideExporterIcon").addClass("collapsed");
    }
    $("#queuedGraphBlock").slideToggle();
  });

  //DR Show/hide toggle
  // Show Hide Graph Block
  $("#showHideDrBlock").on("click", function () {
    $(".drShowHide").toggle();
    var $this = $(this);
    var drState = $(".drShowHide").css("display");
    if (drState == "none") {
      $this.removeClass("expanded");
      $this.addClass("collapsed");
    } else {
      $this.removeClass("collapsed");
      $this.addClass("expanded");
    }
  });

  // Show Hide Command Log Performance Block
  $("#showHideCLPBlock").on("click", function () {
    $(".clpShowHide").toggle();
    var $this = $(this);
    var clpState = $(".clpShowHide").css("display");
    if (clpState == "none") {
      $this.removeClass("expanded");
      $this.addClass("collapsed");
    } else {
      $this.removeClass("collapsed");
      $this.addClass("expanded");
    }
  });

  // Shows memory alerts
  $("#showMemoryAlerts").popup();
  $(".drWarning").popup();

  //Logout popup
  $("#logOut").popup();
  $("#btnlogOut").popup();

  //Shows Save Snapshot status
  $("#btnSaveSnapshotPopup").popup({
    open: function (event, ui, ele) {
      if (
        $("#saveSnapshotStatus")
          .html()
          .indexOf("Snapshot queued successfully") > -1
      ) {
        $("#imgSaveSnapshotStatus").hide();
      } else {
        $("#imgSaveSnapshotStatus").show();
      }
    },
  });

  var refreshCss = function () {
    //Enable Schema specific css only for Schema Tab.
    if (
      VoltDbUI.CurrentTab == NavigationTabs.Schema ||
      NavigationTabs.Exporter
    ) {
      $("#styleBootstrapMin").removeAttr("disabled");
      $("#styleBootstrapResponsive").removeAttr("disabled");
      $("#styleThemeBootstrap").removeAttr("disabled");
      $("#nav").css("left", navLeft - 100);
      $("#styleBootstrapMin").removeProp("disabled");
      $("#styleBootstrapResponsive").removeProp("disabled");
      $("#styleThemeBootstrap").removeProp("disabled");
    } else {
      $("#styleBootstrapMin").attr("disabled", "disabled");
      $("#styleBootstrapResponsive").attr("disabled", "disabled");
      $("#styleThemeBootstrap").attr("disabled", "disabled");
      $("#nav").css("left", navLeft);
      //This is required for firefox
      $("#styleBootstrapMin").prop("disabled", true);
      $("#styleBootstrapResponsive").prop("disabled", true);
      $("#styleThemeBootstrap").prop("disabled", true);
    }
  };

  refreshCss();

  var navLeft = $("#nav").css("left");
  $("#nav li").on("click", function () {
    $(".contents").hide().eq($(this).index()).show();
    $("#nav li").removeClass("active");
    $(this).addClass("active");
    if (isMobile == false) {
      if (!is_iPad) {
        if (VoltDbUI.gutterInstanceHor != undefined)
          VoltDbUI.gutterInstanceHor.destroy();
        VoltDbUI.gutterInstanceHor = Split(["#a", "#inputQuery"], {
          gutterSize: 15,
          minSize: 400,
          sizes: [25, 75],
        });
      }

      var queryLength = $("#ulTabList li").length - 1;

      for (var i = 1; i <= queryLength; i++) {
        if (VoltDbUI.vars["gutterInstanceVer" + i] != undefined)
          VoltDbUI.vars["gutterInstanceVer" + i].destroy();

        VoltDbUI.vars["gutterInstanceVer" + i] = Split(
          ["#querybox-" + i, "#blockContainer" + i],
          {
            direction: "vertical",
            gutterSize: 15,
            minSize: [120, 150],
          }
        );

        VoltDbUI.vars["gutterInstanceVer" + i].setSizes([30, 70]);
      }
    }

    VoltDbUI.CurrentTab = getCurrentTab();
    refreshCss();
    saveSessionCookie("current-tab", VoltDbUI.CurrentTab);

    //Activate Shortcut keys only if the current tab is "SQL Query".
    //Also show proper help contents as per the choosen tab.
    if (VoltDbUI.CurrentTab == NavigationTabs.SQLQuery) {
      showHelpTopic("#VDBQHelp", "SQL Query Help");
      if (
        navigator.userAgent.indexOf("Safari") != -1 &&
        navigator.userAgent.indexOf("Chrome") == -1
      ) {
        shortcut.add("f6", function () {
          var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
          var element_id = element.split("-")[1];
          var btn_id = "#runBTn-" + element_id;

          if ($(btn_id).attr("disabled") != "disabled")
            $(btn_id).button().trigger("click");
        });
      } else {
        shortcut.add("F5", function () {
          var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
          var element_id = element.split("-")[1];
          var btn_id = "#runBTn-" + element_id;

          if ($(btn_id).attr("disabled") != "disabled")
            $(btn_id).button().trigger("click");
        });
      }
    } else {
      //Refresh the charts if the current tab is "DB Monitor"
      if (VoltDbUI.CurrentTab == NavigationTabs.DBMonitor) {
        showHelpTopic("#VDBMonHelp", "DB Monitor Help");
        MonitorGraphUI.UpdateCharts();
      } else if (VoltDbUI.CurrentTab == NavigationTabs.Schema) {
        showHelpTopic("#VDBSchHelp", "Schema Help");
      } else if (VoltDbUI.CurrentTab == NavigationTabs.Admin) {
        showHelpTopic("#VDBAdmHelp", "Admin Help");
      } else if (VoltDbUI.CurrentTab == NavigationTabs.DR) {
        showHelpTopic("#VDBDRHelp", "DR Help");
        MonitorGraphUI.UpdateCharts();
      } else if (VoltDbUI.CurrentTab == NavigationTabs.Importer) {
        showHelpTopic("#VDBImportHelp", "Importer Help");
        MonitorGraphUI.RefreshImporterGraph();
      } else if (VoltDbUI.CurrentTab == NavigationTabs.Exporter) {
        showHelpTopic("#VDBExportHelp", "Exporter Help");
        MonitorGraphUI.RefreshThroughputGraph();
        MonitorGraphUI.RefreshQueuedGraph();
      } else if (VoltDbUI.CurrentTab == NavigationTabs.Analysis) {
        showHelpTopic("#VDBAnalysisHelp", "Analysis Help");
      }
      shortcut.remove("f5");
      shortcut.remove("f6");
    }
    $("#overlay").hide();
  });

  //Attach the login popup to the page.
  $("body").append(voltDbRenderer.GetLoginPopup());

  var serverName = VoltDBConfig.GetDefaultServerIP();
  var portid = VoltDBConfig.GetPortId();

  //If security is enabled, then it displays login popup. After user is verified, it calls loadPage().
  //If security is not enabled, then it simply calls loadPage().
  voltDbRenderer.HandleLogin(serverName, portid, function () {
    getListOfRoles();
    voltDbRenderer.checkRolesUpdate();
    loadPage(serverName, portid);
  });

  window.onscroll = function () {
    if (
      VoltDbUI.isSchemaTabLoading &&
      VoltDbUI.CurrentTab == NavigationTabs.Schema
    )
      window.scrollTo(0, 0);
  };
});

function convertArrayOfObjectsToCSV(args) {
  var result, ctr, keys, columnDelimiter, lineDelimiter, data;

  data = args.data || null;
  if (data == null || !data.length) {
    return null;
  }

  columnDelimiter = args.columnDelimiter || ",";
  lineDelimiter = args.lineDelimiter || "\n";

  keys = Object.keys(data[0]);

  result = "";
  result += keys.join(columnDelimiter);
  result += lineDelimiter;

  data.forEach(function (item) {
    ctr = 0;
    keys.forEach(function (key) {
      if (ctr > 0) result += columnDelimiter;
      if (key == "timestamp") {
        result +=
          new Date(item[key]).getHours() +
          ":" +
          new Date(item[key]).getMinutes() +
          ":" +
          new Date(item[key]).getSeconds();
      } else result += item[key];

      ctr++;
    });
    result += lineDelimiter;
  });

  return result;
}

function downloadCSV(event, args, whichChart, chartId) {
  if (
    (navigator.userAgent.indexOf("Safari") != -1 &&
      navigator.userAgent.indexOf("Chrome") == -1) ||
    navigator.userAgent.indexOf("MSIE") > 0 ||
    navigator.userAgent.indexOf("Trident/") > 0
  ) {
    event.preventDefault();
    event.stopPropagation();
    return;
  }

  var data, filename, link;
  var graphView = $("#graphView").val();
  var drGraphVIew = $("#drGraphView").val();
  var importGraphView = $("#importerGraphView").val();
  var exportGraphView = $("#exporterGraphView").val();
  var chartData = {};

  if (whichChart == "cpu") {
    if (graphView == "Seconds") {
      chartData = JSON.parse(localStorage.cpuDetails);
    } else if (graphView == "Minutes") {
      chartData = JSON.parse(localStorage.cpuDetailsMin);
    } else if (graphView == "Days") {
      chartData = JSON.parse(localStorage.cpuDetailsDay);
    }
  } else if (whichChart == "memory") {
    if (graphView == "Seconds") {
      chartData = JSON.parse(localStorage.memoryDetails);
    } else if (graphView == "Minutes") {
      chartData = JSON.parse(localStorage.memoryDetailsMin);
    } else if (graphView == "Days") {
      chartData = JSON.parse(localStorage.memoryDetailsDay);
    }
  } else if (whichChart == "transaction") {
    if (graphView == "Seconds") {
      chartData = JSON.parse(localStorage.transDetails);
    } else if (graphView == "Minutes") {
      chartData = JSON.parse(localStorage.transDetailsMin);
    } else if (graphView == "Days") {
      chartData = JSON.parse(localStorage.transDetailsDay);
    }
  } else if (whichChart == "latency") {
    if (graphView == "Seconds") {
      chartData = JSON.parse(localStorage.latency);
    } else if (graphView == "Minutes") {
      chartData = JSON.parse(localStorage.latencyMin);
    } else if (graphView == "Days") {
      chartData = JSON.parse(localStorage.latencyDay);
    }
  } else if (whichChart == "partitionIdle") {
    if (graphView == "Seconds") {
      chartData = convertPartitionData(
        JSON.parse(localStorage.partitionDetails)
      );
    } else if (graphView == "Minutes") {
      chartData = convertPartitionData(
        JSON.parse(localStorage.partitionDetailsMin)
      );
    } else if (graphView == "Days") {
      chartData = convertPartitionData(
        JSON.parse(localStorage.partitionDetailsDay)
      );
    }
  } else if (whichChart == "dataReplication") {
    if (drGraphVIew == "Seconds") {
      chartData = JSON.parse(localStorage["drDetails_" + chartId]);
    } else if (drGraphVIew == "Minutes") {
      chartData = JSON.parse(localStorage["drDetailsMin_" + chartId]);
    } else if (drGraphVIew == "Days") {
      chartData = JSON.parse(localStorage["drDetailsDay_" + chartId]);
    }
  } else if (whichChart == "commandLog") {
    var overLayData = convertOverlayData(
      JSON.parse(localStorage.SnapshotOverlayData)
    );
    if (graphView == "Seconds") {
      chartData = JSON.parse(localStorage.cmdLog);
    } else if (graphView == "Minutes") {
      chartData = JSON.parse(localStorage.cmdLogMin);
    } else if (graphView == "Days") {
      chartData = JSON.parse(localStorage.cmdLogDay);
    }
  } else if (whichChart == "throughput") {
    if (exportGraphView == "Seconds") {
      chartData = convertExporterData(
        JSON.parse(localStorage.throughputDetails)
      );
    } else if (exportGraphView == "Minutes") {
      chartData = convertExporterData(
        JSON.parse(localStorage.throughputDetailsMin)
      );
    } else if (exportGraphView == "Days") {
      chartData = convertExporterData(
        JSON.parse(localStorage.throughputDetailsDay)
      );
    }
  } else if (whichChart == "queued") {
    if (exportGraphView == "Seconds") {
      chartData = convertExporterData(JSON.parse(localStorage.queuedDetails));
    } else if (exportGraphView == "Minutes") {
      chartData = convertExporterData(
        JSON.parse(localStorage.queuedDetailsMin)
      );
    } else if (exportGraphView == "Days") {
      chartData = convertExporterData(
        JSON.parse(localStorage.queuedDetailsDay)
      );
    }
  } else if (whichChart == "outTrans") {
    if (importGraphView == "Seconds") {
      chartData = convertImporterData(JSON.parse(localStorage.outTransDetails));
    } else if (importGraphView == "Minutes") {
      chartData = convertImporterData(
        JSON.parse(localStorage.outTransDetailsMin)
      );
    } else if (importGraphView == "Days") {
      chartData = convertImporterData(
        JSON.parse(localStorage.outTransDetailsDay)
      );
    }
  } else if (whichChart == "successRate") {
    if (importGraphView == "Seconds") {
      chartData = convertImporterData(
        JSON.parse(localStorage.successRateDetails)
      );
    } else if (importGraphView == "Minutes") {
      chartData = convertImporterData(
        JSON.parse(localStorage.successRateDetailsMin)
      );
    } else if (importGraphView == "Days") {
      chartData = convertImporterData(
        JSON.parse(localStorage.successRateDetailsDay)
      );
    }
  } else if (whichChart == "failureRate") {
    if (importGraphView == "Seconds") {
      chartData = convertImporterData(
        JSON.parse(localStorage.failureRateDetails)
      );
    } else if (importGraphView == "Minutes") {
      chartData = convertImporterData(
        JSON.parse(localStorage.failureRateDetailsMin)
      );
    } else if (importGraphView == "Days") {
      chartData = convertImporterData(
        JSON.parse(localStorage.failureRateDetailsDay)
      );
    }
  }

  var csv = convertArrayOfObjectsToCSV({
    data: chartData,
  });
  if (csv == null) return;

  if (whichChart == "dataReplication")
    filename = args.filename + "-" + drGraphVIew + ".csv";
  else if (
    whichChart == "outTrans" ||
    whichChart == "successRate" ||
    whichChart == "failureRate"
  )
    filename = args.filename + "-" + importGraphView + ".csv";
  else filename = args.filename + "-" + graphView + ".csv";

  data = encodeURI(csv);

  var blob = new Blob([csv], { type: "text/csv;" });
  saveAs(blob, filename);
}

function convertPartitionData(data) {
  var chartData = [];
  for (var i = 0; i < data.length; i++) {
    for (var j = 0; j < data[i].values.length; j++) {
      chartData.push({
        key: data[i].key.replace(" ", ""),
        type: getPartitionType(data[i].key, data[i].color),
        timestamp: data[i].values[j].x,
        value: data[i].values[j].y,
      });
    }
  }
  return chartData;
}

function convertImporterData(data) {
  var chartData = [];
  for (var i = 0; i < data.length; i++) {
    for (var j = 0; j < data[i].values.length; j++) {
      chartData.push({
        key: data[i].key.replace(" ", ""),
        timestamp: data[i].values[j].x,
        value: data[i].values[j].y,
      });
    }
  }
  return chartData;
}

function convertExporterData(data) {
  var chartData = [];
  for (var i = 0; i < data.length; i++) {
    for (var j = 0; j < data[i].values.length; j++) {
      chartData.push({
        key: data[i].key.replace(" ", ""),
        timestamp: data[i].values[j].x,
        value: data[i].values[j].y,
      });
    }
  }
  return chartData;
}

function getPartitionType(key, color) {
  var type = "local-partitiion";
  if (color == MonitorGraphUI.enumPartitionColor.maxMinPartition) {
    if (key.substr(key.length - 4, 3) == "Max") {
      type = "maximum-partition";
    } else {
      type = "minimum-partition";
    }
  } else if (color == MonitorGraphUI.enumPartitionColor.multiPartition) {
    type = "multi-partition";
  }
  return type;
}

function convertOverlayData(data) {
  var chartData = [];
  for (var i = 0; i < data.length; i++) {
    var startTime = new Date(data[i].startTime);
    var endTime = new Date(data[i].endTime);
    var starthours = startTime.getHours();
    var startminutes = "0" + startTime.getMinutes();
    var startseconds = "0" + startTime.getSeconds();
    var startmilliseconds =
      startTime.getMilliseconds() < 100
        ? "0" + startTime.getMilliseconds()
        : startTime.getMilliseconds();
    var startformattedTime =
      starthours +
      ":" +
      startminutes.substr(-2) +
      ":" +
      startseconds.substr(-2) +
      ":" +
      startmilliseconds;
    var endhours = endTime.getHours();
    var endminutes = "0" + endTime.getMinutes();
    var endseconds = "0" + endTime.getSeconds();
    var endmilliseconds = endTime.getMilliseconds();
    var endformattedTime =
      endhours +
      ":" +
      endminutes.substr(-2) +
      ":" +
      endseconds.substr(-2) +
      ":" +
      endmilliseconds;

    chartData.push({
      startTime: startformattedTime,
      endTime: endformattedTime,
    });
  }
  return chartData;
}

function logout() {
  saveSessionCookie("username", null);
  saveSessionCookie("password", null);
  if (VoltDbAdminConfig.isRoleChanged) {
    saveSessionCookie("role", -1);
  } else {
    saveSessionCookie("role", null);
  }
  saveSessionCookie("current-tab", NavigationTabs.DBMonitor);
  $("#logOut").prop("title", "");
  location.href = "/logout";
}

function changePassword(obj) {
  var id = obj.id;
  $(obj).css("display", "none");
  $(obj.parentElement)
    .children("input")
    .each(function () {
      $(this).css("display", "inline-block"); // "this" is the current element in the loop
    });
}

var loadPage = function (serverName, portid) {
  $(".drShowHide").show();
  $(".clpShowHide").show();
  $("#showHideDrBlock").removeClass("collapsed");
  $("#showHideDrBlock").addClass("expanded");
  $("#showHideCLPBlock").removeClass("collapsed");
  $("#showHideCLPBlock").addClass("expanded");
  loadAnalysisPage();
  var userName =
    VoltDbUI.getCookie("username") != undefined
      ? VoltDbUI.getCookie("username")
      : "";
  var password =
    VoltDbUI.getCookie("password") != undefined
      ? VoltDbUI.getCookie("password")
      : "";

  voltDbRenderer.ChangeServerConfiguration(
    serverName,
    portid,
    userName,
    password,
    true,
    true
  );
  voltDbRenderer.ShowUsername(userName);

  loadSQLQueryPage(serverName, portid, userName);
  VoltDbUI.refreshSqlAndSchemaTab();

  VoltDbUI.partitionGraphInterval = setInterval(function () {
    if (getCurrentServer() != undefined) {
      window.clearInterval(VoltDbUI.partitionGraphInterval);
      voltDbRenderer.GetPartitionIdleTimeInformation(function (
        partitionDetail
      ) {
        MonitorGraphUI.GetPartitionDetailData(partitionDetail);
      });
    }
  }, 5000);

  showEnableDisableDownloadBtn();
  function showEnableDisableDownloadBtn() {
    if (
      (navigator.userAgent.indexOf("Safari") != -1 &&
        navigator.userAgent.indexOf("Chrome") == -1) ||
      navigator.userAgent.indexOf("MSIE") > 0 ||
      navigator.userAgent.indexOf("Trident/") > 0
    ) {
      $(".downloadCls").attr("src", "images/icon_download_disabled.png");
      $(".downloadCls").attr(
        "title",
        "Download file feature is not supported in this browser."
      );
      $(".downloadCls").css("cursor", "default");
    } else {
      $(".downloadCls").attr("src", "images/downloadBtn.png");
      $(".downloadCls").attr("title", "Download data as CSV");
      $(".downloadCls").css("cursor", "pointer");
    }
  }

  var showAdminPage = function () {
    if (VoltDbAdminConfig.isAdmin) {
      if (
        VoltDbUI.getCookie("sql_port_for_paused_db") ==
        sqlPortForPausedDB.UseAdminPort
      ) {
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
        setTimeout(function () {
          $("#navSchema > a").trigger("click");
        }, 100);
      } else if (curTab == NavigationTabs.SQLQuery) {
        $("#overlay").show();
        setTimeout(function () {
          $("#navSqlQuery > a").trigger("click");
        }, 100);
      } else if (curTab == NavigationTabs.Admin) {
        if (VoltDbAdminConfig.isAdmin) {
          $("#overlay").show();
          setTimeout(function () {
            $("#navAdmin > a").trigger("click");
          }, 100);
        } else {
          saveSessionCookie("current-tab", NavigationTabs.DBMonitor);
        }
      } else if (curTab == NavigationTabs.DR) {
        //Do nothing
      } else if (curTab == NavigationTabs.Analysis) {
        $("#overlay").show();
        setTimeout(function () {
          $("#navAnalysis > a").trigger("click");
        }, 100);
      } else {
        setTimeout(function () {
          $("#navDbmonitor > a").trigger("click");
        }, 100);
      }
    }
  };

  var securityChecks = {
    securityChecked: false,
    previlegesChecked: false,
  };

  //Load Admin configurations
  voltDbRenderer.GetAdminDeploymentInformation(
    true,
    function (adminConfigValues, rawConfigValues) {
      securityChecks.securityChecked = true;
      //Show admin page if security is turned off.
      if (
        adminConfigValues != null &&
        adminConfigValues.VMCNoPermission != true &&
        !adminConfigValues.security
      ) {
        $("#navAdmin").show();
      } else if (!VoltDbAdminConfig.isAdmin) {
        $("#navAdmin").hide();
      }

      retainCurrentTab();
    }
  );

  voltDbRenderer.CheckAdminPriviledges(function (hasAdminPrivileges) {
    securityChecks.previlegesChecked = true;
    if (hasAdminPrivileges) {
      $("#navAdmin").show();
      loadAdminPage();
    } else if (!VoltDbAdminConfig.isAdmin) {
      $("#navAdmin").hide();
    }

    retainCurrentTab();
  });

  $("#loginWarnPopup").popup({
    afterOpen: function (event, ui, ele) {
      var popup = $(this)[0];

      $("#btnLoginWarningOk").unbind("click");
      $("#btnLoginWarningOk").on("click", function () {
        if (!VoltDbUI.hasPermissionToView) {
          window.location.reload();
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
    closeContent: "",
    modal: true,
  });

  var defaultSearchTextProcedure = "Search Stored Procedures";
  var defaultSearchTextTable = "Search Database Tables";

  VoltDbUI.CurrentTab = getCurrentTab();

  RefreshServerUI();

  var version = "";
  var setVersionCheckUrl = function (currentServer) {
    if (version == "") {
      version = voltDbRenderer.getVersion(currentServer);
      $("#versioncheck").attr(
        "src",
        "http://community.voltdb.com/versioncheck?app=vmc&ver=" + version
      );
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

        $(".activeServerName")
          .html(htmlData.ServerInformation[1].CurrentServer)
          .attr("title", htmlData.ServerInformation[1].CurrentServer);
        $("#serversList").html(htmlData.ServerInformation[0].ServersList);
        setVersionCheckUrl(htmlData.ServerInformation[1].CurrentServer);

        //Trigger search on the newly loaded list. This is required to
        //search server since we are refreshing the server list.
        if ($("#popServerSearch").val() != "Search Server")
          $("#popServerSearch").trigger("keyup");
      });

      //hide loading icon
      $("#overlay").hide();

      $("#serversList > tr > td.active > a").on("click", function () {
        var clickedServer = $(this).html();
        $(".activeServerName").html(clickedServer).attr("title", clickedServer);

        if ($("#popServerList").css("display") == "none") {
          $("#btnPopServerList").removeClass("showServers");
          $("#btnPopServerList").addClass("hideServers");
        } else {
          $("#btnPopServerList").removeClass("hideServers");
          $("#btnPopServerList").addClass("showServers");
        }

        $("#popServerList").toggle("slide", { direction: "right" }, 1500);
        $(this).parent().prevAll().removeClass("monitoring");
        $(this).parent().nextAll().removeClass("monitoring");
        $(this).parent().addClass("monitoring");

        var serverIp = voltDbRenderer.getServerIP($(this).attr("data-ip"));
        var currentUrl = window.location.href.split("?")[0];
        var urlArray = currentUrl.split("/");
        var newUrl = "";
        if (urlArray != null && urlArray.length > 0) {
          var urlArray2 = urlArray[2].split(":");
          urlArray2[0] = serverIp;
          urlArray[2] = urlArray2.join(":");
          newUrl = urlArray.join("/");
        }
        var data = {
          CurrentServer: clickedServer,
          GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
          DrGraphView: VoltDbUI.getFromLocalStorage("dr-graph-view"),
          ImporterGraphVIew: VoltDbUI.getFromLocalStorage(
            "importer-graph-view"
          ),
          ExporterGraphVIew: VoltDbUI.getFromLocalStorage(
            "exporter-graph-view"
          ),
          DisplayPreferences: VoltDbUI.getFromLocalStorage("user-preferences"),
          AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
          username: VoltDbUI.getCookie("username"),
          password: VoltDbUI.getCookie("password"),
        };

        var win = window.open(
          newUrl + "?data=" + encodeURIComponent(JSON.stringify(data)),
          "_parent"
        );
        win.focus();
      });

      var lUserPreferences = getUserPreferences();
      showHideGraph(lUserPreferences);
    };
    var loadAdminTabPortAndOverviewDetails = function (
      portAndOverviewValues,
      serverSettings
    ) {
      VoltDbAdminConfig.displayPortAndRefreshClusterState(
        portAndOverviewValues,
        serverSettings
      );
    };

    var loadAdminServerList = function (serverList) {
      VoltDbAdminConfig.refreshServerList(serverList);
      $(VoltDbAdminConfig.runningServerIds).on("click", function () {
        openPopup($(this));
      });

      $(".tblshutdown  >tbody > tr.activeHost > td:first-child > a").on(
        "click",
        function () {
          var clickedServer = $(this).html();
          var serverIp = voltDbRenderer.getServerIP(
            $(this)
              .parent()
              .siblings("td:first")
              .next()
              .find("a")
              .attr("data-hostid")
          );
          var currentUrl = window.location.href.split("?")[0];
          var urlArray = currentUrl.split("/");
          var newUrl = "";
          if (urlArray != null && urlArray.length > 0) {
            var urlArray2 = urlArray[2].split(":");
            urlArray2[0] = serverIp;
            urlArray[2] = urlArray2.join(":");
            newUrl = urlArray.join("/");
          }
          var data = {
            CurrentServer: clickedServer,
            GraphView: VoltDbUI.getFromLocalStorage("graph-view"),
            DrGraphView: VoltDbUI.getFromLocalStorage("dr-graph-view"),
            ImporterGraphVIew: VoltDbUI.getFromLocalStorage(
              "importer-graph-view"
            ),
            ExporterGraphVIew: VoltDbUI.getFromLocalStorage(
              "exporter-graph-view"
            ),
            DisplayPreferences:
              VoltDbUI.getFromLocalStorage("user-preferences"),
            AlertThreshold: VoltDbUI.getFromLocalStorage("alert-threshold"),
            username: VoltDbUI.getCookie("username"),
            password: VoltDbUI.getCookie("password"),
            tab: "admin",
          };

          var win = window.open(
            newUrl + "?data=" + encodeURIComponent(JSON.stringify(data)),
            "_parent"
          );
          win.focus();
        }
      );
    };

    var openPopup = function (srcElement) {
      var i = 0;
      var hostName = srcElement.attr("data-HostName");
      var hostId = srcElement.attr("data-HostId");
      var idleServerDetails;

      var popup = new $.Popup({
        content: "#stopConfirmationPop",

        afterOpen: function () {
          $(document).off("click", "#StopConfirmOK");
          $(document).on("click", "#StopConfirmOK", function () {
            //API Request
            try {
              voltDbRenderer.stopServer(
                hostId,
                hostName,
                function (success, statusString) {
                  if (success) {
                    adminClusterObjects.ignoreServerListUpdateCount = 2;
                    updateServers(hostId, hostName, "MISSING");
                    $("#stopServer_" + hostName).addClass("disableServer");
                    $("#stopServer_" + hostName + " span").addClass(
                      "shutdownServer"
                    );
                    $("#stopServer_" + hostName + " span").addClass(
                      "stopDisable"
                    );
                  } else {
                    $("#errorLabel").text(statusString);
                    var popup = new $.Popup({
                      content: "divStopServerError",
                      afterOpen: function () {
                        $(document).off("click", "#A2");
                        $(document).on("click", "#A2", function () {
                          popup.close();
                        });
                      }
                    });
                    popup.open("#divStopServerError", undefined, srcElement);
                  }
                }
              );
            } catch (error) {
              console.log(error);
            }
            //Close the popup
            $($(this).siblings()[0]).trigger("click");
          });

          $("#StopConfirmCancel").unbind("click");
          $(document).on("click", "#StopConfirmCancel", function () {
            popup.close();
          });
        },
      });
      popup.open("#stopConfirmationPop", undefined, srcElement);
    };

    voltDbRenderer.GetSystemInformation(
      loadClusterHealth,
      loadAdminTabPortAndOverviewDetails,
      loadAdminServerList
    );

    //Load Admin configurations
    voltDbRenderer.GetAdminDeploymentInformation(
      false,
      function (adminConfigValues, rawConfigValues) {
        if (!VoltDbUI.hasPermissionToView) return;
        if (rawConfigValues !== undefined && rawConfigValues.status == -3 && VoltDbAdminConfig.isAdmin) {
          setTimeout(function () {
            var checkPermission = function () {
              if (!VoltDbUI.hasPermissionToView) return;
              else
                $("#loginWarningPopupMsg").text(
                  "Security settings have been changed. You no longer have permission to view Admin Tab."
                );

              if (!$("#loginWarningPopup").is(":visible")) {
                $("#loginWarnPopup").trigger("click");
              }
            };
            voltDbRenderer.GetSystemInformation(
              checkPermission,
              function (portAndOverviewValues, serverSettings) { },
              function (data) { }
            );
          }, 2000);
        } else {
          VoltDbAdminConfig.displayAdminConfiguration(
            adminConfigValues,
            rawConfigValues
          );
        }
      }
    );

    //Get System Overview information
    voltDbRenderer.GetDeploymentInformation(function (deploymentDetails) {
      if (deploymentDetails != undefined) {
        var siteCount = deploymentDetails.DETAILS.SITECOUNT;
        var hostCount = deploymentDetails.DETAILS.HOSTCOUNT;
        //check whether command log is enabled or not
        VoltDbUI.isCommandLogEnabled =
          deploymentDetails.DETAILS.COMMANDLOGSTATUS;
        var clusterDetails = voltDbRenderer.getClusterDetail(
          getCurrentServer()
        );
        if (clusterDetails != undefined) {
          if (
            clusterDetails.MODE != undefined &&
            clusterDetails.VERSION != undefined &&
            clusterDetails.BUILDSTRING != undefined &&
            clusterDetails.UPTIME != undefined
          ) {
            $("#mode").html(clusterDetails.MODE);
            $("#voltdbVersion").html(clusterDetails.VERSION);
            $("#buildString").html(clusterDetails.BUILDSTRING);
            $("#clusterComposition").html(
              hostCount +
              " hosts with " +
              hostCount * siteCount +
              " sites (" +
              siteCount +
              " per host)"
            );
            $("#runningSince").html(
              getRunningTimeInfo(
                parseInt(clusterDetails.STARTTIME),
                clusterDetails.UPTIME
              )
            );
            getLicenseInformation(clusterDetails.LICENSE);
          }
        }
      }
    });

    var getRunningTimeInfo = function (startTime, upTime) {
      var strTime = new Date(startTime).toUTCString();
      var upTime1 = upTime.split(" ");
      var upTimeHrMin = upTime1[2].split(":");
      var runningSince =
        strTime +
        " (" +
        parseInt(upTime1[0]) +
        "d " +
        parseInt(upTimeHrMin[0]) +
        "h " +
        parseInt(upTimeHrMin[1]) +
        "m)";
      return runningSince;
    };

    var getLicenseInformation = function (licenseInfo) {
      if (licenseInfo != undefined && licenseInfo != "") {
        var licInfo = JSON.parse(licenseInfo);
        if (!VoltDbAdminConfig.isExportLoading) {
          $("#addNewConfigLink").show();
        }
        $(".licenseInfo").show();
        $("#tdLicenseInfo").hide();
        $("#tdLicenseInfo").css("display", "none");
        $("#tdLicenseType").html(licInfo.type);
        $("#tdExpiration").html(licInfo.expiration);
        $("#tdHostCount").html(licInfo.hostcount);
        $("#tdWanReplication").html(
          licInfo.wanreplication == undefined
            ? ""
            : licInfo.wanreplication.toString()
        );
        $("#tdCommandLogging").html(
          licInfo.commandlogging == undefined
            ? ""
            : licInfo.commandlogging.toString()
        );
        $("#tdTrial").html(
          licInfo.trial == undefined ? "" : licInfo.trial.toString()
        );
      } else {
        $("#addNewConfigLink").hide();
        $(".licenseInfo").hide();
        $("#tdLicenseInfo").show();
      }
    };
  };

  var showHideCmdlogDataAndCharts = function (
    cmdLogStatus,
    graphView,
    currentTab
  ) {
    var logTableSpanSelector = $("#liCommandLogTables").find("span");
    var logTableInputSelector = $("#liCommandLogTables").find("input");
    var logStatSpanSelector = $("#liCommandLogStat").find("span");
    var logStatInputSelector = $("#liCommandLogStat").find("input");
    if (cmdLogStatus == "true") {
      logStatSpanSelector.html("Command Log Statistics");
      logStatSpanSelector.removeClass("notActive");
      logStatInputSelector.removeAttr("disabled");
      logStatInputSelector.removeAttr("title");
      logTableSpanSelector.html("Command Log Data");
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
      logStatSpanSelector.html("Command Log Statistics (not active)");
      logStatSpanSelector.addClass("notActive");
      logStatInputSelector.attr("disabled", true);
      logStatInputSelector.attr("title", "This feature is not active.");
      logStatInputSelector.attr("checked", false);
      $("#chartCommandLogging").hide();
      $("#divCommandLog").hide();

      logTableSpanSelector.html("Command Log Data (not active)");
      logTableSpanSelector.addClass("notActive");
      logTableInputSelector.attr("disabled", true);
      logTableInputSelector.attr("title", "This feature is not active.");
      logTableInputSelector.attr("checked", false);
    }
  };

  var previousTupleCount = {};
  var throughput = {};
  var refreshGraphAndData = function (graphView, currentTab) {
    voltDbRenderer.GetExportProperties(function (rawData) {
      VoltDbAdminConfig.exportTypes = rawData;
    });

    voltDbRenderer.getMemoryGraphInformation(function (memoryDetails) {
      MonitorGraphUI.RefreshMemory(
        memoryDetails,
        getCurrentServer(),
        graphView,
        currentTab
      );
    });

    voltDbRenderer.getLatencyGraphInformation(function (latencyDetails) {
      MonitorGraphUI.RefreshLatency(
        latencyDetails,
        graphView,
        currentTab,
        getCurrentServer()
      );
      MonitorGraphUI.RefreshTransaction(
        latencyDetails,
        graphView,
        currentTab,
        getCurrentServer()
      );
    });

    voltDbRenderer.getCpuGraphInformation(function (cpuDetails) {
      MonitorGraphUI.RefreshCpu(
        cpuDetails,
        getCurrentServer(),
        graphView,
        currentTab
      );
    });

    voltDbRenderer.getImporterGraphInformation(function (importerDetails) {
      if (!$.isEmptyObject(importerDetails)) {
        var curTab = VoltDbUI.getCookie("current-tab");
        graphView = $("#importerGraphView").val();
        $("#navImporter").show();
        if (
          curTab == NavigationTabs.Importer &&
          !$("#navImporter").hasClass("active")
        ) {
          $("#overlay").show();
          setTimeout(function () {
            $("#navImporter> a").trigger("click");
          }, 100);
        }

        if (VoltDbUI.isFirstImporterLoad) {
          MonitorGraphUI.SetImporterData(importerDetails);
          MonitorGraphUI.AddImporterGraph(
            VoltDbUI.getFromLocalStorage("importer-graph-view"),
            $("#chartOutTransaction"),
            $("#chartSuccessRate"),
            $("#chartFailureRate")
          );
          VoltDbUI.isFirstImporterLoad = false;
        }

        var dataMapper = MonitorGraphUI.getImportMapperData();
        var colorIndex = MonitorGraphUI.getDataMapperIndex(dataMapper);
        var dataArray = [
          "outTransData_second",
          "outTransDataMin_minute",
          "outTransDataDay_day",
          "successRateData_second",
          "successRateDataMin_minute",
          "successRateDataDay_day",
          "failureRateData_second",
          "failureRateDataMin_minute",
          "failureRateDataDay_day",
        ];
        $.each(importerDetails["SUCCESSES"], function (key, value) {
          if (key != "TIMESTAMP" && !dataMapper.hasOwnProperty(key)) {
            for (var i = 0; i < dataArray.length; i++) {
              var dataSplit = dataArray[i].split("_");
              MonitorGraphUI.AddImporterGraphLine(
                dataSplit[0],
                key,
                dataSplit[1],
                colorIndex
              );
            }
          }
        });

        MonitorGraphUI.RefreshOutTransGraph(
          importerDetails["OUTSTANDING_REQUESTS"],
          graphView,
          curTab
        );
        MonitorGraphUI.RefreshSuccessRateGraph(
          importerDetails["SUCCESSES"],
          graphView,
          curTab
        );
        MonitorGraphUI.RefreshFailureRateGraph(
          importerDetails["FAILURES"],
          graphView,
          curTab
        );
        MonitorGraphUI.RefreshImporterGraph(
          VoltDbUI.getFromLocalStorage("importer-graph-view")
        );
      } else {
        if ($("#navImporter").hasClass("active"))
          setTimeout(function () {
            $("#navDbmonitor > a").trigger("click");
          }, 100);
        $("#navImporter").hide();
      }
    });

    voltDbRenderer.getExportTableInformation(function (
      exporterDetails,
      deploymentDetails
    ) {
      if (!$.isEmptyObject(exporterDetails)) {
        let dummyRow = $("#DUMMY_EXPORT_PLACE_HOLDER");
        if (dummyRow.length) {
          // trick to make the table-sorter work for export table
          // seems if I build from an empty table, the table-sorter won't work
          // so put a dummy row first and remove it.
          dummyRow.remove();
        }
        // update to clean the cache
        const exportTable = $("#exportTable");
        exportTable
          .trigger("update")
          .trigger("sorton", [exportTable.get(0).config.sortList])
          .trigger("appendCache")
          .trigger("applyWidgets");

        const exportConfigs = deploymentDetails["export"]["configuration"];
        Object.keys(exporterDetails["TUPLE_COUNT"]).forEach((key) => {
          var type = "";
          const target = exporterDetails["TARGET"][key];
          for (let i = 0; i < exportConfigs.length; i++) {
            if (exportConfigs[i]["target"].toUpperCase() === target) {
              type = exportConfigs[i]["type"];
              break;
            }
          }
          var tupleCountDetails = exporterDetails["TUPLE_COUNT"];

          if ($("#" + key).length == 0 && key != "TIMESTAMP") {
            var newRow =
              "<tr id=" +
              key +
              ">" +
              "<td>" +
              key +
              "</td><td>" +
              exporterDetails["TARGET"][key] +
              "</td><td>" +
              type +
              "</td><td>" +
              0 +
              "</td><td>" +
              exporterDetails["TUPLE_PENDING"][key] +
              "</td></tr>";
            let $rowRef = $(newRow);
            $("#exportTable")
              .append($rowRef)
              .trigger("addRows", [$rowRef, true]); // trigger addRows() to resort the table
          }

          if (!previousTupleCount.hasOwnProperty(key)) {
            previousTupleCount[key] = tupleCountDetails[key];
          }

          if (previousTupleCount.hasOwnProperty(key) && key != "TIMESTAMP") {
            if (tupleCountDetails[key] >= previousTupleCount[key]) {
              tupleCountDetails[key] =
                tupleCountDetails[key] - previousTupleCount[key];
            }
          }

          $("#" + key + " td:nth-child(4)").html(tupleCountDetails[key]);
          $("#" + key + " td:nth-child(5)").html(
            exporterDetails["TUPLE_PENDING"][key]
          );

          if (exporterDetails["ACTIVE"][key] == "FALSE") {
            $("#" + key).css("color", "red");
          } else {
            $("#" + key).css("color", "black");
          }
        });
      } else {
        if ($("#navExporter").hasClass("active"))
          setTimeout(function () {
            $("#navDbmonitor > a").trigger("click");
          }, 100);
        $("#navExporter").hide();
      }
    });

    voltDbRenderer.getThroughputGraphInformation(function (exporterDetails) {
      if (!$.isEmptyObject(exporterDetails)) {
        var curTab = VoltDbUI.getCookie("current-tab");
        var tupleCount = exporterDetails["TUPLE_COUNT"];

        graphView = $("#exporterGraphView").val();
        $("#navExporter").show();
        if (
          curTab == NavigationTabs.Exporter &&
          !$("#navExporter").hasClass("active")
        ) {
          $("#overlay").show();
          setTimeout(function () {
            $("#navExporter> a").trigger("click");
          }, 100);
        }

        if (VoltDbUI.isFirstThroughputLoad) {
          MonitorGraphUI.SetThroughputData(exporterDetails);
          MonitorGraphUI.AddThroughputGraph(
            VoltDbUI.getFromLocalStorage("exporter-graph-view"),
            $("#chartThroughput")
          );
          VoltDbUI.isFirstThroughputLoad = false;
        }

        Object.keys(tupleCount).forEach((key) => {
          if (previousTupleCount.hasOwnProperty(key) && key != "TIMESTAMP") {
            if (!throughput.hasOwnProperty(key)) {
              throughput[key] = 0;
            }
            throughput[key] = tupleCount[key] - previousTupleCount[key];
          }
        });
        throughput.TIMESTAMP = tupleCount.TIMESTAMP;
        previousTupleCount = tupleCount;

        var dataMapper = MonitorGraphUI.getExportMapperData();
        var colorIndex = MonitorGraphUI.getDataMapperIndex(dataMapper);
        var dataArray = [
          "throughputData_second",
          "throughputDataMin_minute",
          "throughputDataDay_day",
        ];
        $.each(throughput, function (key, value) {
          if (key != "TIMESTAMP" && !dataMapper.hasOwnProperty(key)) {
            for (var i = 0; i < dataArray.length; i++) {
              var dataSplit = dataArray[i].split("_");
              MonitorGraphUI.AddExporterGraphLine(
                dataSplit[0],
                key,
                dataSplit[1],
                colorIndex
              );
            }
          }
        });

        MonitorGraphUI.RefreshThroughputExporterGraph(
          VoltDbUI.getFromLocalStorage("exporter-graph-view")
        );
        MonitorGraphUI.RefreshThroughputGraph(throughput, graphView, curTab);
      } else {
        if ($("#navExporter").hasClass("active"))
          setTimeout(function () {
            $("#navDbmonitor > a").trigger("click");
          }, 100);
        $("#navExporter").hide();
      }
    });

    voltDbRenderer.getQueuedGraphInformation(function (exporterDetails) {
      if (!$.isEmptyObject(exporterDetails)) {
        var curTab = VoltDbUI.getCookie("current-tab");
        graphView = $("#exporterGraphView").val();
        $("#navExporter").show();
        if (
          curTab == NavigationTabs.Exporter &&
          !$("#navExporter").hasClass("active")
        ) {
          $("#overlay").show();
          setTimeout(function () {
            $("#navExporter> a").trigger("click");
          }, 100);
        }

        if (VoltDbUI.isFirstQueuedLoad) {
          MonitorGraphUI.SetQueuedData(exporterDetails);
          MonitorGraphUI.AddQueuedGraph(
            VoltDbUI.getFromLocalStorage("exporter-graph-view"),
            $("#chartQueued")
          );
          VoltDbUI.isFirstQueuedLoad = false;
        }

        var dataMapper = MonitorGraphUI.getExportMapperData();
        var colorIndex = MonitorGraphUI.getDataMapperIndex(dataMapper);
        var dataArray = [
          "queuedData_second",
          "queuedDataMin_minute",
          "queuedDataDay_day",
        ];
        $.each(exporterDetails["TUPLE_PENDING"], function (key, value) {
          if (key != "TIMESTAMP" && !dataMapper.hasOwnProperty(key)) {
            for (var i = 0; i < dataArray.length; i++) {
              var dataSplit = dataArray[i].split("_");
              MonitorGraphUI.AddExporterGraphLine(
                dataSplit[0],
                key,
                dataSplit[1],
                colorIndex
              );
            }
          }
        });

        MonitorGraphUI.RefreshQueuedExporterGraph(
          VoltDbUI.getFromLocalStorage("exporter-graph-view")
        );
        MonitorGraphUI.RefreshQueuedGraph(
          exporterDetails["TUPLE_PENDING"],
          graphView,
          curTab
        );
      } else {
        if ($("#navExporter").hasClass("active"))
          setTimeout(function () {
            $("#navDbmonitor > a").trigger("click");
          }, 100);
        $("#navExporter").hide();
      }
    });

    voltDbRenderer.GetPartitionIdleTimeInformation(function (partitionDetail) {
      if (getCurrentServer() != undefined)
        MonitorGraphUI.RefreshPartitionIdleTime(
          partitionDetail,
          getCurrentServer(),
          graphView,
          currentTab
        );
    });

    showHideCmdlogDataAndCharts(
      VoltDbUI.isCommandLogEnabled,
      graphView,
      currentTab
    );

    voltDbRenderer.GetDrRoleInformation(function (drRoleDetail) {
      var role = drRoleDetail["DRROLE"][0][0];
      $("#drModeName").html(drRoleDetail["DRROLE"][0][0]);
      if (role == "MASTER" || role == "XDCR") {
        // if it is Master or XDCR, get drDetails using @statistics dr
        voltDbRenderer.GetDrDetails(function (drDetails) {
          populateDRGraphandTable(drRoleDetail, drDetails);
        });
      } else if (role == "REPLICA") {
        // if it is REPLICA, get drInfo using @statistics DRCONSUMER
        voltDbRenderer.GetDrReplicationInformation(function (replicationData) {
          populateDRGraphandTable(drRoleDetail, replicationData["DR_GRAPH"]);
        });
      } else {
        hideDrInformation();
      }
    });

    var populateDRGraphandTable = function (drRoleDetail, drDetails) {
      var response = drDetails;
      var replicaLatency = [];
      var role = drRoleDetail["DRROLE"][0][0];
      var clusterId = drDetails["CLUSTER_ID"];

      if (clusterId != undefined) {
        $("#drCLusterId").html(" (ID: " + clusterId + ")");
        $("#clusterId").html(" (ID: " + clusterId + ")");
      } else {
        $("#drCLusterId").html("");
        $("#clusterId").html("");
      }

      if (drRoleDetail["DRROLE"].length > 0) {
        if (
          JSON.stringify(VoltDbUI.prevDrRoleDetail) !=
          JSON.stringify(drRoleDetail)
        ) {
          VoltDbUI.prevDrRoleDetail = drRoleDetail;
          for (var i = 0; i < drRoleDetail["DRROLE"].length; i++) {
            var htmlContent = getDrHtmlContent(
              i,
              clusterId,
              drRoleDetail,
              response
            );
          }
          MonitorGraphUI.InitializeDrData();
          MonitorGraphUI.InitializeDRGraph();
          MonitorGraphUI.AddDrGraph("Seconds");
        }
        showDrGraphAndData(drRoleDetail, clusterId);
      }
    };

    var showDrGraphAndData = function (drRoleDetail, producerDbId) {
      var combinedId = "";
      var isDisabled = false;
      for (var i = 0; i <= drRoleDetail["DRROLE"].length - 1; i++) {
        var role = drRoleDetail["DRROLE"][i][1];
        combinedId = producerDbId + "_" + drRoleDetail["DRROLE"][i][2];
        if (role == "DISABLED") {
          isDisabled = true;
        }
      }
      if (isDisabled == false) {
        voltDbRenderer.GetClusterReplicaInformation(function (replicaDetail) {
          if (getCurrentServer() != undefined) {
            var isReplicaDataVisible = false;
            var isMasterDataVisible = false;
            var isDrGraphVisible = false;
            var currentServer = getCurrentServer();
            if (replicaDetail.hasOwnProperty(currentServer))
              VoltDbUI.drReplicationRole =
                replicaDetail[currentServer]["status"];
            else return;
            voltDbRenderer.GetDrStatusInformation(function (drDetails) {
              if (getCurrentServer() != undefined) {
                var drResult = drDetails["Details"]["STATUS"];
                if (
                  drResult != -2 &&
                  drDetails.hasOwnProperty(currentServer) &&
                  drDetails[currentServer].hasOwnProperty("MASTERENABLED")
                ) {
                  if (drDetails[currentServer]["STATE"] != "DISABLED") {
                    VoltDbUI.drMasterEnabled =
                      drDetails[currentServer]["MASTERENABLED"] != null &&
                        drDetails[currentServer]["MASTERENABLED"] != false
                        ? true
                        : false;
                    VoltDbUI.drMasterState = drDetails[currentServer]["STATE"];
                    //show master/replica table
                    voltDbRenderer.GetDrConsumerInformation(function (
                      drConsumerDetails
                    ) {
                      if (
                        drConsumerDetails.hasOwnProperty(currentServer) &&
                        drConsumerDetails[currentServer].hasOwnProperty("STATE")
                      )
                        VoltDbUI.drConsumerState =
                          drConsumerDetails[currentServer]["STATE"];
                      else VoltDbUI.drConsumerState = "DISABLE";
                      if (
                        !(
                          drDetails[currentServer]["STATE"] == "OFF" &&
                          VoltDbUI.drConsumerState == "DISABLE"
                        )
                      ) {
                        if (
                          !(
                            VoltDbUI.drReplicationRole.toLowerCase() ==
                            "none" && !VoltDbUI.drMasterEnabled
                          )
                        ) {
                          var userPreference = getUserPreferences();
                          VoltDbUI.isDRInfoRequired = true;
                          VoltDbUI.drStatus =
                            drDetails[currentServer]["SYNCSNAPSHOTSTATE"];
                          $("#divDrWrapperAdmin").show();
                          if (
                            VoltDbUI.drReplicationRole.toLowerCase() ==
                            "replica"
                          ) {
                            if (
                              VoltDbUI.drConsumerState.toLowerCase() !=
                              "disable"
                            ) {
                              showHideDrGraph(true);
                              isDrGraphVisible = true;
                              MonitorGraphUI.refreshGraphDR();
                              $("#drReplicaSection_" + combinedId).css(
                                "display",
                                "block"
                              );
                              isReplicaDataVisible = true;
                            } else {
                              showHideDrGraph(false);
                              $("#drReplicaSection_" + combinedId).css(
                                "display",
                                "none"
                              );
                              isReplicaDataVisible = false;
                              isDrGraphVisible = false;
                            }
                            refreshDrReplicaSection(currentTab);
                            //to show DR Mode and DR tables
                            if (
                              VoltDbUI.drMasterState.toUpperCase() == "ACTIVE"
                            ) {
                              $("#dbDrMode").text("Both");
                              $("#drMasterSection").css("display", "block");
                              isMasterDataVisible = true;
                              refreshDrMasterSection();
                              $(".replicaWrapper").css("top", "-27px");
                            } else {
                              $("#dbDrMode").text("Replica");
                              $(".replicaWrapper").css("top", "0px");
                              $("#drMasterSection").css("display", "none");
                              isMasterDataVisible = false;
                            }
                            if (
                              VoltDbUI.drConsumerState.toLowerCase() !=
                              "disable" ||
                              VoltDbUI.drMasterState.toUpperCase() == "ACTIVE"
                            ) {
                              $("#divDrReplication").show();
                            } else {
                              $("#divDrReplication").hide();
                            }
                          } else {
                            //to show DR Mode
                            if (VoltDbUI.drMasterEnabled) {
                              $("#dbDrMode").text("Master");
                              $("#drMasterSection").css("display", "block");
                              isMasterDataVisible = true;
                              refreshDrMasterSection();
                            } else {
                              isMasterDataVisible = false;
                            }
                            if (
                              VoltDbUI.drMasterEnabled &&
                              VoltDbUI.drConsumerState.toLowerCase() !=
                              "disable"
                            ) {
                              showHideDrGraph(true);
                              isDrGraphVisible = true;
                              MonitorGraphUI.refreshGraphDR();
                              $("#drReplicaSection_" + combinedId).css(
                                "display",
                                "block"
                              );
                              isReplicaDataVisible = true;
                              refreshDrReplicaSection(currentTab);
                            } else {
                              showHideDrGraph(false);
                              isDrGraphVisible = false;
                              $("#drReplicaSection_" + combinedId).css(
                                "display",
                                "none"
                              );
                              isReplicaDataVisible = false;
                            }

                            if (
                              VoltDbUI.drMasterEnabled ||
                              VoltDbUI.drConsumerState.toLowerCase() !=
                              "disable"
                            ) {
                              $("#divDrReplication").show();
                            } else {
                              $("#divDrReplication").hide();
                            }
                          }
                        } else {
                          var userPreference = getUserPreferences();
                          voltDbRenderer.GetDrInformations(function (
                            clusterInfo
                          ) {
                            $("#clusterId").show();
                            $("#clusterId").html(
                              " (ID: " +
                              clusterInfo[getCurrentServer()]["CLUSTER_ID"] +
                              ")"
                            );
                          });
                          VoltDbUI.isDRInfoRequired = true;
                          $("#divDrReplication").hide();
                          $("#divDrWrapperAdmin").show();
                          if (
                            VoltDbUI.drConsumerState.toLowerCase() != "disable"
                          ) {
                            $("#divDrReplication").show();
                            $("#drReplicaSection_" + combinedId).css(
                              "display",
                              "block"
                            );
                            isReplicaDataVisible = true;
                            showHideDrGraph(true);
                            isDrGraphVisible = true;
                            MonitorGraphUI.refreshGraphDR();
                            refreshDrReplicaSection(currentTab);
                          } else {
                            $("#divDrReplication").hide();
                            $("#drReplicaSection_" + combinedId).css(
                              "display",
                              "none"
                            );
                            isReplicaDataVisible = false;
                            showHideDrGraph(false);
                            isDrGraphVisible = false;
                          }
                          $("#drMasterSection").css("display", "none");
                          isMasterDataVisible = false;
                        }
                        if (
                          isDrGraphVisible ||
                          isMasterDataVisible ||
                          isReplicaDataVisible
                        ) {
                          var curTab = VoltDbUI.getCookie("current-tab");
                          if (
                            curTab == NavigationTabs.DR &&
                            !$("#navDR").hasClass("active") &&
                            VoltDbUI.isFirstDRLoad
                          ) {
                            $("#overlay").show();
                            setTimeout(function () {
                              $("#navDR > a").trigger("click");
                            }, 100);
                          }

                          $("#navDR").show();
                        }
                        VoltDbUI.isFirstDRLoad = false;
                      } else {
                        hideDrInformation();
                      }
                    });
                  } else {
                    hideDrInformation();
                  }
                } else {
                  hideDrInformation();
                }
              }
            });
          } else {
            hideDrInformation();
          }
        });
      } else {
        hideDrInformation();
      }
    };

    var getDrHtmlContent = function (i, producerDbId, drRoleDetail, response) {
      var combinedId = producerDbId + "_" + drRoleDetail["DRROLE"][i][2];
      var consumerDbId = drRoleDetail["DRROLE"][i][2];

      if (i == 0) {
        $("#drProducerId").html(producerDbId);
        $("#drRemoteId").html(consumerDbId);
        $("#drCombinedId").html(combinedId);
      }

      replicaLatency = [];
      for (var key in response[combinedId]) {
        if (response[combinedId][key][0] != undefined) {
          if (response[combinedId][key][0].LASTQUEUEDTIMESTAMP != undefined) {
            replicaLatency.push(
              (response[combinedId][key][0].LASTQUEUEDTIMESTAMP -
                response[combinedId][key][0].LASTACKTIMESTAMP) /
              1000000
            );
          }
        }
      }

      var showClass = "expandedDR";
      var displayCss = "display:block";
      if (
        $("#dbPane_" + i)
          .find(".menu_head")
          .hasClass("collapsedDR")
      ) {
        showClass = "collapsedDR";
        displayCss = "display:none";
      }
      $("#dbPane_" + i)
        .parent()
        .remove();

      var role = drRoleDetail["DRROLE"][i][0];
      var htmlContent =
        '<div class="containerMain1" id="containerMain_' +
        i +
        '">' +
        '    <div id="dbPane_' +
        i +
        '" class="menu_list dbPane">' +
        "        <!--Code for menu starts here-->" +
        '        <div class="menu_head drHead ' +
        showClass +
        '">' +
        '            <span class="iconDRDatabase"></span>' +
        '            <h1 class="headText1 DRHeaderWrap">' +
        '                <a href="#" id="showHideGraphBlock_' +
        i +
        '" class="showhideIcon arrowAdjustDR">' +
        '                    <span class="DRHeaderName" id="dRHeaderName_' +
        i +
        '"></span>' +
        '<span class="DRHeaderName drPending" id="drPending_' +
        i +
        '"></span>' +
        "                </a>" +
        "            </h1>" +
        '<div class="drRelation" id="drRelation_' +
        i +
        '"><div class="drRelationLeft"><span id="drArrow_' +
        i +
        '">' +
        "<p></p></span></div>" +
        '<div class="drRelationRight"><span class="iconDRDatabase"></span><div class="headText1 DRHeaderWrap">' +
        '<a href="#" class="showhideIcon arrowAdjustDR">' +
        '<span class="DRHeaderName" id="dRRemoteHeaderName_' +
        i +
        '">Database (' +
        drRoleDetail["DRROLE"][i][2] +
        ")</span>" +
        '</a></div>  <div class="latencyDR latencyDR_' +
        combinedId +
        '"></div></div><div class="clear"></div></div>' +
        '            <div class="clear"></div>' +
        "        </div>";

      var htmlGraph =
        '        <div class="menu_body drBody" style="' +
        displayCss +
        '">' +
        '            <div class="DRContantWrap">' +
        '                <div id="mainGraphBlock_' +
        combinedId +
        '">' +
        '                    <div class="errorMsgLocalStorageFull" style="display:none">' +
        '                        <div class="errorMsgLocalWrapper">' +
        '                            <img src="images/alert.png" alt="Alert"/>' +
        "                        </div>" +
        '                        <div class="textMsgLocalWrapper">' +
        "                            <p>Local storage is full. Please delete some saved queries from SQL Query tab or minimize the retained time interval using the above sliding window.</p>" +
        "                        </div>" +
        '                        <div class="clear"></div>' +
        "                    </div>" +
        '                    <div class="graphChart" id="graphChart_' +
        combinedId +
        '">' +
        '                        <div id="ChartDrReplicationRate_' +
        combinedId +
        '" class="chart chartDR" style="display: block">' +
        '                            <div class="chartHeader">' +
        "                                <h1>Database Replication (DR)" +
        '                                    <a href="#" class="downloadBtnChart" ' +
        '                                     > <img class="downloadCls" src="images/downloadBtn.png" alt="download" title="Download data as CSV"/></a>' +
        '                                    <div class="clear"></div>' +
        "                                </h1>" +
        "                            </div>" +
        '                            <svg id="visualizationDrReplicationRate_' +
        combinedId +
        '" width="100%" height="400"></svg>' +
        "                        </div>" +
        "                    </div>" +
        "                </div>";

      var htmlDrTable =
        '                <div class="drWrapper" id="divDrReplication' +
        combinedId +
        '" style="display:block">' +
        '                    <div class="content drHeader" id="drHeader' +
        combinedId +
        '">' +
        '                        <div class="leftShowhide">' +
        '                            <div class="dr">' +
        '                                <h1 class="headText1">' +
        '                                    <a href="javascript:void(0);" id="showHideDrBlock_' +
        combinedId +
        '" class="showhideIcon expanded arrowAdjust">Show/Hide Database Replication (DR)</a>' +
        "                                </h1>" +
        "                            </div>" +
        "                        </div>" +
        '                        <div class="rightShowhide">' +
        '                            <ul class="drList">' +
        "                                <li>Mode</li>" +
        '                                <li id="dbDrMode_' +
        combinedId +
        '" class="drArrow">Master</li>' +
        "                            </ul>" +
        '                            <ul class="drList">' +
        '                                <li class="alertIcon warningDr" id="drAlertWarning_' +
        combinedId +
        '" style="display: none">' +
        '                                    <a id="drWarning_' +
        combinedId +
        '" href="#drPartitionWarning" class="drWarning">' +
        '                                        <span style="margin:0 0 0 24px">Warning</span>' +
        "                                    </a>" +
        "                                </li>" +
        "                            </ul>" +
        '                            <div class="clear"></div>' +
        "                        </div>" +
        '                        <div class="clear"></div>' +
        "                    </div>" +
        '                    <div id="drSection_' +
        combinedId +
        '" class="drShowHide" style="display:block;">' +
        '                        <div id="drMasterSection_' +
        combinedId +
        '" class="masterWrapper" style="display:block;">' +
        '                            <div id="tblMAster_wrapper_' +
        combinedId +
        '" class="dataTables_wrapper no-footer">' +
        '                                <div class="tabs-filter-wrapperDR">' +
        '                                    <div class="drTitle icon-master drSearch" id="drMasterTitle_' +
        combinedId +
        '">Master</div>' +
        '                                    <div class="filter">' +
        '                                        <input name="filter" id="filterPartitionId_' +
        combinedId +
        '" type="text" class="search-box" onBlur="" placeholder="Search Partition ID"><a id="searchDrMasterData_' +
        combinedId +
        '"  href="javascript:void(0)" class="icon-search drIcon" title="Search">search</a>' +
        "                                    </div>" +
        '                                    <div class="clear"></div>' +
        "                                </div>" +
        '                                <div class="clear"></div>' +
        '                                <div class="dataTables_paginate paging_extStyleLF paginationDefault" id="tblDrMAster_' +
        combinedId +
        '_paginate">' +
        '                                    <span class="paginate_disabled_previous paginate_button" title="Previous Page">Prev</span>' +
        '                                    <div class="navigationLabel">Page <span class="pageIndex"> 0 </span> of <span class="totalPages">0</span></div>' +
        '                                    <span class="paginate_enabled_next paginate_button" title="Next Page">Next</span>' +
        "                                </div>" +
        '                                <div class="drMasterContainer">' +
        '                                    <table width="100%" border="0" cellspacing="0" id="tblDrMAster_' +
        combinedId +
        '" cellpadding="0" class="storeTbl drTbl no-footer dataTable" aria-describedby="tblDrMAster_info" role="grid">' +
        "                                        <thead>" +
        '                                            <tr role="row">' +
        '                                                <th id="Th1" width="25%" data-name="none" class="" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '"  rowspan="1" colspan="1" aria-sort="ascending" aria-label="Partition ID: activate to sort column descending">Partition ID</th>' +
        '                                                <th id="Th2" width="20%" data-name="none" class="sorting" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '" rowspan="1" colspan="1" aria-label="Status: activate to sort column ascending">Status</th>' +
        '                                                <th id="Th3" width="10%" data-name="none" class="sorting" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '" rowspan="1" colspan="1" aria-label="Total Buffer: activate to sort column ascending">Total Buffer</th>' +
        '                                                <th id="Th4" width="10%" data-name="none" class="sorting" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '" rowspan="1" colspan="1" aria-label="Buffer on disk: activate to sort column ascending">Buffer on disk</th>' +
        '                                                <th id="Th5" width="15%" data-name="none" class="sorting" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '" rowspan="1" colspan="1" aria-label="Replica Latency (ms): activate to sort column ascending">Replica Latency (ms)</th>' +
        '                                                <th id="Th6" width="20%" data-name="none" class="sorting" tabindex="0" aria-controls="tblDrMAster_' +
        combinedId +
        '" rowspan="1" colspan="1" aria-label="Replica latency (in transactions): activate to sort column ascending">Replica latency (in transactions)</th>' +
        "                                            </tr>" +
        "                                        </thead>" +
        '                                        <tbody><tr><td colspan="6"> No data to be displayed</td></tr></tbody>' +
        "                                    </table>" +
        "                                </div>" +
        "                            </div>" +
        "                        </div>" +
        '                        <div id="drReplicaSection_' +
        combinedId +
        '" class="replicaWrapper" style="display:block">' +
        '                            <div id="tblReplica_wrapper_' +
        combinedId +
        '" class="dataTables_wrapper no-footer">' +
        '                                <div class="tabs-filter-wrapperDR">' +
        '                                    <div class="drTitle icon-replica drSearch" id="drReplicaTitle_' +
        combinedId +
        '">Replica</div>' +
        '                                    <div class="filter">' +
        '                                        <input name="filter" id="filterHostID_' +
        combinedId +
        '" type="text" class="search-box" onBlur="" placeholder="Search Server"><a id="searchDrMasterData_' +
        combinedId +
        '" href="javascript:void(0)" class="icon-search drIcon" title="Search">search</a>' +
        "                                    </div>" +
        '                                    <div class="clear"></div>' +
        "                                </div>" +
        '                                <div class="clear"></div>' +
        '                                <div class="dataTables_paginate paging_extStyleLF paginationDefault" id="tblDrReplica_paginate_' +
        combinedId +
        '">' +
        '                                    <span class="paginate_disabled_previous paginate_button" title="Previous Page">Prev</span>' +
        '                                    <div class="navigationLabel">Page <span class="pageIndex"> 0 </span> of <span class="totalPages">0</span></div>' +
        '                                    <span class="paginate_enabled_next paginate_button" title="Next Page">Next</span>' +
        "                                </div>" +
        '                                <div class="drReplicaContainer">' +
        '                                    <table width="100%" border="0" cellspacing="0" id="tblDrReplica_' +
        combinedId +
        '" cellpadding="0" class="storeTbl drTbl no-footer dataTable" aria-describedby="tblDrReplica_info" role="grid">' +
        "                                        <thead>" +
        "                                            <tr>" +
        '                                                <th id="Th7" width="25%" data-name="none">Server</th>' +
        '                                                <th id="Th8" width="25%" data-name="none">Status</th>' +
        '                                                <th id="Th9" width="25%" data-name="none">Replication rate (last 1 minute)</th>' +
        '                                                <th id="Th10" width="25%" data-name="none">Replication rate (last 5 minutes)</th>' +
        "                                            </tr>" +
        "                                        </thead>" +
        '                                        <tbody><tr><td colspan="6"> No data to be displayed</td></tr></tbody>' +
        "                                    </table>" +
        "                                </div>" +
        "                            </div>" +
        "                        </div>" +
        "                    </div>" +
        "                </div>" +
        "            </div>" +
        "        </div>" +
        "    </div>" +
        "</div>";

      $("#dr").append(htmlContent + htmlGraph + htmlDrTable);
      if (role == "MASTER") {
        $("#drRelation_" + i)
          .find("p")
          .html(role + " / REPLICA");
        $("#drArrow_" + i).addClass("arrowSingle");
      } else if (role == "REPLICA") {
        $("#drRelation_" + i)
          .find("p")
          .html(role + " / MASTER");
        $("#drArrow_" + i).removeClass("arrowDouble");
        $("#drArrow_" + i).addClass("arrowSingleLeft");
      } else if (role == "XDCR") {
        $("#drRelation_" + i)
          .find("p")
          .html(role);
        $("#drArrow_" + i).removeClass("arrowSingle");
        $("#drArrow_" + i).addClass("arrowDouble");
      }

      VoltDbUI.drChartList.push(combinedId);

      $(".latencyDR_" + combinedId).html("");
      if (replicaLatency.length != 0) {
        $(".latencyDR_" + combinedId).html(
          "<p>Latency <span id='latencyDR_" +
          combinedId +
          "'>" +
          max(replicaLatency) +
          " </span> sec</p>"
        );
      } else {
        $(".latencyDR_" + combinedId).html("");
      }

      if (producerDbId != undefined) {
        $("#dRHeaderName_" + i).html("Database (" + producerDbId + ") ");
      } else {
        $("#dRHeaderName_" + i).html("Database ");
      }

      $("#drPending_" + i).html("");
      if (drRoleDetail["DRROLE"][i][1] == "PENDING") {
        $("#drPending_" + i).html("( No active connection )");
        $("#drPending_" + i).show();
        $("#drRelation_" + i).hide();
      } else {
        $("#drRelation_" + i).show();
        $("#drPending_" + i).hide();
      }

      $("#dbPane_" + i + " div.menu_head").on("click", function (e) {
        e.preventDefault();
        var id = $(this).parent().attr("id").substring(7);
        var headerState = $("#dbPane_" + id + " div.menu_body").css("display");
        if (headerState == "none") {
          $(this).removeClass("collapsedDR");
          $(this).addClass("expandedDR");
        } else {
          $(this).removeClass("expandedDR");
          $(this).addClass("collapsedDR");
        }
        $(this)
          .next("div.menu_body")
          .slideToggle(300)
          .siblings("div.menu_body")
          .slideUp("slow");
      });

      $("#showHideDrBlock_" + combinedId).on("click", function (e) {
        e.preventDefault();
        var headerState = $("#drSection_" + combinedId).css("display");
        if (headerState == "none") {
          $(this).removeClass("collapsed");
          $(this).addClass("expanded");
        } else {
          $(this).removeClass("expanded");
          $(this).addClass("collapsed");
        }
        $("#drSection_" + combinedId).slideToggle(300);
      });

      $(".downloadBtnChart").on("click", function (e) {
        var divChartNameSplit = $(this).closest(".chartDR")[0].id.split("_");
        var chartId = divChartNameSplit[1] + "_" + divChartNameSplit[2];
        var args = { filename: "dr-chart-data" };
        downloadCSV(e, args, "dataReplication", chartId);
      });
    };

    var showHideDrGraph = function (status) {
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          if (status) {
            $("#ChartDrReplicationRate_" + chartList[i]).show();
            $("#divDrView").show();
          } else {
            $("#ChartDrReplicationRate_" + chartList[i]).hide();
            $("#divDrView").hide();
          }
        }
      }
    };

    voltDbRenderer.GetDeploymentInformation(function (deploymentDetails) {
      if (deploymentDetails != undefined) {
        var clusterDetails = voltDbRenderer.getClusterDetail(
          getCurrentServer()
        );
        if (
          clusterDetails != undefined &&
          clusterDetails.LICENSE != undefined
        ) {
          licenseInfo = clusterDetails.LICENSE;

          if (licenseInfo != undefined && licenseInfo != "") {
            $("#row-7").show();
          } else {
            $("#row-7").hide();
          }
        }
      }
    });

    var max = function (array) {
      return Math.max.apply(Math, array);
    };

    var hideDrInformation = function () {
      $("#navDR").hide();
      $("#clusterId").hide();
      $("#clusterId").html("");
      VoltDbUI.isDRInfoRequired = false;
      $("#divDrReplication").hide();
      $("#divDrWrapperAdmin").hide();
      showHideDrGraph(false);
      VoltDbUI.isFirstDRLoad = false;

      var curTab = VoltDbUI.getCookie("current-tab");
      if (curTab == NavigationTabs.DR) {
        setTimeout(function () {
          $("#navDbmonitor > a").trigger("click");
        }, 100);
      }
    };

    var loadProcedureInformations = function (procedureMetadata) {
      var response = procedureMetadata;
      var htmlcontent = "";

      for (var key in response) {
        htmlcontent = htmlcontent + "<tr>";
        htmlcontent =
          htmlcontent +
          "<td>" +
          response[key].PROCEDURE +
          "</td>" +
          "<td>" +
          response[key].INVOCATIONS +
          "</td>" +
          "<td>" +
          response[key].MIN_LATENCY +
          "</td>" +
          "<td>" +
          response[key].MAX_LATENCY +
          "</td>" +
          "<td>" +
          response[key].AVG_LATENCY +
          "</td>" +
          "<td>" +
          response[key].PERC_EXECUTION +
          "</td>";
        htmlcontent = htmlcontent + "</tr>";
      }
      var leftScroll = $("#tblSP_wrapper").find(".tblScroll").scrollLeft();
      if ($.fn.dataTable.isDataTable("#tblSP")) {
        $("#tblSP").DataTable().destroy();
      }

      var content =
        "<table width='100%' border='0' cellspacing='0' id='tblSP' cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblSP_info' role='grid'>" +
        "<thead><tr role='row'><th id='PROCEDURE' width='30%' data-name='none' class='' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Stored Procedure: activate to sort column descending'>Stored Procedure</th>" +
        "<th id='INVOCATIONS' width='10%' data-name='none' class='sorting' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' >Invocations</th>" +
        "<th id='MIN_LATENCY' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' >Min Latency (ms)</th>" +
        "<th id='MAX_LATENCY' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' >Max Latency (ms)</th>" +
        "<th id='AVG_LATENCY' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' >Avg Latency (ms)</th>" +
        "<th id='PERC_EXECUTION' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblSP' rowspan='1' colspan='1' >% Time of Execution</th>" +
        "</thead><tbody>";
      $("#tblSP_wrapper")
        .find(".sPContainer")
        .html(content + htmlcontent + "</tbody></table>");
      spTable = $("#tblSP").DataTable({
        stateSave: true,
        pageLength: 5,
        sPaginationType: "extStyleLF",
        bAutoWidth: false,
        language: {
          zeroRecords: "No data to be displayed",
        },
        fnDrawCallback: function () {
          if (
            $("#tblSP").find("tbody tr td").first().html() ==
            "No data to be displayed"
          ) {
            $(this)
              .parent()
              .parent()
              .find(".dataTables_paginate .navigationLabel .pageIndex")
              .text("0");
          } else {
            $(this)
              .parent()
              .parent()
              .find(".dataTables_paginate .navigationLabel .pageIndex")
              .text(" " + this.fnPagingInfo().iPage + " ");
          }

          $(this)
            .parent()
            .parent()
            .find(".dataTables_paginate .navigationLabel .totalPages")
            .text(this.fnPagingInfo().iTotalPages);

          if (screen.width == 1600 && screen.height == 900) {
            var length = $("#tblSP tr").length - 1;
            if (length >= 5) {
              $("#divSPSection").css("min-height", "280px");
            } else if (length == 4) {
              $("#divSPSection").css("min-height", "250px");
            } else if (length == 3) {
              $("#divSPSection").css("min-height", "230px");
            } else if (length == 2) {
              $("#divSPSection").css("min-height", "200px");
            } else if (length == 1 || length == 0) {
              $("#divSPSection").css("min-height", "170px");
            }
          } else if (screen.width == 360 && screen.height == 640) {
            $("#divSPSection").css("min-height", "380px");
          } else if (screen.width == 640 && screen.height == 960) {
            alert("iphone resolution mode");
            $("#divSPSection").css("min-height", "380px");
          } else if ($(window).width() == "751") {
            $("#divSPSection").css("min-height", "350px");
          }
        },

        sDom: 'p<"tblScroll drScroll"t>',
        aoColumns: [
          null,
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
        ],
      });

      $("#tblSP_wrapper").find(".tblScroll").scrollLeft(leftScroll);
      $("#tblSP_wrapper").find(".paginationDefault").remove();

      //Customizing DataTables to make it as existing pagination
      $(".paginate_disabled_previous").html("Prev");
      $(".paginate_enabled_next").html("Next");
      $(".paginate_disabled_next").html("Next");
      $(".paginate_enabled_previous").html("Prev");

      $(".paginate_disabled_previous").attr("title", "Previous Page");
      $(".paginate_enabled_next").attr("title", "Next Page");
      $(".paginate_disabled_next").attr("title", "Next Page");
      $(".paginate_enabled_previous").attr("title", "Previous Page");

      $("#filterSP").on("keyup", function () {
        spTable.search(this.value).draw();
      });
    };

    voltDbRenderer.GetProceduresInfo(loadProcedureInformations);

    refreshDataTableSection(graphView, currentTab);
  };

  var replicationWarning = function (count) {
    if (count == 0 || count == undefined) {
      $("#drWarning").hide();
      $("#drAlertWarning").hide();
    } else {
      $("#drWarning").show();
      $("#drAlertWarning").show();
      if (count == 1) {
        $("#drPartitionWarningMsg").text(count + " partition is uncovered.");
      } else {
        $("#drPartitionWarningMsg").text(count + " partitions are uncovered.");
      }
    }
  };

  var cmdLogTable = "";
  var dtTable = "";
  var spTable = "";

  var refreshCmdLogSection = function (graphView, currentTab) {
    voltDbRenderer.GetCommandLogInformation(function (cmdLogDetails) {
      var response = cmdLogDetails;
      var htmlcontent = "";

      voltDbRenderer.GetSnapshotStatus(function (snapshotDetails) {
        cmdLogDetails[getCurrentServer()].SNAPSHOTS =
          snapshotDetails[getCurrentServer()];
        MonitorGraphUI.RefreshCommandLog(
          cmdLogDetails,
          getCurrentServer(),
          graphView,
          currentTab
        );
      });

      for (var key in response) {
        htmlcontent = htmlcontent + "<tr>";
        htmlcontent =
          htmlcontent +
          "<td>" +
          key +
          "</td>" +
          "<td>" +
          response[key].OUTSTANDING_BYTES +
          "</td>" +
          "<td>" +
          response[key].OUTSTANDING_TXNS +
          "</td>" +
          "<td>" +
          response[key].SEGMENT_COUNT +
          "</td>" +
          "<td>" +
          response[key].IN_USE_SEGMENT_COUNT +
          "</td>" +
          "<td>" +
          response[key].FSYNC_INTERVAL +
          "</td>";
        htmlcontent = htmlcontent + "</tr>";
      }
      var leftScroll = $("#tblCmdLog_wrapper").find(".tblScroll").scrollLeft();
      if ($.fn.dataTable.isDataTable("#tblCmdLog")) {
        $("#tblCmdLog").DataTable().destroy();
      }

      var content =
        "<table width='100%' border='0' cellspacing='0' id='tblCmdLog' cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblCmdLog_info' role='grid'>" +
        "<thead><tr role='row'><th id='cmdServer' width='25%' data-name='none' class='' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Server: activate to sort column descending'>Server</th>" +
        "<th id='cmdPendingBytes' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Pending (in bytes)</th>" +
        "<th id='cmdPendingTrans' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Pending (in transactions)</th>" +
        "<th id='cmdTotalSegments' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Total segments</th>" +
        "<th id='cmdSegmentsInUse' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Segments in use</th>" +
        "<th id='cmdFsyncInterval' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblCmdLog' rowspan='1' colspan='1' >Fsyncinterval</th>" +
        "</thead><tbody>";
      $("#tblCmdLog_wrapper")
        .find(".cmdLogContainer")
        .html(content + htmlcontent + "</tbody></table>");

      cmdLogTable = $("#tblCmdLog").DataTable({
        stateSave: true,
        pageLength: 5,
        sPaginationType: "extStyleLF",
        bAutoWidth: false,
        language: {
          zeroRecords: "No data to be displayed",
        },
        fnDrawCallback: function () {
          if (
            $("#tblCmdLog").find("tbody tr td").first().html() ==
            "No data to be displayed"
          ) {
            $(this)
              .parent()
              .parent()
              .find(".dataTables_paginate .navigationLabel .pageIndex")
              .text("0");
          } else {
            $(this)
              .parent()
              .parent()
              .find(".dataTables_paginate .navigationLabel .pageIndex")
              .text(" " + this.fnPagingInfo().iPage + " ");
          }

          $(this)
            .parent()
            .parent()
            .find(".dataTables_paginate .navigationLabel .totalPages")
            .text(this.fnPagingInfo().iTotalPages);

          if (screen.width == 1600 && screen.height == 900) {
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
          } else if (screen.width == 360 && screen.height == 640) {
            $("#clpSection").css("min-height", "380px");
          } else if (screen.width == 640 && screen.height == 960) {
            alert("iphone resolution mode");
            $("#clpSection").css("min-height", "380px");
          } else if ($(window).width() == "751") {
            $("#clpSection").css("min-height", "350px");
          }
        },

        sDom: 'p<"tblScroll drScroll"t>',
        aoColumns: [
          null,
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
          { bSearchable: false },
        ],
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

    $("#filterServer").on("keyup", function () {
      cmdLogTable.search(this.value).draw();
    });
  };

  var refreshDataTableSection = function (graphView, currentTab) {
    voltDbRenderer.getTablesInformation(function (tableMetadata) {
      voltDbRenderer.mapTableInformation(function (dtDetails) {
        var response = dtDetails;
        var htmlcontent = "";

        for (var key in response) {
          htmlcontent = htmlcontent + "<tr>";
          htmlcontent =
            htmlcontent +
            "<td>" +
            response[key].TABLE_NAME +
            "</td>" +
            "<td>" +
            response[key].TUPLE_COUNT +
            "</td>" +
            "<td>" +
            response[key].MAX_ROWS +
            "</td>" +
            "<td>" +
            response[key].MIN_ROWS +
            "</td>" +
            "<td>" +
            response[key].AVG_ROWS +
            "</td>" +
            "<td>" +
            response[key].TABLE_TYPE +
            "</td>";
          htmlcontent = htmlcontent + "</tr>";
        }
        var leftScroll = $("#tblDT_wrapper").find(".tblScroll").scrollLeft();
        if ($.fn.dataTable.isDataTable("#tblDT")) {
          $("#tblDT").DataTable().destroy();
        }

        var content =
          "<table width='100%' border='0' cellspacing='0' id='tblDT' cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblCmdLog_info' role='grid'>" +
          "<thead><tr role='row'><th id='TABLE_NAME' width='30%' data-name='none' class='' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Server: activate to sort column descending'>Table</th>" +
          "<th id='TUPLE_COUNT' width='10%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' >Row Count</th>" +
          "<th id='MAX_ROWS' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' >Max Rows (per partition)</th>" +
          "<th id='MIN_ROWS' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' >Min Rows (per partition)</th>" +
          "<th id='AVG_ROWS' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' >Avg Rows (per partition)</th>" +
          "<th id='TABLE_TYPE' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDT' rowspan='1' colspan='1' >Type</th>" +
          "</thead><tbody>";
        $("#tblDT_wrapper")
          .find(".dTContainer")
          .html(content + htmlcontent + "</tbody></table>");
        dtTable = $("#tblDT").DataTable({
          stateSave: true,
          pageLength: 5,
          sPaginationType: "extStyleLF",
          bAutoWidth: false,
          language: {
            zeroRecords: "No data to be displayed",
          },
          fnDrawCallback: function () {
            if (
              $("#tblDT").find("tbody tr td").first().html() ==
              "No data to be displayed"
            ) {
              $(this)
                .parent()
                .parent()
                .find(".dataTables_paginate .navigationLabel .pageIndex")
                .text("0");
            } else {
              $(this)
                .parent()
                .parent()
                .find(".dataTables_paginate .navigationLabel .pageIndex")
                .text(" " + this.fnPagingInfo().iPage + " ");
            }

            $(this)
              .parent()
              .parent()
              .find(".dataTables_paginate .navigationLabel .totalPages")
              .text(this.fnPagingInfo().iTotalPages);

            if (screen.width == 1600 && screen.height == 900) {
              var length = $("#tblDT tr").length - 1;
              if (length >= 5) {
                $("#divDTSection").css("min-height", "280px");
              } else if (length == 4) {
                $("#divDTSection").css("min-height", "250px");
              } else if (length == 3) {
                $("#divDTSection").css("min-height", "230px");
              } else if (length == 2) {
                $("#divDTSection").css("min-height", "200px");
              } else if (length == 1 || length == 0) {
                $("#divDTSection").css("min-height", "170px");
              }
            } else if (screen.width == 360 && screen.height == 640) {
              $("#divDTSection").css("min-height", "380px");
            } else if (screen.width == 640 && screen.height == 960) {
              alert("iphone resolution mode");
              $("#divDTSection").css("min-height", "380px");
            } else if ($(window).width() == "751") {
              $("#divDTSection").css("min-height", "350px");
            }
          },

          sDom: 'p<"tblScroll drScroll"t>',
          aoColumns: [
            null,
            { bSearchable: false },
            { bSearchable: false },
            { bSearchable: false },
            { bSearchable: false },
            { bSearchable: false },
          ],
        });

        $("#tblDT_wrapper").find(".tblScroll").scrollLeft(leftScroll);
        $("#tblDT_wrapper").find(".paginationDefault").remove();

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
    });

    $("#filterDT").on("keyup", function () {
      dtTable.search(this.value).draw();
    });
  };

  var refreshDrMasterSection = function () {
    $("#drMasterSection").show();
    voltDbRenderer.GetDrDetails(function (drDetails) {
      var response = drDetails;
      var htmlcontent = "";
      var replicaLatencyMs = 0;
      var replicaLatencyTrans = 0;
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          htmlcontent = "";
          for (var key in response[chartList[i]]) {
            replicaLatencyTrans =
              response[chartList[i]][key][0].LASTQUEUEDDRID -
              response[chartList[i]][key][0].LASTACKDRID;
            replicaLatencyMs =
              (response[chartList[i]][key][0].LASTQUEUEDTIMESTAMP -
                response[chartList[i]][key][0].LASTACKTIMESTAMP) /
              1000;
            htmlcontent = htmlcontent + "<tr>";
            htmlcontent =
              htmlcontent +
              "<td style='text-align: right;'>" +
              key +
              "</td>" +
              "<td >" +
              VoltDbUI.drStatus +
              "</td>" +
              "<td style='text-align: right;'>" +
              (response[chartList[i]][key][0].TOTALBYTES / 1024 / 1024).toFixed(
                2
              ) +
              "</td >" +
              "<td style='text-align: right;'>" +
              replicaLatencyMs +
              "</td >" +
              "<td style='text-align: right;'>" +
              replicaLatencyTrans +
              "</td >";
            htmlcontent = htmlcontent + "</tr>";
          }
          var leftScroll = $("#tblDrMAster_" + [chartList[i]] + "_wrapper")
            .find(".tblScroll")
            .scrollLeft();
          if ($.fn.dataTable.isDataTable("#tblDrMAster_" + [chartList[i]])) {
            $("#tblDrMAster_" + [chartList[i]])
              .DataTable()
              .destroy();
          }

          var content =
            "<table width='100%' border='0' cellspacing='0' id='tblDrMAster_" +
            [chartList[i]] +
            "'  cellpadding='0' class='storeTbl drTbl no-footer dataTable' aria-describedby='tblDrMAster_info' role='grid'>" +
            "<thead><tr role='row'><th id='partitionID_" +
            [chartList[i]] +
            "' width='20%' data-name='none' class='' tabindex='0' aria-controls='tblDrMAster_" +
            [chartList[i]] +
            "' rowspan='1' colspan='1' aria-sort='ascending' aria-label='Partition ID: activate to sort column descending'>Partition ID</th>" +
            "<th id='status_" +
            [chartList[i]] +
            "' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster_" +
            [chartList[i]] +
            "' rowspan='1' colspan='1' >Status</th>" +
            "<th id='mbOnDisk_" +
            [chartList[i]] +
            "' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster_" +
            [chartList[i]] +
            "' rowspan='1' colspan='1' >MB on disk</th>" +
            "<th id='replicaLatencyMs_" +
            [chartList[i]] +
            "' width='15%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster_" +
            [chartList[i]] +
            "' rowspan='1' colspan='1' >Replica Latency (ms)</th>" +
            "<th id='replicaLatencyTrans_" +
            [chartList[i]] +
            "' width='20%' data-name='none' class='sorting' tabindex='0' aria-controls='tblDrMAster_" +
            [chartList[i]] +
            "' rowspan='1' colspan='1'>Replica latency (in transactions)</th></tr></thead><tbody>";
          $("#tblMAster_wrapper_" + [chartList[i]])
            .find(".drMasterContainer")
            .html(content + htmlcontent + "</tbody></table>");

          $("#tblDrMAster_" + [chartList[i]]).DataTable({
            stateSave: true,
            pageLength: 5,
            sPaginationType: "extStyleLF",
            bAutoWidth: false,
            language: {
              zeroRecords: "No data to be displayed",
            },
            fnDrawCallback: function () {
              if (
                $("#tblDrMAster_" + [chartList[i]])
                  .find("tbody tr td")
                  .first()
                  .html() == "No data to be displayed"
              ) {
                $(this)
                  .parent()
                  .parent()
                  .find(".dataTables_paginate .navigationLabel .pageIndex")
                  .text("0");
              } else {
                $(this)
                  .parent()
                  .parent()
                  .find(".dataTables_paginate .navigationLabel .pageIndex")
                  .text(" " + this.fnPagingInfo().iPage + " ");
              }

              $(this)
                .parent()
                .parent()
                .find(".dataTables_paginate .navigationLabel .totalPages")
                .text(this.fnPagingInfo().iTotalPages);

              if (screen.width == 1600 && screen.height == 900) {
                var length =
                  $("#tblDrMAster_" + [chartList[i]] + " tr").length - 1;
                if (length >= 5) {
                  $("#drMasterSection_" + [chartList[i]]).css(
                    "min-height",
                    "280px"
                  );
                } else if (length == 4) {
                  $("#drMasterSection_" + [chartList[i]]).css(
                    "min-height",
                    "250px"
                  );
                } else if (length == 3) {
                  $("#drMasterSection_" + [chartList[i]]).css(
                    "min-height",
                    "230px"
                  );
                } else if (length == 2) {
                  $("#drMasterSection_" + [chartList[i]]).css(
                    "min-height",
                    "200px"
                  );
                } else if (length == 1 || length == 0) {
                  $("#drMasterSection_" + [chartList[i]]).css(
                    "min-height",
                    "170px"
                  );
                }
              } else if (screen.width == 360 && screen.height == 640) {
                $("#drMasterSection_" + [chartList[i]]).css(
                  "min-height",
                  "380px"
                );
              } else if (screen.width == 640 && screen.height == 960) {
                alert("iphone resolution mode");
                $("#drMasterSection_" + [chartList[i]]).css(
                  "min-height",
                  "380px"
                );
              }
            },

            sDom: 'p<"tblScroll drScroll"t>',
            aoColumns: [
              null,
              { bSearchable: false },
              { bSearchable: false },
              { bSearchable: false },
              { bSearchable: false },
            ],
          });

          $("#tblDrMAster_" + [chartList[i]] + "_wrapper")
            .find(".tblScroll")
            .scrollLeft(leftScroll);

          $("#tblMAster_wrapper_" + [chartList[i]])
            .find(".paginationDefault")
            .remove();

          //Customizing DataTables to make it as existing pagination
          $(".paginate_disabled_previous").html("Prev");
          $(".paginate_enabled_next").html("Next");
          $(".paginate_disabled_next").html("Next");
          $(".paginate_enabled_previous").html("Prev");

          $(".paginate_disabled_previous").attr("title", "Previous Page");
          $(".paginate_enabled_next").attr("title", "Next Page");
          $(".paginate_disabled_next").attr("title", "Next Page");
          $(".paginate_enabled_previous").attr("title", "Previous Page");

          $("#filterPartitionId_" + [chartList[i]]).on("keyup", function () {
            var id = $(this).attr("id").substring(18);
            $("#tblDrMAster_" + id)
              .DataTable()
              .search(this.value)
              .draw();
          });
        }
      }
    });
  };

  var replicaTable = "";

  var refreshDrReplicaSection = function (graphView, currentTab) {
    voltDbRenderer.GetDrReplicationInformation(function (replicationData) {
      $("#clusterId").show();
      $("#clusterId").html(
        " (ID: " + replicationData["DR_GRAPH"]["CLUSTER_ID"] + ")"
      );

      MonitorGraphUI.RefreshDrReplicationGraph(
        replicationData,
        getCurrentServer(),
        graphView,
        currentTab
      );

      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          replicationWarning(replicationData["DR_GRAPH"].WARNING_COUNT);

          var response =
            replicationData["DR_GRAPH"][chartList[i]].REPLICATION_DATA;
          var htmlcontent = "";

          for (var key in response) {
            htmlcontent = htmlcontent + "<tr>";
            htmlcontent =
              htmlcontent +
              "<td>" +
              response[key].HOSTNAME +
              "</td>" +
              "<td>" +
              response[key].STATE +
              "</td>" +
              "<td>" +
              response[key].REPLICATION_RATE_1M +
              "</td >" +
              "<td>" +
              response[key].REPLICATION_RATE_5M +
              "</td >";
            htmlcontent = htmlcontent + "</tr>";
          }

          var leftScroll = $("#tblDrReplica_wrapper_" + chartList[i])
            .find(".tblScroll")
            .scrollLeft();
          if ($.fn.dataTable.isDataTable("#tblDrReplica_" + chartList[i])) {
            $("#tblDrReplica_" + chartList[i])
              .DataTable()
              .destroy();
          }
          var content =
            " <table width='100%' border='0' cellspacing='0' id='tblDrReplica_" +
            [chartList[i]] +
            "' cellpadding='0' class='storeTbl drTbl no-footer dataTable'><thead><tr><th id='replicaServer_" +
            [chartList[i]] +
            "' width='25%' data-name='none'>Server</th><th id='replicaStatus_" +
            [chartList[i]] +
            "' width='25%' data-name='none'>Status</th><th id='replicationRate1_" +
            [chartList[i]] +
            "' width='25%' data-name='none'>Replication rate (last 1 minute)</th>" +
            "<th id='replicationRate5_" +
            chartList[i] +
            "' width='25%' data-name='none'>Replication rate (last 5 minutes)</th></tr></thead>" +
            "<tbody>";
          $("#drReplicaSection_" + chartList[i])
            .find(".drReplicaContainer")
            .html(content + htmlcontent + "</tbody></table>");

          replicaTable = $("#tblDrReplica_" + chartList[i]).DataTable({
            stateSave: true,
            pageLength: 5,
            sPaginationType: "extStyleLF",
            bAutoWidth: false,
            language: {
              zeroRecords: "No data to be displayed",
            },
            fnDrawCallback: function () {
              if (
                $("#tblDrReplica_" + chartList[i])
                  .find("tbody tr td")
                  .first()
                  .html() == "No data to be displayed"
              ) {
                $(this)
                  .parent()
                  .parent()
                  .find(".dataTables_paginate .navigationLabel .pageIndex")
                  .text("0");
              } else {
                $(this)
                  .parent()
                  .parent()
                  .find(".dataTables_paginate .navigationLabel .pageIndex")
                  .text(" " + this.fnPagingInfo().iPage + " ");
              }

              $(this)
                .parent()
                .parent()
                .find(".dataTables_paginate .navigationLabel .totalPages")
                .text(this.fnPagingInfo().iTotalPages);
              var length =
                $("#tblDrReplica_" + chartList[i] + " tr").length - 1;
              if (length >= 5) {
                $("#drReplicaSection_" + chartList[i]).css(
                  "min-height",
                  "280px"
                );
              } else if (length == 4) {
                $("#drReplicaSection_" + chartList[i]).css(
                  "min-height",
                  "250px"
                );
              } else if (length == 3) {
                $("#drReplicaSection_" + chartList[i]).css(
                  "min-height",
                  "230px"
                );
              } else if (length == 2) {
                $("#drReplicaSection_" + chartList[i]).css(
                  "min-height",
                  "200px"
                );
              } else if (length == 1 || length == 0) {
                $("#drReplicaSection_" + chartList[i]).css(
                  "min-height",
                  "170px"
                );
              }
            },
            sDom: 'p<"tblScroll drScroll"t>',
            aoColumns: [
              null,
              { bSearchable: false },
              { bSearchable: false },
              { bSearchable: false },
            ],
          });

          $("#tblDrReplica_wrapper_" + chartList[i])
            .find(".tblScroll")
            .scrollLeft(leftScroll);
          $("#tblReplica_wrapper_" + chartList[i])
            .find(".paginationDefault")
            .remove();

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

          $("#filterHostID_" + [chartList[i]]).on("keyup", function () {
            var id = $(this).attr("id").substring(13);
            $("#tblDrReplica_" + id)
              .DataTable()
              .search(this.value)
              .draw();
          });

          //to show DR Mode and DR tables
          if (VoltDbUI.drMasterState.toUpperCase() == "ACTIVE") {
            $("#dbDrMode_" + chartList[i]).text("Both");
            $("#drMasterSection_" + chartList[i]).css("display", "block");
            isMasterDataVisible = true;
            refreshDrMasterSection();
            $(".replicaWrapper").css("top", "-27px");
          } else {
            $("#dbDrMode_" + chartList[i]).text("Replica");
            $(".replicaWrapper").css("top", "0px");
            $("#drMasterSection_" + chartList[i]).css("display", "none");
            isMasterDataVisible = false;
          }
        }
      }
    });
  };

  var saveThreshold = function () {
    var defaultThreshold = 70;
    var thresholdInput = $("#threshold");

    if (thresholdInput.val() == "") {
      thresholdInput.val(defaultThreshold);
    }
    if (
      VoltDbUI.getFromLocalStorage("alert-threshold") == undefined ||
      VoltDbUI.getFromLocalStorage("alert-threshold") == null
    ) {
      saveInLocalStorage("alert-threshold", defaultThreshold);
    }

    $("#saveThreshold").on("click", function () {
      if (thresholdInput.val() == "" || thresholdInput.val() * 1 > 100) {
        alert('The value of "Alert Threshold" should be between 0 and 100.');
        return false;
      }

      var thresholdValue =
        thresholdInput.val() != "" ? thresholdInput.val() : defaultThreshold;
      saveInLocalStorage("alert-threshold", thresholdValue);
      refreshClusterHealth();
      $("#popServerList").hide();
      $("#overlay").show();
      $("#btnPopServerList").removeClass("hideServers");
      $("#btnPopServerList").addClass("showServers");
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

  if (
    VoltDbUI.getFromLocalStorage("graph-view") == undefined ||
    VoltDbUI.getFromLocalStorage("graph-view") == null
  )
    saveInLocalStorage("graph-view", $("#graphView").val());

  if (
    VoltDbUI.getFromLocalStorage("dr-graph-view") == undefined ||
    VoltDbUI.getFromLocalStorage("dr-graph-view") == null
  )
    saveInLocalStorage("dr-graph-view", $("#drGraphView").val());

  if (
    VoltDbUI.getFromLocalStorage("importer-graph-view") == undefined ||
    VoltDbUI.getFromLocalStorage("importer-graph-view") == null
  )
    saveInLocalStorage("importer-graph-view", $("#importerGraphView").val());

  if (
    VoltDbUI.getFromLocalStorage("exporter-graph-view") == undefined ||
    VoltDbUI.getFromLocalStorage("exporter-graph-view") == null
  )
    saveInLocalStorage("exporter-graph-view", $("#exporterGraphView").val());

  $("#graphView").val(VoltDbUI.getFromLocalStorage("graph-view"));
  $("#drGraphView").val(
    VoltDbUI.getFromLocalStorage("dr-graph-view") != undefined
      ? VoltDbUI.getFromLocalStorage("dr-graph-view")
      : "Seconds"
  );
  $("#importerGraphView").val(
    VoltDbUI.getFromLocalStorage("importer-graph-view") != undefined
      ? VoltDbUI.getFromLocalStorage("importer-graph-view")
      : "Seconds"
  );

  MonitorGraphUI.AddGraph(
    VoltDbUI.getFromLocalStorage("graph-view"),
    $("#chartServerCPU"),
    $("#chartServerRAM"),
    $("#chartClusterLatency"),
    $("#chartClusterTransactions"),
    $("#chartPartitionIdleTime"),
    $("#ChartDrReplicationRate"),
    $("#chartCommandLogging")
  );

  $("#graphView").on("change", function () {
    var graphView = $("#graphView").val();
    saveInLocalStorage("graph-view", graphView);
    MonitorGraphUI.RefreshGraph(graphView);
    MonitorGraphUI.UpdateCharts();
  });

  $("#drGraphView").on("change", function () {
    var drGraphView = $("#drGraphView").val();
    saveInLocalStorage("dr-graph-view", drGraphView);
    MonitorGraphUI.RefreshDrGraph(drGraphView);
    MonitorGraphUI.UpdateDrCharts();
  });

  $("#importerGraphView").on("change", function () {
    var graphView = $("#importerGraphView").val();
    saveInLocalStorage("importer-graph-view", graphView);
    MonitorGraphUI.RefreshImporterGraph(graphView);
    MonitorGraphUI.UpdateImporterCharts();
  });

  $("#exporterGraphView").on("change", function () {
    var graphView = $("#exporterGraphView").val();
    saveInLocalStorage("exporter-graph-view", graphView);
    MonitorGraphUI.RefreshThroughputExporterGraph(graphView);
    MonitorGraphUI.RefreshQueuedExporterGraph(graphView);
    MonitorGraphUI.UpdateExporterCharts();
  });

  //slides the element with class "menu_body" when paragraph with class "menu_head" is clicked
  $("#firstpane div.menu_head").on("click", function () {
    var userPreferences = getUserPreferences();
    if (userPreferences != null) {
      if (
        userPreferences["DatabaseTables"] != false ||
        userPreferences["StoredProcedures"] != false
      ) {
        var headerState = $("#firstpane div.menu_body").css("display");
        if (headerState == "none") {
          $(this).removeClass("collapsed");
          $(this).addClass("expanded");
        } else {
          $(this).removeClass("expanded");
          $(this).addClass("collapsed");
        }
        $(this)
          .next("div.menu_body")
          .slideToggle(300)
          .siblings("div.menu_body")
          .slideUp("slow");
      }
    } else {
      $("ul.user-preferences > li Input:checkbox").each(function (value) {
        userPreferences[$(this)[0].id] = true;
      });
      saveUserPreferences(userPreferences);
      $(this).removeClass("collapsed");
      $(this).addClass("expanded");
      $(this)
        .next("div.menu_body")
        .slideToggle(300)
        .siblings("div.menu_body")
        .slideUp("slow");
    }
  });

  //remove
  $("#dbPane div.menu_head").on("click", function () {
    var headerState = $("#dbPane div.menu_body").css("display");
    if (headerState == "none") {
      $(this).removeClass("collapsedDR");
      $(this).addClass("expandedDR");
    } else {
      $(this).removeClass("expandedDR");
      $(this).addClass("collapsedDR");
    }
    $(this)
      .next("div.menu_body")
      .slideToggle(300)
      .siblings("div.menu_body")
      .slideUp("slow");
  });

  refreshClusterHealth();
  MonitorGraphUI.setStartTime();
  refreshGraphAndData(
    VoltDbUI.getFromLocalStorage("graph-view"),
    VoltDbUI.CurrentTab
  ); // This is not working, causing a 5 second hang up before graphs load
  setInterval(refreshClusterHealth, 5000);
  setInterval(function () {
    refreshGraphAndData(
      VoltDbUI.getFromLocalStorage("graph-view"),
      VoltDbUI.CurrentTab
    );
  }, 5000);

  configureUserPreferences();
  adjustGraphSpacing();
  adjustImporterGraphSpacing();
  adjustExporterGraphSpacing();
  saveThreshold();

  $("#showMyHelp").popup();
  $("#ShowAbout").popup();
  $("#conPopup").popup({
    closeDialog: function () {
      VoltDbUI.isConnectionChecked = false;
      VoltDbUI.refreshConnectionTime("5000");
      $("#connectionPopup").hide();
      window.location.reload();
    },
  });

  $("#rolePopup").popup({
    open: function (event, ui, ele) {
    },
    afterOpen: function () {
      var popup = $(this)[0];
      var securityBtn = document.getElementById("btnPopOk");
      if (securityBtn != undefined) {
        securityBtn.addEventListener('click', function () {
          popup.close();
          var currentTab = VoltDbUI.getCookie('current-tab');
          if (parseInt(currentTab) !== 1) {
            $("#navDbmonitor > a").trigger("click");
          }
        })
      }
    },
    afterClose: function () {
      $("#rolePopup").unbind("click");
      $("#roleChangePopup").hide();
    },
  });

  $("#showAnalysisDetails").popup({
    open: function (event, ui, ele) {
      var procedureName = $("#hidProcedureName").html();
      $("#procedureName").html(procedureName);
      var procDetails = {};
      var finalDetails = [];
      var partitionDetails = [];
      var statement = "";
      var sum = 0;
      var j = 0;
      var count = 0;
      var isMultiple = false;
      var i = 0;

      if (VoltDbAnalysis.latencyDetailValue.length == 0) {
        $("#spanAnalysisLegend").hide();
        $("#execTimeLegend").hide();
      }
      getTooltipValues(procedureName);
      VoltDbAnalysis.latencyDetailValue.forEach(function (item) {
        var newStatement = "";
        var latValue;
        if (item.PROCEDURE == procedureName) {
          if (statement == item.STATEMENT) {
            sum += item.value;
            isMultiple = true;
          } else {
            i++;
            isMultiple = false;
            sum = item.value;
          }
          if (isMultiple) {
            count = calculateStatementCount(
              VoltDbAnalysis.latencyDetailValue,
              item.STATEMENT
            );
            if ($("#hidPartitionType").html() == "Single Partitioned") {
              procDetails[item.STATEMENT] = sum;
            } else {
              procDetails[item.STATEMENT] = sum / count;
            }
          } else {
            if ($("#hidPartitionType").html() == "Single Partitioned") {
              procDetails[item.STATEMENT] = sum;
            } else {
              procDetails[item.STATEMENT] = sum / count;
            }
          }
          statement = item.STATEMENT;
        }
      });

      if ($.isEmptyObject(procDetails)) {
        $("#spanAnalysisLegend").hide();
        $("#execTimeLegend").hide();
      } else {
        $("#spanAnalysisLegend").show();
        $("#execTimeLegend").show();
      }

      for (var key in procDetails) {
        finalDetails.push({ label: key, value: procDetails[key] });
      }
      finalDetails.sort(function (a, b) {
        return b.value > a.value ? 1 : a.value > b.value ? -1 : 0;
      });

      var z = 0;
      var orderedDetails = {};
      var statementList = [];
      for (var key in VoltDbAnalysis.totalProcessingDetail) {
        var obj = VoltDbAnalysis.totalProcessingDetail[key];
        obj.forEach(function (subItems) {
          if (subItems.PROCEDURE == procedureName) {
            count = objectLength(obj, statement);
            if (orderedDetails[subItems.STATEMENT] == undefined) {
              orderedDetails[subItems.STATEMENT] = [];
              statementList.push(subItems.STATEMENT);
            }
            orderedDetails[subItems.STATEMENT].push({
              PARTITION_ID: subItems.PARTITION_ID,
              STATEMENT: subItems.STATEMENT,
              AVG: subItems.AVG,
              z: procDetails[subItems.STATEMENT],
            });
          }
          statement = subItems.STATEMENT;
        });
        z++;
      }

      if (statementList.length > 0) {
        for (var u = 0; u < statementList.length; u++) {
          orderedDetails[statementList[u]].sort(function (a, b) {
            var nameA = a.AVG; // ignore upper and lowercase
            var nameB = b.AVG; // ignore upper and lowercase
            if (nameA > nameB) {
              return -1;
            }
            if (nameA < nameB) {
              return 1;
            }

            // names must be equal
            return 0;
          });
        }
        VoltDbUI.partitionLength = orderedDetails[statementList[0]].length;
        for (var x = 0; x < orderedDetails[statementList[0]].length; x++) {
          var u = 0;
          for (var key in orderedDetails) {
            if (partitionDetails[x] == undefined) {
              partitionDetails.push({ key: "Execution Time SP" });
              partitionDetails[x]["values"] = [];
            }
            partitionDetails[x]["values"].push({
              PARTITION_ID: orderedDetails[key][x].PARTITION_ID,
              x: orderedDetails[key][x].STATEMENT,
              y: orderedDetails[key][x].AVG,
              z: orderedDetails[key][x].z,
            });
          }
          u++;
        }
      }
      sortArray(partitionDetails);

      if ($("#hidPartitionType").html() == "Single Partitioned") {
        MonitorGraphUI.RefreshLatencyDetailGraph(partitionDetails);
      } else {
        MonitorGraphUI.RefreshLatencyDetailGraph(finalDetails);
      }
    },
  });

  $("#showAnalysisFreqDetails").popup({
    open: function (event, ui, ele) {
      var statement = "";
      var totalInvocations = 0;
      var procedureName = $("#hidProcedureName").html();
      $(".procedureName").html(procedureName);
      var finalDetails = [];
      var freqDetails = {};
      var partitionDetails = [];

      var i = 0;
      if (VoltDbAnalysis.latencyDetailValue.length == 0) {
        $("#spanFreqLegend").hide();
        $("#freqLegend").hide();
      }

      getTooltipValues(procedureName);

      VoltDbAnalysis.latencyDetailValue.forEach(function (item) {
        var newStatement = "";
        var latValue;
        if (item.PROCEDURE == procedureName) {
          newStatement = item.STATEMENT;
          if (item.type == "Single Partitioned") {
            if (newStatement == statement) {
              totalInvocations += item.INVOCATION;
            } else {
              i++;
              totalInvocations = item.INVOCATION;
            }
          } else {
            totalInvocations = item.INVOCATION;
          }

          freqDetails[item.STATEMENT] = totalInvocations;

          statement = newStatement;
        }
      });

      if ($.isEmptyObject(freqDetails)) {
        $("#spanFreqLegend").hide();
        $("#freqLegend").hide();
      } else {
        $("#spanFreqLegend").show();
        $("#freqLegend").show();
      }

      for (var key in freqDetails) {
        finalDetails.push({ label: key, value: freqDetails[key] });
      }
      finalDetails.sort(function (a, b) {
        return b.value > a.value ? 1 : a.value > b.value ? -1 : 0;
      });
      var z = 0;
      var orderedDetails = {};
      var statementList = [];
      for (var key in VoltDbAnalysis.totalProcessingDetail) {
        var obj = VoltDbAnalysis.totalProcessingDetail[key];
        obj.forEach(function (subItems) {
          if (subItems.PROCEDURE == procedureName) {
            count = objectLength(obj, statement);

            if (orderedDetails[subItems.STATEMENT] == undefined) {
              orderedDetails[subItems.STATEMENT] = [];
              statementList.push(subItems.STATEMENT);
            }

            orderedDetails[subItems.STATEMENT].push({
              PARTITION_ID: subItems.PARTITION_ID,
              STATEMENT: subItems.STATEMENT,
              INVOCATION: subItems.INVOCATIONS,
              z: freqDetails[subItems.STATEMENT],
            });
          }
          statement = subItems.STATEMENT;
        });
        z++;
      }

      if (statementList.length > 0) {
        for (var u = 0; u < statementList.length; u++) {
          orderedDetails[statementList[u]].sort(function (a, b) {
            var nameA = a.INVOCATION;
            var nameB = b.INVOCATION;
            if (nameA > nameB) {
              return -1;
            }
            if (nameA < nameB) {
              return 1;
            }
            return 0;
          });
        }
        VoltDbUI.partitionLength = orderedDetails[statementList[0]].length;
        for (var x = 0; x < orderedDetails[statementList[0]].length; x++) {
          var u = 0;
          for (var key in orderedDetails) {
            if (partitionDetails[x] == undefined) {
              partitionDetails.push({ key: "Frequency SP" });
              partitionDetails[x]["values"] = [];
            }
            partitionDetails[x]["values"].push({
              PARTITION_ID: orderedDetails[key][x].PARTITION_ID,
              x: orderedDetails[key][x].STATEMENT,
              y: orderedDetails[key][x].INVOCATION,
              z: orderedDetails[key][x].z,
            });
          }
          u++;
        }
      }

      sortArray(partitionDetails);

      if ($("#hidPartitionType").html() == "Single Partitioned") {
        MonitorGraphUI.RefreshFrequencyDetailGraph(partitionDetails);
      } else {
        MonitorGraphUI.RefreshFrequencyDetailGraph(finalDetails);
      }
    },
  });

  $("#showAnalysisCombinedDetails").popup({
    open: function (event, ui, ele) {
      var procedureName = $("#hidProcedureName").html();
      $(".procedureName").html(procedureName);
      var count = 0;
      var combinedDetails = {};
      var partitionDetails = [];
      var combinedWeight = 0;
      var statement = "";
      var finalDetails = [];
      var i = 0;
      VoltDbUI.totalProcessingTime = {};
      var sum = 0;
      var sumProcessingTime = {};

      if (VoltDbAnalysis.latencyDetailValue.length == 0) {
        $("#spanCombinedLegend").hide();
        $("#totalProcTimeLegend").hide();
      }

      getTooltipValues(procedureName);

      for (var key in VoltDbAnalysis.combinedDetail) {
        var newStatement = "";
        var obj = VoltDbAnalysis.combinedDetail[key];
        if (key == procedureName) {
          obj.forEach(function (subItems) {
            if (subItems.STATEMENT == statement) {
              if (subItems.TYPE == "Single Partitioned") {
                sum += subItems.AVG * subItems.INVOCATIONS;
              } else {
                //For Multi partitioned
                sum += subItems.AVG;
              }
            } else {
              i++;
              if (subItems.TYPE == "Single Partitioned") {
                sum = subItems.AVG * subItems.INVOCATIONS;
              } else {
                sum = subItems.AVG;
              }
            }
            statement = subItems.STATEMENT;

            if (subItems.TYPE == "Single Partitioned") {
              combinedDetails[subItems.STATEMENT] = sum;
              VoltDbUI.totalProcessingTime[subItems.STATEMENT] = sum;
            } else {
              count = objectLength(obj, statement);
              combinedWeight = (sum / count) * subItems.INVOCATIONS;
              combinedDetails[subItems.STATEMENT] = combinedWeight;
              VoltDbUI.totalProcessingTime[subItems.STATEMENT] = combinedWeight;
            }
          });

          if ($.isEmptyObject(combinedDetails)) {
            $("#spanCombinedLegend").hide();
            $("#totalProcTimeLegend").hide();
          } else {
            $("#spanCombinedLegend").show();
            $("#totalProcTimeLegend").show();
          }
          for (var key in combinedDetails) {
            finalDetails.push({ label: key, value: combinedDetails[key] });
          }

          finalDetails.sort(function (a, b) {
            return parseFloat(b.value) - parseFloat(a.value);
          });
        }
      }

      var z = 0;
      var orderedDetails = {};
      var statementList = [];
      for (var key in VoltDbAnalysis.totalProcessingDetail) {
        var obj = VoltDbAnalysis.totalProcessingDetail[key];
        obj.forEach(function (subItems) {
          if (subItems.PROCEDURE == procedureName) {
            count = objectLength(obj, statement);
            combinedWeight = subItems.AVG * subItems.INVOCATIONS;
            if (orderedDetails[subItems.STATEMENT] == undefined) {
              orderedDetails[subItems.STATEMENT] = [];
              statementList.push(subItems.STATEMENT);
            }
            orderedDetails[subItems.STATEMENT].push({
              PARTITION_ID: subItems.PARTITION_ID,
              STATEMENT: subItems.STATEMENT,
              combinedWeight: combinedWeight,
              z: VoltDbUI.totalProcessingTime[subItems.STATEMENT],
            });
          }
          statement = subItems.STATEMENT;
        });
        z++;
      }

      if (statementList.length > 0) {
        for (var u = 0; u < statementList.length; u++) {
          orderedDetails[statementList[u]].sort(function (a, b) {
            var nameA = a.combinedWeight;
            var nameB = b.combinedWeight;
            if (nameA > nameB) {
              return -1;
            }
            if (nameA < nameB) {
              return 1;
            }
            return 0;
          });
        }
        VoltDbUI.partitionLength = orderedDetails[statementList[0]].length;
        for (var x = 0; x < orderedDetails[statementList[0]].length; x++) {
          var u = 0;
          for (var key in orderedDetails) {
            if (partitionDetails[x] == undefined) {
              partitionDetails.push({ key: "Total Processing Time SP" });
              partitionDetails[x]["values"] = [];
            }
            partitionDetails[x]["values"].push({
              PARTITION_ID: orderedDetails[key][x].PARTITION_ID,
              x: orderedDetails[key][x].STATEMENT,
              y: orderedDetails[key][x].combinedWeight,
              z: orderedDetails[key][x].z,
            });
          }
          u++;
        }
      }

      sortArray(partitionDetails);

      if ($("#hidPartitionType").html() == "Single Partitioned") {
        MonitorGraphUI.RefreshCombinedDetailGraph(partitionDetails);
      } else {
        MonitorGraphUI.RefreshCombinedDetailGraph(finalDetails);
      }
    },
  });

  function sortArray(partitionDetails) {
    for (var key in partitionDetails) {
      var arr = partitionDetails[key].values;
      arr.sort(function (a, b) {
        var nameA = a.z;
        var nameB = b.z;
        if (nameA > nameB) {
          return -1;
        }
        if (nameA < nameB) {
          return 1;
        }
        return 0;
      });
    }
  }

  function getTooltipValues(procedureName) {
    VoltDbUI.executionDetails = {};
    var statement = "";

    VoltDbAnalysis.latencyDetailValue.forEach(function (item) {
      if (item.PROCEDURE == procedureName) {
        if (VoltDbUI.executionDetails[item.STATEMENT] == undefined) {
          VoltDbUI.executionDetails[item.STATEMENT] = {};
        }
        if (statement == item.STATEMENT) {
          var smallest = GetMinimumValue(
            VoltDbAnalysis.latencyDetailValue,
            item.STATEMENT
          );
          var largest = GetMaximumValue(
            VoltDbAnalysis.latencyDetailValue,
            item.STATEMENT
          );

          if (item.type == "Single Partitioned") {
            VoltDbUI.executionDetails[item.STATEMENT]["INVOCATION"] +=
              item.INVOCATION;
          } else {
            VoltDbUI.executionDetails[item.STATEMENT]["INVOCATION"] =
              item.INVOCATION;
          }

          VoltDbUI.executionDetails[item.STATEMENT]["MIN"] = smallest;
          VoltDbUI.executionDetails[item.STATEMENT]["MAX"] = largest;
        } else {
          VoltDbUI.executionDetails[item.STATEMENT]["INVOCATION"] =
            item.INVOCATION;
        }
      }
      statement = item.STATEMENT;
    });
  }

  function GetMinimumValue(arr, statement) {
    arr = arr.filter(function (obj) {
      return obj.STATEMENT == statement;
    });
    return Math.min.apply(
      Math,
      arr.map(function (o) {
        return o.MIN;
      })
    );
  }

  function GetMaximumValue(arr, statement) {
    arr = arr.filter(function (obj) {
      return obj.STATEMENT == statement;
    });
    return Math.max.apply(
      Math,
      arr.map(function (o) {
        return o.MAX;
      })
    );
  }

  function objectLength(obj, statement) {
    var result = 0;
    for (var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        if (obj[prop]["STATEMENT"] == statement) {
          result++;
        }
      }
    }
    return result;
  }

  function calculateStatementCount(arr, statement) {
    var count = 0;
    var j = 0;
    while (j < arr.length) {
      if (arr[j]["STATEMENT"] == statement) count += 1;
      j += 1;
    }
    return count;
  }

  $("#btnThreshold").popup({
    open: function (event, ui, ele) {
      $("#averageExecutionTime").val(
        VoltDbUI.getFromLocalStorage("averageExecutionTime")
      );
      $("#trShowHideSysProcedures").remove();
      $("#tblAnalysisSettings").append(
        '<tr id="trShowHideSysProcedures">' +
        "<td>Show System Procedures</td>" +
        '<td style="text-align:right"><input type="checkbox" value="" id="chkSystemProcedure"></td>' +
        "<td></td>" +
        "</tr>"
      );
      $("#chkSystemProcedure").iCheck({
        checkboxClass: "icheckbox_square-aero customCheckbox",
        increaseArea: "20%",
      });
      $("#chkSystemProcedure").iCheck(
        VoltDbUI.getFromLocalStorage("showHideSysProcedures")
          ? "check"
          : "uncheck"
      );
    },
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnSaveThreshold").unbind("click");
      $("#btnSaveThreshold").on("click", function () {
        saveInLocalStorage(
          "averageExecutionTime",
          $("#averageExecutionTime").val()
        );
        saveInLocalStorage(
          "showHideSysProcedures",
          $("#chkSystemProcedure").is(":checked")
        );
        $("#btnAnalyzeNow").trigger("click");
        //Close the popup
        popup.close();
      });

      $("#btnCancelThreshold").unbind("click");
      $("#btnCancelThreshold").on("click", function () {
        popup.close();
      });
    },
  });

  VoltDbUI.refreshConnectionTime("5000");
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
  $("#showMyPreference").popup({
    open: function (event, ui, ele) {
      userPreference = getUserPreferences();
      $("ul.user-preferences > li Input:checkbox").each(function (value) {
        $(this)[0].checked = userPreference[$(this)[0].id];
      });
    },
    save: function () {
      $("ul.user-preferences > li Input:checkbox").each(function (value) {
        userPreference[$(this)[0].id] = $(this)[0].checked;
      });
      saveUserPreferences(userPreference);

      MonitorGraphUI.UpdateCharts();
    },
  });
};

/*******************************************************************************************
//common methods
/*******************************************************************************************/

var isNodeButtonRegistered = function (elementName) {
  var isRegistered = false;
  var elementCount = 0;
  if (
    VoltDbAdminConfig.registeredElements != undefined &&
    VoltDbAdminConfig.registeredElements.length > 0
  ) {
    $.each(VoltDbAdminConfig.registeredElements, function (key, value) {
      if (value == elementName) {
        isRegistered = true;
        return false;
      } else if (
        elementCount ==
        VoltDbAdminConfig.registeredElements.length - 1
      ) {
        isRegistered = false;
      }
      elementCount++;
    });
  } else isRegistered = false;

  return isRegistered;
};

//Dummy wrapper for console.log for IE9
if (!(window.console && console.log)) {
  console = {
    log: function () { },
    debug: function () { },
    info: function () { },
    warn: function () { },
    error: function () { },
  };
}

var saveCookie = function (name, value) {
  $.cookie(name + "_" + VoltDBConfig.GetPortId(), value, { expires: 365 });
};

var saveSessionCookie = function (name, value) {
  $.cookie(name + "_" + VoltDBConfig.GetPortId(), value);
};

var saveInLocalStorage = function (name, value) {
  data = {};
  data.value = value;
  data.time = new Date().getTime();
  localStorage.setItem(
    name + "_" + VoltDBConfig.GetPortId(),
    JSON.stringify(data)
  );
};

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
  DR: 5,
  Importer: 6,
  Exporter: 7,
  Analysis: 8,
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
  } else if (activeLinkId == "navDR") {
    $(".nvtooltip").show();
    return NavigationTabs.DR;
  } else if (activeLinkId == "navImporter") {
    $(".nvtooltip").show();
    return NavigationTabs.Importer;
  } else if (activeLinkId == "navExporter") {
    $(".nvtooltip").show();
    return NavigationTabs.Exporter;
  } else if (activeLinkId == "navAnalysis") {
    VoltDbAnalysis.refreshChart();
    $(".nvtooltip").show();
    return NavigationTabs.Analysis;
  }
  $(".nvtooltip").show();
  return NavigationTabs.DBMonitor;
};

var getUserPreferences = function () {
  try {
    voltDbRenderer.userPreferences = JSON.parse(
      VoltDbUI.getFromLocalStorage("user-preferences")
    );
  } catch (e) {
    voltDbRenderer.userPreferences = {};
    var preferencesList = [
      "ServerCPU",
      "ServerRAM",
      "ClusterLatency",
      "ClusterTransactions",
      "StoredProcedures",
      "DatabaseTables",
      "PartitionIdleTime",
      "CommandLogStat",
      "CommandLogTables",
    ];
    for (var i = 0; i < preferencesList.length; i++) {
      if (
        preferencesList[i] == "ServerCPU" ||
        preferencesList[i] == "ServerRAM" ||
        preferencesList[i] == "ClusterLatency" ||
        preferencesList[i] == "ClusterTransactions" ||
        preferencesList[i] == "DatabaseTables"
      ) {
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
  if (userpreferences["ServerCPU"] == false) $("#chartServerCPU").hide();
  else $("#chartServerCPU").show();

  if (userpreferences["ServerRAM"] == false) $("#chartServerRAM").hide();
  else $("#chartServerRAM").show();

  if (userpreferences["ClusterLatency"] == false)
    $("#chartClusterLatency").hide();
  else $("#chartClusterLatency").show();

  if (userpreferences["ClusterTransactions"] == false)
    $("#chartClusterTransactions").hide();
  else $("#chartClusterTransactions").show();

  if (userpreferences["PartitionIdleTime"] == false)
    $("#chartPartitionIdleTime").hide();
  else $("#chartPartitionIdleTime").show();

  if (
    VoltDbUI.drConsumerState.toLowerCase() != "disable" &&
    VoltDbUI.drConsumerState != ""
  ) {
    $("#ChartDrReplicationRate").show();
  } else {
    $("#ChartDrReplicationRate").hide();
  }

  if (userpreferences["StoredProcedures"] == false) {
    $("#tblStoredProcedures").hide();
    $("#divSPSection").hide();
  } else {
    $("#tblStoredProcedures").show();
    $("#divSPSection").show();
  }

  if (userpreferences["DatabaseTables"] == false) {
    $("#tblDataTables").hide();
    $("#tblDT").hide();
  } else {
    $("#tblDataTables").show();
    $("#tblDT").show();
  }

  if (
    userpreferences["DatabaseTables"] == false &&
    userpreferences["StoredProcedures"] == false
  )
    $("#firstpane").hide();
  else $("#firstpane").show();

  if (
    !(
      VoltDbUI.drReplicationRole.toLowerCase() == "none" &&
      !VoltDbUI.drMasterEnabled
    )
  ) {
    if (VoltDbUI.drReplicationRole.toLowerCase() == "replica") {
      if (
        (VoltDbUI.drConsumerState.toLowerCase() != "disable" ||
          VoltDbUI.drMasterState.toUpperCase() == "ACTIVE") &&
        VoltDbUI.drConsumerState.toLowerCase() != "" &&
        VoltDbUI.drMasterState.toUpperCase() != ""
      ) {
        $("#divDrReplication").show();
      } else {
        $("#divDrReplication").hide();
      }
    } else {
      if (
        VoltDbUI.drMasterEnabled ||
        VoltDbUI.drConsumerState.toLowerCase() != "disable"
      ) {
        $("#divDrReplication").show();
      } else {
        $("#divDrReplication").hide();
      }
    }
  } else {
    if (VoltDbUI.drConsumerState.toLowerCase() != "disable") {
      $("#divDrReplication").show();
    } else {
      $("#divDrReplication").hide();
    }
  }

  if (VoltDbUI.isCommandLogEnabled == "true") {
    if (userpreferences["CommandLogStat"] == true) {
      $("#chartCommandLogging").show();
    } else {
      $("#chartCommandLogging").hide();
    }
  } else {
    $("#chartCommandLogging").hide();
  }

  if (VoltDbUI.isCommandLogEnabled == "true") {
    if (userpreferences["CommandLogTables"] == true) {
      $("#divCommandLog").show();
    } else {
      $("#divCommandLog").hide();
    }
  } else {
    $("#divCommandLog").hide();
  }

  if (userpreferences["DatabaseTables"] == true) {
    $("#divDTSection").show();
  } else {
    $("#divDTSection").hide();
  }

  adjustGraphSpacing();
  ChangeGraphLabelColor();
  ChangeTableProcedureLabelColor();
};

function ChangeGraphLabelColor() {
  if (VoltDbUI.getFromLocalStorage("user-preferences") != undefined) {
    var userPreferences = JSON.parse(
      VoltDbUI.getFromLocalStorage("user-preferences")
    );

    if (
      userPreferences["ClusterLatency"] != false ||
      userPreferences["ClusterTransactions"] != false ||
      userPreferences["ServerCPU"] != false ||
      userPreferences["ServerRAM"] != false ||
      userPreferences["PartitionIdleTime"] != false ||
      userPreferences["CommandLogStat"] != false ||
      userPreferences["CommandLogStat"] != false
    ) {
      $("#showHideGraphBlock").css("color", "#000000");
      $("#GraphBlock").removeClass("graphOpacity");
    } else {
      $("#showHideGraphBlock").css("color", "gray");
      $("#GraphBlock").addClass("graphOpacity");
    }
  }
}

function ChangeTableProcedureLabelColor() {
  if (VoltDbUI.getFromLocalStorage("user-preferences") != undefined) {
    var userPreferences = JSON.parse(
      VoltDbUI.getFromLocalStorage("user-preferences")
    );
    if (
      userPreferences["DatabaseTables"] != false ||
      userPreferences["StoredProcedures"] != false
    ) {
      $("#ShowHideBlock").css("color", "#000000");
    } else {
      $("#ShowHideBlock").css("color", "gray");
    }
  }
}

// Graph Spacing adjustment on preference change
var adjustGraphSpacing = function () {
  var graphList = [
    $("#chartServerCPU"),
    $("#chartServerRAM"),
    $("#chartClusterLatency"),
    $("#chartClusterTransactions"),
    $("#chartPartitionIdleTime"),
    $("#chartCommandLogging"),
  ];
  var css = "left";

  for (var i = 0; i < graphList.length; i++) {
    if (graphList[i].is(":visible")) {
      graphList[i].removeClass("left right");
      graphList[i].addClass(css);

      if (css == "left") css = "right";
      else css = "left";
    }
  }
};

//Adjust graph spacing for importer graph
var adjustImporterGraphSpacing = function () {
  var graphList = [
    $("#chartOutTransaction"),
    $("#chartSuccessRate"),
    $("#chartFailureRate"),
  ];
  var css = "left";
  for (var i = 0; i < graphList.length; i++) {
    if (graphList[i].is(":visible")) {
      graphList[i].removeClass("left right");
      graphList[i].addClass(css);
      if (css == "left") css = "right";
      else css = "left";
    }
  }
};

var adjustExporterGraphSpacing = function () {
  var graphList = [$("#chartThroughput"), $("#chartQueued")];
  var css = "left";
  for (var i = 0; i < graphList.length; i++) {
    if (graphList[i].is(":visible")) {
      graphList[i].removeClass("left right");
      graphList[i].addClass(css);
      if (css == "left") css = "right";
      else css = "left";
    }
  }
};

(function (window) {
  var iVoltDbUi = function () {
    this.prevDrRoleDetail = {};
    this.drChartList = [];
    this.isSchemaTabLoading = false;
    this.drMasterEnabled = false;
    this.drMasterState = "";
    this.drConsumerState = "";
    this.drStatus = "";
    this.drReplicationRole = "NONE";
    this.isDRInfoRequired = false;
    this.isCommandLogEnabled = false;
    this.isFirstDRLoad = true;
    this.isFirstImporterLoad = true;
    this.isFirstThroughputLoad = true;
    this.isFirstQueuedLoad = true;
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
      REFRESH_TABLEDATA_NONE: 11,
    };

    this.popups = [];
    this.isPopupRevoked = false;
    this.CurrentTab = NavigationTabs.DBMonitor;
    this.CurrentMemoryProgess =
      this.DASHBOARD_PROGRESS_STATES.REFRESHMEMORY_NONE;
    this.CurrentCpuProgess = this.DASHBOARD_PROGRESS_STATES.REFRESHCPU_NONE;
    this.CurrentLatencyProgess =
      this.DASHBOARD_PROGRESS_STATES.REFRESHLATENCY_NONE;
    this.CurrentTransactionProgess =
      this.DASHBOARD_PROGRESS_STATES.REFRESHTRANSACTION_NONE;
    this.CurrentTableDataProgress =
      this.DASHBOARD_PROGRESS_STATES.REFRESH_TABLEDATA_NONE;
    this.CurrentProcedureDataProgress =
      this.DASHBOARD_PROGRESS_STATES.REFRESH_PROCEDUREDATA_NONE;

    this.isConnectionChecked = false;
    this.connectionTimeInterval = null;
    this.partitionGraphInterval = null;
    this.gutterInstanceHor = null;
    this.gutterInstanceVer = null;
    this.vars = {};
    this.executionDetails = {};
    this.totalProcessingTime = {};
    //load schema tab and table and views tabs inside sql query
    this.refreshSqlAndSchemaTab = function () {
      this.loadSchemaTab();
      SQLQueryRender.populateTablesAndViews();
    };
    this.hasPermissionToView = true;
    this.isLatency = false;
    this.isTotalProcessing = false;
    this.isFrequency = false;
    this.isData = false;
    this.partitionLength = 0;
    this.MIN = 0;
    (this.getCookie = function (name) {
      return $.cookie(name + "_" + VoltDBConfig.GetPortId());
    }),
      (this.getFromLocalStorage = function (name) {
        var value = undefined;
        var data = localStorage.getItem(name + "_" + VoltDBConfig.GetPortId());
        if (data != undefined) value = JSON.parse(data);
        return value == undefined ? value : value["value"];
      }),
      (this.refreshConnectionTime = function (seconds) {
        if (VoltDbUI.connectionTimeInterval != null)
          window.clearInterval(VoltDbUI.connectionTimeInterval);

        VoltDbUI.connectionTimeInterval = window.setInterval(
          checkServerConnection,
          seconds
        );
      });

    setInterval(() => {
      if (VoltDbAdminConfig.isSecurityEnabled) {
        voltDbRenderer.checkRolesUpdate();
      }
    }, 2000)

    var checkServerConnection = function () {
      if (!VoltDbUI.isConnectionChecked) {
        VoltDbUI.isConnectionChecked = true;
        voltDbRenderer.CheckServerConnection(function (result) {
          if (result == false) {
            VoltDBCore.isServerConnected = false;
            if (
              !$("#conpop").is(":visible") &&
              !$("#shutdownPop").is(":visible")
            ) {
              window.clearInterval(VoltDbUI.connectionTimeInterval);
              $("#conPopup").trigger("click");
              // window.location.reload();
            }
          } else {
            VoltDbUI.isConnectionChecked = false;
          }
        });
      }
    };

    this.loadSchemaTab = function () {
      this.isSchemaTabLoading = true;
      var schemaHtml =
        '<div id="schemaOverlay" style="display: block;"><div class="loading"></div></div>';
      schemaHtml = schemaHtml + $("#schema").html();
      $("#schema").html(schemaHtml);

      var templateUrl =
        window.location.protocol +
        "//" +
        window.location.host +
        "/catalog/?" +
        VoltDBCore.shortApiCredentials;
      var templateJavascript = "js/template.js";

      $.post(templateUrl, function (result) {
        result = result.replace("<!--##SIZES##>", "");
        var body = $(result).filter("#wrapper").html();
        $("#schema").html(body);
        $("#overlay").hide();

        $("#schemaLinkSqlQuery").on("click", function (e) {
          $("#navSqlQuery").trigger("click");
          e.preventDefault();
        });

        $.getScript(templateJavascript, function () {
          $(".schm").css("display", "block");
          $(".refreshBtn").unbind("click");
          $(".refreshBtn.schm").unbind("click");
          $(".refreshBtn.schm,.refreshBtn").on("click", function () {
            VoltDbUI.refreshSqlAndSchemaTab();
          });
          VoltDbUI.isSchemaTabLoading = false;
        });
      });
    };
  };
  window.VoltDbUI = VoltDbUi = new iVoltDbUi();
})(window);

function RefreshServerUI() {
  var clickedServer = window.location.hostname;
  if (clickedServer != "") {
    $(".activeServerName").html(clickedServer).attr("title", clickedServer);
    saveCurrentServer(clickedServer);

    // $('#popServerList').toggle('slide', '', 1500);
    $(this).parent().prevAll().removeClass("monitoring");
    $(this).parent().nextAll().removeClass("monitoring");
    $(this).parent().addClass("monitoring");
    $("#btnPopServerList").removeClass("hideServers");
    $("#btnPopServerList").addClass("showServers");
    $("#popServerList").hide();
  }
}

function getParameterByName(name) {
  name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
  var regexS = "[\\?&]" + name + "=([^&#]*)";
  var regex = new RegExp(regexS);
  var results = regex.exec(window.location.href);
  if (results == null) return "";
  else return decodeURIComponent(results[1].replace(/\+/g, " "));
}

/*
 * Use a standard function for hiding all but the currently selected help topic
 */
function showHelpTopic(currTopic, currTitle) {
  var topics = [
    "#VDBMonHelp",
    "#VDBSchHelp",
    "#VDBQHelp",
    "#VDBAdmHelp",
    "#VDBDRHelp",
    "#VDBAnalysisHelp",
    "#VDBImportHelp",
    "#VDBExportHelp",
  ];

  for (var i = 0; i < topics.length; i++) {
    $(topics[i]).hide();
  }
  $(currTopic).show();
  $("#showMyHelp").html(currTitle);
}
