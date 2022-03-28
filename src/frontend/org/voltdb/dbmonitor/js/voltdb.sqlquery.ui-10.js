var sqlPortForPausedDB = {
  UseAdminPort: "UseAdminPort",
  UseNormalPort: "UseNormalPort",
};
var $tabs = null;
var tab_counter = 1;
var INT_MAX_VALUE = 2147483647;

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
  function CheckBrowser() {
    if ("localStorage" in window && window["localStorage"] !== null) {
      return true;
    } else {
      return false;
    }
  }
  CheckBrowser();

  var sqlValidationRule = {
    numericRules: {
      min: 1,
      max: INT_MAX_VALUE,
      digits: true,
    },
    numericMessages: {
      min: "Please enter a positive number. Its minimum value should be 1.",
      max:
        "Please enter a positive number between 1 and " + INT_MAX_VALUE + ".",
      digits: "Please enter a positive number without any decimal.",
    },
  };

  var fixWidth = function () {
    var totalWidth = $(window).width();
    if (totalWidth > 981) {
      var queryWidth = totalWidth - 290;
      var percentWidth =
        (parseInt(queryWidth) / parseInt(totalWidth)) * 100 - 4;
      $("#inputQuery").width(percentWidth + "%");
    } else {
      $("#inputQuery").width("100%");
    }
  };

  var addEvent = function (object, type, callback) {
    if (object == null || typeof object == "undefined") return;
    if (object.addEventListener) {
      object.addEventListener(type, callback, false);
    } else if (object.attachEvent) {
      object.attachEvent("on" + type, callback);
    } else {
      object["on" + type] = callback;
    }
  };

  var lock;
  addEvent(window, "resize", function () {
    fixWidth();
    clearTimeout(lock);
    lock = setTimeout(refreshSplit, 100);
  });

  fixWidth();

  var refreshSplit = function () {
    var mousedown = new CustomEvent("mousedown");
    var mousemove = new CustomEvent("mousemove", { bubbles: true });
    var mouseup = new CustomEvent("mouseup", { bubbles: true });
    var gutter = document.querySelector(".gutter-horizontal");

    mousemove.clientX = gutter === null ? '' : gutter.getBoundingClientRect().left;

    gutter === null ? '' : gutter.dispatchEvent(mousedown);
    gutter === null ? '' : gutter.dispatchEvent(mousemove);
    gutter === null ? '' : gutter.dispatchEvent(mouseup);
  };

  $("#bntTimeoutSetting").popup({
    open: function (event, ui, ele) {
      $("#errorQueryTimeoutConfig").hide();
      $("#txtQueryTimeoutConfig").val(
        SQLQueryRender.getCookie("timeoutTime") == undefined
          ? ""
          : SQLQueryRender.getCookie("timeoutTime")
      );
    },
    afterOpen: function () {
      $("#formQueryTimeoutConfiguration").validate({
        rules: {
          txtQueryTimeoutConfig: sqlValidationRule.numericRules,
        },
        messages: {
          txtQueryTimeoutConfig: sqlValidationRule.numericMessages,
        },
      });
      var popup = $(this)[0];
      $("#btnQueryTimeoutConfigSave").off("click");
      $("#btnQueryTimeoutConfigSave").on("click", function (e) {
        if (!$("#formQueryTimeoutConfiguration").valid()) {
          e.preventDefault();
          e.stopPropagation();
          return;
        }
        var timeoutTime = $("#txtQueryTimeoutConfig").val();
        if (timeoutTime == "") {
          SQLQueryRender.removeCookie("timeoutTime");
        } else {
          SQLQueryRender.SaveQueryTimeout(timeoutTime);
        }
        displayQueryTimeout();
        popup.close();
      });
      $("#btnQueryTimeoutConfigCancel").on("click", function () {
        popup.close();
      });
      $("#btnQueryTimeoutConfigClear").on("click", function () {
        $("#txtQueryTimeoutConfig").val("");
        $("#errorQueryTimeoutConfig").hide();
      });
    },
  });

  $("#btnQuerySaveConfirmation").popup({
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnQuerySaveOk").off("click");
      $("#btnQuerySaveOk").on("click", function () {
        popup.close();
      });

      $("#btnQuerySaveCancel").off("click");
      $("#btnQuerySaveCancel").on("click", function () {
        popup.close();
      });
    },
  });

  $("#btnSaveQueryConfirmation").on("click", function (e) {
    var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
    var element_id = element.split("-")[1];
    if ($("#qTab-" + element_id).data("isold")) {
      saveQueryTab(false);
      e.preventDefault();
      e.stopPropagation();
      return;
    }
  });

  $("#btnSaveQueryConfirmation").popup({
    open: function (event, ui, ele) {
      var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
      var element_id = element.split("-")[1];

      $("#txtQueryName").val(SQLQueryRender.orgQueryName);
      $("#errorQueryName").hide();
      var item = document.getElementById("querybox-" + element_id);
      var queryText = item.innerText || item.textContent;
      if (queryText == "") {
        $("#btnSaveQueryOk").hide();
        $("#queryError").show();
        $("#Tr1").hide();
        $("#saveQueryHeader").html("Error");
        $("#divSaveQueryConfirmation").addClass("errorIcon");
      } else {
        $("#btnSaveQueryOk").show();
        $("#queryError").hide();
        $("#Tr1").show();
        $("#saveQueryHeader").html("Save Query");
        $("#divSaveQueryConfirmation").removeClass("errorIcon");
      }

      $.validator.addMethod(
        "checkDuplicate",
        function (value) {
          var arr = [];
          if (localStorage.queries != undefined) {
            queries = JSON.parse(localStorage.queries);
            $.each(queries, function (key) {
              var keySplit = key.split("_");
              keySplit.shift();
              arr.push(keySplit.join("_"));
            });
          }
          if (
            $.inArray(value, SQLQueryRender.queryNameList) != -1 ||
            $.inArray(value, arr) != -1
          ) {
            if (value == SQLQueryRender.orgQueryName) return true;
            else return false;
          } else {
            return true;
          }
        },
        "Query name already exists."
      );

      $("#formSaveQuery").validate({
        rules: {
          txtQueryName: {
            required: true,
            regex: /^[a-zA-Z0-9_.]+$/,
            checkDuplicate: [],
          },
        },
        messages: {
          txtQueryName: {
            required: "This field is required.",
            regex: "Only alphabets, numbers, _ and . are allowed.",
            checkDuplicate: "Query name already exist.",
          },
        },
      });
    },
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnSaveQueryOk").off("click");
      $("#btnSaveQueryOk").on("click", function (e) {
        if (typeof Storage !== "undefined") {
          if (!$("#formSaveQuery").valid()) {
            e.preventDefault();
            e.stopPropagation();
            return;
          }
          saveQueryTab(true);
        } else {
          $("#btnHtmlSupport").trigger("click");
        }
        popup.close();
      });

      $("#btnSaveQueryCancel").off("click");
      $("#btnSaveQueryCancel").on("click", function () {
        popup.close();
      });
    },
  });

  var saveQueryTab = function (isNewTab) {
    var newTabName = $("#txtQueryName").val();
    var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
    var element_id = element.split("-")[1];
    var oldTabName = $("#qTab-" + element_id)
      .find("a")
      .text();
    queryData = {};
    orgQueryData = "{}";
    query_localStorage = localStorage.queries;
    if (query_localStorage != undefined) {
      queryData = JSON.parse(query_localStorage);
      orgQueryData = query_localStorage;
    }

    if (isNewTab) {
      var key_name = generateKeyIndex(queryData) + "_" + newTabName;
      var item = document.getElementById("querybox-" + element_id);
      var queryText = item.innerText || item.textContent;
      queryData[key_name] = queryText;
    } else {
      var key_index = getKeyIndex(queryData, oldTabName);
      var item = document.getElementById("querybox-" + element_id);
      var queryText = item.innerText || item.textContent;
      queryData[key_index + "_" + oldTabName] = queryText;
      newTabName = oldTabName;
    }

    var orderedData = SQLQueryRender.getOrderedData(queryData);
    saveQueryTabToLocalStorage(
      orderedData,
      orgQueryData,
      newTabName,
      oldTabName,
      element_id
    );
  };

  var saveQueryTabToLocalStorage = function (
    orderedData,
    orgQueryData,
    newTabName,
    oldTabName,
    element_id
  ) {
    try {
      localStorage.queries = JSON.stringify(orderedData);
      $("#qTab-" + element_id)
        .find("a")
        .html(newTabName);
      $("#qTab-" + element_id).data("isold", true);
      deleteQueryFromGlobal(oldTabName);
    } catch (e) {
      errorMsg =
        "Cannot save the current tab." +
        (e.message != undefined ? ' \nError: "' + e.message + '".' : "");
      if (
        navigator.userAgent.indexOf("Mozilla") != -1 &&
        navigator.userAgent.indexOf("Firefox") != -1
      ) {
        if (e.code != undefined && e.code == "1014") {
          errorMsg =
            "Maximum storage limit reached. Cannot save the current tab. " +
            "<br/>Please remove the existing tabs or decrease the length of query.";
        }
      } else {
        if (e.code != undefined && e.code == "22") {
          errorMsg =
            "Maximum storage limit reached. Cannot save the current tab. " +
            "<br/>Please remove the existing tabs or decrease the length of query.";
        }
      }
      $("#memErrorMsg").html(errorMsg);
      if (orgQueryData != "" && orgQueryData != "{}")
        localStorage.queries = orgQueryData;
      $("#btnMemoryError").trigger("click");
    }
  };

  var generateKeyIndex = function (queryData) {
    var key_index = 1;
    if (!$.isEmptyObject(queryData)) {
      $.each(queryData, function (key) {
        if (parseInt(key.split("_")[0]) > key_index)
          key_index = parseInt(key.split("_")[0]);
        key_index++;
      });
    }
    return key_index;
  };

  var getKeyIndex = function (queryData, oldName) {
    key_index = 0;
    if (!$.isEmptyObject(queryData)) {
      $.each(queryData, function (key) {
        keySplit = key.split("_");
        keySplit.shift();
        if (keySplit.join("_") == oldName)
          key_index = parseInt(key.split("_")[0]);
      });
    }
    return key_index;
  };

  var saveQueryNameListToStorage = function (queryName) {
    var queryList = localStorage.queryNameList;
    var queryNameList = [];
    if (queryList != undefined) {
      queryNameList = queryList.split(",");
    }
    queryNameList.push(queryName);
    localStorage.queryNameList = queryNameList.join();
  };

  var deleteQueryFromGlobal = function (queryName) {
    if ($.inArray(queryName, SQLQueryRender.queryNameList) != -1)
      SQLQueryRender.queryNameList.splice(
        $.inArray(queryName, SQLQueryRender.queryNameList),
        1
      );
  };

  $("#btnMemoryError").popup({
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnMemoryErrorOk").off("click");
      $("#btnMemoryErrorOk").on("click", function () {
        popup.close();
      });
    },
  });

  $("#btnHtmlSupport").popup({
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnIsSupportedOk").off("click");
      $("#btnIsSupportedOk").on("click", function () {
        popup.close();
      });
    },
  });

  $("#btnCloseTabConfirmation").on("click", function (e) {
    var tablist = [];
    $("#worktabs ul li")
      .not($("#liNewQuery"))
      .each(function () {
        tablist.push($(this).attr("id").split("-")[1]);
      });
    if (tablist.length <= 1) {
      e.stopPropagation();
      e.preventDefault();
      return;
    }
  });

  $("#btnCloseTabConfirmation").popup({
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnCloseTabOk").off("click");
      $("#btnCloseTabOk").on("click", function (e) {
        var element_id = $("#closeTabConfirmation").data("id");
        var id = element_id.split("-")[1];
        tablist = [];
        $("#worktabs ul li").each(function () {
          tablist.push($(this).attr("id").split("-")[1]);
        });

        $tabs.tabs("refresh");
        var active_id = $tabs.tabs("option", "active");
        var current_position = $.inArray(id, tablist);
        if (current_position == active_id) {
          if (current_position > 0)
            $tabs.tabs("option", "active", active_id - 1);
          else if (current_position == 0 && tablist.length > 0)
            $tabs.tabs("option", "active", active_id + 1);
        } else if (active_id >= tablist.length) {
          //$tabs.tabs( "option", "active", tablist.length - 3)
        }
        removeTabData(element_id);
        removeTabNameFromStorageAndGlobal(element_id);
        $("#" + element_id).remove();
        $("#q-" + id).remove();
        $("#queryBtnList-" + id).remove();
        SQLQueryRender.enableDisableCrossTab();
        SQLQueryRender.showHideNewTab();
        popup.close();
      });

      $("#btnCloseTabCancel").off("click");
      $("#btnCloseTabCancel").on("click", function (e) {
        popup.close();
      });
    },
  });

  var removeTabData = function (elementId) {
    var tabName = $("#" + elementId)
      .find("a")
      .text();
    var keyName = getKeyIndex() + "_" + tabName;
    var queryData = {};
    var sql_localStorage = localStorage.queries;
    if (sql_localStorage != undefined) {
      queryData = JSON.parse(sql_localStorage);
    }
    var keyName = getKeyIndex(queryData, tabName) + "_" + tabName;
    delete queryData[keyName];
    if (typeof Storage !== "undefined")
      localStorage.queries = JSON.stringify(queryData);
  };

  var removeTabNameFromStorageAndGlobal = function (elementId) {
    var tabName = $("#" + elementId)
      .find("a")
      .text();
    globalList = SQLQueryRender.queryNameList;
    if ($.inArray(tabName, globalList) != -1)
      globalList.splice($.inArray(tabName, globalList), 1);
  };

  $("#timeoutCross").on("click", function () {
    SQLQueryRender.removeCookie("timeoutTime");
    displayQueryTimeout();
  });

  var displayQueryTimeout = function () {
    try {
      var timeoutTime = SQLQueryRender.getCookie("timeoutTime");
      if (timeoutTime != undefined) {
        $("#queryTimeoutTxt").html("Query Timeout: " + timeoutTime);
        $("#divQueryTimeOut").show();
      } else {
        $("#divQueryTimeOut").hide();
      }
    } catch (e) {
      $("#divQueryTimeOut").hide();
    }
  };

  displayQueryTimeout();
  //Default Action
  $(".tab_contentSQL").hide(); //Hide all content
  $("ul#tabSQL li:first").addClass("active").show(); //Activate first tab
  $(".tab_contentSQL:first").show(); //Show first tab content

  //On Click Event
  $("ul#tabSQL li").on("click", function () {
    $("ul#tabSQL li").removeClass("active"); //Remove any "active" class
    $(this).addClass("active"); //Add "active" class to selected tab
    $(".tab_contentSQL").hide(); //Hide all tab content
    var activeTab = $(this).find("a").attr("href"); //Find the rel attribute value to identify the active tab + content
    $(activeTab).fadeIn(); //Fade in the active content
    return false;
  });

  $(".tab_content_data").hide(); //Hide all content
  $("ul#tabAnalysis li:first").addClass("active").show(); //Activate first tab
  $(".tab_content_data:first").show(); //Show first tab content

  //On Click Event
  $("ul#tabAnalysis li").on("click", function () {
    $("ul#tabAnalysis li").removeClass("active"); //Remove any "active" class
    $(this).addClass("active"); //Add "active" class to selected tab
    $(".tab_content_data").hide(); //Hide all tab content
    var activeTab = $(this).find("a").attr("href"); //Find the rel attribute value to identify the active tab + content
    $(activeTab).fadeIn(); //Fade in the active content
    return false;
  });

  $(".tab_content_procedure").hide(); //Hide all content
  $("ul#ulProcedure li:first").addClass("active").show(); //Activate first tab
  $(".tab_content_procedure:first").show(); //Show first tab content

  //On Click Event
  $("ul#ulProcedure li").on("click", function () {
    $("ul#ulProcedure li").removeClass("active"); //Remove any "active" class
    $(this).addClass("active"); //Add "active" class to selected tab
    $(".tab_content_procedure").hide(); //Hide all tab content
    var activeTab = $(this).find("a").attr("href"); //Find the rel attribute value to identify the active tab + content
    $(activeTab).fadeIn(); //Fade in the active content
    return false;
  });
  // Table Accordion
  $("#accordionTable").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });

  // Views Accordion
  $("#accordionViews").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });

  // Procedures Accordion
  $("#accordionProcedures").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });

  // Streamed Table Accordion
  $("#accordionStreamedTable").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });

  $tabs = $("#worktabs").tabs();

  if (localStorage.queries == undefined || localStorage.queries == "{}")
    SQLQueryRender.createQueryTab();
  else SQLQueryRender.loadSavedQueries();

  $("#worktabs").tabs({
    activate: function (event, ui) {
      var counter = ui.newTab.attr("id").split("-")[1];

      if (
        $("#querybox-" + counter)
          .parent()
          .find(".gutter").length == 0
      ) {
        if (isMobile == false) {
          Split(["#querybox-" + counter, "#blockContainer" + counter], {
            direction: "vertical",
            sizes: [30, 70],
            gutterSize: 15,
            minSize: [120, 150],
          });
        }
      }
    },
  });
});

(function (window) {
  var counter = 0;
  var tablesArray = [];
  var iSqlQueryRender = function () {
    this.server = null;
    this.userName = null;
    this.useAdminPortCancelled = false;
    this.sqlValidationRule = {
      numericRules: {
        min: 1,
        max: INT_MAX_VALUE,
        digits: true,
      },
      numericMessages: {
        min: "Please enter a positive number. Its minimum value should be 1.",
        max:
          "Please enter a positive number between 1 and " + INT_MAX_VALUE + ".",
        digits: "Please enter a positive number without any decimal.",
      },
    };
    this.queryNameList = [];
    this.orgQueryName = "";

    this.getOrderedData = function (unOrdered) {
      var orderData = {};
      Object.keys(unOrdered)
        .sort()
        .forEach(function (key) {
          orderData[key] = unOrdered[key];
        });
      return orderData;
    };

    this.enableDisableCrossTab = function () {
      var tablist = [];
      $("#worktabs ul li")
        .not($("#liNewQuery"))
        .each(function () {
          var id = $(this).attr("id").split("-")[1];
          tablist.push(id);
          $("#close-tab-" + id).removeClass("disableCloserTab");
        });

      if (tablist.length == 1) {
        $("#close-tab-" + tablist[0]).addClass("disableCloserTab");
      }
    };

    this.loadSavedQueries = function () {
      var sql_localStorage = localStorage.queries;
      var queryData = {};

      if (sql_localStorage != undefined) {
        unOrderedData = JSON.parse(sql_localStorage);
        queryData = SQLQueryRender.getOrderedData(unOrderedData);
      }

      $.each(queryData, function (key, value) {
        var keySplit = key.split("_");
        keySplit.shift();
        SQLQueryRender.createQueryTab(keySplit.join("_"), value);
      });

      if ($.isEmptyObject(queryData)) {
        var ul = $tabs.find("ul");
        html =
          '<li id="liNewQuery" title="New Query Tab"><a class="btnStudio plusBtn" id="new-query"><span>+</span></a></li>';
        $(html).appendTo(ul);
        $("#new-query").off("click");
        $("#new-query").on("click", function () {
          SQLQueryRender.createQueryTab();
        });
      }
    };

    this.getTabCounter = function () {
      localArray = [];
      if (localStorage.queries != undefined) {
        $.each(JSON.parse(localStorage.queries), function (key) {
          localArray.push(key.split("_").splice(1).join());
        });
      }
      if (
        $.inArray("Query" + tab_counter, localArray) != -1 ||
        $.inArray("Query" + tab_counter, SQLQueryRender.queryNameList) != -1
      ) {
        tab_counter++;
        SQLQueryRender.getTabCounter();
      }
    };

    this.createQueryTab = function (tabName, tabQuery) {
      if ($("#worktabs ul li").length == 0 || $("#worktabs ul li").length == 1)
        tab_counter = 1;
      else {
        var last_tab_txt = $(
          $("#worktabs ul li")[$("#worktabs ul li").length - 2]
        ).attr("id");
        tab_counter = parseInt(last_tab_txt.replace(/[^0-9]/gi, "")) + 1;
      }
      if (tabName == undefined) SQLQueryRender.getTabCounter();
      var ul = $tabs.find("ul");
      var html = "";
      if ($("#new-query").length == 0) {
        html =
          '<li data-isold="' +
          (tabName == undefined ? false : true) +
          '" id="qTab-' +
          tab_counter +
          '"><a href="#q-' +
          tab_counter +
          '">' +
          (tabName == undefined ? "Query" + tab_counter : tabName) +
          '</a> <div class="ui-icon ui-icon-close close-tab" id="close-tab-' +
          tab_counter +
          '" href="#closeTabConfirmation" title="Close Tab">Close</div></li><li id="liNewQuery" title="New Query Tab"><a class="btnStudio plusBtn" id="new-query"><span>+</span></a></li>';
      } else {
        html =
          '<li data-isold="' +
          (tabName == undefined ? false : true) +
          '" id="qTab-' +
          tab_counter +
          '"><a href="#q-' +
          tab_counter +
          '">' +
          (tabName == undefined ? "Query" + tab_counter : tabName) +
          '</a> <div class="ui-icon ui-icon-close close-tab" id="close-tab-' +
          tab_counter +
          '" href="#closeTabConfirmation" title="Close Tab">Close</div></li>';
      }

      var html_body =
        '<div class="verticalWrapper"><div id="querybox-' +
        tab_counter +
        '" class="querybox-' +
        tab_counter +
        ' querybox split split-vertical" contenteditable></div>';
      var html_query =
        '<div class="blockWrapper split split-vertical" id="blockContainer' +
        tab_counter +
        '">' +
        '   <div class="exportType">' +
        '<form name="" id="queryResult-' +
        tab_counter +
        '">' +
        '<select id="exportType-' +
        tab_counter +
        '">' +
        '   <option selected="selected">HTML</option>' +
        "   <option>CSV</option>" +
        "   <option>Monospace</option>" +
        "</select>" +
        "</form>" +
        "</div>";

      if (!isMobile) {
        html_query =
          html_query +
          '<h1 class="theHeading"><span class="icon-queryResult"></span><span class="queryResultStyle">Query Result</span><div id="queryResults-' +
          tab_counter +
          '" class="queryStatus"></div><div class="clear"></div></h1>';
      } else {
        html_query =
          html_query +
          '<h1 class="theHeading"><span class="icon-queryResult"></span><span class="queryResultStyle">Query Result</span></h1>';
      }

      html_query =
        html_query +
        '<div id="queryWrapper-' +
        tab_counter +
        '" class="queryWrapper">' +
        '<div class="queryResult-' +
        tab_counter +
        '">' +
        '<div id="resultHtml-' +
        tab_counter +
        '" style="display: none;" class="resultHtml"></div>' +
        '<div id="resultCsv-' +
        tab_counter +
        '" style="display: none;" class="resultCsv"></div>' +
        '<div id="resultMonospace-' +
        tab_counter +
        '" style="display: block;" class="resultMonospace">' +
        "<pre>                    </pre>" +
        "</div>" +
        "</div>";

      if (isMobile) {
        html_query =
          html_query +
          '<div id="queryResults-' +
          tab_counter +
          '" class="queryStatus"></div><div class="clear"></div>';
      }

      html_query = html_query + "</div></div>";

      $(html).appendTo(ul);
      $("#ulTabList").append($("#liNewQuery"));
      $("#worktabs").append(
        '<div id="q-' + tab_counter + '" >' + html_body + html_query + "</div>"
      );
      $("#querybox-" + tab_counter).text(tabQuery == undefined ? "" : tabQuery);
      SQLQueryRender.addQueryBtn(tab_counter);

      $("#exportType-" + tab_counter).on("change", function () {
        var tab_id = $(this).attr("id").split("-")[1];
        if ($("#exportType-" + tab_id).val() == "HTML") {
          $("#resultHtml-" + tab_id).css("display", "block");
          $("#resultCsv-" + tab_id).css("display", "none");
          $("#resultMonospace-" + tab_id).css("display", "none");
        } else if ($("#exportType-" + tab_id).val() == "CSV") {
          $("#resultCsv-" + tab_id).css("display", "block");
          $("#resultHtml-" + tab_id).css("display", "none");
          $("#resultMonospace-" + tab_id).css("display", "none");
        } else if ($("#exportType-" + tab_id).val() == "Monospace") {
          $("#resultMonospace-" + tab_id).css("display", "block");
          $("#resultHtml-" + tab_id).css("display", "none");
          $("#resultCsv-" + tab_id).css("display", "none");
        }
      });

      $("#resultHtml-" + tab_counter).css("display", "block");
      $("#resultCsv-" + tab_counter).css("display", "none");
      $("#resultMonospace-" + tab_counter).css("display", "none");

      $("#new-query").off("click");
      $("#new-query").on("click", function () {
        SQLQueryRender.createQueryTab();
        tab_counter = tab_counter - 1;

        if (
          $("#querybox-" + tab_counter)
            .parent()
            .find(".gutter").length != 0
        ) {
          $("#querybox-" + tab_counter)
            .parent()
            .find(".gutter")
            .remove();
        }

        if (
          $("#querybox-" + tab_counter)
            .parent()
            .find(".gutter").length == 0
        ) {
          if (isMobile == false) {
            VoltDbUI.vars["gutterInstanceVer" + tab_counter] = Split(
              ["#querybox-" + tab_counter, "#blockContainer" + tab_counter],
              {
                direction: "vertical",
                sizes: [30, 70],
                gutterSize: 15,
                minSize: [120, 150],
              }
            );
          }
        }
      });

      $("#close-tab-" + tab_counter).off("click");
      $("#close-tab-" + tab_counter).on("click", function () {
        var element_id = $(this.parentElement).attr("id");
        $("#closeTabConfirmation").data("id", element_id);
        $("#btnCloseTabConfirmation").trigger("click");
      });

      $tabs.tabs("refresh");
      $tabs.tabs("option", "active", $("#worktabs ul li").length - 2);
      $tabs.tabs({
        activate: function (event, ui) {
          tab_id = ui.newTab.attr("id").split("-")[1];
          SQLQueryRender.ShowQueryBtnById(tab_id);
        },
      });

      if (tabName == undefined) {
        SQLQueryRender.queryNameList.push(
          tabName == undefined ? "Query" + tab_counter : tabName
        );
      }

      tab_counter++;
      this.showHideNewTab();
      SQLQueryRender.enableDisableCrossTab();
    };

    this.addQueryBtn = function (tab_id) {
      var htmlBtn =
        '<ul class="btnList clsQueryBtnList" id="queryBtnList-' +
        tab_id +
        '"> ' +
        " <li> " +
        '     <button class="btnStudio" id="runBTn-' +
        tab_id +
        '">Run</button> ' +
        " </li> " +
        " <li> " +
        '     <button class="btnStudio" id="clearQuery-' +
        tab_id +
        '">Clear</button> ' +
        " </li> " +
        " <li> " +
        '     <button class="btnStudio" id="querySaveBtn-' +
        tab_id +
        '">Save</button> ' +
        " </li> " +
        "</ul>";
      $("#divQueryBtns").append(htmlBtn);

      $("#runBTn-" + tab_id).off("click");

      $("#runBTn-" + tab_id).on("click", function () {
        var queryTab = $(
          $("#worktabs div.ui-tabs-panel")[$tabs.tabs("option", "active")]
        );
        var query = queryTab.text().split(' ');
        if (queryTab.length == 1) {
          var queryUI = new QueryUI(
            $($("#worktabs div.ui-tabs-panel")[$tabs.tabs("option", "active")])
          );
          queryUI.execute(query);
          setTimeout(function(){
          }, 5000);
        }
      });

      $("#clearQuery-" + tab_id).off("click");
      $("#clearQuery-" + tab_id).on("click", function () {
        query_id = $(this).attr("id").split("-")[1];
        $("#querybox-" + query_id).text("");
      });

      $("#querySaveBtn-" + tab_id).off("click");
      $("#querySaveBtn-" + tab_id).on("click", function () {
        var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
        var element_id = element.split("-")[1];
        SQLQueryRender.orgQueryName = $("#qTab-" + element_id)
          .find("a")
          .text();
        $("#btnSaveQueryConfirmation").trigger("click");
      });

      SQLQueryRender.ShowQueryBtnById(tab_id);
    };

    this.ShowQueryBtnById = function (tab_id) {
      $("#divQueryBtns ul").each(function () {
        var ul_id = $(this).attr("id");
        if (ul_id == "queryBtnList-" + tab_id) $("#" + ul_id).show();
        else $("#" + ul_id).hide();
      });
    };

    this.showHideNewTab = function () {
      var count = 0;
      $("#worktabs ul li").each(function () {
        count++;
      });
      if (count >= 11) $("#liNewQuery").hide();
      else $("#liNewQuery").show();
    };

    this.saveConnectionKey = function (useAdminPort) {
      var server =
        SQLQueryRender.server == null
          ? VoltDBConfig.GetDefaultServerNameForKey()
          : SQLQueryRender.server.trim();
      var user = SQLQueryRender.userName == "" ? null : SQLQueryRender.userName;
      var processNameSuffix = useAdminPort ? "" : "_CLIENTPORT";
      var processName = "TABLE_INFORMATION" + processNameSuffix;
      var key = (
        server +
        "_" +
        (user == "" ? "" : user) +
        "_" +
        processName
      ).replace(/[^_a-zA-Z0-9]/g, "_");
      saveSessionCookie("connectionkey", key);
    };

    this.populateTablesAndViews = function () {
      toggleSpinner(true);
      voltDbRenderer.GetTableInformation(function (
        tablesData,
        viewsData,
        proceduresData,
        procedureColumnsData,
        sysProcedureData,
        exportTableData
      ) {
        var tables = tablesData["tables"];
        populateTableData(tables);
        var views = viewsData["views"];
        populateViewData(views);
        var streamedTables = exportTableData["exportTables"];
        populateStreamedTablesData(streamedTables);
        var procedures = proceduresData["procedures"];
        var procedureColumns = procedureColumnsData["procedureColumns"];
        var sysProcedure = sysProcedureData["sysProcedures"];
        populateStoredProcedure(procedures, procedureColumns, sysProcedure);
        toggleSpinner(false);
      });
      voltDbRenderer.GetTableInformationClientPort();
    };

    var populateTableData = function (tables) {
      var count = 0;
      var src = "";
      for (var k in tables) {
        tablesArray.push(k);
        src += "<h3>" + k + "</h3>";
        var item = tables[k];
        src += '<div id="column_' + count + '" class="listView">';
        if (item.columns != null) {
          src += "<ul>";
          for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
          src += "</ul>";
        } else src += "<ul><li>Schema could not be read...</li></ul>";
        src += "</div>";
        count++;
      }
      if (src == "") {
        src += "<h3>No tables found.</h3>";
        $("#accordionTable").html(src);
      } else {
        $("#accordionTable").html(src);
        $("#accordionTable").accordion("refresh");
      }
    };

    var populateViewData = function (views) {
      var count = 0;
      var src = "";
      for (var k in views) {
        src += "<h3>" + k + "</h3>";
        var item = views[k];
        src += '<div id="view_' + count + '" class="listView">';
        if (item.columns != null) {
          src += "<ul>";
          for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
          src += "</ul>";
        } else src += "<ul><li>Schema could not be read...</li></ul>";
        src += "</div>";
        count++;
      }
      if (src == "") {
        src += "<h3>No views found.</h3>";
        $("#accordionViews").html(src);
      } else {
        $("#accordionViews").html(src);
        $("#accordionViews").accordion("refresh");
      }
    };

    var populateStoredProcedure = function (
      proceduresData,
      procedureColumnData,
      sysProcedure
    ) {
      global_refresh_sqlquery_proclist(  
        proceduresData,
        procedureColumnData,
        sysProcedure,
        tablesArray
      );
    };

    var populateStreamedTablesData = function (tables) {
      var count = 0;
      var src = "";
      for (var k in tables) {
        tablesArray.push(k);
        src += "<h3>" + k + "</h3>";
        var item = tables[k];
        src += '<div id="column_' + count + '" class="listView">';
        if (item.columns != null) {
          src += "<ul>";
          for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
          src += "</ul>";
        } else src += "<ul><li>Schema could not be read...</li></ul>";
        src += "</div>";
        count++;
      }
      if (src == "") {
        src += "<h3>No stream tables found.</h3>";
        $("#accordionStreamedTable").html(src);
      } else {
        $("#accordionStreamedTable").html(src);
        $("#accordionStreamedTable").accordion("refresh");
      }
    };

    var toggleSpinner = function (show) {
      if (!show) {
        $("#sqlQueryOverlay").hide();
        //                $("#tabScroller").css("height", "calc(100% - 96px)");
        //                $("#tabScroller").css("height", "-moz-calc(100% - 96px)");
        //$("#tabScroller").css("height", "-webkit-calc(100% - 96px)");
        $(".slimScrollBar").css("z-index", "99");
      } else if (show) {
        $("#tabScroller").css("height", "");
        $(".slimScrollBar").css("z-index", "-9999");
        $("#sqlQueryOverlay").show();
      }
    };

    this.SaveQueryTimeout = function (timeoutTime) {
      saveInLocalStorage("timeoutTime", timeoutTime);
    };

    this.getCookie = function (name) {
      var value = undefined;
      var data = localStorage.getItem(name + "_" + VoltDBConfig.GetPortId());
      if (data != undefined) value = JSON.parse(data);
      return value == undefined ? value : value["value"];
    };

    this.removeCookie = function (name) {
      return localStorage.removeItem(name + "_" + VoltDBConfig.GetPortId());
    };
  };
  window.SQLQueryRender = SQLQueryRender = new iSqlQueryRender();
})(window);

function loadSQLQueryPage(serverName, portid, userName) {
  var tablesArray = [];
  SQLQueryRender.server = serverName;
  SQLQueryRender.userName = userName;
  VoltDBService.SetConnectionForSQLExecution(false);
  SQLQueryRender.saveConnectionKey(false);

  var sqlChangePortName = "sql_port_for_paused_db";
  $("#queryDatabasePausedErrorPopupLink").popup({
    open: function (event, ui, ele) {
      SQLQueryRender.useAdminPortCancelled = false;
    },
    afterOpen: function () {
      var popup = $(this)[0];
      $("#btnQueryDatabasePausedErrorOk").off("click");
      $("#btnQueryDatabasePausedErrorOk").on("click", function () {
        saveSessionCookie(sqlChangePortName, sqlPortForPausedDB.UseAdminPort);
        VoltDBService.SetConnectionForSQLExecution(true);
        SQLQueryRender.saveConnectionKey(true);
        popup.close();
        //Rerun the query
        var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
        var element_id = element.split("-")[1];
        var btn_id = "#runBTn-" + element_id;
        $(btn_id).button().trigger("click");
      });

      $("#btnQueryDatabasePausedErrorCancel").off("click");
      $("#btnQueryDatabasePausedErrorCancel").on("click", function () {
        SQLQueryRender.useAdminPortCancelled = true;
        popup.close();
      });
    },
    beforeClose: function () {
      if (
        VoltDbUI.getCookie(sqlChangePortName) != sqlPortForPausedDB.UseAdminPort
      ) {
        saveSessionCookie(sqlChangePortName, sqlPortForPausedDB.UseNormalPort);
        //Rerun the query
        var element = $("#worktabs .ui-tabs-panel:visible").attr("id");
        var element_id = element.split("-")[1];
        var btn_id = "#runBTn-" + element_id;
        $(btn_id).button().trigger("click");
      }
    },
    closeContent: "",
    modal: true,
  });

  // Export Type Change
  $("#exportType").on("change", function () {
    if ($("#exportType").val() == "HTML") {
      $("#resultHtml").css("display", "block");
      $("#resultCsv").css("display", "none");
      $("#resultMonospace").css("display", "none");
    } else if ($("#exportType").val() == "CSV") {
      $("#resultCsv").css("display", "block");
      $("#resultHtml").css("display", "none");
      $("#resultMonospace").css("display", "none");
    } else if ($("#exportType").val() == "Monospace") {
      $("#resultMonospace").css("display", "block");
      $("#resultHtml").css("display", "none");
      $("#resultCsv").css("display", "none");
    }
  });
  // Clears Query
  $("#clearQuery").on("click", function () {
    $("#theQueryText").val("");
  });

  // Tooltip
  $(".tooltip").tooltipster();

  var populateTableData = function (tables) {
    var count = 0;
    var src = "";
    for (var k in tables) {
      tablesArray.push(k);
      src += "<h3>" + k + "</h3>";
      var item = tables[k];
      src += '<div id="column_' + count + '" class="listView">';
      if (item.columns != null) {
        src += "<ul>";
        for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
        src += "</ul>";
      } else src += "<ul><li>Schema could not be read...</li></ul>";
      src += "</div>";
      count++;
    }
    if (src == "") {
      src += "<h3>No tables found.</h3>";
      $("#accordionTable").html(src);
    } else {
      $("#accordionTable").html(src);
      $("#accordionTable").accordion("refresh");
    }
  };

  var populateViewData = function (views) {
    var count = 0;
    var src = "";
    for (var k in views) {
      src += "<h3>" + k + "</h3>";
      var item = views[k];
      src += '<div id="view_' + count + '" class="listView">';
      if (item.columns != null) {
        src += "<ul>";
        for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
        src += "</ul>";
      } else src += "<ul><li>Schema could not be read...</li></ul>";
      src += "</div>";
      count++;
    }
    if (src == "") {
      src += "<h3>No views found.</h3>";
      $("#accordionViews").html(src);
    } else {
      $("#accordionViews").html(src);
      $("#accordionViews").accordion("refresh");
    }
  };

  var populateStreamedTablesData = function (tables) {
    var count = 0;
    var src = "";
    for (var k in tables) {
      tablesArray.push(k);
      src += "<h3>" + k + "</h3>";
      var item = tables[k];
      src += '<div id="column_' + count + '" class="listView">';
      if (item.columns != null) {
        src += "<ul>";
        for (var c in item.columns) src += "<li>" + item.columns[c] + "</li>";
        src += "</ul>";
      } else src += "<ul><li>Schema could not be read...</li></ul>";
      src += "</div>";
      count++;
    }
    if (src == "") {
      src += "<h3>No stream tables found.</h3>";
      $("#accordionStreamedTable").html(src);
    } else {
      $("#accordionStreamedTable").html(src);
      $("#accordionStreamedTable").accordion("refresh");
    }
  };

  var populateStoredProcedure = function (
    proceduresData,
    procedureColumnData,
    sysProcedure
  ) {
    global_refresh_sqlquery_proclist(  
      proceduresData,
      procedureColumnData,
      sysProcedure,
      tablesArray
    );
  };

  var toggleSpinner = function (show) {
    if (!show) {
      $("#sqlQueryOverlay").hide();
      $(".slimScrollBar").css("z-index", "99");
    } else if (show) {
      $("#tabScroller").css("height", "");
      $(".slimScrollBar").css("z-index", "-9999");
      $("#sqlQueryOverlay").show();
    }
  };

  var populateTablesAndViews = function () {
    toggleSpinner(true);
    voltDbRenderer.GetTableInformation(function (
      tablesData,
      viewsData,
      proceduresData,
      procedureColumnsData,
      sysProcedureData,
      exportTableData
    ) {
      var tables = tablesData["tables"];
      populateTableData(tables);
      var views = viewsData["views"];
      populateViewData(views);
      var streamedTables = exportTableData["exportTables"];
      populateStreamedTablesData(streamedTables);
      var procedures = proceduresData["procedures"];
      var procedureColumns = procedureColumnsData["procedureColumns"];
      var sysProcedure = sysProcedureData["sysProcedures"];
      populateStoredProcedure(procedures, procedureColumns, sysProcedure);
      toggleSpinner(false);
    });
    voltDbRenderer.GetTableInformationClientPort();
  };
  populateTablesAndViews();
  $("#overlay").hide();
}
function global_refresh_sqlquery_proclist (
  proceduresData,
  procedureColumnData,
  sysProcedure,
  tablesArray
) {
  // Stored Procedures
  var sysScr = "";
  var src = "";
  var defSrc = "";
  var cmpdSrc = "";

  sysScr += '<h3 class="systemHeader">System Stored Procedures</h3>';
  sysScr += '<div id="systemProcedure" class="listView">';
  for (var k in sysProcedure) {
    for (var paramCount in sysProcedure[k]) {
      sysScr += "<h3>" + k + "</h3>";
      sysScr += '<div class="listView">';
      sysScr += "<ul>";
      for (var i = 0; i < sysProcedure[k][paramCount].length - 1; i++) {
        sysScr +=
          '<li class="parameterValue">' +
          sysProcedure[k][paramCount][i] +
          "</li>";
      }
      sysScr +=
        '<li class="returnValue">' +
        sysProcedure[k][paramCount][i] +
        "</li>";
      sysScr += "</ul>";
      sysScr += "</div>";
    }
  }
  sysScr += "</div>";

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
          if (paramType.toLowerCase() == "tinyint")
            // ENG-2040 and ENG-3101, identify it as an array (byte[])
            paramType = "byte[]";
          else paramType += "_array";
        }
        procParams[paramOrder] = {
          name: paramName,
          type: paramType.toLowerCase(),
        };
      }
    }

    var procArray = procName.split(".");
    if (
      procArray.length > 1 &&
      jQuery.inArray(procArray[0], tablesArray) != -1
    ) {  // CRUD procs
      defSrc += "<h3>" + procName + "</h3>";
      defSrc += '<div class="listView">';
      defSrc += "<ul>";
      for (var p = 0; p < procParams.length; ++p) {
        defSrc +=
          '<li class="parameterValue">Param' +
          p +
          " (" +
          procParams[p].type +
          ")</li>";
      }
      defSrc += '<li class="returnValue">Return Table[]</li>';
      defSrc += "</ul>";
      defSrc += "</div>";
    } else {
      var procRemark = JSON.parse(proceduresData[i][6]);
      if (procRemark["compound"]) {   // compound procedures
        cmpdSrc += "<h3>" + procName + "</h3>";
        cmpdSrc += '<div class="listView">';
        cmpdSrc += "<ul>";
        for (var p = 0; p < procParams.length; ++p) {
          cmpdSrc +=
            '<li class="parameterValue">Param' +
            p +
            " (" +
            procParams[p].type +
            ")</li>";
        }
        cmpdSrc += '<li class="returnValue">Return Table[]</li>';
        cmpdSrc += "</ul>";
        cmpdSrc += "</div>";

      } else {    // user-define procs
        src += "<h3>" + procName + "</h3>";
        src += '<div class="listView">';
        src += "<ul>";
        for (var p = 0; p < procParams.length; ++p) {
          src +=
            '<li class="parameterValue">Param' +
            p +
            " (" +
            procParams[p].type +
            ")</li>";
        }
        src += '<li class="returnValue">Return Table[]</li>';
        src += "</ul>";
        src += "</div>";

      }

    }
  }
  var defSrcHeader = "";
  defSrcHeader += '<h3 class="systemHeader">Default Stored Procedures</h3>';
  defSrcHeader += '<div id="defaultProcedure" class="listView">';
  var defSrcFooter = "</div>";
  defSrc =
    defSrcHeader +
    (defSrc != ""
      ? defSrc
      : '<div style="font-size:12px">No default stored procedures found.</div>') +
    defSrcFooter;

  var userProcHeader = "";
  userProcHeader +=
    '<h3 id="userDefinedStoredProcs" class="systemHeader">User-Defined Stored Procedures</h3>';
  userProcHeader += '<div id="userProcedure" class="listView">';
  var userProcFooter = "</div>";
  var userSrc =
    userProcHeader +
    (src != ""
      ? src
      : '<div style="font-size:12px">No user defined stored procedures found.</div>') +
    userProcFooter;

  var cmpdProcHeader = "";
  if (cmpdSrc != "") {
    cmpdProcHeader +=
      '<h3 id="compoundProcs" class="systemHeader">Compound Procedures</h3>';
    cmpdProcHeader += '<div id="compoundProcedure" class="listView">';
    var cmpdProcFooter = "</div>";
    cmpdSrc =
      cmpdProcHeader +
      (cmpdSrc != ""
        ? cmpdSrc
        : '<div style="font-size:12px">No compound procedures found.</div>') +
        cmpdProcFooter;

  };

  $("#accordionProcedures").html(sysScr + defSrc + userSrc + cmpdSrc);
  $("#accordionProcedures").accordion("refresh");
  $("#systemProcedure").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });
  $("#defaultProcedure").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });
  $("#userProcedure").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });
  $("#cmpdProcedure").accordion({
    collapsible: true,
    active: false,
    beforeActivate: function (event, ui) {
      // The accordion believes a panel is being opened
      if (ui.newHeader[0]) {
        var currHeader = ui.newHeader;
        var currContent = currHeader.next(".ui-accordion-content");
        // The accordion believes a panel is being closed
      } else {
        var currHeader = ui.oldHeader;
        var currContent = currHeader.next(".ui-accordion-content");
      }
      // Since we've changed the default behavior, this detects the actual status
      var isPanelSelected = currHeader.attr("aria-selected") == "true";

      // Toggle the panel's header
      currHeader
        .toggleClass("ui-corner-all", isPanelSelected)
        .toggleClass(
          "accordion-header-active ui-state-active ui-corner-top",
          !isPanelSelected
        )
        .attr("aria-selected", (!isPanelSelected).toString());

      // Toggle the panel's icon
      currHeader
        .children(".ui-icon")
        .toggleClass("ui-icon-triangle-1-e", isPanelSelected)
        .toggleClass("ui-icon-triangle-1-s", !isPanelSelected);

      // Toggle the panel's content
      currContent.toggleClass("accordion-content-active", !isPanelSelected);
      if (isPanelSelected) {
        currContent.slideUp(50);
      } else {
        currContent.slideDown(50);
      }

      return false; // Cancels the default action
    },
  });
}