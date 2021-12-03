(function (window) {
  var IMonitorGraphUI = function () {
    var RETAINED_TIME_INTERVAL = 60; //60 means graph data within 60 minutes time interval will be stored in local storage.
    var currentView = "Seconds";
    var currentViewDr = "Seconds";
    var currentViewImporter = "Seconds";
    var currentViewExporter = "Seconds";
    var cpuSecCount = 0;
    var cpuMinCount = 0;
    var cmdLogSecCount = 0;
    var cmdLogMinCount = 0;
    var tpsSecCount = 0;
    var tpsMinCount = 0;
    var memSecCount = 0;
    var memMinCount = 0;
    var latSecCount = 0;
    var latMinCount = 0;
    var partitionSecCount = 0;
    var partitionMinCount = 0;
    var drSecCount = 0;
    var drMinCount = 0;
    var throughputSecCount = 0;
    var throughputMinCount = 0;
    var queuedSecCount = 0;
    var queuedMinCount = 0;
    var outTransSecCount = 0;
    var outTransMinCount = 0;
    var successRateSecCount = 0;
    var successRateMinCount = 0;
    var failureRateSecCount = 0;
    var failureRateMinCount = 0;
    var totalEmptyData = 121;
    var totalEmptyDataForMinutes = 121;
    var totalEmptyDataForDays = 360;
    var cpuChart;
    var ramChart;
    var latencyChart;
    var transactionChart;
    var partitionChart;
    var drReplicationChart;
    var outTransChart;
    var successRateChart;
    var failureRateChart;
    var throughputChart;
    var queuedChart;
    var drReplicationCharts = {};
    var cmdLogChart;
    var cmdLogOverlay = [];
    var cmdLogOverlayMin = [];
    var cmdLogOverlayDay = [];
    var physicalMemory = -1;
    var Monitors = {};
    var ChartCpu = nv.models.lineChart();
    var ChartRam = nv.models.lineChart();
    var ChartLatency = nv.models.lineChart();
    var ChartTransactions = nv.models.lineChart();
    var ChartPartitionIdleTime = nv.models.lineChart();
    var ChartDrReplicationRate = nv.models.lineChart();
    var ChartOutTrans = nv.models.lineChart();
    var ChartSuccessRate = nv.models.lineChart();
    var ChartFailureRate = nv.models.lineChart();
    var ChartTupleCount = nv.models.lineChart();
    var ChartThroughput = nv.models.lineChart();
    var ChartQueued = nv.models.lineChart();
    var ChartLatencyAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartFrequencyAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartProcessingTimeAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartLatencyDetailAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartFrequencyDetailAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartCombinedDetailAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(false)
      .showControls(false);
    var ChartDataTableAnalysis = nv.models
      .multiBarHorizontalChart()
      .showLegend(false)
      .stacked(true)
      .showControls(false);

    var drChartList = {};
    var ChartCommandlog = nv.models.lineChart();
    var dataMapperSec = {};
    var dataMapperMin = {};
    var dataMapperDay = {};

    var dataMapperImporterSec = {};
    var dataMapperImporterMin = {};
    var dataMapperImporterDay = {};

    var dataMapperExporterSec = {};
    var dataMapperExporterMin = {};
    var dataMapperExporterDay = {};

    var previousSuccessRate = {};
    var previousFailureRate = {};

    this.enumPartitionColor = {
      localPartition: "#D3D3D3",
      maxMinPartition: "#4C76B0",
      multiPartition: "#FF8C00",
    };

    var colorList = [
      "#A48805",
      "#1B87C8",
      "#D3D3D3",
      "#4C76B0",
      "#FF8C00",
      "#468706",
      "#C70000",
      "#544a48",
      "#AA4567",
      "#783300",
    ];

    this.enumMaxTimeGap = {
      secGraph: 300000,
      minGraph: 1800000,
      dayGraph: 27000000,
    };

    this.GetPartitionDetailData = function (partitionDetails) {
      dataPartitionDetails = partitionDetails;
    };

    var dataImporterDetails = [];
    this.SetImporterData = function (importerDetails) {
      dataImporterDetails = importerDetails;
    };

    var dataThroughputDetails = [];
    this.SetThroughputData = function (throughputDetails) {
      dataThroughputDetails = throughputDetails;
    };

    var dataQueuedDetails = [];
    this.SetQueuedData = function (queuedDetails) {
      dataQueuedDetails = queuedDetails;
    };

    function getEmptyData() {
      var arr = [];
      var theDate = new Date();

      for (var i = totalEmptyData; i >= 0; i--) {
        arr[i] = { x: new Date(theDate.getTime()), y: null };
        theDate.setSeconds(theDate.getSeconds() - 5);
      }

      return arr;
    }

    function getEmptyDataForMinutes() {
      var arr = [];
      var theDate = new Date();

      for (var i = totalEmptyDataForMinutes; i >= 0; i--) {
        arr[i] = { x: new Date(theDate.getTime()), y: null };
        theDate.setSeconds(theDate.getSeconds() - 30);
      }

      return arr;
    }

    function getEmptyDataForDays() {
      var arr = [];
      var theDate = new Date();

      for (var i = totalEmptyDataForDays; i >= 0; i--) {
        arr[i] = { x: new Date(theDate.getTime()), y: null };
        theDate.setMinutes(theDate.getMinutes() - 5);
      }

      return arr;
    }

    function getRandomColor() {
      var letters = "0123456789ABCDEF";
      var color = "#";
      for (var i = 0; i < 6; i++) {
        color += letters[Math.floor(Math.random() * 16)];
      }
      return color;
    }

    this.getImportMapperData = function () {
      return dataMapperImporterSec;
    };

    this.getExportMapperData = function () {
      return dataMapperExporterSec;
    };

    function getThroughputExportData(emptyData, dataMapper) {
      var count = 0;
      var tupleCounts = [];
      var colorIndex = 1;
      if (dataThroughputDetails != undefined) {
        $.each(dataThroughputDetails, function (key, value) {
          if (key == "TUPLE_COUNT") {
            $.each(value, function (dataType, dataTypeValue) {
              if (dataType != "TIMESTAMP") {
                var arr = [];
                arr.push(emptyData[0]);
                arr.push(emptyData[emptyData.length - 1]);
                colorIndex++;
                tupleCounts.push({
                  key: dataType,
                  values: arr,
                  color: colorList[colorIndex],
                });
                dataMapper[dataType] = count;
                count++;
              }
            });
          }
        });
      }
      return { THROUGHPUT: tupleCounts };
    }

    function getQueuedExportData(emptyData, dataMapper) {
      var count = 0;
      var tuplePending = [];
      var colorIndex = 1;
      if (dataQueuedDetails != undefined) {
        $.each(dataQueuedDetails, function (key, value) {
          if (key == "TUPLE_PENDING") {
            $.each(value, function (dataType, dataTypeValue) {
              if (dataType != "TIMESTAMP") {
                var arr = [];
                arr.push(emptyData[0]);
                arr.push(emptyData[emptyData.length - 1]);
                colorIndex++;
                tuplePending.push({
                  key: dataType,
                  values: arr,
                  color: colorList[colorIndex],
                });
                dataMapper[dataType] = count;
                count++;
              }
            });
          }
        });
      }
      return { QUEUED: tuplePending };
    }

    function getImportData(emptyData, dataMapper) {
      var count = 0;
      var dataImporterSuccess = [];
      var dataImporterFailures = [];
      var dataImporterOutTrans = [];
      if (dataImporterDetails != undefined) {
        $.each(dataImporterDetails, function (key, value) {
          if (
            key == "SUCCESSES" ||
            key == "FAILURES" ||
            key == "OUTSTANDING_REQUESTS"
          ) {
            var colorIndex = -1;
            var prevKey = "";
            $.each(value, function (dataType, dataTypeValue) {
              if (dataType != "TIMESTAMP") {
                var arr = [];
                arr.push(emptyData[0]);
                arr.push(emptyData[emptyData.length - 1]);
                if (prevKey != key) {
                  colorIndex = 0;
                  prevKey = key;
                }
                if (key == "SUCCESSES") {
                  dataImporterSuccess.push({
                    key: dataType,
                    values: arr,
                    color: colorList[colorIndex],
                  });
                  dataMapper[dataType] = count;
                  count++;
                } else if (key == "FAILURES") {
                  dataImporterFailures.push({
                    key: dataType,
                    values: arr,
                    color: colorList[colorIndex],
                  });
                } else if (key == "OUTSTANDING_REQUESTS") {
                  dataImporterOutTrans.push({
                    key: dataType,
                    values: arr,
                    color: colorList[colorIndex],
                  });
                }
                colorIndex++;
              }
            });
          }
        });
      }
      return {
        SUCCESSES: dataImporterSuccess,
        FAILURES: dataImporterFailures,
        OUTSTANDING_REQUESTS: dataImporterOutTrans,
      };
    }

    function getEmptyDataForPartition() {
      var count = 0;
      var dataPartition = [];

      if (dataPartitionDetails != undefined) {
        $.each(dataPartitionDetails, function (key, value) {
          $.each(value, function (datatype, datatypeValue) {
            if (typeof datatypeValue === "object") {
              $.each(datatypeValue, function (partitionKey, partitionValue) {
                var arr = [];
                arr.push(emptyData[0]);
                arr.push(emptyData[emptyData.length - 1]);
                if (datatype == "data") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: MonitorGraphUI.enumPartitionColor.localPartition,
                  });
                } else if (datatype == "dataMPI") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: MonitorGraphUI.enumPartitionColor.multiPartition,
                  });
                } else if (datatype == "dataMax" || datatype == "dataMin") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: MonitorGraphUI.enumPartitionColor.maxMinPartition,
                  });
                }
                dataMapperSec[partitionKey] = count;
                count++;
              });
            }
          });
        });
      }
      return dataPartition;
    }

    function getEmptyDataForPartitionForMinutes() {
      var count = 0;
      var dataPartition = [];
      if (dataPartitionDetails != undefined) {
        $.each(dataPartitionDetails, function (key, value) {
          $.each(value, function (datatype, datatypeValue) {
            if (typeof datatypeValue === "object") {
              $.each(datatypeValue, function (partitionKey, partitionValue) {
                var arr = [];
                arr.push(emptyDataForMinutes[0]);
                arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);
                if (datatype == "data") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#D3D3D3",
                  });
                } else if (datatype == "dataMPI") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#FF8C00",
                  });
                } else if (datatype == "dataMax" || datatype == "dataMin") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#4C76B0",
                  });
                }
                dataMapperMin[partitionKey] = count;
                count++;
              });
            }
          });
        });
      }
      return dataPartition;
    }

    function getEmptyDataForPartitionForDay() {
      var count = 0;
      var dataPartition = [];
      if (dataPartitionDetails != undefined) {
        $.each(dataPartitionDetails, function (key, value) {
          $.each(value, function (datatype, datatypeValue) {
            if (typeof datatypeValue === "object") {
              $.each(datatypeValue, function (partitionKey, partitionValue) {
                var arr = [];
                arr.push(emptyDataForDays[0]);
                arr.push(emptyDataForDays[emptyDataForDays.length - 1]);
                if (datatype == "data") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#D3D3D3",
                  });
                } else if (datatype == "dataMPI") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#FF8C00",
                  });
                } else if (datatype == "dataMax" || datatype == "dataMin") {
                  dataPartition.push({
                    key: partitionKey,
                    values: arr,
                    color: "#4C76B0",
                  });
                }
                dataMapperDay[partitionKey] = count;
                count++;
              });
            }
          });
        });
      }
      return dataPartition;
    }

    var emptyData = getEmptyData();
    var emptyDataForMinutes = getEmptyDataForMinutes();
    var emptyDataForDays = getEmptyDataForDays();

    function getEmptyDataOptimized() {
      var arr = [];
      arr.push(emptyData[0]);
      arr.push(emptyData[emptyData.length - 1]);

      return arr;
    }

    function getEmptyDataForMinutesOptimized() {
      var arr = [];
      arr.push(emptyDataForMinutes[0]);
      arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);

      return arr;
    }

    function getEmptyDataForDaysOptimized() {
      var arr = [];
      arr.push(emptyDataForDays[0]);
      arr.push(emptyDataForDays[emptyDataForDays.length - 1]);

      return arr;
    }

    var dataCpu = [
      {
        key: "CPU",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataOutTrans = [
      {
        key: "Outstanding Transactions",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataThroughput = [
      {
        key: "Throughput",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataQueued = [
      {
        key: "Queued",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataSuccessRate = [
      {
        key: "Success Rate",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataFailureRate = [
      {
        key: "Failure Rate",
        values: getEmptyDataOptimized(),
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataRam = [
      {
        key: "RAM",
        values: getEmptyDataOptimized(),
        color: "rgb(164, 136, 5)",
      },
    ];

    var dataLatency = [
      {
        key: "Latency",
        values: getEmptyDataOptimized(),
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataTransactions = [
      {
        key: "Transactions",
        values: getEmptyDataOptimized(),
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataDrReplicationRate = [
      {
        key: "Replication Rate",
        values: getEmptyDataOptimized(),
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataDrReplication = {};

    var dataCommandLog = [
      {
        key: "Command Log Statistics",
        values: getEmptyDataOptimized(),
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataLatencyAnalysis = [
      {
        key: "Execution Time",
        values: [],
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataFrequencyAnalysis = [
      {
        key: "Frequency",
        values: [],
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataProcessingTimeAnalysis = [
      {
        key: "Total Processing Time",
        values: [],
        color: "rgb(27, 135, 200)",
      },
    ];

    var dataLatencyDetailAnalysis = [
      {
        key: "Avg Execution Time",
        values: [],
        color: "rgb(118, 189, 29)",
      },
    ];

    var dataFrequencyDetailAnalysis = [
      {
        key: "Frequency Detail",
        values: [],
        color: "rgb(118, 189, 29)",
      },
    ];

    var dataCombinedDetailAnalysis = [
      {
        key: "Processing Time Detail",
        values: [],
        color: "rgb(118, 189, 29)",
      },
    ];

    var dataPartitionIdleTime = [];

    var dataPartitionDetails = [];

    var dataTableAnalysis = [
      {
        key: "Tuple Count",
        values: [],
        color: "rgb(27, 135, 200)",
      },
    ];

    var barHeight = 0;

    function getBarHeightAndSpacing(dataSet, chart) {
      var dataCount = dataSet.length;
      if (dataCount == 0) {
        barHeight = 400;
      } else if (dataCount == 1) {
        barHeight = 260;
        chart.groupSpacing(0.6);
      } else if (dataCount == 2) {
        barHeight = 305;
        chart.groupSpacing(0.5);
      } else if (dataCount < 5) {
        barHeight = dataCount * 111 + 56;
        chart.groupSpacing(0.5);
      } else if (dataCount < 8) {
        barHeight = dataCount * 75 + 25;
        chart.groupSpacing(0.3);
      } else if (dataCount < 14) {
        barHeight = dataCount * 72 + 24;
        chart.groupSpacing(0.3);
      } else if (dataCount < 25) {
        barHeight = dataCount * 70 + 24;
        chart.groupSpacing(0.3);
      } else {
        barHeight = dataCount * 66 + 22;
        chart.groupSpacing(0.3);
      }
    }

    function updateTableAnalysis() {
      ChartDataTableAnalysis.update();
    }

    function updateLatencyAnalysis() {
      ChartLatencyAnalysis.update();

      d3.selectAll("#chartLatencyAnalysis .nv-bar").on(
        "click",
        function (data) {
          VoltDbUI.isTotalProcessing = false;
          VoltDbUI.isLatency = true;
          VoltDbUI.isFrequency = false;
          VoltDbUI.isData = false;
          $("#hidProcedureName").html(data.label);
          $("#hidPartitionType").html(data.type);
          $("#showAnalysisDetails").trigger("click");
        }
      );
    }

    function updateFrequencyAnalysis() {
      ChartFrequencyAnalysis.update();
      d3.selectAll("#chartFrequencyAnalysis .nv-bar").on(
        "click",
        function (data) {
          VoltDbUI.isTotalProcessing = false;
          VoltDbUI.isLatency = false;
          VoltDbUI.isFrequency = true;
          VoltDbUI.isData = false;
          $("#hidProcedureName").html(data.label);
          $("#hidPartitionType").html(data.type);
          $("#showAnalysisFreqDetails").trigger("click");
        }
      );
    }

    function updateCombinedAnalysis() {
      ChartProcessingTimeAnalysis.update();
      d3.selectAll("#chartProcessingTimeAnalysis .nv-bar").on(
        "click",
        function (data) {
          VoltDbUI.isTotalProcessing = true;
          VoltDbUI.isLatency = false;
          VoltDbUI.isFrequency = false;
          VoltDbUI.isData = false;
          $("#hidProcedureName").html(data.label);
          $("#hidPartitionType").html(data.type);
          $("#showAnalysisCombinedDetails").trigger("click");
        }
      );
    }

    var wordWrap = function (
      textObj,
      data,
      width,
      labelSpacingX,
      labelSpacingY
    ) {
      var txtElement = d3.select(textObj);
      var parentElement = d3.select(textObj.parentNode);
      parentElement
        .append("foreignObject")
        .attr("x", labelSpacingX)
        .attr("y", labelSpacingY)
        .attr("width", width)
        .attr("height", 60)
        .append("xhtml:p")
        .attr("style", "word-wrap: break-word;font-size:11px;text-align:right;")
        .html(data);
      txtElement.remove();
    };

    function updateLatencyDetailAnalysis() {
      ChartLatencyDetailAnalysis.update;
    }

    this.initializeAnalysisTableGraph = function () {
      nv.addGraph(function () {
        //                ChartDataTableAnalysis
        //                  .y(function(d) { return d.value }).height(barHeight)
        //                  .x(function(d) { return  d.label })
        //                  .showValues(true);
        $("#chartDataTableAnalysis").css("height", barHeight + 50);
        ChartDataTableAnalysis.margin({ left: 115, right: 70 });
        ChartDataTableAnalysis.valueFormat(d3.format(",.0d"));
        ChartDataTableAnalysis.yAxis.tickFormat(d3.format(",.0d"));
        ChartDataTableAnalysis.xAxis.axisLabelDistance(10);
        ChartDataTableAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualiseDataTable")
          .datum(dataTableAnalysis)
          .transition()
          .duration(350)
          .call(ChartDataTableAnalysis);
        nv.utils.windowResize(updateTableAnalysis);
        return ChartDataTableAnalysis;
      });
    };

    this.initializeAnalysisGraph = function () {
      nv.addGraph(function () {
        ChartLatencyAnalysis.y(function (d) {
          return d.value;
        })
          .height(barHeight)
          .x(function (d) {
            return d.label;
          })
          .showValues(true);

        $("#chartLatencyAnalysis").css("height", barHeight - 10);
        ChartLatencyAnalysis.margin({ left: 115, right: 40 });
        ChartLatencyAnalysis.valueFormat(d3.format(",.3f"));
        ChartLatencyAnalysis.yAxis.tickFormat(d3.format(",.2f"));
        ChartLatencyAnalysis.xAxis.axisLabelDistance(10);
        ChartLatencyAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualiseLatencyAnalysis")
          .datum(dataLatencyAnalysis)
          .transition()
          .duration(350)
          .call(ChartLatencyAnalysis);
        d3.selectAll(
          "#visualiseLatencyAnalysis .nv-barsWrap .nv-bar rect"
        ).attr("style", "cursor: pointer");
        d3.selectAll(
          "#visualiseLatencyAnalysis .nv-barsWrap .nv-bar rect"
        ).style("fill", function (d, i) {
          var procedureType = VoltDbAnalysis.procedureValue[d.label].TYPE;
          return procedureType == "Multi Partitioned" ? "#14416d" : "#1B87C8";
        });
        d3.select(
          "#visualiseLatencyAnalysis > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
        )
          .selectAll("text")
          .each(function (d, i) {
            wordWrap(this, d, 110, -115, -6);
          });
        d3.selectAll("#chartLatencyAnalysis .nv-bar").on(
          "click",
          function (data) {
            $("#hidProcedureName").html(data.label);
            $("#showAnalysisDetails").trigger("click");
          }
        );
        nv.utils.windowResize(updateLatencyAnalysis);
        return ChartLatencyAnalysis;
      });

      nv.addGraph(function () {
        ChartFrequencyAnalysis.y(function (d) {
          return d.value;
        })
          .height(barHeight)
          .x(function (d) {
            return d.label;
          })
          .showValues(true);

        $("#chartFrequencyAnalysis").css("height", barHeight - 10);
        ChartFrequencyAnalysis.margin({ left: 115, right: 40 });
        ChartFrequencyAnalysis.valueFormat(d3.format(",.0d"));
        ChartFrequencyAnalysis.yAxis.tickFormat(d3.format(",.0d"));
        ChartFrequencyAnalysis.xAxis.axisLabelDistance(10);
        ChartFrequencyAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualiseFrequencyAnalysis")
          .datum(dataFrequencyAnalysis)
          .transition()
          .duration(350)
          .call(ChartFrequencyAnalysis);
        d3.selectAll(
          "#visualiseFrequencyAnalysis .nv-barsWrap .nv-bar rect"
        ).attr("style", "cursor: pointer");
        d3.selectAll(
          "#visualiseFrequencyAnalysis .nv-barsWrap .nv-bar rect"
        ).style("fill", function (d, i) {
          var procedureType = VoltDbAnalysis.procedureValue[d.label].TYPE;
          return procedureType == "Multi Partitioned" ? "#14416d" : "#1B87C8";
        });
        d3.select(
          "#visualiseFrequencyAnalysis > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
        )
          .selectAll("text")
          .each(function (d, i) {
            wordWrap(this, d, 110, -115, -6);
          });
        d3.selectAll("#chartFrequencyAnalysis .nv-bar").on(
          "click",
          function (data) {
            $("#hidProcedureName").html(data.label);
            $("#showAnalysisFreqDetails").trigger("click");
          }
        );
        nv.utils.windowResize(updateFrequencyAnalysis);
        return ChartFrequencyAnalysis;
      });

      nv.addGraph(function () {
        ChartProcessingTimeAnalysis.y(function (d) {
          return d.value;
        })
          .height(barHeight)
          .x(function (d) {
            return d.label;
          })
          .showValues(true);

        $("#chartProcessingTimeAnalysis").css("height", barHeight - 10);
        ChartLatencyAnalysis.margin({ left: 115, right: 40 });
        ChartProcessingTimeAnalysis.valueFormat(d3.format(",.3f"));
        ChartProcessingTimeAnalysis.yAxis.tickFormat(d3.format(",.2f"));
        ChartProcessingTimeAnalysis.xAxis.axisLabelDistance(10);
        ChartProcessingTimeAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualiseProcessingTimeAnalysis")
          .datum(dataProcessingTimeAnalysis)
          .transition()
          .duration(350)
          .call(ChartProcessingTimeAnalysis);
        d3.selectAll(
          "#visualiseProcessingTimeAnalysis .nv-barsWrap .nv-bar rect"
        ).attr("style", "cursor: pointer");
        d3.selectAll(
          "#visualiseProcessingTimeAnalysis .nv-barsWrap .nv-bar rect"
        ).style("fill", function (d, i) {
          var procedureType = VoltDbAnalysis.procedureValue[d.label].TYPE;
          return procedureType == "Multi Partitioned" ? "#14416d" : "#1B87C8";
        });
        d3.select(
          "#visualiseProcessingTimeAnalysis > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
        )
          .selectAll("text")
          .each(function (d, i) {
            wordWrap(this, d, 110, -115, -6);
          });
        d3.selectAll("#chartProcessingTimeAnalysis .nv-bar").on(
          "click",
          function (data) {
            $("#hidProcedureName").html(data.label);
            $("#showAnalysisCombinedDetails").trigger("click");
          }
        );
        nv.utils.windowResize(updateCombinedAnalysis);
        return ChartProcessingTimeAnalysis;
      });
    };

    this.initializeProcedureDetailGraph = function () {
      nv.addGraph(function () {
        ChartLatencyDetailAnalysis.valueFormat(d3.format(",.6f"));
        ChartLatencyDetailAnalysis.yAxis.tickFormat(d3.format(",.2f"));
        ChartLatencyDetailAnalysis.xAxis.axisLabelDistance(10);
        ChartLatencyDetailAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualizeLatencyDetail")
          .datum(dataLatencyDetailAnalysis)
          .transition()
          .duration(350)
          .call(ChartLatencyDetailAnalysis);
        nv.utils.windowResize(ChartLatencyDetailAnalysis.update);
        return ChartLatencyDetailAnalysis;
      });
    };

    this.initializeFrequencyDetailGraph = function () {
      nv.addGraph(function () {
        ChartFrequencyDetailAnalysis.x(function (d) {
          return d.label;
        })
          .y(function (d) {
            return d.value;
          })
          .height(barHeight)
          .showValues(true);
        ChartFrequencyDetailAnalysis.valueFormat(d3.format(",.0d"));
        ChartFrequencyDetailAnalysis.yAxis.tickFormat(d3.format(",.0d"));
        ChartFrequencyDetailAnalysis.xAxis.axisLabelDistance(10);
        ChartFrequencyDetailAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualizeFrequencyDetail")
          .datum(dataFrequencyDetailAnalysis)
          .transition()
          .duration(350)
          .call(ChartFrequencyDetailAnalysis);

        nv.utils.windowResize(ChartFrequencyDetailAnalysis.update);
        return ChartFrequencyDetailAnalysis;
      });
    };

    this.initializeCombinedDetailGraph = function () {
      nv.addGraph(function () {
        ChartCombinedDetailAnalysis.valueFormat(d3.format(",.3f"));
        ChartCombinedDetailAnalysis.yAxis.tickFormat(d3.format(",.2f"));
        ChartCombinedDetailAnalysis.xAxis.axisLabelDistance(10);
        ChartCombinedDetailAnalysis.yAxis.axisLabelDistance(10);
        d3.select("#visualizeCombinedDetail")
          .datum(dataCombinedDetailAnalysis)
          .transition()
          .duration(350)
          .call(ChartCombinedDetailAnalysis);

        nv.utils.windowResize(ChartCombinedDetailAnalysis.update);
        return ChartCombinedDetailAnalysis;
      });
    };

    this.RefreshAnalysisTableGraph = function (dataTable) {
      ChartDataTableAnalysis.update;
      getBarHeightAndSpacing(dataTable[0].values, ChartDataTableAnalysis);
      ChartDataTableAnalysis.height(barHeight);
      VoltDbUI.isTotalProcessing = false;
      VoltDbUI.isLatency = false;
      VoltDbUI.isFrequency = false;
      VoltDbUI.isData = true;
      $("#chartDataTableAnalysis").css("height", barHeight - 10);
      ChartDataTableAnalysis.margin({ left: 115, right: 60 });
      dataTableAnalysis[0]["values"] = dataTable;
      d3.select("#visualiseDataTable")
        .datum(dataTable)
        .transition()
        .duration(500)
        .call(ChartDataTableAnalysis);
      d3.selectAll("#visualiseDataTable .nv-barsWrap .nv-bar rect").attr(
        "style",
        "cursor: pointer"
      );
      d3.selectAll(
        "#visualiseDataTable > g > g > g.nv-barsWrap.nvd3-svg > g > g > g > g.nv-group > g.nv-bar > rect"
      ).style("fill", function (d, i) {
        var tableType = VoltDbAnalysis.tablePropertyValue[d.x].PARTITION_TYPE;
        return tableType != "Partitioned" ? "#14416d" : "#1B87C8";
      });
      d3.select("#visualiseDataTable > g > g > g.nv-x.nv-axis.nvd3-svg > g > g")
        .selectAll("text")
        .each(function (d, i) {
          wordWrap(this, d, 110, -115, -6);
        });
    };

    this.RefreshAnalysisLatencyGraph = function (dataLatency, dataFrequency) {
      ChartLatencyAnalysis.update;
      getBarHeightAndSpacing(dataLatency, ChartLatencyAnalysis);
      ChartLatencyAnalysis.height(barHeight);
      $("#chartLatencyAnalysis").css("height", barHeight - 10);
      ChartLatencyAnalysis.margin({ left: 115, right: 40 });
      dataLatencyAnalysis[0]["values"] = dataLatency;
      d3.select("#visualiseLatencyAnalysis")
        .datum(dataLatencyAnalysis)
        .transition()
        .duration(500)
        .call(ChartLatencyAnalysis);
    };

    this.RefreshAnalysisFrequencyGraph = function (dataFrequency) {
      ChartFrequencyAnalysis.update;
      getBarHeightAndSpacing(dataFrequency, ChartFrequencyAnalysis);
      ChartFrequencyAnalysis.height(barHeight);
      $("#chartFrequencyAnalysis").css("height", barHeight - 10);
      ChartFrequencyAnalysis.margin({ left: 115, right: 40 });
      dataFrequencyAnalysis[0]["values"] = dataFrequency;
      d3.select("#visualiseFrequencyAnalysis")
        .datum(dataFrequencyAnalysis)
        .transition()
        .duration(500)
        .call(ChartFrequencyAnalysis);
    };

    this.RefreshAnalysisProcessingTimeGraph = function (dataProcessingTime) {
      ChartProcessingTimeAnalysis.update;
      getBarHeightAndSpacing(dataProcessingTime, ChartProcessingTimeAnalysis);
      ChartProcessingTimeAnalysis.height(barHeight);
      $("#chartProcessingTimeAnalysis").css("height", barHeight - 10);
      ChartProcessingTimeAnalysis.margin({ left: 115, right: 40 });
      dataProcessingTimeAnalysis[0]["values"] = dataProcessingTime;
      d3.select("#visualiseProcessingTimeAnalysis")
        .datum(dataProcessingTimeAnalysis)
        .transition()
        .duration(500)
        .call(ChartProcessingTimeAnalysis);
    };

    this.RefreshLatencyDetailGraph = function (dataLatency) {
      ChartLatencyDetailAnalysis = nv.models
        .multiBarHorizontalChart()
        .showLegend(false)
        .stacked(false)
        .showControls(false);
      if ($("#hidPartitionType").html() == "Single Partitioned") {
        ChartLatencyDetailAnalysis = nv.models
          .multiBarHorizontalChart()
          .showLegend(false)
          .stacked(true)
          .showControls(false);
        getBarHeightAndSpacing(
          dataLatency[0].values,
          ChartLatencyDetailAnalysis
        );
      } else {
        getBarHeightAndSpacing(dataLatency, ChartLatencyDetailAnalysis);
        ChartLatencyDetailAnalysis.x(function (d) {
          return d.label;
        })
          .y(function (d) {
            return d.value;
          })
          .showValues(true);
      }

      ChartLatencyDetailAnalysis.update;
      ChartLatencyDetailAnalysis.height(barHeight);
      $("#divVisualizeLatencyDetail").css("height", barHeight - 10);
      ChartLatencyDetailAnalysis.margin({ left: 80, right: 60 });
      dataLatencyDetailAnalysis[0]["values"] = dataLatency;

      if ($("#hidPartitionType").html() == "Single Partitioned") {
        d3.select("#visualizeLatencyDetail")
          .datum(dataLatency)
          .transition()
          .duration(500)
          .call(ChartLatencyDetailAnalysis);
      } else {
        d3.select("#visualizeLatencyDetail")
          .datum(dataLatencyDetailAnalysis)
          .transition()
          .duration(500)
          .call(ChartLatencyDetailAnalysis);
      }

      d3.select(
        "#visualizeLatencyDetail > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
      )
        .selectAll("text")
        .each(function (d, i) {
          wordWrap(this, d, 110, -115, -8);
        });
    };

    this.RefreshFrequencyDetailGraph = function (freqDetails) {
      ChartFrequencyDetailAnalysis = nv.models
        .multiBarHorizontalChart()
        .showLegend(false)
        .stacked(false)
        .showControls(false);
      if ($("#hidPartitionType").html() == "Single Partitioned") {
        ChartFrequencyDetailAnalysis = nv.models
          .multiBarHorizontalChart()
          .showLegend(false)
          .stacked(true)
          .showControls(false);
        getBarHeightAndSpacing(
          freqDetails[0].values,
          ChartFrequencyDetailAnalysis
        );
      } else {
        getBarHeightAndSpacing(freqDetails, ChartFrequencyDetailAnalysis);
        ChartFrequencyDetailAnalysis.x(function (d) {
          return d.label;
        })
          .y(function (d) {
            return d.value;
          })
          .showValues(true);
      }

      ChartFrequencyDetailAnalysis.update;
      ChartFrequencyDetailAnalysis.height(barHeight);
      $("#divVisualizeFreqDetail").css("height", barHeight - 10);
      ChartFrequencyDetailAnalysis.margin({ left: 80, right: 70 });
      dataFrequencyDetailAnalysis[0]["values"] = freqDetails;

      if ($("#hidPartitionType").html() == "Single Partitioned") {
        d3.select("#visualizeFrequencyDetails")
          .datum(freqDetails)
          .transition()
          .duration(500)
          .call(ChartFrequencyDetailAnalysis);
      } else {
        d3.select("#visualizeFrequencyDetails")
          .datum(dataFrequencyDetailAnalysis)
          .transition()
          .duration(500)
          .call(ChartFrequencyDetailAnalysis);
      }

      d3.select(
        "#visualizeFrequencyDetails > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
      )
        .selectAll("text")
        .each(function (d, i) {
          wordWrap(this, d, 110, -115, -8);
        });
    };

    this.RefreshCombinedDetailGraph = function (dataCombined) {
      ChartCombinedDetailAnalysis = nv.models
        .multiBarHorizontalChart()
        .showLegend(false)
        .stacked(false)
        .showControls(false);
      if ($("#hidPartitionType").html() == "Single Partitioned") {
        ChartCombinedDetailAnalysis = nv.models
          .multiBarHorizontalChart()
          .showLegend(false)
          .stacked(true)
          .showControls(false);
        getBarHeightAndSpacing(
          dataCombined[0].values,
          ChartCombinedDetailAnalysis
        );
      } else {
        getBarHeightAndSpacing(dataCombined, ChartCombinedDetailAnalysis);
        ChartCombinedDetailAnalysis.x(function (d) {
          return d.label;
        })
          .y(function (d) {
            return d.value;
          })
          .showValues(true);
      }
      ChartCombinedDetailAnalysis.update;

      $("#divVisualizeCombinedDetail").css("height", barHeight - 10);
      ChartCombinedDetailAnalysis.margin({ left: 80, right: 60 });
      dataCombinedDetailAnalysis[0]["values"] = dataCombined;
      if ($("#hidPartitionType").html() == "Single Partitioned") {
        d3.select("#visualizeCombinedDetails")
          .datum(dataCombined)
          .transition()
          .duration(500)
          .call(ChartCombinedDetailAnalysis);
      } else {
        d3.select("#visualizeCombinedDetails")
          .datum(dataCombinedDetailAnalysis)
          .transition()
          .duration(500)
          .call(ChartCombinedDetailAnalysis);
      }

      d3.select(
        "#visualizeCombinedDetails > g > g > g.nv-x.nv-axis.nvd3-svg > g > g"
      )
        .selectAll("text")
        .each(function (d, i) {
          wordWrap(this, d, 110, -115, -8);
        });
      $("#visualizeCombinedDetails").find(".nvd3").attr("x", 344);
      $("#visualizeCombinedDetails").find(".nvd3").attr("y", 172);
    };

    nv.addGraph({
      generate: function () {
        ChartCpu.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartCpu.xAxis.rotateLabels(-20);

        ChartCpu.yAxis.tickFormat(d3.format(",.2f"));

        ChartCpu.yAxis.axisLabel("(%)").axisLabelDistance(10);

        ChartCpu.margin({ left: 100 });
        ChartCpu.yAxis.scale().domain([0, 100]);
        ChartCpu.lines.forceY([0, 100]);

        d3.select("#visualisationCpu")
          .datum(dataCpu)
          .transition()
          .duration(500)
          .call(ChartCpu);

        nv.utils.windowResize(ChartCpu.update);

        return ChartCpu;
      },
      callback: function (p) {
        ChartCpu.useInteractiveGuideline(true);
        return ChartCpu;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartRam.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartRam.xAxis.rotateLabels(-20);

        ChartRam.yAxis.tickFormat(d3.format(",.4f"));

        ChartRam.yAxis.axisLabel("(GB)").axisLabelDistance(10);

        ChartRam.margin({ left: 100 });
        ChartRam.lines.forceY([0, 0.1]);

        d3.select("#visualisationRam")
          .datum(dataRam)
          .transition()
          .duration(500)
          .call(ChartRam);

        nv.utils.windowResize(ChartRam.update);
      },
      callback: function (p) {
        ChartRam.useInteractiveGuideline(true);
        return ChartCpu;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartLatency.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartLatency.xAxis.rotateLabels(-20);

        ChartLatency.yAxis.tickFormat(d3.format(",.2f"));

        ChartLatency.yAxis.axisLabel("(ms)").axisLabelDistance(10);

        ChartLatency.margin({ left: 100 });
        ChartLatency.lines.forceY([0, 1]);

        d3.select("#visualisationLatency")
          .datum(dataLatency)
          .transition()
          .duration(500)
          .call(ChartLatency);

        nv.utils.windowResize(ChartLatency.update);
      },
      callback: function (p) {
        ChartLatency.useInteractiveGuideline(true);
        return ChartLatency;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartTransactions.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartTransactions.xAxis.rotateLabels(-20);

        ChartTransactions.yAxis.tickFormat(d3.format(",.2f"));

        ChartTransactions.yAxis
          .axisLabel("(Transactions/s)")
          .axisLabelDistance(10);

        ChartTransactions.margin({ left: 100 });
        ChartTransactions.lines.forceY([0, 1]);

        d3.select("#visualisationTransaction")
          .datum(dataTransactions)
          .transition()
          .duration(500)
          .call(ChartTransactions);

        nv.utils.windowResize(ChartTransactions.update);
      },
      callback: function (p) {
        ChartTransactions.useInteractiveGuideline(true);
        return ChartTransactions;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartPartitionIdleTime.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartPartitionIdleTime.showLegend(false);
        ChartPartitionIdleTime.xAxis.rotateLabels(-20);

        ChartPartitionIdleTime.yAxis.tickFormat(d3.format(",.2f"));

        ChartPartitionIdleTime.yAxis.axisLabel("(%)").axisLabelDistance(10);

        ChartPartitionIdleTime.margin({ left: 100 });
        ChartPartitionIdleTime.yAxis.scale().domain([0, 100]);
        ChartPartitionIdleTime.lines.forceY([0, 100]);

        d3.select("#visualisationPartitionIdleTime")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartPartitionIdleTime);

        nv.utils.windowResize(ChartPartitionIdleTime.update);
      },
      callback: function () {
        ChartPartitionIdleTime.useInteractiveGuideline(true);
        return ChartPartitionIdleTime;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartCommandlog.showLegend(false);

        ChartCommandlog.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartCommandlog.xAxis.rotateLabels(-20);

        ChartCommandlog.yAxis.tickFormat(d3.format(",.2f"));

        ChartCommandlog.yAxis
          .axisLabel("(Pending Transactions)")
          .axisLabelDistance(10);

        ChartCommandlog.margin({ left: 100 });
        ChartCommandlog.lines.forceY([0, 0.1]);

        d3.select("#visualisationCommandLog")
          .datum(dataCommandLog)
          .transition()
          .duration(500)
          .call(ChartCommandlog);

        nv.utils.windowResize(ChartCommandlog.update);
      },
      callback: function () {
        ChartCommandlog.useInteractiveGuideline(true);
        return ChartCommandlog;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartThroughput.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartThroughput.xAxis.rotateLabels(-20);

        ChartThroughput.yAxis.tickFormat(d3.format(",.2f"));

        ChartThroughput.yAxis.axisLabel("(Counts)").axisLabelDistance(10);

        ChartThroughput.margin({ left: 100 });
        ChartThroughput.lines.forceY([0, 0.1]);

        d3.select("#visualisationThroughput")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartThroughput);

        nv.utils.windowResize(ChartThroughput.update);
      },
      callback: function () {
        ChartThroughput.useInteractiveGuideline(true);
        return ChartThroughput;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartQueued.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartQueued.xAxis.rotateLabels(-20);

        ChartQueued.yAxis.tickFormat(d3.format(",.2f"));

        ChartQueued.yAxis.axisLabel("(Counts)").axisLabelDistance(10);

        ChartQueued.margin({ left: 100 });
        ChartQueued.lines.forceY([0, 0.1]);

        d3.select("#visualisationQueued")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartQueued);

        nv.utils.windowResize(ChartQueued.update);
      },
      callback: function () {
        ChartQueued.useInteractiveGuideline(true);
        return ChartQueued;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartOutTrans.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartOutTrans.xAxis.rotateLabels(-20);

        ChartOutTrans.yAxis.tickFormat(d3.format(",.2f"));

        ChartOutTrans.yAxis.axisLabel("(Transactions)").axisLabelDistance(10);

        ChartOutTrans.margin({ left: 100 });
        ChartOutTrans.lines.forceY([0, 0.1]);

        d3.select("#visualisationOutTrans")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartOutTrans);

        nv.utils.windowResize(ChartOutTrans.update);
      },
      callback: function () {
        ChartOutTrans.useInteractiveGuideline(true);
        return ChartOutTrans;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartSuccessRate.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartSuccessRate.xAxis.rotateLabels(-20);

        ChartSuccessRate.yAxis.tickFormat(d3.format(",.2f"));

        ChartSuccessRate.yAxis
          .axisLabel("(Transactions/s)")
          .axisLabelDistance(10);

        ChartSuccessRate.margin({ left: 100 });
        ChartSuccessRate.lines.forceY([0, 0.1]);

        d3.select("#visualisationSuccessRate")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartSuccessRate);

        nv.utils.windowResize(ChartSuccessRate.update);
      },
      callback: function () {
        ChartSuccessRate.useInteractiveGuideline(true);
        return ChartSuccessRate;
      },
    });

    nv.addGraph({
      generate: function () {
        ChartFailureRate.xAxis.tickFormat(function (d) {
          return d3.time.format("%X")(new Date(d));
        });

        ChartFailureRate.xAxis.rotateLabels(-20);

        ChartFailureRate.yAxis.tickFormat(d3.format(",.2f"));

        ChartFailureRate.yAxis
          .axisLabel("(Transactions/s)")
          .axisLabelDistance(10);

        ChartFailureRate.margin({ left: 100 });
        ChartFailureRate.lines.forceY([0, 0.1]);

        d3.select("#visualisationFailureRate")
          .datum([])
          .transition()
          .duration(500)
          .call(ChartFailureRate);

        nv.utils.windowResize(ChartFailureRate.update);
      },
      callback: function () {
        ChartFailureRate.useInteractiveGuideline(true);
        return ChartFailureRate;
      },
    });

    goog.math.Long.prototype.numberOfLeadingZeros = function () {
      var n = 1;
      var x = this.high_;
      if (x == 0) {
        n += 32;
        x = this.low_;
      }
      if (x >>> 16 == 0) {
        n += 16;
        x <<= 16;
      }
      if (x >>> 24 == 0) {
        n += 8;
        x <<= 8;
      }
      if (x >>> 28 == 0) {
        n += 4;
        x <<= 4;
      }
      if (x >>> 30 == 0) {
        n += 2;
        x <<= 2;
      }
      n -= x >>> 31;
      return n;
    };

    function read32(str) {
      var s1 = str.substring(0, 2);
      var s2 = str.substring(2, 4);
      var s3 = str.substring(4, 6);
      var s4 = str.substring(6, 8);
      return s4 + s3 + s2 + s1;
    }

    function read64(str) {
      var s1 = read32(str);
      var s2 = read32(str.substring(8, 16));
      return s2 + s1;
    }

    var getEmptyDataForView = function (view) {
      view = view != undefined ? view.toLowerCase() : "seconds";

      if (view == "minutes") return getEmptyDataForMinutesOptimized();
      else if (view == "days") return getEmptyDataForDaysOptimized();

      return getEmptyDataOptimized();
    };

    var getEmptyDataForPartitionView = function (view) {
      view = view != undefined ? view.toLowerCase() : "seconds";

      if (view == "minutes") return getEmptyDataForPartitionForMinutes();
      else if (view == "days") return getEmptyDataForPartitionForDay();

      return getEmptyDataForPartition();
    };

    var getEmptyDataForImporterView = function (view) {
      view = view != undefined ? view.toLowerCase() : "seconds";

      if (view == "minutes")
        return getImportData(emptyDataForMinutes, dataMapperImporterMin);
      else if (view == "days")
        return getImportData(emptyDataForDays, dataMapperImporterDay);

      return getImportData(emptyData, dataMapperImporterSec);
    };

    var getEmptyDataForExporterView = function (view) {
      view = view != undefined ? view.toLowerCase() : "seconds";

      if (view == "minutes")
        return getThroughputExportData(
          emptyDataForMinutes,
          dataMapperExporterMin
        );
      else if (view == "days")
        return getThroughputExportData(emptyDataForDays, dataMapperExporterDay);

      return getThroughputExportData(emptyData, dataMapperExporterSec);
    };

    this.AddGraph = function (
      view,
      cpuChartObj,
      ramChartObj,
      clusterChartObj,
      transactinoChartObj,
      partitionChartObj,
      drReplicationCharObj,
      cmdLogChartObj
    ) {
      cpuChart = cpuChartObj;
      ramChart = ramChartObj;
      latencyChart = clusterChartObj;
      transactionChart = transactinoChartObj;
      partitionChart = partitionChartObj;
      drReplicationChart = drReplicationCharObj;
      cmdLogChart = cmdLogChartObj;
      currentView = view;
      Monitors = {
        latHistogram: {},
        latData: getEmptyDataOptimized(),
        latDataMin: getEmptyDataForMinutesOptimized(),
        latDataDay: getEmptyDataForDaysOptimized(),
        latFirstData: true,
        latMaxTimeStamp: null,
        tpsData: getEmptyDataOptimized(),
        tpsDataMin: getEmptyDataForMinutesOptimized(),
        tpsDataDay: getEmptyDataForDaysOptimized(),
        tpsFirstData: true,
        tpsMaxTimeStamp: null,
        memData: getEmptyDataOptimized(),
        memDataMin: getEmptyDataForMinutesOptimized(),
        memDataDay: getEmptyDataForDaysOptimized(),
        memFirstData: true,
        memMaxTimeStamp: null,
        cpuData: getEmptyDataOptimized(),
        cpuDataMin: getEmptyDataForMinutesOptimized(),
        cpuDataHrs: getEmptyDataForDaysOptimized(),
        cpuFirstData: true,
        cpuMaxTimeStamp: null,
        partitionData: getEmptyDataForPartition(),
        partitionDataMin: getEmptyDataForPartitionForMinutes(),
        partitionDataDay: getEmptyDataForPartitionForDay(),
        partitionFirstData: true,
        partitionMaxTimeStamp: null,
        cmdLogData: getEmptyDataOptimized(),
        cmdLogDataMin: getEmptyDataForMinutesOptimized(),
        cmdLogDataDay: getEmptyDataForDaysOptimized(),
        cmdLogFirstData: true,
        cmdLogMaxTimeStamp: null,
        lastTimedTransactionCount: -1,
        lastTimerTick: -1,
      };
      dataCpu[0]["values"] = getEmptyDataForView(view);
      dataRam[0]["values"] = getEmptyDataForView(view);
      dataLatency[0]["values"] = getEmptyDataForView(view);
      dataTransactions[0]["values"] = getEmptyDataForView(view);
      dataPartitionIdleTime = getEmptyDataForPartitionView(view);
      dataCommandLog[0]["values"] = getEmptyDataForView(view);
      changeAxisTimeFormat(view);
    };

    this.InitializeDrData = function () {
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          dataDrReplication["dataDrReplication_" + chartList[i]] = [
            {
              key: "Replication Rate",
              values: getEmptyDataOptimized(),
              color: "rgb(27, 135, 200)",
            },
          ];
        }
      }
    };

    this.AddDrGraph = function (view) {
      currentViewDr = view;
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          drReplicationCharts["ChartDrReplicationRate_" + chartList[i]] = $(
            "#ChartDrReplicationRate_" + chartList[i]
          );
          Monitors["drReplicationData_" + chartList[i]] =
            getEmptyDataOptimized();
          Monitors["drReplicationDataMin_" + chartList[i]] =
            getEmptyDataForMinutesOptimized();
          Monitors["drReplicationDataDay_" + chartList[i]] =
            getEmptyDataForDaysOptimized();
          Monitors["drFirstData_" + chartList[i]] = true;
          Monitors["drMaxTimeStamp_" + chartList[i]] = null;
          Monitors["drSecCount_" + chartList[i]] = 0;
          Monitors["drMinCount_" + chartList[i]] = 0;
          dataDrReplication["dataDrReplication_" + chartList[i]][0]["values"] =
            getEmptyDataForView();
        }
      }
      changeDrAxisTimeFormat(view);
    };

    this.AddImporterGraph = function (
      view,
      outTransChartObj,
      successRateChartObj,
      failureRateChartObj
    ) {
      outTransChart = outTransChartObj;
      successRateChart = successRateChartObj;
      failureRateChart = failureRateChartObj;
      currentViewImporter = view;

      Monitors["outTransData"] = getImportData(
        emptyData,
        dataMapperImporterSec
      )["OUTSTANDING_REQUESTS"];
      Monitors["outTransDataMin"] = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      )["OUTSTANDING_REQUESTS"];
      Monitors["outTransDataDay"] = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      )["OUTSTANDING_REQUESTS"];
      Monitors["outTransFirstData"] = true;
      Monitors["outTransMaxTimeStamp"] = null;

      Monitors["successRateData"] = getImportData(
        emptyData,
        dataMapperImporterSec
      )["SUCCESSES"];
      Monitors["successRateDataMin"] = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      )["SUCCESSES"];
      Monitors["successRateDataDay"] = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      )["SUCCESSES"];
      Monitors["successRateFirstData"] = true;
      Monitors["successRateMaxTimeStamp"] = null;

      Monitors["failureRateData"] = getImportData(
        emptyData,
        dataMapperImporterSec
      )["FAILURES"];
      Monitors["failureRateDataMin"] = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      )["FAILURES"];
      Monitors["failureRateDataDay"] = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      )["FAILURES"];
      Monitors["failureRateFirstData"] = true;
      Monitors["failureRateMaxTimeStamp"] = null;

      dataOutTrans = getEmptyDataForImporterView(view)["OUTSTANDING_REQUESTS"];
      dataSuccessRate = getEmptyDataForImporterView(view)["SUCCESSES"];
      dataFailureRate = getEmptyDataForImporterView(view)["FAILURES"];

      changeImporterAxisTimeFormat(view);
    };

    this.AddThroughputGraph = function (view, throughputChartObj) {
      // successRateChart = successRateChartObj;
      // failureRateChart = failureRateChartObj;
      throughputChart = throughputChartObj;
      currentViewExporter = view;

      // Monitors['successRateData'] = getImportData(emptyData, dataMapperImporterSec)['SUCCESSES'];
      // Monitors['successRateDataMin'] = getImportData(emptyDataForMinutes, dataMapperImporterMin)['SUCCESSES'];
      // Monitors['successRateDataDay'] = getImportData(emptyDataForDays, dataMapperImporterDay)['SUCCESSES'];
      // Monitors['successRateFirstData'] = true;
      // Monitors['successRateMaxTimeStamp'] = null;
      //
      // Monitors['failureRateData'] = getImportData(emptyData, dataMapperImporterSec)['FAILURES'];
      // Monitors['failureRateDataMin'] = getImportData(emptyDataForMinutes, dataMapperImporterMin)['FAILURES'];
      // Monitors['failureRateDataDay'] = getImportData(emptyDataForDays, dataMapperImporterDay)['FAILURES'];
      // Monitors['failureRateFirstData'] = true;
      // Monitors['failureRateMaxTimeStamp'] = null;

      Monitors["throughputData"] = getThroughputExportData(
        emptyData,
        dataMapperExporterSec
      )["THROUGHPUT"];
      Monitors["throughputDataMin"] = getThroughputExportData(
        emptyDataForMinutes,
        dataMapperExporterMin
      )["THROUGHPUT"];
      Monitors["throughputDataDay"] = getThroughputExportData(
        emptyDataForDays,
        dataMapperExporterDay
      )["THROUGHPUT"];
      Monitors["throughputFirstData"] = true;
      Monitors["throughputMaxTimeStamp"] = null;

      dataSuccessRate = getEmptyDataForExporterView(view)["SUCCESSES"];
      dataFailureRate = getEmptyDataForExporterView(view)["FAILURES"];
      dataThroughput = getEmptyDataForExporterView(view)["THROUGHPUT"];

      changeExporterAxisTimeFormat(view);
    };

    this.AddQueuedGraph = function (view, queuedChartObj) {
      queuedChart = queuedChartObj;
      currentViewExporter = view;

      Monitors["queuedData"] = getQueuedExportData(
        emptyData,
        dataMapperExporterSec
      )["QUEUED"];
      Monitors["queuedDataMin"] = getQueuedExportData(
        emptyDataForMinutes,
        dataMapperExporterMin
      )["QUEUED"];
      Monitors["queuedDataDay"] = getQueuedExportData(
        emptyDataForDays,
        dataMapperExporterDay
      )["QUEUED"];
      Monitors["queuedFirstData"] = true;
      Monitors["queuedMaxTimeStamp"] = null;

      changeExporterAxisTimeFormat(view);
    };

    this.RefreshGraph = function (view) {
      currentView = view;
      if (view == "Days") {
        dataCpu[0]["values"] = Monitors.cpuDataHrs;
        dataTransactions[0]["values"] = Monitors.tpsDataDay;
        dataRam[0]["values"] = Monitors.memDataDay;
        dataLatency[0]["values"] = Monitors.latDataDay;
        dataPartitionIdleTime = Monitors.partitionDataDay;
        dataCommandLog[0]["values"] = Monitors.cmdLogDataDay;
      } else if (view == "Minutes") {
        dataCpu[0]["values"] = Monitors.cpuDataMin;
        dataTransactions[0]["values"] = Monitors.tpsDataMin;
        dataRam[0]["values"] = Monitors.memDataMin;
        dataLatency[0]["values"] = Monitors.latDataMin;
        dataPartitionIdleTime = Monitors.partitionDataMin;
        dataCommandLog[0]["values"] = Monitors.cmdLogDataMin;
      } else {
        dataCpu[0]["values"] = Monitors.cpuData;
        dataTransactions[0]["values"] = Monitors.tpsData;
        dataRam[0]["values"] = Monitors.memData;
        dataLatency[0]["values"] = Monitors.latData;
        dataPartitionIdleTime = Monitors.partitionData;
        dataCommandLog[0]["values"] = Monitors.cmdLogData;
      }

      nv.utils.windowResize(ChartCpu.update);
      changeAxisTimeFormat(view);
    };

    this.RefreshDrGraph = function (view) {
      currentViewDr = view;
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          if (view == "Days") {
            dataDrReplication["dataDrReplication_" + chartList[i]][0][
              "values"
            ] = Monitors["drReplicationDataDay_" + chartList[i]];
          } else if (view == "Minutes") {
            dataDrReplication["dataDrReplication_" + chartList[i]][0][
              "values"
            ] = Monitors["drReplicationDataMin_" + chartList[i]];
          } else {
            dataDrReplication["dataDrReplication_" + chartList[i]][0][
              "values"
            ] = Monitors["drReplicationData_" + chartList[i]];
          }
        }
      }

      changeDrAxisTimeFormat(view);
    };

    this.RefreshImporterGraph = function (view) {
      currentViewImporter = view;
      if (view == "Days") {
        dataOutTrans = Monitors.outTransDataDay;
        dataSuccessRate = Monitors.successRateDataDay;
        dataFailureRate = Monitors.failureRateDataDay;
      } else if (view == "Minutes") {
        dataOutTrans = Monitors.outTransDataMin;
        dataSuccessRate = Monitors.successRateDataMin;
        dataFailureRate = Monitors.failureRateDataMin;
      } else {
        dataOutTrans = Monitors.outTransData;
        dataSuccessRate = Monitors.successRateData;
        dataFailureRate = Monitors.failureRateData;
      }
      changeImporterAxisTimeFormat(view);

      d3.select("#visualisationSuccessRate")
        .datum(dataSuccessRate)
        .transition()
        .duration(500)
        .call(ChartSuccessRate);
      d3.select("#visualisationFailureRate")
        .datum(dataFailureRate)
        .transition()
        .duration(500)
        .call(ChartFailureRate);
      d3.select("#visualisationOutTrans")
        .datum(dataOutTrans)
        .transition()
        .duration(500)
        .call(ChartOutTrans);
    };

    this.RefreshThroughputExporterGraph = function (view) {
      currentViewExporter = view;
      if (view == "Days") {
        dataThroughput = Monitors.throughputDataDay;
      } else if (view == "Minutes") {
        dataThroughput = Monitors.throughputDataMin;
      } else {
        dataThroughput = Monitors.throughputData;
      }
      changeExporterAxisTimeFormat(view);

      d3.select("#visualisationThroughput")
        .datum(dataThroughput)
        .transition()
        .duration(500)
        .call(ChartThroughput);
    };

    this.RefreshQueuedExporterGraph = function (view) {
      currentViewExporter = view;
      if (view == "Days") {
        dataQueued = Monitors.queuedDataDay;
      } else if (view == "Minutes") {
        dataQueued = Monitors.queuedDataMin;
      } else {
        dataQueued = Monitors.queuedData;
      }
      changeExporterAxisTimeFormat(view);

      d3.select("#visualisationQueued")
        .datum(dataQueued)
        .transition()
        .duration(500)
        .call(ChartQueued);
    };

    this.AddImporterGraphLine = function (
      dataType,
      keyValue,
      timeUnit,
      colorIndex
    ) {
      var arr = [];
      if (timeUnit == "second") {
        arr.push(emptyData[0]);
        arr.push(emptyData[emptyData.length - 1]);
      } else if (timeUnit == "minute") {
        arr.push(emptyDataForMinutes[0]);
        arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);
      } else if (timeUnit == "day") {
        arr.push(emptyDataForDays[0]);
        arr.push(emptyDataForDays[emptyDataForDays.length - 1]);
      }
      Monitors[dataType].push({
        key: keyValue,
        values: arr,
        color: colorList[colorIndex],
      });
      if (dataType == "successRateData") {
        dataMapperImporterSec[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperImporterSec
        );
        dataMapperImporterMin[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperImporterMin
        );
        dataMapperImporterDay[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperImporterDay
        );
      }
    };

    this.AddExporterGraphLine = function (
      dataType,
      keyValue,
      timeUnit,
      colorIndex
    ) {
      var arr = [];
      if (timeUnit == "second") {
        arr.push(emptyData[0]);
        arr.push(emptyData[emptyData.length - 1]);
      } else if (timeUnit == "minute") {
        arr.push(emptyDataForMinutes[0]);
        arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);
      } else if (timeUnit == "day") {
        arr.push(emptyDataForDays[0]);
        arr.push(emptyDataForDays[emptyDataForDays.length - 1]);
      }
      Monitors[dataType].push({
        key: keyValue,
        values: arr,
        color: colorList[colorIndex],
      });
      if (dataType == "throughputData" || dataType == "queuedData") {
        dataMapperExporterSec[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperExporterSec
        );
        dataMapperExporterMin[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperExporterMin
        );
        dataMapperExporterDay[keyValue] = MonitorGraphUI.getDataMapperIndex(
          dataMapperExporterDay
        );
      }
    };

    this.getDataMapperIndex = function (dataMap) {
      var count = 0;
      $.each(dataMap, function (key, value) {
        if (dataMap[key] > count) count = dataMap[key];
      });
      count++;
      return count;
    };

    this.UpdateCharts = function () {
      if (ramChart.is(":visible")) ChartRam.update();

      if (cpuChart.is(":visible")) ChartCpu.update();

      if (latencyChart.is(":visible")) ChartLatency.update();

      if (transactionChart.is(":visible")) ChartTransactions.update();

      if (partitionChart.is(":visible")) ChartPartitionIdleTime.update();

      if (cmdLogChart.is(":visible")) ChartCommandlog.update();
    };

    this.UpdateImporterCharts = function () {
      if (outTransChart.is(":visible")) ChartOutTrans.update();

      if (successRateChart.is(":visible")) ChartSuccessRate.update();

      if (failureRateChart.is(":visible")) ChartFailureRate.update();
    };

    this.UpdateExporterCharts = function () {
      if (throughputChart.is(":visible")) ChartThroughput.update();
      if (queuedChart.is(":visible")) ChartQueued.update();
    };

    this.UpdateDrCharts = function () {
      var chartList = VoltDbUI.drChartList;
      if (
        chartList != undefined &&
        chartList.length > 0 &&
        !$.isEmptyObject(drReplicationCharts)
      ) {
        for (var i = 0; i < chartList.length; i++) {
          if (
            drReplicationCharts["ChartDrReplicationRate_" + chartList[i]].is(
              ":visible"
            )
          ) {
            drChartList["ChartDrReplicationRate_" + chartList[i]].update();
          }
        }
      }
    };

    this.InitializeDRGraph = function () {
      var drChartIds = VoltDbUI.drChartList;
      if (drChartIds.length > 0) {
        for (var i = 0; i < drChartIds.length; i++) {
          initializeGraph(drChartIds[i]);
        }
      }
    };

    var initializeGraph = function (i) {
      drChartList["ChartDrReplicationRate_" + i] = nv.models.lineChart();

      nv.addGraph({
        generate: function () {
          drChartList["ChartDrReplicationRate_" + i].xAxis.tickFormat(function (
            d
          ) {
            return d3.time.format("%X")(new Date(d));
          });

          drChartList["ChartDrReplicationRate_" + i].xAxis.rotateLabels(-20);

          drChartList["ChartDrReplicationRate_" + i].yAxis.tickFormat(
            d3.format(",.2f")
          );

          drChartList["ChartDrReplicationRate_" + i].yAxis
            .axisLabel("(KBps)")
            .axisLabelDistance(10);

          drChartList["ChartDrReplicationRate_" + i].margin({ left: 100 });
          drChartList["ChartDrReplicationRate_" + i].lines.forceY([0, 1]);
          d3.select("#visualizationDrReplicationRate_" + i)
            .datum(dataDrReplication["dataDrReplication_" + i])
            .transition()
            .duration(500)
            .call(drChartList["ChartDrReplicationRate_" + i]);

          nv.utils.windowResize(
            drChartList["ChartDrReplicationRate_" + i].update
          );
        },
        callback: function () {
          drChartList["ChartDrReplicationRate_" + i].useInteractiveGuideline(
            true
          );
          return drChartList["ChartDrReplicationRate_" + i];
        },
      });
    };

    var changeAxisTimeFormat = function (view) {
      var dateFormat = "%X";
      if (view == "Days") dateFormat = "%d %b %X";

      ChartCpu.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartRam.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartLatency.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartTransactions.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartPartitionIdleTime.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartCommandlog.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
    };

    var changeDrAxisTimeFormat = function (view) {
      var dateFormat = "%X";
      if (view == "Days") dateFormat = "%d %b %X";

      var chartIds = VoltDbUI.drChartList;
      if (chartIds.length > 0) {
        for (var i = 0; i < chartIds.length; i++) {
          drChartList["ChartDrReplicationRate_" + chartIds[i]].xAxis.tickFormat(
            function (d) {
              return d3.time.format(dateFormat)(new Date(d));
            }
          );
        }
      }
    };

    var changeImporterAxisTimeFormat = function (view) {
      var dateFormat = "%X";
      if (view == "Days") dateFormat = "%d %b %X";

      ChartOutTrans.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartSuccessRate.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartFailureRate.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
    };

    var changeExporterAxisTimeFormat = function (view) {
      var dateFormat = "%X";
      if (view == "Days") dateFormat = "%d %b %X";

      ChartThroughput.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
      ChartQueued.xAxis.tickFormat(function (d) {
        return d3.time.format(dateFormat)(new Date(d));
      });
    };

    var dataView = {
      Seconds: 0,
      Minutes: 1,
      Days: 2,
    };

    function sliceFirstData(dataArray, view) {
      var total = totalEmptyData;
      var refEmptyData = emptyData;

      if (view == dataView.Minutes) {
        total = totalEmptyDataForMinutes;
        refEmptyData = emptyDataForMinutes;
      } else if (view == dataView.Days) {
        total = totalEmptyDataForDays;
        refEmptyData = emptyDataForDays;
      }

      if (dataArray.length <= total)
        dataArray[0] = refEmptyData[dataArray.length - 1];
      else dataArray = dataArray.slice(1);

      return dataArray;
    }

    var currentTime = new Date();

    this.setStartTime = function () {
      currentTime = new Date();
    };

    this.RefreshLatency = function (
      latency,
      graphView,
      currentTab,
      currentServer
    ) {
      var monitor = Monitors;
      var dataLat = monitor.latData;
      var dataLatMin = monitor.latDataMin;
      var dataLatDay = monitor.latDataDay;
      var timeStamp;
      var maxLatency = 0;
      var latencyArr = [];
      var latencyArrMin = [];
      var latencyArrDay = [];
      if (
        $.isEmptyObject(latency) ||
        latency == undefined ||
        !latency.hasOwnProperty("CLUSTER_DETAILS") ||
        latency["CLUSTER_DETAILS"].P99 == undefined ||
        latency["CLUSTER_DETAILS"].TIMESTAMP == undefined
      )
        return;

      if (localStorage.latencyMin != undefined) {
        latencyArrMin = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.latencyMin)
        );
      } else {
        latencyArrMin = JSON.stringify(
          convertDataFormat(dataLatMin, "timestamp", "latency")
        );
        latencyArrMin = JSON.parse(latencyArrMin);
      }

      if (localStorage.latency != undefined) {
        latencyArr = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.latency)
        );
      } else {
        latencyArr = JSON.stringify(
          convertDataFormat(dataLat, "timestamp", "latency")
        );
        latencyArr = JSON.parse(latencyArr);
      }

      if (localStorage.latencyDay != undefined) {
        latencyArrDay = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.latencyDay)
        );
      } else {
        latencyArrDay = JSON.stringify(
          convertDataFormat(dataLatDay, "timestamp", "latency")
        );
        latencyArrDay = JSON.parse(latencyArrDay);
      }

      if (monitor.latFirstData) {
        if (
          latencyArr.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(latencyArr[latencyArr.length - 1].timestamp).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          dataLat = [];
          for (var i = 0; i < latencyArr.length; i++) {
            dataLat = sliceFirstData(dataLat, dataView.Seconds);
            dataLat.push({
              x: new Date(latencyArr[i].timestamp),
              y: latencyArr[i].latency,
            });
          }
        }
        if (
          latencyArrMin.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                latencyArrMin[latencyArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          dataLatMin = [];
          for (var j = 0; j < latencyArrMin.length; j++) {
            dataLatMin = sliceFirstData(dataLatMin, dataView.Minutes);
            dataLatMin.push({
              x: new Date(latencyArrMin[j].timestamp),
              y: latencyArrMin[j].latency,
            });
          }
        }

        if (
          latencyArrDay.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                latencyArrMin[latencyArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          dataLatDay = [];
          for (var k = 0; k < latencyArrDay.length; k++) {
            dataLatDay = sliceFirstData(dataLatDay, dataView.Days);
            dataLatDay.push({
              x: new Date(latencyArrDay[k].timestamp),
              y: latencyArrDay[k].latency,
            });
          }
        }
      }

      var timeStamp = new Date(latency["CLUSTER_DETAILS"].TIMESTAMP);
      var lat = parseFloat(latency["CLUSTER_DETAILS"].P99).toFixed(1) * 1;

      if (monitor.latMaxTimeStamp <= timeStamp) {
        if (latSecCount >= 6 || monitor.latFirstData) {
          dataLatMin = sliceFirstData(dataLatMin, dataView.Minutes);
          if (monitor.latMaxTimeStamp == timeStamp) {
            dataLatMin.push({
              x: new Date(timeStamp),
              y: dataLatMin[dataLatMin.length - 1].y,
            });
            latencyArrMin = saveLocalStorageInterval(latencyArrMin, {
              timestamp: new Date(timeStamp),
              latency: dataLatMin[dataLatMin.length - 1].y,
            });
          } else {
            dataLatMin.push({ x: new Date(timeStamp), y: lat });
            latencyArrMin = saveLocalStorageInterval(latencyArrMin, {
              timestamp: new Date(timeStamp),
              latency: lat,
            });
          }
          Monitors.latDataMin = dataLatMin;
          latSecCount = 0;
        }

        if (latMinCount >= 60 || monitor.latFirstData) {
          dataLatDay = sliceFirstData(dataLatDay, dataView.Days);
          if (monitor.latMaxTimeStamp == timeStamp) {
            dataLatDay.push({
              x: new Date(timeStamp),
              y: dataLatDay[dataLatDay.length - 1].y,
            });
            latencyArrDay = saveLocalStorageInterval(latencyArrDay, {
              timestamp: new Date(timeStamp),
              latency: dataLatMin[dataLatMin.length - 1].y,
            });
          } else {
            dataLatDay.push({ x: new Date(timeStamp), y: lat });
            latencyArrDay = saveLocalStorageInterval(latencyArrDay, {
              timestamp: new Date(timeStamp),
              latency: lat,
            });
          }
          Monitors.latDataDay = dataLatDay;
          latMinCount = 0;
        }

        dataLat = sliceFirstData(dataLat, dataView.Seconds);
        if (monitor.latMaxTimeStamp == timeStamp) {
          dataLat.push({
            x: new Date(timeStamp),
            y: dataLat[dataLat.length - 1].y,
          });
          latencyArr = saveLocalStorageInterval(latencyArr, {
            timestamp: new Date(timeStamp),
            latency: dataLat[dataLat.length - 1].y,
          });
        } else {
          dataLat.push({ x: new Date(timeStamp), y: lat });
          latencyArr = saveLocalStorageInterval(latencyArr, {
            timestamp: new Date(timeStamp),
            latency: lat,
          });
        }
        Monitors.latData = dataLat;

        localStorage.latency = JSON.stringify(latencyArr);
        localStorage.latencyMin = JSON.stringify(latencyArrMin);
        localStorage.latencyDay = JSON.stringify(latencyArrDay);

        if (graphView == "Minutes") dataLatency[0]["values"] = dataLatMin;
        else if (graphView == "Days") dataLatency[0]["values"] = dataLatDay;
        else dataLatency[0]["values"] = dataLat;

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          latencyChart.is(":visible")
        ) {
          d3.select("#visualisationLatency")
            .datum(dataLatency)
            .transition()
            .duration(500)
            .call(ChartLatency);
        }
        monitor.latFirstData = false;
      }
      if (timeStamp > monitor.latMaxTimeStamp)
        monitor.latMaxTimeStamp = timeStamp;
      latSecCount++;
      latMinCount++;
      latency = null;
    };

    this.RefreshMemory = function (
      memoryDetails,
      currentServer,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;
      var dataMem = monitor.memData;
      var dataMemMin = monitor.memDataMin;
      var dataMemDay = monitor.memDataDay;
      var memDetails = memoryDetails;
      var x = 0;
      var y = 0;
      var memoryDetailsArr = [];
      var memoryDetailsArrMin = [];
      var memoryDetailsArrDay = [];

      if (
        $.isEmptyObject(memDetails) ||
        memDetails == undefined ||
        memDetails[currentServer].PHYSICALMEMORY == undefined ||
        memDetails[currentServer].RSS == undefined ||
        memDetails[currentServer].TIMESTAMP == undefined
      )
        return;

      if (localStorage.memoryDetailsMin != undefined) {
        memoryDetailsArrMin = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.memoryDetailsMin)
        );
      } else {
        memoryDetailsArrMin = JSON.stringify(
          convertDataFormat(dataMemMin, "timestamp", "physicalMemory")
        );
        memoryDetailsArrMin = JSON.parse(memoryDetailsArrMin);
      }

      if (localStorage.memoryDetails != undefined) {
        memoryDetailsArr = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.memoryDetails)
        );
      } else {
        memoryDetailsArr = JSON.stringify(
          convertDataFormat(dataMem, "timestamp", "physicalMemory")
        );
        memoryDetailsArr = JSON.parse(memoryDetailsArr);
      }

      if (localStorage.memoryDetailsDay != undefined) {
        memoryDetailsArrDay = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.memoryDetailsDay)
        );
      } else {
        memoryDetailsArrDay = JSON.stringify(
          convertDataFormat(dataMemDay, "timestamp", "physicalMemory")
        );
        memoryDetailsArrDay = JSON.parse(memoryDetailsArrDay);
      }

      if (monitor.memFirstData) {
        if (
          memoryDetailsArr.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                memoryDetailsArr[memoryDetailsArr.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          dataMem = [];
          for (var i = 0; i < memoryDetailsArr.length; i++) {
            dataMem = sliceFirstData(dataMem, dataView.Seconds);
            dataMem.push({
              x: new Date(memoryDetailsArr[i].timestamp),
              y: memoryDetailsArr[i].physicalMemory,
            });
          }
        }
        if (
          memoryDetailsArrMin.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                memoryDetailsArrMin[memoryDetailsArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          dataMemMin = [];
          for (var j = 0; j < memoryDetailsArrMin.length; j++) {
            dataMemMin = sliceFirstData(dataMemMin, dataView.Minutes);
            dataMemMin.push({
              x: new Date(memoryDetailsArrMin[j].timestamp),
              y: memoryDetailsArrMin[j].physicalMemory,
            });
          }
        }

        if (
          memoryDetailsArrDay.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                memoryDetailsArrDay[memoryDetailsArrDay.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          dataMemDay = [];
          for (var k = 0; k < memoryDetailsArrDay.length; k++) {
            dataMemDay = sliceFirstData(dataMemDay, dataView.Days);
            dataMemDay.push({
              x: new Date(memoryDetailsArrDay[k].timestamp),
              y: memoryDetailsArrDay[k].physicalMemory,
            });
          }
        }
      }

      var memTimeStamp = new Date(memDetails[currentServer].TIMESTAMP);

      if (memTimeStamp >= monitor.memMaxTimeStamp) {
        var memRss =
          parseFloat((memDetails[currentServer].RSS * 1.0) / 1048576.0).toFixed(
            3
          ) * 1;

        if (
          memDetails[currentServer].PHYSICALMEMORY != -1 &&
          physicalMemory != memDetails[currentServer].PHYSICALMEMORY
        ) {
          physicalMemory =
            parseFloat(
              (memDetails[currentServer].PHYSICALMEMORY * 1.0) / 1048576.0
            ).toFixed(3) * 1;

          ChartRam.yAxis.scale().domain([0, physicalMemory]);
          ChartRam.lines.forceY([0, physicalMemory]);
        }

        if (memRss < 0) memRss = 0;
        else if (physicalMemory != -1 && memRss > physicalMemory)
          memRss = physicalMemory;

        if (memSecCount >= 6 || monitor.memFirstData) {
          dataMemMin = sliceFirstData(dataMemMin, dataView.Minutes);
          if (memTimeStamp == monitor.memMaxTimeStamp) {
            dataMemMin.push({
              x: new Date(memTimeStamp),
              y: dataMemMin[dataMemMin.length - 1].y,
            });
            memoryDetailsArrMin = saveLocalStorageInterval(
              memoryDetailsArrMin,
              {
                timestamp: new Date(memTimeStamp),
                physicalMemory: dataMemMin[dataMemMin.length - 1].y,
              }
            );
          } else {
            dataMemMin.push({ x: new Date(memTimeStamp), y: memRss });
            memoryDetailsArrMin = saveLocalStorageInterval(
              memoryDetailsArrMin,
              { timestamp: new Date(memTimeStamp), physicalMemory: memRss }
            );
          }
          Monitors.memDataMin = dataMemMin;
          memSecCount = 0;
        }

        if (memMinCount >= 60 || monitor.memFirstData) {
          dataMemDay = sliceFirstData(dataMemDay, dataView.Days);
          if (memTimeStamp == monitor.memMaxTimeStamp) {
            dataMemDay.push({
              x: new Date(memTimeStamp),
              y: dataMemDay[dataMemDay.length - 1].y,
            });
            memoryDetailsArrDay = saveLocalStorageInterval(
              memoryDetailsArrDay,
              {
                timestamp: new Date(memTimeStamp),
                physicalMemory: dataMemDay[dataMemDay.length - 1].y,
              }
            );
          } else {
            dataMemDay.push({ x: new Date(memTimeStamp), y: memRss });
            memoryDetailsArrDay = saveLocalStorageInterval(
              memoryDetailsArrDay,
              { timestamp: new Date(memTimeStamp), physicalMemory: memRss }
            );
          }
          Monitors.memDataDay = dataMemDay;
          memMinCount = 0;
        }

        dataMem = sliceFirstData(dataMem, dataView.Seconds);
        if (memTimeStamp == monitor.memMaxTimeStamp) {
          dataMem.push({
            x: new Date(memTimeStamp),
            y: dataMem[dataMem.length - 1].y,
          });
          memoryDetailsArr = saveLocalStorageInterval(memoryDetailsArr, {
            timestamp: new Date(memTimeStamp),
            physicalMemory: dataMem[dataMem.length - 1].y,
          });
        } else {
          dataMem.push({ x: new Date(memTimeStamp), y: memRss });
          memoryDetailsArr = saveLocalStorageInterval(memoryDetailsArr, {
            timestamp: new Date(memTimeStamp),
            physicalMemory: memRss,
          });
        }
        Monitors.memData = dataMem;

        localStorage.memoryDetails = JSON.stringify(memoryDetailsArr);
        localStorage.memoryDetailsMin = JSON.stringify(memoryDetailsArrMin);
        localStorage.memoryDetailsDay = JSON.stringify(memoryDetailsArrDay);

        if (graphView == "Minutes") dataRam[0]["values"] = dataMemMin;
        else if (graphView == "Days") dataRam[0]["values"] = dataMemDay;
        else dataRam[0]["values"] = dataMem;

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          ramChart.is(":visible")
        ) {
          d3.select("#visualisationRam")
            .datum(dataRam)
            .transition()
            .duration(500)
            .call(ChartRam);
        }
        monitor.memFirstData = false;
      }
      if (memTimeStamp > monitor.memMaxTimeStamp)
        monitor.memMaxTimeStamp = memTimeStamp;
      memSecCount++;
      memMinCount++;
    };

    this.RefreshTransaction = function (
      transactionDetails,
      graphView,
      currentTab,
      currentServer
    ) {
      var monitor = Monitors;
      var datatrans = monitor.tpsData;
      var datatransMin = monitor.tpsDataMin;
      var datatransDay = monitor.tpsDataDay;
      var timeStamp;
      var transDetailsArr = [];
      var transDetailsArrMin = [];
      var transDetailsArrDay = [];

      if (
        $.isEmptyObject(transactionDetails) ||
        transactionDetails == undefined ||
        !transactionDetails.hasOwnProperty("CLUSTER_DETAILS") ||
        transactionDetails["CLUSTER_DETAILS"].TPS == undefined ||
        transactionDetails["CLUSTER_DETAILS"].TIMESTAMP == undefined
      )
        return;

      if (localStorage.transDetailsMin != undefined) {
        transDetailsArrMin = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.transDetailsMin)
        );
      } else {
        transDetailsArrMin = JSON.stringify(
          convertDataFormat(datatransMin, "timestamp", "transaction")
        );
        transDetailsArrMin = JSON.parse(transDetailsArrMin);
      }

      if (localStorage.transDetails != undefined) {
        transDetailsArr = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.transDetails)
        );
      } else {
        transDetailsArr = JSON.stringify(
          convertDataFormat(datatrans, "timestamp", "transaction")
        );
        transDetailsArr = JSON.parse(transDetailsArr);
      }

      if (localStorage.transDetailsDay != undefined) {
        transDetailsArrDay = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.transDetailsDay)
        );
      } else {
        transDetailsArrDay = JSON.stringify(
          convertDataFormat(datatransDay, "timestamp", "transaction")
        );
        transDetailsArrDay = JSON.parse(transDetailsArrDay);
      }

      if (monitor.tpsFirstData) {
        if (
          transDetailsArr.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                transDetailsArr[transDetailsArr.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          datatrans = [];
          for (var i = 0; i < transDetailsArr.length; i++) {
            datatrans = sliceFirstData(datatrans, dataView.Seconds);
            datatrans.push({
              x: new Date(transDetailsArr[i].timestamp),
              y: transDetailsArr[i].transaction,
            });
          }
        }
        if (
          transDetailsArrMin.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                transDetailsArrMin[transDetailsArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          datatransMin = [];
          for (var j = 0; j < transDetailsArrMin.length; j++) {
            datatransMin = sliceFirstData(datatransMin, dataView.Minutes);
            datatransMin.push({
              x: new Date(transDetailsArrMin[j].timestamp),
              y: transDetailsArrMin[j].transaction,
            });
          }
        }

        if (
          transDetailsArrDay.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                transDetailsArrMin[transDetailsArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          datatransDay = [];
          for (var k = 0; k < transDetailsArrDay.length; k++) {
            datatransDay = sliceFirstData(datatransDay, dataView.Days);
            datatransDay.push({
              x: new Date(transDetailsArrDay[k].timestamp),
              y: transDetailsArrDay[k].transaction,
            });
          }
        }
      }

      var timeStamp = new Date(transactionDetails["CLUSTER_DETAILS"].TIMESTAMP);
      var tps =
        parseFloat(transactionDetails["CLUSTER_DETAILS"].TPS).toFixed(1) * 1;

      if (monitor.tpsMaxTimeStamp <= timeStamp) {
        if (tpsSecCount >= 6 || monitor.tpsFirstData) {
          datatransMin = sliceFirstData(datatransMin, dataView.Minutes);
          if (monitor.tpsMaxTimeStamp == timeStamp) {
            datatransMin.push({
              x: new Date(timeStamp),
              y: datatransMin[datatransMin.length - 1].y,
            });
            transDetailsArrMin = saveLocalStorageInterval(transDetailsArrMin, {
              timestamp: new Date(timeStamp),
              transaction: datatransMin[datatransMin.length - 1].y,
            });
          } else {
            datatransMin.push({ x: new Date(timeStamp), y: tps });
            transDetailsArrMin = saveLocalStorageInterval(transDetailsArrMin, {
              timestamp: new Date(timeStamp),
              transaction: tps,
            });
          }
          Monitors.tpsDataMin = datatransMin;
          tpsSecCount = 0;
        }

        if (tpsMinCount >= 60 || monitor.tpsFirstData) {
          datatransDay = sliceFirstData(datatransDay, dataView.Days);
          if (monitor.tpsMaxTimeStamp == timeStamp) {
            datatransDay.push({
              x: new Date(timeStamp),
              y: datatransDay[datatransDay.length - 1].y,
            });
            transDetailsArrDay = saveLocalStorageInterval(transDetailsArrDay, {
              timestamp: new Date(timeStamp),
              transaction: datatransMin[datatransMin.length - 1].y,
            });
          } else {
            datatransDay.push({ x: new Date(timeStamp), y: tps });
            transDetailsArrDay = saveLocalStorageInterval(transDetailsArrDay, {
              timestamp: new Date(timeStamp),
              transaction: tps,
            });
          }
          Monitors.tpsDataDay = datatransDay;
          tpsMinCount = 0;
        }

        datatrans = sliceFirstData(datatrans, dataView.Seconds);
        if (monitor.tpsMaxTimeStamp == timeStamp) {
          datatrans.push({
            x: new Date(timeStamp),
            y: datatrans[datatrans.length - 1].y,
          });
          transDetailsArr = saveLocalStorageInterval(transDetailsArr, {
            timestamp: new Date(timeStamp),
            transaction: datatrans[datatrans.length - 1].y,
          });
        } else {
          datatrans.push({ x: new Date(timeStamp), y: tps });
          transDetailsArr = saveLocalStorageInterval(transDetailsArr, {
            timestamp: new Date(timeStamp),
            transaction: tps,
          });
        }
        Monitors.tpsData = datatrans;

        localStorage.transDetails = JSON.stringify(transDetailsArr);
        localStorage.transDetailsMin = JSON.stringify(transDetailsArrMin);
        localStorage.transDetailsDay = JSON.stringify(transDetailsArrDay);

        if (graphView == "Minutes")
          dataTransactions[0]["values"] = datatransMin;
        else if (graphView == "Days")
          dataTransactions[0]["values"] = datatransDay;
        else dataTransactions[0]["values"] = datatrans;

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          transactionChart.is(":visible")
        ) {
          d3.select("#visualisationTransaction")
            .datum(dataTransactions)
            .transition()
            .duration(500)
            .call(ChartTransactions);
        }
        monitor.tpsFirstData = false;
      }
      if (timeStamp > monitor.tpsMaxTimeStamp)
        monitor.tpsMaxTimeStamp = timeStamp;
      tpsSecCount++;
      tpsMinCount++;
      transactionDetails = null;
    };

    this.timeUnit = {
      sec: 5,
      min: 30,
      day: 300,
    };

    this.RefreshCpu = function (
      cpuDetails,
      currentServer,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;
      var cpuDetailsArr = [];
      var cpuDetailsArrMin = [];
      var cpuDetailsArrDay = [];

      var cpuData = monitor.cpuData;
      var cpuDataMin = monitor.cpuDataMin;
      var cpuDataDay = monitor.cpuDataHrs;
      var cpuDetail = cpuDetails;

      if (
        $.isEmptyObject(cpuDetail) ||
        cpuDetail == undefined ||
        !cpuDetail.hasOwnProperty(currentServer) ||
        cpuDetail[currentServer].PERCENT_USED == undefined ||
        cpuDetail[currentServer].TIMESTAMP == undefined
      )
        return;

      if (localStorage.cpuDetailsMin != undefined)
        cpuDetailsArrMin = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cpuDetailsMin)
        );
      else {
        cpuDetailsArrMin = JSON.stringify(
          convertDataFormat(cpuDataMin, "timestamp", "percentUsed")
        );
        cpuDetailsArrMin = JSON.parse(cpuDetailsArrMin);
      }

      if (localStorage.cpuDetailsDay != undefined)
        cpuDetailsArrDay = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cpuDetailsDay)
        );
      else {
        cpuDetailsArrDay = JSON.stringify(
          convertDataFormat(cpuDataDay, "timestamp", "percentUsed")
        );
        cpuDetailsArrDay = JSON.parse(cpuDetailsArrDay);
      }

      if (localStorage.cpuDetails != undefined) {
        cpuDetailsArr = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cpuDetails)
        );
      } else {
        cpuDetailsArr = JSON.stringify(
          convertDataFormat(cpuData, "timestamp", "percentUsed")
        );
        cpuDetailsArr = JSON.parse(cpuDetailsArr);
      }

      if (monitor.cpuFirstData) {
        if (
          cpuDetailsArr.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                cpuDetailsArr[cpuDetailsArr.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          cpuData = [];
          for (var i = 0; i < cpuDetailsArr.length; i++) {
            cpuData = sliceFirstData(cpuData, dataView.Seconds);
            cpuData.push({
              x: new Date(cpuDetailsArr[i].timestamp),
              y: cpuDetailsArr[i].percentUsed,
            });
          }
        }

        if (
          cpuDetailsArrMin.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                cpuDetailsArrMin[cpuDetailsArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          cpuDataMin = [];
          for (var j = 0; j < cpuDetailsArrMin.length; j++) {
            cpuDataMin = sliceFirstData(cpuDataMin, dataView.Minutes);
            cpuDataMin.push({
              x: new Date(cpuDetailsArrMin[j].timestamp),
              y: cpuDetailsArrMin[j].percentUsed,
            });
          }
        }

        if (
          cpuDetailsArrDay.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                cpuDetailsArrDay[cpuDetailsArrDay.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          cpuDataDay = [];
          for (var k = 0; k < cpuDetailsArrDay.length; k++) {
            cpuDataDay = sliceFirstData(cpuDataDay, dataView.Days);
            cpuDataDay.push({
              x: new Date(cpuDetailsArrDay[k].timestamp),
              y: cpuDetailsArrDay[k].percentUsed,
            });
          }
        }
      }

      var percentageUsage =
        parseFloat(cpuDetail[currentServer].PERCENT_USED).toFixed(1) * 1;
      var timeStamp = cpuDetail[currentServer].TIMESTAMP;

      if (timeStamp >= monitor.cpuMaxTimeStamp) {
        if (percentageUsage < 0) percentageUsage = 0;
        else if (percentageUsage > 100) percentageUsage = 100;

        if (cpuSecCount >= 6 || monitor.cpuFirstData) {
          cpuDataMin = sliceFirstData(cpuDataMin, dataView.Minutes);
          if (timeStamp == monitor.cpuMaxTimeStamp) {
            cpuDataMin.push({
              x: new Date(timeStamp),
              y: cpuDataMin[cpuDataMin.length - 1].y,
            });
            cpuDetailsArrMin = saveLocalStorageInterval(
              cpuDetailsArrMin,
              {
                timestamp: new Date(timeStamp),
                percentUsed: cpuDataMin[cpuDataMin.length - 1].y,
              },
              MonitorGraphUI.timeUnit.min
            );
          } else {
            cpuDataMin.push({ x: new Date(timeStamp), y: percentageUsage });
            cpuDetailsArrMin = saveLocalStorageInterval(
              cpuDetailsArrMin,
              { timestamp: new Date(timeStamp), percentUsed: percentageUsage },
              MonitorGraphUI.timeUnit.min
            );
          }
          Monitors.cpuDataMin = cpuDataMin;
          cpuSecCount = 0;
        }
        if (cpuMinCount >= 60 || monitor.cpuFirstData) {
          cpuDataDay = sliceFirstData(cpuDataDay, dataView.Days);
          if (timeStamp == monitor.cpuMaxTimeStamp) {
            cpuDataDay.push({
              x: new Date(timeStamp),
              y: cpuDataDay[cpuDataDay.length - 1].y,
            });
            cpuDetailsArrDay = saveLocalStorageInterval(
              cpuDetailsArrDay,
              {
                timestamp: new Date(timeStamp),
                percentUsed: cpuDataDay[cpuDataDay.length - 1].y,
              },
              MonitorGraphUI.timeUnit.day
            );
          } else {
            cpuDataDay.push({ x: new Date(timeStamp), y: percentageUsage });
            cpuDetailsArrDay = saveLocalStorageInterval(
              cpuDetailsArrDay,
              { timestamp: new Date(timeStamp), percentUsed: percentageUsage },
              MonitorGraphUI.timeUnit.day
            );
          }
          Monitors.cpuDataHrs = cpuDataDay;
          cpuMinCount = 0;
        }
        cpuData = sliceFirstData(cpuData, dataView.Seconds);
        if (timeStamp == monitor.cpuMaxTimeStamp) {
          cpuData.push({
            x: new Date(timeStamp),
            y: cpuData[cpuData.length - 1].y,
          });
          cpuDetailsArr = saveLocalStorageInterval(
            cpuDetailsArr,
            {
              timestamp: new Date(timeStamp),
              percentUsed: cpuData[cpuData.length - 1].y,
            },
            MonitorGraphUI.timeUnit.sec
          );
        } else {
          cpuData.push({ x: new Date(timeStamp), y: percentageUsage });
          cpuDetailsArr = saveLocalStorageInterval(
            cpuDetailsArr,
            { timestamp: new Date(timeStamp), percentUsed: percentageUsage },
            MonitorGraphUI.timeUnit.sec
          );
        }
        try {
          $(".errorMsgLocalStorageFull").hide();
          localStorage.cpuDetails = JSON.stringify(cpuDetailsArr);
          localStorage.cpuDetailsMin = JSON.stringify(cpuDetailsArrMin);
          localStorage.cpuDetailsDay = JSON.stringify(cpuDetailsArrDay);
        } catch (e) {
          $(".errorMsgLocalStorageFull").show();
        }
        Monitors.cpuData = cpuData;
        monitor.cpuFirstData = false;

        if (graphView == "Minutes") dataCpu[0]["values"] = cpuDataMin;
        else if (graphView == "Days") dataCpu[0]["values"] = cpuDataDay;
        else {
          dataCpu[0]["values"] = cpuData;
        }

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          cpuChart.is(":visible")
        ) {
          d3.select("#visualisationCpu")
            .datum(dataCpu)
            .transition()
            .duration(500)
            .call(ChartCpu);
        }
      }
      if (timeStamp > monitor.cpuMaxTimeStamp)
        monitor.cpuMaxTimeStamp = timeStamp;
      cpuSecCount++;
      cpuMinCount++;
    };

    var saveLocalStorageInterval = function (rawDataArr, newItem) {
      var interval_end = new Date();
      var interval_start = new Date();
      interval_end.setMinutes(
        interval_end.getMinutes() - RETAINED_TIME_INTERVAL
      );
      var dataArr = [];
      for (var i = 0; i < rawDataArr.length; i++) {
        var timeStamp = new Date(rawDataArr[i].timestamp);
        if (
          timeStamp.getTime() >= interval_end.getTime() &&
          timeStamp.getTime() <= interval_start.getTime()
        ) {
          dataArr.push(rawDataArr[i]);
        }
      }
      dataArr.push(newItem);
      return dataArr;
    };

    var getFormattedDataFromLocalStorage = function (rawDataArr) {
      var interval_end = new Date();
      var interval_start = new Date();
      interval_end.setMinutes(
        interval_end.getMinutes() - RETAINED_TIME_INTERVAL
      );
      var dataArr = [];
      for (var i = 0; i < rawDataArr.length; i++) {
        var timeStamp = new Date(rawDataArr[i].timestamp);
        if (
          timeStamp.getTime() >= interval_end.getTime() &&
          timeStamp.getTime() <= interval_start.getTime()
        ) {
          dataArr.push(rawDataArr[i]);
        }
      }
      return dataArr;
    };

    var getFormattedPartitionDataFromLocalStorage = function (rawDataArr) {
      var interval_end = new Date();
      var interval_start = new Date();
      interval_end.setMinutes(
        interval_end.getMinutes() - RETAINED_TIME_INTERVAL
      );
      var partitionData = [];
      for (var i = 0; i < rawDataArr.length; i++) {
        var keyIndex = i;
        partitionData[keyIndex] = {};
        partitionData[keyIndex]["values"] = [];
        partitionData[keyIndex]["key"] = rawDataArr[keyIndex]["key"];
        partitionData[keyIndex]["color"] = rawDataArr[keyIndex]["color"];
        for (var b = 0; b < rawDataArr[i]["values"].length; b++) {
          var timeStamp = new Date(rawDataArr[i]["values"][b].x);
          if (
            timeStamp.getTime() >= interval_end.getTime() &&
            timeStamp.getTime() <= interval_start.getTime()
          ) {
            partitionData[keyIndex]["values"].push(rawDataArr[i]["values"][b]);
          }
        }
      }
      return partitionData;
    };

    var savePartitionDataToLocalStorage = function (data, newItem, keyIndex) {
      var interval_end = new Date();
      var interval_start = new Date();
      interval_end.setMinutes(
        interval_end.getMinutes() - RETAINED_TIME_INTERVAL
      );
      var values = data[keyIndex].values;
      var dataArr = [];
      for (var i = 0; i < values.length; i++) {
        var timeStamp = new Date(values[i].x);
        if (
          timeStamp.getTime() >= interval_end.getTime() &&
          timeStamp.getTime() <= interval_start.getTime()
        ) {
          dataArr.push(values[i]);
        }
      }
      dataArr.push(newItem);
      data[keyIndex].values = dataArr;
      return data;
    };

    function getPartitionData() {
      var monitor = Monitors;
      monitor.partitionData = getEmptyDataForPartition();
      monitor.partitionDataMin = getEmptyDataForPartitionForMinutes();
      monitor.partitionDataDay = getEmptyDataForPartitionForDay();
    }

    this.RefreshPartitionIdleTime = function (
      partitionDetails,
      currentServer,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;

      if (
        monitor.partitionData.length < 1 ||
        monitor.partitionDataMin.length < 1 ||
        monitor.partitionDataDay.length < 1
      ) {
        getPartitionData();
      }

      if (dataMapperSec == undefined || $.isEmptyObject(dataMapperSec)) return;

      if (dataMapperDay == undefined || $.isEmptyObject(dataMapperDay)) return;

      if (dataMapperMin == undefined || $.isEmptyObject(dataMapperMin)) return;

      var partitionData = monitor.partitionData;
      var partitionDataMin = monitor.partitionDataMin;
      var partitionDataDay = monitor.partitionDataDay;
      var partitionDetail = partitionDetails;
      var partitionDetailsArr = [];
      var partitionDetailsArrMin = [];
      var partitionDetailsArrDay = [];

      if (localStorage.partitionDetailsMin != undefined) {
        partitionDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.partitionDetailsMin)
        );
      } else {
        partitionDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(partitionDataMin)
        );
        partitionDetailsArrMin = JSON.parse(partitionDetailsArrMin);
      }

      if (localStorage.partitionDetailsDay != undefined) {
        partitionDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.partitionDetailsDay)
        );
      } else {
        partitionDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(partitionDataDay)
        );
        partitionDetailsArrDay = JSON.parse(partitionDetailsArrDay);
      }
      if (localStorage.partitionDetails != undefined) {
        partitionDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.partitionDetails)
        );
      } else {
        partitionDetailsArr = JSON.stringify(
          convertDataFormatForPartition(partitionData)
        );
        partitionDetailsArr = JSON.parse(partitionDetailsArr);
      }

      if (monitor.partitionFirstData) {
        for (var i = 0; i < partitionDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            partitionDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  partitionDetailsArr[i]["values"][
                    partitionDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            partitionData[keyIndexSec]["values"] = [];
            for (var b = 0; b < partitionDetailsArr[i]["values"].length; b++) {
              partitionData[keyIndexSec]["values"] = sliceFirstData(
                partitionData[keyIndexSec]["values"],
                dataView.Seconds
              );
              partitionData[keyIndexSec]["values"].push({
                x: new Date(partitionDetailsArr[i]["values"][b].x),
                y: partitionDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var j = 0; j < partitionDetailsArrMin.length; j++) {
          var keyIndexMin = j;
          if (
            partitionDetailsArrMin[j]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  partitionDetailsArrMin[j]["values"][
                    partitionDetailsArrMin[j]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            partitionDataMin[keyIndexMin]["values"] = [];
            for (
              var a = 0;
              a < partitionDetailsArrMin[j]["values"].length;
              a++
            ) {
              partitionDataMin[keyIndexMin]["values"] = sliceFirstData(
                partitionDataMin[keyIndexMin]["values"],
                dataView.Minutes
              );
              partitionDataMin[keyIndexMin]["values"].push({
                x: new Date(partitionDetailsArrMin[j]["values"][a].x),
                y: partitionDetailsArrMin[j]["values"][a].y,
              });
            }
          }
        }

        for (var k = 0; k < partitionDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            partitionDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  partitionDetailsArrDay[k]["values"][
                    partitionDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            partitionDataDay[keyIndexMin]["values"] = [];
            for (
              var c = 0;
              c < partitionDetailsArrDay[k]["values"].length;
              c++
            ) {
              partitionDataDay[keyIndexDay]["values"] = sliceFirstData(
                partitionDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              partitionDataDay[keyIndexDay]["values"].push({
                x: new Date(partitionDetailsArrDay[k]["values"][c].x),
                y: partitionDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(partitionDetail) ||
        partitionDetail == undefined ||
        partitionDetail["partitionDetail"]["timeStamp"] == undefined
      )
        return;

      var timeStamp = partitionDetails["partitionDetail"]["timeStamp"];
      if (timeStamp >= monitor.partitionMaxTimeStamp) {
        $.each(
          partitionDetail["partitionDetail"],
          function (datatype, datavalue) {
            if (typeof datavalue === "object") {
              $.each(datavalue, function (partitionKey, partitionValue) {
                var keyValue = partitionKey;
                var percentValue = partitionValue;

                if (percentValue < 0) percentValue = 0;
                else if (percentValue > 100) percentValue = 100;
                if (partitionSecCount >= 6 || monitor.partitionFirstData) {
                  if (!partitionDataMin.hasOwnProperty(keyValue)) {
                    var keyIndex = dataMapperMin[keyValue];
                    partitionDataMin[keyIndex]["values"] = sliceFirstData(
                      partitionDataMin[keyIndex]["values"],
                      dataView.Minutes
                    );
                    if (timeStamp == monitor.partitionMaxTimeStamp) {
                      partitionDataMin[keyIndex]["values"].push({
                        x: new Date(timeStamp),
                        y: partitionDataMin[keyIndex]["values"][
                          partitionDataMin[keyIndex]["values"].length - 1
                        ].y,
                      });
                      partitionDetailsArrMin = savePartitionDataToLocalStorage(
                        partitionDetailsArrMin,
                        {
                          x: new Date(timeStamp),
                          y: partitionDataMin[keyIndex]["values"][
                            partitionDataMin[keyIndex]["values"].length - 1
                          ].y,
                        },
                        keyIndex
                      );
                    } else {
                      partitionDataMin[keyIndex]["values"].push({
                        x: new Date(timeStamp),
                        y: percentValue,
                      });
                      partitionDetailsArrMin = savePartitionDataToLocalStorage(
                        partitionDetailsArrMin,
                        { x: new Date(timeStamp), y: percentValue },
                        keyIndex
                      );
                    }
                    Monitors.partitionDataMin = partitionDataMin;
                  }
                }

                if (partitionMinCount >= 60 || monitor.partitionFirstData) {
                  var keyIndexDay = dataMapperDay[keyValue];
                  partitionDataDay[keyIndexDay]["values"] = sliceFirstData(
                    partitionDataDay[keyIndexDay]["values"],
                    dataView.Days
                  );
                  if (timeStamp == monitor.partitionMaxTimeStamp) {
                    partitionDataDay[keyIndexDay]["values"].push({
                      x: new Date(timeStamp),
                      y: partitionDataDay[keyIndexDay]["values"][
                        partitionDataDay[keyIndexDay]["values"].length - 1
                      ].y,
                    });
                    partitionDetailsArrDay = savePartitionDataToLocalStorage(
                      partitionDetailsArrDay,
                      {
                        x: new Date(timeStamp),
                        y: partitionDataDay[keyIndexDay]["values"][
                          partitionDataDay[keyIndexDay]["values"].length - 1
                        ].y,
                      },
                      keyIndexDay
                    );
                  } else {
                    partitionDataDay[keyIndexDay]["values"].push({
                      x: new Date(timeStamp),
                      y: percentValue,
                    });
                    partitionDetailsArrDay = savePartitionDataToLocalStorage(
                      partitionDetailsArrDay,
                      { x: new Date(timeStamp), y: percentValue },
                      keyIndexDay
                    );
                  }
                  Monitors.partitionDataDay = partitionDataDay;
                }

                var keyIndexSec = dataMapperSec[keyValue];

                partitionData[keyIndexSec]["values"] = sliceFirstData(
                  partitionData[keyIndexSec]["values"],
                  dataView.Seconds
                );
                if (timeStamp == monitor.partitionMaxTimeStamp) {
                  partitionData[keyIndexSec]["values"].push({
                    x: new Date(timeStamp),
                    y: partitionData[keyIndexSec]["values"][
                      partitionData[keyIndexSec]["values"].length - 1
                    ].y,
                  });
                  partitionDetailsArr = savePartitionDataToLocalStorage(
                    partitionDetailsArr,
                    {
                      x: new Date(timeStamp),
                      y: partitionData[keyIndexSec]["values"][
                        partitionData[keyIndexSec]["values"].length - 1
                      ].y,
                    },
                    keyIndexSec
                  );
                } else {
                  partitionData[keyIndexSec].values.push({
                    x: new Date(timeStamp),
                    y: percentValue,
                  });
                  partitionDetailsArr = savePartitionDataToLocalStorage(
                    partitionDetailsArr,
                    { x: new Date(timeStamp), y: percentValue },
                    keyIndexSec
                  );
                }
                Monitors.partitionData = partitionData;
              });
            }
          }
        );

        localStorage.partitionDetails = JSON.stringify(partitionDetailsArr);
        localStorage.partitionDetailsMin = JSON.stringify(
          partitionDetailsArrMin
        );
        localStorage.partitionDetailsDay = JSON.stringify(
          partitionDetailsArrDay
        );
        if (monitor.partitionFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.partitionFirstData = false;
        if (partitionSecCount >= 6) partitionSecCount = 0;
        if (partitionMinCount >= 60) partitionMinCount = 0;

        if (graphView == "Minutes") dataPartitionIdleTime = partitionDataMin;
        else if (graphView == "Days") dataPartitionIdleTime = partitionDataDay;
        else {
          dataPartitionIdleTime = partitionData;
        }

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          partitionChart.is(":visible")
        ) {
          d3.select("#visualisationPartitionIdleTime")
            .datum(dataPartitionIdleTime)
            .transition()
            .duration(500)
            .call(ChartPartitionIdleTime);
        }
      }
      if (timeStamp > monitor.partitionMaxTimeStamp)
        monitor.partitionMaxTimeStamp = timeStamp;

      partitionSecCount++;
      partitionMinCount++;
    };

    this.RefreshDrReplicationGraph = function (
      drDetails,
      currentServer,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;
      var chartList = VoltDbUI.drChartList;
      if (chartList != undefined && chartList.length > 0) {
        for (var i = 0; i < chartList.length; i++) {
          var drData = monitor["drReplicationData_" + chartList[i]];
          var drDataMin = monitor["drReplicationDataMin_" + chartList[i]];
          var drDataDay = monitor["drReplicationDataDay_" + chartList[i]];
          var drDetail = drDetails;
          var drDetailsArr = [];
          var drDetailsArrMin = [];
          var drDetailsArrDay = [];

          if (
            $.isEmptyObject(drDetail) ||
            drDetail == undefined ||
            drDetail["DR_GRAPH"][chartList[i]].REPLICATION_RATE_1M ==
              undefined ||
            drDetail["DR_GRAPH"][chartList[i]].TIMESTAMP == undefined
          )
            return true;

          if (localStorage["drDetailsMin_" + chartList[i]] != undefined) {
            drDetailsArrMin = getFormattedDataFromLocalStorage(
              JSON.parse(localStorage["drDetailsMin_" + chartList[i]])
            );
          } else {
            drDetailsArrMin = JSON.stringify(
              convertDataFormat(drDataMin, "timestamp", "replicationRate")
            );
            drDetailsArrMin = JSON.parse(drDetailsArrMin);
          }

          if (localStorage["drDetailsDay_" + chartList[i]] != undefined) {
            drDetailsArrDay = getFormattedDataFromLocalStorage(
              JSON.parse(localStorage["drDetailsDay_" + chartList[i]])
            );
          } else {
            drDetailsArrDay = JSON.stringify(
              convertDataFormat(drDataDay, "timestamp", "replicationRate")
            );
            drDetailsArrDay = JSON.parse(drDetailsArrDay);
          }

          if (localStorage["drDetails_" + chartList[i]] != undefined) {
            drDetailsArr = getFormattedDataFromLocalStorage(
              JSON.parse(localStorage["drDetails_" + chartList[i]])
            );
          } else {
            drDetailsArr = JSON.stringify(
              convertDataFormat(drData, "timestamp", "replicationRate")
            );
            drDetailsArr = JSON.parse(drDetailsArr);
          }

          if (monitor["drFirstData_" + chartList[i]]) {
            if (
              drDetailsArr.length > 0 &&
              !(
                currentTime.getTime() -
                  new Date(
                    drDetailsArr[drDetailsArr.length - 1].timestamp
                  ).getTime() >
                MonitorGraphUI.enumMaxTimeGap.secGraph
              )
            ) {
              drData = [];
              for (var l = 0; l < drDetailsArr.length; l++) {
                drData = sliceFirstData(drData, dataView.Seconds);
                drData.push({
                  x: new Date(drDetailsArr[l].timestamp),
                  y: drDetailsArr[l].replicationRate,
                });
              }
            }

            if (
              drDetailsArrMin.length > 0 &&
              !(
                currentTime.getTime() -
                  new Date(
                    drDetailsArrMin[drDetailsArrMin.length - 1].timestamp
                  ).getTime() >
                MonitorGraphUI.enumMaxTimeGap.minGraph
              )
            ) {
              drDataMin = [];
              for (var j = 0; j < drDetailsArrMin.length; j++) {
                drDataMin = sliceFirstData(drDataMin, dataView.Minutes);
                drDataMin.push({
                  x: new Date(drDetailsArrMin[j].timestamp),
                  y: drDetailsArrMin[j].replicationRate,
                });
              }
            }

            if (
              drDetailsArrDay.length > 0 &&
              !(
                currentTime.getTime() -
                  new Date(
                    drDetailsArrDay[drDetailsArrDay.length - 1].timestamp
                  ).getTime() >
                MonitorGraphUI.enumMaxTimeGap.dayGraph
              )
            ) {
              drDataDay = [];
              for (var k = 0; k < drDetailsArrDay.length; k++) {
                drDataDay = sliceFirstData(drDataDay, dataView.Days);
                drDataDay.push({
                  x: new Date(drDetailsArrDay[k].timestamp),
                  y: drDetailsArrDay[k].replicationRate,
                });
              }
            }
          }

          var timeStamp = drDetail["DR_GRAPH"][chartList[i]].TIMESTAMP;

          if (timeStamp >= monitor["drMaxTimeStamp_" + chartList[i]]) {
            var plottingPoint =
              parseFloat(
                drDetail["DR_GRAPH"][chartList[i]].REPLICATION_RATE_1M
              ).toFixed(1) * 1;

            if (
              monitor["drSecCount_" + chartList[i]] >= 6 ||
              monitor["drFirstData_" + chartList[i]]
            ) {
              drDataMin = sliceFirstData(drDataMin, dataView.Minutes);
              if (timeStamp == monitor["drMaxTimeStamp_" + chartList[i]]) {
                drDataMin.push({
                  x: new Date(timeStamp),
                  y: drDataMin[drDataMin.length - 1].y,
                });
                drDetailsArrMin = saveLocalStorageInterval(drDetailsArrMin, {
                  timestamp: new Date(timeStamp),
                  replicationRate: drDataMin[drDataMin.length - 1].y,
                });
              } else {
                drDataMin.push({ x: new Date(timeStamp), y: plottingPoint });
                drDetailsArrMin = saveLocalStorageInterval(drDetailsArrMin, {
                  timestamp: new Date(timeStamp),
                  replicationRate: plottingPoint,
                });
              }
              monitor["drReplicationDataMin_" + chartList[i]] = drDataMin;
              monitor["drSecCount_" + chartList[i]] = 0;
            }
            if (
              monitor["drMinCount_" + chartList[i]] >= 60 ||
              monitor["drFirstData_" + chartList[i]]
            ) {
              drDataDay = sliceFirstData(drDataDay, dataView.Days);
              if (timeStamp == monitor["drMaxTimeStamp_" + chartList[i]]) {
                drDataDay.push({
                  x: new Date(timeStamp),
                  y: drDataDay[drDataDay.length - 1].y,
                });
                drDetailsArrDay = saveLocalStorageInterval(drDetailsArrDay, {
                  timestamp: new Date(timeStamp),
                  replicationRate: drDataDay[drDataDay.length - 1].y,
                });
              } else {
                drDataDay.push({ x: new Date(timeStamp), y: plottingPoint });
                drDetailsArrDay = saveLocalStorageInterval(drDetailsArrDay, {
                  timestamp: new Date(timeStamp),
                  replicationRate: plottingPoint,
                });
              }
              monitor["drReplicationDataDay_" + chartList[i]] = drDataDay;
              monitor["drMinCount_" + chartList[i]] = 0;
            }
            drData = sliceFirstData(drData, dataView.Seconds);
            if (timeStamp == monitor["drMaxTimeStamp_" + chartList[i]]) {
              drData.push({
                x: new Date(timeStamp),
                y: drData[drData.length - 1].y,
              });
              drDetailsArr = saveLocalStorageInterval(drDetailsArr, {
                timestamp: new Date(timeStamp),
                replicationRate: drData[drData.length - 1].y,
              });
            } else {
              drData.push({ x: new Date(timeStamp), y: plottingPoint });
              drDetailsArr = saveLocalStorageInterval(drDetailsArr, {
                timestamp: new Date(timeStamp),
                replicationRate: plottingPoint,
              });
            }

            localStorage["drDetails_" + chartList[i]] =
              JSON.stringify(drDetailsArr);
            localStorage["drDetailsMin_" + chartList[i]] =
              JSON.stringify(drDetailsArrMin);
            localStorage["drDetailsDay_" + chartList[i]] =
              JSON.stringify(drDetailsArrDay);

            monitor["drReplicationData_" + chartList[i]] = drData;
            monitor["drFirstData_" + chartList[i]] = false;

            if (graphView == "Minutes")
              dataDrReplication["dataDrReplication_" + chartList[i]][0][
                "values"
              ] = drDataMin;
            else if (graphView == "Days")
              dataDrReplication["dataDrReplication_" + chartList[i]][0][
                "values"
              ] = drDataDay;
            else {
              dataDrReplication["dataDrReplication_" + chartList[i]][0][
                "values"
              ] = drData;
            }
            if (currentTab == NavigationTabs.DR && currentViewDr == graphView) {
              d3.select("#visualizationDrReplicationRate_" + chartList[i])
                .datum(dataDrReplication["dataDrReplication_" + chartList[i]])
                .transition()
                .duration(500)
                .call(drChartList["ChartDrReplicationRate_" + chartList[i]]);
            }
          }

          if (timeStamp > monitor["drMaxTimeStamp_" + chartList[i]])
            monitor["drMaxTimeStamp_" + chartList[i]] = timeStamp;

          monitor["drSecCount_" + chartList[i]]++;
          monitor["drMinCount_" + chartList[i]]++;
        }
      }
    };

    this.RefreshCommandLog = function (
      cmdLogDetails,
      currentServer,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;
      var cmdLogData = monitor.cmdLogData;
      var cmdLogDataMin = monitor.cmdLogDataMin;
      var cmdLogDataDay = monitor.cmdLogDataDay;
      var cmdLogDetail = cmdLogDetails;
      var cmdLogArr = [];
      var cmdLogArrMin = [];
      var cmdLogArrDay = [];
      var overlayDataArr = [];
      var overlayDataArrMin = [];
      var overlayDataArrDay = [];

      if (localStorage.cmdLogMin != undefined)
        cmdLogArrMin = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cmdLogMin)
        );
      else {
        cmdLogArrMin = JSON.stringify(convertDataFormat(cmdLogDataMin));
        cmdLogArrMin = JSON.parse(cmdLogArrMin);
      }

      if (localStorage.cmdLogDay != undefined)
        cmdLogArrDay = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cmdLogDay)
        );
      else {
        cmdLogArrDay = JSON.stringify(convertDataFormat(cmdLogDataDay));
        cmdLogArrDay = JSON.parse(cmdLogArrDay);
      }

      if (localStorage.cmdLog != undefined)
        cmdLogArr = getFormattedDataFromLocalStorage(
          JSON.parse(localStorage.cmdLog)
        );
      else {
        cmdLogArr = JSON.stringify(convertDataFormat(cmdLogData));
        cmdLogArr = JSON.parse(cmdLogArr);
      }

      if (localStorage.SnapshotOverlayData != undefined)
        overlayDataArr = JSON.parse(localStorage.SnapshotOverlayData);

      if (localStorage.SnapshotOverlayDataMin != undefined)
        overlayDataArrMin = JSON.parse(localStorage.SnapshotOverlayDataMin);

      if (localStorage.SnapshotOverlayDataDay != undefined)
        overlayDataArrDay = JSON.parse(localStorage.SnapshotOverlayDataDay);

      if (monitor.cmdLogFirstData) {
        if (
          cmdLogArr.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(cmdLogArr[cmdLogArr.length - 1].timestamp).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          cmdLogData = [];
          for (var i = 0; i < cmdLogArr.length; i++) {
            cmdLogData = sliceFirstData(cmdLogData, dataView.Seconds);
            cmdLogData.push({
              x: new Date(cmdLogArr[i].timestamp),
              y: cmdLogArr[i].outstandingTxn,
            });
          }
        }

        if (
          cmdLogArrMin.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                cmdLogArrMin[cmdLogArrMin.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          cmdLogDataMin = [];
          for (var j = 0; j < cmdLogArrMin.length; j++) {
            cmdLogDataMin = sliceFirstData(cmdLogDataMin, dataView.Minutes);
            cmdLogDataMin.push({
              x: new Date(cmdLogArrMin[j].timestamp),
              y: cmdLogArrMin[j].outstandingTxn,
            });
          }
        }

        if (
          cmdLogArrDay.length > 0 &&
          !(
            currentTime.getTime() -
              new Date(
                cmdLogArrDay[cmdLogArrDay.length - 1].timestamp
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          cmdLogDataDay = [];
          for (var k = 0; k < cmdLogArrDay.length; k++) {
            cmdLogDataDay = sliceFirstData(cmdLogDataDay, dataView.Days);
            cmdLogDataDay.push({
              x: new Date(cmdLogArrDay[k].timestamp),
              y: cmdLogArrDay[k].outstandingTxn,
            });
          }
        }
        var overlayData = GetSnapshotOverlay(overlayDataArr);
        if (
          overlayData.length != 0 &&
          !(
            currentTime.getTime() -
              new Date(overlayData[overlayData.length - 1].endTime).getTime() >
            MonitorGraphUI.enumMaxTimeGap.secGraph
          )
        ) {
          cmdLogOverlay = [];
          cmdLogOverlay = overlayData;
        }

        var overlayDataMin = GetSnapshotOverlay(overlayDataArrMin);
        if (
          overlayDataMin.length != 0 &&
          !(
            currentTime.getTime() -
              new Date(
                overlayDataMin[overlayDataMin.length - 1].endTime
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.minGraph
          )
        ) {
          cmdLogOverlayMin = overlayDataMin;
          overlayDataMin = [];
        }

        var overlayDataDay = GetSnapshotOverlay(overlayDataArrDay);
        if (
          overlayDataDay.length != 0 &&
          !(
            currentTime.getTime() -
              new Date(
                overlayDataDay[overlayDataDay.length - 1].endTime
              ).getTime() >
            MonitorGraphUI.enumMaxTimeGap.dayGraph
          )
        ) {
          cmdLogOverlayDay = overlayDataDay;
          overlayDataDay = [];
        }
      }

      if (
        $.isEmptyObject(cmdLogDetail) ||
        cmdLogDetail == undefined ||
        cmdLogDetail[currentServer].OUTSTANDING_TXNS == undefined ||
        cmdLogDetail[currentServer].TIMESTAMP == undefined
      )
        return;

      var timeStamp = cmdLogDetail[currentServer].TIMESTAMP;
      if (timeStamp >= monitor.cmdLogMaxTimeStamp) {
        var outStandingTxn =
          parseFloat(cmdLogDetail[currentServer].OUTSTANDING_TXNS).toFixed(1) *
          1;

        if (cmdLogSecCount >= 6 || monitor.cmdLogFirstData) {
          cmdLogDataMin = sliceFirstData(cmdLogDataMin, dataView.Minutes);
          if (timeStamp == monitor.cmdLogMaxTimeStamp) {
            cmdLogDataMin.push({
              x: new Date(timeStamp),
              y: cmdLogDataMin[cmdLogDataMin.length - 1].y,
            });
            cmdLogArrMin = saveLocalStorageInterval(cmdLogArrMin, {
              timestamp: new Date(timeStamp),
              outstandingTxn: cmdLogDataMin[cmdLogDataMin.length - 1].y,
            });
          } else {
            cmdLogDataMin.push({ x: new Date(timeStamp), y: outStandingTxn });
            cmdLogArrMin = saveLocalStorageInterval(cmdLogArrMin, {
              timestamp: new Date(timeStamp),
              outstandingTxn: outStandingTxn,
            });
          }
          Monitors.cmdLogDataMin = cmdLogDataMin;

          var isDuplicate = false;
          if (!$.isEmptyObject(cmdLogDetail[currentServer].SNAPSHOTS)) {
            for (
              var i = 0;
              i < cmdLogDetail[currentServer].SNAPSHOTS.length;
              i++
            ) {
              isDuplicate = false;
              for (var j = 0; j < cmdLogOverlayMin.length; j++) {
                var x1 = cmdLogOverlayMin[j].startTime;
                if (x1 == cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME) {
                  isDuplicate = true;
                  break;
                }
              }
              if (!isDuplicate) {
                cmdLogOverlayMin.push({
                  startTime:
                    cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                  endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
                });
                overlayDataArrMin.push({
                  startTime:
                    cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                  endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
                });
              }
            }
            cmdLogOverlayMin = GetSnapshotOverlay(cmdLogOverlayMin, 90);
          }

          localStorage.SnapshotOverlayDataMin = JSON.stringify(
            GetSnapshotOverlay(overlayDataArrMin)
          );
          overlayDataArrMin = [];
          cmdLogSecCount = 0;
        }

        if (cmdLogMinCount >= 60 || monitor.cmdLogFirstData) {
          cmdLogDataDay = sliceFirstData(cmdLogDataDay, dataView.Days);
          if (timeStamp == monitor.cmdLogMaxTimeStamp) {
            cmdLogDataDay.push({
              x: new Date(timeStamp),
              y: cmdLogDataDay[cmdLogDataDay.length - 1].y,
            });
            cmdLogArrDay = saveLocalStorageInterval(cmdLogArrDay, {
              timestamp: new Date(timeStamp),
              outstandingTxn: cmdLogDataDay[cmdLogDataDay.length - 1].y,
            });
          } else {
            cmdLogDataDay.push({ x: new Date(timeStamp), y: outStandingTxn });
            cmdLogArrDay = saveLocalStorageInterval(cmdLogArrDay, {
              timestamp: new Date(timeStamp),
              outstandingTxn: outStandingTxn,
            });
          }
          Monitors.cmdLogDataDay = cmdLogDataDay;

          var isDuplicate = false;
          if (!$.isEmptyObject(cmdLogDetail[currentServer].SNAPSHOTS)) {
            for (
              var i = 0;
              i < cmdLogDetail[currentServer].SNAPSHOTS.length;
              i++
            ) {
              isDuplicate = false;
              for (var j = 0; j < cmdLogOverlayDay.length; j++) {
                var x1 = cmdLogOverlayDay[j].startTime;
                if (x1 == cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME) {
                  isDuplicate = true;
                  break;
                }
              }
              if (!isDuplicate) {
                cmdLogOverlayDay.push({
                  startTime:
                    cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                  endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
                });
                overlayDataArrDay.push({
                  startTime:
                    cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                  endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
                });
              }
            }
            cmdLogOverlayDay = GetSnapshotOverlay(cmdLogOverlayDay, 2400);
          }
          localStorage.SnapshotOverlayDataDay = JSON.stringify(
            GetSnapshotOverlay(overlayDataArrDay)
          );
          overlayDataArrDay = [];
          cmdLogMinCount = 0;
        }
        cmdLogData = sliceFirstData(cmdLogData, dataView.Seconds);
        if (timeStamp == monitor.cmdLogMaxTimeStamp) {
          cmdLogData.push({
            x: new Date(timeStamp),
            y: cmdLogData[cmdLogData.length - 1].y,
          });
          cmdLogArr = saveLocalStorageInterval(cmdLogArr, {
            timestamp: new Date(timeStamp),
            outstandingTxn: cmdLogData[cmdLogData.length - 1].y,
          });
        } else {
          cmdLogData.push({ x: new Date(timeStamp), y: outStandingTxn });
          cmdLogArr = saveLocalStorageInterval(cmdLogArr, {
            timestamp: new Date(timeStamp),
            outstandingTxn: outStandingTxn,
          });
        }
        Monitors.cmdLogData = cmdLogData;

        localStorage.cmdLog = JSON.stringify(cmdLogArr);
        localStorage.cmdLogMin = JSON.stringify(cmdLogArrMin);
        localStorage.cmdLogDay = JSON.stringify(cmdLogArrDay);

        var isDuplicate = false;
        if (!$.isEmptyObject(cmdLogDetail[currentServer].SNAPSHOTS)) {
          for (
            var i = 0;
            i < cmdLogDetail[currentServer].SNAPSHOTS.length;
            i++
          ) {
            isDuplicate = false;
            for (var j = 0; j < cmdLogOverlay.length; j++) {
              var x1 = cmdLogOverlay[j].startTime;
              if (x1 == cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME) {
                isDuplicate = true;
                break;
              }
            }
            if (!isDuplicate) {
              cmdLogOverlay.push({
                startTime: cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
              });
              overlayDataArr.push({
                startTime: cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME,
                endTime: cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME,
              });
            }
          }
          cmdLogOverlay = GetSnapshotOverlay(cmdLogOverlay, 15);
        }
        localStorage.SnapshotOverlayData = JSON.stringify(
          GetSnapshotOverlay(overlayDataArr)
        );

        if (monitor.cmdLogFirstData) {
          $(".cmdLogLegend").css("display", "block");
        }
        monitor.cmdLogFirstData = false;

        dataOverlay = [];
        if (graphView == "Minutes") {
          dataCommandLog[0]["values"] = cmdLogDataMin;
          dataOverlay = cmdLogOverlayMin;
        } else if (graphView == "Days") {
          dataCommandLog[0]["values"] = cmdLogDataDay;
          dataOverlay = cmdLogOverlayDay;
        } else {
          dataCommandLog[0]["values"] = cmdLogData;
          dataOverlay = cmdLogOverlay;
        }

        if (
          currentTab == NavigationTabs.DBMonitor &&
          currentView == graphView &&
          cmdLogChart.is(":visible")
        ) {
          d3.select("#visualisationCommandLog")
            .datum(dataCommandLog)
            .transition()
            .duration(500)
            .call(ChartCommandlog);
        }

        $(".overlayGraph").detach();

        for (var i = 0; i < dataOverlay.length; i++) {
          var x1 = ChartCommandlog.xScale()(dataOverlay[i].startTime);
          var x2 = ChartCommandlog.xScale()(dataOverlay[i].endTime);
          var opacity = 1;
          if (x1 > 3 && x1 < 560 && x2 - x1 > 0) {
            opacity = x2 - x1 > 4 ? 0.2 : 1;
            d3.select("#visualisationCommandLog .nv-y")
              .append("rect")
              .attr("x", x1)
              .attr("width", x2 - x1)
              .style("fill", "red")
              .style("opacity", opacity)
              .attr("y", 0)
              .attr("class", "overlayGraph")
              .attr("height", ChartCommandlog.yAxis.range()[0]);
          }
        }
      }
      if (timeStamp > monitor.cmdLogMaxTimeStamp) {
        monitor.cmdLogMaxTimeStamp = timeStamp;
      }
      cmdLogSecCount++;
      cmdLogMinCount++;
    };

    var convertDataFormat = function (rawData, key1, key2) {
      var requiredFormat = [];
      for (var i = 0; i < rawData.length; i++) {
        var newObj = {};
        newObj[key1] = rawData[i].x;
        newObj[key2] = rawData[i].y;
        requiredFormat.push(newObj);
      }
      return requiredFormat;
    };

    var convertDataFormatForPartition = function (partitionData) {
      var requiredFormat = [];
      for (var i = 0; i < partitionData.length; i++) {
        requiredFormat.push({
          key: partitionData[i].key,
          values: partitionData[i].values,
          color: partitionData[i].color,
        });
      }
      return requiredFormat;
    };

    var GetSnapshotOverlay = function (snapshotData, timeInterval) {
      var interval_end = new Date();
      var interval_start = new Date();
      var interval =
        timeInterval == undefined ? RETAINED_TIME_INTERVAL : timeInterval;
      interval_end.setMinutes(interval_end.getMinutes() - interval);
      var snapshotDataArr = [];
      for (var i = 0; i < snapshotData.length; i++) {
        var start_timeStamp = snapshotData[i].startTime;
        var stop_timeStamp = snapshotData[i].endTime;
        if (
          start_timeStamp >= interval_end.getTime() &&
          start_timeStamp <= interval_start.getTime() &&
          start_timeStamp >= interval_end.getTime() &&
          start_timeStamp <= interval_start.getTime()
        ) {
          snapshotDataArr.push(snapshotData[i]);
        }
      }
      return snapshotDataArr;
    };

    this.refreshGraphCmdLog = function () {
      if ($.isFunction(ChartCommandlog.update)) ChartCommandlog.update();
    };

    this.refreshGraphDR = function () {
      var drChartIds = VoltDbUI.drChartList;
      if (drChartIds.length > 0 && !$.isEmptyObject(drChartList)) {
        for (var i = 0; i < drChartIds.length; i++) {
          if (
            $.isFunction(
              drChartList["ChartDrReplicationRate_" + drChartIds[i]].update
            )
          )
            drChartList["ChartDrReplicationRate_" + drChartIds[i]].update();
        }
      }
    };

    this.RefreshThroughputGraph = function (
      throughputDetails,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;

      if (
        monitor.throughputData.length < 1 ||
        monitor.throughputDataMin.length < 1 ||
        monitor.throughputDataDay.length < 1
      ) {
        getThroughputData();
      }

      if (
        dataMapperExporterSec == undefined ||
        $.isEmptyObject(dataMapperExporterSec)
      )
        return;

      if (
        dataMapperExporterDay == undefined ||
        $.isEmptyObject(dataMapperExporterDay)
      )
        return;

      if (
        dataMapperExporterMin == undefined ||
        $.isEmptyObject(dataMapperExporterMin)
      )
        return;

      var throughputData = monitor.throughputData;
      var throughputDataMin = monitor.throughputDataMin;
      var throughputDataDay = monitor.throughputDataDay;
      var throughputDetail = throughputDetails;
      var throughputDetailsArr = [];
      var throughputDetailsArrMin = [];
      var throughputDetailsArrDay = [];

      if (
        localStorage.throughputDetailsMin != undefined &&
        JSON.parse(localStorage.throughputDetailsMin).length ==
          throughputDataMin.length
      ) {
        throughputDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.throughputDetailsMin)
        );
      } else {
        throughputDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(throughputDataMin)
        );
        throughputDetailsArrMin = JSON.parse(throughputDetailsArrMin);
      }

      if (
        localStorage.throughputDetailsDay != undefined &&
        JSON.parse(localStorage.throughputDetailsDay).length ==
          throughputDataDay.length
      ) {
        throughputDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.throughputDetailsDay)
        );
      } else {
        throughputDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(throughputDataDay)
        );
        throughputDetailsArrDay = JSON.parse(throughputDetailsArrDay);
      }

      if (
        localStorage.throughputDetails != undefined &&
        JSON.parse(localStorage.throughputDetails).length ==
          throughputData.length
      ) {
        throughputDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.throughputDetails)
        );
      } else {
        throughputDetailsArr = JSON.stringify(
          convertDataFormatForPartition(throughputData)
        );
        throughputDetailsArr = JSON.parse(throughputDetailsArr);
      }

      if (monitor.throughputFirstData) {
        for (var i = 0; i < throughputDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            throughputDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  throughputDetailsArr[i]["values"][
                    throughputDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            throughputData[keyIndexSec]["values"] = [];
            for (var b = 0; b < throughputDetailsArr[i]["values"].length; b++) {
              throughputData[keyIndexSec]["values"] = sliceFirstData(
                throughputData[keyIndexSec]["values"],
                dataView.Seconds
              );
              throughputData[keyIndexSec]["values"].push({
                x: new Date(throughputDetailsArr[i]["values"][b].x),
                y: throughputDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var k = 0; k < throughputDetailsArrMin.length; k++) {
          var keyIndexMin = k;
          if (
            throughputDetailsArrMin[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  throughputDetailsArrMin[k]["values"][
                    throughputDetailsArrMin[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            throughputDataDay[keyIndexMin]["values"] = [];
            for (
              var c = 0;
              c < throughputDetailsArrMin[k]["values"].length;
              c++
            ) {
              throughputDataMin[keyIndexMin]["values"] = sliceFirstData(
                throughputDataMin[keyIndexMin]["values"],
                dataView.Days
              );
              throughputDataMin[keyIndexMin]["values"].push({
                x: new Date(throughputDetailsArrMin[k]["values"][c].x),
                y: throughputDetailsArrMin[k]["values"][c].y,
              });
            }
          }
        }

        for (var k = 0; k < throughputDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            throughputDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  throughputDetailsArrDay[k]["values"][
                    throughputDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            throughputDataDay[keyIndexDay]["values"] = [];
            for (
              var c = 0;
              c < throughputDetailsArrDay[k]["values"].length;
              c++
            ) {
              throughputDataDay[keyIndexDay]["values"] = sliceFirstData(
                throughputDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              throughputDataDay[keyIndexDay]["values"].push({
                x: new Date(throughputDetailsArrDay[k]["values"][c].x),
                y: throughputDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(throughputDetail) ||
        throughputDetail == undefined ||
        throughputDetail["TIMESTAMP"] == undefined
      )
        return;

      var timeStamp = throughputDetail["TIMESTAMP"];
      if (timeStamp >= monitor.throughputMaxTimeStamp) {
        $.each(throughputDetail, function (key, value) {
          if (key != "TIMESTAMP") {
            var keyValue = key;
            var newValue = value;

            if (throughputSecCount >= 6 || monitor.throughputFirstData) {
              if (!throughputDataMin.hasOwnProperty(keyValue)) {
                var keyIndex = dataMapperExporterMin[keyValue];
                throughputDataMin[keyIndex]["values"] = sliceFirstData(
                  throughputDataMin[keyIndex]["values"],
                  dataView.Minutes
                );
                if (timeStamp == monitor.throughputMaxTimeStamp) {
                  throughputDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: throughputDataMin[keyIndex]["values"][
                      throughputDataMin[keyIndex]["values"].length - 1
                    ].y,
                  });
                  throughputDetailsArrMin = savePartitionDataToLocalStorage(
                    throughputDetailsArrMin,
                    {
                      x: new Date(timeStamp),
                      y: throughputDataMin[keyIndex]["values"][
                        throughputDataMin[keyIndex]["values"].length - 1
                      ].y,
                    },
                    keyIndex
                  );
                } else {
                  throughputDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: newValue,
                  });
                  throughputDetailsArrMin = savePartitionDataToLocalStorage(
                    throughputDetailsArrMin,
                    { x: new Date(timeStamp), y: newValue },
                    keyIndex
                  );
                }
                Monitors.throughputDataMin = throughputDataMin;
              }
            }

            if (throughputMinCount >= 60 || monitor.throughputFirstData) {
              var keyIndexDay = dataMapperExporterDay[keyValue];
              throughputDataDay[keyIndexDay]["values"] = sliceFirstData(
                throughputDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              if (timeStamp == monitor.throughputMaxTimeStamp) {
                throughputDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: throughputDataDay[keyIndexDay]["values"][
                    throughputDataDay[keyIndexDay]["values"].length - 1
                  ].y,
                });
                throughputDetailsArrDay = savePartitionDataToLocalStorage(
                  throughputDetailsArrDay,
                  {
                    x: new Date(timeStamp),
                    y: throughputDataDay[keyIndexDay]["values"][
                      throughputDataDay[keyIndexDay]["values"].length - 1
                    ].y,
                  },
                  keyIndexDay
                );
              } else {
                throughputDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: newValue,
                });
                throughputDetailsArrDay = savePartitionDataToLocalStorage(
                  throughputDetailsArrDay,
                  { x: new Date(timeStamp), y: newValue },
                  keyIndexDay
                );
              }
              Monitors.throughputDataDay = throughputDataDay;
            }

            var keyIndexSec = dataMapperExporterSec[keyValue];

            throughputData[keyIndexSec]["values"] = sliceFirstData(
              throughputData[keyIndexSec]["values"],
              dataView.Seconds
            );
            if (timeStamp == monitor.throughputMaxTimeStamp) {
              throughputData[keyIndexSec]["values"].push({
                x: new Date(timeStamp),
                y: throughputData[keyIndexSec]["values"][
                  throughputData[keyIndexSec]["values"].length - 1
                ].y,
              });
              throughputDetailsArr = savePartitionDataToLocalStorage(
                throughputDetailsArr,
                {
                  x: new Date(timeStamp),
                  y: throughputData[keyIndexSec]["values"][
                    throughputData[keyIndexSec]["values"].length - 1
                  ].y,
                },
                keyIndexSec
              );
            } else {
              throughputData[keyIndexSec].values.push({
                x: new Date(timeStamp),
                y: newValue,
              });
              throughputDetailsArr = savePartitionDataToLocalStorage(
                throughputDetailsArr,
                { x: new Date(timeStamp), y: newValue },
                keyIndexSec
              );
            }

            Monitors.throughputData = throughputData;
          }
        });

        localStorage.throughputDetails = JSON.stringify(throughputDetailsArr);
        localStorage.throughputDetailsMin = JSON.stringify(
          throughputDetailsArrMin
        );
        localStorage.throughputDetailsDay = JSON.stringify(
          throughputDetailsArrDay
        );
        if (monitor.throughputFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.throughputFirstData = false;
        if (throughputSecCount >= 6) throughputSecCount = 0;
        if (throughputMinCount >= 60) throughputMinCount = 0;
        if (graphView == "Minutes") datathroughput = throughputDataMin;
        else if (graphView == "Days") datathroughput = throughputDataDay;
        else {
          datathroughput = throughputData;
        }

        dataThroughput = monitor.throughputData;
      }

      if (timeStamp > monitor.throughputMaxTimeStamp)
        monitor.throughputMaxTimeStamp = timeStamp;

      throughputSecCount++;
      throughputMinCount++;
    };

    this.RefreshQueuedGraph = function (queuedDetails, graphView, currentTab) {
      var monitor = Monitors;

      if (
        monitor.queuedData.length < 1 ||
        monitor.queuedDataMin.length < 1 ||
        monitor.queuedDataDay.length < 1
      ) {
        getQueuedData();
      }

      if (
        dataMapperExporterSec == undefined ||
        $.isEmptyObject(dataMapperExporterSec)
      )
        return;

      if (
        dataMapperExporterDay == undefined ||
        $.isEmptyObject(dataMapperExporterDay)
      )
        return;

      if (
        dataMapperExporterMin == undefined ||
        $.isEmptyObject(dataMapperExporterMin)
      )
        return;

      var queuedData = monitor.queuedData;
      var queuedDataMin = monitor.queuedDataMin;
      var queuedDataDay = monitor.queuedDataDay;
      var queuedDetail = queuedDetails;
      var queuedDetailsArr = [];
      var queuedDetailsArrMin = [];
      var queuedDetailsArrDay = [];

      if (
        localStorage.queuedDetailsMin != undefined &&
        JSON.parse(localStorage.queuedDetailsMin).length == queuedDataMin.length
      ) {
        queuedDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.queuedDetailsMin)
        );
      } else {
        queuedDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(queuedDataMin)
        );
        queuedDetailsArrMin = JSON.parse(queuedDetailsArrMin);
      }

      if (
        localStorage.queuedDetailsDay != undefined &&
        JSON.parse(localStorage.queuedDetailsDay).length == queuedDataDay.length
      ) {
        queuedDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.queuedDetailsDay)
        );
      } else {
        queuedDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(queuedDataDay)
        );
        queuedDetailsArrDay = JSON.parse(queuedDetailsArrDay);
      }

      if (
        localStorage.queuedDetails != undefined &&
        JSON.parse(localStorage.queuedDetails).length == queuedData.length
      ) {
        queuedDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.queuedDetails)
        );
      } else {
        queuedDetailsArr = JSON.stringify(
          convertDataFormatForPartition(queuedData)
        );
        queuedDetailsArr = JSON.parse(queuedDetailsArr);
      }

      if (monitor.queuedFirstData) {
        for (var i = 0; i < queuedDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            queuedDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  queuedDetailsArr[i]["values"][
                    queuedDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            queuedData[keyIndexSec]["values"] = [];
            for (var b = 0; b < queuedDetailsArr[i]["values"].length; b++) {
              queuedData[keyIndexSec]["values"] = sliceFirstData(
                queuedData[keyIndexSec]["values"],
                dataView.Seconds
              );
              queuedData[keyIndexSec]["values"].push({
                x: new Date(queuedDetailsArr[i]["values"][b].x),
                y: queuedDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var k = 0; k < queuedDetailsArrMin.length; k++) {
          var keyIndexMin = k;
          if (
            queuedDetailsArrMin[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  queuedDetailsArrMin[k]["values"][
                    queuedDetailsArrMin[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            queuedDataDay[keyIndexMin]["values"] = [];
            for (var c = 0; c < queuedDetailsArrMin[k]["values"].length; c++) {
              queuedDataMin[keyIndexMin]["values"] = sliceFirstData(
                queuedDataMin[keyIndexMin]["values"],
                dataView.Days
              );
              queuedDataMin[keyIndexMin]["values"].push({
                x: new Date(queuedDetailsArrMin[k]["values"][c].x),
                y: queuedDetailsArrMin[k]["values"][c].y,
              });
            }
          }
        }

        for (var k = 0; k < queuedDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            queuedDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  queuedDetailsArrDay[k]["values"][
                    queuedDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            queuedDataDay[keyIndexDay]["values"] = [];
            for (var c = 0; c < queuedDetailsArrDay[k]["values"].length; c++) {
              queuedDataDay[keyIndexDay]["values"] = sliceFirstData(
                queuedDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              queuedDataDay[keyIndexDay]["values"].push({
                x: new Date(queuedDetailsArrDay[k]["values"][c].x),
                y: queuedDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(queuedDetail) ||
        queuedDetail == undefined ||
        queuedDetail["TIMESTAMP"] == undefined
      )
        return;

      var timeStamp = queuedDetail["TIMESTAMP"];
      if (timeStamp >= monitor.queuedMaxTimeStamp) {
        $.each(queuedDetail, function (key, value) {
          if (key != "TIMESTAMP") {
            var keyValue = key;
            var newValue = value;

            if (queuedSecCount >= 6 || monitor.queuedFirstData) {
              if (!queuedDataMin.hasOwnProperty(keyValue)) {
                var keyIndex = dataMapperExporterMin[keyValue];
                queuedDataMin[keyIndex]["values"] = sliceFirstData(
                  queuedDataMin[keyIndex]["values"],
                  dataView.Minutes
                );
                if (timeStamp == monitor.queuedMaxTimeStamp) {
                  queuedDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: queuedDataMin[keyIndex]["values"][
                      queuedDataMin[keyIndex]["values"].length - 1
                    ].y,
                  });
                  queuedDetailsArrMin = savePartitionDataToLocalStorage(
                    queuedDetailsArrMin,
                    {
                      x: new Date(timeStamp),
                      y: queuedDataMin[keyIndex]["values"][
                        queuedDataMin[keyIndex]["values"].length - 1
                      ].y,
                    },
                    keyIndex
                  );
                } else {
                  queuedDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: newValue,
                  });
                  queuedDetailsArrMin = savePartitionDataToLocalStorage(
                    queuedDetailsArrMin,
                    { x: new Date(timeStamp), y: newValue },
                    keyIndex
                  );
                }
                Monitors.queuedDataMin = queuedDataMin;
              }
            }

            if (queuedMinCount >= 60 || monitor.queuedFirstData) {
              var keyIndexDay = dataMapperExporterDay[keyValue];
              queuedDataDay[keyIndexDay]["values"] = sliceFirstData(
                queuedDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              if (timeStamp == monitor.queuedMaxTimeStamp) {
                queuedDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: queuedDataDay[keyIndexDay]["values"][
                    queuedDataDay[keyIndexDay]["values"].length - 1
                  ].y,
                });
                queuedDetailsArrDay = savePartitionDataToLocalStorage(
                  queuedDetailsArrDay,
                  {
                    x: new Date(timeStamp),
                    y: queuedDataDay[keyIndexDay]["values"][
                      queuedDataDay[keyIndexDay]["values"].length - 1
                    ].y,
                  },
                  keyIndexDay
                );
              } else {
                queuedDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: newValue,
                });
                queuedDetailsArrDay = savePartitionDataToLocalStorage(
                  queuedDetailsArrDay,
                  { x: new Date(timeStamp), y: newValue },
                  keyIndexDay
                );
              }
              Monitors.queuedDataDay = queuedDataDay;
            }

            var keyIndexSec = dataMapperExporterSec[keyValue];

            queuedData[keyIndexSec]["values"] = sliceFirstData(
              queuedData[keyIndexSec]["values"],
              dataView.Seconds
            );
            if (timeStamp == monitor.queuedMaxTimeStamp) {
              queuedData[keyIndexSec]["values"].push({
                x: new Date(timeStamp),
                y: queuedData[keyIndexSec]["values"][
                  queuedData[keyIndexSec]["values"].length - 1
                ].y,
              });
              queuedDetailsArr = savePartitionDataToLocalStorage(
                queuedDetailsArr,
                {
                  x: new Date(timeStamp),
                  y: queuedData[keyIndexSec]["values"][
                    queuedData[keyIndexSec]["values"].length - 1
                  ].y,
                },
                keyIndexSec
              );
            } else {
              queuedData[keyIndexSec].values.push({
                x: new Date(timeStamp),
                y: newValue,
              });
              queuedDetailsArr = savePartitionDataToLocalStorage(
                queuedDetailsArr,
                { x: new Date(timeStamp), y: newValue },
                keyIndexSec
              );
            }

            Monitors.queuedData = queuedData;
          }
        });

        localStorage.queuedDetails = JSON.stringify(queuedDetailsArr);
        localStorage.queuedDetailsMin = JSON.stringify(queuedDetailsArrMin);
        localStorage.queuedDetailsDay = JSON.stringify(queuedDetailsArrDay);
        if (monitor.queuedFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.queuedFirstData = false;
        if (queuedSecCount >= 6) queuedSecCount = 0;
        if (queuedMinCount >= 60) queuedMinCount = 0;
        if (graphView == "Minutes") dataqueued = queuedDataMin;
        else if (graphView == "Days") dataqueued = queuedDataDay;
        else {
          dataqueued = queuedData;
        }

        dataQueued = monitor.queuedData;
      }

      if (timeStamp > monitor.queuedMaxTimeStamp)
        monitor.queuedMaxTimeStamp = timeStamp;

      queuedSecCount++;
      queuedMinCount++;
    };

    this.RefreshOutTransGraph = function (
      outTransDetails,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;

      if (
        monitor.outTransData.length < 1 ||
        monitor.outTransDataMin.length < 1 ||
        monitor.outTransDataDay.length < 1
      ) {
        getOutTransData();
      }

      if (
        dataMapperImporterSec == undefined ||
        $.isEmptyObject(dataMapperImporterSec)
      )
        return;

      if (
        dataMapperImporterDay == undefined ||
        $.isEmptyObject(dataMapperImporterDay)
      )
        return;

      if (
        dataMapperImporterMin == undefined ||
        $.isEmptyObject(dataMapperImporterMin)
      )
        return;

      var outTransData = monitor.outTransData;
      var outTransDataMin = monitor.outTransDataMin;
      var outTransDataDay = monitor.outTransDataDay;
      var outTransDetail = outTransDetails;
      var outTransDetailsArr = [];
      var outTransDetailsArrMin = [];
      var outTransDetailsArrDay = [];

      if (
        localStorage.outTransDetailsMin != undefined &&
        JSON.parse(localStorage.outTransDetailsMin).length ==
          outTransDataMin.length
      ) {
        outTransDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.outTransDetailsMin)
        );
      } else {
        outTransDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(outTransDataMin)
        );
        outTransDetailsArrMin = JSON.parse(outTransDetailsArrMin);
      }

      if (
        localStorage.outTransDetailsDay != undefined &&
        JSON.parse(localStorage.outTransDetailsDay).length ==
          outTransDataDay.length
      ) {
        outTransDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.outTransDetailsDay)
        );
      } else {
        outTransDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(outTransDataDay)
        );
        outTransDetailsArrDay = JSON.parse(outTransDetailsArrDay);
      }

      if (
        localStorage.outTransDetails != undefined &&
        JSON.parse(localStorage.outTransDetails).length == outTransData.length
      ) {
        outTransDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.outTransDetails)
        );
      } else {
        outTransDetailsArr = JSON.stringify(
          convertDataFormatForPartition(outTransData)
        );
        outTransDetailsArr = JSON.parse(outTransDetailsArr);
      }

      if (monitor.outTransFirstData) {
        for (var i = 0; i < outTransDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            outTransDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  outTransDetailsArr[i]["values"][
                    outTransDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            outTransData[keyIndexSec]["values"] = [];
            for (var b = 0; b < outTransDetailsArr[i]["values"].length; b++) {
              outTransData[keyIndexSec]["values"] = sliceFirstData(
                outTransData[keyIndexSec]["values"],
                dataView.Seconds
              );
              outTransData[keyIndexSec]["values"].push({
                x: new Date(outTransDetailsArr[i]["values"][b].x),
                y: outTransDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var j = 0; j < outTransDetailsArrMin.length; j++) {
          var keyIndexMin = j;
          if (
            outTransDetailsArrMin[j]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  outTransDetailsArrMin[j]["values"][
                    outTransDetailsArrMin[j]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            outTransDataMin[keyIndexMin]["values"] = [];
            for (
              var a = 0;
              a < outTransDetailsArrMin[j]["values"].length;
              a++
            ) {
              outTransDataMin[keyIndexMin]["values"] = sliceFirstData(
                outTransDataMin[keyIndexMin]["values"],
                dataView.Minutes
              );
              outTransDataMin[keyIndexMin]["values"].push({
                x: new Date(outTransDetailsArrMin[j]["values"][a].x),
                y: outTransDetailsArrMin[j]["values"][a].y,
              });
            }
          }
        }

        for (var k = 0; k < outTransDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            outTransDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  outTransDetailsArrDay[k]["values"][
                    outTransDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            outTransDataDay[keyIndexMin]["values"] = [];
            for (
              var c = 0;
              c < outTransDetailsArrDay[k]["values"].length;
              c++
            ) {
              outTransDataDay[keyIndexDay]["values"] = sliceFirstData(
                outTransDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              outTransDataDay[keyIndexDay]["values"].push({
                x: new Date(outTransDetailsArrDay[k]["values"][c].x),
                y: outTransDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(outTransDetail) ||
        outTransDetail == undefined ||
        outTransDetail["TIMESTAMP"] == undefined
      )
        return;

      var timeStamp = outTransDetail["TIMESTAMP"];
      if (timeStamp >= monitor.outTransMaxTimeStamp) {
        $.each(outTransDetail, function (key, value) {
          if (key != "TIMESTAMP") {
            var keyValue = key;
            var newValue = value;

            if (outTransSecCount >= 6 || monitor.outTransFirstData) {
              if (!outTransDataMin.hasOwnProperty(keyValue)) {
                var keyIndex = dataMapperImporterMin[keyValue];
                outTransDataMin[keyIndex]["values"] = sliceFirstData(
                  outTransDataMin[keyIndex]["values"],
                  dataView.Minutes
                );
                if (timeStamp == monitor.outTransMaxTimeStamp) {
                  outTransDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: outTransDataMin[keyIndex]["values"][
                      outTransDataMin[keyIndex]["values"].length - 1
                    ].y,
                  });
                  outTransDetailsArrMin = savePartitionDataToLocalStorage(
                    outTransDetailsArrMin,
                    {
                      x: new Date(timeStamp),
                      y: outTransDataMin[keyIndex]["values"][
                        outTransDataMin[keyIndex]["values"].length - 1
                      ].y,
                    },
                    keyIndex
                  );
                } else {
                  outTransDataMin[keyIndex]["values"].push({
                    x: new Date(timeStamp),
                    y: newValue,
                  });
                  outTransDetailsArrMin = savePartitionDataToLocalStorage(
                    outTransDetailsArrMin,
                    { x: new Date(timeStamp), y: newValue },
                    keyIndex
                  );
                }
                Monitors.outTransDataMin = outTransDataMin;
              }
            }

            if (outTransMinCount >= 60 || monitor.outTransFirstData) {
              var keyIndexDay = dataMapperImporterDay[keyValue];
              outTransDataDay[keyIndexDay]["values"] = sliceFirstData(
                outTransDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              if (timeStamp == monitor.outTransMaxTimeStamp) {
                outTransDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: outTransDataDay[keyIndexDay]["values"][
                    outTransDataDay[keyIndexDay]["values"].length - 1
                  ].y,
                });
                outTransDetailsArrDay = savePartitionDataToLocalStorage(
                  outTransDetailsArrDay,
                  {
                    x: new Date(timeStamp),
                    y: outTransDataDay[keyIndexDay]["values"][
                      outTransDataDay[keyIndexDay]["values"].length - 1
                    ].y,
                  },
                  keyIndexDay
                );
              } else {
                outTransDataDay[keyIndexDay]["values"].push({
                  x: new Date(timeStamp),
                  y: newValue,
                });
                outTransDetailsArrDay = savePartitionDataToLocalStorage(
                  outTransDetailsArrDay,
                  { x: new Date(timeStamp), y: newValue },
                  keyIndexDay
                );
              }
              Monitors.outTransDataDay = outTransDataDay;
            }

            var keyIndexSec = dataMapperImporterSec[keyValue];

            outTransData[keyIndexSec]["values"] = sliceFirstData(
              outTransData[keyIndexSec]["values"],
              dataView.Seconds
            );
            if (timeStamp == monitor.outTransMaxTimeStamp) {
              outTransData[keyIndexSec]["values"].push({
                x: new Date(timeStamp),
                y: outTransData[keyIndexSec]["values"][
                  outTransData[keyIndexSec]["values"].length - 1
                ].y,
              });
              outTransDetailsArr = savePartitionDataToLocalStorage(
                outTransDetailsArr,
                {
                  x: new Date(timeStamp),
                  y: outTransData[keyIndexSec]["values"][
                    outTransData[keyIndexSec]["values"].length - 1
                  ].y,
                },
                keyIndexSec
              );
            } else {
              outTransData[keyIndexSec].values.push({
                x: new Date(timeStamp),
                y: newValue,
              });
              outTransDetailsArr = savePartitionDataToLocalStorage(
                outTransDetailsArr,
                { x: new Date(timeStamp), y: newValue },
                keyIndexSec
              );
            }
            Monitors.outTransData = outTransData;
          }
        });

        localStorage.outTransDetails = JSON.stringify(outTransDetailsArr);
        localStorage.outTransDetailsMin = JSON.stringify(outTransDetailsArrMin);
        localStorage.outTransDetailsDay = JSON.stringify(outTransDetailsArrDay);
        if (monitor.outTransFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.outTransFirstData = false;
        if (outTransSecCount >= 6) outTransSecCount = 0;
        if (outTransMinCount >= 60) outTransMinCount = 0;
        if (graphView == "Minutes") dataOutTrans = outTransDataMin;
        else if (graphView == "Days") dataOutTrans = outTransDataDay;
        else {
          dataOutTrans = outTransData;
        }

        if (
          currentTab == NavigationTabs.Importer &&
          currentViewImporter == graphView
        ) {
          d3.select("#visualisationOutTrans")
            .datum(dataOutTrans)
            .transition()
            .duration(500)
            .call(ChartOutTrans);
        }
      }
      if (timeStamp > monitor.outTransMaxTimeStamp)
        monitor.outTransMaxTimeStamp = timeStamp;

      outTransSecCount++;
      outTransMinCount++;
    };

    this.RefreshSuccessRateGraph = function (
      successDetails,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;

      if (
        monitor.successRateData.length < 1 ||
        monitor.successRateDataMin.length < 1 ||
        monitor.successRateDataDay.length < 1
      ) {
        getSuccessRateData();
      }

      if (
        dataMapperImporterSec == undefined ||
        $.isEmptyObject(dataMapperImporterSec)
      )
        return;

      if (
        dataMapperImporterDay == undefined ||
        $.isEmptyObject(dataMapperImporterDay)
      )
        return;

      if (
        dataMapperImporterMin == undefined ||
        $.isEmptyObject(dataMapperImporterMin)
      )
        return;

      var successRateData = monitor.successRateData;
      var successRateDataMin = monitor.successRateDataMin;
      var successRateDataDay = monitor.successRateDataDay;
      var successRateDetail = successDetails;
      var successRateDetailsArr = [];
      var successRateDetailsArrMin = [];
      var successRageDetailsArrDay = [];

      if (
        localStorage.successRateDetailsMin != undefined &&
        JSON.parse(localStorage.successRateDetailsMin).length ==
          successRateDataMin.length
      ) {
        successRateDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.successRateDetailsMin)
        );
      } else {
        successRateDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(successRateDataMin)
        );
        successRateDetailsArrMin = JSON.parse(successRateDetailsArrMin);
      }

      if (
        localStorage.successRateDetailsDay != undefined &&
        JSON.parse(localStorage.successRateDetailsDay).length ==
          successRateDataDay.length
      ) {
        successRageDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.successRateDetailsDay)
        );
      } else {
        successRageDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(successRateDataDay)
        );
        successRageDetailsArrDay = JSON.parse(successRageDetailsArrDay);
      }
      if (
        localStorage.successRateDetails != undefined &&
        JSON.parse(localStorage.successRateDetails).length ==
          successRateData.length
      ) {
        successRateDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.successRateDetails)
        );
      } else {
        successRateDetailsArr = JSON.stringify(
          convertDataFormatForPartition(successRateData)
        );
        successRateDetailsArr = JSON.parse(successRateDetailsArr);
      }

      if (monitor.successRateFirstData) {
        for (var i = 0; i < successRateDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            successRateDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  successRateDetailsArr[i]["values"][
                    successRateDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            successRateData[keyIndexSec]["values"] = [];
            for (
              var b = 0;
              b < successRateDetailsArr[i]["values"].length;
              b++
            ) {
              successRateData[keyIndexSec]["values"] = sliceFirstData(
                successRateData[keyIndexSec]["values"],
                dataView.Seconds
              );
              successRateData[keyIndexSec]["values"].push({
                x: new Date(successRateDetailsArr[i]["values"][b].x),
                y: successRateDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var j = 0; j < successRateDetailsArrMin.length; j++) {
          var keyIndexMin = j;
          if (
            successRateDetailsArrMin[j]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  successRateDetailsArrMin[j]["values"][
                    successRateDetailsArrMin[j]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            successRateDataMin[keyIndexMin]["values"] = [];
            for (
              var a = 0;
              a < successRateDetailsArrMin[j]["values"].length;
              a++
            ) {
              successRateDataMin[keyIndexMin]["values"] = sliceFirstData(
                successRateDataMin[keyIndexMin]["values"],
                dataView.Minutes
              );
              successRateDataMin[keyIndexMin]["values"].push({
                x: new Date(successRateDetailsArrMin[j]["values"][a].x),
                y: successRateDetailsArrMin[j]["values"][a].y,
              });
            }
          }
        }

        for (var k = 0; k < successRageDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            successRageDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  successRageDetailsArrDay[k]["values"][
                    successRageDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            successRateDataDay[keyIndexMin]["values"] = [];
            for (
              var c = 0;
              c < successRageDetailsArrDay[k]["values"].length;
              c++
            ) {
              successRateDataDay[keyIndexDay]["values"] = sliceFirstData(
                successRateDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              successRateDataDay[keyIndexDay]["values"].push({
                x: new Date(successRageDetailsArrDay[k]["values"][c].x),
                y: successRageDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(successRateDetail) ||
        successRateDetail == undefined ||
        successRateDetail["TIMESTAMP"] == undefined
      )
        return;

      var timeStamp = successRateDetail["TIMESTAMP"];
      if (timeStamp >= monitor.successRateMaxTimeStamp) {
        $.each(successRateDetail, function (key, value) {
          if (key != "TIMESTAMP") {
            var keyValue = key;
            var newValue = value;
            var keyIndex = dataMapperImporterMin[keyValue];
            if (
              !$.isEmptyObject(previousSuccessRate) &&
              previousSuccessRate.hasOwnProperty(keyValue)
            ) {
              var previousTimeStamp = previousSuccessRate[keyValue].timeStamp;
              var previousValue = previousSuccessRate[keyValue].value;

              var calculatedValue =
                ((previousValue - newValue) * -1) /
                ((timeStamp - previousTimeStamp) / 1000);
              if (calculatedValue == -0) calculatedValue = 0;
              previousSuccessRate[keyValue] = {
                timeStamp: timeStamp,
                value: newValue,
              };
              if (successRateSecCount >= 6 || monitor.successRateFirstData) {
                if (!successRateDataMin.hasOwnProperty(keyValue)) {
                  successRateDataMin[keyIndex]["values"] = sliceFirstData(
                    successRateDataMin[keyIndex]["values"],
                    dataView.Minutes
                  );
                  if (timeStamp == monitor.successRateMaxTimeStamp) {
                    successRateDataMin[keyIndex]["values"].push({
                      x: new Date(timeStamp),
                      y: successRateDataMin[keyIndex]["values"][
                        successRateDataMin[keyIndex]["values"].length - 1
                      ].y,
                    });
                    successRateDetailsArrMin = savePartitionDataToLocalStorage(
                      successRateDetailsArrMin,
                      {
                        x: new Date(timeStamp),
                        y: successRateDataMin[keyIndex]["values"][
                          successRateDataMin[keyIndex]["values"].length - 1
                        ].y,
                      },
                      keyIndex
                    );
                  } else {
                    successRateDataMin[keyIndex]["values"].push({
                      x: new Date(timeStamp),
                      y: calculatedValue,
                    });
                    successRateDetailsArrMin = savePartitionDataToLocalStorage(
                      successRateDetailsArrMin,
                      { x: new Date(timeStamp), y: calculatedValue },
                      keyIndex
                    );
                  }
                  Monitors.successRateDataMin = successRateDataMin;
                }
              }

              if (successRateMinCount >= 60 || monitor.successRateFirstData) {
                var keyIndexDay = dataMapperImporterDay[keyValue];
                successRateDataDay[keyIndexDay]["values"] = sliceFirstData(
                  successRateDataDay[keyIndexDay]["values"],
                  dataView.Days
                );
                if (timeStamp == monitor.successRateMaxTimeStamp) {
                  successRateDataDay[keyIndexDay]["values"].push({
                    x: new Date(timeStamp),
                    y: successRateDataDay[keyIndexDay]["values"][
                      successRateDataDay[keyIndexDay]["values"].length - 1
                    ].y,
                  });
                  successRageDetailsArrDay = savePartitionDataToLocalStorage(
                    successRageDetailsArrDay,
                    {
                      x: new Date(timeStamp),
                      y: successRateDataDay[keyIndexDay]["values"][
                        successRateDataDay[keyIndexDay]["values"].length - 1
                      ].y,
                    },
                    keyIndexDay
                  );
                } else {
                  successRateDataDay[keyIndexDay]["values"].push({
                    x: new Date(timeStamp),
                    y: calculatedValue,
                  });
                  successRageDetailsArrDay = savePartitionDataToLocalStorage(
                    successRageDetailsArrDay,
                    { x: new Date(timeStamp), y: calculatedValue },
                    keyIndexDay
                  );
                }
                Monitors.successRateDataDay = successRateDataDay;
              }

              var keyIndexSec = dataMapperImporterSec[keyValue];

              successRateData[keyIndexSec]["values"] = sliceFirstData(
                successRateData[keyIndexSec]["values"],
                dataView.Seconds
              );
              if (timeStamp == monitor.successRateMaxTimeStamp) {
                successRateData[keyIndexSec]["values"].push({
                  x: new Date(timeStamp),
                  y: successRateData[keyIndexSec]["values"][
                    successRateData[keyIndexSec]["values"].length - 1
                  ].y,
                });
                successRateDetailsArr = savePartitionDataToLocalStorage(
                  successRateDetailsArr,
                  {
                    x: new Date(timeStamp),
                    y: successRateData[keyIndexSec]["values"][
                      successRateData[keyIndexSec]["values"].length - 1
                    ].y,
                  },
                  keyIndexSec
                );
              } else {
                successRateData[keyIndexSec].values.push({
                  x: new Date(timeStamp),
                  y: calculatedValue,
                });
                successRateDetailsArr = savePartitionDataToLocalStorage(
                  successRateDetailsArr,
                  { x: new Date(timeStamp), y: calculatedValue },
                  keyIndexSec
                );
              }
              Monitors.successRateData = successRateData;
            } else {
              previousSuccessRate[keyValue] = {
                timeStamp: timeStamp,
                value: newValue,
              };
            }
          }
        });

        localStorage.successRateDetails = JSON.stringify(successRateDetailsArr);
        localStorage.successRateDetailsMin = JSON.stringify(
          successRateDetailsArrMin
        );
        localStorage.successRateDetailsDay = JSON.stringify(
          successRageDetailsArrDay
        );
        if (monitor.successRateFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.successRateFirstData = false;
        if (successRateSecCount >= 6) successRateSecCount = 0;
        if (successRateMinCount >= 60) successRateMinCount = 0;

        if (graphView == "Minutes") dataSuccessRate = successRateDataMin;
        else if (graphView == "Days") dataSuccessRate = successRateDataDay;
        else {
          dataSuccessRate = successRateData;
        }

        if (
          currentTab == NavigationTabs.Importer &&
          currentViewImporter == graphView
        ) {
          d3.select("#visualisationSuccessRate")
            .datum(dataSuccessRate)
            .transition()
            .duration(500)
            .call(ChartSuccessRate);
        }
      }
      if (timeStamp > monitor.successRateMaxTimeStamp)
        monitor.successRateMaxTimeStamp = timeStamp;

      successRateSecCount++;
      successRateMinCount++;
    };

    this.RefreshFailureRateGraph = function (
      failureDetails,
      graphView,
      currentTab
    ) {
      var monitor = Monitors;

      if (
        monitor.failureRateData.length < 1 ||
        monitor.failureRateDataMin.length < 1 ||
        monitor.failureRateDataDay.length < 1
      ) {
        getFailureRateData();
      }

      if (
        dataMapperImporterSec == undefined ||
        $.isEmptyObject(dataMapperImporterSec)
      )
        return;

      if (
        dataMapperImporterDay == undefined ||
        $.isEmptyObject(dataMapperImporterDay)
      )
        return;

      if (
        dataMapperImporterMin == undefined ||
        $.isEmptyObject(dataMapperImporterMin)
      )
        return;

      var failureRateData = monitor.failureRateData;
      var failureRateDataMin = monitor.failureRateDataMin;
      var failureRateDataDay = monitor.failureRateDataDay;
      var failureRateDetail = failureDetails;
      var failureRateDetailsArr = [];
      var failureRateDetailsArrMin = [];
      var failureRageDetailsArrDay = [];

      if (
        localStorage.failureRateDetailsMin != undefined &&
        JSON.parse(localStorage.failureRateDetailsMin).length ==
          failureRateDataMin.length
      ) {
        failureRateDetailsArrMin = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.failureRateDetailsMin)
        );
      } else {
        failureRateDetailsArrMin = JSON.stringify(
          convertDataFormatForPartition(failureRateDataMin)
        );
        failureRateDetailsArrMin = JSON.parse(failureRateDetailsArrMin);
      }

      if (
        localStorage.failureRateDetailsDay != undefined &&
        JSON.parse(localStorage.failureRateDetailsDay).length ==
          failureRateDataDay.length
      ) {
        failureRageDetailsArrDay = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.failureRateDetailsDay)
        );
      } else {
        failureRageDetailsArrDay = JSON.stringify(
          convertDataFormatForPartition(failureRateDataDay)
        );
        failureRageDetailsArrDay = JSON.parse(failureRageDetailsArrDay);
      }
      if (
        localStorage.failureRateDetails != undefined &&
        JSON.parse(localStorage.failureRateDetails).length ==
          failureRateData.length
      ) {
        failureRateDetailsArr = getFormattedPartitionDataFromLocalStorage(
          JSON.parse(localStorage.failureRateDetails)
        );
      } else {
        failureRateDetailsArr = JSON.stringify(
          convertDataFormatForPartition(failureRateData)
        );
        failureRateDetailsArr = JSON.parse(failureRateDetailsArr);
      }

      if (monitor.failureRateFirstData) {
        for (var i = 0; i < failureRateDetailsArr.length; i++) {
          var keyIndexSec = i;
          if (
            failureRateDetailsArr[i]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  failureRateDetailsArr[i]["values"][
                    failureRateDetailsArr[i]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.secGraph
            )
          ) {
            failureRateData[keyIndexSec]["values"] = [];
            for (
              var b = 0;
              b < failureRateDetailsArr[i]["values"].length;
              b++
            ) {
              failureRateData[keyIndexSec]["values"] = sliceFirstData(
                failureRateData[keyIndexSec]["values"],
                dataView.Seconds
              );
              failureRateData[keyIndexSec]["values"].push({
                x: new Date(failureRateDetailsArr[i]["values"][b].x),
                y: failureRateDetailsArr[i]["values"][b].y,
              });
            }
          }
        }

        for (var j = 0; j < failureRateDetailsArrMin.length; j++) {
          var keyIndexMin = j;
          if (
            failureRateDetailsArrMin[j]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  failureRateDetailsArrMin[j]["values"][
                    failureRateDetailsArrMin[j]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.minGraph
            )
          ) {
            failureRateDataMin[keyIndexMin]["values"] = [];
            for (
              var a = 0;
              a < failureRateDetailsArrMin[j]["values"].length;
              a++
            ) {
              failureRateDataMin[keyIndexMin]["values"] = sliceFirstData(
                failureRateDataMin[keyIndexMin]["values"],
                dataView.Minutes
              );
              failureRateDataMin[keyIndexMin]["values"].push({
                x: new Date(failureRateDetailsArrMin[j]["values"][a].x),
                y: failureRateDetailsArrMin[j]["values"][a].y,
              });
            }
          }
        }

        for (var k = 0; k < failureRageDetailsArrDay.length; k++) {
          var keyIndexDay = k;
          if (
            failureRageDetailsArrDay[k]["values"].length > 0 &&
            !(
              currentTime.getTime() -
                new Date(
                  failureRageDetailsArrDay[k]["values"][
                    failureRageDetailsArrDay[k]["values"].length - 1
                  ].timestamp
                ).getTime() >
              MonitorGraphUI.enumMaxTimeGap.dayGraph
            )
          ) {
            failureRateDataDay[keyIndexMin]["values"] = [];
            for (
              var c = 0;
              c < failureRageDetailsArrDay[k]["values"].length;
              c++
            ) {
              failureRateDataDay[keyIndexDay]["values"] = sliceFirstData(
                failureRateDataDay[keyIndexDay]["values"],
                dataView.Days
              );
              failureRateDataDay[keyIndexDay]["values"].push({
                x: new Date(failureRageDetailsArrDay[k]["values"][c].x),
                y: failureRageDetailsArrDay[k]["values"][c].y,
              });
            }
          }
        }
      }

      if (
        $.isEmptyObject(failureRateDetail) ||
        failureRateDetail == undefined ||
        failureRateDetail["TIMESTAMP"] == undefined
      )
        return;

      var timeStamp = failureRateDetail["TIMESTAMP"];
      if (timeStamp >= monitor.failureRateMaxTimeStamp) {
        $.each(failureRateDetail, function (key, value) {
          if (key != "TIMESTAMP") {
            var keyValue = key;
            var newValue = value;
            var keyIndex = dataMapperImporterMin[keyValue];
            if (
              !$.isEmptyObject(previousFailureRate) &&
              previousFailureRate.hasOwnProperty(keyValue)
            ) {
              var previousTimeStamp = previousFailureRate[keyValue].timeStamp;
              var previousValue = previousFailureRate[keyValue].value;

              var calculatedValue =
                ((previousValue - newValue) * -1) /
                ((timeStamp - previousTimeStamp) / 1000);
              if (calculatedValue == -0) calculatedValue = 0;

              previousFailureRate[keyValue] = {
                timeStamp: timeStamp,
                value: newValue,
              };
              if (failureRateSecCount >= 6 || monitor.failureRateFirstData) {
                if (!failureRateDataMin.hasOwnProperty(keyValue)) {
                  failureRateDataMin[keyIndex]["values"] = sliceFirstData(
                    failureRateDataMin[keyIndex]["values"],
                    dataView.Minutes
                  );
                  if (timeStamp == monitor.failureRateMaxTimeStamp) {
                    failureRateDataMin[keyIndex]["values"].push({
                      x: new Date(timeStamp),
                      y: failureRateDataMin[keyIndex]["values"][
                        failureRateDataMin[keyIndex]["values"].length - 1
                      ].y,
                    });
                    failureRateDetailsArrMin = savePartitionDataToLocalStorage(
                      failureRateDetailsArrMin,
                      {
                        x: new Date(timeStamp),
                        y: failureRateDataMin[keyIndex]["values"][
                          failureRateDataMin[keyIndex]["values"].length - 1
                        ].y,
                      },
                      keyIndex
                    );
                  } else {
                    failureRateDataMin[keyIndex]["values"].push({
                      x: new Date(timeStamp),
                      y: calculatedValue,
                    });
                    failureRateDetailsArrMin = savePartitionDataToLocalStorage(
                      failureRateDetailsArrMin,
                      { x: new Date(timeStamp), y: calculatedValue },
                      keyIndex
                    );
                  }
                  Monitors.failureRateDataMin = failureRateDataMin;
                }
              }

              if (failureRateMinCount >= 60 || monitor.failureRateFirstData) {
                var keyIndexDay = dataMapperImporterDay[keyValue];
                failureRateDataDay[keyIndexDay]["values"] = sliceFirstData(
                  failureRateDataDay[keyIndexDay]["values"],
                  dataView.Days
                );
                if (timeStamp == monitor.failureRateMaxTimeStamp) {
                  failureRateDataDay[keyIndexDay]["values"].push({
                    x: new Date(timeStamp),
                    y: failureRateDataDay[keyIndexDay]["values"][
                      failureRateDataDay[keyIndexDay]["values"].length - 1
                    ].y,
                  });
                  failureRageDetailsArrDay = savePartitionDataToLocalStorage(
                    failureRageDetailsArrDay,
                    {
                      x: new Date(timeStamp),
                      y: failureRateDataDay[keyIndexDay]["values"][
                        failureRateDataDay[keyIndexDay]["values"].length - 1
                      ].y,
                    },
                    keyIndexDay
                  );
                } else {
                  failureRateDataDay[keyIndexDay]["values"].push({
                    x: new Date(timeStamp),
                    y: calculatedValue,
                  });
                  failureRageDetailsArrDay = savePartitionDataToLocalStorage(
                    failureRageDetailsArrDay,
                    { x: new Date(timeStamp), y: calculatedValue },
                    keyIndexDay
                  );
                }
                Monitors.failureRateDataDay = failureRateDataDay;
              }

              var keyIndexSec = dataMapperImporterSec[keyValue];

              failureRateData[keyIndexSec]["values"] = sliceFirstData(
                failureRateData[keyIndexSec]["values"],
                dataView.Seconds
              );
              if (timeStamp == monitor.failureRateMaxTimeStamp) {
                failureRateData[keyIndexSec]["values"].push({
                  x: new Date(timeStamp),
                  y: failureRateData[keyIndexSec]["values"][
                    failureRateData[keyIndexSec]["values"].length - 1
                  ].y,
                });
                failureRateDetailsArr = savePartitionDataToLocalStorage(
                  failureRateDetailsArr,
                  {
                    x: new Date(timeStamp),
                    y: failureRateData[keyIndexSec]["values"][
                      failureRateData[keyIndexSec]["values"].length - 1
                    ].y,
                  },
                  keyIndexSec
                );
              } else {
                failureRateData[keyIndexSec].values.push({
                  x: new Date(timeStamp),
                  y: calculatedValue,
                });
                failureRateDetailsArr = savePartitionDataToLocalStorage(
                  failureRateDetailsArr,
                  { x: new Date(timeStamp), y: calculatedValue },
                  keyIndexSec
                );
              }
              Monitors.failureRateData = failureRateData;
            } else {
              previousFailureRate[keyValue] = {
                timeStamp: timeStamp,
                value: newValue,
              };
            }
          }
        });

        localStorage.failureRateDetails = JSON.stringify(failureRateDetailsArr);
        localStorage.failureRateDetailsMin = JSON.stringify(
          failureRateDetailsArrMin
        );
        localStorage.failureRateDetailsDay = JSON.stringify(
          failureRageDetailsArrDay
        );
        if (monitor.failureRateFirstData) {
          $(".legend").css("display", "block");
        }
        monitor.failureRateFirstData = false;
        if (failureRateSecCount >= 6) failureRateSecCount = 0;
        if (failureRateMinCount >= 60) failureRateMinCount = 0;

        if (graphView == "Minutes") dataFailureRate = failureRateDataMin;
        else if (graphView == "Days") dataFailureRate = failureRateDataDay;
        else {
          dataFailureRate = failureRateData;
        }

        if (
          currentTab == NavigationTabs.Importer &&
          currentViewImporter == graphView
        ) {
          d3.select("#visualisationFailureRate")
            .datum(dataFailureRate)
            .transition()
            .duration(500)
            .call(ChartFailureRate);
        }
      }
      if (timeStamp > monitor.failureRateMaxTimeStamp)
        monitor.failureRateMaxTimeStamp = timeStamp;

      failureRateSecCount++;
      failureRateMinCount++;
    };

    function getThroughputData() {
      var monitor = Monitors;
      monitor.throughputData = getThroughputExportData(
        emptyData,
        dataMapperExporterSec
      );
      monitor.throughputDataMin = getThroughputExportData(
        emptyDataForMinutes,
        dataMapperExporterMin
      );
      monitor.throughputDataDay = getThroughputExportData(
        emptyDataForDays,
        dataMapperExporterDay
      );
    }

    function getQueuedData() {
      var monitor = Monitors;
      monitor.queuedData = getQueuedExportData(
        emptyData,
        dataMapperExporterSec
      );
      monitor.queuedDataMin = getQueuedExportData(
        emptyDataForMinutes,
        dataMapperExporterMin
      );
      monitor.queuedDataDay = getQueuedExportData(
        emptyDataForDays,
        dataMapperExporterDay
      );
    }

    function getOutTransData() {
      var monitor = Monitors;
      monitor.outTransData = getImportData(emptyData, dataMapperImporterSec);
      monitor.outTransDataMin = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      );
      monitor.outTransDataDay = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      );
    }

    function getSuccessRateData() {
      var monitor = Monitors;
      monitor.successRateData = getImportData(emptyData, dataMapperImporterSec);
      monitor.successRateDataMin = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      );
      monitor.successRateDataDay = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      );
    }

    function getFailureRateData() {
      var monitor = Monitors;
      monitor.failureRateData = getImportData(emptyData, dataMapperImporterSec);
      monitor.failureRateDataMin = getImportData(
        emptyDataForMinutes,
        dataMapperImporterMin
      );
      monitor.failureRateDataDay = getImportData(
        emptyDataForDays,
        dataMapperImporterDay
      );
    }
  };

  window.MonitorGraphUI = new IMonitorGraphUI();
})(window);
