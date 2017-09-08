var latencyDetails = [];
function loadAnalysisPage(){
    VoltDbAnalysis.setDefaultAnalysisSettings();
    $("#tabAnalysis li a").on("click", function(){
        VoltDbAnalysis.refreshChart();
        VoltDbAnalysis.refreshChart();
    })

    $("#ulProcedure li a").on("click", function(){
        refreshLegend($($(this)[0]).text());
    })

    $("#tabProcedureBtn").on("click", function(){
        VoltDbUI.isData = false;
    })

    $("#ulData").on("click", function(){
        VoltDbUI.isData = true;
        VoltDbUI.isFrequency = false;
        VoltDbUI.isLatency = false;
        VoltDbUI.isTotalProcessing = false;
    })



    function refreshLegend(legendTitle){
        if(legendTitle == "Frequency"){
            VoltDbUI.isTotalProcessing = false;
            VoltDbUI.isLatency = false;
            VoltDbUI.isFrequency = true;
            VoltDbUI.isData = false;
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Number Of Invocations(" : "Number Of Invocations");
            VoltDbAnalysis.currentTab =  "Frequency";
        } else if(legendTitle == "Total Processing Time"){
            VoltDbUI.isTotalProcessing = true;
            VoltDbUI.isLatency = false;
            VoltDbUI.isFrequency = false;
            VoltDbUI.isData = false;
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Total Processing Time(" : "Total Processing Time");
            VoltDbAnalysis.currentTab = "Total Processing Time";
        } else {
            VoltDbUI.isTotalProcessing = false;
            VoltDbUI.isLatency = true;
            VoltDbUI.isFrequency = false;
            VoltDbUI.isData = false;
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Average Execution Time(" : "Average Execution Time");
            VoltDbAnalysis.currentTab = "Average Execution Time";
        }
        //this method is called twice to ensure graph reloads properly
        VoltDbAnalysis.refreshChart();
        VoltDbAnalysis.refreshChart();
    }

    function formatAnalysisLegend(isMP, isP){
        if(isMP && isP){
            $("#legendAnalysisMP").hide();
            $("#legendAnalysisP").hide();
            $("#legendAnalysisBoth").show();
            VoltDbAnalysis.partitionStatus = "both"
        } else if(isMP){
            $("#legendAnalysisMP").show();
            $("#legendAnalysisP").hide();
            $("#legendAnalysisBoth").hide();
            VoltDbAnalysis.partitionStatus = "MP"
        } else {
            $("#legendAnalysisMP").hide();
            $("#legendAnalysisP").show();
            $("#legendAnalysisBoth").hide();
            VoltDbAnalysis.partitionStatus = "SP"
        }
        refreshLegend(VoltDbAnalysis.currentTab);
    }

    function formatAnalysisTableLegend(isP, isR){
        if(isP && isR){
            $("#legendDataAnalysisP").hide();
            $("#legendDataAnalysisR").hide();
            $("#legendDataAnalysisBoth").show();
        } else if(isP){
            $("#legendDataAnalysisP").show();
            $("#legendDataAnalysisR").hide();
            $("#legendAnalysisBoth").hide();
        } else {
            $("#legendDataAnalysisP").hide();
            $("#legendDataAnalysisR").show();
            $("#legendDataAnalysisBoth").hide();
        }
    }

    function displayWarningMessages(warningMsgList){
        for(var i = 0; i < warningMsgList.length; i++){
            $("#procedureWarning").append(warningMsgList[i]["MESSAGE"]);
        }
    }

    function fetchData (){
        $("#analysisLoader").show();
        $("#analysisRemarks").hide();
        $("#procedureWarning").html("");
        $("#tableWarning").html("");
        VoltDbAnalysis.refreshChart();

        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            if(procedureDetails != undefined){
                if(!$.isEmptyObject(procedureDetails["PROCEDURE_DETAIL"])){
                    $(".analyzeNowContent").hide();
                    $(".dataContent").show();
                    $(".noDataContent").hide();
                } else {
                    $(".mainContentAnalysis").hide();
                    $(".dataContent").hide();
                    $(".noDataContent").show();

                }
            }
            $("#analysisLoader").hide();
            $("#procedureWarningSection").hide();
            var dataLatencyProcedures = [];
            var dataLatencySysProcedures = [];
            var dataFrequencySysProcedures = [];
            var dataFrequencyProcedures = [];
            var dataTotalProcessingProcedures = [];
            var dataTotalProcessingSysProcedures = [];
            var timestamp;
            var isMPPresent = false;
            var isPPresent = false;
            var averageExecutionTime = VoltDbUI.getFromLocalStorage("averageExecutionTime");
            var showHideSysProcedures = VoltDbUI.getFromLocalStorage("showHideSysProcedures");
            var procedureObj = {}
            var type = "Single Partitioned";

            for(var i = 0; i < procedureDetails["PROCEDURE_DETAIL"].length; i++ ){
                var statement = procedureDetails["PROCEDURE_DETAIL"][i].STATEMENT;
                var procedure = procedureDetails["PROCEDURE_DETAIL"][i].PROCEDURE;
                if(i == 0)
                    timestamp = procedureDetails["PROCEDURE_DETAIL"][i].TIMESTAMP;

                if(!procedureObj.hasOwnProperty(procedure)){
                    procedureObj[procedure] = {};
                    procedureObj[procedure]["COUNT"] = 0;
                    procedureObj[procedure]["AVG"] = 0;
                    procedureObj[procedure]["INVOCATION"] = 0;
                    procedureObj[procedure]["TYPE"] = "Single Partitioned"
                }

                if(statement == "<ALL>"){
                    procedureObj[procedure]["COUNT"]++;
                    procedureObj[procedure]["AVG"] += procedureDetails["PROCEDURE_DETAIL"][i].AVG_EXECUTION_TIME;
                    procedureObj[procedure]["INVOCATION"] += procedureDetails["PROCEDURE_DETAIL"][i].INVOCATIONS;
                }

                if(procedureObj[procedure]["TYPE"] != "Multi Partitioned"
                && procedureDetails["PROCEDURE_DETAIL"][i].PARTITION_ID == 16383){
                    procedureObj[procedure]["TYPE"] = "Multi Partitioned";
                }
            }

            var isSPresent = false;
            var isMPPresent = false;
            var warningMsgList = [];
            $.each(procedureObj, function(key, value){
                var warningMsgObj = {};
                warningMsgObj["PROCEDURE"] = key;
                var avgExecTime = (value["AVG"] / value["COUNT"]) / 1000000;
                var calculatedProcessingTime = (avgExecTime * value["INVOCATION"]/1000);
                var procedureName = key;
                var invocation = value["INVOCATION"];
                var type = value["TYPE"];
                var warningToolTip = '';

                if((procedureName.indexOf("org.voltdb.sysprocs") > -1 && showHideSysProcedures)
                || procedureName.indexOf("org.voltdb.sysprocs") == -1){
                    if(type == "Single Partitioned"){
                        isSPresent = true;
                    } else {
                        isMPPresent = true;
                    }

                    if(averageExecutionTime != undefined && averageExecutionTime != ""){
                        if(avgExecTime > averageExecutionTime){
                            $("#analysisRemarks").show();
                            $("#procedureWarningSection").show();
                            warningMsgObj["MESSAGE"] = "<p>" + procedureName + " has average execution time greater than "+ averageExecutionTime +"ms.</p>"
                            warningToolTip = warningToolTip + "<br/>"+ procedureName + " <br/>has average execution time greater<br/> than "+ averageExecutionTime +"ms.";
                        }
                    }
                }

                warningMsgList.push(warningMsgObj);

                VoltDbAnalysis.procedureValue[procedureName] = {
                    AVG: avgExecTime,
                    INVOCATIONS: invocation,
                    TOTAL_PROCESSING_TIME: calculatedProcessingTime,
                    TYPE:type,
                    WARNING: warningToolTip
                }

                if(procedureName.indexOf("org.voltdb.sysprocs") > -1){
                    dataLatencySysProcedures.push({"label": procedureName , "value": avgExecTime, "index": avgExecTime, "type": type});
                    dataFrequencySysProcedures.push({"label": procedureName, "value": invocation, "index": invocation, "type": type});
                    dataTotalProcessingSysProcedures.push({"label": procedureName, "value": calculatedProcessingTime, "index": calculatedProcessingTime, "type": type});
                } else {
                    dataLatencyProcedures.push({"label": procedureName , "value": avgExecTime, "index": avgExecTime, "type": type});
                    dataFrequencyProcedures.push({"label": procedureName, "value": invocation, "index": invocation, "type": type});
                    dataTotalProcessingProcedures.push({"label": procedureName, "value": calculatedProcessingTime, "index": calculatedProcessingTime, "type": type});
                }
            });

            var formatDate = VoltDbAnalysis.formatDateTime(timestamp);
            $("#analysisDate").html(formatDate);
            $("#analysisDateTable").html(formatDate);
            formatAnalysisLegend(isMPPresent, isSPresent);
            MonitorGraphUI.initializeAnalysisGraph();

            if(showHideSysProcedures){
                dataLatencyProcedures = $.merge(dataLatencyProcedures, dataLatencySysProcedures);
                dataFrequencyProcedures = $.merge(dataFrequencyProcedures, dataFrequencySysProcedures);
                dataTotalProcessingProcedures = $.merge(dataTotalProcessingProcedures, dataTotalProcessingSysProcedures);
            }

            //sort warning message alphabetically in ascending order and display them.
            warningMsgList.sort(function(a,b) {return ((a.PROCEDURE) > (b.PROCEDURE)) ? 1 : (((b.PROCEDURE) > (a.index)) ? -1 : 0);});
            displayWarningMessages(warningMsgList);
            //order the procedure by their respective values.
            dataLatencyProcedures.sort(function(a,b) {return ((b.index) > (a.index)) ? 1 : (((a.index) > (b.index)) ? -1 : 0);});
            dataFrequencyProcedures.sort(function(a,b) {return ((b.index) > (a.index)) ? 1 : (((a.index) > (b.index)) ? -1 : 0);});
            dataTotalProcessingProcedures.sort(function(a,b) {return ((b.index) > (a.index)) ? 1 : (((a.index) > (b.index)) ? -1 : 0);});

            MonitorGraphUI.RefreshAnalysisLatencyGraph(dataLatencyProcedures);
            MonitorGraphUI.RefreshAnalysisFrequencyGraph(dataFrequencyProcedures);
            MonitorGraphUI.RefreshAnalysisProcessingTimeGraph(dataTotalProcessingProcedures);
        });

        VoltDbAnalysis.totalProcessingDetail = {};
        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            var latencyDetails = [];
            procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                var procedureName = item.PROCEDURE;
                var type = "Single Partitioned";
                procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                    if(procedureName == item.PROCEDURE && item.PARTITION_ID == 16383){
                        type = "Multi Partitioned"
                        return false;
                    }
                });

                if(VoltDbAnalysis.combinedDetail[item.PROCEDURE] == undefined){
                    VoltDbAnalysis.combinedDetail[item.PROCEDURE] = [];
                }

                if(item.STATEMENT != "<ALL>"){
                    if(VoltDbAnalysis.totalProcessingDetail[item.PARTITION_ID] == undefined){
                        VoltDbAnalysis.totalProcessingDetail[item.PARTITION_ID] = [];
                    }

                    VoltDbAnalysis.combinedDetail[item.PROCEDURE].push({
                        AVG: item.AVG_EXECUTION_TIME/1000000,
                        INVOCATIONS: item.INVOCATIONS,
                        PARTITION_ID : item.PARTITION_ID,
                        STATEMENT: item.STATEMENT,
                        TIMESTAMP: item.TIMESTAMP,
                        PROCEDURE: item.PROCEDURE,
                        TYPE: type
                    })

                    VoltDbAnalysis.totalProcessingDetail[item.PARTITION_ID].push({
                        AVG: item.AVG_EXECUTION_TIME/1000000,
                        INVOCATIONS: item.INVOCATIONS,
                        PARTITION_ID : item.PARTITION_ID,
                        STATEMENT: item.STATEMENT,
                        TIMESTAMP: item.TIMESTAMP,
                        PROCEDURE: item.PROCEDURE,
                        TYPE: type
                    })

                }

                VoltDbAnalysis.latencyDetail[item.STATEMENT] =
                    {
                        AVG: item.AVG_EXECUTION_TIME/1000000,
                        MIN: item.MIN_EXECUTION_TIME/1000000,
                        MAX: item.MAX_EXECUTION_TIME/1000000,
                        PARTITION_ID: item.PARTITION_ID,
                        INVOCATIONS: item.INVOCATIONS
                    }


                if(item.STATEMENT != "<ALL>"){
                    VoltDbAnalysis.latencyDetailValue.push({"type": type,  "STATEMENT": item.STATEMENT , "value": item.AVG_EXECUTION_TIME/1000000, "PROCEDURE": item.PROCEDURE, "TIMESTAMP": item.TIMESTAMP, "INVOCATION": item.INVOCATIONS, "MIN": item.MIN_EXECUTION_TIME, "MAX": item.MAX_EXECUTION_TIME});
                }
            });

            MonitorGraphUI.initializeFrequencyDetailGraph();
            MonitorGraphUI.initializeProcedureDetailGraph();
            MonitorGraphUI.initializeCombinedDetailGraph();
        });

      voltDbRenderer.GetTableInformationOnly(function(tableDetails){
            if(tableDetails != undefined){
                if(!$.isEmptyObject(tableDetails["TABLES"])){
                    $(".analyzeNowContent").hide();
                    $("#divTabData").show();
                    $("#divNoContentTable").hide();
                } else {
                    $(".analyzeNowContent").hide();
                    $("#divTabData").hide();
                    $("#divNoContentTable").show();
                }
            }
            MonitorGraphUI.initializeAnalysisTableGraph();
            var isReplicated = false;
            var isPartitioned = false;
            var timeStamp;
            var data = {};
            var statementList = [];
            var i =0;
            var partitionDetails = [];
            var orderedDetails = {};

             $.each(tableDetails["TABLES"], function(key, value){
                var tableName = key;
                var tupleCountTotal = value["TUPLE_COUNT"];
                var partition_type = value["PARTITION_TYPE"]
                timeStamp = value["TIMESTAMP"];
                if(value["PARTITION_TYPE"] == "Partitioned"){
                    isPartitioned = true;
                } else {
                    isReplicated = true;
                }

                VoltDbAnalysis.tablePropertyValue[tableName] = {
                    "PARTITION_TYPE": value["PARTITION_TYPE"]
                }

               if(orderedDetails[tableName] == undefined){
                    orderedDetails[tableName] = [];
                    statementList.push(tableName)
               }

               $.each(value["PARTITIONS"], function(key, value1){
                var tupleCount = 0;
                if(orderedDetails[tableName].length != 0 && value["PARTITION_TYPE"] != "Partitioned" ){
                    tupleCount = 0;
                }
                else{
                    tupleCount = value1["tupleCount"];
                }
                orderedDetails[tableName].push({"PARTITION_ID" : value1["partition_id"], "TABLE" : tableName, "TUPLE_COUNT": tupleCount, "type": partition_type, "TOTAL_TUPLE": tupleCountTotal})
               })
             });

             if(statementList.length > 0){
                for(var u=0; u< statementList.length; u++){
                     orderedDetails[statementList[u]].sort(function(a, b) {
                          var nameA = a.TUPLE_COUNT;
                          var nameB = b.TUPLE_COUNT;
                          if (nameA > nameB) {
                            return 1;
                          }
                          if (nameA < nameB) {
                            return -1;
                          }
                          return 0;
                    });
                }
             }

            for(var x=0; x< orderedDetails[statementList[0]].length; x++){
                var u = 0;
                for(var key in orderedDetails){
                    if(partitionDetails[x]== undefined){
                        partitionDetails.push({"key": "Tuple Count"})
                        partitionDetails[x]["values"] = [];
                    }
                    partitionDetails[x]["values"].push({"PARTITION_ID": orderedDetails[key][x].PARTITION_ID,  "x": orderedDetails[key][x].TABLE, "y": orderedDetails[key][x].TUPLE_COUNT, "z": orderedDetails[key][x].TOTAL_TUPLE})
                }
                u++;
            }

            formatAnalysisTableLegend(isPartitioned, isReplicated)
            var formatTableDate = VoltDbAnalysis.formatDateTime(timeStamp);
            $("#analysisDateTable").html(formatTableDate);
            setTimeout(function(){
                MonitorGraphUI.RefreshAnalysisTableGraph(partitionDetails);
            }, 100)

        })
    }

    $("#btnAnalyzeNow").on("click", function(){
        fetchData();
    })
}

(function(window) {
    iVoltDbAnalysis = (function(){
        this.procedureValue = {};
        this.tablePropertyValue = {};
        this.latencyDetailValue = [];
        this.latencyDetail = {};
        this.combinedDetail = {};
        this.partitionStatus = "SP"
        this.latencyDetailTest = {};
        this.currentTab = "Average Execution Time";
        this.totalProcessingDetail = {};
        this.formatDateTime = function(timestamp) {
            var dateTime = new Date(timestamp);
            //get date
            var days = dateTime.getDate();
            var months = dateTime.getMonth() + 1;
            var years = dateTime.getFullYear();

            days = days < 10 ? "0" + days : days;
            months = months < 10 ? "0" + months : months;

            //get time
            var timePeriod = "AM"
            var hours = dateTime.getHours();
            var minutes = dateTime.getMinutes();
            var seconds = dateTime.getSeconds();

            timePeriod = hours >= 12 ? 'PM' : 'AM';
            hours = hours % 12;
            hours = hours ? hours : 12;
            hours = hours < 10 ? "0" + hours : hours
            minutes = minutes < 10 ? "0" + minutes : minutes;
            seconds = seconds < 10 ? "0" + seconds : seconds;

            //get final date time
            var date = months + "/" + days + "/" + years;
            var time = hours + ":" + minutes + ":" + seconds + " " + timePeriod;
            return date + " " + time;
        };

        this.refreshChart= function(){
            setTimeout(function(){
                window.dispatchEvent(new Event('resize'));
            },200)
        }

        this.setDefaultAnalysisSettings = function(){
            if(VoltDbUI.getFromLocalStorage("averageExecutionTime") == undefined){
                saveInLocalStorage("averageExecutionTime", 500)
            }
            if(VoltDbUI.getFromLocalStorage("showHideSysProcedures") == undefined){
                saveInLocalStorage("showHideSysProcedures", false)
            }
        }
    });


    window.VoltDbAnalysis = new iVoltDbAnalysis();
})(window);