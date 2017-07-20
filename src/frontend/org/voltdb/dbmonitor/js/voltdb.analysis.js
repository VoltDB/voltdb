var latencyDetails = [];
function loadAnalysisPage(){
    VoltDbAnalysis.setDefaultAnalysisSettings();

    $("#tabProcedureBtn").trigger("click");
    $("#tabAnalysis li a").on("click", function(){
        VoltDbAnalysis.refreshChart();
    })

    $("#ulProcedure li a").on("click", function(){
        if($($(this)[0]).text() == "Frequency"){
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Frequency(" : "Frequency");
        } else if($($(this)[0]).text() == "Total Processing Time"){
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Total Processing Time(" : "Total Processing Time");
        } else {
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Average Execution Time(" : "Average Execution Time");
        }
        //this method is called twice to ensure graph reloads properly
        VoltDbAnalysis.refreshChart();
        VoltDbAnalysis.refreshChart();
    })

    function calculateCombinedValue(profileData){
        var totalValue = 0;
        for(var j = 0; j < profileData.length; j++){
            totalValue += (profileData[j].AVG/1000000) * profileData[j].INVOCATIONS;
        }
        return totalValue;
    }

    function checkObjForLongProcedureName(profileData){
        for(var j = 0; j < profileData.length; j++){
            if(profileData[j].PROCEDURE.length > 28){
                return true;
            }
        }
        return false;
    }

    function checkObjForLongStatementName(profileData){
        for(var j = 0; j < profileData.length; j++){
            if(profileData[j].STATEMENT.length > 14){
                return true;
            }
        }
        return false;
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
    }

    function fetchData (){
        $("#analysisLoader").show();
        $("#analysisRemarks").hide();
        $("#procedureWarning").html("");
        $("#tableWarning").html("");
        VoltDbAnalysis.refreshChart();
        voltDbRenderer.GetProcedureProfileInformation(function(profileData){
            voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
                if(profileData != undefined){
                    if(!$.isEmptyObject(profileData["PROCEDURE_PROFILE"])){
                        $(".analyzeNowContent").hide();
                        $(".dataContent").show();
                        $(".noDataContent").hide();
                    } else {
                        $(".mainContentAnalysis").hide();
                        $(".dataContent").hide();
                        $(".noDataContent").show();

                    }
                    //For data section
                    //$("#tblAnalyzeNowContent").hide();
                    //$("#tblNoDataContent").show();
                }
                $("#analysisLoader").hide();
                VoltDbAnalysis.proceduresCount = profileData["PROCEDURE_PROFILE"].length;
                //order the procedure by  their (avg_exec_time * #of invocation) value
                profileData["PROCEDURE_PROFILE"].sort(function(a,b) {return ((b.AVG * b.INVOCATIONS) > (a.AVG * a.INVOCATIONS)) ? 1 : (((a.AVG * a.INVOCATIONS) > (b.AVG * b.INVOCATIONS)) ? -1 : 0);} );
                var containLongName = checkObjForLongProcedureName(profileData["PROCEDURE_PROFILE"])
                var dataLatencyProcedures = [];
                var dataLatencySysProcedures = [];
                var dataFrequencySysProcedures = [];
                var dataFrequencyProcedures = [];
                var dataTotalProcessingProcedures = [];
                var dataTotalProcessingSysProcedures = [];
                var timestamp;
                var sumOfAllProcedure = calculateCombinedValue(profileData["PROCEDURE_PROFILE"])
                var isMPPresent = false;
                var isPPresent = false;
                var totalProcessingTime = VoltDbUI.getFromLocalStorage("totalProcessingTime");
                var averageExecutionTime = VoltDbUI.getFromLocalStorage("averageExecutionTime");
                var showHideSysProcedures = VoltDbUI.getFromLocalStorage("showHideSysProcedures");

                for(var i = 0; i < profileData["PROCEDURE_PROFILE"].length; i++){
                    if(i == 0)
                        timestamp = profileData["PROCEDURE_PROFILE"][i].TIMESTAMP;
                    var calculatedProcessingTime = (profileData["PROCEDURE_PROFILE"][i].AVG/1000000) * profileData["PROCEDURE_PROFILE"][i].INVOCATIONS;
                    var procedureName = profileData["PROCEDURE_PROFILE"][i].PROCEDURE;
                    var avgExecTime = profileData["PROCEDURE_PROFILE"][i].AVG / 1000000;
                    var invocation = profileData["PROCEDURE_PROFILE"][i].INVOCATIONS;
                    var wtPercentage = profileData["PROCEDURE_PROFILE"][i].WEIGHTED_PERC;
                    //find procedure type
                    var type = "Single Partitioned";
                    procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                        if(procedureName == item.PROCEDURE && item.PARTITION_ID == 16383){
                            type = "Multi Partitioned"
                            return false;
                        }
                    });

                    if(containLongName)
                        procedureName =(i + 1) + ") " + procedureName;

                    if(type == "Multi Partitioned")
                        isMPPresent = true;
                    else
                        isPPresent = true;

                    var warningString = '';
                    var warningToolTip = '';

                    if(calculatedProcessingTime > totalProcessingTime && totalProcessingTime != "") {
                        $("#analysisRemarks").show();
                        $("#procedureWarningSection").show();
                        warningString = "<p>" + procedureName.split(" ")[1] + " has total processing time greater than "+ totalProcessingTime +"ms.</p>";
                        warningToolTip = procedureName.split(" ")[1] + " <br> has total processing time greater <br> than "+ totalProcessingTime +"ms.";
                    }

                    if(averageExecutionTime != undefined && averageExecutionTime != ""){
                        if(avgExecTime > averageExecutionTime){
                            $("#analysisRemarks").show();
                            $("#procedureWarningSection").show();
                            warningString = warningString + "<p>" + procedureName.split(" ")[1] + " has average execution time greater than "+ averageExecutionTime +"ms.</p>"
                            warningToolTip = warningToolTip + "<br/>"+ procedureName.split(" ")[1] + " <br/>has average execution time greater<br/> than "+ averageExecutionTime +"ms.";
                        }
                    }

                    $("#procedureWarning").append(warningString);

                    VoltDbAnalysis.procedureValue[procedureName] =
                        {
                            AVG: avgExecTime,
                            INVOCATIONS: invocation,
                            TOTAL_PROCESSING_TIME: calculatedProcessingTime,
                            TYPE:type,
                            WEIGHTED_PERC: wtPercentage,
                            WARNING: warningToolTip
                        }
                    if(procedureName.indexOf("org.voltdb.sysprocs") > -1){
                        dataLatencySysProcedures.push({"label": procedureName , "value": avgExecTime});
                        dataFrequencySysProcedures.push({"label": procedureName, "value": invocation});
                        dataTotalProcessingSysProcedures.push({"label": procedureName, "value": calculatedProcessingTime});
                    } else {
                        dataLatencyProcedures.push({"label": procedureName , "value": avgExecTime});
                        dataFrequencyProcedures.push({"label": procedureName, "value": invocation});
                        dataTotalProcessingProcedures.push({"label": procedureName, "value": calculatedProcessingTime});
                    }
                }
                var formatDate = VoltDbAnalysis.formatDateTime(timestamp);
                $("#analysisDate").html(formatDate);
                formatAnalysisLegend(isMPPresent, isPPresent);
                MonitorGraphUI.initializeAnalysisGraph();
                if(showHideSysProcedures){
                    dataLatencyProcedures = $.merge(dataLatencyProcedures, dataLatencySysProcedures);
                    dataFrequencyProcedures = $.merge(dataFrequencyProcedures, dataFrequencySysProcedures);
                    dataTotalProcessingProcedures = $.merge(dataTotalProcessingProcedures, dataTotalProcessingSysProcedures);
                }
                MonitorGraphUI.RefreshAnalysisLatencyGraph(dataLatencyProcedures);
                MonitorGraphUI.RefreshAnalysisFrequencyGraph(dataFrequencyProcedures);
                MonitorGraphUI.RefreshAnalysisCombinedGraph(dataTotalProcessingProcedures);
            })
        })

        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            var latencyDetails = [];
//            procedureDetails["PROCEDURE_DETAIL"].sort(function(a, b) {
//                return parseFloat(b.AVG_EXECUTION_TIME) - parseFloat(a.AVG_EXECUTION_TIME);
//            });


             //find procedure type

            procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                var type = "Single Partitioned";
                if(procedureName == item.PROCEDURE && item.PARTITION_ID == 16383){
                    type = "Multi Partitioned"
                    return false;
                }

                if(VoltDbAnalysis.combinedDetail[item.PROCEDURE] == undefined){
                    VoltDbAnalysis.combinedDetail[item.PROCEDURE] = [];
                }

                if(item.STATEMENT != "<ALL>"){
                    VoltDbAnalysis.combinedDetail[item.PROCEDURE].push({
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

    }

    $("#btnAnalyzeNow").on("click", function(){
        fetchData();
    })
}

(function(window) {
    iVoltDbAnalysis = (function(){
        this.procedureValue = {};
        this.latencyDetailValue = [];
        this.latencyDetail = {};
        this.combinedDetail = {};
        this.partitionStatus = "SP"
        this.proceduresCount = 0;
        this.latencyDetailTest = {};
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
            if(VoltDbUI.getFromLocalStorage("totalProcessingTime") == undefined){
               saveInLocalStorage("totalProcessingTime", 3000)
            }
            if(VoltDbUI.getFromLocalStorage("averageExecutionTime") == undefined){
                saveInLocalStorage("averageExecutionTime", 500)
            }
            if(VoltDbUI.getFromLocalStorage("showHideSysProcedures") == undefined){
                saveInLocalStorage("showHideSysProcedures", true)
            }
        }
    });


    window.VoltDbAnalysis = new iVoltDbAnalysis();
})(window);