var latencyDetails = [];
function loadAnalysisPage(){
    $("#tabProcedureBtn").trigger("click");
    $("#tabAnalysis li a").on("click", function(){
        VoltDbAnalysis.refreshChart();
    })

    $("#ulProcedure li a").on("click", function(){
        if($($(this)[0]).text() == "Frequency"){
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Frequency(" : "Frequency");
        } else if($($(this)[0]).text() == "Combined"){
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Combined(" : "Combined");
        } else {
            $(".spnAnalysisLegend").html(VoltDbAnalysis.partitionStatus == "both" ?"Execution Time(" : "Execution Time");
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
                var dataLatency = [];
                var dataFrequency = [];
                var dataCombined = [];
                var timestamp;
                var sumOfAllProcedure = calculateCombinedValue(profileData["PROCEDURE_PROFILE"])
                var isMPPresent = false;
                var isPPresent = false;
                var partitionThreshold = VoltDbUI.getFromLocalStorage("usagePercentage");
                var frequencyThreshold = VoltDbUI.getFromLocalStorage("frequencyForProc");

                if(partitionThreshold == undefined){
                    partitionThreshold = 20;
                }
                for(var i = 0; i < profileData["PROCEDURE_PROFILE"].length; i++){
                    if(i == 0)
                        timestamp = profileData["PROCEDURE_PROFILE"][i].TIMESTAMP;

                    var combinedWeight = (((profileData["PROCEDURE_PROFILE"][i].AVG/1000000) * profileData["PROCEDURE_PROFILE"][i].INVOCATIONS)/sumOfAllProcedure) * 100;
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

                    if(combinedWeight > partitionThreshold) {
                        $("#analysisRemarks").show();
                        $("#procedureWarningSection").show();
                        warningString = "<p>" + procedureName.split(" ")[1] + " has combined usage greater than "+ partitionThreshold +"%.</p>";
                        warningToolTip = procedureName.split(" ")[1] + " <br> has combined usage greater <br> than "+ partitionThreshold +"%.";
                    }

                    if(frequencyThreshold != undefined){
                        if(invocation > frequencyThreshold ){
                            $("#analysisRemarks").show();
                            $("#procedureWarningSection").show();
                            warningString = warningString + "<p>" + procedureName.split(" ")[1] + " has frequency greater than "+ frequencyThreshold +".</p>"
                            warningToolTip = warningToolTip + "<br>"+ procedureName.split(" ")[1] + " <br> has frequency greater than <br> than "+ frequencyThreshold +".";
    //                        $("#procedureWarning").append("<p>" + procedureName.split(" ")[1] + " has frequency greater than "+ frequencyThreshold +"%.</p>");
                        }
                    }

                    $("#procedureWarning").append(warningString);


                    VoltDbAnalysis.procedureValue[procedureName] =
                        {
                            AVG: avgExecTime,
                            INVOCATIONS: invocation,
                            COMBINED: combinedWeight,
                            TYPE:type,
                            WEIGHTED_PERC: wtPercentage,
                            WARNING: warningToolTip
                        }
                    dataLatency.push({"label": procedureName , "value": avgExecTime})
                    dataFrequency.push({"label": procedureName, "value": invocation})
                    dataCombined.push({"label": procedureName, "value": combinedWeight})
                }
                var formatDate = VoltDbAnalysis.formatDateTime(timestamp);
                $("#analysisDate").html(formatDate);
                formatAnalysisLegend(isMPPresent, isPPresent);
                MonitorGraphUI.initializeAnalysisGraph();
                MonitorGraphUI.RefreshAnalysisLatencyGraph(dataLatency);
                MonitorGraphUI.RefreshAnalysisFrequencyGraph(dataFrequency);
                MonitorGraphUI.RefreshAnalysisCombinedGraph(dataCombined);
            })
        })

        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            var latencyDetails = [];
            procedureDetails["PROCEDURE_DETAIL"].sort(function(a, b) {
                return parseFloat(b.AVG_EXECUTION_TIME) - parseFloat(a.AVG_EXECUTION_TIME);
            });
            procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                if(VoltDbAnalysis.combinedDetail[item.PROCEDURE + '(' + item.STATEMENT + ')'] == undefined){
                    VoltDbAnalysis.combinedDetail[item.PROCEDURE + '(' + item.STATEMENT + ')'] = [];
                }

                VoltDbAnalysis.combinedDetail[item.PROCEDURE + '(' + item.STATEMENT + ')'].push({
                    AVG: item.AVG_EXECUTION_TIME/1000000,
                    INVOCATIONS: item.INVOCATIONS,
                    PARTITION_ID : item.PARTITION_ID,
                    STATEMENT: item.STATEMENT,
                    TIMESTAMP: item.TIMESTAMP
                })
                VoltDbAnalysis.latencyDetail[item.STATEMENT + '(' + item.PARTITION_ID + ')'] =
                    {
                        AVG: item.AVG_EXECUTION_TIME/1000000,
                        MIN: item.MIN_EXECUTION_TIME/1000000,
                        MAX: item.MAX_EXECUTION_TIME/1000000,
                        PARTITION_ID: item.PARTITION_ID,
                        INVOCATIONS: item.INVOCATIONS
                    }

                VoltDbAnalysis.latencyDetailValue.push({"label": item.STATEMENT + '(' + item.PARTITION_ID + ')' , "value": item.AVG_EXECUTION_TIME/1000000, "PROCEDURE": item.PROCEDURE, "TIMESTAMP": item.TIMESTAMP, "INVOCATION": item.INVOCATIONS});
            });

            MonitorGraphUI.initializeFrequencyDetailGraph();
            MonitorGraphUI.initializeProcedureDetailGraph();
            MonitorGraphUI.initializeCombinedDetailGraph();
        });

    }

    $("#btnAnalyzeNow").on("click", function(){
        if(VoltDbUI.getFromLocalStorage("usagePercentage") == undefined){
           saveInLocalStorage("usagePercentage", 20)
        }
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
    });


    window.VoltDbAnalysis = new iVoltDbAnalysis();
})(window);