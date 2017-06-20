var latencyDetails = [];
function loadAnalysisPage(){
    $("#tabProcedureBtn").trigger("click");
    $("#tabAnalysis li a").on("click", function(){
        VoltDbAnalysis.refreshChart();
    })

    $("#ulProcedure li a").on("click", function(){
        if($($(this)[0]).text() == "Frequency"){
            $("#spanAnalysisLegend").html("Frequency");
        } else if($($(this)[0]).text() == "Combined"){
            $("#spanAnalysisLegend").html("Combined");
        } else {
            $("#spanAnalysisLegend").html("Execution Time");
        }
        VoltDbAnalysis.refreshChart()
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

    function fetchData (){
        $("#analysisLoader").show();
        VoltDbAnalysis.refreshChart();
        voltDbRenderer.GetProcedureProfileInformation(function(profileData){
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
                $("#tblAnalyzeNowContent").hide();
                $("#tblNoDataContent").show();
            }
            $("#analysisLoader").hide();
            //order the procedure by  their (avg_exec_time * #of invocation) value
            profileData["PROCEDURE_PROFILE"].sort(function(a,b) {return ((b.AVG * b.INVOCATIONS) > (a.AVG * a.INVOCATIONS)) ? 1 : (((a.AVG * a.INVOCATIONS) > (b.AVG * b.INVOCATIONS)) ? -1 : 0);} );
            var containLongName = checkObjForLongProcedureName(profileData["PROCEDURE_PROFILE"])
            var dataLatency = [];
            var dataFrequency = [];
            var dataCombined = [];
            var timestamp;
            var sumOfAllProcedure = calculateCombinedValue(profileData["PROCEDURE_PROFILE"])
            for(var i = 0; i < profileData["PROCEDURE_PROFILE"].length; i++){
                if(i == 0)
                    timestamp = profileData["PROCEDURE_PROFILE"][i].TIMESTAMP;

                var combinedWeight = (((profileData["PROCEDURE_PROFILE"][i].AVG/1000000) * profileData["PROCEDURE_PROFILE"][i].INVOCATIONS)/sumOfAllProcedure) * 100;
                var procedureName = profileData["PROCEDURE_PROFILE"][i].PROCEDURE;
                if(containLongName)
                    procedureName = "(" + (i + 1) + ") " + profileData["PROCEDURE_PROFILE"][i].PROCEDURE;

                VoltDbAnalysis.procedureValue[procedureName] =
                    {
                        AVG: profileData["PROCEDURE_PROFILE"][i].AVG/1000000,
                        INVOCATIONS: profileData["PROCEDURE_PROFILE"][i].INVOCATIONS,
                        COMBINED: combinedWeight
                    }

                dataLatency.push({"label": procedureName , "value": profileData["PROCEDURE_PROFILE"][i].AVG/1000000})
                dataFrequency.push({"label": procedureName, "value": profileData["PROCEDURE_PROFILE"][i].INVOCATIONS})
                dataCombined.push({"label": procedureName, "value": combinedWeight})
            }
            var formatDate = VoltDbAnalysis.formatDateTime(timestamp);
            $("#analysisDate").html(formatDate);
            MonitorGraphUI.initializeAnalysisGraph();
            MonitorGraphUI.RefreshAnalysisLatencyGraph(dataLatency);
            MonitorGraphUI.RefreshAnalysisFrequencyGraph(dataFrequency);
            MonitorGraphUI.RefreshAnalysisCombinedGraph(dataCombined);
        })

        voltDbRenderer.GetProcedureDetailInformation(function (procedureDetails){
            var latencyDetails = [];
            procedureDetails["PROCEDURE_DETAIL"].sort(function(a, b) {
                return parseFloat(b.AVG_EXECUTION_TIME) - parseFloat(a.AVG_EXECUTION_TIME);
            });
            procedureDetails["PROCEDURE_DETAIL"].forEach (function(item){
                VoltDbAnalysis.latencyDetail[item.STATEMENT + '(' + item.PARTITION_ID + ')'] =
                    {
                        AVG: item.AVG_EXECUTION_TIME,
                        MIN: item.MIN_EXECUTION_TIME,
                        MAX: item.MAX_EXECUTION_TIME
                    }

                VoltDbAnalysis.latencyDetailValue.push({"label": item.STATEMENT + '(' + item.PARTITION_ID + ')' , "value": item.AVG_EXECUTION_TIME, "PROCEDURE": item.PROCEDURE, "TIMESTAMP": item.TIMESTAMP});
            });

            MonitorGraphUI.initializeProcedureDetailGraph();
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