function loadAnalysisPage(){
    function fetchData (){
     $("#tabAnalysis li a").on("click", function(){
        setInterval(function(){
            window.dispatchEvent(new Event('resize'));
        },200)

    })

    voltDbRenderer.GetProceduresInfo(function (procedureDetails) {
        if(procedureDetails != undefined){
            if(!$.isEmptyObject(procedureDetails)){
                $(".analyzeNowContent").hide();
                $(".dataContent").show();
            } else {
                $(".mainContentAnalysis").hide();
                $(".noDataContent").hide();
                $(".noDataContent").show();

            }
            $("#tblAnalyzeNowContent").hide();
            $("#tblNoDataContent").show();

        }
        var dataLatency = [];
        procedureDetails.sort(function(a, b) {
            return parseFloat(b.AVG_LATENCY) - parseFloat(a.AVG_LATENCY);
        });
        procedureDetails.forEach (function(item){
            //order items w.r.to latency
            var latValue;
            VoltDbAnalysis.procedureValue[item.PROCEDURE] = {AVG_LATENCY: item.AVG_LATENCY, INVOCATIONS: item.INVOCATIONS}

            dataLatency.push({"label": item.PROCEDURE , "value": item.AVG_LATENCY})
        });
        MonitorGraphUI.initializeLatencyAnalysis();
        MonitorGraphUI.RefreshLatencyAnalysis(dataLatency);

    });

}

    $("#btnAnalyzeNow").on("click", function(){
        fetchData();
    })
}

(function(window) {
    iVoltDbAnalysis = (function(){
        this.procedureValue = {}
    });
    window.VoltDbAnalysis = new iVoltDbAnalysis();
})(window);