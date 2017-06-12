

function fetchData (){

  voltDbRenderer.GetProceduresInfo(function (procDetails) {
        debugger;
        var data = [];

        data[0] ={};
        data[0]["key"] = "Latency",
        data[0]["color"] = "#d62728",
        data[0]["values"] = [];

         procDetails.sort(function(a, b) {
    return parseFloat(b.AVG_LATENCY) - parseFloat(a.AVG_LATENCY);
});
        procDetails.forEach (function(item){
            //order items w.r.to latency

            data[0]["values"].push({"label": item.PROCEDURE , "value": item.AVG_LATENCY})
        });

        nv.addGraph(function() {
          var chart = nv.models.multiBarHorizontalChart()
              .x(function(d) { return d.label })
              .y(function(d) { return d.value })
              .margin({top: 30, right: 20, bottom: 50, left: 175})
              .showValues(true)
              .tooltips(false)
              .showControls(false);

          chart.yAxis
              .tickFormat(d3.format(',.2f'));

          d3.select('#chart svg')
              .datum(data)
            .transition().duration(500)
              .call(chart);

          nv.utils.windowResize(chart.update);

          return chart;
        });

    });

}