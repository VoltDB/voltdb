(function( window, undefined ){

var IMonitorUI = (function(){

this.Speed = 'pau';
this.Interval = null;
this.Monitors = {};

function InitializeChart(id, chart, metric)
{
	$('#'+chart+'chart-'+id).empty();
	var data = MonitorUI.Monitors[id][metric+'Data'];
	var max = MonitorUI.Monitors[id][metric+'Max'];
	var tickValues = MonitorUI.Monitors[id].tickValues;
	var opt = null;
	switch(metric)
	{
		case 'lat':
		    opt = {
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:'%.2f'} } },
		    	series: [{showMarker:false, color:'Lime', yaxis:'y2axis', lineWidth: 1.5, shadow: false}],
		    	grid: { shadow:false, background:'#000', borderWidth: 1, borderColor: 'DarkGreen', gridLineColor:'DarkGreen'}
		    };
			break;
		case 'tps':
		    opt = {
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:"%'d"} } },
		    	series: [{showMarker:false, color:'Lime', yaxis:'y2axis', lineWidth: 1.5, shadow: false}],
		    	grid: { shadow:false, background:'#000', borderWidth: 1, borderColor: 'DarkGreen', gridLineColor:'DarkGreen'}
		    };
			break;
		case 'mem':
		    opt = {
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:'%.2f'} } },
		    	series: [{showMarker:false, color:'Lime', yaxis:'y2axis', lineWidth: 1.5, shadow: false}],
		    	grid: { shadow:false, background:'#000', borderWidth: 1, borderColor: 'DarkGreen', gridLineColor:'DarkGreen'}
		    };
			break;
		case 'str':
			var siteCount = VoltDB.GetConnection(id.substr(2)).Metadata['siteCount'];
			var seriesStr = [];
		    for(var j=0;j<siteCount;j++)
		    	seriesStr.push({showMarker:false, yaxis:'y2axis', lineWidth: 2, shadow: false, label: (j+1)});
		    opt = {
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:"%.2f"} } },
		    	series: seriesStr,
		    	grid: { shadow:false, background:'#000', borderWidth: 1, borderColor: 'DarkGreen', gridLineColor:'DarkGreen'},
		    	legend: {show: true, location: 'sw', placement: 'insideGrid', renderer: $.jqplot.EnhancedLegendRenderer, rendererOptions: {numberRows:1} }
		    };
			break;
	}
	
    var plot = $.jqplot(chart+'chart-'+id,data,opt);
    
    MonitorUI.Monitors[id][chart+'Plot'] = plot;
}
this.ChangeChartMetric = function(id,chart,metric)
{
	MonitorUI.Monitors[id][chart+'Metric'] = metric;
	InitializeChart(id, chart, metric);
}
this.AddMonitor = function(tab)
{
	var id = $(tab).attr('id');
	var partitionCount = VoltDB.GetConnection(id.substr(2)).Metadata['partitionCount'];
	var siteCount = VoltDB.GetConnection(id.substr(2)).Metadata['siteCount'];
	
	var data = [];
    for(var i = 0;i<121;i++)
    	data[i] = [i,0];
	
    var tickValues = [];
    for(var i=0;i<121;i+=6)
    	tickValues.push(i);
    	
	var dataStr = [];
    for(var j=0;j<siteCount;j++)
    	dataStr.push(data);
    
    MonitorUI.Monitors[id] = { 'id': id
    , 'tab': tab
    , 'leftPlot': null //lplot
    , 'rightPlot': null //rplot
    , 'bottomPlot': null //bplot
    , 'memStatsCallback': function(response) {MonitorUI.Monitors[id].memStatsResponse = response;}
    , 'procStatsCallback': function(response) {MonitorUI.Monitors[id].procStatsResponse = response;}
    , 'starvStatsCallback': function(response) {MonitorUI.Monitors[id].starvStatsResponse = response;}
    , 'memStatsResponse': null
    , 'procStatsResponse': null
    , 'starvStatsResponse': null
    , 'lastTransactionCount': -1
    , 'lastTimedTransactionCount': -1
    , 'lastLatencyAverage': 0.0
    , 'lastTimerTick': -1
    , 'leftMetric': 'lat'
    , 'rightMetric': 'tps'
    , 'latData': [data]
    , 'tpsData': [data]
    , 'memData': [data]
    , 'strData': dataStr
    , 'latMax': 1
    , 'tpsMax': 1
    , 'memMax': 1
    , 'strMax': 100
    , 'tickValues': tickValues
    , 'partitionCount': partitionCount
    , 'siteCount': siteCount
    };

    InitializeChart(id, 'left', 'lat');
    InitializeChart(id, 'right', 'tps');

    if(MonitorUI.Interval == null)
    	MonitorUI.RefreshMonitorData(id);
	MonitorUI.SetRefreshSpeed();
}
this.RemoveMonitor = function(id)
{
    if (!(id in MonitorUI.Monitors))
        return;
	$(MonitorUI.Monitors[id].tab.find('.monitoritemselection')).attr('disabled','disabled');
	$('a[href=#' + id + ']').text('Monitor: Disconnected');
	delete MonitorUI.Monitors[id];
	MonitorUI.SetRefreshSpeed();
}
this.SetRefreshSpeed = function()
{
	var speed = $('#'+$('#monitor-speed label[aria-pressed=true]').attr('for'))[0].value;
	if (Object.size(MonitorUI.Monitors) == 0)
	{
		if (MonitorUI.Interval != null)
		{
			clearInterval(MonitorUI.Interval);
			MonitorUI.Interval = null;
		}
	}
	else
	{
		if (speed != MonitorUI.Speed)
		{
			if (MonitorUI.Interval != null)
			{
				clearInterval(MonitorUI.Interval);
				MonitorUI.Interval = null;
			}
		}
		if ((speed != 'pau') && (MonitorUI.Interval == null))
			MonitorUI.Interval = setInterval(function() {MonitorUI.RefreshData();}, (speed == 'hig' ? 1 : (speed == 'nor' ? 2 : 10))*500);
		MonitorUI.Speed = speed;
	}
}
this.Redraw = function()
{
	for(var id in this.Monitors)
	{
        try
        {
    		MonitorUI.Monitors[id].leftPlot.replot();
	    	MonitorUI.Monitors[id].rightPlot.replot();
        } catch(x) {} // Will happen if charts are hidden
	}
}

this.RefreshMonitorData = function(id)
{
	if (id in MonitorUI.Monitors)
    {
		var connection = VoltDB.GetConnection(id.substring(2));
		if (connection == null)
		{
			MonitorUI.RemoveMonitor(id);
			return;
		}
		connection.getQueue().Start()
                			.BeginExecute('@Statistics', ['MEMORY', 0], MonitorUI.Monitors[id].memStatsCallback)
    			            .BeginExecute('@Statistics', ['PROCEDUREPROFILE', 0], MonitorUI.Monitors[id].procStatsCallback)
    			            .BeginExecute('@Statistics', ['STARVATION', 1], MonitorUI.Monitors[id].starvStatsCallback)
    	                	.End(MonitorUI.RefreshMonitor, id);
    }
}
this.RefreshData = function()
{
	for(var id in MonitorUI.Monitors)
        this.RefreshMonitorData(id);
}

this.RefreshMonitor = function(id, Success)
{
    if (!(id in MonitorUI.Monitors))
        return;

    var monitor = MonitorUI.Monitors[id];

	if (!Success)
	{
		$(monitor.tab.find('.monitoritemselection')).attr('disabled','disabled');
		MonitorUI.RemoveMonitor(id);
		return;
	}
	if (!(id in MonitorUI.Monitors))
		return;

	if ((monitor.starvStatsResponse == null) || (monitor.strData == null))
        return;

	var currentTimerTick = (new Date()).getTime();
	var latData = monitor.latData;
	var tpsData = monitor.tpsData;
	var memData = monitor.memData;
	var strData = monitor.strData;
	
	var dataMem = memData[0];
	var dataLat = latData[0];
	var dataTPS = tpsData[0];
	var dataIdx  = dataMem[dataMem.length-1][0]+1;
	var Mem = 0;
	// Compute the memory statistics
	var table = monitor.memStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
		Mem += table[j][3]*1.0/1048576.0;
	dataMem = dataMem.slice(1);
	dataMem.push([dataIdx, Mem]);
	var Lat = 0;
	var TPS = 0;
	var procStats = {};
    // Compute procedure statistics 
	table = monitor.procStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
	{
		var srcData = table[j];
		var data = null;
		if (srcData[1] in procStats)
		{
			data = procStats[srcData[1]];
			data[1] = srcData[3]; // invocations
			data[2] = srcData[2]; // %
			data[3] = srcData[5]; // min latency
            data[4] = srcData[4]; // ave latency
            data[5] = srcData[6]; // max latency
		}
		else
			data = [srcData[1], srcData[3], srcData[2], srcData[5], srcData[4], srcData[6]];
		procStats[srcData[1]] = data;
	}
    // Compute memory usage 
	table = monitor.starvStatsResponse.results[0].data;
	var starvStats = {}
	for(var j=0;j<table.length;j++)
	{
		var data = null;
		if(table[j][3] in starvStats)
		{
			data = starvStats[table[j][3]];
			data[0] = table[j][5];
			data[1] += 1.0;
		}
		else
			data = [table[j][5],1.0];
		starvStats[table[j][3]] = data;
	}
	// Compute latency 
	var currentTimedTransactionCount = 0.0;
    var currentLatencySum = 0.0;
    var currentLatencyAverage = 0.0;
	for(var proc in procStats)
	{
		currentTimedTransactionCount += procStats[proc][1];
		currentLatencySum += procStats[proc][1]*procStats[proc][4];
		currentLatencyAverage += procStats[proc][4];
	}
	// Compute initial latency averge. We'll compute the delta average next.
	currentLatencyAverage = currentLatencySum / currentTimedTransactionCount;
	
	if (monitor.lastTransactionCount > 0 && monitor.lastTimerTick > 0)
	{
		var delta = currentTimedTransactionCount - monitor.lastTransactionCount;
		dataTPS = dataTPS.slice(1);
		dataTPS.push([dataIdx, delta*1000.0 / (currentTimerTick - monitor.lastTimerTick)]);
		dataLat = dataLat.slice(1);
		if (currentTimedTransactionCount == monitor.lastTimedTransactionCount)
		{
			if (delta < 10)
				dataLat.push([dataIdx,0]);
			else
				dataLat.push([dataIdx,currentLatencyAverage/1000000.0]);
		}
		else
		{
		    // Compute delta latency
		    var latency_val = currentLatencySum - (monitor.lastLatencyAverage * monitor.lastTimedTransactionCount);
		    var delta_latency = latency_val / delta;
			dataLat.push([dataIdx,delta_latency/1000000.0]);
        }
	}
	// Update procedure statistics table
	if ($('#stats-' + id + ' tbody tr').size() == Object.size(procStats))
	{
		var rows = $('#stats-' + id + ' tbody tr');
		for(var j in procStats)
		{
			for(var k = 0;k<rows.size();k++)
			{
				var cells = $(rows[k]).find('td');
				if ($(cells[0]).text() == procStats[j][0])
				{
                    $(cells[1]).text(procStats[j][1]);
                    $(cells[2]).text(procStats[j][2]);
					$(cells[3]).text((Math.round(procStats[j][3]/10.0/1000.0)/100.0));
					$(cells[4]).text((Math.round(procStats[j][4]/10.0/1000.0)/100.0));
					$(cells[5]).text((Math.round(procStats[j][5]/10.0/1000.0)/100.0));
				}
			}
		}
		
	}
	else
	{
		var src = '<table id="stats-' + id + '" class="sortable tablesorter statstable" border="0" cellpadding="0" cellspacing="1"><thead class="ui-widget-header noborder"><tr>';
		src += '<th>Procedure</th><th>Invocations</th><th>Percent (%)</th><th>Min (ms)</th><th>Avg (ms)</th><th>Max (ms)</th>';
		src += '</tr></thead><tbody>';
		for(var j in procStats)
		{
			src += '<tr>';
            src += '<td align="left">' + procStats[j][0] + '</td>';
			src += '<td align="right">' + procStats[j][1] + '</td>';
            src += '<td align="right">'  + procStats[j][2] + '</td>';
			for(var k = 3; k < 6; k++)
				src += '<td align="right">' + (Math.round(procStats[j][k]/10.0/1000.00)/100.0) + '</td>';
			src += '</tr>';
		}
		src += '</tbody></table>';
		$(monitor.tab.find('.tablebar')).html(src);
	}
	sorttable.makeSortable(document.getElementById('stats-' + id));
	
	monitor.lastTransactionCount = currentTimedTransactionCount;
	monitor.lastTimedTransactionCount = currentTimedTransactionCount;
	monitor.lastLatencyAverage = currentLatencyAverage;
	
    var keys = [];
	for(var k in starvStats)
        keys.push(k);
    keys.sort();

    for(var k=0;k<keys.length;k++)
    {
		var dataStarv = strData[k];
        if (dataStarv != null)
        {
		    dataStarv = dataStarv.slice(1);
		    dataStarv.push([dataIdx,starvStats[keys[k]][0]/starvStats[keys[k]][1]]);
		    strData[k] = dataStarv;
        }
    }

	var lymax = 0.25;
	var rymax = 0.05;
	var ry2max = 1000;
	for(var j=0;j<121;j++)
	{
		if (lymax < dataMem[j][1])
			lymax = dataMem[j][1];
		if (rymax < dataLat[j][1])
			rymax = dataLat[j][1];
		if (ry2max < dataTPS[j][1])
			ry2max = dataTPS[j][1];
	}
	ry2max = Math.ceil(ry2max/1000)*1000;
	var tickValues = [];
	tickValues.push(dataIdx-120);
	for(var j=(dataIdx-120)+6-((dataIdx-120)%6);j<dataIdx+1;j+=6)
		tickValues.push(j);
	tickValues.push(dataIdx);
	
	monitor.latMax = rymax;
	monitor.tpsMax = ry2max;
	monitor.memMax = lymax;
	monitor.strMax = 100;
	
	monitor.latData = [dataLat];
	monitor.tpsData = [dataTPS];
	monitor.memData = [dataMem];
	monitor.strData = strData;

	monitor.tickValues = tickValues;
	// Update the monitor graphs
	var lmax = 1;
	var rmax = 1;
	switch(monitor.leftMetric)
	{
		case 'lat':
			monitor.leftPlot.series[0].data = dataLat;
			lmax = rymax;
			break;
		case 'tps':
			monitor.leftPlot.series[0].data = dataTPS;
			lmax = ry2max;
			break;
		case 'mem':
			monitor.leftPlot.series[0].data = dataMem;
			lmax = lymax;
			break;
		case 'str':
			for(var k=0;k<strData.length;k++)
            {
				monitor.leftPlot.series[k].data = strData[k];
				monitor.leftPlot.series[k].label = keys[k];
            }
			lmax = 100;
			break;
	}
	switch(monitor.rightMetric)
	{
		case 'lat':
			monitor.rightPlot.series[0].data = dataLat;
			rmax = rymax;
			break;
		case 'tps':
			monitor.rightPlot.series[0].data = dataTPS;
			rmax = ry2max;
			break;
		case 'mem':
			monitor.rightPlot.series[0].data = dataMem;
			rmax = lymax;
			break;
		case 'str':
			for(var k=0;k<strData.length;k++)
            {
				monitor.rightPlot.series[k].data = strData[k];
				monitor.rightPlot.series[k].label = keys[k];
            }
			rmax = 100;
			break;
	}

	try
	{
		monitor.leftPlot.replot({clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: lmax, numberTicks: 5 } }});
		monitor.rightPlot.replot({clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: rmax, numberTicks: 5 } }});
	} catch (x) {}

	MonitorUI.UpdateMonitorItem(id);
	monitor.lastTimerTick = currentTimerTick;
	MonitorUI.Monitors[id] = monitor;

}

this.UpdateMonitorItem = function(id)
{
	if (!(id in MonitorUI.Monitors))
		return;
		
	var item = $(MonitorUI.Monitors[id].tab.find('.monitoritemselection')).val();
	var data = [];
	switch(item)
	{
		case 'm':
			data = MonitorUI.Monitors[id].memData[0];
			break;
		case 'l':
			data = MonitorUI.Monitors[id].latData[0];
			break;
		case 't':
			data = MonitorUI.Monitors[id].tpsData[0];
			break;
	}
	var counters = [data[data.length-1][1], 1000000000,-1,0]
	if (data != null)
	{
		var cnt = 0;
		for(var i = 0; i < data.length; i++)
		{
			if (data[i][0] > 120)
			{
				if (counters[1] > data[i][1]) counters[1] = data[i][1];
				if (counters[2] < data[i][1]) counters[2] = data[i][1];
				counters[3] += data[i][1];
				cnt++;
			}
		}
		counters[3] = counters[3]/cnt;
	}
	var targets = MonitorUI.Monitors[id].tab.find('.monitoritem');
	for(var i = 0; i < 4; i++)
		$(targets[i]).text(Math.round(counters[i]*100)/100);
}
});
window.MonitorUI = MonitorUI = new IMonitorUI();
})(window);
