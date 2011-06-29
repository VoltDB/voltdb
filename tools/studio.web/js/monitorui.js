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
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:'%d'} } },
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
			var partitionCount = VoltDB.GetConnection(id.substr(2)).Metadata['partitionCount'];
			var seriesStr = [];
		    for(var j=0;j<partitionCount;j++)
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
	
	var data = [];
    for(var i = 0;i<121;i++)
    	data[i] = [i,0];
	
    var tickValues = [];
    for(var i=0;i<121;i+=6)
    	tickValues.push(i);
    	
	var dataStr = [];
    for(var j=0;j<partitionCount;j++)
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
    };

    InitializeChart(id, 'left', 'lat');
    InitializeChart(id, 'right', 'tps');

    if(MonitorUI.Interval == null)
    	MonitorUI.RefreshMonitorData(id);
	MonitorUI.SetRefreshSpeed();
}
this.RemoveMonitor = function(id)
{
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
		if (speed != 'pau')
			MonitorUI.Interval = setInterval(function() {MonitorUI.RefreshData();}, (speed == 'hig' ? 1 : (speed == 'nor' ? 2 : 10))*500);
		MonitorUI.Speed = speed;
	}
}
this.Redraw = function()
{
	for(var id in this.Monitors)
	{
		MonitorUI.Monitors[id].leftPlot.replot();
		MonitorUI.Monitors[id].rightPlot.replot();
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
		connection.Queue.Start();
			connection.Queue.BeginExecute('@Statistics', ['MEMORY', 0], MonitorUI.Monitors[id].memStatsCallback);
			connection.Queue.BeginExecute('@Statistics', ['PROCEDURE', 0], MonitorUI.Monitors[id].procStatsCallback);
			connection.Queue.BeginExecute('@Statistics', ['STARVATION', 1], MonitorUI.Monitors[id].starvStatsCallback);
		connection.Queue.End(MonitorUI.RefreshMonitor, id);
	}
}
this.RefreshData = function()
{
	for(var id in MonitorUI.Monitors)
	{
		var connection = VoltDB.GetConnection(id.substring(2));
		if (connection == null)
			MonitorUI.RemoveMonitor(id);
		else
		{
			connection.Queue.Start();
				connection.Queue.BeginExecute('@Statistics', ['MEMORY', 0], MonitorUI.Monitors[id].memStatsCallback);
				connection.Queue.BeginExecute('@Statistics', ['PROCEDURE', 0], MonitorUI.Monitors[id].procStatsCallback);
				connection.Queue.BeginExecute('@Statistics', ['STARVATION', 1], MonitorUI.Monitors[id].starvStatsCallback);
			connection.Queue.End(MonitorUI.RefreshMonitor, id);
		}
	}
}

this.RefreshMonitor = function(id, Success)
{
    if (!(id in MonitorUI.Monitors))
        return;

	if (!Success)
	{
		$(MonitorUI.Monitors[id].tab.find('.monitoritemselection')).attr('disabled','disabled');
		MonitorUI.RemoveMonitor(id);
		return;
	}
	if (!(id in MonitorUI.Monitors))
		return;
		
	var currentTimerTick = (new Date()).getTime();
	var latData = MonitorUI.Monitors[id].latData;
	var tpsData = MonitorUI.Monitors[id].tpsData;
	var memData = MonitorUI.Monitors[id].memData;
	var strData = MonitorUI.Monitors[id].strData;
	
	var dataMem = memData[0];
	var dataLat = latData[0];
	var dataTPS = tpsData[0];
	var dataIdx  = dataMem[dataMem.length-1][0]+1;
	var Mem = 0;
	var table = MonitorUI.Monitors[id].memStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
		Mem += table[j][3]*1.0/1048576.0;
	dataMem = dataMem.slice(1);
	dataMem.push([dataIdx, Mem]);
	var Lat = 0;
	var TPS = 0;
	var procStats = {};
	table = MonitorUI.Monitors[id].procStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
	{
		var data = null;
		if (table[j][5] in procStats)
		{
			data = procStats[table[j][5]];
			data[1] += table[j][6];
			if (data[2] > table[j][8]) data[2] = table[j][8];
			data[3] += table[j][10]*table[j][7];
			if (data[4] < table[j][9]) data[4] = table[j][9];
			data[5] += table[j][7];
		}
		else
			data = [table[j][5], table[j][6], table[j][8], table[j][10]*table[j][7], table[j][9], table[j][7]];
		procStats[table[j][5]] = data;
	}
	table = MonitorUI.Monitors[id].starvStatsResponse.results[0].data;
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
			starvStats
		starvStats[table[j][3]] = data;
	}
	var currentTransactionCount = 0;
	var currentTimedTransactionCount = 0;
	var currentLatencyAverage = 0;
	for(var proc in procStats)
	{
		currentTransactionCount += procStats[proc][1];
		currentTimedTransactionCount += procStats[proc][5];
		currentLatencyAverage += procStats[proc][3];
		procStats[proc][3] = procStats[proc][3]/procStats[proc][5];
	}
	currentLatencyAverage = currentLatencyAverage / currentTimedTransactionCount*1.0;
	
	if (MonitorUI.Monitors[id].lastTransactionCount > 0 && MonitorUI.Monitors[id].lastTimerTick > 0)
	{
		var delta = currentTransactionCount - MonitorUI.Monitors[id].lastTransactionCount;
		dataTPS = dataTPS.slice(1);
		dataTPS.push([dataIdx, delta*1000.0 / (currentTimerTick - MonitorUI.Monitors[id].lastTimerTick)]);
		dataLat = dataLat.slice(1);
		if (currentTimedTransactionCount == MonitorUI.Monitors[id].lastTimedTransactionCount)
		{
			if (delta < 10)
				dataLat.push([dataIdx,0]);
			else
				dataLat.push([dataIdx,currentLatencyAverage/1000.0]);
		}
		else
			dataLat.push([dataIdx,((currentLatencyAverage * currentTimedTransactionCount - MonitorUI.Monitors[id].lastLatencyAverage * MonitorUI.Monitors[id].lastTimedTransactionCount) / (currentTimedTransactionCount - MonitorUI.Monitors[id].lastTimedTransactionCount)) /1000.0]);
	}
	
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
					$(cells[2]).text((Math.round(procStats[j][2]/10.0)/100.0));
					$(cells[3]).text((Math.round(procStats[j][3]/10.0)/100.0));
					$(cells[4]).text((Math.round(procStats[j][4]/10.0)/100.0));
				}
			}
		}
		
	}
	else
	{
		var src = '<table id="stats-' + id + '" class="tablesorter statstable" border="0" cellpadding="0" cellspacing="1"><thead class="ui-widget-header noborder">';
		src += '<th>Procedure</th><th>Calls</th><th>Min (ms)</th><th>Avg (ms)</th><th>Max (ms)</th>';
		src += '</thead><tbody>';
		for(var j in procStats)
		{
			src += '<tr>';
			src += '<td align="left">' + procStats[j][0] + '</td>';
			src += '<td align="right">' + procStats[j][1] + '</td>';
			for(var k = 2; k < 5; k++)
				src += '<td align="right">' + (Math.round(procStats[j][k]/10.0)/100.0) + '</td>';
			src += '</tr>';
		}
		src += '</tbody></table>';
		$(MonitorUI.Monitors[id].tab.find('.tablebar')).html(src);
	}
	$("#stats-" + id).tablesorter({widgets: ['zebra']});
	
	MonitorUI.Monitors[id].lastTransactionCount = currentTransactionCount;
	MonitorUI.Monitors[id].lastTimedTransactionCount = currentTimedTransactionCount;
	MonitorUI.Monitors[id].lastLatencyAverage = currentLatencyAverage;
	
	for(var k in starvStats)
	{
		var dataStarv = strData[k-1]; //MonitorUI.Monitors[id].bottomPlot.series[k-1].data;
		dataStarv = dataStarv.slice(1);
		dataStarv.push([dataIdx,starvStats[k][0]/starvStats[k][1]]);
		strData[k-1] = dataStarv;
	}

	var lymax = 0.25;
	var rymax = 100;
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
	rymax = Math.ceil(rymax/100)*100;
	ry2max = Math.ceil(ry2max/1000)*1000;
	var tickValues = [];
	tickValues.push(dataIdx-120);
	for(var j=(dataIdx-120)+6-((dataIdx-120)%6);j<dataIdx+1;j+=6)
		tickValues.push(j);
	tickValues.push(dataIdx);
	
	MonitorUI.Monitors[id].latMax = rymax;
	MonitorUI.Monitors[id].tpsMax = ry2max;
	MonitorUI.Monitors[id].memMax = lymax;
	MonitorUI.Monitors[id].strMax = 100;
	
	MonitorUI.Monitors[id].latData = [dataLat];
	MonitorUI.Monitors[id].tpsData = [dataTPS];
	MonitorUI.Monitors[id].memData = [dataMem];
	MonitorUI.Monitors[id].strData = strData;

	MonitorUI.Monitors[id].tickValues = tickValues;
	
	var lmax = 1;
	var rmax = 1;
	switch(MonitorUI.Monitors[id].leftMetric)
	{
		case 'lat':
			MonitorUI.Monitors[id].leftPlot.series[0].data = dataLat;
			lmax = rymax;
			break;
		case 'tps':
			MonitorUI.Monitors[id].leftPlot.series[0].data = dataTPS;
			lmax = ry2max;
			break;
		case 'mem':
			MonitorUI.Monitors[id].leftPlot.series[0].data = dataMem;
			lmax = lymax;
			break;
		case 'str':
			for(var k in starvStats)
				MonitorUI.Monitors[id].leftPlot.series[k-1].data = strData[k-1];
			lmax = 100;
			break;
	}
	switch(MonitorUI.Monitors[id].rightMetric)
	{
		case 'lat':
			MonitorUI.Monitors[id].rightPlot.series[0].data = dataLat;
			rmax = rymax;
			break;
		case 'tps':
			MonitorUI.Monitors[id].rightPlot.series[0].data = dataTPS;
			rmax = ry2max;
			break;
		case 'mem':
			MonitorUI.Monitors[id].rightPlot.series[0].data = dataMem;
			rmax = lymax;
			break;
		case 'str':
			for(var k in starvStats)
				MonitorUI.Monitors[id].rightPlot.series[k-1].data = strData[k-1];
			rmax = 100;
			break;
	}

    try
    {
	    MonitorUI.Monitors[id].leftPlot.replot({clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: lmax, numberTicks: 5 } }});
	    MonitorUI.Monitors[id].rightPlot.replot({clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: rmax, numberTicks: 5 } }});
    } catch (x) {}

	MonitorUI.UpdateMonitorItem(id);
	MonitorUI.Monitors[id].lastTimerTick = currentTimerTick;
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
