(function( window, undefined ){

var IMonitorUI = (function(){

this.Speed = 'pau';
this.Interval = null;
this.Monitors = {};

function Histogram(lowestTrackableValue, highestTrackableValue, nSVD, totalCount) {
    this.lowestTrackableValue = lowestTrackableValue;
    this.highestTrackableValue = highestTrackableValue;
    this.nSVD = nSVD;
    this.totalCount = totalCount;
    this.count = [];
    this.init();
}

Histogram.prototype.init = function() {
    var largestValueWithSingleUnitResolution = 2 * Math.pow(10, this.nSVD);
    this.unitMagnitude = Math.floor(Math.log(this.lowestTrackableValue)/Math.log(2));
    var subBucketCountMagnitude = Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
    this.subBucketHalfCountMagnitude = ((subBucketCountMagnitude > 1) ? subBucketCountMagnitude : 1) - 1;
    this.subBucketCount = Math.pow(2, (this.subBucketHalfCountMagnitude + 1));
    this.subBucketHalfCount = this.subBucketCount / 2;
    this.subBucketMask = (this.subBucketCount - 1) << this.unitMagnitude;
    var trackableValue = (this.subBucketCount - 1) << this.unitMagnitude;
    var bucketsNeeded = 1;
    while (trackableValue < this.highestTrackableValue) {
        trackableValue *= 2;
        bucketsNeeded++;
    }
    this.bucketCount = bucketsNeeded;

    this.countsArrayLength = (this.bucketCount + 1) * (this.subBucketCount / 2);
}

Histogram.prototype.diff = function(newer) {
    var h = new Histogram(newer.lowestTrackableValue, newer.highestTrackableValue, newer.nSVD, newer.totalCount - this.totalCount);
    for (var i = 0; i < h.countsArrayLength; i++) {
        h.count[i] = newer.count[i] - this.count[i];
    }
    return h;
}

Histogram.prototype.getCountAt = function(bucketIndex, subBucketIndex) {
    var bucketBaseIndex = (bucketIndex + 1) << this.subBucketHalfCountMagnitude;
    var offsetInBucket = subBucketIndex - this.subBucketHalfCount;
    var countIndex = bucketBaseIndex + offsetInBucket;
    return this.count[countIndex];
}

Histogram.prototype.valueFromIndex = function(bucketIndex, subBucketIndex) {
    return subBucketIndex * Math.pow(2, bucketIndex + this.unitMagnitude);
}

Histogram.prototype.getValueAtPercentile = function(percentile) {
    var totalToCurrentIJ = 0;
    var countAtPercentile = Math.floor(((percentile / 100.0) * this.totalCount) + 0.5); // round to nearest
    for (var i = 0; i < this.bucketCount; i++) {
        var j = (i == 0) ? 0 : (this.subBucketCount / 2);
        for (; j < this.subBucketCount; j++) {
            totalToCurrentIJ += this.getCountAt(i, j);
            if (totalToCurrentIJ >= countAtPercentile) {
                var valueAtIndex = this.valueFromIndex(i, j);
                return valueAtIndex / 1000.0;
            }
        }
    }
}

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

function convert2Histogram(str) {
    // Read lowestTrackableValue
    var lowestTrackableValue = parseInt(read64(str), 16);
    str = str.substring(16, str.length);
    
    // Read highestTrackableValue
    var highestTrackableValue = parseInt(read64(str), 16);
    str = str.substring(16, str.length);
    
    // Read numberOfSignificantValueDigits
    var nSVD = parseInt(read32(str), 16);
    str = str.substring(8, str.length);
    
    // Read totalCount
    var totalCount = parseInt(read64(str), 16);
    str = str.substring(16, str.length);
    
    var histogram = new Histogram(lowestTrackableValue, highestTrackableValue, nSVD, totalCount);
    
    var i = 0;
    while (str.length >= 16) {
        var value = parseInt(read64(str), 16);
        histogram.count[i] = value;
        str = str.substring(16, str.length);
        i++;
    }
    return histogram;
}

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
		    opt = {
		    	axes: { xaxis: { showTicks: false, min:0, max:120, ticks: tickValues }, y2axis: { min: 0, max: max, numberTicks: 5, tickOptions:{formatString:"%.2f"} } },
		    	grid: { shadow:false, background:'#000', borderWidth: 1, borderColor: 'DarkGreen', gridLineColor:'DarkGreen'},
			series: [{label: "unknown"}],
			seriesDefaults: {showMarker:false, yaxis:'y2axis', lineWidth: 2, shadow: false},
		    	legend: {show: true, location: 'sw', placement: 'insideGrid', renderer: $.jqplot.EnhancedLegendRenderer, rendererOptions: {numberRows:1} }
		    };
			break;
		case 'tb':
		    opt = {
                        seriesDefaults: { renderer: jQuery.jqplot.PieRenderer, rendererOptions: { showDataLabels: true } },
                        legend: { show: true, location: 'e' }
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
	var siteCount = VoltDB.GetConnection(id.substr(2)).Metadata['siteCount'];
	
	var data = [];
	var dataTB = [['Unknown', 100]];
    for(var i = 0;i<121;i++)
    	data[i] = [i,0];
	
    var tickValues = [];
    for(var i=0;i<121;i+=6)
    	tickValues.push(i);
    	
    MonitorUI.Monitors[id] = { 'id': id
    , 'tab': tab
    , 'leftPlot': null //lplot
    , 'rightPlot': null //rplot
    , 'bottomPlot': null //bplot
    , 'memStatsCallback': function(response) {MonitorUI.Monitors[id].memStatsResponse = response;}
    , 'procStatsCallback': function(response) {MonitorUI.Monitors[id].procStatsResponse = response;}
    , 'starvStatsCallback': function(response) {MonitorUI.Monitors[id].starvStatsResponse = response;}
    , 'latStatsCallback': function(response) {MonitorUI.Monitors[id].latStatsResponse = response;}
    , 'memStatsResponse': null
    , 'procStatsResponse': null
    , 'starvStatsResponse': null
    , 'latStatsResponse': null
    , 'lastTimedTransactionCount': -1
    , 'noTransactionCount': 0
    , 'lastTimerTick': -1
    , 'leftMetric': 'lat'
    , 'rightMetric': 'tps'
    , 'latHistogram': null
    , 'latData': [data]
    , 'tpsData': [data]
    , 'memData': [data]
    , 'strData': [data]
    , 'tbData': [dataTB]
    , 'strKeys': []
    , 'emptyData': data
    , 'latMax': 1
    , 'tpsMax': 1
    , 'memMax': 1
    , 'strMax': 100
    , 'tickValues': tickValues
    , 'siteCount': 0
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
			MonitorUI.Interval = setInterval(function() {MonitorUI.RefreshData();}, (speed == 'hig' ? 1 : (speed == 'nor' ? 2 : 5))*1000);
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
				    .BeginExecute('@Statistics', ['LATENCY_HISTOGRAM', 0], MonitorUI.Monitors[id].latStatsCallback)
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

	var currentTimerTick = 0;
	var latData = monitor.latData;
	var tpsData = monitor.tpsData;
	var memData = monitor.memData;
	var strData = monitor.strData;
	
	var dataMem = memData[0];
	var dataLat = latData[0];
	var dataTPS = tpsData[0];
	var dataTB = [];
	var dataIdx  = dataMem[dataMem.length-1][0]+1;
	var Mem = 0;
	// Compute the memory statistics
	var table = monitor.memStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
		Mem += table[j][3]*1.0/1048576.0;
	dataMem = dataMem.slice(1);
	dataMem.push([dataIdx, Mem]);

	// Compute latency statistics 
	table = monitor.latStatsResponse.results[0].data;
	var latStats = convert2Histogram(table[0][5]);
	var lat = 0;
	if (monitor.latHistogram == null)
		lat = latStats.getValueAtPercentile(99);
	else
		lat = monitor.latHistogram.diff(latStats).getValueAtPercentile(99);
	monitor.latHistogram = latStats;
	dataLat = dataLat.slice(1);
	dataLat.push([dataIdx, lat]);

	var procStats = {};
    // Compute procedure statistics 
	table = monitor.procStatsResponse.results[0].data;
	for(var j = 0; j < table.length; j++)
	{
		var srcData = table[j];
		var data = null;
                currentTimerTick = srcData[0];
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
    // Compute latency 
	var currentTimedTransactionCount = 0.0;
	for(var proc in procStats)
	{
		currentTimedTransactionCount += procStats[proc][1];
		dataTB.push([procStats[proc][0], procStats[proc][2]]);
	}
	
	if (monitor.lastTimedTransactionCount > 0 && monitor.lastTimerTick > 0 && monitor.lastTimerTick != currentTimerTick)
	{
		var delta = currentTimedTransactionCount - monitor.lastTimedTransactionCount;
		dataTPS = dataTPS.slice(1);
		dataTPS.push([dataIdx, delta*1000.0 / (currentTimerTick - monitor.lastTimerTick)]);
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
		src += '<th>Procedure</th><th>Invocations</th><th>Weighted Percentage (%)</th><th>Min (ms)</th><th>Avg (ms)</th><th>Max (ms)</th>';
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
	
	monitor.lastTimedTransactionCount = currentTimedTransactionCount;
	
	var keys = [];
	var starvStats = {};
	table = monitor.starvStatsResponse.results[0].data;
	for(var j=0;j<table.length;j++)
    	{
		var key = table[j][2] + table[j][3];
		keys.push(key);
		starvStats[key] = table[j][5];
    	}
	keys.sort();
	
	var dataStr = [];
	for(var j=0;j<table.length;j++)
	{
		var i = monitor.strKeys.indexOf(keys[j]);
		if(i == -1) {
			var dataStarv = monitor.emptyData;
			dataStarv = dataStarv.slice(1);
			dataStarv.push([dataIdx,starvStats[keys[j]]]);
			dataStr.push(dataStarv);
		}
		else {
			var dataStarv = strData[i];
			dataStarv = dataStarv.slice(1);
			dataStarv.push([dataIdx,starvStats[keys[j]]]);
			dataStr.push(dataStarv);
		}
	}
	monitor.strKeys = keys;
	strData = dataStr;

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
	monitor.tbData = dataTB;

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
			for(var k=0;k<monitor.siteCount;k++)
            		{
				if(k < strData.length) {
					monitor.leftPlot.series[k].data = strData[k];
					monitor.leftPlot.series[k].label = keys[k];
					monitor.leftPlot.series[k].show = true;
				}
				else {
					monitor.leftPlot.series[k].show = false;
				}
            		}
			lmax = 100;
			break;
		case 'tb':
                        monitor.leftPlot.series[0].data = dataTB;
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
			for(var k=0;k<monitor.siteCount;k++)
            		{
				if(k < strData.length) {
					monitor.rightPlot.series[k].data = strData[k];
					monitor.rightPlot.series[k].label = keys[k];
					monitor.rightPlot.series[k].show = true;
				}
				else {
					monitor.rightPlot.series[k].show = false;
				}
            		}
			rmax = 100;
			break;
		case 'tb':
                        monitor.rightPlot.series[0].data = dataTB;
			break;
	}
	monitor.siteCount = strData.length;

	if (monitor.leftMetric != 'tb') {
               var left_opt = {clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: lmax, numberTicks: 5 } }};
        }
	else {
               var left_opt = {};
	}

        if (monitor.rightMetric != 'tb') {
               var right_opt = {clear:true, resetAxes: true, axes: { xaxis: { showTicks: false, min:dataIdx-120, max:dataIdx, ticks:tickValues }, y2axis: { min: 0, max: rmax, numberTicks: 5 } }};
        }
	else {
               var right_opt = {};
	}

	try
	{
		monitor.leftPlot.replot(left_opt);
		monitor.rightPlot.replot(right_opt);
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
