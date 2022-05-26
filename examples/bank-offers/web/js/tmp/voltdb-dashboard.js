/*
 This file is part of VoltDB.
 Copyright (C) 2008-2022 Volt Active Data Inc.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
*/

var con;
var chartIntervalId;
var statsIntervalId;


// initialize actions
$(document).ready(function(){

    // connect to VoltDB HTTP/JSON interface
    con = VoltDB.AddConnection(location.hostname, 8080, false, null, null, false, (function(connection, success){}));

    // set the chart interval to 1 second default
    SetChartRefreshInterval(1);

    // set the stats interval to run every 1000 milliseconds
    statsIntervalId = setInterval(RefreshStats,1000);
    
});

function AddConnectionRetry(

// set/reset the chart interval
function SetChartRefreshInterval(interval) {
    if (chartIntervalId !== null) {
        clearInterval(chartIntervalId);
        chartIntervalId = null;
    }
    if(interval > 0) {
        chartIntervalId = setInterval(RefreshData, interval*1000);
    }
}


// Refresh drop-down
$('#refresh-1').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetChartRefreshInterval(1);
});
$('#refresh-5').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetChartRefreshInterval(5);
});
$('#refresh-10').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetChartRefreshInterval(10);
});
$('#refresh-pause').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetChartRefreshInterval(-1);
});


// ----------------- Stats Charts -----------------

// this runs every second
function RefreshStats() {
    con.BeginExecute('@Statistics',
                     ['PROCEDUREPROFILE','0'],
                     function(response) { 
                         DrawTPSChart(response,'#tps_chart'); 
                     }
                    );
    
}
var tpsVals = [];
var tcount0;
function DrawTPSChart(response, someDiv) {
    var tables = response.results;
    var table0 = tables[0];
    //var colcount = table0.schema.length;
    var time = 0;
    var tcount1 = 0;
    for(var r=0;r<table0.data.length;r++){ // for each row
        time = table0.data[r][0]/1000;
        tcount1 += table0.data[r][3];
    }
    var tps;
    if (tcount0 == null) {
        tps = 0;
    } else {
        tps = tcount1 - tcount0;
    }
    tcount0 = tcount1;
    tpsVals.push([time,tps]);

    var tpsline = { label: "TPS", data: tpsVals };

    var options = {
        series: {
	    lines: { show: true, fill: true },
	    //bars: { show: true, barWidth : 60*1000, fill: true},
	    points: { show: false }
        },
        xaxis: { mode: "time" },
        yaxis: { position: "right" },
        legend: { position: 'nw' }
    };

    $.plot($(someDiv), [tpsline], options);
}



// Draw a basic HTML table from a VoltTable
function DrawTable(response, tableName, selectedRow) {
    try {
        var tables = response.results;
        var hmt = tables[0];
        var colcount = hmt.schema.length;
        
        // the first time, initialize the table head
        if ($(tableName+' thead tr').length == 0) {
            var theadhtml = '<tr>';
            for (var i=0; i<colcount; i++) {
                theadhtml += '<th>' + hmt.schema[i].name + '</th>';
            }
            $(tableName).append('<thead></thead>');
            $(tableName).append('<tbody></tbody>');
            $(tableName).children('thead').html(theadhtml);
        }
        
        var tbodyhtml;
        for(var r=0;r<hmt.data.length;r++){ // for each row
            if (r==selectedRow) {
                tbodyhtml += '<tr class="success">';
            } else {
                tbodyhtml += '<tr>';
            }
            for (var c=0;c<colcount;c++) { // for each column
                var f = hmt.data[r][c];

                // if type is DECIMAL
                if (hmt.schema[c].type == 22 || hmt.schema[c].type == 8) {
                    f = formatDecimal(f);
                }
                if (hmt.schema[c].type == 11) {
                    f = formatDate(f);
                }
                tbodyhtml += '<td>' + f + '</td>';
            }
            tbodyhtml += '</tr>';
        }
        $(tableName).children('tbody').html(tbodyhtml);

    } catch(x) {}
}

// Data Formatting Methods
function formatDecimal(n) {
    return (Math.round(parseFloat(n) * 100) / 100).toFixed(2);
}

function formatDate(n) {
    //return new Date(n/1000).toLocaleDateString();
    return new Date(n/1000).toUTCString();
}

function formatDateAsTime(n) {
    //return new Date(n/1000).toLocaleDateString();
    var d = new Date(n/1000).toUTCString();
    var s = d.toString().substring(17,29);
    return s;
}

