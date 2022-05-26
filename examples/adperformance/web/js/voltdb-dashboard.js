/*
 Copyright (C) 2022 Volt Active Data Inc.
*/

var con;
var intervalId;
var statsIntervalId;

function formatDecimal2(n) {
    return (Math.round(parseFloat(n) * 100) / 100).toFixed(2);
}

function DrawTable(response, tableName, selectedRow) {
    try {
        var tables = response.results;
        var t0 = tables[0];
        var colcount = t0.schema.length;
        
        // the first time, initialize the table head
        if ($(tableName+' thead tr').length == 0) {
            var theadhtml = '<tr>';
            for (var i=0; i<colcount; i++) {
                theadhtml += '<th>' + t0.schema[i].name + '</th>';
            }
            $(tableName).append('<thead></thead>');
            $(tableName).append('<tbody></tbody>');
            $(tableName).children('thead').html(theadhtml);
        }
        
        var tbodyhtml;
        for(var r=0;r<t0.data.length;r++){ // for each row
            if (r==selectedRow) {
                tbodyhtml += '<tr class="success">';
            } else {
                tbodyhtml += '<tr>';
            }
            for (var c=0;c<colcount;c++) { // for each column
                var f = t0.data[r][c];

                // if type is DECIMAL
                if (t0.schema[c].type == 22) {
                    f = formatDecimal2(f);
                }
                tbodyhtml += '<td>' + f + '</td>';
            }
            tbodyhtml += '</tr>';
        }
        $(tableName).children('tbody').html(tbodyhtml);

    } catch(x) {}
}

function SetRefreshInterval(interval) {
    if (intervalId != null) {
        clearInterval(intervalId);
        intervalId = null;
    }
    if(interval > 0) {
        intervalId = setInterval(RefreshData, interval*1000);
        statsIntervalId = setInterval(RefreshStats,1000);
    }
}

$(document).ready(function(){
    $('#loadingModal').modal('show');
    checkConnection();
});

function checkConnection() {
    VoltDB.TestConnection(location.hostname, 8080, false, null, null, false,
                          function(connected){
                              if (connected) {
                                  $('#loadingModal').modal('hide');
                                  connectToDatabase();
                              } else {
                                  checkConnection();
                              }
                          });
}

function connectToDatabase() {
    // Prepopulate the tps graph with 0s.
    initTpsVals();

    con = VoltDB.AddConnection(location.hostname, 8080, false, null, null, false, (function(connection, success){}));
    SetRefreshInterval(1);
}

// Refresh drop-down actions
$('#refresh-1').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetRefreshInterval(1);
});
$('#refresh-5').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetRefreshInterval(5);
});
$('#refresh-10').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetRefreshInterval(10);
});
$('#refresh-pause').click(function(e) {
    e.preventDefault();// prevent the default anchor functionality
    SetRefreshInterval(-1);
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

function initTpsVals() {
    if (tpsVals.length == 0) {
        var now = (new Date()).getTime();
        var interval = 1000; // 1 second
        var ts = now - 60 * 1000;
        while (ts < now) {
            tpsVals.push([ts, 0]);
            ts += interval;
        }
    }
}

var tpsVals = [];
var prevTsMs = null;
var tcount0 = null;
function DrawTPSChart(response, someDiv) {
    var tables = response.results;
    var table0 = tables[0];
    //var colcount = table0.schema.length;

    var cTsMs = table0.data[0][0];
    if (prevTsMs != null && cTsMs == prevTsMs) {
        // Skip cached old results
        return;
    }
    var durationMs = cTsMs - prevTsMs;
    prevTsMs = cTsMs;

    var time = table0.data[0][0]; // milliseconds
    var tcount1 = 0;
    for(var r=0;r<table0.data.length;r++){ // for each row
        //var time = table0.data[r][0]/1000;
        tcount1 += table0.data[r][3];
    }
    var tps;
    if (tcount0 == null) {
        tps = 0;
    } else {
        tps = (tcount1 - tcount0)*1000/durationMs;
    }
    tcount0 = tcount1;
    tpsVals.push([time,tps]);

    // Only keep the last minute's data to bound memory usage
    if (tpsVals[tpsVals.length - 1][0] - tpsVals[0][0] > 60000) {
        tpsVals.shift();
    }

    var tpsline = { label: "TPS", data: tpsVals };

    var options = {
        series: {
	    lines: { show: true, fill: true },
	    //bars: { show: true, barWidth : 60*1000, fill: true},
	    points: { show: false }
        },
        xaxis: { mode: "time", timezone: "browser", minTickSize: [30, "second"], ticks: 4 },
        yaxis: { position: "right" },
        legend: { position: 'nw' }
    };

    $.plot($(someDiv), [tpsline], options);
}
