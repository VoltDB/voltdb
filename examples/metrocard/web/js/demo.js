// schedule refresh functions to run periodically
function RefreshData(){

    con.BeginExecute('GetBusiestStationInLastMinute',
                     [],
                     function(response) {
                         DrawTable(response,'#table_minute_station')}
                    );

// begin of code added by Patrice
  if ((Math.random() * 5)>4) 
   {
    var cardid = Math.floor((Math.random() * 1000) + 1);
    var stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_2')}
                    );

    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_3')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_4')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_5')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_6')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_7')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_8')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_9')}
                    );
    cardid = Math.floor((Math.random() * 1000) + 1);
    stationid = Math.floor((Math.random() * 50) + 1);
    con.BeginExecute('CardSwipe',
                     [cardid,stationid],
                     function(response) {
                         DrawTable(response,'#table_card_swipe_10')}
                    );
   }
// end of code added by Patrice

    con.BeginExecute('GetSwipesPerSecond',
                     [30],
                     function(response) {
                         DrawTimeLinesChart(response,'#swipe_chart');
                     }
                    );

}

function RefreshStats() {
    con.BeginExecute('@Statistics',
                     ['PROCEDUREPROFILE','0'],
                     function(response) {
                         DrawTPSChart(response,'#tps_chart');
                     }
                    );

}

function DrawTimeLinesChart(response, placeholder) {
    var tables = response.results;
    var t0 = tables[0];
    var swipes = [];
    var entries = [];

    for(var r=0;r<t0.data.length;r++){ // for each row
        var time = t0.data[r][0]/1000;
        var swipe = t0.data[r][1];
        var entry = t0.data[r][2];
        swipes.push([time,swipe]);
        entries.push([time,entry]);
    }

    var swipeline = { label: "Swipes", data: swipes };
    var entryline = { label: "Entries", data: entries };

    var options = {
        series: {
	    lines: { show: true, fill: false },
	    points: { show: false }
        },
        xaxis: { mode: "time", timezone: "browser", minTickSize: [20, "second"], ticks: 10 },
        yaxis: { position: "right" },
        legend: { position: 'nw' }
    };

    $.plot($(placeholder), [swipeline, entryline], options);
}



function DrawTable(response, tableName) {
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
            tbodyhtml += '<tr>';
            for (var c=0;c<colcount;c++) { // for each column
                var f = hmt.data[r][c];

                // if type is DECIMAL
                if (hmt.schema[c].type == 22 || hmt.schema[c].type == 8) {
                    f = formatDecimal(f);
                }

                if (hmt.schema[c].type == 11) {
                    f = formatDateAsTime(f);
                }
                tbodyhtml += '<td>' + f + '</td>';
            }
            tbodyhtml += '</tr>';
        }
        $(tableName).children('tbody').html(tbodyhtml);

    } catch(x) {}
}

function startTime() {
    document.getElementById('time').innerHTML = "Current Time: " + new Date().toUTCString();
    // repeat every 500ms
    t = setTimeout(function () {
        startTime()
    }, 500);
}
startTime();
