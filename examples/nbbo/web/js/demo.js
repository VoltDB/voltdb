var symbol = $('#symbol').val();

// schedule refresh functions to run periodically
function RefreshData(){

    con.BeginExecute('nbbo_last_ask_symbol', 
                     [symbol], 
                     function(response) {
                         DrawNBBOTable(response,'#table_nbbo_ask')}
                    );

    con.BeginExecute('nbbo_last_bid_symbol', 
                     [symbol], 
                     function(response) {
                         DrawNBBOTable(response,'#table_nbbo_bid')}
                    );

    con.BeginExecute('last_bids_symbol', 
                     [symbol], 
                     function(response) {
                         DrawNBBOTable(response,'#table_last_bids');
                     }
                    );

    con.BeginExecute('last_asks_symbol', 
                     [symbol], 
                     function(response) {
                         DrawNBBOTable(response,'#table_last_asks');
                     }
                    );
    
    con.BeginExecute('nbbo_hist_symbol',
                     [symbol],
                     function(response) { 
                         DrawTimeLinesChart(response,'#nbbo_chart'); 
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


// when you click to select a row
$('#symbol').change(function(event) {
    // row was clicked
    symbol = $(this).val();
    console.log('symbol=' + symbol);

    // immediately refresh the drill-down table
    RefreshData();
});

function DrawTimeLinesChart(response, placeholder) {
    var tables = response.results;
    var t0 = tables[0];
    var colcount = t0.schema.length;
    var bids = [];
    var asks = [];

    for(var r=0;r<t0.data.length;r++){ // for each row
        var time = t0.data[r][1]/1000;
        var bid = t0.data[r][3]/10000;
        var ask = t0.data[r][6]/10000;
        bids.push([time,bid]);
        asks.push([time,ask]);
    }

    var askline = { label: "Ask", color: "#5bc0de", data: asks };
    var bidline = { label: "Bid", color: "#f89406", data: bids };

    var options = {
        series: {
	    lines: { show: true, fill: false },
	    //bars: { show: true, barWidth : 60*1000, fill: true},
	    points: { show: false }
        },
        xaxis: { mode: "time", timezone: "browser", minTickSize: [20, "second"], ticks: 10 },
        yaxis: { position: "right" },
        legend: { position: 'nw' }
    };

    $.plot($(placeholder), [askline, bidline], options);
}



function DrawNBBOTable(response, tableName) {
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
                var style = '';
                var f = hmt.data[r][c];

                // if type is DECIMAL
                if (hmt.schema[c].type == 22 || hmt.schema[c].type == 8) {
                    f = formatDecimal(f);
                }
                // custom for NBBO
                if (hmt.schema[c].name == 'BID' || hmt.schema[c].name == 'ASK') {
                    f = formatDecimal(f/10000);
                }

                if (hmt.schema[c].type == 11) {
                    f = formatDateAsTime(f);
                }

                if (r==0 && hmt.schema[c].name == 'BID') {
                    style=' class="text-warning"';
                }
                if (r==0 && hmt.schema[c].name == 'ASK') {
                    style=' class="text-info"';
                }
                
                tbodyhtml += '<td' + style + '>' + f + '</td>';
            }
            tbodyhtml += '</tr>';
        }
        $(tableName).children('tbody').html(tbodyhtml);

    } catch(x) {}
}

function startTime() {
    document.getElementById('time').innerHTML = new Date().toUTCString();
    // repeat every 500ms
    t = setTimeout(function () {
        startTime()
    }, 500);
}
startTime();
