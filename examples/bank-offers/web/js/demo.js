//var symbol = 'IBM';

// custom chart or table functions
//var bids = [];
//var asks = [];

// schedule refresh functions to run periodically
function RefreshData(){

    con.BeginExecute('RecentOffersList',
                     [],
                     function(response) {
                         DrawTable(response,'#offers_table')}
                    );

    con.BeginExecute('recent_offer_totals',
                     [],
                     function(response) { 
                         DrawTimeLinesChart(response,'#offers_chart'); 
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
// $('#symbol').change(function(event) {
//     // row was clicked
//     symbol = $(this).val();
//     console.log('symbol=' + symbol);

//     // immediately refresh the drill-down table
//     RefreshData();
// });

function DrawTimeLinesChart(response, placeholder) {
    var tables = response.results;
    var t0 = tables[0];
    var colcount = t0.schema.length;
    var offers = [];

    for(var r=1;r<t0.data.length;r++){ // for each row
        var time = t0.data[r][0]/1000;
        var offer = t0.data[r][1];
        offers.push([time,offer]);
    }

    var offerline = { label: "Offers", data: offers };

    var options = {
        series: {
            color: 3,
	    lines: { show: true, fill: false },
	    //bars: { show: true, barWidth : 60*1000, fill: true},
	    points: { show: false }
        },
        xaxis: { mode: "time", timezone: "browser", minTickSize: [30, "second"], ticks: 10 },
        yaxis: { position: "right" },
        legend: { position: 'nw' }
    };

    $.plot($(placeholder), [offerline], options);
}



// function DrawNBBOTable(response, tableName) {
//     try {
//         var tables = response.results;
//         var hmt = tables[0];
//         var colcount = hmt.schema.length;
        
//         // the first time, initialize the table head
//         if ($(tableName+' thead tr').length == 0) {
//             var theadhtml = '<tr>';
//             for (var i=0; i<colcount; i++) {
//                 theadhtml += '<th>' + hmt.schema[i].name + '</th>';
//             }
//             $(tableName).append('<thead></thead>');
//             $(tableName).append('<tbody></tbody>');
//             $(tableName).children('thead').html(theadhtml);
//         }
        
//         var tbodyhtml;
//         for(var r=0;r<hmt.data.length;r++){ // for each row
//             tbodyhtml += '<tr>';
//             for (var c=0;c<colcount;c++) { // for each column
//                 var f = hmt.data[r][c];

//                 // if type is DECIMAL
//                 if (hmt.schema[c].type == 22 || hmt.schema[c].type == 8) {
//                     f = formatDecimal(f);
//                 }
//                 // custom for NBBO
//                 if (hmt.schema[c].name == 'BID' || hmt.schema[c].name == 'ASK') {
//                     f = formatDecimal(f/10000);
//                 }

//                 if (hmt.schema[c].type == 11) {
//                     f = formatDateAsTime(f);
//                 }
//                 tbodyhtml += '<td>' + f + '</td>';
//             }
//             tbodyhtml += '</tr>';
//         }
//         $(tableName).children('tbody').html(tbodyhtml);

//     } catch(x) {}
// }

// function startTime() {
//     document.getElementById('time').innerHTML = "Current Time: " + new Date().toUTCString();
//     // repeat every 500ms
//     t = setTimeout(function () {
//         startTime()
//     }, 500);
// }
// startTime();
