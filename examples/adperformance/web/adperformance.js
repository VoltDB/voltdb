// variables for selected row of table
var selectedCampaign = $('#table_ad_sum').children('td:first').text();
var selectedIndex = -1;
var advertiser = 1;

// connect stored procedure calls with chart or table function
function RefreshTable1(){
    con.BeginExecute('advertiser_summary', 
                     [advertiser], 
                     function(response) {
                         DrawTable(response,'#table_ad_sum',selectedIndex)}
                    );
}

function RefreshTable2(){
    con.BeginExecute('campaign_summary', 
                     [advertiser,selectedCampaign], 
                     function(response) {
                         DrawTable(response,'#table_camp_sum',-1);
                     }
                    );
}

function RefreshChart1(){
    con.BeginExecute('advertiser_minutely_clicks',
                     [advertiser],
                     function(response) { 
                         DrawTimeLinesChart(response,'#chart1'); 
                     }
                    );

}

// schedule refresh functions to run periodically
function RefreshData(){
    RefreshTable1();
    RefreshTable2();
    RefreshChart1();
}

// when you click to select a row
$('#table_ad_sum').on('click', 'tbody tr', function(event) {
    // row was clicked
    selectedCampaign = $(this).children('td:first').text();
    selectedIndex = $(this).index();
    $(this).addClass('success').siblings().removeClass('success');
    // immediately refresh the drill-down table
    RefreshTable2();
});

// custom chart or table functions
function DrawTimeLinesChart(response, placeholder) {
    var tables = response.results;
    var t0 = tables[0];
    var colcount = t0.schema.length;
    var d1 = [];
    var d2 = [];

    for(var r=0;r<t0.data.length;r++){ // for each row
        var time = t0.data[r][0]/1000;
        var v1 = t0.data[r][1];
        var v2 = t0.data[r][2];
        d1.push([time,v1]);
        d2.push([time,v2]);
    }
    
    //var d1 = [[0,0], [2,3], [3,2], [5,8]];
    //var d2 = [[0,0], [1,5], [3,8], [5,9]];
    var line1 = { label: "Clicks", data: d1 };
    var line2 = { label: "Conversions", data: d2 };

    var options = {
        series: {
            lines: { show: true, fill: true },
            //bars: { show: true, barWidth : 60*1000, fill: true},
            points: { show: false }
        },
        xaxis: { mode: "time" },
        legend: { position: 'nw' }
    };

    $.plot($(placeholder), [line1, line2], options);
}

