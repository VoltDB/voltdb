// variables for selected row of table
var selectedValue = $('#table_1').children('td:first').text();
var selectedIndex = -1;

function RefreshTable1(){
    con.BeginExecute('FraudFirst50', 
                     [], 
                     function(response) {
                         DrawTable(response,'#table_1',selectedIndex)}
                    );
}

function RefreshTable2(){
    con.BeginExecute('GetAcct', 
                     [selectedValue], 
                     function(response) {
                         DrawTable(response,'#table_2',-1);
                     }
                    );
}

function RefreshTable3(){
    con.BeginExecute('GetTransactions', 
                     [selectedValue], 
                     function(response) {
                         DrawTable(response,'#table_3',-1);
                     }
                    );
}

function RefreshData(){
    RefreshTable1();
    RefreshTable2();
    RefreshTable3();
}

// when you click to select a row
$('#table_1').on('click', 'tbody tr', function(event) {
    // row was clicked
    selectedValue = $(this).children('td:first').text();
    selectedIndex = $(this).index();
    $(this).addClass('success').siblings().removeClass('success');
    // immediately refresh the drill-down table
    RefreshTable2();
    RefreshTable3();
});
