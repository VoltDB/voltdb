/*! jQuery v1.11.1 | (c) 2005, 2014 jQuery Foundation, Inc. | jquery.org/license */
$(document).ready(function () {

    // Toogle Server popup
    $('#btnPopServerList').click(function (event) {

        $("#popServerSearch").val('');
        $("#popServerSearch").attr('placeholder', 'Search Server');
        $('#popServerSearch').keyup();
        event.stopPropagation();
        if ($('#popServerList').css('display') == 'none') {
            $(this).removeClass('showServers');
            $(this).addClass('hideServers');
        } else {
            $(this).removeClass('hideServers');
            $(this).addClass('showServers');

        };
        $('#popServerList').toggle('slide', '', 1500);


        $("#wrapper").click(function () {
            if ($('#popServerList').css('display') == 'block') {
                $('#popServerList').hide();
                $('#btnPopServerList').removeClass('hideServers');
                $('#btnPopServerList').addClass('showServers');
            }

        });
    });

    $("#popServerList").on("click", function (event) {
        event.stopPropagation();
    });


    // Pop Slide Server Search		
    $('#popServerSearch').keyup(function () {
        var searchText = $(this).val().toLowerCase();
        $('ul.servers-list > li').each(function () {
            var currentLiText = $(this).text().toLowerCase(),
                showCurrentLi = currentLiText.indexOf(searchText) !== -1;
            $(this).toggle(showCurrentLi);
        });
    });

    // Implements Scroll in Server List div
    $('#serverListWrapper').slimscroll({
        disableFadeOut: true,
        height: '225px'
    });

    // Shows the active server name





   

    // Tabs Default Action
    $(".tab_content").hide(); //Hide all content
    $("ul.tabs li:first").addClass("active").show(); //Activate first tab
    $(".tab_content:first").show(); //Show first tab content

    //On Click Event
    $("ul.tabs li").click(function () {
        $("ul.tabs li").removeClass("active"); //Remove any "active" class
        $(this).addClass("active"); //Add "active" class to selected tab
        $(".tab_content").hide(); //Hide all tab content
        var activeTab = $(this).find("a").attr("href"); //Find the rel attribute value to identify the active tab + content
        $(activeTab).fadeIn(); //Fade in the active content
        return false;
    });

    // Show Hide Graph Block
    $('#showHideGraphBlock').click(function () {
        var userPreferences = getUserPreferences();
        if (userPreferences != null) {
            if (userPreferences['ClusterLatency'] != false || userPreferences['ClusterTransactions'] != false || userPreferences['ServerCPU'] != false || userPreferences['ServerRAM'] != false) {
                var graphState = $("#graphBlock").css('display');
                if (graphState == 'none') {
                    $(".showhideIcon").removeClass('collapsed');
                    $(".showhideIcon").addClass('expanded');
                } else {
                    $(".showhideIcon").removeClass('expanded');
                    $(".showhideIcon").addClass('collapsed');
                }
                $('#graphBlock').slideToggle();

                MonitorGraphUI.ChartRam.update();
                MonitorGraphUI.ChartCpu.update();
                MonitorGraphUI.ChartLatency.update();
                MonitorGraphUI.ChartTransactions.update();
            }
        }
    });



    // Shows memory alerts
    $('#showMemoryAlerts').popup();

    // shows server not reachable
    $('#showServerUnreachable').popup();

    // Changes graph view
    $("select").change(function () {
        //$('#showServerUnreachable').trigger('click');
        //location.reload();

    });

    // Filters Stored Procedures
    $('#filterStoredProc').keyup(function () {
        var that = this;
        $.each($('.storeTbl tbody tr'),
        function (i, val) {
            if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                $('.storeTbl tbody tr').eq(i).hide();
            } else {
                $('.storeTbl tbody tr').eq(i).show();
            }
        });
    });
    // clears the searched text and displays all rows	
    //$(document).click(function(){
    //	$("#filterStoredProc").val('');
    //	//$("#filterStoredProc").attr('placeholder','Search Stored Procedures');
    //	//$('#filterStoredProc').keyup();
    //});	


    // Filters Database Tables
    $('#filterDatabaseTable').keyup(function () {
        var that = this;
        $.each($('.dbTbl tbody tr'),
        function (i, val) {
            if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                $('.dbTbl tbody tr').eq(i).hide();
            } else {
                $('.dbTbl tbody tr').eq(i).show();
            }
        });
    });
    // clears the searched text and displays all rows	
    $(document).click(function () {
        //$("#filterDatabaseTable").val('');
        //$("#filterDatabaseTable").attr('placeholder','Search Database Tables');
        //$('#filterDatabaseTable').keyup();
    });

    //$('#showServerUnreachable').trigger('click');

});






