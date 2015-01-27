
//Dummy wrapper for console.log for IE9
if (!(window.console && console.log)) {
    console = {
        log: function () { },
        debug: function () { },
        info: function () { },
        warn: function () { },
        error: function () { }
    };
}

function resetpage() {
	 // reset everything
   $('.tablesorter-childRow>td').hide();
   $('.dropdown2>td').hide();
   $('.primaryrow>td').show();
   $('.primaryrow2>td').show();
   // make arrows point the right way
   $('.icon-chevron-down').removeClass('icon-chevron-down').addClass('icon-chevron-right');
   $('.primaryrow').css('background-color', '#ffffff');
}

function navigate(hash) {
    console.log(hash);
    if (hash.length == 0) hash = '#o';
    hash = hash.toLowerCase();
    var anchor = hash.substring(1);
    var parts = anchor.split('-');
    var toppage = parts[0];

    $('.nav-collapse ul li').removeClass('active');
    $('div.reportpage').hide();
    $('#' + toppage + '-nav').addClass('active');
    $('#' + toppage).show();

    if (parts.length > 1) {
        var primary = parts[1];
        console.log("primary = " + primary);

        var iconid = "#" + toppage + "-" + primary + "--icon";
        var dropdownid = "#" + toppage + "-" + primary + "--dropdown";

        $(dropdownid).show();
        $(iconid).parentsUntil("tbody", 'tr').css('background-color', '#dddddd');
        $(iconid).removeClass('icon-chevron-right').addClass('icon-chevron-down');

        if (parts.length > 2) {
            var secondary = parts[2];
            console.log("secondary = " + secondary);

            var iconid2 = "#" + toppage + "-" + primary + "-" + secondary + "--icon";
            var dropdownid2 = "#" + toppage + "-" + primary + "-" + secondary + "--dropdown";

            $(dropdownid2).show();
            $(iconid2).removeClass('icon-chevron-right').addClass('icon-chevron-down');
        }
    }
}

$(document).ready(function () {
    
    $('#o').find('br').remove();//for removing br form id o
    
    var hash = window.location.hash.substr(1);
    if (hash == "" || hash == undefined) {
        $("#o-nav").addClass('active');
    } else {
        $('ul.catalogNav').children().removeClass('active');
        $("#" + hash + "-nav").addClass('active');
    }

    $(window).bind('hashchange', function () {
        
        var hash = window.location.hash.substr(1);
        $('ul.catalogNav').children().removeClass('active');
        $("#" + hash + "-nav").addClass('active');
    });

    var timeOut = (navigator.userAgent.indexOf('Firefox') >= 0) ? 20 : 1;
    
    $(".catalogNav > li > a").on("click", function () {
        
        setTimeout(function() {
            window.scrollTo(0, 0);
        }, timeOut);
    });
    
    $.extend($.tablesorter.themes.bootstrap, {
        // these classes are added to the table. To see other table classes available,
        // look here: http://twitter.github.com/bootstrap/base-css.html#tables
        table: 'table table-bordered',
        header: 'bootstrap-header', // give the header a gradient background
        footerRow: '',
        footerCells: '',
        icons: '', // add "icon-white" to make them white; this icon class is added to the <i> in the header
        sortNone: 'bootstrap-icon-unsorted',
        sortAsc: 'icon-chevron-up',
        sortDesc: 'icon-chevron-down',
        active: '', // applied when column is sorted
        hover: '', // use custom css here - bootstrap class may not override it
        filterRow: '', // filter row class
        even: '', // odd row zebra striping
        odd: ''  // even row zebra striping
    });

    $(".tableL1").tablesorter({
        theme: 'bootstrap',
        widthFixed: true,
        headerTemplate: '{content} {icon}',
        widgets: ["uitheme", "filter", "stickyHeaders"],
        widgetOptions: {
            filter_reset: ".reset",
            filter_hideFilters: true
        },
        cssChildRow: "tablesorter-childRow"
    });

    $('.tableL1').delegate('.togglex', 'click', function () {
        var id = $(this).attr('id');
        var toppage = id.split('-')[0];
        var dropdownid = "#" + id + "--dropdown";

        var hash = "#" + id;
        if ($(dropdownid).is(":visible")) {
			hash = "#" + toppage;
			var iconid = "#" + toppage + "-" + id.split('-')[1] + "--icon";
            $(iconid).parentsUntil("tbody", 'tr').css('background-color', '#ffffff');
            $(iconid).removeClass('icon-chevron-down').addClass('icon-chevron-right');
            $(dropdownid).hide();
            $("[id^="+id+"]").each(function (i, el) {
                var element = '#' + this.id;
                if(element.match("--icon$")) {
                    $(element).removeClass('icon-chevron-down').addClass('icon-chevron-right');
                } else if(element.match("--dropdown$")) {
                    $(element).hide();
                }
            });
		}
        history.replaceState(null, null, hash);
        navigate(hash);
        return false;
    });

    $('.tableL2').delegate('.togglex', 'click', function () {
        var id = $(this).attr('id');

        console.log("l2click: " + id);

        var parts = id.split('-');
        var toppage = parts[0];
        var primary = parts[1];
        var dropdownid = "#" + id + "--dropdown";

        var hash = "#" + id;
        if ($(dropdownid).is(":visible")) {
            console.log("l2clickVISIBLE");
            hash = "#" + toppage + "-" + primary;
            var iconid = "#" + toppage + "-" + primary + "-" + id.split('-')[2] + "--icon";
            $(iconid).removeClass('icon-chevron-down').addClass('icon-chevron-right');
            $(dropdownid).hide();
        }
        else {
            console.log("not visible");
        }
         history.replaceState(null, null, hash);
        navigate(hash);
        return false;
    });

    $(".tableL3").tablesorter({
        theme: 'bootstrap',
        widthFixed: true,
        headerTemplate: '{content} {icon}',
        widgets: ["uitheme", "filter", "stickyHeaders"],
        widgetOptions: {
            filter_reset: ".reset",
            filter_hideFilters: true
        },
        cssChildRow: "tablesorter-childRow",
        textExtraction: function (elem) {
            var $input = $("input[type=text]", elem);
            return $input.val() || $(elem).text();
        }
    });

    $('.tableL3').delegate('.togglex', 'click', function () {
        var id = $(this).attr('id');
        var toppage = id.split('-')[0];
        var dropdownid = "#" + id + "--dropdown";

        var hash = "#" + id;
        if ($(dropdownid).is(":visible")) {
            hash = "#" + toppage;
        }
        // history.replaceState(null, null, hash);
        navigate(hash);
        return false;
    });

    $('.cb').click(function () {
        if ($(this).is(":checked")) {
            var elementList = $(".tableL1").find(".togglex");
            for ( var i = 0; i < elementList.length; i++ ) {
                var item = elementList[i];
                var id = item["id"];

                var anchor = id.toLowerCase();
                var parts = anchor.split('-');
                var toppage = parts[0];

                if (parts.length > 1) {
                    var primary = parts[1];
                    var iconid = "#" + toppage + "-" + primary + "--icon";
                    var dropdownid = "#" + id + "--dropdown";
                    $(iconid).parentsUntil("tbody", 'tr').css('background-color', '#dddddd');
                    $(iconid).removeClass('icon-chevron-right').addClass('icon-chevron-down');
                    $(dropdownid).show();
                    if (parts.length > 2) {
                        var secondary = parts[2];
                        var iconid2 = "#" + toppage + "-" + primary + "-" + secondary + "--icon";
                        var dropdownid2 = "#" + toppage + "-" + primary + "-" + secondary + "--dropdown";
                        $(iconid).parentsUntil("tbody", 'tr').css('background-color', '#dddddd');
                        $(iconid2).removeClass('icon-chevron-right').addClass('icon-chevron-down');
                        $(dropdownid2).show();
                    }
                }
            }
        } else {
            resetpage();
        }
    });

    $('#s-nav, #p-nav').click(function(){
        $('.cb').prop('checked',false);
    });

    resetpage();
    navigate($(location).attr('hash'));

    $(window).bind('hashchange', function () { //detect hash change
        resetpage();
        navigate(window.location.hash);
    });

     //tooltips
    $('.l-Table').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'Persistent Relational Table'
    });
    $('.l-Materialized_View').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'Views are always materialized and incrementally maintained.'
    });
    $('.l-Replicated').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'Table data is present at all partitions. Updates are a multi-partition operation.'
    });
    $('.l-Partitioned').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'Table data is partitioned on the hash of a column. Data is replicated to k+1 hosts.'
    });
    $('.l-Has-PKey').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'The table has a primary key and corresponding index.'
    });
    $('.l-No-PKey').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'The table has no primary key. Deletes and updates will require scans.'
    });
    $('.l-Read').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure doesn\'t alter the content of the database.'
    });
    $('.l-Write').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure can alter the contents of the database.'
    });
    $('.l-Single').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure only runs at one partition.'
    });
    $('.l-Multi').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure runs at all partitions, which can affect performance.'
    });
    $('.l-Java').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure uses Java code for logic.'
    });
    $('.l-Single-Stmt').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure was defined in the DDL file as a single SQL statement.'
    });
    $('.l-Scans').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This procedure or statement might be using full table scans. While this isn\'t ' +
            'always bad, it\'s possible adding an index could improve performance.'
    });
    $('.l-Determinism').popover({
        trigger: 'focus hover', delay: { show: 500, hide: 100 }, placement: 'top', content:
            'This statement or procedure could potentially use SQL that outputs non-deterministic ' +
            'results which then may be used to modify state. If this happens, the cluster will ' +
            'halt. Consider adding an ORDER BY clause to make a SELECT statement deterministic.'
    });

    sizes_update_all();
});

function numberWithCommas(n) {
    n = Math.round(n / 1000.0);
    var parts = n.toString().split(".");
    return parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",") + (parts[1] ? "." + parts[1] : "") + " K";
}

function size_table_element(name, nrow) {
    var id = "s-size-".concat(nrow, '-', name);
    var e = document.getElementById(id);
    return e;
}

function size_table_value(name, nrow) {
    var elem = size_table_element(name, nrow);
    var value = Math.floor(name == "count" ? elem.value : elem.innerHTML);
    return value;
}

function size_table_update(name, nrow, value) {
    var elem = size_table_element(name, nrow);
    if (elem != null) {
        elem.innerHTML = numberWithCommas(value);
    }
}

function size_summary_element(type, name) {
    var id = "s-size-summary-".concat(type, '-', name);
    var e = document.getElementById(id);
    return e;
}

function size_summary_value(type, name) {
    var elem = size_summary_element(type, name);
    var value = Math.floor(elem.innerHTML);
    return value;
}

function size_summary_update(type, name, value) {
    var elem = size_summary_element(type, name);
    if (elem != null) {
        elem.innerHTML = numberWithCommas(value);
    }
}

function sizes_get_row(nrow) {
    return {
        "count": size_table_value("count", nrow),
        "rmin": size_table_value("rmin", nrow),
        "rmax": size_table_value("rmax", nrow),
        "imin": size_table_value("imin", nrow),
        "imax": size_table_value("imax", nrow),
        "tmin": size_table_value("tmin", nrow),
        "tmax": size_table_value("tmax", nrow)
    };
}

// Do the size worksheet calculations for a single row.
function sizes_update_row(nrow) {
    var v = sizes_get_row(nrow);
    size_table_update("tmin", nrow, (v["rmin"] + v["imin"]) * v["count"]);
    size_table_update("tmax", nrow, (v["rmax"] + v["imax"]) * v["count"]);
}

/*
 * Update the size worksheet summary.
 */
function sizes_update_summary() {
    var ntables = size_summary_value("table", "count");
    var nviews = size_summary_value("view", "count");
    var nrow = 0;
    var tmin = 0;
    var tmax = 0;
    var vmin = 0;
    var vmax = 0;
    var imin = 0;
    var imax = 0;
    for (var ntable = 0; ntable < ntables; ++ntable) {
        var v = sizes_get_row(++nrow);
        tmin += (v["rmin"] * v["count"]);
        tmax += (v["rmax"] * v["count"]);
        imin += (v["imin"] * v["count"]);
        imax += (v["imax"] * v["count"]);
    }
    for (var nview = 0; nview < nviews; ++nview) {
        var v = sizes_get_row(++nrow);
        vmin += (v["rmin"] * v["count"]);
        vmax += (v["rmax"] * v["count"]);
        imin += (v["imin"] * v["count"]);
        imax += (v["imax"] * v["count"]);
    }
    size_summary_update("table", "min", tmin);
    size_summary_update("table", "max", tmax);
    size_summary_update("view", "min", vmin);
    size_summary_update("view", "max", vmax);
    size_summary_update("index", "min", imin);
    size_summary_update("index", "max", imax);
    size_summary_update("total", "min", tmin + vmin + imin);
    size_summary_update("total", "max", tmax + vmax + imax);
}

/*
 * Do all the size worksheet calculations at once.
 * Call sizes_update_row() when a single row is changed.
 */
function sizes_update_all() {
    // Use the summary counts to determine the number of table rows.
    var ntables = size_summary_value("table", "count");
    var nviews = size_summary_value("view", "count");
    for (var nrow = 1; nrow <= ntables + nviews; ++nrow) {
        sizes_update_row(nrow);
    }
    sizes_update_summary();
    
    //Update the table for sorting.
    $("#sizetable").trigger("update");
}