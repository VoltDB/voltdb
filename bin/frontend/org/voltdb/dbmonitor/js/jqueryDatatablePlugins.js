/**
 * Datatable plugin pagination style
 */
$.fn.dataTableExt.oPagination.extStyleLF = {
	/*
	* Function: oPagination.extStyle.fnInit
	* Purpose:  Initalise dom elements required for pagination with a list of the pages
	* Returns:  -
	* Inputs:   object:oSettings - dataTables settings object
	*           node:nPaging - the DIV which contains this pagination control
	*           function:fnCallbackDraw - draw function which must be called on update
	*/
	"fnInit": function (oSettings, nPaging, fnCallbackDraw) {
	    
	    /*
	    *	code to create pagination buttons
	    */
	    
	    nPrevious = $('<span />', { 'class': 'paginate_button previous' });
	    nNext = $('<span />', { 'class': 'paginate_button next' });
	    
	    /*
	    *	code to create pagination information field
	    */
	    
	    var navigationLabel = 'Page <span class="pageIndex">#</span> of <span class="totalPages">TOTAL</span>';

	    navLabel = $('<div />', { html: navigationLabel, 'class': 'navigationLabel'});
	    
	    $(nPaging)
	        .append(nPrevious)
	        .append(navLabel)
	        .append(nNext);
	
	    nPrevious.click(function () {
	        oSettings.oApi._fnPageChange(oSettings, "previous");
	        fnCallbackDraw(oSettings);
	    }).bind('selectstart', function () { return false; });
	
	    nNext.click(function () {
	        oSettings.oApi._fnPageChange(oSettings, "next");
	        fnCallbackDraw(oSettings);
	    }).bind('selectstart', function () { return false; });
	
	},
	
	/*
	* Function: oPagination.extStyle.fnUpdate
	* Purpose:  Update the list of page buttons shows
	* Returns:  -
	* Inputs:   object:oSettings - dataTables settings object
	*           function:fnCallbackDraw - draw function which must be called on update
	*/
	"fnUpdate": function (oSettings, fnCallbackDraw) {
	    if (!oSettings.aanFeatures.p) {
	        return;
	    }
	
	    /* Loop over each instance of the pager */
	    var an = oSettings.aanFeatures.p;
	    for (var i = 0, iLen = an.length; i < iLen; i++) {
	        //var buttons = an[i].getElementsByTagName('span');
	        var buttons = $(an[i]).find('span.paginate_button');
	        if (oSettings._iDisplayStart === 0) {
	            buttons.eq(0).attr("class", "paginate_disabled_previous paginate_button");
	        }
	        else {
	            buttons.eq(0).attr("class", "paginate_enabled_previous paginate_button");
	        }
	
	        if (oSettings.fnDisplayEnd() == oSettings.fnRecordsDisplay()) {
	            buttons.eq(1).attr("class", "paginate_disabled_next paginate_button");
	        }
	        else {
	            buttons.eq(1).attr("class", "paginate_enabled_next paginate_button");
	            }
	        }
	  }
};


/**
 * Datatable plugin page information
 */
$.fn.dataTableExt.oApi.fnPagingInfo = function ( oSettings )
{
  return {
    "iStart":         oSettings._iDisplayStart,
    "iEnd":           oSettings.fnDisplayEnd(),
    "iLength":        oSettings._iDisplayLength,
    "iTotal":         oSettings.fnRecordsTotal(),
    "iFilteredTotal": oSettings.fnRecordsDisplay(),
    "iPage":          Math.ceil( (oSettings._iDisplayStart + oSettings._iDisplayLength) / oSettings._iDisplayLength ),
    "iTotalPages":    Math.ceil( oSettings.fnRecordsDisplay() / oSettings._iDisplayLength )
  };
}