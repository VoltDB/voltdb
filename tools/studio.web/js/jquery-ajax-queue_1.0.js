/**
 * Ajax Queue Plugin
 * 
 * Homepage: http://jquery.com/plugins/project/ajaxqueue
 * Documentation: http://docs.jquery.com/AjaxQueue
 */

/**

<script>
$(function(){
	jQuery.ajaxQueue({
		url: "test.php",
		success: function(html){ jQuery("ul").append(html); }
	});
	jQuery.ajaxQueue({
		url: "test.php",
		success: function(html){ jQuery("ul").append(html); }
	});
	jQuery.ajaxSync({
		url: "test.php",
		success: function(html){ jQuery("ul").append("<b>"+html+"</b>"); }
	});
	jQuery.ajaxSync({
		url: "test.php",
		success: function(html){ jQuery("ul").append("<b>"+html+"</b>"); }
	});
});
</script>
<ul style="position: absolute; top: 5px; right: 5px;"></ul>

 */
/*
 * Queued Ajax requests.
 * A new Ajax request won't be started until the previous queued 
 * request has finished.
 */
jQuery.ajaxQueue = function(o){
	var _old = o.complete;
	o.complete = function(){
		if ( _old ) _old.apply( this, arguments );
		jQuery.dequeue( jQuery.ajaxQueue, "ajax" );
	};

	jQuery([ jQuery.ajaxQueue ]).queue("ajax", function(){
		jQuery.ajax( o );
	});
};

/*
 * Synced Ajax requests.
 * The Ajax request will happen as soon as you call this method, but
 * the callbacks (success/error/complete) won't fire until all previous
 * synced requests have been completed.
 */
jQuery.ajaxSync = function(o){
	var fn = jQuery.ajaxSync.fn, data = jQuery.ajaxSync.data, pos = fn.length;
	
	fn[ pos ] = {
		error: o.error,
		success: o.success,
		complete: o.complete,
		done: false
	};

	data[ pos ] = {
		error: [],
		success: [],
		complete: []
	};

	o.error = function(){ data[ pos ].error = arguments; };
	o.success = function(){ data[ pos ].success = arguments; };
	o.complete = function(){
		data[ pos ].complete = arguments;
		fn[ pos ].done = true;

		if ( pos == 0 || !fn[ pos-1 ] )
			for ( var i = pos; i < fn.length && fn[i].done; i++ ) {
				if ( fn[i].error ) fn[i].error.apply( jQuery, data[i].error );
				if ( fn[i].success ) fn[i].success.apply( jQuery, data[i].success );
				if ( fn[i].complete ) fn[i].complete.apply( jQuery, data[i].complete );

				fn[i] = null;
				data[i] = null;
			}
	};

	return jQuery.ajax(o);
};

jQuery.ajaxSync.fn = [];
jQuery.ajaxSync.data = [];

