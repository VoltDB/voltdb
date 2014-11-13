// JavaScript Document

$( document ).ready(function() { 
   // Make Expandable Rows.
   $('tr.parent')
		   .css("cursor","pointer")
		   .attr("title","Click to expand/collapse")
		   .click(function(){
				   $(this).siblings('.child-'+this.id).toggle();
				   $(this).find(".labelCollapsed").toggleClass("labelExpanded");
		   });
   $('tr[class^=child-]').hide().children('td');
	
	// btnServerConfigAdmin
	$('#btnServerConfigAdmin').click(function(){
			$('#serverConfigAdmin').slideToggle("slide");
		});
		
	// Implements Scroll in Server List div
		$('#serverListWrapperAdmin').slimscroll({
		  disableFadeOut: true,
		  height:'225px'	  
		});
		
	
	// Cluster Controls
	$('.adminClusterList a').click(function(){
			var theClass;
			theClass = $(this).attr('class');	
			
			//alert(theClass);
			
			if(theClass == 'resume'){
				$(this).removeClass('resume').addClass('pause');
				$(this).html('Resume');
				$('.adminD').hide();
				$('.adminE').show();
				
			}
			if(theClass == 'pause'){
				$(this).removeClass('pause').addClass('resume');
				$(this).html('Pause');
				$('.adminE').hide();
				$('.adminD').show();
				
			}
			if(theClass == 'restore'){
				$(this).removeClass('restore').addClass('save');
				$(this).html('Save');
			}
			
			if(theClass == 'save'){
				$(this).removeClass('save').addClass('restore');
				$(this).html('Restore');
			}
			
			if(theClass == 'stop'){
				$(this).removeClass('stop').addClass('shutdown');
				$(this).html('Shutdown');
			}
			if(theClass == 'shutdown'){
				$(this).removeClass('shutdown').addClass('stop');
				$(this).html('Stop');
			}
		})	
	

});