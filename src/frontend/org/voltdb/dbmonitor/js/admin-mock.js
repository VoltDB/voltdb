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

});