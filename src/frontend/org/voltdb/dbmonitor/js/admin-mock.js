// JavaScript Document

$( document ).ready(function() { 


$(".tblshutdown").find(".edit").on("click",function(){	
	var $this=$(this).closest("tr");
		
	var tdVal=$this.find("td:nth-child(3)");;
	var val=tdVal.text();
	
	
	if(val=="On"){
		$this.find("td:nth-child(2)").find("div").removeClass("onIcon").addClass("offIcon");
		tdVal.text("Off");
		}else{
			$this.find("td:nth-child(2)").find("div").removeClass("offIcon").addClass("onIcon");
			tdVal.text("On");
			}
			
		
	
	
	
	
	})
   // Make Expandable Rows.
    $('tr.parent > td:first-child' || 'tr.parent > td:fourth-child')
        .css("cursor", "pointer")
        .attr("title", "Click to expand/collapse")
        .click(function () {
            var parent = $(this).parent();
            parent.siblings('.child-' + parent.attr("id")).toggle();
            parent.find(".labelCollapsed").toggleClass("labelExpanded");
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
			
					
			
			
			/*if(theClass == 'resume'){
				$(this).removeClass('resume').addClass('pause');
				$(this).html('Resume');
				$(this).attr('id','resumeConfirmation');
				$(this).attr('href','#resumeConfirmationPop');
				
				
			}
			if(theClass == 'pause'){
				$(this).removeClass('pause').addClass('resume');
				$(this).html('Pause');
				$(this).attr('id','pauseConfirmation');	
				$(this).attr('href','#pauseConfirmationPop');			
				
				
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
			}*/
		})
		
		$('#pauseConfirmation').click(function(){
			$('#resumeConfirmation').show();
			$(this).hide();
		});
		
		$('#resumeConfirmation').click(function(){
			$('#pauseConfirmation').show();
			$(this).hide();
		});
		 
		
		//$('a.shutdown').click(function(){
			    $('#shutDownConfirmation').popup();
		//});	
		
		//$('a.pause').click(function(){
			    $('#pauseConfirmation').popup();
		//});	
		
		//$('a.resume').click(function(){
			    $('#resumeConfirmation').popup();
		//});
		 $('#securityEdit').popup();
		 $('#autoSnapshotEdit').popup();
		 $('#saveConfirmation').popup();
		 $('#restoreConfirmation').popup();
		 $('#btnSaveHrtTimeOut').popup();
		 $('#shutdownPopupPopConfirmation').popup();
		  $('#shutdownPopupPopConfirmation1').popup();
		  $('#shutdownClusterPopupPopConfirmation').popup();
		 // $('.saveConfirmation').popup();
		 // $('.restoreConfirmation').popup();
		  
		  
	
	
		 
		 $('#autoSnapshotSave').popup({
		  	saveSnaps: function () {
					$("#frequencySpan").show();
					$("#txtFrequency").hide();
					$("#frequencySpan").html($("#txtFrequency").val())
					
					$("#retainedSpan").show();
					$("#txtRetained").hide();
					$("#retainedSpan").html($("#txtRetained").val())
					
					$("#autoSnapshotSave").hide();
					$("#autoSnapshotEdit").show();
					
				}
		 });

		 
		 	
		$('#btnEditHrtTimeOut').click(function(e){
			if(  $("#hrtTimeOutSpan").is(":visible") == true ){
				$('#btnEditHrtTimeOut').hide();
				$('#btnSaveHrtTimeOut').show();
				$("#hrtTimeOutSpan").hide();
				$("#txtHrtTimeOutSpan").show();
				$("#txtHrtTimeOutSpan").val($("#hrtTimeOutSpan").html())
			}else{
				$("#hrtTimeOutSpan").show();
				$("#txtHrtTimeOutSpan").hide();
				$("#hrtTimeOutSpan").html($("#txtHrtTimeOutSpan").val())
			}
			e.preventDefault();
		});
		
		$('#btnSaveHrtTimeOut').click(function(e){
			$("#hrtTimeOutSpan").show();
			$("#txtHrtTimeOutSpan").hide();
			$('#btnEditHrtTimeOut').show();
			$('#btnSaveHrtTimeOut').hide();
			$("#hrtTimeOutSpan").html($("#txtHrtTimeOutSpan").val())
		});
		
		$('#autoSnapshotEdit').click(function(){
			var parent = $(this).parent().parent();
            parent.siblings('.child-' + parent.attr("id")).show();
            parent.find(".labelCollapsed").addClass("labelExpanded");

            $("#autoSnapshotIcon").hide();
            $("#chkAutoSnapshot").show();
			
			$("#autoSnapshotSave").show();
			$("#autoSnapshotEdit").hide();
			
			$("#frequencySpan").hide();
			$("#txtFrequency").show();
		    $("#txtFrequency").val($("#frequencySpan").html());
			
			$("#retainedSpan").hide();
			$("#txtRetained").show();
		    $("#txtRetained").val($("#retainedSpan").html());
		});
		
	 // Filters servers list
    $('#popServerSearchAdmin').keyup(function () {
        var that = this;
        $.each($('.tblshutdown tbody tr'),
        function (i, val) {
            if ($(val).text().indexOf($(that).val().toLowerCase()) == -1) {
                $('.tblshutdown tbody tr').eq(i).hide();
            } else {
                $('.tblshutdown tbody tr').eq(i).show();
            }
        });
    });	
	
	// Hides opened serverlist
		$(document).on('click', function (e) {
			if ( !$(event.target).hasClass('adminIcons')) {
				if ($(e.target).closest("#serverConfigAdmin").length === 0) {
					$("#serverConfigAdmin").hide();
				}
			}
		}); 
		

   
});

