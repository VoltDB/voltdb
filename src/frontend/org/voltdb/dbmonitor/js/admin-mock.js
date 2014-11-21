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
	
	$(".adminTbl1").find(".edit").on("click",function(){	
	var $this=$(this).closest("tr.security");
		
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
		
	
		
		$('#pauseConfirmation').click(function(){
			$('#resumeConfirmation').show();
			$(this).hide();
		});
		
		$('#resumeConfirmation').click(function(){
			$('#pauseConfirmation').show();
			$(this).hide();
		});
		 
		
		
		$('#shutDownConfirmation').popup();
		
		$('#pauseConfirmation').popup();
		
		$('#resumeConfirmation').popup();
		
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
					$(".icheckbox_square-aero").removeClass('customCheckbox')
					
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
			$("td.heartbeattd span").toggleClass("unit");
			e.preventDefault();
		});
		
		$('#btnSaveHrtTimeOut').click(function(e){
			$("#hrtTimeOutSpan").show();
			$("#txtHrtTimeOutSpan").hide();
			$('#btnEditHrtTimeOut').show();
			$('#btnSaveHrtTimeOut').hide();
			$("#hrtTimeOutSpan").html($("#txtHrtTimeOutSpan").val())
			$("td.heartbeattd span").toggleClass("unit");
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
			$(".icheckbox_square-aero").addClass('customCheckbox')
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
	// Checkbox style
	
	  $('input').iCheck({
		checkboxClass: 'icheckbox_square-aero',
		//radioClass: 'iradio_square',
		increaseArea: '20%' // optional
	
		});		

   
});

