/*-------------------------------

	POPUP.JS

	Simple Popup plugin for jQuery

	@author Todd Francis
	@version 2.2.0

-------------------------------*/

;(function($, window){

	'use strict';

	/**
	 * Popup jQuery method
	 *
	 * @param  {Object} settings
	 * @return {Object}
	 */
	$.fn.popup = function(settings){

		var selector = this.selector,
			popup = new $.Popup(settings),
			content = settings && settings.content
				? settings.content
				: $(this).attr('href');

		$(document)
			.on('click.popup', selector, function(e){

				e.preventDefault();
				popup.open(content, undefined, this);

			});

		return this.each(function(){

			$(this)
				.data('popup', popup);

		});

	};

	/**
	 * Main Popup Class
	 *
	 * @param {Object} settings
	 */
	$.Popup = function(settings) {

	    var p = this,
	        defaults = {
	            // Markup
	            backClass: 'popup_back',
	            backOpacity: 0.7,
	            containerClass: 'popup_cont',
	            closeContent: '<div class="popup_close">&times;</div>',
	            markup: '<div class="popup"><div class="popup_content"/></div>',
	            contentClass: 'popup_content',
	            preloaderContent: '<p class="preloader">Loading</p>',
	            activeClass: 'popup_active',
	            hideFlash: false,
	            speed: 200,
	            popupPlaceholderClass: 'popup_placeholder',
	            keepInlineChanges: true,

	            // Content
	            modal: false,
	            content: null,
	            type: 'auto',
	            width: null,
	            height: null,

	            // Params
	            typeParam: 'pt',
	            widthParam: 'pw',
	            heightParam: 'ph',

	            // Callbacks
	            beforeOpen: function(type) {
	            },
	            afterOpen: function() {
	            },
	            beforeClose: function() {
	            },
	            afterClose: function() {
	            },
	            error: function() {
	            },
	            save: function() {
	            },
	            login: function() {
	            },
	            autoLogin: function() {
	            },
	            closeDialog: function() {
	            },
				
				show : function($popup, $back){

					var plugin = this;

					// Center the popup
					plugin.center();

					// Animate in
					$popup
						.animate({opacity : 1}, plugin.o.speed, function(){
						    $('body').css("height", $(window).height());
							$('body').css("overflow", "hidden");
							$('body').css("position", "fixed");
							$('body').css("overflow-y", "scroll");
							$('body').css("width", "100%");
							//$('body').bind('touchmove', function(e){e.preventDefault()});//mobile
							// Call the open callback
							plugin.o.afterOpen.call(plugin);
						});

				},
				replaced : function($popup, $back){

					// Center the popup and call the open callback
					this
						.center()
						.o.afterOpen.call(this);

				},
				hide: function ($popup, $back) {
					if( $popup !== undefined ){

						// Fade the popup out
						$popup.animate({opacity : 0}, this.o.speed);
						$('body').css("height", $(window).height()); $('body').css("overflow", "auto");
						$('body').css("position", "static");
						$('body').css("width", "auto");
					 //$('body').unbind('touchmove');//mobile
					}

				},
				types : {
					inline : function(content, callback){

						var $content = $(content);

						$content
							.addClass(p.o.popupPlaceholderClass);

						// If we don't want to keep any inline changes,
						// get a fresh copy now
						if( !p.o.keepInlineChanges ){
							cachedContent = $content.html();
						}

						callback.call(this, $content.children());

					},
					image : function(content, callback){

						var plugin = this;

						var $image = $('<img />')
							.one('load', function(){

								var img = this;

								// Timeout for Webkit
								// As the width/height of the image is 0 initially
								setTimeout(function(){

									callback.call(plugin, img);

								}, 0);

							})
							.one('error', function(){

								p.o.error.call(p, content, 'image');

							})
							.attr('src', content)
							.each(function() {

								if( this.complete ){

									$(this).trigger('load');

								}

							});

					},
					external : function(content, callback){

						var $frame = $('<iframe />')
							.attr({
								src : content,
								frameborder : 0,
								width : p.width,
								height : p.height
							});

							callback.call(this, $frame);

					},
					html					: function(content, callback){

						callback.call(this, content);

					},
					jQuery					: function(content, callback){

						callback.call(this, content.html());

					},
					'function'				: function(content, callback){

						callback.call(this, content.call(p));

					},
					ajax					: function(content, callback){

						$.ajax({
							url : content,
							success : function(data){
								callback.call(this, data);
							},
							error : function(data){
								p.o.error.call(p, content, 'ajax');
							}
						});

					}
				}
			},
			imageTypes = ['png', 'jpg', 'gif'],
			type,
			cachedContent,
			$back,
			$pCont,
			$close,
			$preloader,
			$p;

		p.ele = undefined;
		
		p.o = $.extend(true, {}, defaults, settings);
		
		/**
		 * Opens a new popup window
		 *
		 * @param  {string} content
		 * @param  {string} popupType
		 * @param  {Object} ele
		 * @return {void}
		 */
		p.open = function (content, popupType, ele) {
		    
		    var saveBtn = $("a[id='savePreference']");
		    if (saveBtn != undefined) {
		        saveBtn.unbind('click');
		        saveBtn.bind('click', function () {
		            p.close();
		            p.o.save();
		        });
		    }
		    
		    var loginBtn = $("input[id='LoginBtn']");
		    if (loginBtn != undefined) {
		        loginBtn.unbind('click');
		        loginBtn.bind('click', function () {
		            p.o.login(function() {
		                p.close();
		            });
		        });
		    }
		    
		    var serverUnavailableBtn = $("a[id='serverUnavailableBtn']");
		    if (serverUnavailableBtn != undefined) {
		        serverUnavailableBtn.unbind('click');
		        serverUnavailableBtn.bind('click', function () {
		            p.o.autoLogin(function () {
		                p.close();
		            });
		        });
		    }

		    var errorMsgBtn = $("a[id='btnOk']");
		    if (errorMsgBtn != undefined) {
		        errorMsgBtn.unbind('click');
		        errorMsgBtn.bind('click', function () {
		            p.close();
		        });
		    }
		    
		    var btnSaveSnapshotStatus = $("a[id='btnSaveSnapshotStatus']");
		    if (btnSaveSnapshotStatus != undefined) {
		        btnSaveSnapshotStatus.unbind('click');
		        btnSaveSnapshotStatus.bind('click', function () {
		            p.close();
		        });
		    }

		    var cancelBtn = $("a[id='btnCancel']");
		    if (cancelBtn != undefined) {
		        cancelBtn.unbind('click');
		        cancelBtn.bind('click', function (e) {
		            p.close();
		            e.preventDefault();
		        });
		    }

		    var connectionBtn = $("a[id='btnConOk']");
		    if (connectionBtn != undefined) {
		        connectionBtn.unbind('click');
		        connectionBtn.bind('click', function (e) {
		            e.preventDefault();
		            p.o.closeDialog();
		            VoltDBCore.isServerConnected = true;
		            p.close();
		        });
		    }

		    var serverShutdownBtn = $("a[id=btnServerShutdownOk]");
		    if (serverShutdownBtn != undefined) {
		        serverShutdownBtn.unbind('click');
		        serverShutdownBtn.bind('click', function (e) {
		            e.preventDefault();
		            p.o.closeDialog();
		            VoltDBCore.isServerConnected = true;
		            p.close();
		        });
		    }

			//save cluster
			$('.saveBtn').click(function(){
				$('.saveInfo').hide();
				$('.saveConfirmation').show();
			});
			
			$('.confirmNoSave').click(function(){
				$('.saveConfirmation').hide();
				$('.saveInfo').show();
			});
			
				
			
			
			
			//admin
			var closeBtn = $(".closeBtn");
		    if (closeBtn != undefined) {
		        closeBtn.unbind('click');
		        closeBtn.bind('click', function () {
					$('.saveConfirmation').hide();
					$('.saveInfo').show();
					$('.restoreConfirmation').hide();
					$('.restoreInfo').show();
		            p.close();
		    });
		    }
			
			// Get the content
			content = ( content === undefined || content === '#' )
				? p.o.content
				: content;

			// If no content is set
			if( content === null ) {
				p.o.error.call(p, content, type);
				return false;

			}

			// Was an element passed in?
			if( ele !== undefined ){

				// Remove current active class
				if( p.ele && p.o.activeClass ){

					$(p.ele).removeClass(p.o.activeClass);

				}

				// Record the element
				p.ele = ele;

				// Add an active class
				if( p.ele && p.o.activeClass ){

					$(p.ele).addClass(p.o.activeClass);

				}

			}

			// If we're not open already
			if( $back === undefined ) {
				// Create back and fade in
				$back = $('<div class="'+p.o.backClass+'"/>')
					.appendTo($('body'))
					.css('opacity', 0)
					.animate({
						opacity : p.o.backOpacity
					}, p.o.speed);

                
			    // If modal isn't specified, bind click event
				if (!p.o.modal && p.ele.id != "loginLink" && p.ele.id != "conPopup" && p.ele.id != "serUnavailablePopup") {

					$back.one('click.popup', function(){
						p.close();
					});

				}

				// Should we hide the flash?
				if( p.o.hideFlash ){

					$('object, embed').css('visibility', 'hidden');

				}

				// Preloader
				if( p.o.preloaderContent ){

					$preloader = $(p.o.preloaderContent)
						.appendTo($('body'));

				}

			}

			// Get the popupType
			popupType = getValue([popupType, p.o.type]);

			// If it's at auto, guess a real type
			popupType = ( popupType === 'auto' )
				? guessType(content)
				: popupType;

			// Cache the type to use globally
			type = popupType;

			// Do we have a width set?
			p.width = ( p.o.width )
				? p.o.width
				: null;

			// Do we have a height set?
			p.height = ( p.o.height )
				? p.o.height
				: null;

			// If it's not inline, jQuery or a function
			// it might have params, and they are top priority
			if( $.inArray(popupType, ['inline', 'jQuery', 'function']) === -1 ){

				var paramType = getParameterByName(p.o.typeParam, content),
					paramWidth = getParameterByName(p.o.widthParam, content),
					paramHeight = getParameterByName(p.o.heightParam, content);

				// Do we have an overriding paramter?
				popupType = ( paramType !== null )
					? paramType
					: popupType;

				// Do we have an overriding width?
				p.width = ( paramWidth !== null )
					? paramWidth
					: p.width;

				// Do we have an overriding height?
				p.height = ( paramHeight !== null )
					? paramHeight
					: p.height;
			}

			// Callback
			p.o.beforeOpen.call(p, popupType);

			// Show the content based
			if( p.o.types[popupType] ){

				p.o.types[popupType].call(p, content, showContent);

			}else{

				p.o.types.ajax.call(p, content, showContent);

			}

		    if (p.o.open != undefined) {
		        // Call the open callback
		        p.o.open.call(p, content, showContent);
		        //return true;
		    }
		};

		/**
		 * Return the correct value to be used
		 *
		 * @param  {array} items
		 * @return {mixed}
		 */
		function getValue(items){

			var finalValue;

			$.each(items, function(i, value){

				if( value ){
					finalValue = value;
					return false;
				}

			});

			return finalValue;

		}

		/**
		 * Guess the type of content to show
		 *
		 * @param  {string|Object|function} content
		 * @return {string}
		 */
		function guessType(content){

			if( typeof content === 'function' ){

				return 'function';

			} else if( content instanceof $ ){

				return 'jQuery';

			} else if( content.substr(0, 1) === '#' || content.substr(0, 1) === '.' ){

				return 'inline';

			} else if( $.inArray(content.substr(content.length - 3), imageTypes) !== -1 ) {

				return 'image';

			} else if( content.substr(0, 4) === 'http' ) {

				return 'external';

			}else{

				return 'ajax';

			}

		}

		/**
		 * Shows the content
		 *
		 * @param  {string} content
		 * @return {void}
		 */
		function showContent(content){

			// Do we have a preloader?
			if( $preloader ){

				// If so, hide!
				$preloader.fadeOut('fast', function(){

					$(this).remove();

				});

			}

			// Presume we're replacing
			var replacing = true;

			// If we're not open already
			if( $pCont === undefined ){

				// We're not replacing!
				replacing = false;

				// Create the container
				$pCont = $('<div class="'+p.o.containerClass+'">');

				// Add in the popup markup
				$p = $(p.o.markup)
					.appendTo($pCont);
			    
			    // Add in the close button
				if (p.ele.id != "loginLink" && p.ele.id != "conPopup" && p.ele.id != "serUnavailablePopup") {
			        $close = $(p.o.closeContent)
			            .one('click', function() {

			                p.close();

			            })
			            .appendTo($pCont);
			    }

			    // Bind the resize event
				$(window).resize(p.center);

				// Append the container to the body
				// and set the opacity
				$pCont
					.appendTo($('body'))
					.css('opacity', 0);

			}

			// Get the actual content element
			var $pContent = $('.'+p.o.contentClass, $pCont);

			// Do we have a set width/height?
			if( p.width ){

				$pContent.css('width', p.width, 10);

			}else{

				$pContent.css('width', '');
			}

			if( p.height ){

				$pContent.css('height', p.height, 10);

			}else{

				$pContent.css('height', '');

			}

			// Put the content in place!
			if( $p.hasClass(p.o.contentClass) ){

				$p
					.html(content);

			}else{

				$p
					.find('.'+p.o.contentClass)
					.html(content);

			}

			// Callbacks!
			if( !replacing ){

				p.o.show.call(p, $pCont, $back);

			}else{

				p.o.replaced.call(p, $pCont, $back);

			}

		}

		/**
		 * Close the popup
		 *
		 * @return {Object}
		 */
		p.close = function(){

			p.o.beforeClose.call(p);

			// If we got some inline content, cache it
			// so we can put it back
			if(
				type === 'inline' &&
				p.o.keepInlineChanges
			){
				cachedContent = $('.'+p.o.contentClass).html();
			}

			if( $back !== undefined ){

				// Fade out the back
				$back.animate({opacity : 0}, p.o.speed, function(){

					// Clean up after ourselves
					p.cleanUp();

				});

			}

			// Call the hide callback
			p.o.hide.call(p, $pCont, $back);
		    

			return p;

		};

		/**
		 * Clean up the popup
		 *
		 * @return {Object}
		 */
		p.cleanUp = function(){

			$back
				.add($pCont)
				.remove();

			$pCont = $back = undefined;

			// Unbind the resize event
			$(window).unbind('resize', p.center);

			// Did we hide the flash?
			if( p.o.hideFlash ){

				$('object, embed').css('visibility', 'visible');

			}

			// Remove active class if we can
			if( p.ele && p.o.activeClass ){

				$(p.ele).removeClass(p.o.activeClass);

			}

			var $popupPlaceholder = $('.'+p.o.popupPlaceholderClass);

			// If we got inline content
			// put it back
			if(
				type == 'inline' &&
				$popupPlaceholder.length
			){
				$popupPlaceholder
					.html(cachedContent)
					.removeClass(p.o.popupPlaceholderClass);
			}

			type = null;

			// Call the afterClose callback
			p.o.afterClose.call(p);

			return p;

		};

		/**
		 * Centers the popup
		 *
		 * @return {Object}
		 */
		p.center = function(){

			$pCont.css(p.getCenter());

			// Only need force for IE6
			$back.css({
				height : document.documentElement.clientHeight
			});

			return p;

		};

		/**
		 * Get the center co-ordinates
		 *
		 * Returns the top/left co-ordinates to
		 * put the popup in the center
		 *
		 * @return {Object} top/left keys
		 */
		p.getCenter = function(){

			var pW = $pCont.children().outerWidth(true),
				pH = $pCont.children().outerHeight(true),
				wW = document.documentElement.clientWidth,
				wH = document.documentElement.clientHeight;

			return {
				top : wH * 0.5 - pH * 0.5,
				left : wW * 0.5 - pW * 0.5
			};

		};

		/**
		 * Get parameters by name
		 * @param  {string} name
		 * @return {null|string} null if not found
		 */
		function getParameterByName(name, url){

			var match = new RegExp('[?&]'+name+'=([^&]*)')
				.exec(url);

			return match && decodeURIComponent(match[1].replace(/\+/g, ' '));

		}

	};

}(jQuery, window));
