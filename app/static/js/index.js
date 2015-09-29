/**
 * Main JS file for Casper behaviours
 */
var $post = $('.post'),
	$first = $('.post.first'),
	$last = $('.post.last'),
	$fnav = $('.fixed-nav'),
	$postholder = $('.post-holder'),
	$postafter = $('.post-after'),
	$sitehead = $('#site-head');

/*globals jQuery, document */
(function ($) {
    "use strict";
    function srcTo (el) {
    	$('html, body').animate({
			scrollTop: el.offset().top
		}, 500);
    }
    $(document).ready(function(){

        // $postholder.each(function (e) {
        // 	if(e % 2 != 0)
        // 		$(this).css({
        //             // 'background': '#e782a4',
        //             // 'color'     : 'white',
        //         })
        // })

        $postafter.each(function (e) {
        	var bg = $(this).parent().css('background-color')
        	$(this).css('border-top-color', bg)

        	if(e % 2 == 0)
        		$(this).css('left', '6%')

        })

        $('.btn').click( function () {
            var url = $(this).attr('url')
            srcTo ($(".post[url='"+url+"']"))
        })

        $('#header-arrow').click(function () {
            srcTo ($first)
        })

        $('.post-title').each(function () {

        	$('.fn-item').click(function () {
                var url = $(this).attr('url')
                srcTo ($(".post[url='"+url+"']"))

        	})
        })

        // Begin faded out
        $('.fixed-nav').fadeOut('fast')

        $('.post.last').next('.post-after').hide();
        $(window).scroll( function () {
        	var w = $(window).scrollTop(),
        		g = $('#site-head').offset().top - 200,
        		h = $('#site-head').offset().top + $(this).height()-500;

                // alert( w, g, h);

            if(w >= g && w<=h) {
            // if(false) {
        		$('.fixed-nav').fadeOut('fast')
        	} else {
                if($(window).width()>500)
        		  $('.fixed-nav').fadeIn('fast')
        	}

        	$post.each(function () {
        		var f = $(this).offset().top,
        			b = $(this).offset().top + $(this).height(),
        			t = $(this).parent('.post-holder').index(),
        		 	i = $(".fn-item[item_index='"+t+"']"),
        		 	a = $(this).parent('.post-holder').prev('.post-holder').find('.post-after');

        		 $(this).attr('item_index', t);

        		if(w >= f && w<=b) {

        			i.addClass('active');
        			a.fadeOut('slow')
        		} else {
        			i.removeClass('active');
        			a.fadeIn('slow')
        		}
        	})
        });
        // $('li').before('<span class="bult icon-asterisk"></span>')
        // $('blockquote p').prepend('<span class="quo icon-quote-left"></span>')
        //     .append('<span class="quo icon-quote-right"></span>')

    });

}(jQuery));
