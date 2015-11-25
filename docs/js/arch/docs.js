
// Set the active status in the main menu bar
function endsWith(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

function markMenuBarActive() { 
	$('#mainNavBar li a').each(function(index) { 
		if (endsWith(window.location.href, $(this).attr('href'))) { 
			console.log( 'Marking ' + $(this).attr('href') + ' as active');
			$(this).parent().addClass('active');
		}});
}

$( document ).ready(function() { markMenuBarActive(); } );
