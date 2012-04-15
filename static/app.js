$(document).bind("mobileinit", function() {
  $.mobile.page.prototype.options.addBackBtn = true;
});

$(document).delegate("#artindex", "pageinit", function() {
  $("#stopForm").validate();
});

$(document).delegate("#wmatabus", "pageinit", function() {
  $("#stopForm").validate();
});

$(document).delegate("#geo", "pageinit", function() {
  function get_location() {
    if (true /*Modernizr.geolocation*/) {
      navigator.geolocation.getCurrentPosition(submit_location, handle_error);
    } else {
      // no native support; maybe try Gears?
    }
  }

  function submit_location(position) {
    var latitude = position.coords.latitude;
    var longitude = position.coords.longitude;
    $.mobile.changePage(destination,
                        {'type': 'get',
                        'transition': 'slide',
                        'data': {'latitude': latitude,
                                 'longitude': longitude}});
  }

  function handle_error(err) {
    if (err.code == 1) {
      alert('You did not allow your location to be used.');
    } else {
      alert('A geolocation error occured');
    }
    history.back();
  }
                       
  var destination = $(this).data('destination');
  get_location();
});