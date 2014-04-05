var heatmap;

$(function(){
    var myLatlng = new google.maps.LatLng(43.293466, 5.364575);

    var myOptions = {
      zoom: 4,
      center: myLatlng,
      mapTypeId: google.maps.MapTypeId.ROADMAP,
      disableDefaultUI: false,
      scrollwheel: true,
      draggable: true,
      navigationControl: true,
      mapTypeControl: false,
      scaleControl: true,
      disableDoubleClickZoom: false
    };
    var map = new google.maps.Map($("#heatmapArea")[0], myOptions);
    
    heatmap = new HeatmapOverlay(map, {
        "radius":5,
        "visible":true, 
        "opacity":60
    });
    geoData = new Array();
    once=false;
    // this is important, because if you set the data set too early, the latlng/pixel projection doesn't work
    google.maps.event.addListenerOnce(map, 'idle', function(){
    	updateMap(map);
        // heatmap.setDataSet({max: 2, data: geoData});
    });
    google.maps.event.addListener(map, 'click', function(e) {
    	listFiles(e.latLng, map);
    });
    google.maps.event.addListener(map, 'bounds_changed', function(e) {
        //updateMap(map);
      // heatmap.setDataSet({max: 2, data: geoData});
    });
});
        
function updateMap(map){
	var bound = map.getBounds();
	var queryObject = {
	};
	$.getJSON('/api/geodata', queryObject, function(data) {
		// var geoData = new Array();
		//var googleLatLng = new Array(); 
		$.each(data, function(key, val) {
			geoData.push({lng:val.longitude, lat:val.latitude, count:val.count});
			//googleLatLng.push(latLng = new google.maps.LatLng(val.latitude, val.longitude));
		});
		console.log("nb points:",geoData.length);
		// ajax implementation
		
		//heatmap.setDataSet({max: 2, data: geoData});
		heatmap.setDataSet({max: 10, data: geoData});
		// heatmap.setDataSet(testData);

		// google implementation
		//		var gHeatmap = new google.maps.visualization.HeatmapLayer({
		//			  data: googleLatLng
		//			});
		//			gHeatmap.setMap(map);
	});
};
            
function placeMarker(position, map) {
  var marker = new google.maps.Marker({
    position: position,
    map: map
  });
  map.panTo(position);
}

var clickMarker = null;
function listFiles(position, map) {
	if(clickMarker){
		clickMarker.setVisible(false);
	};
	var span = map.getBounds().toSpan();
	var radius = span.lat()<span.lng()?span.lng():span.lat();
	// radius = radius;
	console.log("radius",radius);
	clickMarker = new google.maps.Marker({
	    position: position,
	    map: map
	  });
	  map.panTo(position);
	var latLng = {
			latitude:position.lat(),
			longitude:position.lng(),
			radius:radius * 4
	};
	console.log(latLng);
	$.getJSON('api/geo-datasets', latLng, function(data) {
		$("#datasets").html("");
		$.each(data, function(key, val) {
			console.log(val);
			$("#datasets").append("<tr><td>"+val.name+"</td></tr>")
		});
	});
}
