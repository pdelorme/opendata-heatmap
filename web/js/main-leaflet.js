var myLat = 48.856614;
var myLng = 2.352222;

var heatmap;
var loading = false;
var refresh = false;
var marker;

function initLeafletMap(){
  lmap = L.map('heatmapArea').setView([myLat, myLng], 5);

  var tiles = L.tileLayer('http://{s}.tiles.mapbox.com/v3/{id}/{z}/{x}/{y}.png', {
      attribution: '<a href="https://www.mapbox.com/about/maps/">Terms and Feedback</a>',
      id: 'examples.map-20v6611k'
  }).addTo(lmap);

  var dummy = new Array();
  var options = {
      //max:2,
      radius:20,
      blur:30,
      maxZoom:8
  }
  heatmap = L.heatLayer(dummy,options).addTo(lmap);
  lmap.on("moveend", function(){
    updateMap(lmap);
  });
  lmap.on("click", function(event){
    listFiles(event.latlng, lmap);
  });
};

$(function(){
    initLeafletMap();
    updateMap(lmap);
    listFiles({lat:myLat, lng:myLng}, lmap);
    $( "#density-slider" ).slider({
      value: heatmap.options.maxZoom,
      min: 0,
      max: 50,
      step:0.1,
      slide: function( event, ui ) {
        $( "#density" ).html(ui.value );
        heatmap.setOptions({maxZoom:ui.value});
      }
    });
    $( "#density" ).html($( "#density-slider" ).slider( "value" ) );
});
        
function updateMap(map){
  if(loading){
    refresh=true;
    return;
  }
  loading=true;
  var bound = map.getBounds();
  console.log("zoom",map.getZoom(),Math.pow(2,map.getZoom()));
  var queryObject = {
      north: bound.getNorth(),
      south: bound.getSouth(),
      east : bound.getEast(),
      west : bound.getWest(),
      tile : Math.pow(2,map.getZoom()),
  };
	$.getJSON('/api/area-geodata', queryObject, function(data) {
	    var geoData = new Array();
		//var googleLatLng = new Array(); 
		var max = 0;
	    $.each(data, function(key, val) {
		  geoData.push([val.lat, val.lng, val.nb]);
		  if(max<val.nb)
		    max = val.nb;
		});
        radius = Math.max(3,Math.log(max)*1);
		console.log("nb points,max,radius:",geoData.length,max,radius);

		heatmap.setLatLngs(geoData);
		//heatmap.setOptions({radius:radius,blur:radius});
        loading=false;
		if(refresh){
		  refresh=false;
		  updateMap(map);
		}
	});
};

//            
//function placeMarker(position, map) {
//  var marker = new google.maps.Marker({
//    position: position,
//    map: map
//  });
//  map.panTo(position);
//}
//
//var clickMarker = null;
function listFiles(position, map) {
  if(!marker){
    marker = L.marker(position);
    marker.addTo(map);
  }
  marker.setLatLng(position);
//	if(clickMarker){
//		clickMarker.setVisible(false);
//	};
	var bounds = map.getBounds();
	latSpan = bounds.getNorth() - bounds.getSouth();
	lngSpan = bounds.getEast() - bounds.getWest();
	var span = Math.min(latSpan,lngSpan);
	// radius = radius;
	console.log("span",span);
//	clickMarker = new google.maps.Marker({
//	    position: position,
//	    map: map
//	  });
	map.panTo(position);
	var latLng = {
	    latitude:position.lat,
		longitude:position.lng,
		radius:span*2,
		limit:20
	};
	console.log(latLng);
	$.getJSON('api/geo-datasets', latLng, function(data) {
		$("#datasets").html("");
		if(!data)
		  return
		data.sort(function(a,b){
		  return a.d - b.d;
		});
		$.each(data, function(key, val) {
			console.log(val);
			var dist = "";
			if(val.d<1){
			  dist+=val.d*1000 +"&nbsp;m";
			} else {
			  dist+=new Number(val.d).toFixed(1)+"&nbsp;Km";
			}
			$("#datasets").append("<tr><td>"+dist+"</td><td>"+unescape(val.title)+"</td><td><a class='btn btn-default' href='"+unescape(val.data_url)+"'><span class='glyphicon glyphicon-star'>csv</span></a></td></tr>");
		});
	});
}
