/**
 * New node file
 */

var Batch = require('batch');
var csv = require('csv');
var csvOptions = {
    columns: true,
    delimiter: ';',
    quote: '"',
    encoding: 'utf-8'
};

module.exports = {
    extractCsvGeoObjects : function(filename, callback){
      extractCsvGeoObjects(filename, callback);
    }
};

/**
 * extract geoCoordinates from csv files
 * @param filename
 * @param callback(err, geoObjects) 
 */
function extractCsvGeoObjects(filename, callback){
  // console.log("processing "+filename);
  var geoObjects = Array();
  csv()
  .from.path(filename,csvOptions)
  .on('end', function() {
      // 2 no data found : exit
      if(geoObjects.length==0) {
        console.log(filename +":no data");
        return callback();
      }
          
      console.log(filename +":"+geoObjects.length);
      return callback(null, geoObjects);
  })
  .transform(function(row, index) {
    dataObject = parseRow(row);
    if(dataObject)
      geoObjects.push(dataObject);
  });
}
/**
 * extract geoObject from row.
 * @param row
 * @returns {latitude,longitude}
 */
function parseRow(row){
  if(row.wgs84){
    return gpsToGeoObject(row.wgs84);
  }
  if(row.geo_shape){
    geo_shape = JSON.parse(row.geo_shape);
    if(geo_shape.type=="Point"){
      var dataObject = {
          latitude  : geo_shape.coordinates[0],
          longitude : geo_shape.coordinates[1],
      };
      return dataObject;
    }
  }
  if(row.geo_point_2d){
    return gpsToGeoObject(row.geo_point_2d);
  }
  if(row.lat_lon){
    return gpsToGeoObject(row.lat_lon);
  }      
  if(row.geometry_x_y){
    return gpsToGeoObject(row.geometry_x_y);
  }      
  if(row.latitude && row.longitude){
      var dataObject = {
          latitude  : row.latitude,
          longitude : row.longitude
      };
      return dataObject;
  }
  if(row.Latitude && row.Longitude){
      var dataObject = {
              latitude  : row.Latitude,
              longitude : row.Longitude
      };
      return dataObject;
  } else if(row.lat && row.lon){
      var dataObject = {
              latitude  : row.lat,
              longitude : row.lon
          };
      return dataObject;
  } 
}
/**
 * transforme la chaine "lat,lon" en objet {latitude:lat, longitude:lon}
 * @param gps une chaine d coordonnée
 * @returns {latitude:lat, longitude:lon}
 */
function gpsToGeoObject(gps){
  gpsSplit = gps.split(",");
  var geoObject = {
      latitude  : gpsSplit[0].trim(),
      longitude : gpsSplit[1].trim(),
  };
  return geoObject;
}