/**
 * Outil de parsing des coordonées geo.
 */

const csvTools = require('./csvTools');
const isNumber = require('is-number');

module.exports = {
    parseCSVFile : function(filename, processGeoObject, endCallback){
      parseCSVFile(filename, processGeoObject, endCallback);
    },
    isGeoDataHeader: function(headers){
      return isGeoDataHeader(headers)
    },
    parseRow : function(jsonData){
      return parseData(jsonData);
    },

};

/**
 * extract geoCoordinates from csv files
 * @param filename
 * @param processGeoObject un geoObject to process.
 * @param callback(err)
 *--/
function extractCsvGeoObjects_deprecated(filename, processGeoObject, callback, options){
  // console.log("processing "+filename);
  csv()
  .from.path(filename,options?options:csvOptions)
  .on('end', function() {
      // console.log(filename +":"+geoObjects.length);
    if(callback)
      callback();
    return;
  })
  .transform(function(row, index) {
    geoObject = parseRow(row);
    if(geoObject){
      processGeoObject(geoObject);
    }
  });
}
*/

function parseCSVFile(filename, processGeoObject, endCallback){
  csvTools.parseCSVFile(
    filename,
    function(columns){
      // console.log("checking columns", columns);
      return isGeoDataHeader(columns);
    },
    function(row){
      // console.log("processing row", row);
      geoObject = parseData(row);
      if(geoObject && isNumber(geoObject.latitude) && isNumber(geoObject.longitude)){
        processGeoObject(geoObject);
      }
    },
    endCallback);
}


var okHeaders = ["wgs84", "geo_point", "geo_point_2d", "lat_lon", "geometry_x_y", ["latitude", "longitude"], ["lat","lon"], ["long","lat"]];
/**
 * returns true if given headers contains geodata fields
 * @param headers list of headers to check.
 * @return true when a least one header match.
 */
function isGeoDataHeader(headers){
  for(var i=0;i<headers.length;i++){
    headers[i]=headers[i].toLowerCase();
  }
  for(var okHeader of okHeaders){
    // console.log(okHeader);
    if(okHeader instanceof Array){
      // console.log(headers);
      if (headers.indexOf(okHeader[0])>-1 && headers.indexOf(okHeader[1])>-1)
        return true;
    }
    if(headers.indexOf(okHeader)>-1)
      return true;
  }
  return false;
}

/**
 * extract geoObject from row.
 * @param row
 * @returns {latitude,longitude}
 */
function parseData(row){
  // wgs84
  if(row.wgs84){
    return gpsToGeoObject(row.wgs84);
  }

  // geo_shape
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

  // geo_point
  if(row.geo_point){
    return gpsToGeoObject(row.geo_point);
  }

  // geo_point_2d
  if(row.geo_point_2d){
    return gpsToGeoObject(row.geo_point_2d);
  }

  // lat_lon
  if(row.lat_lon){
    return gpsToGeoObject(row.lat_lon);
  }

  // geometry_x_y
  if(row.geometry_x_y){
    return gpsToGeoObject(row.geometry_x_y);
  }

  // latitude, longitude
  if(row.latitude && row.longitude){
      var dataObject = {
        latitude  : row.latitude,
        longitude : row.longitude
      };
      return dataObject;
  }

  // lat, lon
  if(row.lat && row.lon){
      var dataObject = {
        latitude  : row.lat,
        longitude : row.lon
      };
      return dataObject;
  }

  // lat, long
  if(row.lat && row.long){
      var dataObject = {
        latitude  : row.lat,
        longitude : row.long
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

/**
 * check that latitude and longitude are acceptable. i.e. numbers withing lat/lon range.
 * @param the geoObject
 * @return true only if both latitude and longitude are acceptable.
 */
function validateGeoObject(geoObject){
  if(isNaN(geoObject.longitude) || isNaN(geoObject.latitude))
    return false;
  if(geoObject.latitude<-90 || geoObject.latitude>90)
    return false;
  if(geoObject.longitude<-180 || geoObject.longitude>180)
    return false;
  return false;
  // var floatRX="[0-9]*\.?[0-9]+";
  // if( geoObject.longitude.match("^"+floatRX+"(E|W)$")) {
  //}
}
