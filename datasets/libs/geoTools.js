/**
 * Outil de parsing des coordonées geo.
 */

const 
  csvTools = require('./csvTools');


module.exports = {
    extractCsvGeoObjects : function(filename, processGeoObject, endCallback){
      extractCsvGeoObjects(filename, processGeoObject, endCallback);
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

function extractCsvGeoObjects(filename, processGeoObject, endCallback){
  csvTools.parseCSVFile(
    filename, 
    function(columns){
      return !isGeoDataHeader(columns);
    }, 
    function(row){
      geoObject = parseData(row);
      if(geoObject){
        processGeoObject(geoObject);
      }
    }, 
    endCallback);
}


var okHeaders = ["wgs84", "geo_point_2d", "lat_lon", "geometry_x_y", ["latitude", "longitude"], ["lat","lon"]];
/**
 * returns true if given headers contains geodata fields
 * @param headers list of headers to check.
 * @return true when a least one header match.
 */
function isGeoDataHeader(headers){
  for(i=0;i<headers.length;i++){
    headers[i]=headers[i].toLowerCase();
  }
  for(okHeader of okHeaders){
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
