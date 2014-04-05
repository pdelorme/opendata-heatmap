var config 	= require('../config');
var fs     	= require('fs');
var crypto 	= require('crypto');
var db     	= require('./db-mysql');
var csv 	= require('csv');
var services= require('./services');

var options = {
	  columns: true,
	  delimiter: ';',
	  quote: '"',
	  encoding: 'utf-8'
};

// creates the connection pool.
db.init_db(config.db_host,
		   config.db_database,
		   config.db_user,
		   config.db_password
);

// loads queries.
db.loadXMLQueries(__dirname + '/sql.xml', function(){
	console.log("file LOADED");
});

exports.test = function(req, res){
	services.test(
		function(){
			return res.send("merci");
		}
	);
//	services.parseDataGouvDump("/data/Projets/data-heatmap/data/catalogue.csv",null,function(){
//		return res.send("merci");
//	});
}
/**
 * retourne la liste des données géo dans la zone selectionnées
 */
exports.get_area_geodata = function(req,res){
	var params = req.query;
	var queryObject = {
			east 	: params.east,
			west	: params.west,
			south	: params.south,
			north	: params.north,
			tile    : params.tile,
	};
	
	db.list('get_area_geodata',queryObject, function(err, data){
	  console.log(data.length);
		return res.send(data);
	});
};

/**
 * retourne la liste de toutes les données géo groupé par maille.
 */
exports.get_geodata = function(req,res){
  var params = req.query;
  var queryObject = {
      tile:1000,
  };
  db.list('get_geodata',queryObject, function(err, data){
      return res.send(data);
  });
};
/**
 * retourne la liste des jeux de données dans la zone.
 */
exports.get_geo_datasets = function(req,res){
	var params = req.query;
	var queryObject = {
			latitude : params.latitude,
			longitude: params.longitude,
			radius   : params.radius?params.radius:1,
			limit    : params.limit?params.limit:20
	};
	db.list('get_geo_datasets',queryObject, function(err, data){
	  if(data){
  	    for(var i = 0; i< data.length; i++){
  	      data[i].d =new Number(distance(data[i].lat, data[i].lng, params.latitude, params.longitude, "K")).toFixed(3);
          // console.log(data[i]);
        }
	  }
	  return res.send(data);
	});
};

exports.get_ajax_dataset = function(req,res){
	var params = req.query;
	var queryObject = {
			name : params.name
	};
	db.list('get_ajax_dataset',queryObject, function(err, data){
		return res.send(data);
	});
};

exports.upload_dataset = function f(req,res){
	// console.log(req);
	var file = req.files.file;
	var params = req.body;
	
	if(file){
		var fileSpliter = file.path.split(/(.+)?\//);
		var filename = fileSpliter[2];
		var uploadFile = 'uploads/'+filename;
		var targetFile = config.web_root+uploadFile;
		console.log(file.path, targetFile);
		fs.renameSync(file.path, targetFile);
		var dbObject = {
			auteur      : params.login?params.login:req.connection.remoteAddress,
		    site_id     : params.site_id,
		    filename    : uploadFile,
		    score_insolite:0,
		    score_qualite :0,
		    commentaire :params.commentaire
		};
		db.update('insert_photo', dbObject, function(err,data){
			return res.respond(dbObject,200);
		});

	}
}

exports.parse_directory = function(path, recursive) {
	console.log("parsing...",path);
	var files = fs.readdirSync(path);
	for(var i=0;i<files.length;i++){
		var file = files[i];
		var filePath = path+file;
		console.log(">>",filePath);
		var stats = fs.statSync(filePath);
		if(stats.isDirectory()){
			// c'est un dossier.
			if(recursive){
				this.parse_directory(filePath+'/',recursive);
			}
		} else {
			this.parse_file(filePath);
		}	
	}
};

/**
 * charge le fichier en base.
 * utilise les info si disponible.
 * @param path
 * @param info
 */
exports.parse_file = function(filepath, info, callback){
	var fileSpliter = filepath.split(/(.+)?\./);
	var ext = fileSpliter[2];
	if(ext=='csv'){
		console.log("parsing...",filepath);
		// 1 pase file for data.
		var dataObjects = Array();
		csv()
		.from.path(filepath,options)
		.on('end', function() {
			// 2 no data found : exit
			if(dataObjects.length==0)
				return;
			var filenameSpliter = filepath.split(/(.+)?\//);
			var filename = filenameSpliter[2];
			// 3 insert le fichier dans dataset
			var datasetObject = {
				name      : info?info.name:filename
			}
			db.update('insert_dataset', datasetObject, function(err,data){
				//console.log(">>>",data);
				
				for(var i=0;i<dataObjects.length;i++){
					// console.log(">>>",dataObjects[i]);
					dataObjects[i]['dataset_id']=data.insertId;
				}
				console.log(">>>BAATCH");
				// 4 insert that data
				db.batchUpdate('insert_geodata', dataObjects, callback);
				if(callback)
					callback();
			});
			
			// 2 insert toutes les lignes dans geodata		
		})
		.transform(function(row, index) {
			if(row.latitude && row.longitude){
				var dataObject = {
					latitude  : row.latitude,
					longitude : row.longitude
				};
				dataObjects.push(dataObject);
			} else if(row.Latitude && row.Longitude){
				var dataObject = {
						latitude  : row.Latitude,
						longitude : row.Longitude
				};
				dataObjects.push(dataObject);
			} else if(row.lat && row.lon){
				var dataObject = {
						latitude  : row.lat,
						longitude : row.lon
					};
					dataObjects.push(dataObject);
			} else if(row.Longitude_point && row.Latitude_point) {
				var dataObject = {
						latitude  : row.Latitude_point,
						longitude : row.Longitude_point
					};
					dataObjects.push(dataObject);
			} else {
				// console.log('no match',row);
			}
		});

	} else {
		console.log("filepath, n'est pas un .csv : fichier ignor�.");
	}
};

exports.init = function(callback){
	setTimeout(callback,1000);
}

//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//:::                                                                         :::
//:::  This routine calculates the distance between two points (given the     :::
//:::  latitude/longitude of those points). It is being used to calculate     :::
//:::  the distance between two locations using GeoDataSource (TM) prodducts  :::
//:::                                                                         :::
//:::  Definitions:                                                           :::
//:::    South latitudes are negative, east longitudes are positive           :::
//:::                                                                         :::
//:::  Passed to function:                                                    :::
//:::    lat1, lon1 = Latitude and Longitude of point 1 (in decimal degrees)  :::
//:::    lat2, lon2 = Latitude and Longitude of point 2 (in decimal degrees)  :::
//:::    unit = the unit you desire for results                               :::
//:::           where: 'M' is statute miles                                   :::
//:::                  'K' is kilometers (default)                            :::
//:::                  'N' is nautical miles                                  :::
//:::                                                                         :::
//:::  Worldwide cities and other features databases with latitude longitude  :::
//:::  are available at http://www.geodatasource.com                          :::
//:::                                                                         :::
//:::  For enquiries, please contact sales@geodatasource.com                  :::
//:::                                                                         :::
//:::  Official Web site: http://www.geodatasource.com                        :::
//:::                                                                         :::
//:::               GeoDataSource.com (C) All Rights Reserved 2014            :::
//:::                                                                         :::
//:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

function distance(lat1, lon1, lat2, lon2, unit) {
  var radlat1 = Math.PI * lat1/180
  var radlat2 = Math.PI * lat2/180
  var radlon1 = Math.PI * lon1/180
  var radlon2 = Math.PI * lon2/180
  var theta = lon1-lon2
  var radtheta = Math.PI * theta/180
  var dist = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(radtheta);
  dist = Math.acos(dist)
  dist = dist * 180/Math.PI
  dist = dist * 60 * 1.1515
  if (unit=="K") { dist = dist * 1.609344 }
  if (unit=="N") { dist = dist * 0.8684 }
  return dist
}                                                                           
