
var options = {
		  columns: true,
		  delimiter: ';',
		  quote: '"',
		  encoding: 'utf-8'
	};

var csv = require('csv');
var fs = require('fs');
var db = require('./db-mysql');

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
				parse_directory(filePath, recursive);
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
	console.log("parsing...",filepath);
	var fileSpliter = filepath.split(/(.+)?\./);
	var ext = fileSpliter[2];
	if(ext=='csv'){
		// 1 pase file for data.
		var dataObjects = Array();
		csv()
		.from.path(filepath,options)
		.on('end', function() {
			// 2 no data found : exit
			if(dataObjects.length==0)
				return;
			
			// 3 insert le fichier dans dataset
			var datasetObject = {
				name      : info?info.name:filepath
			}
			db.insert('insert_dataset', datasetObject, function(err,data){
				// 4 insert that data
				batchInsert(insertKey, dataObjects, callback)
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
			}
		});

	} else {
		console.log("filepath, n'est pas un .csv : fichier ignoré.");
	}
};

