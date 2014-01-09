var config 	= require('../config');
var fs     	= require('fs');
var crypto 	= require('crypto');
var db     	= require('./db-mysql');
var csv 	= require('csv');


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


/**
 * retourne la liste des données géo dans la zone selectionnées
 */
exports.get_area_geodata = function(req,res){
	var params = req.query;
	var queryObject = {
			east 	: params.east,
			west	: params.west,
			south	: params.south,
			north	: params.north
	};
	db.list('get_area_geodata',queryObject, function(err, data){
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
			radius   : params.radius?params.radius:1
	};
	db.list('get_geo_datasets',queryObject, function(err, data){
		console.log(err,data);
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
		console.log("filepath, n'est pas un .csv : fichier ignoré.");
	}
};
/**
 * USER BUISNESS
 */

/**
 * registering new user.
 */
exports.register_user = function f(req,res){
	var params = req.query;
	console.log("register_user params",params);
	if(!params.login || ! params.password){
		return res.respond('login and password must be provided',400);
	}
	var dbObject = {
		login      : params.login,
		password   : password(params.password), // password hash.
		first_name : params.first_name,
		last_name  : params.last_name,
		email      : params.email
	}
	
	db.select('select_user',dbObject, function(err, data){
		console.log(">>user:",err,data);
		if(err)
			return res.respond(err,400);
		if(data){
			if(data.password!=dbObject.password){
				// user already exists.
				return res.respond('invalid login or password',400);
			}
			// updates data values from params
			for (var key in dbObject) { 
				if(data[key]){
					data[key] = dbObject[key];
				}
			}
			if(params.newPassword){
				console.log("new PASSWORD.");
				dbObject.password = password(params.newPassword);
			}
			// reinjects data with updated values.
			db.update('update_user', dbObject, function(err,data){
				dbObject.password=undefined;
				return res.respond(dbObject,200);
			});
		} else {
			db.insert('insert_user',dbObject, function(err,data){
				dbObject.password=undefined;
				return res.respond(dbObject,200);
			});
		}
	});
}

/**
 * checking user/password.
 * @return user.
 */
exports.login = function f(req,res){
	var params = req.query;
	console.log("login params",params);
	if(!params.login || ! params.password){
		return res.respond('login and password must be provided',400);
	}

	params.password = password(params.password);
	db.select('select_user',params, function(err, data){
		// console.log("data",data);
		if(!err && data && data.password==params.password){
			data.password = undefined;
			req.session = {};
			req.session.login = params.login;
			return res.send(data);
		}
		return res.respond('invalid login or password',400);
	});
	
}

exports.logout = function f(req,res){
	req.session = null;
	return res.respond('OK',200);
}

function password(password){
	if(!password)
		return;
	var shasum = crypto.createHash('sha1');
	shasum.update(password);
	return shasum.digest('hex');
}
/**
 * gets user info
 */
exports.get_user_info = function f(req,res){
	if(!req.params.login){
		req.params.login = req.session.login;
	}
	db.select('select_user',req.params, function(err, data){
		var user_info = {
				login      : data.login,
				first_name : data.first_name,
				last_name  : data.last_name
		};
		
		return res.send(user_info);
	});
}

exports.init = function(callback){
	setTimeout(callback,1000);
}
