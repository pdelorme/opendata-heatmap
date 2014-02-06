var fs      = require('fs');
var csv     = require('csv');
var request = require('request');
var db     	= require('./db-mysql');
var Batch   = require('batch');
var LineReader = require('line-by-line');
var S = require('string');

var options = {
	  columns: true,
	  delimiter: ';',
	  quote: '"',
	  encoding: 'utf-8'
};

var catalogOptions = {
	  columns: true,
	  delimiter: ',',
	  quote: '"',
	  encoding: 'utf-8'
};

exports.test = function(done){
	parseDataGouvDump("/data/Projets/data-heatmap/data/catalogue.csv",done);
};
/**
 * Parse le catalogue de données.
 * @param dumppath
 */
function parseDataGouvDump(dumppath, done){
	var batch = new Batch;
	batch.concurrency(1);
	
	csv()
	.from.path(dumppath,catalogOptions)
	.on('end', function() {
		batch.end(function(err){
	    	if(err)
	    		console.log(err);
	    	console.log("parseDataGouvDump finished");
			if(done)
				done();
		});
		//if(done) done("over");
		//return;
	})
	.on('record', function(row, index) {
		if(!row.resources || row.resources.size==0)
			return;
		//console.log(row.resources.size);
		var resources = row.resources;
		//console.log("avant:",resources);
		resources = resources.replace(/"(.*)"/g, "'__'");
		resources = resources.replace(/\\'/g, "_");
		resources = resources.replace(/__''/g, "__'");
		//console.log("bis:",resources);
		resources = resources.replace(/u'/g, "'");
		resources = resources.replace(/u"/g, "\"");
		resources = resources.replace(/\\x/g, "-");
		resources = resources.replace(/'/g, "\"");
		//console.log("après:",resources);
		try {
			allRes = JSON.parse(resources);
			for(var i=0;i<allRes.length;i++){
				res = allRes[i];
				if(res.format && res.format.toUpperCase()=="CSV"){
					(function(row,res){
						//console.log("pushing");
						batch.push(function(done) {
							var id = row.id;
							var title = row.title;
							var revision = row.revision;
							var ckan_url = row.ckan_url;
							//console.log("processing url");
							processURL(res.url, res.format, id, title, revision, ckan_url, function(err){
								if(err)
									console.log(err);
								done();
							});
						});
					})(row,res);
					break;
				}
			}
		} catch (err){
			console.log("cannot parse ressources :"+ resources);
		}
	});
};

/**
 * parses all files in the directory structure.
 * @param path
 * @param recursive
 */
function parseDirectory(path, recursive, done) {
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
				parseDirectory(filePath+'/',recursive, done);
			}
		} else {
			parseCsvFile(filePath, null, done);
		}
	}
};

/**
 * fetch and parse URL
 */
function processURL(data_url, type, ref, title, revision, user_url, done){
	var filePath = "temp/dataGouv_"+ref+".csv";
	// 1/ fetch file.
	console.log("fetching "+data_url+" into "+filePath);
	request(data_url, 
		function(error, response, body){
			if(error){
				if(done) done(error);
				return;
			}
			// 2/ processFile.
			return parseFile(filePath, type, ref, title, revision, data_url, user_url, done);
			
		}
	).pipe(fs.createWriteStream(filePath));

};

function processFile(data_url, type, ref, title, revision, user_url, done){
	var filePath = "temp/dataGouv_"+ref+".csv";
	return parseFile(filePath, type, ref, title, revision, data_url, user_url, done);
}
/**
 * parse le fichier et extrait toutes les données de localisation.
 * essai de déterminer le type d'après l'extention si le type est absent.
 * @param file le fichier à parser
 * @param type le format du fichier.
 * @param dataset_ref l'id du dataset pour la source
 * @param data_url l'url des données
 * @param user_url l'url à présenter à l'utilisateur.
 */
function parseFile(filepath, type, dataset_ref, title, revision, data_url, user_url, done){
	if(!type){
		var fileSpliter = filepath.split(/(.+)?\./);
		type= fileSpliter[2];
	}
	type = type.toLowerCase();
	if(type=="csv"){
		return parseCsv2File(filepath, type, dataset_ref, title, revision, data_url, user_url, done);
	}
	if(done) done("doesnt kno this type :"+type+", file ignored :"+filepath);
	return
};

/**
 * crée le dataset.
 * @param filepath
 * @param type
 * @param dataset_ref : l'id du dataset pour la source.
 * @param title
 * @param revision
 * @param web_url
 * @param done(err, dataset_id) : retourne l'id de dataset.
 */
function insertDataSet(filepath, type, dataset_ref, title, revision, data_url, user_url, done){
	console.log("insertDataSet :"+filepath);
	var filenameSpliter = filepath.split(/(.+)?\//);
	var filename = filenameSpliter[2];
	// 3 insert le fichier dans dataset
	var datasetObject = {
		filepath : filepath,
		type:type,
		dataset_ref:dataset_ref?dataset_ref:"UNDEFINED",
		title:title?title:"UNDEFINED",
		revision:revision,
		data_url:data_url,
		user_url:user_url?user_url:"UNDEFINED"
	}
	db.insert('insert_dataset', datasetObject, function(err, data){
		if(err && done)
			return done(err);
		if(done)
			return done(err, data.insertId);
	});
};

/**
 * parse le fichier CSV et extrait toutes les données de localisation.
 * @param file le fichier à parser
 */
function parseCsvFile(filepath, done){
	console.log("parsing csv "+filepath);
	csv()
	.from.path(filepath,options)
	.on('record',function(row, index) {
		insertData(dataset_id, row);
	})
	.on('error', function(error) {
		if(done) done("error parsing :"+filepath+", "+error);
		return;
	})
	.on('finish', function() {
		if(done) done("parseCsv finished :"+filepath);
		return;
	});
//	.transform(function(row, index) {
//		insertData(dataset_id, row);
//	});
};

function parseCsv2File(filepath, type, dataset_ref, title, revision, data_url, user_url, done){
	var count = 0;
	var sep   = ",";
	var quote = "";
	var latitudeLabel;
	var longitudeLabel;
	var latitudeIndex;
	var longitudeIndex;
	var lr = new LineReader(filepath);
	var dataset_id;
	var error
	lr.on('error', function (err) {
		count=-1;
		error =err;
		lr.close();
	});
	lr.on('line', function (line) {
		if(count==0){
			line = line.trim();
			first = line.charAt(0);
			if(first=="'" || first=="\"")
				quote = first;
			comaCount = S(line).count(",");
			semiCount = S(line).count(";");
			if(comaCount>semiCount){
				sep = ",";
			} else if(semiCount>0){
				sep = ";";
			}
			// console.log("config for "+filepath+" is sep/quote:"+sep+"/"+quote+" from "+line );
			headerArray = S(line).parseCSV(sep,quote);
			for(var i=0;i<headerArray.size;i++){
				var label = headerArray[i].toLowerCase();
				if(label=="lat" || label=="latitude"){
					latitudeLabel = label;
					latitudeIndex = i;
					continue;
				}
				if(label=="lon" || label=="longitude" || label=="lng"){
					longitudeLabel = label;
					longitudeIndex = i;
					continue;
				}
			}
			if(!latitudeLabel || !longitudeLabel){
				console.log("impossible de trouver latitude et longitude :"+line);
				return lr.close();
			} else {
				console.log("latitude et longitude :"+latitudeLabel+"/"+longitudeLabel);
				lr.pause();
				insertDataSet(filepath, type, dataset_ref, title, revision, data_url, user_url, 
					function(error, id){
						if(error)
							return lr.close();
						dataset_id = id;
						lr.resume();
					}
				);
			}
			count=1;
			return;
		}
		lineArray = S(line).parseCSV(sep,quote);
		if(latitudeIndex && longitudeIndex){
			latitude = lineArray[latitudeIndex];
			longitude = lineArray[longitudeIndex];
			if(latitude && longitude)
				console.log("seen position : "+count++, latitude,longitude);
		} else {
			console.log("hugh ? "+line);
		}
	    // 'line' contains the current line without the trailing newline character.
	});
	lr.on('end', function () {
	    // All lines are read, file is closed now.
		if(count==-1){
			return done("error reading file "+filepath+" : "+error);

		}
		if(count==0){
			return done("fichier "+filepath+" vide ou illisible");
		}
		done();
	});
}

/**
 * insert data in database by parsing the row.
 * @param dataset
 * @param row
 * @param done
 */
function insertData(dataset_id, row, done){
	var lat;
	var long;
	
	if(row.latitude)
		lat = row.latitude;
	if(row.Latitude)
		lat = row.Latitude;
	if(row.lat)
		lat = row.lat;
	if(row.Lat)
		lat = row.Lat;
	
	if(row.longitude)
		long = row.longitude;
	if(row.Longitude)
		long = row.Longitude;
	if(row.lon)
		long = row.lon;
	if(row.Lon)
		long = row.Lon;
	if(row.lng)
		long = row.lng;
	
	if(lat && long){
		// 3 insert le fichier dans dataset
		var dataObject = {
			latitude  : parseFloat(lat),
			longitude : parseFloat(long),
			dataset_id  : dataset_id
		};
		// 4 insert that data
		db.insert("insert_geodata", dataObject, done);
	}	
};
