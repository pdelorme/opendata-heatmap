var mysql = require('mysql');
var fs    = require('fs');
var xml2js = require('xml2js');
var debug = false;

module.exports = {
  init_db : function(host, database, user, password){
	  init_db(host, database, user, password);
  },
  loadXMLQueries : function(filename, callback){
	  loadXMLQueries(filename, callback);
  },
  select: function (sql, param, callback) {
	  select(sql, param, callback);
  },
  list: function (sql, param, callback, options) {
	  list(sql, param, callback, options);
  },
  insert: function (sql, object, callback) {
	  insert(sql, object, callback);
  },
  batchInsert: function (insertKey, objectArray, callback) {
	  batchInsert(insertKey, objectArray, callback);
  },
  batchUpdate: function (updateKey, objectArray, callback) {
	  batchUpdate(updateKey, objectArray, callback);
  },
  update: function (sql, object, callback) {
	  update(sql, object, callback);
  },
  terminate : function(){
	  terminate();
  }
};

var pool;

/**
 * init database pool with cusom values.
 * @param host
 * @param user
 * @param password
 */
function init_db(host, database, user, password){
	pool  = mysql.createPool({
		  host     : host,
		  database : database,
		  user     : user,
		  password : password
	});
	return pool;
}
/**
 * terminates database pool
 */
function terminate(){
	pool.end();
}

var sqlQueries = { };

/**
 * loading xmlFile into queries array.
 * @param xmlFile
 */
function loadXMLQueries(xmlFile, callback){
	console.log("loading xml file",xmlFile);
	var parser = new xml2js.Parser();
	fs.readFile(xmlFile, function(err, data) {
		parser.parseString(data, function (err, result) {
			if(err) {
				console.log(err);
				return;
			}
			for (var query in result.queries) { 
				console.log("registering query",query);
				sqlQueries[query] = result.queries[query]; 
			}
			// console.log("xmlFile loaded",xmlFile);
			// console.log(sqlQueries);
			if(callback)
				callback();
		});
	});
}

/**
 * returns the SQL associated with that key
 * @param sqlKey
 * @returns
 */
function getQuery(sqlKey){
	return sqlQueries[sqlKey];
}

/**
 * updates or inserts if no rows where updated.
 * ATTENTION updateQuery doit inclure la clause WHERE.
 * @param updateQuery the update SQL
 * @param insertQuery the insert SQL
 * @param object the object to be escaped and injected into queries.
 * @param callback callback once update or insert is finished.
 */
function update_or_insert(updateQuery, insertQuery, object, callback){
	pool.getConnection(function(err, connection) {
		// Use the connections
		connection.query( updateQuery, object, function(err, result) {
			// console.log(err,result);
			if (err) throw err;
			if(result.affectedRows>0){
				console.log("object updated :", object);
				connection.release();
				if(callback) 
					callback();
				return;
			}
				
			// And done with the connection.
			connection.query(insertQuery, object, function(err, result) {
				if (err) throw err;
				console.log("object inserted :",object);
				// And done with the connection.
				connection.release();
				if(callback)
					callback();
			});
		});
	});		
}


/**
 * repaces all :xxx par xxx value.
 * @param query
 * @param values
 * @returns
 */
function prepare(query, values) {
	if (!values) 
		return query;
	return query.replace(/\:(\w+)/g, function (txt, key) {
		if (values.hasOwnProperty(key)) {
			return this.escape(values[key]);
		}
		return txt;
	}.bind(this));
};


/**
 * executes a query with params.
 * @param queryKey
 * @param params
 */
function list(queryKey, params, callback, options) {
	var queryString = getQuery(queryKey)[0];
	if(!queryString){
		console.log('Could not find sqlString',queryKey);
		var err = 'Could not find sqlString'+queryKey;
		if(callback)
			callback(err,null);
		return;
	}
	queryString = prepare(queryString,params);
	if(debug)
		console.log('queryString : ',queryString);
	if(!options)
		options={};
	options.sql = queryString;
	pool.getConnection(function(err, connection) {
		connection.query( options, function(err, rows) {
		    // And done with the connection.
		    connection.release();
		    if(callback)
		    	callback(err,rows);
		    // Don't use the connection here, it has been returned to the pool.
		});
	});
};

/**
 * returns zero or one value from select.
 * throws error otherwise.
 * @param queryKey
 * @param params
 * @param callback
 */
function select(queryKey, params, callback){
	list(queryKey,params, function(err,rows){
		if(err)
			return callback(err,rows);
		if(rows && rows.length>1)
			return callback('single value not found',rows);
		if(rows && rows.length==1)
			return callback(err,rows[0]);
		return callback();
	});
}

/**
 * insert objet.
 * @param insertKey
 * @param object
 * @param callback
 */
function insert(insertKey, object, callback){
	var insertString = getQuery(insertKey)[0];
	if(!insertString){
		console.log('Could not find sqlString',insertKey);
		var err = 'Could not find sqlString'+insertKey;
		if(callback)
			callback(err,null);
		return;
	}
	pool.getConnection(function(err, connection) {
		// Use the connections
		insertString = prepare(insertString,object);
		if(debug)
			console.log(insertString);
		try {
			connection.query(insertString, function(err, result) {
				if (err) throw err;
				//console.log("object inserted :",object);
				connection.release();
				if(callback)
					callback(err, result);
			});
		} catch(error){
			console.log("FATAL ERROR :",error);
			if(callback)
				callback(error);
		}
	});
}

/**
 * insert un tableau d'objet dans une m�me transaction.
 * @param insertKey
 * @param objectArray
 * @param callback
 */
function batchInsert(insertKey, objectArray, callback){
	var insertString = getQuery(insertKey)[0];
	if(!insertString){
		console.log('Could not find sqlString',insertKey);
		var err = 'Could not find sqlString'+insertKey;
		if(callback)
			callback(err,null);
		return;
	}
	pool.getConnection(function(err, connection) {
		var insertedObjects = 0;
		var objectsToInsert = objectArray.length;
		for(var i=0; i<objectsToInsert; i++){
			var object = objectArray[i];
			if(debug)
				console.log(insertString,object);
			// Use the connections
			connection.query(insertString, object, function(err, result) {
				if (err) {
					console.log("erro inserting object :",object);
					throw err;
				}
				insertedObjects++;
				if(objectsToInsert == insertedObjects){
					if(debug)
						console.log("last object inserted. calling back");
					connection.release();
					if(callback)
						callback();
				}
			});
		}
	});
}

/**
 * insert un tableau d'objet dans une m�me transaction.
 * @param insertKey
 * @param objectArray
 * @param callback
 */
function batchUpdate(updateKey, objectArray, callback){
	var updateString = getQuery(updateKey)[0];
	if(!updateString){
		console.log('Could not find sqlString',updateKey);
		var err = 'Could not find sqlString'+updateKey;
		if(callback)
			callback(err,null);
		return;
	}
	pool.getConnection(function(err, connection) {
		var insertedObjects = 0;
		var objectsToInsert = objectArray.length;
		for(var i=0; i<objectsToInsert; i++){
			var object = objectArray[i];
			updateString = prepare(updateString,object);
			if(debug)
				console.log(updateString);
			// Use the connections
			connection.query(updateString, function(err, result) {
				if (err) {
					console.log("erro inserting object :",object);
					throw err;
				}
				insertedObjects++;
				if(objectsToInsert == insertedObjects){
					if(debug)
						console.log("last object inserted. calling back");
					connection.release();
					if(callback)
						callback();
				}
			});
		}
	});
}

/**
 * update value.
 * @param insertKey
 * @param object
 * @param callback
 */
function update(updateKey, params, callback){
	var updateString = getQuery(updateKey)[0];
	if(!updateString){
		console.log('Could not find sqlString',updateKey);
		var err = 'Could not find sqlString'+updateKey;
		if(callback)
			callback(err,null);
		return;
	}
	updateString = prepare(updateString,params);
	if(debug)
		console.log(updateString);
	pool.getConnection(function(err, connection) {
		// Use the connections
		connection.query(updateString, function(err, result) {
			if (err) throw err;
			//console.log("object inserted :",object);
			connection.release();
			if(callback)
				callback(err, result);
		});
	});
}

