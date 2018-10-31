/**
 * New node file
 */
var db = require('../../shared/db-mysql');

module.exports = {
    init : function(config, callback){
      init(config, callback);
    },
    createDataset : function(datasetInfo, callback){
      createDataset(datasetInfo, callback);
    },
    insertGeodata : function(dataset_id, geoObject, callback){
      insertGeodata(dataset_id, geoObject, callback);
    },
    insertDataset : function(datasetInfo, geoObjects, callback){
      insertDataset(datasetInfo, geoObjects, callback);
    },
    getDataset : function(datasetId, revision, callback){
      return getDataset(datasetId, revision, callback);
    }
};

/**
 * initialise la base de donnée.
 * @param config
 * @param callback
 */
function init(config, callback){
  //creates the connection pool.
  db.init_db(config.db_host,
           config.db_database,
           config.db_user,
           config.db_password
  );
  
  //loads queries.
  db.loadXMLQueries("../server/lib/sql.xml", function(){
    console.log("file LOADED");
    callback();
  });
}

/**
 * create dataset if not exists.
 * @param datasetInfo
 * @param callback
 * @returns
 */
function createDataset(datasetInfo, callback){
  db.list('select_dataset', datasetInfo, function(err, data){
    if(data && data.length>0){
      return callback (null, data[0].id);
    }
    db.insert('insert_dataset', datasetInfo, function(err,data){
      var datasetId = data.insertId;
      return callback(null,datasetId);
    });
  });
}

function insertGeodata(dataset_id, geoObject, callback) {
  geoObject.dataset_id = dataset_id;
  db.insert('insert_geodata', geoObject, callback);
}

/**
 * Insert le dataset en base.
 * @param datasetInfo
 * @param geoObjects array of {latitude, longitude}
 */
function insertDataset(datasetInfo, geoObjects, callback){
  console.log("inserting dataset data :",datasetInfo.dataset_ref);
  if(!geoObjects)
    return callback();
  
  var datasetObject = {
      filepath :datasetInfo.filepath,
  };
  db.insert('insert_dataset', datasetInfo, function(err,data){
    var datasetId = data.insertId;
    for(var i=0;i<geoObjects.length;i++){
      geoObjects[i].dataset_id = datasetId;
      //console.log(geoObjects[i]);
    }
    db.batchInsert('insert_geodata', geoObjects, callback);
  });
}

/**
 * callback le/les datasets pour ce datasetId, revision (optional)
 * @param dataset_ref
 * @param revision
 * @param callback
 */
function getDataset(dataset_ref, revision, callback){
  var queryObject = {
      dataset_ref : dataset_ref,
      revision : revision
  };
  db.list("select_dataset",queryObject, callback);
}