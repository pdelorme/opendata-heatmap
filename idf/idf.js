/**
 * New node file
 */

var fs      = require("fs");
var request = require('request');
var Batch   = require("batch");

var idf_url = "http://data.iledefrance.fr/";

module.exports = {
    /**
     * fetch IDF datasets descriptor
     * @param callback(datasets_filename)
     * @param date sous la forme YYYY/MM/DD (optional)
     */
    fetchIDFDatasets : function(callback, date){
      fetchIDFDatasets(callback, date);
    },
    /**
     * passes all datasets through datasetProcessor
     * @param dataset_file
     * @param concurency
     * @param datasetProcessor(dataset, callback)
     * @param callback(err)
     */
    processAllDatasets : function(dataset_file, concurency, datasetProcessor, callback){
      processAllDatasets(dataset_file, concurency, datasetProcessor, callback);
    },
    /**
     * returns normalized datasetInfo from idf dataset.
     * @param dataset
     * @returns {___anonymous489_625}
     */
    getDatasetInfo : function (dataset){
      return getDatasetInfo(dataset);
    },
    /**
     * donwloads a dataset from databaseid
     * @param datasetId
     * @param format json, csv
     * @param callback(err, dataset_file)
     */
    downloadDataset : function(datasetId, format, callback){
      downloadDataset(datasetId, format, callback);
    }
};

/**
 * returns normalized datasetInfo from idf dataset.
 * @param dataset
 * @returns {___anonymous489_625}
 */
function getDatasetInfo(dataset){
  var datasetInfo = {
    title : dataset.metas.title,
    dataset_ref : dataset.datasetid,
    revision : dataset.metas.modified,
    user_url : dataset.metas.references
  };
  return datasetInfo;
}

/**
 * fetch IDF datasets descriptor
 * @param callback(datasets_filename)
 * @param date sous la forme YYYY/MM/DD (optional)
 */
function fetchIDFDatasets(callback, date){
  console.log("loading IDF dataset");
  var datasets_url = idf_url+"api/datasets/1.0/search?rows=10000000";
  var datasets_file = "idf-datasets.json";
  if(date){
    datasets_url+="&facet=modified&refine.modified="+date.replace("/","%2F");
    datasets_file = "idf-datasets-"+date.replace("/","-")+".json";
  }
  request(datasets_url, 
      function(error, response, body){
          if(error){
              if(done) done(error);
              return;
          }
          console.log("IDF dataset : loaded");
          if(callback)
            callback(datasets_file);
      }
  ).pipe(fs.createWriteStream(datasets_file));
}

/**
 * passes all datasets through datasetProcessor
 * @param dataset_file
 * @param concurency
 * @param datasetProcessor(dataset, callback)
 * @param callback(err)
 */
function processAllDatasets(dataset_file, concurency, datasetProcessor, callback){
  fs.readFile(dataset_file, 'utf8', function (err, data) {
    if (err) {
      console.log('Error: ' + err);
      return callback(err);
    }
    datasets = JSON.parse(data).datasets;
    
    var batch = new Batch;
    batch.concurrency(concurency);

    // process all datasets.
    var count = 0;
    var length = datasets.length;
    for(var i=0; i < length; i++){
      (function(dataset){
        batch.push(function(done){
          datasetProcessor(dataset, done);       
        });
      })(datasets[i]);
    }
    
    // end.
    batch.end(function(err, data){
      if(err) {
        console.log("PROCESS ALL DATASET ERROR",err);
        if(callback) callback(err);
        return;
      }
      console.log("all datasets processed");
      if(callback)
        return callback();
    });
  });
}

/**
 * download and process the dataset.
 * @param dataset
 * @param callback
 */
function processDataset(dataset, callback){
  downloadDataset(dataset.datasetId, "csv", function(filename){
    processCsvDataset(filename, callback);
  });
}

/**
 * get all datasetids based on dataset file.
 * @param dataset_file le fichier datasets.json
 * @param callback(datasetIds[])
 */
function getAllDatasetIds(dataset_file, callback){
  var datasetids = [];
  fs.readFile(dataset_file, 'utf8', function (err, data) {
    if (err) {
      console.log('Error: ' + err);
      return;
    }
    datasets = JSON.parse(data).datasets;
    var count = 0;
    for(var i=0; i < datasets.length; i++){
      var datasetid = datasets[i].datasetid;
      var features = datasets[i].features;
      //console.log(features);
      if(features.indexOf("geo")>-1){
        // console.log(i,count++,datasetid);
        datasetids.push(datasetid);
      }
    }
    if(callback)
      callback(datasetids);
  });
}

/**
 * download all dataset files based based on datasetIds
 * @param datasetIds : array of datasetid
 * @param format json, csv
 */
function donwloadAllDatasets(datasetIds, format, callback){
  console.log("donwload all datasets : count ="+datasetIds.length);
  var batch = new Batch;
  batch.concurrency(3);
  
  for(var i = 0; i<datasetIds.length; i++){
    (function(datasetId){
      batch.push(function(done){
        downloadDataset(datasetId, format, done);         
      });
    })(datasetIds[i]);
  }
  
  batch.end(function(err, data){
    if(err)
        console.log("DONWLOAD ALL ERROR",err);
    console.log("all datasets donwloaded");
    if(callback)
      callback();
  });
}

/**
 * donwloads a dataset from databaseid
 * @param datasetInfo
 * @param format json, csv
 * @param callback(err, dataset_file)
 */
function downloadDataset(datasetInfo, format, callback){
  var datasetId = datasetInfo.dataset_ref;
  var download_url = idf_url+"api/records/1.0/download?dataset="+datasetId+"&format="+format;
  var dataset_file = "datasets/"+format+"/"+datasetId+"."+format;
  datasetInfo.data_url = download_url;
  datasetInfo.filepath = dataset_file;
  console.log("donwloading :",datasetId);
  request(download_url, 
      function(error, response, body){
          if(error){
              console.log("DOWNLOAD ERROR",error);
              if(callback)
                callback(error);
              return;
          }
          console.log(datasetId, "downloaded");
          if(callback)
            callback(null, dataset_file);
      }
  ).pipe(fs.createWriteStream(dataset_file));
}


//1/ loading all datasets.
// fetchIDFDatasets(function(datasets_file){
function run(){
  getAllDatasetIds("idf-datasets.json", function(datasetIds){
    donwloadAllDatasets(datasetIds, "csv", function(){
      console.log("gameover");
    });
  });
}