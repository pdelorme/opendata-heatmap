/**
 * New node file
 */

var fs      = require("fs");
var request = require('request');
var Batch   = require("batch");

var checkLocalDir = true; // when true, we first check files are present locally to avoid unnecessary download.
var datasud_url = "http://trouver.datasud.fr/api/3/action/";

module.exports = {
    /**
     * fetch datasets descriptor
     * @param callback(datasets_filename)
     * @param date sous la forme YYYY/MM/DD (optional)
     */
    fetchDatasets : function(callback, date){
      fetchDatasets(callback, date);
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
     * returns normalized datasetInfo from dataset.
     * @param dataset
     * @returns {___anonymous489_625}
     */
    getDatasetInfo : function (dataset, callback){
      return getDatasetInfo(dataset, callback);
    },
    /**
     * donwloads a dataset from databaseid
     * @param datasetInfo
     * @param format json, csv
     * @param callback(err, dataset_file)
     */
    downloadDataset : function(datasetInfo, callback){
      downloadDataset(datasetInfo, callback);
    }
};

/**
 * returns normalized datasetInfo from dataset.
 * @param dataset
 * @returns {___anonymous489_625}
 */
function getDatasetInfo(datasetId, callback){
  if(datasetId==="base-sirene-des-entreprises-et-de-leurs-etablissements-en-provence-alpes-cote-dazur"){
    callback("SKIPING dataset");
  }
  // https://trouver.datasud.fr/api/3/action/package_show?id=episodes-de-pollution-pour-les-departements-de-la-region-sud-paca
  var download_url = datasud_url+"package_show?id="+datasetId;
  var package_dir = "paca/datasets/packages";
  if(!fs.existsSync(package_dir)){
    fs.mkdirSync(package_dir);
  }
  var dataset_file = package_dir+"/"+datasetId+".packageInfo";
  var thisDatasetId = datasetId;
  //console.log("downloading DataSetInfo :",datasetId);
  if(checkLocalDir && fs.existsSync(dataset_file)) {
    // console.log("getting dataset.packageInfo from local folder:", datasetId);
    return readDatasetInfo(dataset_file, thisDatasetId, callback);
  }
  request(download_url,
      function(error, response, body){
          if(error){
              console.log("DOWNLOAD ERROR",error);
              if(callback)
                callback(error);
              return;
          }
          // // console.log(datasetId, "downloaded");
          // var packageInfo = JSON.parse(body).result;
          // var hasResource = false;
          // var avalaibleFormats="";
          // for(var i=0; i < packageInfo.resources.length; i++){
          //   var resource = packageInfo.resources[i];
          //   var datasetInfo = {
          //     title : packageInfo.title,
          //     dataset_ref : thisDatasetId,
          //     revision :  packageInfo.revision_id,
          //     user_url :  resource.url,
          //     data_url : resource.url,
          //     format   : resource.format
          //   };
          //   if(resource.format=='CSV'){
          //     hasResource = true;
          //     return callback(null, datasetInfo);
          //   }
          //   avalaibleFormats+=(avalaibleFormats===""?"":",")+resource.format;
          // }
          // if(!hasResource)
          //   return callback("Formats : "+ avalaibleFormats);
          readDatasetInfo(dataset_file, thisDatasetId, callback);
      }
  ).pipe(fs.createWriteStream(dataset_file));
}

/**
 * read dataset_info from file
 */
function readDatasetInfo(dataset_file, thisDatasetId, callback){
  fs.readFile(dataset_file, 'utf8', function (err, body) {
    if (err) {
      console.log('Error: ' + err);
      return callback(err);
    }
    var packageInfo = ""
    try {
      var packageInfo = JSON.parse(body).result;
    } catch (e){
      console.log(body);
      throw e;
    }
    var hasResource = false;
    var avalaibleFormats="";
    for(var i=0; i < packageInfo.resources.length; i++){
      var resource = packageInfo.resources[i];
      var datasetInfo = {
        title : packageInfo.title,
        dataset_ref : thisDatasetId,
        revision :  packageInfo.revision_id,
        user_url :  resource.url,
        data_url : resource.url,
        format   : resource.format
      };
      if(resource.format=='CSV'){
        hasResource = true;
        return callback(null, datasetInfo);
      }
      avalaibleFormats+=(avalaibleFormats===""?"":",")+resource.format;
    }
    if(!hasResource)
      return callback("Formats : "+ avalaibleFormats);
  });
}
/**
 * donwloads a dataset from databaseInfo
 * @param datasetInfo
 * @param format json, csv
 * @param callback(err, dataset_file)
 */

function downloadDataset(datasetInfo, callback){
  var datasetId = datasetInfo.dataset_ref;
  var download_url = datasetInfo.data_url;
  var dataset_dir  = "paca/datasets/"+datasetInfo.format;
  var dataset_file = dataset_dir +"/"+datasetId;
  if(!fs.existsSync(dataset_dir)){
    fs.mkdirSync(dataset_dir);
  }
  datasetInfo.data_url = download_url;
  datasetInfo.filepath = dataset_file;
  // don't download if already downloaded.
  if(checkLocalDir && fs.existsSync(dataset_file)) {
    // console.log("getting dataset from local folder:", datasetId);
    if(callback)
      callback(null, dataset_file);
    return;
  }
  // console.log("donwloading DataSet:",datasetId);
  request(download_url,
      function(error, response, body){
          if(error){
              console.log(">! DOWNLOAD ERROR",error);
              if(callback)
                callback(error);
              return;
          }
          //console.log(datasetId, "downloaded");
          if(callback)
            callback(null, dataset_file);
      }
  ).pipe(fs.createWriteStream(dataset_file));
}

/**
 * fetch datasets descriptor
 * @param callback(datasets_filename)
 * @param date sous la forme YYYY/MM/DD (optional)
 */
function fetchDatasets(callback, date){
  console.log("loading PACASUD dataset");
  var datasets_url = datasud_url+"package_list";
  var datasets_file = "paca/datasud-datasets.json";
  if(date){
    console.log("fetching Dataset from DATE not implemented. returning full dataset");
  }
  console.log("fetching url :"+datasets_url);
  request(datasets_url,
      function(error, response, body){
          if (error) throw err;
          //var Datasets = JSON.parse(body);
          fs.writeFile(datasets_file, body, function (err) {
            if (err) throw err;
            console.log("PACASUD dataset : loaded & saved");
            if(callback)
              callback(datasets_file);
          });
      }
  );
}

/**
 * passes all datasets through datasetProcessor
 * @param dataset_file
 * @param concurency
 * @param datasetProcessor(dataset, callback)
 * @param callback(err)
 */
function processAllDatasets(dataset_file, concurency, datasetProcessor, callback){
  console.log("processAlldatasets", dataset_file);
  fs.readFile(dataset_file, 'utf8', function (err, data) {
    if (err) {
      console.log('Error: ' + err);
      return callback(err);
    }
    datasets = JSON.parse(data).result;

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



//1/ loading all datasets.
// fetchDatasets(function(datasets_file){
function run(){
  getAllDatasetIds("paca/datasud-datasets.json", function(datasetIds){
    donwloadAllDatasets(datasetIds, "csv", function(){
      console.log("gameover");
    });
  });
}