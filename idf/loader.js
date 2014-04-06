/**
 * New node file
 */
var config = require('./config');
var fileUtils = require('./fileUtils');
var csvTools = require('./csvTools');
var idf = require('./idf');

var odhm = require('./odhmService');

function main(){
  /**
   * called for every file.
   * @param filename
   */
  function _fileProcessor(datasetInfo, done){ 
    console.log("processing dataset file :",datasetInfo.dataset_ref);
    // 1/ extract geoObjects from files
    odhm.createDataset(datasetInfo, function(err, dataset_id){
      csvTools.extractCsvGeoObjects(datasetInfo.filepath,
          function(geoObject){
            odhm.insertGeodata(dataset_id, geoObject, null);
          },
          function(err,geoObjects){
            // 2/ insert geoObjets
            odhm.insertDataset(datasetInfo, geoObjects, done);
          }
      );
    })

  }
  
  /**
   * download and update the dataset.
   * @param dataset
   * @param done
   */
  function _datasetProcessor(dataset, done){
    var datasetInfo = idf.getDatasetInfo(dataset);
    console.log("processing dataset :",datasetInfo.dataset_ref);
    // 1 - check this dataset is not already in database.
    odhm.getDataset(datasetInfo.dataset_ref, datasetInfo.revision, function(err, data){
      if(err)
        return done(err);
      if(data && data.length>0){
        console.log("dataset already processed :",datasetInfo.dataset_ref);
        return done();
      }
      // on peut parser le dataset.
      // callback(err, dataset_file)
      idf.downloadDataset(datasetInfo, "csv", function(err, dataset_file){
        if(err)
          return done(err);
        datasetInfo.filepath = dataset_file;
        _fileProcessor(datasetInfo, done);
      });
    });
  }
  
  
  // 1 - charge le fichier datasets.json
  idf.fetchIDFDatasets(function (dataset_file){
    // 2 - parse le fichier dataset.json
    idf.processAllDatasets(dataset_file, 3, _datasetProcessor, function(err){
      console.log("game over");
    });
  });

};


odhm.init(config, main);

