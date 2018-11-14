/**
 * New node file
 */
var config    = require('./config');
var fileUtils = require('./libs/fileUtils');
var csvTools  = require('./libs/csvTools');
var odhm      = require('./libs/odhmService');

var paca      = require('./paca/paca-loader');


function main(){
  /**
   * called for every file.
   * @param filename
   */
  function _fileProcessor(datasetInfo, done){ 
    console.log("> processing dataset file :",datasetInfo.filepath);
    var thisDatasetInfo = datasetInfo;
    // 1/ extract geoObjects from files
    odhm.createDataset(datasetInfo, function(err, dataset_id){
      var csvOptions = {
          columns: true,
          //delimiter: ',',
          quote: '"',
          encoding: 'utf-8'
      };
      csvTools.extractCsvGeoObjects(thisDatasetInfo.filepath,
          function(geoObject){
            odhm.insertGeodata(dataset_id, geoObject, null);
          },
          function(err){
            if(err){
              console.log(">>! aborting file :",thisDatasetInfo.filepath);
              done();
            }
            // console.log(">> end of file :",datasetInfo.filepath);
            done();
          },
          csvOptions
      );
    })

  }
  
  /**
   * download and update the dataset.
   * @param dataset
   * @param done
   */
  function _datasetProcessor(dataset, done){
    var thisDataset=dataset;
    // 1 get dataset info
    paca.getDatasetInfo(dataset, function( err, datasetInfo) {
      if(err) {
        console.log("> skiping dataset",err,thisDataset, datasetInfo);
        return done();
      }
      var thisDatasetInfo = datasetInfo;
      // 2 - check this dataset is not already in database.
      odhm.getDataset(datasetInfo.dataset_ref, datasetInfo.revision, function(err, data){
        if(err)
          return done();
        // console.log(thisDatasetInfo,data);
        if(data && data.length>0){
          console.log("> dataset already processed :",thisDatasetInfo.dataset_ref);
          return done();
        }
        // 3 load dataset
        paca.downloadDataset(thisDatasetInfo, function(err, dataset_file){
          if(err)
            return done();
            // 4 - process data
            _fileProcessor(thisDatasetInfo, done);
          });
        }
      );
    });
  }
  
  // 1 - charge le fichier datasets.json
  paca.fetchDatasets(function (dataset_file){
    // 2 - parse le fichier dataset.json
    paca.processAllDatasets(dataset_file, 3, _datasetProcessor, function(err){
      console.log("!!! game over");
    });
  });

};


odhm.init(config, main);

