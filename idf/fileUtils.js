/**
 * New node file
 */
var fs    = require("fs");
var Batch = require("batch");

module.exports = {
    processDirectory : function(path, fileProcessor, concurency, recursive, done){
      processDirectory(path, fileProcessor, concurency, recursive, done);
    }
};

/**
 * process all files within the given directory.
 * @param path the directory to be processed
 * @param fileProcessor(filename, done) le file processor
 * @param concurency the nuber of files to be processed simultaneously.
 * @param recursive
 */
function processDirectory(path, fileProcessor, concurency, recursive, done) {
  console.log("parsing...",path);
  var batch = new Batch;
  batch.concurrency(concurency);
  
  _processDirectory(path, batch, fileProcessor, recursive);
  
  batch.end(function(err, data){
    if(err)
        console.log("PROCESS ALL ERROR",err);
    console.log("all files in directory processed");
    if(done)
      done();
  });
}

/**
 * iner recursive function.
 * @param path
 * @param batch
 * @param fileParser
 * @param recursive
 */
function _processDirectory(path, batch, fileProcessor, recursive) {
  var files = fs.readdirSync(path);
  for(var i=0;i<files.length;i++){
      var file = files[i];
      var filePath = path+file;
      console.log(">>",filePath);
      var stats = fs.statSync(filePath);
      if(stats.isDirectory()){
          // c'est un dossier.
          if(recursive){
            processDirectory(filePath, batch, fileProcessor, recursive);
          }
      } else {
        (function(filePath){
          batch.push(function(done){
            fileProcessor(filePath, done);         
          });
        })(filePath);
      }   
  }
};

/**
 * indent le fichier JSON.
 * @param filename
 * @param done
 */
function indentJsonDataset(filename, done){
  fs.readFile(filename, 'utf8', function (err, data) {
    if (err) {
      console.log('Error: ' + err);
      done(err);
    }
    json_data = JSON.parse(data);
    fs.writeFile(filename+"-indent",function(err,data){
      if (err) {
        console.log('Error: ' + err);
        done(err);
      }
    });
  });
}

