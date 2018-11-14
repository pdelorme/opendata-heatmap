/**
 * Outils de parsing des CSV.
 */

const 
  fs = require('fs'),
  Batch = require('batch'),
  es = require('event-stream'),
  detect = require('detect-csv'),
  iconv = require('iconv-lite');

module.exports = {
    parseCSVFile: function(filename, columnFilter, rowProcessor, endCallback){
      parseCSVFile(filename, columnFilter, rowProcessor, endCallback);
    },
    parseCSVLine : function(line, columns, separator){
      return parseCSVLine(line,columns,separator);
    }
};

/**
 * parse le fichier filename.
 * @param filename le chemin du fichier.
 * @param rowProcessor une fonction à laquelle est passé la ligne.
 * @param columnFilter une fontion qui retourne true si le fichier doit etre traité (optionnel).
 * @param endCallback appellé en fin de traitement. en erreur si parametre.
 */
function parseCSVFile(filename, columnFilter, rowProcessor, endCallback){
  var first = true;
  var thisFilename = filename;
  var reader = fs.createReadStream(filename).pipe(iconv.decodeStream('utf8'));
  reader
  .pipe(es.split())
  .pipe(es.mapSync(line => {
    if(first){
      first = false;
      // delimiter
      delimiter = detect(line).delimiter;

      // headers
      columns = parseCSVLine(line, delimiter);
      if(columnFilter && columnFilter(columns)){
        throw "Columns not supported";
      }
    } else {
      // console.log('\n\nparsing line in',i++, line);
      data = parseCSVLine(line, delimiter, columns);
      rowProcessor(data);
    }
  })
  .on('error', function(e){
    console.log('>> Error while reading file:',e, thisFilename);
    endCallback('error');
  })
  .on('end', function(){
      console.log('>> Read entirefile :',thisFilename);
      endCallback();
  }))
}

/**
 * parse une ligne CSV en gérant intelligement les quotes.
 */
function parseCSVLine(line, separator, columns){
  newCol=true;
  quote='';
  value='';
  colIndex=0;
  values=[];
  json= new Object();
  for(i=0; i < line.length;i++){
    c = line[i];
    if(newCol){
      newCol=false;
      if(c==='\"' || c==='\''){
        quote=c;
        continue;
      } else
        quote='';
    }
    if(c===quote){
      // right quote.
      if(columns) 
        json[columns[colIndex++].toLowerCase()]=value;
      else
        values.push(value);
      value='';
      // quote='';
      i++;
      if(i<line.length&&line[i]!==separator){
        console.log("WARNING, invalid quote or separator");
      }
      newCol=true;
      continue;
    }
    if(quote==='' && c===separator){
      //end of column.
      if(columns) 
        json[columns[colIndex++].toLowerCase()]=value;
      else
        values.push(value);
      value='';
      // quote='';
      newCol=true;
      continue;
    }
    value=value+c;
  }
  if(quote===''){
    // last column with no quote.
      if(columns) 
        json[columns[colIndex++].toLowerCase()]=value;
      else
        values.push(value);
  }
  if(columns) 
    return json;
  return values;
}