/**
 * Outils de parsing des CSV.
 */

const LogLevel = { "DEBUG":1,"DEBUG2":2,"INFO":3};
const Log = LogLevel.INFO;

const 
  fs = require('fs'),
  Batch = require('batch'),
  es = require('event-stream'),
  detect = require('detect-csv'),
  iconv = require('iconv-lite'),
  through = require('through'),
  Decoder = require('string_decoder').StringDecoder;


module.exports = {
    parseCSVFile: function(filename, columnFilter, rowProcessor, endCallback){
      parseCSVFile(filename, columnFilter, rowProcessor, endCallback);
    },
    parseCSVReader: function(reader, columnFilter, rowProcessor, endCallback){
      parseCSVReader(reader, columnFilter, rowProcessor, endCallback);
    },
    parseCSVLine : function(line, columns, separator){
      return parseCSVLine(line,columns,separator);
    },
    parseCSVChunk : function(chunk, separator, rowProcessor, processTrailing){
      return parseCSVChunk(chunk, separator, rowProcessor, processTrailing);
    },
    splitCSV : function(){
      return splitCSV();
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
  return parseCSVReader(reader, columnFilter, rowProcessor, endCallback);
}

function parseCSVReader(reader, columnFilter, rowProcessor, endCallback) {
  reader
  .pipe(splitCSV())
  .pipe(es.mapSync(data => {
    //console.log(">>> ", data);
    rowProcessor(data);
  })
  .on('error', function(e){
    if( Log == LogLevel.DEBUG ) console.log('>> Error while reading stream:',e);
    endCallback('error');
  })
  .on('end', function(){
      if( Log == LogLevel.DEBUG ) console.log('>> Read entire stream');
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
      // check it is not an escape caractere.
      if(line[i+1]===quote){
        // doubled quote : skip next and continue.
        value=value+c;
        i++;
        continue;
      }
      // right quote.
      if(columns)
        json = buildJsonResult(colIndex++, columns, value, json);
      else
        values = buildRawResult(value, values, line);
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
        json = buildJsonResult(colIndex++, columns, value, json);
      else
        values = buildRawResult(value, values, line);
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
      json = buildJsonResult(colIndex++, columns, value, json);
    else
      values = buildRawResult(value, values, line);
  }
  if(columns) {
    for(i=colIndex;i< columns.length; i++){
      json = buildJsonResult(i, columns, undefined, json);
    }
    return json;
  }
  return values;
}

function buildJsonResult(colIndex, columns, value, json){
  if(columns){
    column = columns[colIndex];
    if(column)
      colum=column.toLowerCase();
    else {
      column = 'c'+colIndex;
    }
    if(value)
      value=value.trim();
    json[column]=value;
  }
  return json;
}

/**
 * convertit un tableau de valeur et de colonnes en object json.
 * @param values un tableau de valeurs
 * @param columns un tableau de colonnes.
 */
function arrayToJson(values, columns){
  json = new Object();
  for(i=0;i<values.length;i++){
    column = columns[i];
    if(column)
      colum=column.toLowerCase();
    else {
      column = 'c'+i;
    }
    value = values[i];
    if(value)
      value=value.trim();
    json[column]=value;
  }
  return json;
}

function buildRawResult(value, values){
  if(value)
      value=value.trim();
  values.push(value.trim());
  return values;
}

function cleanValue(value){
  if ( Log == LogLevel.DEBUG ) console.log(">>", value);
  if(value)
    return value.trim();
  return value;

}
/**
 * parse un bout de CSV en gérant intelligement les quotes.
 * @param chunk le buffer en cours.
   @param separator le separateur choisi
   @param rowProcessor un processeur de tableau de valeur.
 * @return le reste des données non traités.
 */
function parseCSVChunk(chunk, separator, rowProcessor, processTrailing){
  // exit on empty string.
  if( chunk === null || chunk.trim() === '')
    return '';

  // index of last unparsed data.
  lastRowIndex=0; 
  
  // new line
  newCol=true;
  endCol=false;
  values=[];
  current='';
  closing = false;

  // parsing chunk.
  for( i=0; i < chunk.length; i++ ){
    // last char
    last = current;
    // reading chars.
    current = chunk[i];
    
    // console.log(c1);
    c2 = chunk[i+1];

    // starting a column, skiping white spaces?
    if( (newCol || endCol) && current === ' ' ){
      if( Log == LogLevel.DEBUG ) console.log("skipping white spaces");
      continue;
    }

    // starting a column, is it quoted ?
    if( newCol ){      // init column
      value='';
      quote='';
      newCol=false;
      // is it quoted ?
      if( current==='\"' || current==='\'' ){
        quote = current;
        closing = false;
        if( Log == LogLevel.DEBUG ) console.log("new quoted column", quote);
        continue;
      }
      if( Log == LogLevel.DEBUG ) console.log("new column");
    }

    // found a quote is it doubled ?
    if( current === quote && last === current ) {
      if( Log == LogLevel.DEBUG ) console.log("escaping doubled quotes");
      closing = false;
      value=value+current;
      continue;
    }

    // found a quote is it doubled ?
    if( current === quote ) {
      if( Log == LogLevel.DEBUG ) console.log("closing quote ?");
      closing=true;
      continue;
    }

    if( last === quote && closing ) {
      if( Log == LogLevel.DEBUG ) console.log("after closing quote");
      quote='';
      endCol = true;
      if( current === ' ') {
        if( Log == LogLevel.DEBUG ) console.log("skipping white spaces after quote");
        continue;
      }
    }

    // found separator in valid state. end of col !
    if( quote==='' && current===separator ){
      if( Log == LogLevel.DEBUG2 ) console.log("end of col ", cleanValue(value));
      //end of column.
      values.push(cleanValue(value));
      endCol = false;
      newCol = true;
      continue;
    }

    // skipping /r
    if( current === '\r'){
      value=value+current;
      continue;
    }

    // found end of line in valid state. process row !
    if( quote==='' && current==='\n') {
      if( Log == LogLevel.DEBUG2 ) console.log("end of line", cleanValue(value));
      // end of col
      values.push(cleanValue(value));
      // end of line.
      rowProcessor(values);
      lastRowIndex = i+1;
      // new line.
      endCol = false;
      newCol = true;
      values=[];
      continue;
    }

    if( endCol ){
      // invalid state. col should be terminated.
      console.log("column should end now", ">"+chunk.substring(i-10>0?i-10:0, i)+"*"+chunk.substring(i,i+10)+"<");
      console.log("char are",last.charCodeAt(0), current?current.charCodeAt(0):'-');
      console.log("index is",i);
      throw new Error("invalid CSV. column should end now");
    }
    value=value+current;
  }
  // end of chunk not reached. 
  // if( closing || endCol ){
  //   if ( Log == LogLevel.DEBUG2 ) console.log("last row",cleanValue(value));
  //   values.push(cleanValue(value));
  //   // end of line.
  //   rowProcessor(values);
  //   return '';
  // }
  // last line. is is complete?
  if( processTrailing ) {
    if ( Log == LogLevel.DEBUG2 ) console.log("processing trail");
    if ( quote==='' || closing || endCol ){
      if ( Log == LogLevel.DEBUG2 ) console.log("end of col", cleanValue(value));
      // end of col
      values.push(cleanValue(value));
      // end of line.
      rowProcessor(values);
      // everything has been processed.
      return '';
    };
    // the last column is expeting some closing quote.
    console.log("ERROR : last value ", values, value, endCol);
    throw new Error("invalid CSV. trailing column is not closed");
  } 

  // returning last line.
  soFar = chunk.substring(lastRowIndex)
  if ( Log == LogLevel.DEBUG2 ) console.log("unprocessed chunk",soFar, lastRowIndex);
  return soFar;
}


/**
 * split a CSV stream into json objects.
 * first row is used for column and separator detection.
 */
function splitCSV () {
  var decoder = new Decoder()
  var soFar = null;
  var firstLine = true;
  var last = false;
  var delimiter = '';
  var columns = [];

  function emit(stream, piece) {
      stream.queue(piece)
  }

  function next (stream, buffer, lastChunk) {
    // soFar + buffer.
    buffer = (soFar != null ? soFar : '') + buffer;
    if( firstLine ){
      // resolve delimiter
      theFirstLine = buffer.split('\n',1)[0];
      delimiter = detect(theFirstLine).delimiter;
      firstLine = false;
    }
    //console.log("last :",lastChunk);
    soFar = parseCSVChunk(buffer, delimiter, function( values ){
      if( firstLine ){
        columns = values;
        firstLine = false;
      } else {
        // json = arrayToJson(values, columns);
        emit(stream, values);
      }
    }, lastChunk);
  }

  return through(
    // write
    function (b) {
      next(this, decoder.write(b), false);
    },
    //end
    function () {
      if( Log == LogLevel.DEBUG2 ) console.log("ending stream", decoder.end());
      next(this, decoder.end(), true);
      
      // soFar should be null
      if(soFar && soFar.size > 0) {
        return emit('error', new Error('maximum buffer reached'))
      }
      this.queue(null)
    }
  )
}
