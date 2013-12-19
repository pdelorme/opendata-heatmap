function readFileSync_encoding(filename, encoding) {
    var content = fs.readFileSync(filename);
    var converter = new Iconv(encoding, 'UTF-8');
    var buffer = converter.convert(content);
    return buffer.toString('utf8');
}

function readStream_encoding(filename, encoding) {
    var converter = new Iconv(encoding, 'UTF-8');
	fs.createReadStream(filename, {
	  'bufferSize': 4 * 1024
	}).pipe(converter).pipe()
}	
//readStream_encoding(filepath,utf8encoder);

var input = fs.createReadStream(filepath,
        { bufferSize: 64,
          lowWaterMark: 0,
          highWaterMark: 64 });
var outfile = __dirname + '/tmp/lorem-ipsum.txt';
var output = fs.createWriteStream(outfile);
input.pipe(Iconv('ascii', 'utf-16le')).pipe(output);



var rowString = JSON.stringify(row);
console.log('>avant :',rowString);
var buffer = utf8encoder.convert(rowString);
var utf8String = buffer.toString('utf8');
console.log('>utf8 :',utf8String);
utf8row =  JSON.parse(utf8String);
//utf8row = row;
