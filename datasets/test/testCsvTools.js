/**
 * New node file
 */
var assert = require('assert');
var stream = require('stream');
var fs = require('fs');
var csvTools  = require('../libs/csvTools');

describe('csvTools', function() {

  it('parsing simple csv with number ending', function() {
    result = csvTools.parseCSVLine("12,45.7,\"toto\",\'titi',69",',');
    assert.equal(result.length, 5);
    assert.equal(result[0],12);
    assert.equal(result[1],45.7);
    assert.equal(result[2],"toto");
    assert.equal(result[3],"titi");
    assert.equal(result[4],69);
  });

  it('parsing simple csv string ending', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',\"tata\"",',');
    assert.equal(result.length, 5);
    assert.equal(result[0],12);
    assert.equal(result[1],45.7);
    assert.equal(result[2],"to'to");
    assert.equal(result[3],"titi");
    assert.equal(result[4],"tata");
  });

  it('parsing simple csv with extra spaces', function() {
    result = csvTools.parseCSVLine(" 12 , 45.7 ,\" toto \",\' titi ', 69 ",',');
    assert.equal(result.length, 5);
    assert.equal(result[0],12);
    assert.equal(result[1],45.7);
    assert.equal(result[2],"toto");
    assert.equal(result[3],"titi");
    assert.equal(result[4],69);
  });

  it('parsing csv with columns and string ending', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',\"tata\"",',',['val1','val2','text1','text2','last_val']);
    assert.equal(Object.keys(result).length, 5);
    assert.equal(result.val1,12);
    assert.equal(result.val2,45.7);
    assert.equal(result.text1,"to'to");
    assert.equal(result.text2,"titi");
    assert.equal(result.last_val,"tata");
  });

  it('parsing csv with columns and number ending', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',22",',',['val1','val2','text1','text2','last_val']);
    assert.equal(Object.keys(result).length, 5);
    assert.equal(result.val1,12);
    assert.equal(result.val2,45.7);
    assert.equal(result.text1,"to'to");
    assert.equal(result.text2,"titi");
    assert.equal(result.last_val,22);
  });

  it('parsing simple csv doubled single quotes', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to''to\",22",',');
    assert.equal(Object.keys(result).length, 4);
    assert.equal(result[0],12);
    assert.equal(result[1],45.7);
    assert.equal(result[2],"to''to");
    assert.equal(result[3],22);
  });

  it('parsing simple csv doubled double quotes', function() {
    result = csvTools.parseCSVLine("12,45.7,\"ti\"\"ti\",22",',');
    assert.equal(Object.keys(result).length, 4);
    assert.equal(result[0],12);
    assert.equal(result[1],45.7);
    assert.equal(result[2],"ti\"ti");
    assert.equal(result[3],22);
  });

  it('parsing csv with too many columns', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',22",',',['val1','val2','text1','text2','last_val','the_end']);
    assert.equal(Object.keys(result).length, 6);
    assert.equal(result.val1,12);
    assert.equal(result.val2,45.7);
    assert.equal(result.text1,"to'to");
    assert.equal(result.text2,"titi");
    assert.equal(result.last_val,22);
    assert.equal(result.the_end,undefined);
  });

  it('parsing csv with too few columns', function() {
    result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',22",',',['val1','val2','text1','text2']);
    assert.equal(Object.keys(result).length, 5);
    assert.equal(result.val1,12);
    assert.equal(result.val2,45.7);
    assert.equal(result.text1,"to'to");
    assert.equal(result.text2,"titi");
    assert.equal(result.c4,22);
  });

  it('parsing csv chunk', function(done) {
    rowIndex = 0;
    result = csvTools.parseCSVChunk("12,45.7,\"toto\",\'titi',69\n  24 ,\t2 ,  \"tata\"  ,  toto  ,35\n.5,123456789,\"hello\nworld\",\"hello\tworld\",\"hello\r\nworld\"",',', function(row){
      assert.equal(row.length, 5);
      switch (rowIndex++) {
        case 0 :
          assert.equal(row[0],12);
          assert.equal(row[1],45.7);
          assert.equal(row[2],"toto");
          assert.equal(row[3],"titi");
          assert.equal(row[4],69);
          break;
        case 1 :
          assert.equal(row[0],24);
          assert.equal(row[1],2);
          assert.equal(row[2],"tata");
          assert.equal(row[3],"toto");
          assert.equal(row[4],35);
          break;
        case 2 :
          assert.equal(row[0],0.5);
          assert.equal(row[1],123456789);
          assert.equal(row[2],"hello\nworld");
          assert.equal(row[3],"hello\tworld");
          assert.equal(row[4],"hello\r\nworld");
          break;
        default :
          throw new Error("no 4rd row :"+row);
      }
    }, true);
    assert.equal(result,"");
    done();
  });

  it('parsing csv chunks', function(done) {
    result1 = csvTools.parseCSVChunk("12,45.7,\"toto\",\'titi',69\n  24 ,\t2 ,  \"tata\"  ,  toto  ,35\n.5,123456789,\"hello\nworld\",\"hello\tworld\",\"hello\r\nwor",',',
      function(row){
        // do not check first chunk
      }, false);
    assert.equal(result1,".5,123456789,\"hello\nworld\",\"hello\tworld\",\"hello\r\nwor");
    rowIndex = 0;
    result2 = csvTools.parseCSVChunk(result1+"ld\"",',',
      function(row){
        // check second chunk
        assert.equal(row.length, 5);
        switch (rowIndex++) {
          case 0 :
            assert.equal(row[0],0.5);
            assert.equal(row[1],123456789);
            assert.equal(row[2],"hello\nworld");
            assert.equal(row[3],"hello\tworld");
            assert.equal(row[4],"hello\r\nworld");
            break;
          default :
            throw new Error("no 4rd row :"+row);
        }
      }, true);
    assert.equal(result2,"");
    done();
  });

  it('parsing csv stream', function(done) {
   itStream = new stream.Readable();
   itStream.push("12,45.7,\"toto\",\'titi',69\n  24 ,\t2 ,  \"tata\"  ,  toto  ,35\n.5,123456789,\"hello\nworld\",\"hello");
   itStream.push("\tworld\",\"hello\r\nworld\"");
   itStream.push(null);
   rowIndex = 0;
   csvTools.parseCSVReader(itStream,
      null,
      function( row ){
        // row Processor
        assert.equal(row.length, 5);
        switch (rowIndex++) {
          case 0 :
            assert.equal(row[0],12);
            assert.equal(row[1],45.7);
            assert.equal(row[2],"toto");
            assert.equal(row[3],"titi");
            assert.equal(row[4],69);
            break;
          case 1 :
            assert.equal(row[0],24);
            assert.equal(row[1],2);
            assert.equal(row[2],"tata");
            assert.equal(row[3],"toto");
            assert.equal(row[4],35);
            break;
          case 2 :
            assert.equal(row[0],0.5);
            assert.equal(row[1],123456789);
            assert.equal(row[2],"hello\nworld");
            assert.equal(row[3],"hello\tworld");
            assert.equal(row[4],"hello\r\nworld");
            break;
          default :
            throw new Error("no 3rd row :"+row);
        }
      },
      function() {
        assert.equal(rowIndex,3);
        // end callback
        done();
      }
    );
  });

  it('parsing csv stream with valid headers', function(done) {
   itStream = new stream.Readable();
   itStream.push("val1,val2,txt1,txt2,'ext1'\n12,45.7,\"toto\",\'titi',69\n  24 ,\t2 ,  \"tata\"  ,  toto  ,35\n.5,123456789,\"hello\nworld\",\"hello\tworld\",\"hello\r\nworld\"");
   itStream.push(null);
   rowIndex = 0;
   csvTools.parseCSVReader(itStream,
      function( headers ){
        return true
      },
      function( row ) {
        // row Processor
        switch (rowIndex++) {
          case 0 :
            assert.equal(row.val1,12);
            assert.equal(row.val2,45.7);
            assert.equal(row.txt1,"toto");
            assert.equal(row.txt2,"titi");
            assert.equal(row.ext1,69);
            break;
          case 1 :
            assert.equal(row.val1,24);
            assert.equal(row.val2,2);
            assert.equal(row.txt1,"tata");
            assert.equal(row.txt2,"toto");
            assert.equal(row.ext1,35);
            break;
          case 2 :
            assert.equal(row.val1,0.5);
            assert.equal(row.val2,123456789);
            assert.equal(row.txt1,"hello\nworld");
            assert.equal(row.txt2,"hello\tworld");
            assert.equal(row.ext1,"hello\r\nworld");
            break;
          default :
             throw new Error("too many rows :"+row);
        }
      },
      function() {
        // end callback
        assert.equal(rowIndex, 3);
        done();
      }
    );
  });

  it('parsing csv stream2 with invalid headers', function(done) {
   itStream = new stream.Readable();
   itStream.push("val1,val2,txt1,txt2,'ext1'\n12,45.7,\"toto\",\'titi',69\n  24 ,\t2 ,  \"tata\"  ,  toto  ,35\n.5,123456789,\"hello\nworld\",\"hello\tworld\",\"hello\r\nworld\"");
   itStream.push(null);
   csvTools.parseCSVReader(itStream,
      function( headers ){ return false },
      function( row ) {
        throw new Error("stream should be skipped");
      },
      function() {
        // end callback
        done();
      }
    );
  });

  it('parsing csv file stream with short buffer', function(done) {
    var reader = fs.createReadStream("test/testCsvTools-1.csv", { highWaterMark: 8 });
    rowIndex = 0;
    csvTools.parseCSVReader(reader,
      null,
      function( row ){
        // row Processor
        assert.equal(row.length, 5);
        switch (rowIndex++) {
          case 0 :
            assert.equal(row[0],12);
            assert.equal(row[1],45.7);
            assert.equal(row[2],"toto");
            assert.equal(row[3],"titi");
            assert.equal(row[4],69);
            break;
          case 1 :
            assert.equal(row[0],24);
            assert.equal(row[1],2);
            assert.equal(row[2],"tata");
            assert.equal(row[3],"toto");
            assert.equal(row[4],35);
            break;
          case 2 :
            assert.equal(row[0],0.5);
            assert.equal(row[1],123456789);
            assert.equal(row[2],"hello\nworld");
            assert.equal(row[3],"hello\tworld");
            assert.equal(row[4],"hello\n\nworld");
            break;
          default :
            throw new Error("no 3rd row :"+row);
        }
      },
      function() {
        assert.equal(rowIndex,3);
        // end callback
        done();
      }
    );
  });


});