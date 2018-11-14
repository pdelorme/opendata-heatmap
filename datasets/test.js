/**
 * New node file
 */
var assert = require('assert');
var config    = require('./config');
var fileUtils = require('./libs/fileUtils');
var csvTools  = require('./libs/csvTools');
var geoTools  = require('./libs/geoTools');
var odhm      = require('./libs/odhmService');
var paca      = require('./paca/paca-loader');

before(() => {
  return new Promise((resolve) => {
    odhm.init(config, resolve);
  });
});

describe('csvTools', function() {
  describe('parseCsvTools', function() {
    it('parsing simple csv 1', function() {
      result = csvTools.parseCSVLine("12,45.7,\"toto\",\'titi',69",',');
      assert.equal(result.length, 5);
      assert.equal(result[0],12);
      assert.equal(result[1],45.7);
      assert.equal(result[2],"toto");
      assert.equal(result[3],"titi");
      assert.equal(result[4],69);
    });
    it('parsing simple csv 2', function() {
      result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',\"tata\"",',');
      assert.equal(result.length, 5);
      assert.equal(result[0],12);
      assert.equal(result[1],45.7);
      assert.equal(result[2],"to'to");
      assert.equal(result[3],"titi");
      assert.equal(result[4],"tata");
    });
    it('parsing csv with columns 1', function() {
      result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',\"tata\"",',',['val1','val2','text1','text2','last_val']);
      assert.equal(Object.keys(result).length, 5);
      assert.equal(result.val1,12);
      assert.equal(result.val2,45.7);
      assert.equal(result.text1,"to'to");
      assert.equal(result.text2,"titi");
      assert.equal(result.last_val,"tata");
    });
    it('parsing csv with columns 2', function() {
      result = csvTools.parseCSVLine("12,45.7,\"to'to\",'titi',22",',',['val1','val2','text1','text2','last_val']);
      assert.equal(Object.keys(result).length, 5);
      assert.equal(result.val1,12);
      assert.equal(result.val2,45.7);
      assert.equal(result.text1,"to'to");
      assert.equal(result.text2,"titi");
      assert.equal(result.last_val,22);
    });
    it('extractCsvGeoObjects', function(done) {
      geoTools.extractCsvGeoObjects(
        "test-geo.csv",
        function(geoObject){
          // row processor
          // console.log(">>>", geoObject);
        },
        function(err){
          // end of file
          //console.log(err);
          done();
        }
      );
    });
    it('isGeoDataHeader', function() {
      assert.equal(geoTools.isGeoDataHeader(["lAT","Geo"]), false, "need lat && lon");
      assert.equal(geoTools.isGeoDataHeader(["lAT","Lon"]), true, "should ignore case");
      assert.equal(geoTools.isGeoDataHeader(["lon","wgs84"]), true, "should see wgs84");
      assert.equal(geoTools.isGeoDataHeader(["pim","pam","poum"]), false, "just bulshit");
    });
    

  });
});