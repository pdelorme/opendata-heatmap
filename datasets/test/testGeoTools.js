/**
 * Testing Geo Tools
 */
var assert = require('assert');
var config    = require('../config');
var geoTools  = require('../libs/geoTools');
var odhm      = require('../libs/odhmService');

before(() => {
  return new Promise((resolve) => {
    odhm.init(config, resolve);
  });
});

describe('geoTools', function() {
  it('should read geoObjects from file', function(done) {
    rowIndex = 0;
    geoTools.parseCSVFile(
      "test/testGeoTools.csv",
      function(geoObject){
        // console.log(geoObject);
        switch (rowIndex++) {
          case 0 :
            assert.equal(geoObject.latitude, "1");
            assert.equal(geoObject.longitude,"1");
            break;
          case 1 :
            assert.equal(geoObject.latitude, "2");
            assert.equal(geoObject.longitude,"2");
            break;
          case 2 :
            assert.equal(geoObject.latitude, "3");
            assert.equal(geoObject.longitude,"3");
            break;
          default :
            throw new Error("too many rows :"+row);
        }
      },
      function(err){
        // end of file
        if( err ) console.log("error", err);
        assert.equal(rowIndex,3);
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