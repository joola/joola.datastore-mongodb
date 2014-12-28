var
  Converter = require('csvtojson').core.Converter,
  fs = require('fs'),
  path = require('path'),
  joola = require('joola.sdk'),
  Provider = require('../lib/index');

describe("Bulk load", function () {
  var self = this;
  var data=[];
  before(function (done) {
    self.provider = new Provider({
      dsn: 'mongodb://localhost:27017/cache'
    }, {
      logger: joola.logger,
      common: joola.common
    }, function (err) {
      if (err)
        return done(err);
      self.csvFileName = path.join(__dirname, "./fixtures/small.csv");
      return done();
    });
  });
  


  it("should load a fixtures file", function (done) {
    this.timeout = 99999999999;
    self.fileStream = fs.createReadStream(self.csvFileName);
    self.csvConverter = new Converter({constructResult: false});
    self.fileStream.pipe(self.csvConverter);

    function randomDate(start, end) {
      return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime())).toISOString();
    }

    self.csvConverter.on("end_parsed", function (jsonObj) {
      console.log('done parsing');
      //done();
    });

    var init = false;
    var count = 0;
    self.csvConverter.on("record_parsed", function (resultRow, rawRow, rowIndex) {
      count++;
      init = true;
      resultRow.timestamp = randomDate(new Date(2013, 0, 1), new Date());
      delete resultRow.hour;

      data.push(resultRow);
    });

    var i = 0;
    var timerID = 0;
    var push = function () {
      var _data = data.splice(0, 15000);
      if (_data.length === 0)
        return done();
      self.provider.insert({key: 'demo_kaggle2', storeKey: 'demo_kaggle2'}, _data, {}, function (err) {
        if (err)
          return done(err);

        console.log('Saved chunk', ++i);
        setTimeout(push, 0);
      });
    };
    setTimeout(push, 1000);
  });
});