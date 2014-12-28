var
  Converter = require('csvtojson').core.Converter,
  fs = require('fs'),
  path = require('path'),
  joola = require('joola.sdk'),
  Provider = require('../lib/index');

describe("Bulk load", function () {
  var self = this;
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
    self.fileStream = fs.createReadStream(self.csvFileName);
    self.csvConverter = new Converter({constructResult: false});
    self.fileStream.pipe(self.csvConverter);

    function randomDate(start, end) {
      return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime())).toISOString();
    }

    self.csvConverter.on("end_parsed", function (jsonObj) {
      done();
    });

    self.csvConverter.on("record_parsed", function (resultRow, rawRow, rowIndex) {
      resultRow.timestamp = randomDate(new Date(2013, 0, 1), new Date());
      delete resultRow.hour;

      self.provider.insert({key: 'kaggle2', storeKey: 'kaggle2'}, resultRow, function (err) {
        if (err)
          return done(err);
      });
    });
  });
});