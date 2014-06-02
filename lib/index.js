var
  mongodb = require('mongodb'),
  Client = require('mongodb').MongoClient,
  traverse = require('traverse'),
  async = require('async');

module.exports = MongoDBProvider;

function MongoDBProvider(options, helpers, callback) {
  if (!(this instanceof MongoDBProvider)) return new MongoDBProvider(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'MongoDB';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  this.client = null;
  this.db = null;
  this.dbs = {};

  if (!this.options.dsn)
    return callback('Provider [' + this.name + '] initialized without valid DSN.');

  return this.init(options, function (err) {
    if (err)
      return callback(err);

    return callback(null, self);
  });
}

MongoDBProvider.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Initializing connection to provider [' + self.name + '].');

  return self.openConnection(options, callback);
};

MongoDBProvider.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  var calls = [];
  Object.keys(self.dbs).forEach(function (key) {
    var db = self.dbs[key];
    var call = function (callback) {
      self.logger.info('Destroying connection to database [' + key + '].');
      self.closeConnection(db, callback);
    };
    calls.push(call);
  });
  async.series(calls, function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};

MongoDBProvider.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;

  if (!Array.isArray(documents))
    documents = [documents];

  var furnish = function (collection, documents, callback) {
    var bucket = {};

    var dimensions = [];
    var metrics = [];
    var dateDimensions = [];
    traverse(collection.meta).forEach(function (attribute) {
      if (attribute.datatype === 'date' && attribute.key !== 'ourTimestamp')
        dateDimensions.push(attribute);
      if (attribute.type === 'dimension') {
        attribute.path = this.path.join('.');
        this.update(attribute);
        dimensions.push(attribute);
      }
      else if (attribute.type === 'metric') {
        attribute.path = this.path.join('.');
        if (collection[attribute.key] && collection[attribute.key].hasOwnProperty('min'))
          attribute.min = collection[attribute.key].min;
        if (collection[attribute.key] && collection[attribute.key].hasOwnProperty('max'))
          attribute.max = collection[attribute.key].max;
        this.update(attribute);
        metrics.push(attribute);
      }
    });

    documents.forEach(function (document, index) {
      var patched = false;
      dateDimensions.forEach(function (dateDimension) {
        var _dimension = self.common.extend({}, dateDimension);
        if (!document[_dimension.key]) {
          document[_dimension.key] = new Date().getTime();
          patched = true;
        }

        var _date = new Date(document[_dimension.key]);
        bucket.dow = new Date(_date).getDay();
        bucket.hod = new Date(_date).getHours();
        _date.setMilliseconds(0);
        bucket.second = new Date(_date);
        _date.setSeconds(0);
        bucket.minute = new Date(_date);
        _date.setMinutes(0);
        bucket.hour = new Date(_date);
        _date.setUTCHours(0, 0, 0, 0);
        bucket.ddate = new Date(_date);
        _date.setDate(1);
        bucket.month = new Date(_date);
        _date.setMonth(0);
        bucket.year = new Date(_date);

        document[dateDimension.key + '_timebucket'] = bucket;
        document[dateDimension.key] = new Date(document[_dimension.key]);//bucket.second;
      });


      document.ourTimestamp = new Date();
      var documentKey = {};
      dimensions.forEach(function (dimension) {
        //var d = collection.dimensions[key];
        if (['timestamp', '_key', 'ourTimestamp'].indexOf(dimension.key) === -1)
          documentKey[dimension.key] = document[dimension.key];
        else if (dimension.key === 'timestamp')
          documentKey[dimension.key] = document[dimension.key].toISOString();
      });

      //this will ensure that if we assign the timestamp, there's no collision
      if (patched && documents.length > 1) {
        documentKey.index = index;
      }

      if (collection.unique || true)
        document._key = self.common.hash(JSON.stringify(documentKey).toString());
      else
        document._key = self.common.uuid();

      documents[index] = document;
    });

    return setImmediate(function () {
      return callback(null, collection, collection.meta);
    });
  };

  var individualInsert = function (collection, documents, callback) {
    //we have an array of documents failing over duplicates, try one by one.
    var expected = documents.length;
    var aborted = false;
    documents.forEach(function (document) {
      if (!aborted) {
        collection.insert(document, options, function (err, result) {
          expected--;
          document.saved = err ? false : true;
          document.error = err ? err.message : null;
          if (err && err.code == 11000) {
            //ignore
          }
          else if (err) {
            aborted = true;
            return callback(err);
          }
          if (expected <= 0 && !aborted) {
            return callback(null, documents);
          }
        });
      }
    });
  };

  return furnish(collection, documents, function (err) {
    if (err)
      return callback(err);

    self.db.collection(collection.storeKey, options.collectionOptions, function (err, collection) {
      if (err)
        return callback(err);

      if (!collection)
        return callback(new Error('Failed to insert document(s) into collection [' + collection + ']@[' + self.db.options.url + ']'));

      return collection.insert(documents, options, function (err, result) {
        if (err && err.code == 11000 && Array.isArray(documents)) {
          individualInsert(collection, documents, callback);
        }
        else if (err && err.toString().indexOf('maximum allowed bson size') > -1) {
          individualInsert(collection, documents, callback);
        }
        else if (err) {
          require('util').inspect(err);
          return callback(err);
        }
        else {
          documents.forEach(function (doc) {
            doc.saved = true;
          });
          return callback(null, result);
        }
      });
    });
  });
};

MongoDBProvider.prototype.query = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  //expected DSN format: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
  //reference: http://docs.mongodb.org/manual/reference/connection-string/

  //check if we have a cached connection
  if (self.dbs[options.dsn])
    return self.checkConnection(self.dbs[options.dsn], callback);

  this.logger.trace('Open MongoDB connection @ ' + this.options.dsn);
  if (!self.client)
    self.client = new Client();
  self.client.connect(options.dsn, function (err, db) {
    if (err)
      return callback(err);

    self.dbs[options.dsn] = db;
    self.db = db;
    return callback(null, db);
  });
};

MongoDBProvider.prototype.closeConnection = function (db, callback) {
  callback = callback || function () {
  };

  var self = this;

  return db.close(callback);
};

MongoDBProvider.prototype.checkConnection = function (db, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, db);
};