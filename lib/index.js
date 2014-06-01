var
  mongodb = require('mongodb'),
  Client = require('mongodb').MongoClient,
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

MongoDBProvider.prototype.insert = function (collectionName, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;

  if (!Array.isArray(documents))
    documents = [documents];

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

  self.db.collection(collectionName, options.collectionOptions, function (err, collection) {
    if (err) {
      return callback(err);
    }

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