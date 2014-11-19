var
  mongodb = require('mongodb'),
  Client = require('mongodb').MongoClient,
  traverse = require('traverse'),
  _ = require('underscore'),
  ce = require('cloneextend'),
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
    var _meta = self.common.extend(collection.meta);
    delete _meta.dimensions;
    delete _meta.metrics;
    delete _meta.description;
    delete _meta.storeKey;
    traverse(_meta).forEach(function (attribute) {
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

        document[dateDimension.key + '_timebucket'] = ce.clone(bucket);
        document[dateDimension.key] = new Date(document[_dimension.key]);//bucket.second;
      });

      document.ourTimestamp = new Date();
      var documentKey = {};
      dimensions.forEach(function (dimension) {
        //var d = collection.dimensions[key];
        if (['timestamp', '_key', 'ourTimestamp'].indexOf(dimension.key) === -1)
          documentKey[dimension.key] = document[dimension.key];
        else if (dimension.key === 'timestamp')
          documentKey[dimension.key] = document[dimension.key].getTime();
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

    self.db.collection(cleanup(collection.storeKey), options.collectionOptions, function (err, collection) {
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

MongoDBProvider.prototype.buildQueryPlan = function (query, callback) {
  var self = this;
  var plan = {
    uid: self.common.uuid(),
    cost: 0,
    colQueries: {},
    query: query
  };
  var $match = {};
  var $project = {};
  var $group = {};
  var $limit;

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    $match.timestamp = {$gte: query.timeframe.start, $lt: query.timeframe.end};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = {$limit: query.timeframe.last_n_items};
  }

  if (query.limit)
    $limit = {$limit: parseInt(query.limit)};

  if (query.filter) {
    query.filter.forEach(function (f) {
      if (f[1] == 'eq')
        $match[f[0]] = f[2];
      else {
        $match[f[0]] = {};
        $match[f[0]]['$' + f[1]] = f[2];
      }
    });
  }

  $group._id = {};
  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        $group._id[dimension.key] = '$' + dimension.key + '_' + query.interval;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $group._id[dimension.key] = '$' + (dimension.attribute || dimension.key);
        break;
      case 'geo':
        break;
      default:
        return setImmediate(function () {
          return callback(new Error('Dimension [' + dimension.key + '] has unknown type of [' + dimension.datatype + ']'));
        });
    }
  });

  if (query.metrics.length === 0) {
    try {
      query.metrics.push({
        key: 'fake',
        dependsOn: 'fake',
        collection: query.collection.key || query.dimensions ? query.dimensions[0].collection : null
      });
    }
    catch (ex) {
      query.metrics = [];
    }
  }

  query.metrics.forEach(function (metric) {
    var colQuery = {
      collection: metric.collection ? metric.collection.key : null,
      query: []
    };

    if (!metric.formula && metric.collection) {
      if (metric.aggregation)
        metric.aggregation = metric.aggregation.toLowerCase();
      if (metric.aggregation == 'ucount')
        colQuery.type = 'ucount';
      else
        colQuery.type = 'plain';

      var _$match = self.common.extend({}, $match);

      var _$unwind;// = '$' + metric.dependsOn || metric._key;
      if (metric.dependsOn.indexOf('.') > 0 && self.common.checkNestedArray(metric.collection, metric.dependsOn))
        _$unwind = '$' + metric.dependsOn.substring(0, metric.dependsOn.indexOf('.')) || metric._key;
      var _$project = self.common.extend({}, $project);
      var _$group = self.common.extend({}, $group);

      if (metric.filter) {
        metric.filter.forEach(function (f) {
          if (f[1] == 'eq')
            _$match[f[0]] = f[2];
          else {
            _$match[f[0]] = {};
            _$match[f[0]]['$' + f[1]] = f[2];
          }
        });
      }
      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match) + JSON.stringify(_$unwind));

      if (colQuery.type == 'plain') {
        if (plan.colQueries[colQuery.key]) {
          if (_$unwind)
            _$group = self.common.extend({}, plan.colQueries[colQuery.key].query[2].$group);
          else
            _$group = self.common.extend({}, plan.colQueries[colQuery.key].query[1].$group);
        }

        if (metric.key !== 'fake') {
          _$group[metric.key] = {};
          if (metric.aggregation == 'count')
            _$group[metric.key].$sum = 1;
          /*else if (metric.aggregation == 'avg') {
           _$group[metric.key]['$' + (typeof metric.aggregation === 'undefined' ? 'sum' : metric.aggregation)] = '$' + metric.attribute;
           _$group[metric.key + '_count'] = {};
           _$group[metric.key + '_total'] = {};
           _$group[metric.key + '_count'].$sum = 1;
           _$group[metric.key + '_total'].$sum = '$' + metric.attribute;
           }*/
          else
            _$group[ metric.key]['$' + (typeof metric.aggregation === 'undefined' ? 'sum' : metric.aggregation)] = '$' + metric.attribute;
        }
        if (_$unwind) {
          colQuery.query = [
            {$match: _$match},
            {$unwind: _$unwind},
            // {$project: _$project},
            {$group: _$group},
            {$sort: {timestamp: -1}}
          ];
        }
        else {
          colQuery.query = [
            {$match: _$match},
            // {$project: _$project},
            {$group: _$group},
            {$sort: {timestamp: -1}}
          ];
        }

        if ($limit) {
          colQuery.query.push($limit);
        }
      }
      else {
        var _$group2;

        _$group[metric.dependsOn] = {'$addToSet': '$' + metric.dependsOn};
        _$unwind = '$' + metric.dependsOn;
        _$group2 = {'_id': '$_id'};
        _$group2[metric.key] = {'$sum': 1};
        colQuery.query = [
          {$match: _$match},
          // {$project: _$project},
          {$group: _$group},
          {$unwind: _$unwind},
          {$group: _$group2},
          {$sort: {timestamp: -1}}
        ];

        if ($limit) {
          colQuery.query.push($limit);
        }
      }
      plan.colQueries[colQuery.key] = colQuery;
    }
  });

  //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  return setImmediate(function () {
    return callback(null, plan);
  });
};

MongoDBProvider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };
  var self = this;

  return self.buildQueryPlan(query, function (err, queryplan) {
    var result = {
      dimensions: [],
      metrics: [],
      documents: [],
      queryplan: queryplan
    };

    var arrCols = _.toArray(queryplan.colQueries);
    if (arrCols.length === 0) {
      var output = {
        queryplan: queryplan
      };
      output.dimensions = queryplan.dimensions;
      output.metrics = queryplan.metrics;
      output.documents = [];
      return setImmediate(function () {
        return callback(null, output);
      });
    }

    return async.map(arrCols, function iterator(_query, next) {
      var collectionName = context.user.workspace + '_' + _query.collection;

      //console.log(require('util').inspect(_query.query, {depth: null, colors: true}));
      self.db.collection(cleanup(collectionName), function (err, collection) {
        if (err)
          return next(err);
        //console.log(require('util').inspect(_query.query, {depth: null, colors: true}));
        collection.aggregate(_query.query, {}, function (err, results) {
          if (err)
            return next(err);

          result.dimensions = queryplan.dimensions;
          result.metrics = queryplan.metrics;

          //prevent circular references on output.
          result.metrics.forEach(function (m, i) {
            if (!m.formula) {
              if (m.collection)
                m.collection = m.collection.key;
              result.metrics[i] = m;
            }
          });
          result.documents = results;
          return next(null, self.common.extend({}, result));
        });
      });
    }, function (err, results) {
      if (err)
        return setImmediate(function () {
          return callback(err);
        });

      var output = {
        queryplan: queryplan
      };
      var keys = [];
      var final = [];

      if (results && results.length > 0) {
        output.dimensions = results[0].dimensions;
        output.metrics = results[0].metrics;

        results.forEach(function (_result) {
          _result.documents.forEach(function (document) {
            var key = self.common.hash(JSON.stringify(document._id));
            var row;

            if (keys.indexOf(key) == -1) {
              row = {};
              Object.keys(document._id).forEach(function (key) {
                row[key] = document._id[key];
              });
              row.key = key;
              keys.push(key);
              final.push(row);
            }
            else {
              row = _.find(final, function (f) {
                return f.key == key;
              });
            }

            Object.keys(document).forEach(function (attribute) {
              if (attribute != '_id') {
                row[attribute] = document[attribute];
              }
            });
            output.metrics.forEach(function (m) {
              if (!row[m.key])
                row[m.key] = null;
            });
            output.dimensions.forEach(function (d) {
              if (typeof row[d.key] === 'undefined') {
                row[d.key] = '(not set)';
              }
            });

            final[keys.indexOf(key)] = row;
          });
        });
        output.documents = final;
        return setImmediate(function () {
          return callback(null, output);
        });
      }
      else {
        output.dimensions = queryplan.dimensions;
        output.metrics = queryplan.metrics;
        output.documents = [];
        return setImmediate(function () {
          return callback(null, output);
        });
      }
    });
  });
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

MongoDBProvider.prototype.stats = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.db.collection(cleanup(collectionName), function (err, collection) {
    if (err)
      return callback(err);

    return collection.stats(callback);
  });
};

MongoDBProvider.prototype.drop = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.db.dropCollection(cleanup(collectionName), callback);
};

MongoDBProvider.prototype.purge = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.db.dropDatabase(callback);
};

var cleanup = function (string) {
  return string.replace(/[^\w\s]/gi, '');
};