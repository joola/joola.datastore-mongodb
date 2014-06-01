var
  mongodb = require('mongodb');

module.exports = MongoDBProvider;

function MongoDBProvider(options, helpers, callback) {
  if (!(this instanceof MongoDBProvider)) return new MongoDBProvider(options);

  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  return this.init(options, callback);
}

MongoDBProvider.prototype.init = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.find = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.delete = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.update = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.insert = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.query = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.openConnection = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.closeConnection = function (options, callback) {
  return callback(null);
};

MongoDBProvider.prototype.checkConnection = function (options, callback) {
  return callback(null);
};