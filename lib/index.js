var
  mongodb = require('mongodb');

module.exports = MongoDBProvider;

function MongoDBProvider(options, helpers, callback) {
  if (!(this instanceof MongoDBProvider)) return new MongoDBProvider(options);

  this.name = 'MongoDB';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;



  return this.init(options, callback);
}

MongoDBProvider.prototype.init = function (options, callback) {
  var self = this;

  self.logger.info('Initializing connection to provider [' + self.name + '].');

  return callback(null, self);
};

MongoDBProvider.prototype.destroy = function (callback) {
  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  return callback(null);
};

MongoDBProvider.prototype.find = function (options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.delete = function (options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.update = function (options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.insert = function (documents, options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.query = function (options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.openConnection = function (options, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.closeConnection = function (connection, callback) {
  var self = this;

  return callback(null);
};

MongoDBProvider.prototype.checkConnection = function (connection, callback) {
  var self = this;

  return callback(null);
};