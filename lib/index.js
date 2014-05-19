var _ = require('lodash');
var bluebird = require('bluebird');
var debug = false;

module.exports = function(connectionObject) {
  return {
    startTransaction: function() {
      var defer = bluebird.defer();

      this.runQuery("START TRANSACTION").then(function(results) {
        defer.resolve(results);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    commitTransaction: function() {
      var defer = bluebird.defer();

      this.runQuery("COMMIT").then(function(results) {
        defer.resolve(results);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    rollbackTransaction: function() {
      var defer = bluebird.defer();

      this.runQuery("ROLLBACK").then(function(results) {
        defer.resolve(results);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    getOne: function(query, params) {
      var defer = bluebird.defer();

      this.runQuery(query, params).then(function(results) {
        defer.resolve(results[0][Object.keys(results[0])[0]]);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    getRow: function(query, params) {
      var defer = bluebird.defer();

      this.runQuery(query, params).then(function(results) {
        defer.resolve(results[0]);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    //must only select one property in query for guaranteed results
    getColumn: function(query, params) {
      var defer = bluebird.defer();

      this.runQuery(query, params).then(function(results) {
        defer.resolve(_.pluck(results, Object.keys(results[0])[0]));
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    getAll: function(query, params) {
      var defer = bluebird.defer();

      this.runQuery(query, params).then(function(results) {
        defer.resolve(results);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    runQuery: function(query, params) {
      var defer = bluebird.defer();

      //TODO: what about supporting pools or different mysql configurations
      var sql = connectionObject.connection.query(query, params, function(error, results) {
        if(error) {
          defer.reject(error);
        }

        defer.resolve(results);
      });

      if(debug === true) {
        console.log('QUERY: ' + sql.sql);
      }

      return defer.promise;
    }
  };
};