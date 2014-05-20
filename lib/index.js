var _ = require('lodash');
var bluebird = require('bluebird');
var squel = require('squel');
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

    getRow: function(query, params) {
      var defer = bluebird.defer();

      this.runQuery(query, params).then(function(results) {
        defer.resolve(results[0]);
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
    },

    insert: function(model) {
      var adapter = this;
      var defer = bluebird.defer();

      var query = squel
      .insert()
      .into(model._table)
      .setFields(model.getInsertSqlValues())
      .toParam();

      adapter.runQuery(query.text, query.values).then((function(results) {
        //set the insert id so the next select with execute properly
        if(model._insertIdColumn && results['insertId'] != 0) {
          model[model._insertIdColumn] = results['insertId'];
        }

        //we want to load the data from the database in order to pull values that are set by the database
        var selectQuery = squel
        .select()
        .from(model._table);

        _.forEach(model._selectColumns, function(value) {
          selectQuery.field(value);
        });

        var primaryKeyData = model.getPrimaryKeyData();

        _.forEach(primaryKeyData, function(value, key) {
          selectQuery.where('%s = ?'.format(key), value)
        });

        selectQuery = selectQuery.toParam();

        adapter.getRow(selectQuery.text, selectQuery.values).then((function(data) {
          model.loadData(data);
          model._status = 'loaded';
          defer.resolve(true);
        }).bind(model), function(error) {
          defer.reject(error);
        });
      }).bind(model), function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    update: function(model) {
      var adapter = this;
      var defer = bluebird.defer();

      var query = squel
      .update()
      .table(model._table)
      .setFields(model.getUpdateSqlValues());

      var primaryKeyData = model.getPrimaryKeyData();

      _.forEach(primaryKeyData, function(value, key) {
        query.where('%s = ?'.format(key), value)
      });

      query = query.toParam();

      adapter.runQuery(query.text, query.values).then((function(results) {
        //we want to load the data from the database in order to pull values that are set by the database
        var selectQuery = squel
        .select()
        .from(model._table);

        _.forEach(model._selectColumns, function(value) {
          selectQuery.field(value);
        });

        var primaryKeyData = model.getPrimaryKeyData();

        _.forEach(primaryKeyData, function(value, key) {
          selectQuery.where('%s = ?'.format(key), value)
        });

        selectQuery = selectQuery.toParam();

        adapter.getRow(selectQuery.text, selectQuery.values).then((function(data) {
          model.loadData(data);
          model._status = 'loaded';
          defer.resolve(true);
        }).bind(model), function(error) {
          defer.reject(error);
        });
      }).bind(model), function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    remove: function(model) {
      var adapter = this;
      var defer = bluebird.defer();

      var primaryKeyData = model.getPrimaryKeyData();
      var query = squel
      .delete()
      .from(model._table);

      _.forEach(primaryKeyData, function(value, key) {
        query.where('%s = ?'.format(key), value)
      });

      query = query.toParam();

      adapter.runQuery(query.text, query.values).then(function(results) {
        defer.resolve(true);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    find: function(model, data, createModelInstance) {
      var adapter = this;
      var defer = bluebird.defer();
      var query = this._buildSelectQuery(model, data);

      adapter.getRow(query.text, query.values).then(function(results) {
        var returnObject;

        if(!results) {
          returnObject = null
        } else {
          returnObject = createModelInstance(results);
          returnObject._status = 'loaded';
        }

        defer.resolve(returnObject);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    findAll: function(model, data, createModelInstance) {
      var adapter = this;
      var defer = bluebird.defer();
      var query = this._buildSelectQuery(model, data);

      adapter.getAll(query.text, query.values).then(function(results) {
        var collection = [];

        if(results) {
          _.forEach(results, function(row) {
            var newObject = createModelInstance(row);
            newObject._status = 'loaded';
            collection.push(newObject);
          });
        }

        defer.resolve(collection);
      }, function(error) {
        defer.reject(error);
      });

      return defer.promise;
    },

    _buildSelectQuery: function(model, data) {
      var query = squel
      .select()
      .from(model._table);

      _.forEach(model._selectColumns, function(value) {
        query.field(value);
      });

      _.forEach(data, function(value, key) {
        query.where('%s = ?'.format(key), value);
      });

      return query.toParam();
    }
  };
};