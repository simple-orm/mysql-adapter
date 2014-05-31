var _ = require('lodash');
var bluebird = require('bluebird');
var squel = require('squel');
var debug = false;
var noValuedComparisons = [
  'is null',
  'is not null'
];
var betweenComparisons = [
  'between',
  'not between'
];

var objectToArray = function(data) {
  var returnData = [];

  _.forEach(data, function(value, key) {
    returnData.push(key);
    returnData.push(value);
  });

  return returnData;
};

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

    find: function(model, criteria, createModelInstance) {
      var adapter = this;
      var defer = bluebird.defer();
      var query = this._buildSelectQuery(model, criteria);

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

    findAll: function(model, criteria, createModelInstance) {
      var adapter = this;
      var defer = bluebird.defer();
      var query = this._buildSelectQuery(model, criteria);

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

    enableDebug: function() {
      debug = true;
    },

    disableDebug: function() {
      debug = false;
    },

    _buildSelectQuery: function(model, criteria) {
      var query = squel
      .select()
      .from(model._table);

      _.forEach(model._selectColumns, function(value) {
        query.field("%s.%s".format(model._table, value));
      });

      this._appendWhere(query, criteria.where);
      this._appendJoin(query, criteria.join);

      //ensure that one 1 record of the primary models table is pulled
      _.forEach(model._primaryKeys, function(value) {
        query.group("%s.%s".format(model._table, value));
      });

      return query.toParam();
    },

    _appendWhere: function(query, data) {
      if(data) {
        _.forEach(data, function(value, key) {
          if(_.isObject(value)) {
            var sqlValue = value.value;
            var comparison = value.comparison.toLowerCase();
          } else {
            var sqlValue = value;
            var comparison = '=';
          }

          var sqlValue = value.valueType !== 'field' && _.isString(value.value) ? "'%s'".format(sqlValue) : sqlValue;

          if(noValuedComparisons.indexOf(comparison) !== -1) {
            query.where('%s %s'.format(key, comparison));
          } else if(betweenComparisons.indexOf(comparison) !== -1) {
            query.where('%s %s ? AND ?'.format(key, comparison), sqlValue[0], sqlValue[1]);
          } else {
            query.where('%s %s ?'.format(key, comparison), sqlValue);
          }
        });
      }
    },

    _appendJoin: function(query, data) {
      if(data) {
        _.forEach(data, function(value) {

          var table = value.repository._model._table;
          var on = '';

          _.forEach(value.on, function(value, key) {
            if(on != '') {
              on += ' AND ';
            }

            if(_.isObject(value)) {
              var sqlValue = value.value;

              if(value.comparison) {
                var comparison = value.comparison.toLowerCase();
              } else {
                var comparison = '=';
              }
            } else {
              var sqlValue = value;
              var comparison = '=';
            }

            //we need to make sure string are property escape of the on clause of the join
            var sqlValue = value.valueType !== 'field' && _.isString(value.value) ? connectionObject.connection.escape(sqlValue) : sqlValue;

            if(_.isArray(sqlValue)) {
              sqlValues.forEach(function(value, key) {
                if(_.isString(value)) {
                  sqlValue[key] = connectionObject.connection.escape(value);
                }
              });
            }

            if(noValuedComparisons.indexOf(comparison) !== -1) {
              on += '%s %s'.format(key, comparison);
            } else if(betweenComparisons.indexOf(comparison) !== -1) {
              on += ('%s %s ' + sqlValue[0] + ' AND ' + sqlValue[1]).format(key, comparison);
            } else {
              on += ('%s %s ' + sqlValue).format(key, comparison);
            }
          });

          query.join(table, null, on);
        }, this);
      }
    }
  };
};