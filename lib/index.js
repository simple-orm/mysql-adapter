require('string-format');
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

module.exports = {
  create: function() {
    var connection;

    return {
      _dataConverters: {
        boolean: function(value) {
          return value === true ? 1 : 0;
        },
        date: function(value) {
          if(!value) {
            return null;
          }

          return value.format('YYYY-MM-DD');
        },
        datetime: function(value) {
          if(!value) {
            return null;
          }

          return value.format('YYYY-MM-DD HH:mm:ss');
        }
      },

      setConnection: function(newConnection) {
        connection = newConnection;
      },

      releaseConnection: function() {
        if(connection) {
          //pool connections are released with the release() method and regular ones are with the end() method
          if(_.isFunction(connection.release)) {
            connection.release();
          } else {
            connection.end();
          }
        }
      },

      _escapeValue: function(value) {
        return connection.escape(value);
      },

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
        var sql = connection.query(query, params, function(error, results) {
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
        .into("{0}.{1}".format(model._database, model._table))
        .setFields(model._getInsertDataStoreValues(this._dataConverters))
        .toParam();

        adapter.runQuery(query.text, query.values).then((function(results) {
          //set the insert id so the next select with execute properly
          if(model._insertIdProperty && results['insertId'] != 0) {
            model[model._insertIdProperty] = results['insertId'];
          }

          //we want to load the data from the database in order to pull values that are set by the database
          var selectQuery = squel
          .select()
          .from("{0}.{1}".format(model._database, model._table));

          _.forEach(model._selectColumns, function(value, key) {
            selectQuery.field(key);
          });

          var primaryKeyData = model._getDataStorePrimaryKeyData();

          _.forEach(primaryKeyData, function(value, key) {
            selectQuery.where('{0} = ?'.format(key), value)
          });

          selectQuery = selectQuery.toParam();

          adapter.getRow(selectQuery.text, selectQuery.values).then((function(data) {
            model.loadData(data, 'loaded');
            defer.resolve(true);
          }).bind(model), function(error) {
            defer.reject(error);
          });
        }).bind(model), function(error) {
          defer.reject(error);
        });

        return defer.promise;
      },

      bulkInsert: function(models) {
        var adapter = this;
        var defer = bluebird.defer();

        if(_.isFunction(models.toArray)) {
          models = models.toArray();
        } else if(!_.isArray(models)) {
          defer.reject('Bulk insert can only work on a collection or array of models');
          return defer.promise;
        }

        if(models.lenght === 0) {
          defer.reject('You must pass at least 1 model to insert');
          return defer.promise;
        }

        var fieldRows = [];

        models.forEach(function(model) {
          fieldRows.push(model._getInsertDataStoreValues(this._dataConverters));
        })

        var query = squel
        .insert()
        .into("{0}.{1}".format(models[0]._database, models[0]._table))
        .setFieldsRows(fieldRows)
        .toParam();

        adapter.runQuery(query.text, query.values).then(function(results) {
          defer.resolve(true);
        }, function(error) {
          defer.reject(error);
        });

        return defer.promise;
      },

      update: function(model) {
        var adapter = this;
        var defer = bluebird.defer();

        var query = squel
        .update()
        .table("{0}.{1}".format(model._database, model._table))
        .setFields(model._getUpdateDataStoreValues(this._dataConverters));

        var primaryKeyData = model._getDataStorePrimaryKeyData();

        _.forEach(primaryKeyData, function(value, key) {
          query.where('{0} = ?'.format(key), value)
        });

        query = query.toParam();

        adapter.runQuery(query.text, query.values).then((function(results) {
          //we want to load the data from the database in order to pull values that are set by the database
          var selectQuery = squel
          .select()
          .from("{0}.{1}".format(model._database, model._table));

          _.forEach(model._selectColumns, function(value, key) {
            selectQuery.field(key);
          });

          var primaryKeyData = model._getDataStorePrimaryKeyData();

          _.forEach(primaryKeyData, function(value, key) {
            selectQuery.where('{0} = ?'.format(key), value)
          });

          selectQuery = selectQuery.toParam();

          adapter.getRow(selectQuery.text, selectQuery.values).then((function(data) {
            model.loadData(data, 'loaded');
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

        var primaryKeyData = model._getDataStorePrimaryKeyData();
        var query = squel
        .delete()
        .from("{0}.{1}".format(model._database, model._table));

        _.forEach(primaryKeyData, function(value, key) {
          query.where('{0} = ?'.format(key), value)
        });

        query = query.toParam();

        adapter.runQuery(query.text, query.values).then(function(results) {
          defer.resolve(true);
        }, function(error) {
          defer.reject(error);
        });

        return defer.promise;
      },

      bulkRemove: function(models) {
        var adapter = this;
        var defer = bluebird.defer();

        //to properly work with collection
        if(_.isFunction(models.toArray)) {
          models = models.toArray();
        } else if(!_.isArray(models)) {
          defer.reject('Bulk remove can only work on a collection or array of models');
          return defer.promise;
        }

        if(models.lenght === 0) {
          defer.reject('You must passs at least 1 model to remove');
          return defer.promise;
        }

        var queryParameters = [];
        var query = squel
        .delete()
        .from("{0}.{1}".format(models[0]._database, models[0]._table));
        var whereExpression = squel.expr();

        models.forEach(function(model, key) {
          var primaryKeyData = model._getDataStorePrimaryKeyData();

          whereExpression.or_begin();

          _.forEach(primaryKeyData, function(value, key) {
            whereExpression.and('{0} = ?'.format(key), value);
            queryParameters.push(value);
          });

          whereExpression.end();
        });

        query.where(whereExpression);

        query = query.toParam();

        //TODO: investigate: can find a where to do parameters with where expression
        query.values = queryParameters;

        adapter.runQuery(query.text, query.values).then(function(results) {
          defer.resolve(true);
        }, function(error) {
          defer.reject(error);
        });

        return defer.promise;
      },

      find: function(model, criteria) {
        var adapter = this;
        var defer = bluebird.defer();
        var query = this._buildSelectQuery(model, criteria);

        adapter.getRow(query.text, query.values).then(function(results) {
          defer.resolve(results);
        }, function(error) {
          defer.reject(error);
        });

        return defer.promise;
      },

      findAll: function(model, criteria) {
        var adapter = this;
        var defer = bluebird.defer();
        var query = this._buildSelectQuery(model, criteria);

        adapter.getAll(query.text, query.values).then(function(results) {
          defer.resolve(results);
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
        .from("{0}.{1}".format(model._database, model._table));

        _.forEach(model._selectColumns, function(value, key) {
          query.field("{0}.{1}".format(model._table, key));
        });

        this._appendJoin(query, criteria.join);
        this._appendWhere(query, criteria.where, model._table);

        //ensure that one 1 record of the primary models table is pulled
        _.forEach(model._primaryKeyColumns, function(value) {
          query.group("{0}.{1}".format(model._table, value));
        });

        return query.toParam();
      },

      _appendWhere: function(query, data, tableName) {
        if(data) {
          _.forEach(data, function(value, key) {
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

            if(noValuedComparisons.indexOf(comparison) !== -1) {
              query.where('{0}.{1} {2}'.format(tableName, key, comparison));
            } else if(betweenComparisons.indexOf(comparison) !== -1) {
              query.where('{0}.{1} {2} ? AND ?'.format(tableName, key, comparison), sqlValue[0], sqlValue[1]);
            } else if (value.valueType === 'field') {
              query.where('{0}.{1} {2} {3}'.format(tableName, key, comparison, sqlValue));
            } else {
              query.where('{0}.{1} {2} ?'.format(tableName, key, comparison), sqlValue);
            }
          });
        }
      },

      _appendJoin: function(query, data) {
        if(data) {
          _.forEach(data, function(value) {

            var table = "{0}.{1}".format(value.repository._model._database, value.repository._model._table);
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
              var sqlValue = value.valueType !== 'field' && _.isString(value.value) ? this._escapeValue(sqlValue) : sqlValue;

              if(_.isArray(sqlValue)) {
                sqlValues.forEach(function(value, key) {
                  if(_.isString(value)) {
                    sqlValue[key] = this._escapeValue(value);
                  }
                });
              }

              if(noValuedComparisons.indexOf(comparison) !== -1) {
                on += '{0} {1}'.format(key, comparison);
              } else if(betweenComparisons.indexOf(comparison) !== -1) {
                var sqlValueOne = this._escapeValue(sqlValue[0]);
                var sqlValueTwo = this._escapeValue(sqlValue[1]);

                on += ('{0} {1} ' + sqlValueOne + ' AND ' + sqlValueTwo).format(key, comparison);
              } else {
                on += ('{0} {1} ' + sqlValue).format(key, comparison);
              }
            }, this);

            query.join(table, null, on);
          }, this);
        }
      }
    };
  }
};
