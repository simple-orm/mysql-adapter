# Simple ORM MySQL Adapter

Mysql (and MariaDB) adapter for [simple orm](https://github.com/simple-orm/core).

# Tests

There are no tests in this repository, tests are handled with the [core](https://github.com/simple-orm/core) repository.

# Feature Support

* Transactions - Supported
* Joins - Supported

# Quick Start

You can install this data adapter with:

```npm install simple-orm-data-adapter```

The data adapter does not assume anything with the creation of the database connection, you are free to create the connection however you want.  To create an instance of the data adapter, you need to pass the require an object with a property called connection.  you could do something like this:

```javascript
//mysql-connection.js
var mysql = require('mysql');
var configuration = require('../configurations/data.json');

module.exports = {
  connection: mysql.createConnection({
    host: configuration.host,
    user: configuration.username,
    password: configuration.password,
    port: configuration.port,
    database: configuration.database
  })
};
```

```javascript
//permission.js
var mysqlAdapter = require('simple-orm-mysql-adapter')(require('./mysql-connection'));

//rest of data object creation ...
```

# TODO:

* Support pools

## LICENSE

MIT
