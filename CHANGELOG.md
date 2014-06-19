# master
- now able to create multiple instances (transaction support)
- connection object is now set and released through `setConnection()`/`releaseConnection()` methods which supports regular and pooled connections (transaction support)
- no longer converts data to model/collection (core library does that now)

# 0.5.0
- updates based on core library changes
- converted `throw`'s to `defer.reject()`'s
- added `bulkInsert()` method
- added `bulkRemove()` method

# 0.4.0
- updates based on core code changes

# 0.3.0
- initial change log
