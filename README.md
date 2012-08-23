## trident-mongodb

This library implements a Trident state on top of MongoDB. It supports non-transactional, transactional, and opaque state types.

Pretty much a quick and dirty cut-n-paste from the trident-memcached state code.

### Features

* Multi-column key support
* Jackson serialization
* Native mongo types for numbers, dates and strings (allowing indexing and queries into the stored data)

### TODO

* Add an option to use reliable writes in the Mongo client. Currently default writes are not durable.
* Unroll embedded JSON into a mongo document to allow querying on complex types