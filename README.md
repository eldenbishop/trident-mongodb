This library implements a Trident state on top of MongoDB. It supports non-transactional, transactional, and opaque state types.

Pretty much a quick and dirty cut-n-paste from the trident-memcached state code.

TODO: Add an option to use reliable writes in the Mongo client. Currently default writes are not durable.