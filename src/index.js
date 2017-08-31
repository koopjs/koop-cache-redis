const redis = require('redis')
const Util = require('util')
const EventEmitter = require('events')
const _ = require('lodash')
const FEATURES = 'features'
const METADATA = 'metadata'
const Logger = require('koop-logger')
const log = new Logger()
const config = require('config')

function Cache (options = {}) {
  const host = options.host || config.cache.redis.host
  this.client = this.catalog.client = redis.createClient(host)
  this.client.on('error', e => log.error(e))

  process.on('SIGTERM', () => this.disconnect())
  process.on('SIGINT', () => this.disconnect())

  this.catalog.set = this.set
  this.catalog.get = this.get
}

Cache.prototype.disconnect = function () {
  this.client.quit()
}

Cache.prototype.set = function (type, key, value, callback) {
  this.client.hset(type, key, JSON.stringify(value), callback)
}

Cache.prototype.get = function (type, key, callback) {
  this.client.hget(type, key, (e, string) => {
    callback(e, JSON.parse(string))
  })
}

Cache.name = 'Redis Cache'
Cache.type = 'cache'
Cache.version = require('../package.json').version

Util.inherits(Cache, EventEmitter)

Cache.prototype.insert = function (key, geojson, options = {}, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  // support a feature collection or an array of features
  this.client.hexists(FEATURES, key, (e, exists) => {
    if (e) return callback(e)
    else if (exists) return callback(new Error('Cache key is already in use'))
    const features = geojson.features ? geojson.features : geojson
    this.set(FEATURES, key, features, e => {
      if (e) return callback(e)
      const metadata = geojson.metadata || {}
      if (options.ttl) metadata.expires = Date.now() + (options.ttl * 1000)
      metadata.updated = Date.now()
      this.set(METADATA, key, metadata, callback)
    })
  })
}

Cache.prototype.upsert = function (key, geojson, options = {}, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  this.client.hexists(FEATURES, key, (e, exists) => {
    if (e) {
      return callback(e)
    } else if (exists) {
      this.update(key, geojson, options, callback)
    } else {
      this.insert(key, geojson, options, callback)
    }
  })
}

Cache.prototype.update = function (key, geojson, options = {}, callback) {
  if (!callback) {
    callback = options
    options = {}
  }
  // support a feature collection or an array of features
  this.client.hexists(FEATURES, key, (e, exists) => {
    if (e) return callback(e)
    else if (!exists) return callback(new Error('Resource not found'))
    const features = geojson.features ? geojson.features : geojson
    this.set(FEATURES, key, features, e => {
      if (e) return callback(e)
      this.catalog.retrieve(key, (e, existingMetadata) => {
        if (e) return callback(e)
        const metadata = geojson.metadata || existingMetadata
        if (options.ttl) metadata.expires = Date.now() + (options.ttl * 1000)
        this.set(METADATA, key, metadata, callback)
      })
    })
  })
}

Cache.prototype.append = function (key, geojson, options = {}, callback) {
  // TODO use lists instead of hash keys
  if (!callback) {
    callback = options
    options = {}
  }
  const features = geojson.features ? geojson.features : geojson
  this.get(FEATURES, key, (err, existing) => {
    if (err) return callback(err)
    this.set(FEATURES, key, features.concat(existing), e => {
      if (e) return callback(e)
      this.catalog.update(key, { updated: Date.now() }, callback)
    })
  })
}

Cache.prototype.retrieve = function (key, options, callback) {
  // TODO use promise.all
  if (!callback) {
    callback = options
    options = {}
  }
  this.get(FEATURES, key, (e, features) => {
    if (e || !features) return callback(new Error('Resource not found'))
    this.get(METADATA, key, (e, metadata) => {
      if (e) return callback(e)
      const geojson = { type: 'FeatureCollection', metadata, features }
      callback(null, geojson)
    })
  })
}

Cache.prototype.createStream = function (key, options = {}) {
  throw new Error('Streaming not yet supported')
}

Cache.prototype.delete = function (key, callback) {
  // TODO use HEXISTS
  this.client.hdel(FEATURES, key, e => {
    if (e) return callback(new Error('Resource not found'))
    this.get(METADATA, key, (e, metadata) => {
      if (e) return callback(e)
      metadata.status = 'deleted'
      metadata.updated = Date.now()
      this.catalog.update(key, metadata, callback)
    })
  })
}

Cache.prototype.catalog = {}

Cache.prototype.catalog.insert = function (key, metadata, callback) {
  this.client.hexists(METADATA, key, (e, exists) => {
    if (e) return callback(e)
    else if (exists) return callback(new Error('Catalog key is already in use'))
    metadata.updated = Date.now()
    this.set(METADATA, key, metadata, callback)
  })
}

Cache.prototype.catalog.update = function (key, update, callback) {
  this.get(METADATA, key, (e, existing) => {
    if (e) return callback(e)
    else if (!existing) return callback(new Error('Resource not found'))
    const metadata = _.merge(existing, update)
    metadata.updated = Date.now()
    this.set(METADATA, key, metadata, callback)
  })
}

Cache.prototype.catalog.retrieve = function (key, callback) {
  this.get(METADATA, key, (e, metadata) => {
    if (e) return callback(e)
    else if (!metadata) return callback(new Error('Resource not found'))
    callback(null, metadata)
  })
}

Cache.prototype.catalog.delete = function (key, callback) {
  this.client.hexists(FEATURES, key, (e, exists) => {
    if (e) return callback(e)
    else if (exists) return callback(new Error('Cannot delete catalog entry while data is still in cache'))
    this.client.hdel(METADATA, key, e => {
      callback(e)
    })
  })
}

module.exports = Cache
