const redis = require('redis');
const EventEmitter = require('events');
const _ = require('lodash');
const FEATURES = 'koop-cache-redis::features';
const METADATA = 'koop-cache-redis::metadata';
const Logger = require('@koopjs/logger');
const log = new Logger();
const config = require('config');

// Convenience to make callbacks optional in most functions
function noop () {}

class Cache extends EventEmitter {
  static pluginName = 'Redis Cache';
  static type = 'cache';
  static version = require('../package.json').version;

  constructor (options = {}) {
    super();
    const host = options.host || config.cache.redis.host;
    this.client = redis.createClient(host);
    this.client.on('error', e => log.error(e));

    process.on('SIGTERM', () => this.disconnect());
    process.on('SIGINT', () => this.disconnect());

    this.client.connect();
  }

  insert(key, geojson, options = {}, callback = noop) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
 
    // support a feature collection or an array of features
    this.hashExists(key, FEATURES, (e, exists) => {
      if (e) return callback(e);
      else if (exists) return callback(new Error('Cache key is already in use'));
      const features = geojson.features ? geojson.features : geojson;
      this.set(FEATURES, key, features, e => {
        if (e) return callback(e);
        const metadata = geojson.metadata || {};
        if (options.ttl) metadata.expires = Date.now() + (options.ttl * 1000);
        metadata.updated = Date.now();
        this.set(METADATA, key, metadata, callback);
      });
    });
  }

  update(key, geojson, options = {}, callback = noop) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    // support a feature collection or an array of features
    this.hashExists(key, FEATURES, (e, exists) => {
      if (e) return callback(e);
      else if (!exists) return callback(new Error('Resource not found'));
      const features = geojson.features ? geojson.features : geojson;
      this.set(FEATURES, key, features, e => {
        if (e) return callback(e);
        this.catalogRetrieve(key, (e, existingMetadata) => {
          if (e) return callback(e);
          const metadata = geojson.metadata || existingMetadata;
          if (options.ttl) metadata.expires = Date.now() + (options.ttl * 1000);
          this.set(METADATA, key, metadata, callback);
        });
      });
    });
  }

  upsert(key, geojson, options = {}, callback = noop) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    this.hashExists(key, FEATURES, (e, exists) => {
      if (e) {
        return callback(e);
      } else if (exists) {
        this.update(key, geojson, options, callback);
      } else {
        this.insert(key, geojson, options, callback);
      }
    });
  }

  append(key, geojson, options = {}, callback = noop) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    const features = geojson.features ? geojson.features : geojson;
    this.get(FEATURES, key, (err, existing) => {
      if (err) return callback(err);
      this.set(FEATURES, key, features.concat(existing), e => {
        if (e) return callback(e);
        this.catalogUpdate(key, { updated: Date.now() }, callback);
      });
    });
  }

  retrieve(key, options, callback = noop) {
    if (typeof options === 'function') {
      callback = options;
      options = {};
    }
    this.get(FEATURES, key, (e, features) => {
      if (e || !features) return callback(new Error('Resource not found'));
      this.get(METADATA, key, (e, metadata) => {
        if (e) return callback(e);
        const geojson = { type: 'FeatureCollection', metadata, features };
        callback(null, geojson);
      });
    });
  }

  // eslint-disable-next-line no-unused-vars
  createStream(key, options = {}) {
    throw new Error('Streaming not yet supported');
  }

  delete(key, callback) {
    this.hashExists(key, FEATURES, (e, exists) => {
      if (e) return callback(e);
      else if (!exists) return callback(new Error('Resource not found'));
      this.hashDelete(key, FEATURES, e => {
        if (e) return callback(e);
        this.get(METADATA, key, (e, metadata) => {
          if (e) return callback(e);
          metadata = metadata || {};
          metadata.status = 'deleted';
          metadata.updated = Date.now();
          this.catalogUpdate(key, metadata, callback);
        });
      });
    });
  }

  catalogInsert(key, metadata, callback) {
    this.hashExists(key, METADATA, (e, exists) => {
      if (e) return callback(e);
      else if (exists) return callback(new Error('Catalog key is already in use'));
      metadata.updated = Date.now();
      this.set(METADATA, key, metadata, callback);
    });
  }

  catalogUpdate(key, update, callback) {
    this.get(METADATA, key, (e, existing) => {
      if (e) return callback(e);
      else if (!existing) return callback(new Error('Resource not found'));
      const metadata = _.merge(existing, update);
      metadata.updated = Date.now();
      this.set(METADATA, key, metadata, callback);
    });
  }

  catalogRetrieve(key, callback) {
    this.get(METADATA, key, (e, metadata) => {
      if (e) return callback(e);
      else if (!metadata) return callback(new Error('Resource not found'));
      callback(null, metadata);
    });
  }
  
  catalogDelete(key, callback) {
    this.hashExists(key, FEATURES, (e, exists) => {
      if (e) return callback(e);
      else if (exists) return callback(new Error('Cannot delete catalog entry while data is still in cache'));
      this.hashDelete(key, METADATA, e => {
        callback(e);
      });
    });
  }

  
  set(field, key, value, callback) {
    this.client.hSet(key, [field, JSON.stringify(value)])
      .then(r => callback(null, r))
      .catch(callback);
  }

  get(field, key, callback) {
    this.client.hGet(key, field)
      .then(result => callback(null, JSON.parse(result)))
      .catch(callback); 
  }
  
  hashDelete(key, field, callback) {
    this.client.hDel(key, field)
      // eslint-disable-next-line no-unused-vars
      .then(r => callback())
      .catch(callback);
  }

  hashExists(key, field, callback) {
    this.client.hExists(key, field)
      .then(exists => callback(null, exists))
      .catch(callback);
  }

  disconnect() {
    this.client.quit();
  }

}




module.exports = Cache;
