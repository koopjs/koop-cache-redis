const Redis = require('ioredis')
const config = require('config')
function Cache () {
  this.client = new Redis(config.queue.connection)
}


Cache.prototype.type = 'cache';
Cache.prototype.plugin_name = 'Redis Cache';
Cache.prototype.version = '#!';

Cache.prototype.connect = function () {
  return this
};

Cache.prototype.serviceRegister = function (type, info, callback) {
console.log(type, info)
  this.client.hset(type, info.id, info.host, function (err) {
    callback(err);
  });
};

Cache.prototype.serviceGet = function (type, id, callback) {
console.log(type, id)
  if (!id) return callback(null, [{'foo': 'bar'}])
   this.client.hget(type, id, function (err, host) {
    console.log("this is the host", host)
    callback(err, {id: id, host: host});
  });
};

Cache.prototype.getCount = function (table, options, callback) {
  callback(null, 0);
};

Cache.prototype.updateInfo = function (table, info, callback) {
  this.client.hset('info', table, JSON.stringify(info), function (err) {
    callback(err);
  });
};

Cache.prototype.getInfo = function (table, callback) {
  this.client.hget('info', table, function (err, json) {
    if (err) return callback(err);
    if (!json) return callback(new Error('Resource not found'));
    var info = undefined;
    try {
      info = JSON.parse(json);
    } catch (e) {
      console.log(e, json);
      return callback(new Error('Error parsing JSON'));
    }
    callback(null, info);
  });
};

Cache.prototype.insert = function (id, table, layerId, callback) {
console.log(id, table, layerId, callback)
  if (table.info) {
    var info = table.info;
    info.name = table.name;
    info.status = table.status;
    info.updated_at = table.updated_at;
    info.expires_at = table.expires_at;
    info.retrieved_at = table.retrieved_at;
    info.geomtype = table.geomtype;
    info.host = table.host;
    this.updateInfo(id + ':' + layerId, info, callback);
  } else {
    callback(null);
  }
};

Cache.prototype.insertPartial = function (id, geojson, layer, callback) {
  callback(null);
};

Cache.prototype.addIndexes = function (table, options, callback) {
  callback(null);
};

var cache = new Cache()

module.exports = cache
