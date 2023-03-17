const test = require('tape');
const Cache = require('.');
const cache = new Cache({host: 'redis://localhost:6379'});
const _ = require('lodash');
const geojson = {
  type: 'FeatureCollection',
  metadata: {
    name: 'Test',
    description: 'Test'
  },
  features: [
    {
      type: 'Feature',
      properties: {
        key: 'value'
      },
      geometry: {
        foo: 'bar'
      }
    }
  ]
};

test('flush redis', async t => {
  await cache.client.flushAll();

  t.pass('flushed cache');
  t.end();
});

test('Inserting and retrieving from the cache', t => {
  cache.insert('key', geojson, {ttl: 600}, e => {
    t.error(e, 'no error in callback');
    cache.retrieve('key', (e, cached) => {
      t.error(e, 'no error in callback');
      t.equal(cached.features[0].properties.key, 'value', 'retrieved features');
      t.equal(cached.metadata.name, 'Test', 'retrieved metadata');
      t.ok(cached.metadata.expires, 'expiration set');
      t.ok(cached.metadata.updated, 'updated set');
      t.end();
    });
  });
});

test('Inserting and retrieving from the cache using upsert when the cache is empty', t => {
  cache.upsert('keyupsert', geojson, {ttl: 600}, e => {
    t.error(e, 'no error in callback');
    cache.retrieve('keyupsert', (e, cached) => {
      t.error(e, 'no error in callback');
      t.equal(cached.features[0].properties.key, 'value', 'retrieved features');
      t.equal(cached.metadata.name, 'Test', 'retrieved metadata');
      t.ok(cached.metadata.expires, 'expiration set');
      t.ok(cached.metadata.updated, 'updated set');
      t.end();
    });
  });
});

test('Inserting and retrieving from the cache using upsert when the cache is filled', t => {
  cache.insert('keyupsertupdate', geojson, {ttl: 600}, e => {
    t.error(e, 'no error in callback');
    const geojson2 = _.cloneDeep(geojson);
    geojson2.features[0].properties['key'] = 'updated';
    cache.upsert('keyupsertupdate', geojson2, {ttl: 600}, e => {
      t.error(e, 'no error in callback');
      cache.retrieve('keyupsertupdate', (e, cached) => {
        t.error(e, 'no error in callback');
        t.equal(cached.features[0].properties.key, 'updated', 'retrieved features');
        t.equal(cached.metadata.name, 'Test', 'retrieved metadata');
        t.ok(cached.metadata.expires, 'expiration set');
        t.ok(cached.metadata.updated, 'updated set');
        t.end();
      });
    });
  });
});

test('Inserting and appending to the cache', t => {
  cache.insert('key2', geojson, {ttl: 600}, e => {
    t.error(e, 'no error in callback');
    cache.append('key2', geojson, e => {
      t.error(e, 'no error in callback');
      cache.retrieve('key2', (e, cached) => {
        t.error(e, 'no error in callback');
        t.equal(cached.features.length, 2, 'retrieved all features');
        t.equal(cached.metadata.name, 'Test', 'retrieved metadata');
        t.ok(cached.metadata.expires, 'expiration set');
        t.ok(cached.metadata.updated, 'updated set');
        t.end();
      });
    });
  });
});

test('Updating an existing entry in the cache', t => {
  cache.insert('key3', geojson, {ttl: 600}, e => {
    t.error(e, 'no error in callback');
    const geojson2 = _.cloneDeep(geojson);
    geojson2.features[0].properties.key = 'test2';
    cache.update('key3', geojson2, {ttl: 1000}, e => {
      t.error(e, 'no error in callback');
      cache.retrieve('key3', (e, cached) => {
        t.equal(cached.features[0].properties.key, 'test2', 'retrieved only new features');
        t.equal(cached.features.length, 1, 'retrieved only new features');
        t.equal(cached.metadata.name, 'Test', 'retrieved original metadata');
        t.ok(cached.metadata.expires, 'expiration set');
        t.ok(cached.metadata.updated, 'updated set');
        t.end();
      });
    });
  });
});

test('Inserting and deleting from the cache', t => {
  cache.insert('key4', geojson, e => {
    t.error(e, 'no error in callback');
    cache.delete('key4', e => {
      t.error(e, 'no error in callback');
      cache.retrieve('key4', {}, (err) => {
        t.ok(err, 'Should return an error');
        t.equal(err.message, 'Resource not found', 'Error should have correct message');
        t.end();
      });
    });
  });
});

test('Trying to call insert when something is already in the cache', t => {
  cache.insert('key5', geojson, {}, e => {
    t.error(e, 'no error in callback');
    cache.insert('key5', geojson, {}, err => {
      t.ok(err, 'Should return an error');
      t.equal(err.message, 'Cache key is already in use', 'Error should have correct message');
      t.end();
    });
  });
});

test('Trying to delete the catalog entry when something is still in the cache', t => {
  cache.insert('key6', geojson, e => {
    t.error(e, 'no error in callback');
    cache.catalogDelete('key6', err => {
      t.ok(err, 'Should return an error');
      t.equal(err.message, 'Cannot delete catalog entry while data is still in cache', 'Error should have correct message');
      t.end();
    });
  });
});

test('Ensure multiple entries are hashed properly', t => {
  cache.insert('key7', geojson, {}, e => {
    t.error(e, 'no error in callback');
    const geojson2 = _.cloneDeep(geojson);
    geojson2.features[0].properties.key = 'test2';
    geojson2.metadata.name = 'Test2';
    cache.insert('key8', geojson2, {}, e => {
      t.error(e, 'no error in callback');
      cache.retrieve('key8', (e, cached) => {
        t.error(e, 'no error in callback');
        t.equals(cached.metadata.name, 'Test2');
        t.equal(cached.features[0].properties.key, 'test2', 'retrieved only new features');
        cache.retrieve('key7', (e, cached) => {
          t.error(e, 'no error in callback');
          t.equals(cached.metadata.name, 'Test');
          t.equal(cached.features[0].properties.key, 'value', 'retrieved only new features');
          t.end();
        });
      });
    });
  });
});

test('teardown', t => {
  cache.disconnect();
  t.pass('disconnected');
  t.end();
});
