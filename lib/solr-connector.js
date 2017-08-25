var assert = require('assert'),
     debug = require('debug')('loopback-connector-solr:solr-connector'),
      solr = require('solr-client'),
         _ = require('underscore');

var client;

function Solr() {}

Solr.search = function (query, callback) {
  assert.ok(callback, 'callback required');

  debug('performing search for query:', query);

  var solrQuery = client.createQuery();

  _.keys(query).forEach(function (k) {
    if (k === 'fq') return;
    if (typeof solrQuery[k] === 'function') {
      if (_.isArray(query[k])) {
        query[k].forEach(function (p) {
          debug('applying %s() with %s', k, query[k][p]);
          solrQuery[k](query[k][p]);
        });
      } else {
        debug('applying %s() with %s', k, query[k]);
        solrQuery[k](query[k]);
      }
    }
  });

  if (query.fq) {
    _.keys(query.fq).forEach(function (k) {
      if (_.isArray(query.fq[k])) {
        query.fq[k].forEach(function (p) {
          debug('applying matchFilter() with %s, %s', k, p);
          solrQuery.matchFilter(k, p);
        });
      } else {
        debug('applying matchFilter() with %s, %s', k, query.fq[k]);
        solrQuery.matchFilter(k, query.fq[k]);
      }
    });
  }

  client.search(solrQuery, callback);
};

Solr.add = function (documents, callback) {
  assert.ok(documents, 'nothing to add');

  client.add(documents, callback);
};

Solr.commit = function (callback) {
  client.commit(callback);
};

Solr.remove = function (id, callback) {
  assert.ok(id, 'id required');

  client.deleteByID(id, callback);
};

function SolrConnector(options) {
  if (!this instanceof SolrConnector)
    return new SolrConnector(options);

  options = options || {};

  client = solr.createClient(options.solr);
  client.autoCommit = true;
}

exports.SolrConnector = SolrConnector;

exports.initialize = function (dataSource, callback) {
  var settings = dataSource.settings || {};

  var connector = new SolrConnector(settings);

  Solr.connector = connector;

  connector.DataAccessObject = Solr;

  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;

  for (var m in Solr.prototype) {
      var method = Solr.prototype[m];
      if ('function' === typeof method) {
          connector[m] = method.bind(connector);
          for(var k in method) {
              connector[m][k] = method[k];
          }
      }
  }

  callback();
};
