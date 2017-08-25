var util = require('util');
var solrClient = require('solr-client');
// var deleteByQuery;
var Connector = require('loopback-connector').Connector;
var log = require('debug')('loopback:connector:solr');
var _ = require('underscore');

/**
 * Initialize connector with datasource, configure settings and return
 * @param {object} dataSource
 * @param {function} callback
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
	if (!solrClient) {
		return;
	}

	console.log(dataSource.settings);
	var settings = dataSource.settings || {};

	dataSource.connector = new SolrConnector(settings, dataSource);

	if (callback) {
		dataSource.connector.connect(callback);
	}
};

/**
 * Connector constructor
 * @param {object} settings
 * @param {object} dataSource
 * @constructor
 */
var SolrConnector = function (settings, dataSource) {
	Connector.call(this, 'solr', settings);

	// this.searchIndex = settings.index || '';
	// this.searchIndexSettings = settings.settings || {};
	// this.searchType = settings.type || '';
	// this.defaultSize = (settings.defaultSize || 10);
	// this.idField = 'id';
	// this.apiVersion = (settings.apiVersion || '2.x');
	// this.refreshOn = (settings.refreshOn || ['create', 'save', 'destroy', 'destroyAll', 'updateAttributes', 'updateOrCreate', 'updateAll']);

	this._models = {};

	this.debug = settings.debug || log.enabled;
	if (this.debug) {
		log('Settings: %j', settings);
	}

	this.dataSource = dataSource;
};

/**
 * Inherit the prototype methods
 */
util.inherits(SolrConnector, Connector);

/**
 * Generate a client configuration object based on settings.
 */
SolrConnector.prototype.getClientConfig = function () {
	// http://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
	var config = {
		host: this.settings.host || '127.0.0.1',
		port: this.settings.port || 8086,
		// requestTimeout: this.settings.requestTimeout,
		// apiVersion: this.settings.apiVersion,
		log: this.settings.log || 'error',
		// suggestCompression: true,
		path: this.settings.path || "solr",
		core: this.settings.core || "mentions",
		// "adminPath": "/solr/admin",
		username: this.settings.username || "solr",
		password: this.settings.username || "solr"
	};

	// if (this.settings.amazonES) {
	// 	config.connectionClass = require('http-aws-es');
	// 	config.amazonES = this.settings.amazonES || {
	// 		region: 'us-east-1',
	// 		accessKey: 'AKID',
	// 		secretKey: 'secret'
	// 	}
	// }
	//
	// if (this.settings.ssl) {
	// 	config.ssl = {
	// 		ca: (this.settings.ssl.ca) ? fs.readFileSync(path.join(__dirname, this.settings.ssl.ca)) : fs.readFileSync(path.join(__dirname, '..', 'cacert.pem')),
	// 		rejectUnauthorized: this.settings.ssl.rejectUnauthorized || true
	// 	};
	// }
	// Note: http://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
	//       Due to the complex nature of the configuration, the config object you pass in will be modified
	//       and can only be used to create one Client instance.
	//       Related Github issue: https://github.com/elasticsearch/elasticsearch-js/issues/33
	//       Luckily getClientConfig() pretty much clones settings so we shouldn't have to worry about it.
	return config;
};

/**
 * Connect to Elasticsearch client
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The solr client
 */

SolrConnector.prototype.connect = function (callback) {
	// TODO: throw error if callback isn't provided?
	//       what are the corner-cases when the loopback framework does not provide callback
	//       and we need to be able to live with that?
	var self = this;
	if (self.db) {
		process.nextTick(function () {
			callback && callback(null, self.db);
		});
	}
	else {
		self.db = new solrClient.createClient(self.getClientConfig());
		// if (self.settings.apiVersion.indexOf('2') === 0) {
		// 	log('injecting deleteByQuery');
		// 	deleteByQuery = require('elastic-deletebyquery');
		// 	deleteByQuery(self.db);
		// 	self.db.deleteByQuery = Promise.promisify(self.db.deleteByQuery);
		// }

		// NOTE: any & all indices and mappings will be created or their existence verified before proceeding
		// if (self.settings.mappings) {
		// 	self.setupMappings()
		// 		.then(function () {
		// 			log('ESConnector.prototype.connect', 'setupMappings', 'finished');
		// 			callback && callback(null, self.db);
		// 		})
		// 		.catch(function (err) {
		// 			log('ESConnector.prototype.connect', 'setupMappings', 'failed', err);
		// 			callback && callback(err);
		// 		});
		// }
		// else {
			process.nextTick(function () {
				callback && callback(null, self.db);
			});
		// }
	}
};

SolrConnector.prototype.all = function (model, filter, callback) {
	//connector implementation logic
	// var self = this;
	//
	// var solrQuery = self.db.createQuery();
	//
	// var where = filter.where;
	//
	//
	// if (filter.where) {
	// 	_.keys(filter.where).forEach(function (k) {
	// 		if (_.isArray(query.fq[k])) {
	// 			query.fq[k].forEach(function (p) {
	// 				debug('applying matchFilter() with %s, %s', k, p);
	// 				solrQuery.matchFilter(k, p);
	// 			});
	// 		} else {
	// 			debug('applying matchFilter() with %s, %s', k, query.fq[k]);
	// 			solrQuery.matchFilter(k, query.fq[k]);
	// 		}
	// 	});
	// }
	//
	console.log('filter: ', filter);
	console.log('_.keys(filter.where): ', _.keys(filter.where));
	//
	// self.db.search(query, function(err, data) {
	//
	//
	// });

	callback(err, []);
};

/**
 * Create a new model instance
 * @param {String} model name
 * @param {object} data info
 * @param {Function} done - invoke the callback with the created model's id as an argument
 */
SolrConnector.prototype.create = function (model, data, done) {
	var self = this;
	if (self.debug) {
		log('ESConnector.prototype.create', model, data);
	}

	this.db.add(data, function (err, res) {
		callback(err, res)
	})
};


module.exports.name = SolrConnector.name;
module.exports.SolrConnector = SolrConnector;
