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

	// console.log(dataSource.settings);
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
	};
	
	if(this.settings.username){
		config.username = this.settings.username;
		config.password = this.settings.password;
	}

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
		
		if(this.settings.username){
			self.db.basicAuth(this.settings.username,this.settings.password);
		}

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

/**
 * Create a new model instance
 * @param {String} model name
 * @param {object} data info
 * @param {Function} done - invoke the callback with the created model's id as an argument
 */
SolrConnector.prototype.updateOrCreate = function (model, data, done) {
    var self = this;
    if (self.debug) {
        log('ESConnector.prototype.create', model, data);
    }

    var idValue = self.getIdValue(model, data);
    var idName = self.idName(model);

    var newDoc = {};
    newDoc[idName] = idValue;
    delete data.idName;

    _.keys(data).forEach(function (key) {
        // debug('applying matchFilter() with %s, %s', k, p);
        newDoc.key = {set: data[key]}
    });

	self.db.add(newDoc, function(err,res) {
		if(err){
			// console.log(err);
			return done(err, null);
		}
		done(null, idValue);
	})
};

/**
 * Update a model instance by id
 *
 * NOTES:
 * > The _source field need to be enabled for this feature to work.
 */
SolrConnector.prototype.updateAttributes = function(modelName, id, data, callback) {
	var self = this;
	log('ESConnector.prototype.updateAttributes', 'modelName', modelName, 'id', id, 'data', data);


	console.log("updateAttributes: ");
	// var idValue = self.getIdValue(model, data);
	var idName = self.idName(model);

	var newDoc = {};
	newDoc[idName] = id;

	delete data[idName];

	if(data.shard){
		newDoc.shard = data.shard;
		delete data.shard;
	}

	_.keys(data).forEach(function (key) {
		// debug('applying matchFilter() with %s, %s', k, p);
		newDoc[key] = {set: data[key]}
	});


	self.db.add(newDoc, callback)
};


SolrConnector.prototype.all = function (model, filter, callback) {
	//connector implementation logic
	// var self = this;
	//
	var self = this;
	//
	var solrQuery = self.db.createQuery().q('*:*');
	//
	// var where = filter.where;
	//
	//

	if (filter.fields) {
		var fields = filter.fields;

		if (_.isArray(fields)) {
			solrQuery.fl(fields.join(','))
		} else if(_.isString(fields)){
			solrQuery.fl(fields.join(','))
		}
	}

	if (filter.where) {
		var where = filter.where;
		_.keys(where).forEach(function (p, index) {
			// debug('applying matchFilter() with %s, %s', k, p);

			solrQuery.matchFilter(p, where[p]);
		});

	}

	if (filter.order) {
		var order = filter.order;

		var sort = {};

		if (_.isArray(order)) {
			solrQuery.sort(order.join(','))
		} else if(_.isString(order)){
			solrQuery.sort(order)
		}
		solrQuery.sort(sort)
	}

	self.db.search(solrQuery, function(err, data) {
		if(err){
			console.log('err: ', err);
			console.log('solrQuery: ', solrQuery);

			callback(err, null);
			return;
		}

		callback(null, data.response.docs);
	});
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

	var idValue = self.getIdValue(model, data);
	// var idName = self.idName(model);

	self.db.add(data, function(err,res){
		if(err){
			// console.log(err);
			return done(err, null);
		}
		done(null, idValue);
	});
	// done(null, 'hihi');
	// })
};


module.exports.name = SolrConnector.name;
module.exports.SolrConnector = SolrConnector;
