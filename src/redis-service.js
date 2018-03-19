'use strict';

var async = require('async'),
	redis = require("redis"),
	md5 = require('md5'),
	redisStatus = require('redis-status'),
	client,
	redisConfig,
	logger;

var PREFERENCES = {
	MAX_SESSION_AGE: 60 * 60
};

function init(config) {
	redisConfig = config;	//TODO (denise) validate
	logger = redisConfig.logger;
	return RedisService;
}

function getClient(overrideConfig) {
	if (!client || !client.connected) {
		var config = overrideConfig || redisConfig;
		client = redis.createClient(config.port, config.host, {});
		client.on('ready', function() {
			logger.info('Redis connected');
			return client;
		});
		client.on('error', function(){
			// If Redis is not responsive, `node_redis` will emit an error on the next turn of the event
			// loop. If we don't provide an error handler, that error will bring down the process. Providing
			// an error handler will cause `node_redis` to begin attempting to reconnect
		});
		client.on('end', function() {
			logger.warn('Redis disconnected');
			return null;
		});

		return client;
	} else {
		return client;
	}
}

function remove(user, fileBuffer, callback) {
	async.waterfall([
		function(cb) {
			var currentMD5Key = user + ":" + md5(fileBuffer);
			cb(null, currentMD5Key);
		},
		function(key, cb) {
			logger.info('Deleting from key from redis: ', key);
			client.del(key, cb);
		}
	], function(err, data) {
		callback(err, data);
	});
}

function get(key, callback) {
	getClient().get(key, callback);
}

function setex(key, expiration, data) {
	expiration = expiration || PREFERENCES.EXPIRATION;
	getClient().setex(key, expiration, data);
}

function isRedisAvailable(callback) {

	redisStatus(redisConfig).checkStatus(function(err) {
		if (err) {
			return callback(err);
		}
		return callback();
	});
}

function clientAvailable() {
	if (!client) {
		client = getClient();
	}

	return !!client && client.connected;
}

var RedisService = {
	init: init,
	get: get,
	getClient: getClient,
	delete: remove,
	setex: setex,
	isRedisAvailable: isRedisAvailable,
	clientAvailable: clientAvailable
};

module.exports = RedisService;

