'use strict';

var async = require('async'),
	redis = require("redis"),
	md5 = require('md5'),
	client,
	redisConfig,
	logger;

var isServiceOK = true;

var PREFERENCES = {
	MAX_SESSION_AGE: 60 * 60
};

function init(config) {
	redisConfig = config;	//TODO (denise) validate
	logger = redisConfig.logger;
	return RedisService;
}

function changeStatus(status) {
	isServiceOK = status;
}

function getClient(overrideConfig) {
	if (!client || !client.connected) {
		var config = overrideConfig || redisConfig;
		client = redis.createClient(config.port, config.host, {});
		client.on('ready', function() {
			logger.info('Redis connected');
			changeStatus(true);
			return client;
		});
		client.on('error', function(){
			// If Redis is not responsive, `node_redis` will emit an error on the next turn of the event
			// loop. If we don't provide an error handler, that error will bring down the process. Providing
			// an error handler will cause `node_redis` to begin attempting to reconnect
		});
		client.on('end', function() {
			changeStatus(false);
			logger.warn('Redis disconnected');
			return null;
		});

		return client;
	} else {
		return client;
	}
}

function remove(user, fileBuffer, callback) {
	if (isRedisAvailable()) {
		return async.waterfall([
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
	return callback(null, null);
}

function get(key, callback) {
	if (isRedisAvailable()) {
		return getClient().get(key, callback);
	}
	return null;
}

function setex(key, expiration, data) {
	if (isRedisAvailable()) {
		expiration = expiration || PREFERENCES.EXPIRATION;
		return getClient().setex(key, expiration, data);
	}
	return null;
}

function isRedisAvailable() {
	return isServiceOK;
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

