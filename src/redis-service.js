'use strict';

var async = require('async'),
	redis = require("redis"),
	md5 = require('md5'),
	client,
	redisConfig;

var PREFERENCES = {
	MAX_SESSION_AGE: 60 * 60
};

function init(config) {
	redisConfig = config;	//TODO (denise) validate
	return RedisService;
}

function getClient(overrideConfig) {
	if (!client || !client.connected) {
		var config = overrideConfig || redisConfig;
		client = redis.createClient(config.port, config.host, {});
		client.on('connect', function() {
			console.log('Redis connected');
			return client;
		});
		client.on('err', function() {
			console.warn('Redis disconnected');
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
			console.log('Deleting from key from redis: ', key);
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

var RedisService = {
	init: init,
	get: get,
	getClient: getClient,
	delete: remove,
	setex: setex
};

module.exports = RedisService;

