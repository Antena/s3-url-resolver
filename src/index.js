'use strict';

var redisService = require('./redis-service'),
	s3Service = require('./s3-service'),
	s3,
	redis,
	redisConfig;

var redisStatus = require('redis-status');

var PREFERENCES = {
	expirationLimit: 3600 //1 hrs
};

function clientAvailable() {
	if (!redis) {
		redis = redisService.getClient();
	}

	return !!redis && redis.connected;
}

function isRedisAvailable(callback) {
	redisStatus(redisConfig).checkStatus(function(err) {
		if (err) {
			return callback(err);
		}
		return callback();
	});
}

var resolveUrl = function resolve(bucket, key, callback) {
	isRedisAvailable(function(err) {
		if (!err && clientAvailable()) {
			return resolveUsingCache(bucket, key, callback);
		}
		return resolveUsingS3(bucket, key, callback);
	});
};

function toRedisKey(bucket, key) {
	return bucket + ":" + key;
}

var resolveUsingS3 = function(bucket, key, callback) {
	var params = {
		Bucket: bucket,
		Key: key,
		Expires: PREFERENCES.expirationLimit
	};

	var redisKey = toRedisKey(bucket, key);

	s3.getSignedUrl('getObject', params, function(err, signedURL) {
		if (err) {
			console.error(err);
		}

		isRedisAvailable(function(err) {
			if (!err && clientAvailable()) {
				if (redisKey) {
					return redis.setex(redisKey, (PREFERENCES.expirationLimit - 300), signedURL);
				}
			}
			return console.error('Redis NOT connected');
		});

		callback(err, signedURL);
	});
};

var resolveUsingCache = function(bucket, key, callback) {
	var redisKey = toRedisKey(bucket, key);

	isRedisAvailable(function(err) {
		if (!err && clientAvailable()) {
			redis.get(redisKey, function(err, stored) {
				if (stored) {
					return callback(null, stored);
				}
				return resolveUsingS3(bucket, key, callback);
			});
		} else {
			return resolveUsingS3(bucket, key, callback);
		}
	});
};

module.exports = function (config, s3Client, redisClient) {
	//TODO (denise) validate config
	redisConfig = config.redis;

	s3 = s3Client || s3Service.init(config.s3).getS3Client();

	isRedisAvailable(function(err) {
		if (err) {
			console.error('Redis NOT connected');
		}
		redis = redisClient || redisService.init(redisConfig).getClient();
	});

	return {
		resolveUrl: resolveUrl,
		resolveUsingCache: resolveUsingCache,
		resolveUsingS3: resolveUsingS3
	};
};
