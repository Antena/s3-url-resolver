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

function redisAvailable() {
	redisStatus(redisConfig).checkStatus(function(err) {
		console.log('pepe')
		if (!err) {
			console.log('pepito')
			if (!redis) {
				redis = redisService.getClient();
			}

			return !!redis && redis.connected;
		}
		console.error('Redis NOT connected');
	});
}

var resolveUrl = function resolve(bucket, key, callback) {
	if (redisAvailable()) {
		return resolveUsingCache(bucket, key, callback);
	} else {
		return resolveUsingS3(bucket, key, callback);
	}
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

		if (redisKey) {
			redis.setex(redisKey, (PREFERENCES.expirationLimit - 300), signedURL);
		}

		callback(err, signedURL);
	});
};

var resolveUsingCache = function(bucket, key, callback) {
	var redisKey = toRedisKey(bucket, key);

	if (redisAvailable()) {
		redis.get(redisKey, function(err, stored) {
			if (stored) {
				callback(null, stored);
			} else {
				resolveUsingS3(bucket, key, callback);
			}
		});
	} else {
		resolveUsingS3(bucket, key, callback);
	}
};

module.exports = function (config, s3Client, redisClient) {
	//TODO (denise) validate config
	redisConfig = config.redis;

	s3 = s3Client || s3Service.init(config.s3).getS3Client();

	redisStatus(redisConfig).checkStatus(function(err) {
		if (!err) {
			redis = redisClient || redisService.init(redisConfig).getClient();
		}
		console.error('Redis NOT connected');
	});

	return {
		resolveUrl: resolveUrl,
		resolveUsingCache: resolveUsingCache,
		resolveUsingS3: resolveUsingS3
	};
};
