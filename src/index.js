'use strict';

var redisService = require('./redis-service'),
	s3Service = require('./s3-service'),
	s3Client,
	redisClient;

var PREFERENCES = {
	expirationLimit: 3600 //1 hrs
};

function redisAvailable() {
	if (!redisClient) {
		redisClient = redisService.getClient();
	}

	return !!redisClient && redisClient.connected;
}

var resolveUrl = function resolve(resource, callback) {
	if (redisAvailable()) {
		return resolveUsingCache(resource, callback);
	} else {
		return resolveUsingS3(resource, callback);
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

	s3Client.getSignedUrl('getObject', params, function(err, signedURL) {
		if (err) {
			console.error(err);
		}

		if (redisKey) {
			redisClient.setex(redisKey, (PREFERENCES.expirationLimit - 300), signedURL);
		}

		callback(err, signedURL);
	});
};

var resolveUsingCache = function(bucket, key, callback) {
	var redisKey = toRedisKey(bucket, key);

	if (redisAvailable()) {
		redisClient.get(redisKey, function(err, stored) {
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

module.exports = function(config, s3Client, redisClient) {

	//TODO (denise) validate config
	redisClient = redisClient || redisService.init(config.redis).getClient();
	s3Client = s3Client || s3Service.init(config.s3).getS3Client();

	return {
		resolveUrl: resolveUrl,
		resolveUsingCache: resolveUsingCache,
		resolveUsingS3: resolveUsingS3
	};
};
