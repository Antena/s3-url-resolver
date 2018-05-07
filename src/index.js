'use strict';

var redisService = require('./redis-service'),
	s3Service = require('./s3-service'),
	s3,
	redis,
	redisConfig,
	logger;

var PREFERENCES = {
	expirationLimit: 3600 //1 hrs
};

function getClient() {
	if(!redis) {
		redis = redisService.getClient();
	}
	return redis;
}

var resolveUrl = function resolve(bucket, key, callback) {
	if (redisService.isRedisAvailable()) {
		return resolveUsingCache(bucket, key, callback);
	}
	return resolveUsingS3(bucket, key, callback);
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
			logger.error(err);
		}

		if (redisService.isRedisAvailable() && redisKey) {
			getClient().setex(redisKey, (PREFERENCES.expirationLimit - 300), signedURL);
		}

		callback(err, signedURL);
	});
};

var resolveUsingCache = function(bucket, key, callback) {
	var redisKey = toRedisKey(bucket, key);

	if (redisService.isRedisAvailable()) {
		return getClient().get(redisKey, function(err, stored) {
			if (stored) {
				return callback(null, stored);
			}
			return resolveUsingS3(bucket, key, callback);
		});
	}
	return resolveUsingS3(bucket, key, callback);
};

module.exports = function (config, s3Client, redisClient) {
	// TODO (denise) validate config
	logger = config.logger || console;

	redisConfig = config.redis;
	redisConfig.logger = logger;
	config.s3.logger = logger;

	s3 = s3Client || s3Service.init(config.s3).getS3Client();

	redisService.init(redisConfig);

	if (redisService.isRedisAvailable()) {
		redis = redisClient || redisService.getClient();
	}

	return {
		resolveUrl: resolveUrl,
		resolveUsingCache: resolveUsingCache,
		resolveUsingS3: resolveUsingS3
	};
};
