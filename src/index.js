'use strict';

var redisService = require('./redis-service'),
	s3Service = require('./s3-service'),
	AWS = require('aws-sdk'),
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
	var desiredExpirationInSecondsFromNow = PREFERENCES.expirationLimit;

	var credentials = AWS.config.credentials;

	logger.info("AWS.config = ", JSON.stringify(AWS.config, null, 2));	//TODO (denise) remove log

	if (!!credentials && !credentials.expired && credentials.expireTime) {
		var credentialsExpireTime = credentials.expireTime.getTime();
		var now = new Date().getTime();
		var credentialsExpireTimeInSecondsFromNow = (credentialsExpireTime - now) / 1000;
		desiredExpirationInSecondsFromNow = Math.min(desiredExpirationInSecondsFromNow, credentialsExpireTimeInSecondsFromNow);
	}

	var params = {
		Bucket: bucket,
		Key: key,
		Expires: desiredExpirationInSecondsFromNow
	};

	var redisKey = toRedisKey(bucket, key);

	s3.getSignedUrl('getObject', params, function(err, signedURL) {
		if (err) {
			logger.error(err);
		}

		if (redisService.isRedisAvailable() && redisKey) {
			getClient().setex(redisKey, (desiredExpirationInSecondsFromNow - 300), signedURL);
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
