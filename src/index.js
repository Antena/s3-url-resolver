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

var resolveUrlAsAttachment = function resolve(bucket, key, callback) {
	if (redisService.isRedisAvailable()) {
		return resolveUsingCacheOrS3(bucket, key, null, true, callback);
	}
	return resolveUsingS3AsAttachment(bucket, key, null, callback);
};

var resolveUrl = function resolve(bucket, key, callback) {
	if (redisService.isRedisAvailable()) {
		return resolveUsingCache(bucket, key, null, callback);
	}
	return resolveUsingS3(bucket, key, callback);
};

var resolveUrlWithFilenameSuffix = function resolve(bucket, key, filenameSuffix, callback) {
	if (redisService.isRedisAvailable()) {
		return resolveUsingCache(bucket, key, filenameSuffix, callback);
	}
	return resolveUsingS3(bucket, key, callback);
}

function toRedisKey(bucket, key) {
	return bucket + ":" + key;
}

function last(array) {
	var length = array == null ? 0 : array.length;
	return length ? array[length - 1] : undefined;
}

function filenameSafe(s) {
	return s.replace(/[^a-z0-9]/gi, '_').toLowerCase();
}

var resolveUsingS3AsAttachment = function(bucket, key, filenameSuffix, callback) {
	var filename = key;
	if (key.indexOf('/') !== -1) {
		filename = last(key.split('/'));
	}

	if (filenameSuffix) {
		filename = [filename, filenameSafe(filenameSuffix)].join('-')
	}


	var extraParams = {
		ResponseContentDisposition: 'attachment; filename=' + filename
	};

	internalResolveUsingS3(bucket, key, extraParams, callback);

};

var resolveUsingS3 = function(bucket, key, callback) {
	internalResolveUsingS3(bucket, key, null, callback);
};

var internalResolveUsingS3 = function(bucket, key, extraParams, callback) {

	var desiredExpirationInSecondsFromNow = PREFERENCES.expirationLimit;

	var credentials = AWS.config.credentials;

	if (!credentials.expired && credentials.expireTime) {
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

	if (!!extraParams && Object.keys(extraParams).length > 0) {
		params = Object.assign(params, extraParams);
	}

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

var resolveUsingCache = function(bucket, key, filenameSuffix, callback) {
	return resolveUsingCacheOrS3(bucket, key, filenameSuffix, false, callback);
};

var resolveUsingCacheOrS3 = function(bucket, key, filenameSuffix, asAttachment, callback) {
	var redisKey = toRedisKey(bucket, key);

	if (redisService.isRedisAvailable()) {
		return getClient().get(redisKey, function(err, stored) {
			if (stored) {
				return callback(null, stored);
			}
			return asAttachment ? resolveUsingS3AsAttachment(bucket, key, filenameSuffix, callback) : resolveUsingS3(bucket, key, callback);
		});
	}
	return asAttachment ? resolveUsingS3AsAttachment(bucket, key, filenameSuffix, callback) : resolveUsingS3(bucket, key, callback);
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
		resolveUrlAsAttachment: resolveUrlAsAttachment,
		resolveUrl: resolveUrl,
		resolveUrlWithFilenameSuffix: resolveUrlWithFilenameSuffix,
		resolveUsingCache: resolveUsingCache,
		resolveUsingS3: resolveUsingS3,
		resolveUsingS3AsAttachment: resolveUsingS3AsAttachment
	};
};
