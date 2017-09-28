'use strict';

var AWS = require('aws-sdk'),
	s3;

function init(config) {
	AWS.config.update(config);

	s3 = new AWS.S3({ computeChecksums: true });

	var params = {
		Bucket: config.bucket
	};

	// Ensure the main bucket is created and we have access to it
	s3.headBucket(params, function(err) {
		if (err) {
			console.error("No access to uploads S3 Bucket '" + config.bucket + "'");
		} else {
			console.log("Access to S3 Bucket '" + config.bucket + "' OK");
		}
	});

	return S3Service;
}

function getS3Client() {
	return s3;
}

function setS3Client(client) {
	s3 = client;
}

var S3Service = {
	init: init,
	setS3Client: setS3Client,
	getS3Client: getS3Client
};

module.exports = S3Service;
