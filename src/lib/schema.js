// bucket schema
const bucketStateKeys = {
    storageUtilized: (l, id) => `s3:${l}:${id}:storageUtilized`,
    numberOfObjects: (l, id) => `s3:${l}:${id}:numberOfObjects`,
};

const bucketCounters = {
    storageUtilizedCounter: (l, id) => `s3:${l}:${id}:storageUtilized:counter`,
    numberOfObjectsCounter: (l, id) => `s3:${l}:${id}:numberOfObjects:counter`,
};

const bucketKeys = {
    createBucket: (l, id, t) => `s3:${l}:${t}:${id}:CreateBucket`,
    deleteBucket: (l, id, t) => `s3:${l}:${t}:${id}:DeleteBucket`,
    listBucket: (l, id, t) => `s3:${l}:${t}:${id}:ListBucket`,
    getBucketAcl: (l, id, t) => `s3:${l}:${t}:${id}:GetBucketAcl`,
    putBucketAcl: (l, id, t) => `s3:${l}:${t}:${id}:PutBucketAcl`,
    putBucketWebsite: (l, id, t) => `s3:${l}:${t}:${id}:PutBucketWebsite`,
    getBucketWebsite: (l, id, t) => `s3:${l}:${t}:${id}:GetBucketWebsite`,
    deleteBucketWebsite: (l, id, t) => `s3:${l}:${t}:${id}:DeleteBucketWebsite`,
    listBucketMultipartUploads: (l, id, t) =>
        `s3:${l}:${t}:${id}:ListBucketMultipartUploads`,
    listMultipartUploadParts: (l, id, t) =>
        `s3:${l}:${t}:${id}:ListMultipartUploadParts`,
    initiateMultipartUpload: (l, id, t) =>
        `s3:${l}:${t}:${id}:InitiateMultipartUpload`,
    completeMultipartUpload: (l, id, t) =>
        `s3:${l}:${t}:${id}:CompleteMultipartUpload`,
    abortMultipartUpload: (l, id, t) =>
        `s3:${l}:${t}:${id}:AbortMultipartUpload`,
    deleteObject: (l, id, t) => `s3:${l}:${t}:${id}:DeleteObject`,
    multiObjectDelete: (l, id, t) => `s3:${l}:${t}:${id}:MultiObjectDelete`,
    uploadPart: (l, id, t) => `s3:${l}:${t}:${id}:UploadPart`,
    getObject: (l, id, t) => `s3:${l}:${t}:${id}:GetObject`,
    getObjectAcl: (l, id, t) => `s3:${l}:${t}:${id}:GetObjectAcl`,
    putObject: (l, id, t) => `s3:${l}:${t}:${id}:PutObject`,
    copyObject: (l, id, t) => `s3:${l}:${t}:${id}:CopyObject`,
    putObjectAcl: (l, id, t) => `s3:${l}:${t}:${id}:PutObjectAcl`,
    headBucket: (l, id, t) => `s3:${l}:${t}:${id}:HeadBucket`,
    headObject: (l, id, t) => `s3:${l}:${t}:${id}:HeadObject`,
    incomingBytes: (l, id, t) => `s3:${l}:${t}:${id}:incomingBytes`,
    outgoingBytes: (l, id, t) => `s3:${l}:${t}:${id}:outgoingBytes`,
};

function getSchemaPrefix(params) {
    const map = {
        bucket: 'buckets',
        account: 'account',
    };
    const level = Object.keys(params).filter(k => k in map)[0];
    const data = {};
    data.level = map[level];
    data.name = params[level];
    return data;
}

/**
* Returns the metric key in schema for the bucket
* @param {string} params - bucket name
* @param {string} metric - metric name
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string} - schema key
*/
export function genBucketKey(params, metric, timestamp) {
    const { level, name } = getSchemaPrefix(params);
    return bucketKeys[metric](level, name, timestamp);
}

/**
* Returns a list of the counters for a bucket
* @param {string} params - bucket name
* @return {string[]} - array of keys for counters
*/
export function getBucketCounters(params) {
    const { level, name } = getSchemaPrefix(params);
    return Object.keys(bucketCounters).map(
        item => bucketCounters[item](level, name));
}

/**
* Returns a list of all keys for a bucket
* @param {string} params - bucket name
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string[]} - list of keys
*/
export function getBucketKeys(params, timestamp) {
    const { level, name } = getSchemaPrefix(params);
    return Object.keys(bucketKeys)
        .map(item => bucketKeys[item](level, name, timestamp));
}

/**
* Returns metric from key
* @param {string} key - schema key
* @param {string} value - the metric value
* @return {string} metric - S3 metric
*/
export function getMetricFromKey(key, value) {
    // s3:buckets:1473451689898:demo:putObject
    return key.slice(25).replace(`${value}:`, '');
}

/**
* Returns the keys representing state of the bucket
* @param {string} params - bucket name
* @return {string[]} - list of keys
*/
export function getBucketStateKeys(params) {
    const { level, name } = getSchemaPrefix(params);
    return Object.keys(bucketStateKeys)
        .map(item => bucketStateKeys[item](level, name));
}

/**
* Returns the state metric key in schema for the bucket
* @param {string} params - bucket name
* @param {string} metric - metric name
* @return {string} - schema key
*/
export function genBucketStateKey(params, metric) {
    const { level, name } = getSchemaPrefix(params);
    return bucketStateKeys[metric](level, name);
}

export function genBucketCounter(params, metric) {
    const { level, name } = getSchemaPrefix(params);
    return bucketCounters[metric](level, name);
}
