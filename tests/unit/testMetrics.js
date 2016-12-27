import assert from 'assert';
import MemoryBackend from '../../src/lib/backend/Memory';
import Datastore from '../../src/lib/Datastore';
import ListMetrics from '../../src/lib/ListMetrics';
import { generateStateKey, generateKey } from '../../src/lib/schema';
import { Logger } from 'werelogs';
const logger = new Logger('UtapiTest');
const memBackend = new MemoryBackend();
const datastore = new Datastore();
const metricTypeObject = {
    bucket: 'foo-bucket',
    account: 'foo-account',
};
datastore.setClient(memBackend);

// Create the metric response object for a given metric.
function getMetricRes(type) {
    const metricRes = {
        timeRange: [],
        storageUtilized: [0, 0],
        incomingBytes: 0,
        outgoingBytes: 0,
        numberOfObjects: [0, 0],
        operations: {
            's3:DeleteBucket': 0,
            's3:ListBucket': 0,
            's3:GetBucketAcl': 0,
            's3:CreateBucket': 0,
            's3:PutBucketAcl': 0,
            's3:PutBucketWebsite': 0,
            's3:DeleteBucketWebsite': 0,
            's3:GetBucketWebsite': 0,
            's3:PutObject': 0,
            's3:CopyObject': 0,
            's3:UploadPart': 0,
            's3:ListBucketMultipartUploads': 0,
            's3:ListMultipartUploadParts': 0,
            's3:InitiateMultipartUpload': 0,
            's3:CompleteMultipartUpload': 0,
            's3:AbortMultipartUpload': 0,
            's3:DeleteObject': 0,
            's3:MultiObjectDelete': 0,
            's3:GetObject': 0,
            's3:GetObjectAcl': 0,
            's3:PutObjectAcl': 0,
            's3:HeadBucket': 0,
            's3:HeadObject': 0,
        },
    };
    // Map defining the appropriate metric key in the response.
    const map = {
        bucket: 'bucketName',
        account: 'accountId',
    };
    metricRes[map[type]] = metricTypeObject[type];
    return metricRes;
}

function assertMetrics(type, metricName, props, done) {
    const timestamp = new Date().setMinutes(0, 0, 0);
    const timeRange = [timestamp, timestamp];
    const expectedRes = getMetricRes(type);
    const expectedResProps = props || {};
    // To instantiate bucket-level metrics, ListMetrics class uses 'buckets'.
    const metricType = type === 'bucket' ? 'buckets' : type;
    const MetricType = new ListMetrics(metricType);
    MetricType.getMetrics(metricName, timeRange, datastore, logger,
        (err, res) => {
            assert.strictEqual(err, null);
            // overwrite operations metrics
            if (expectedResProps.operations) {
                Object.assign(expectedRes.operations,
                    expectedResProps.operations);
                delete expectedResProps.operations;
            }
            assert.deepStrictEqual(res, Object.assign(expectedRes,
                { timeRange }, expectedResProps));
            done();
        });
}

// Create the metric object for retrieving Redis keys from schema methods.
function getSchemaObject(metric) {
    const schemaObject = {};
    schemaObject[metric] = metricTypeObject[metric];
    return schemaObject;
}

function testOps(type, keyIndex, metricindex, done) {
    const schemaObject = getSchemaObject(type);
    const timestamp = new Date().setMinutes(0, 0, 0);
    let key;
    let props = {};
    let val;
    if (keyIndex === 'storageUtilized' || keyIndex === 'numberOfObjects') {
        key = generateStateKey(schemaObject, keyIndex);
        val = 1024;
        props[metricindex] = [val, val];
        memBackend.zadd(key, timestamp, val, () =>
            assertMetrics(type, schemaObject[type], props, done));
    } else if (keyIndex === 'incomingBytes' || keyIndex === 'outgoingBytes') {
        key = generateKey(schemaObject, keyIndex, timestamp);
        val = 1024;
        props[metricindex] = val;
        memBackend.incrby(key, val, () =>
            assertMetrics(type, schemaObject[type], props, done));
    } else {
        key = generateKey(schemaObject, keyIndex, timestamp);
        val = 1;
        props = { operations: {} };
        props.operations[metricindex] = val;
        memBackend.incr(key, () =>
            assertMetrics(type, schemaObject[type], props, done));
    }
}

Object.keys(metricTypeObject).forEach(type => {
    describe(`Get ${type} level metrics`, () => {
        afterEach(() => memBackend.flushDb());

        it(`should list default (0s) ${type} level metrics of a bucket`, done =>
            assertMetrics(type, metricTypeObject[type], null, done));

        it(`should return ${type} level metrics for storage utilized`, done =>
            testOps(type, 'storageUtilized', 'storageUtilized', done));

        it(`should return ${type} level metrics for number of objects`, done =>
            testOps(type, 'numberOfObjects', 'numberOfObjects', done));

        it(`should return ${type} level metrics for incoming bytes`, done =>
            testOps(type, 'incomingBytes', 'incomingBytes', done));

        it(`should return ${type} level metrics for outgoing bytes`, done =>
            testOps(type, 'outgoingBytes', 'outgoingBytes', done));

        it(`should return ${type} level metrics for delete bucket`, done =>
            testOps(type, 'deleteBucket', 's3:DeleteBucket', done));

        it(`should return ${type} level metrics for list bucket`, done =>
            testOps(type, 'listBucket', 's3:ListBucket', done));

        it(`should return ${type} level metrics for get bucket acl`, done =>
            testOps(type, 'getBucketAcl', 's3:GetBucketAcl', done));

        it(`should return ${type} level metrics for put bucket acl`, done =>
            testOps(type, 'putBucketAcl', 's3:PutBucketAcl', done));

        it(`should return ${type} level metrics for put bucket website`, done =>
            testOps(type, 'putBucketWebsite', 's3:PutBucketWebsite', done));

        it(`should return ${type} level metrics for put object`, done =>
            testOps(type, 'putObject', 's3:PutObject', done));

        it(`should return ${type} level metrics for copy object`, done =>
            testOps(type, 'copyObject', 's3:CopyObject', done));

        it(`should return ${type} level metrics for upload part`, done =>
            testOps(type, 'uploadPart', 's3:UploadPart', done));

        it(`should return ${type} level metrics for list bucket multipart ` +
            'uploads', done => testOps(type, 'listBucketMultipartUploads',
                's3:ListBucketMultipartUploads', done));

        it(`should return ${type} level metrics for list multipart upload ` +
            'parts', done => testOps(type, 'listMultipartUploadParts',
                's3:ListMultipartUploadParts', done));

        it(`should return ${type} level metrics for initiate multipart ` +
            'upload', done => testOps(type, 'initiateMultipartUpload',
                's3:InitiateMultipartUpload', done));

        it(`should return ${type} level metrics for complete multipart ` +
            'upload', done => testOps(type, 'completeMultipartUpload',
                's3:CompleteMultipartUpload', done));

        it(`should return ${type} level metrics for abort multipart ` +
            'upload', done => testOps(type, 'abortMultipartUpload',
                's3:AbortMultipartUpload', done));

        it(`should return ${type} level metrics for delete object`, done =>
            testOps(type, 'deleteObject', 's3:DeleteObject', done));

        it(`should return ${type} level metrics for multiObjectDelete`, done =>
            testOps(type, 'multiObjectDelete', 's3:MultiObjectDelete', done));

        it(`should return ${type} level metrics for get object`, done =>
            testOps(type, 'getObject', 's3:GetObject', done));

        it(`should return ${type} level metrics for get object acl`, done =>
            testOps(type, 'getObjectAcl', 's3:GetObjectAcl', done));

        it(`should return ${type} level metrics for put object acl`, done =>
            testOps(type, 'putObjectAcl', 's3:PutObjectAcl', done));

        it(`should return ${type} level metrics for head bucket`, done =>
            testOps(type, 'headBucket', 's3:HeadBucket', done));

        it(`should return ${type} level metrics for head object`, done =>
            testOps(type, 'headObject', 's3:HeadObject', done));
    });
});
