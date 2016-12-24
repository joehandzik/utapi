import assert from 'assert';
import { Logger } from 'werelogs';
import Datastore from '../../src/lib/Datastore';
import MemoryBackend from '../../src/lib/backend/Memory';
import UtapiClient from '../../src/lib/UtapiClient';
import { getNormalizedTimestamp } from '../testUtils';

const memoryBackend = new MemoryBackend();
const ds = new Datastore();
ds.setClient(memoryBackend);
const REQUID = 'aaaaaaaaaaaaaaaaaaa';
const metricObj = {
    bucket: 'foo-bucket',
    account: 'foo-account',
};

// Set mock data of a particular size and count.
function setData(objectSize, objectCount, timestamp, key) {
    memoryBackend.data[`${key}:storageUtilized:counter`] = objectSize;
    memoryBackend.data[`${key}:numberOfObjects:counter`] = objectCount;
    memoryBackend.data[`${key}:storageUtilized`] =
        [[timestamp, objectSize]];
    memoryBackend.data[`${key}:numberOfObjects`] =
        [[timestamp, objectCount]];
    return undefined;
}

function getValues(metric) {
    const timestamp = getNormalizedTimestamp(Date.now());
    const name = metricObj[metric];
    const level = metric === 'bucket' ? 'buckets' : metric;
    return {
        key: `s3:${level}:${name}`,
        timestampKey: `s3:${level}:${timestamp}:${name}`,
        timestamp,
        level,
    };
}

function testMetric(action, params, expected, cb) {
    const c = new UtapiClient();
    c.setDataStore(ds);
    c.pushMetric(action, REQUID, params, () => {
        assert.deepStrictEqual(memoryBackend.data, expected);
        cb();
    });
}

describe('UtapiClient:: enable/disable client', () => {
    it('should disable client when no redis config is provided', () => {
        const c = new UtapiClient();
        assert.strictEqual(c instanceof UtapiClient, true);
        assert.strictEqual(c.disableClient, true);
        assert.strictEqual(c.log instanceof Logger, true);
        assert.strictEqual(c.ds, null);
    });

    it('should enable client when redis config is provided', () => {
        const c = new UtapiClient({ redis: { host: 'localhost', port: 6379 } });
        assert.strictEqual(c instanceof UtapiClient, true);
        assert.strictEqual(c.disableClient, false);
        assert.strictEqual(c.log instanceof Logger, true);
        assert.strictEqual(c.ds instanceof Datastore, true);
    });
});

describe('UtapiClient:: push metrics', () => {
    let expected;
    beforeEach(() => { expected = {}; });
    afterEach(() => memoryBackend.flushDb());

    Object.keys(metricObj).forEach(metric => {
        const params = {};
        params[metric] = metricObj[metric];
        const { key, timestampKey, timestamp, level } = getValues(metric);

        it(`should push ${level} level createBucket metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:CreateBucket`] = '1';
            testMetric('createBucket', params, expected, done);
        });

        it(`should push ${level} level deleteBucket metrics`, done => {
            expected[`${timestampKey}:DeleteBucket`] = '1';
            testMetric('deleteBucket', params, expected, done);
        });

        it(`should push ${level} level listBucket metrics`, done => {
            expected[`${timestampKey}:ListBucket`] = '1';
            testMetric('listBucket', params, expected, done);
        });

        it(`should push ${level} level getBucketAcl metrics`, done => {
            expected[`${timestampKey}:GetBucketAcl`] = '1';
            testMetric('getBucketAcl', params, expected, done);
        });

        it(`should push ${level} level putBucketAcl metrics`, done => {
            expected[`${timestampKey}:PutBucketAcl`] = '1';
            testMetric('putBucketAcl', params, expected, done);
        });

        it('should push metric for putBucketWebsite', done => {
            expected[`${timestampKey}:PutBucketWebsite`] = '1';
            testMetric('putBucketWebsite', params, expected, done);
        });

        it('should push metric for getBucketWebsite', done => {
            expected[`${timestampKey}:GetBucketWebsite`] = '1';
            testMetric('getBucketWebsite', params, expected, done);
        });
        it('should push metric for deleteBucketWebsite', done => {
            expected[`${timestampKey}:DeleteBucketWebsite`] = '1';
            testMetric('deleteBucketWebsite', params, expected, done);
        });

        it(`should push ${level} level uploadPart metrics`, done => {
            const newByteLength = 1024;
            expected[`${key}:storageUtilized:counter`] = `${newByteLength}`;
            expected[`${key}:storageUtilized`] =
                [[timestamp, `${newByteLength}`]];
            expected[`${timestampKey}:incomingBytes`] = `${newByteLength}`;
            expected[`${timestampKey}:UploadPart`] = '1';
            const uploadPartParams = Object.assign({}, params,
                { newByteLength });
            testMetric('uploadPart', uploadPartParams, expected, done);
        });

        it(`should push ${level} level initiateMultipartUpload metrics`,
        done => {
            expected[`${timestampKey}:InitiateMultipartUpload`] = '1';
            testMetric('initiateMultipartUpload', params, expected, done);
        });

        it(`should push ${level} level completeMultipartUpload metrics`,
        done => {
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CompleteMultipartUpload`] = '1';
            testMetric('completeMultipartUpload', params, expected,
                done);
        });

        it(`should push ${level} listMultipartUploads level metrics`, done => {
            expected[`${timestampKey}:ListBucketMultipartUploads`] = '1';
            testMetric('listMultipartUploads', params, expected, done);
        });

        it(`should push ${level} listMultipartUploadParts level metrics`,
        done => {
            expected[`${timestampKey}:ListMultipartUploadParts`] = '1';
            testMetric('listMultipartUploadParts', params, expected,
                done);
        });

        it(`should push ${level} abortMultipartUpload level metrics`, done => {
            expected[`${timestampKey}:AbortMultipartUpload`] = '1';
            testMetric('abortMultipartUpload', params, expected, done);
        });

        it(`should push ${level} deleteObject level metrics`, done => {
            // Set mock data of one, 1024 byte object for `deleteObject` to
            // update.
            setData('1024', '1', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:DeleteObject`] = '1';
            const deleteObjectParams = Object.assign({}, params, {
                byteLength: 1024,
                numberOfObjects: 1,
            });
            testMetric('deleteObject', deleteObjectParams, expected,
                done);
        });

        it(`should push ${level} multiObjectDelete level metrics`, done => {
            // Set mock data of two, 1024 byte objects for `multiObjectDelete`
            // to update.
            setData('2048', '2', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:MultiObjectDelete`] = '1';
            const multiObjectDeleteParams = Object.assign({}, params, {
                byteLength: 2048,
                numberOfObjects: 2,
            });
            testMetric('multiObjectDelete', multiObjectDeleteParams,
                expected, done);
        });

        it(`should push ${level} getObject level metrics`, done => {
            expected[`${timestampKey}:outgoingBytes`] = '1024';
            expected[`${timestampKey}:GetObject`] = '1';
            const getObjectParams = Object.assign({}, params, {
                newByteLength: 1024,
            });
            testMetric('getObject', getObjectParams, expected, done);
        });

        it(`should push ${level} getObjectAcl level metrics`, done => {
            expected[`${timestampKey}:GetObjectAcl`] = '1';
            testMetric('getObjectAcl', params, expected, done);
        });

        it(`should push ${level} putObject level metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '1024';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '1024']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:PutObject`] = '1';
            expected[`${timestampKey}:incomingBytes`] = '1024';
            const putObjectParams = Object.assign({}, params, {
                newByteLength: 1024,
                oldByteLength: null,
            });
            testMetric('putObject', putObjectParams, expected, done);
        });

        it(`should push ${level} putObject level overwrite metrics`, done => {
            // Set mock data of one, 1024 byte object for `putObject` to
            // overwrite.
            setData('1024', '1', timestamp, key);
            // Counter should not increment as this is an overwrite.
            expected[`${key}:storageUtilized:counter`] = '2048';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '2048']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:PutObject`] = '1';
            expected[`${timestampKey}:incomingBytes`] = '2048';
            const putObjectParams = Object.assign({}, params, {
                newByteLength: 2048,
                oldByteLength: 1024,
            });
            testMetric('putObject', putObjectParams, expected, done);
        });

        it(`should push ${level} copyObject level metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '1024';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '1024']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CopyObject`] = '1';
            const copyObjectParams = Object.assign({}, params, {
                newByteLength: 1024,
                oldByteLength: null,
            });
            testMetric('copyObject', copyObjectParams, expected, done);
        });

        it(`should push ${level} copyObject level overwrite metrics`, done => {
            // Set mock data of one, 1024 byte object for `copyObject` to
            // overwrite.
            setData('1024', '1', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '2048';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '2048']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CopyObject`] = '1';
            const copyObjectParams = Object.assign({}, params, {
                newByteLength: 2048,
                oldByteLength: 1024,
            });
            testMetric('copyObject', copyObjectParams, expected, done);
        });

        it(`should push ${level} putObjectAcl level metrics`, done => {
            expected[`${timestampKey}:PutObjectAcl`] = '1';
            testMetric('putObjectAcl', params, expected, done);
        });

        it('should push metric for headBucket', done => {
            expected[`${timestampKey}:HeadBucket`] = '1';
            testMetric('headBucket', params, expected, done);
        });

        it(`should push ${level} headObject level metrics`, done => {
            expected[`${timestampKey}:HeadObject`] = '1';
            testMetric('headObject', params, expected, done);
        });
    });
});
