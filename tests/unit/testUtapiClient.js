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
const metricTypeObject = {
    bucket: 'foo-bucket',
    account: 'foo-account',
};

// Set mock data of a particular size and count.
function setMockData(objectSize, objectCount, timestamp, key) {
    memoryBackend.data[`${key}:storageUtilized:counter`] = objectSize;
    memoryBackend.data[`${key}:numberOfObjects:counter`] = objectCount;
    memoryBackend.data[`${key}:storageUtilized`] = [[timestamp, objectSize]];
    memoryBackend.data[`${key}:numberOfObjects`] = [[timestamp, objectCount]];
    return undefined;
}

// Get prefix values to construct the expected Redis schema keys.
function getPrefixValues(metric, timestamp) {
    const name = metricTypeObject[metric];
    const type = metric === 'bucket' ? 'buckets' : metric;
    return {
        key: `s3:${type}:${name}`,
        timestampKey: `s3:${type}:${timestamp}:${name}`,
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

Object.keys(metricTypeObject).forEach(metric => {
    describe(`UtapiClient:: push ${metric} metrics`, () => {
        const timestamp = getNormalizedTimestamp(Date.now());
        const { key, timestampKey } = getPrefixValues(metric, timestamp);
        let expected;
        let params;
        beforeEach(() => {
            expected = {};
            params = {};
            params[metric] = metricTypeObject[metric];
        });
        afterEach(() => memoryBackend.flushDb());

        it(`should push ${metric} level createBucket metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:CreateBucket`] = '1';
            testMetric('createBucket', params, expected, done);
        });

        it(`should push ${metric} level deleteBucket metrics`, done => {
            expected[`${timestampKey}:DeleteBucket`] = '1';
            testMetric('deleteBucket', params, expected, done);
        });

        it(`should push ${metric} level listBucket metrics`, done => {
            expected[`${timestampKey}:ListBucket`] = '1';
            testMetric('listBucket', params, expected, done);
        });

        it(`should push ${metric} level getBucketAcl metrics`, done => {
            expected[`${timestampKey}:GetBucketAcl`] = '1';
            testMetric('getBucketAcl', params, expected, done);
        });

        it(`should push ${metric} level putBucketAcl metrics`, done => {
            expected[`${timestampKey}:PutBucketAcl`] = '1';
            testMetric('putBucketAcl', params, expected, done);
        });

        it(`should push ${metric} level putBucketWebsite metrics`, done => {
            expected[`${timestampKey}:PutBucketWebsite`] = '1';
            testMetric('putBucketWebsite', params, expected, done);
        });

        it(`should push ${metric} level getBucketWebsite metrics`, done => {
            expected[`${timestampKey}:GetBucketWebsite`] = '1';
            testMetric('getBucketWebsite', params, expected, done);
        });
        it(`should push ${metric} level deleteBucketWebsite metrics`, done => {
            expected[`${timestampKey}:DeleteBucketWebsite`] = '1';
            testMetric('deleteBucketWebsite', params, expected, done);
        });

        it(`should push ${metric} level uploadPart metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '1024';
            expected[`${key}:storageUtilized`] = [[timestamp, '1024']];
            expected[`${timestampKey}:incomingBytes`] = '1024';
            expected[`${timestampKey}:UploadPart`] = '1';
            Object.assign(params, { newByteLength: 1024 });
            testMetric('uploadPart', params, expected, done);
        });

        it(`should push ${metric} level initiateMultipartUpload metrics`,
        done => {
            expected[`${timestampKey}:InitiateMultipartUpload`] = '1';
            testMetric('initiateMultipartUpload', params, expected, done);
        });

        it(`should push ${metric} level completeMultipartUpload metrics`,
        done => {
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CompleteMultipartUpload`] = '1';
            testMetric('completeMultipartUpload', params, expected, done);
        });

        it(`should push ${metric} level listMultipartUploads metrics`, done => {
            expected[`${timestampKey}:ListBucketMultipartUploads`] = '1';
            testMetric('listMultipartUploads', params, expected, done);
        });

        it(`should push ${metric} level listMultipartUploadParts metrics`,
        done => {
            expected[`${timestampKey}:ListMultipartUploadParts`] = '1';
            testMetric('listMultipartUploadParts', params, expected, done);
        });

        it(`should push ${metric} level abortMultipartUpload metrics`, done => {
            expected[`${timestampKey}:AbortMultipartUpload`] = '1';
            testMetric('abortMultipartUpload', params, expected, done);
        });

        it(`should push ${metric} level deleteObject metrics`, done => {
            // Set mock data of one, 1024 byte object for `deleteObject` to
            // update.
            setMockData('1024', '1', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:DeleteObject`] = '1';
            Object.assign(params, {
                byteLength: 1024,
                numberOfObjects: 1,
            });
            testMetric('deleteObject', params, expected, done);
        });

        it(`should push ${metric} level multiObjectDelete metrics`, done => {
            // Set mock data of two, 1024 byte objects for `multiObjectDelete`
            // to update.
            setMockData('2048', '2', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '0';
            expected[`${key}:numberOfObjects:counter`] = '0';
            expected[`${key}:storageUtilized`] = [[timestamp, '0']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '0']];
            expected[`${timestampKey}:MultiObjectDelete`] = '1';
            Object.assign(params, {
                byteLength: 2048,
                numberOfObjects: 2,
            });
            testMetric('multiObjectDelete', params, expected, done);
        });

        it(`should push ${metric} level getObject metrics`, done => {
            expected[`${timestampKey}:outgoingBytes`] = '1024';
            expected[`${timestampKey}:GetObject`] = '1';
            Object.assign(params, { newByteLength: 1024 });
            testMetric('getObject', params, expected, done);
        });

        it(`should push ${metric} level getObjectAcl metrics`, done => {
            expected[`${timestampKey}:GetObjectAcl`] = '1';
            testMetric('getObjectAcl', params, expected, done);
        });

        it(`should push ${metric} level putObject metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '1024';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '1024']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:PutObject`] = '1';
            expected[`${timestampKey}:incomingBytes`] = '1024';
            Object.assign(params, {
                newByteLength: 1024,
                oldByteLength: null,
            });
            testMetric('putObject', params, expected, done);
        });

        it(`should push ${metric} level putObject overwrite metrics`, done => {
            // Set mock data of one, 1024 byte object for `putObject` to
            // overwrite. Counter does not increment because it is an overwrite.
            setMockData('1024', '1', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '2048';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '2048']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:PutObject`] = '1';
            expected[`${timestampKey}:incomingBytes`] = '2048';
            Object.assign(params, {
                newByteLength: 2048,
                oldByteLength: 1024,
            });
            testMetric('putObject', params, expected, done);
        });

        it(`should push ${metric} level copyObject metrics`, done => {
            expected[`${key}:storageUtilized:counter`] = '1024';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '1024']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CopyObject`] = '1';
            Object.assign(params, {
                newByteLength: 1024,
                oldByteLength: null,
            });
            testMetric('copyObject', params, expected, done);
        });

        it(`should push ${metric} level copyObject overwrite metrics`, done => {
            // Set mock data of one, 1024 byte object for `copyObject` to
            // overwrite. Counter does not increment because it is an overwrite.
            setMockData('1024', '1', timestamp, key);
            expected[`${key}:storageUtilized:counter`] = '2048';
            expected[`${key}:numberOfObjects:counter`] = '1';
            expected[`${key}:storageUtilized`] = [[timestamp, '2048']];
            expected[`${key}:numberOfObjects`] = [[timestamp, '1']];
            expected[`${timestampKey}:CopyObject`] = '1';
            Object.assign(params, {
                newByteLength: 2048,
                oldByteLength: 1024,
            });
            testMetric('copyObject', params, expected, done);
        });

        it(`should push ${metric} level putObjectAcl metrics`, done => {
            expected[`${timestampKey}:PutObjectAcl`] = '1';
            testMetric('putObjectAcl', params, expected, done);
        });

        it(`should push ${metric} level headBucket metrics`, done => {
            expected[`${timestampKey}:HeadBucket`] = '1';
            testMetric('headBucket', params, expected, done);
        });

        it(`should push ${metric} level headObject metrics`, done => {
            expected[`${timestampKey}:HeadObject`] = '1';
            testMetric('headObject', params, expected, done);
        });
    });
});
