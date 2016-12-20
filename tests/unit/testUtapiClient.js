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
                testMetric('completeMultipartUpload', params, expected, done);
            });
    });
});
