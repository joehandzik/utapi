import BucketsHandler from '../handlers/BucketsHandler';
import AccountsHandler from '../handlers/AccountsHandler';
import validateBucketsListMetrics from '../validators/bucketsListMetrics';
import validateAccountsListMetrics from '../validators/accountsListMetrics';
import bucketsListMetricsResponse from '../responses/bucketsListMetrics';
import accountsListMetricsResponse from '../responses/accountsListMetrics';

export default [
    {
        validator: validateBucketsListMetrics,
        handler: BucketsHandler.listMetrics,
        method: 'POST',
        action: 'ListMetrics',
        resource: 'buckets',
        responseBuilder: bucketsListMetricsResponse,
        statusCode: 200,
    },
    {
        validator: validateAccountsListMetrics,
        handler: AccountsHandler.listMetrics,
        method: 'POST',
        action: 'ListMetrics',
        resource: 'account',
        responseBuilder: accountsListMetricsResponse,
        statusCode: 200,
    },
];
