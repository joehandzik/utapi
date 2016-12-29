/**
* Validate accounts parameter
* @param {string[]} accounts - array of account names
* @return {boolean} - validation result
*/
export default function validateAccounts(accounts) {
    return Array.isArray(accounts) && accounts.length > 0
        && accounts.every(item => typeof item === 'string');
}
