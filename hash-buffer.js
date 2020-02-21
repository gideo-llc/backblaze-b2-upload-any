try {
    const asyncHash = require('@ronomon/crypto-async').hash;
    const { promisify } = require('./promise-utils');

    const promiseHash = promisify(asyncHash);

    module.exports = async buffer =>
        (await promiseHash('sha1', buffer)).toString('hex');
} catch (err) {
    const crypto = require('crypto');

    module.exports = buf => {
        const h = crypto.createHash('sha1');
        h.update(buf);
        return Promise.resolve(h.digest('hex'));
    };
}
