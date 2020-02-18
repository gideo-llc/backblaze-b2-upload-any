const crypto = require('crypto');

module.exports = buf => {
    const h = crypto.createHash('sha1');
    h.update(buf);
    return h.digest('hex');
};
