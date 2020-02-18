const crypto = require('crypto');
const stream = require('stream');

// Pass-through stream that emits a 'hash' event after end() is called.  The
// event argument is the digest of all of the content to pass through the
// stream using the specified algorithm.
module.exports = algo => {
    const h = crypto.createHash(algo);

    return stream.Transform({
        transform(chunk, encoding, cb) {
            h.update(chunk, encoding);
            this.push(chunk, encoding);

            cb();
        },

        flush(cb) {
            this.emit('hash', h.digest());
            cb();
        },
    });
};
