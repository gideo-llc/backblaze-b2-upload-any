const stream = require('stream');

// Simple readable stream that provides an array of buffers as a stream.
module.exports = buffers => {
    let i = 0;

    return new stream.Readable({
        read(size) {
            while (i < buffers.length && this.push(buffers[i++])) { }

            if (i >= buffers.length) {
                this.push(null);
            }
        },
    });
};
