const stream = require('stream');

// Concatenate multiple streams. Each stream will be fully read, in order.
module.exports = streams => {
    const o = new stream.PassThrough();
    let i = 0;

    function nextStream() {
        const c = streams[i++];

        if (!c) {
            o.end();
            return;
        }

        c.on('error', err => { o.emit('error', err); });
        c.on('end', nextStream);
        c.pipe(o, { end: false });
    }

    nextStream();

    return o;
};
