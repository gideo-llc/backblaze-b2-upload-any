const stream = require('stream');

const bufferStream = require('../streams/buffer');
const concatStream = require('../streams/concat');
const hashBuffer = require('../hash-buffer');
const syncPromise = require('../sync-promise');

function streamLargeInterface(o, readBuffer) {
    let piece = 1;
    let done = false;

    // Combine the buffers we already read while probing the stream's size with
    // the rest of the stream.
    const source = concatStream([
        bufferStream(readBuffer),
        o.data
    ]);

    return {
        // We use syncPromise because this function is not safe to invoke
        // concurrently; otherwise the contents of the stream could be
        // duplicated or assembled in the wrong order.  This shouldn't happen
        // anyway, but we guard against it to be safe.
        next: syncPromise(() =>
            new Promise((resolve, reject) => {
                if (done) {
                    resolve(undefined);
                    return;
                }

                const number = piece;
                piece += 1;

                const buf = Buffer.allocUnsafe(o.partSize);
                let pos = 0;

                const reader = new stream.Writable({
                    write(chunk, encoding, cb) {
                        chunk.copy(buf, pos);
                        pos += chunk.length;

                        if (pos >= buf.length) {
                            // We read a full chunk, deliver it.
                            deliver(buf);

                            if (pos > buf.length) {
                                // We read too much, put some back for next time.
                                const overflow = pos - buf.length;
                                source.unshift(chunk.subarray(chunk.length - overflow));
                            }
                        }

                        cb();
                    },

                    final(cb) {
                        // The stream ended. If we read anything, deliver it.
                        // Otherwise signal that we're done by resolving as
                        // undefined.
                        deliver(pos !== 0 ? buf.subarray(0, pos) : undefined);
                        done = true;

                        cb();
                    },
                });

                function deliver(result) {
                    source.off('error', reject);
                    source.unpipe(reader);

                    resolve(
                        result &&

                        hashBuffer(result)
                        .then(hash => ({
                            number,

                            hash,

                            obtain: () => result,

                            destroy() { },
                        }))
                    );
                }

                source.on('error', reject);
                source.pipe(reader);
            })
        ),

        destroy() {
            o.data.destroy();
        },
    };
}

module.exports = async o => {
    const s = o.data;

    const readBuffer = [];
    let readBufferSize = 0;

    const large = await new Promise((resolve, reject) => {
        s.on('error', reject);

        const processor = new stream.Writable({
            write(chunk, encoding, cb) {
                readBuffer.push(chunk);
                readBufferSize += chunk.length;

                if (readBufferSize >= o.largeFileThreshold) {
                    finished();
                }

                cb();
            },

            final(cb) {
                finished();
                cb();
            },
        });

        function finished() {
            s.off('error', reject);
            s.unpipe(processor);

            resolve(readBufferSize >= o.largeFileThreshold);
        }

        s.pipe(processor);
    });

    return large ? streamLargeInterface(o, readBuffer) :
    {
        size: () => Promise.resolve(readBufferSize),
        makeStream: () => bufferStream(readBuffer),
    };
};
