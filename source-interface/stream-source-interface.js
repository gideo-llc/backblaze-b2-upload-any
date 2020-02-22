const stream = require('stream');

const bufferStream = require('../streams/buffer');
const concatStream = require('../streams/concat');
const hashBuffer = require('../hash-buffer');
const safePipe = require('../safe-pipe');
const syncPromise = require('../sync-promise');

function streamLargeInterface(o, readBuffer) {
    let piece = 1;
    let done = false;

    const source = safePipe(
        // Combine the buffers we already read while probing the stream's size
        // with the rest of the stream.
        concatStream([
            bufferStream(readBuffer),
            o.data
        ]),

        // For optimal throughput, we want to buffer one extra part.
        //
        // If we don't do this, we observe a ping-ponging between N and N-1
        // uploading workers (N=concurrency).  When all workers are uploading,
        // the source stream ceases being read with only a minimal amount of
        // data buffered.  As soon as a worker requests another piece, the
        // source stream starts flowing again.  If the source stream is not
        // extremely fast, the available worker will have to sit and do nothing
        // while waiting for the next piece to be read.
        //
        // Effectively, while all workers are uploading, the source stream is
        // sitting idle and we waste time we could use reading another piece.
        // While not all workers are uploading, we are wasting upload
        // throughput while we wait for the source stream.
        //
        // To solve this problem, we pipe through a pass-through stream with a
        // calculated high watermark: the current part size, less the amount of
        // buffering the source stream will do by itself.
        //
        // If there is backpressure even with this buffering, then the upload
        // workers cannot keep up with the source stream.  Either the outbound
        // network is the bottleneck, or the number of upload workers should be
        // increased (free RAM permitting).
        //
        // Enforce a minimum of 1MB.
        new stream.PassThrough({
            readableHighWaterMark: Math.max(1000000, o.partSize - o.data.readableHighWaterMark),
        })
    );

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
