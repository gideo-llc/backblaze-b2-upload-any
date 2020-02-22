const bufferStream = require('../streams/buffer');
const hashBuffer = require('../hash-buffer');

function bufferLargeInterface(o) {
    let piece = 0;

    const total = Math.ceil(o.data.length / o.partSize);

    return {
        async next() {
            if (piece >= total) {
                return Promise.resolve(undefined);
            }

            const data = o.data.subarray(
                piece * o.partSize,
                (piece + 1) * o.partSize /* end is exclusive */
            );

            piece += 1;

            return Promise.resolve({
                number: piece,

                hash: await hashBuffer(data),

                obtain: () => data,

                // No-op for this producer type.
                destroy() { },
            });
        },

        // No-op for this producer type.
        destroy() { },
    };
}

module.exports = o =>
    o.data.length >= o.largeFileThreshold ? bufferLargeInterface(o) :
    Promise.resolve({
        size: () => Promise.resolve(o.data.length),
        makeStream: () => bufferStream([o.data]),
    });
