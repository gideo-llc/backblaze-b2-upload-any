const bufferStream = require('../streams/buffer');
const hashBuffer = require('../hash-buffer');

function bufferLargeInterface(o) {
    let piece = 0;

    const total = Math.ceil(o.data.length / o.partSize);

    return {
        async next() {
            if (piece >= total) {
                return undefined;
            }

            const data = o.data.subarray(
                piece * o.partSize,
                (piece + 1) * o.partSize /* end is exclusive */
            );

            piece += 1;

            return {
                number: piece,

                hash: hashBuffer(data),

                obtain: () => data,

                // No-op for this producer type.
                destroy() { },
            };
        },

        // No-op for this producer type.
        destroy() { },
    };
}

module.exports = async o =>
    o.data.length >= o.largeFileThreshold ? bufferLargeInterface(o) :
    {
        size: async () => o.data.length,
        makeStream: () => bufferStream([o.data]),
    };
