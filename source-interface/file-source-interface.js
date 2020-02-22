const fs = require('fs');

const hashStream = require('../streams/hash');
const pUtils = require('../promise-utils');

const stat = pUtils.promisify(fs.stat);

// Obtains the size of a local file.  Throws on any non-file (uploading a
// directory, device node, etc. doesn't make sense).  Note that fs.stat()
// follows symlinks, so symlinks to files are acceptable.
async function getFileSize(path) {
    const s = await stat(path);

    if (!s.isFile()) {
        throw new Error(`File is not a regular file: ${path}`);
    }

    return s.size;
}

function fileLargeInterface(o) {
    let piece = 0;

    const detail = getFileSize(o.data).then(size => ({
        size,
        totalParts: Math.ceil(size / o.partSize),
    }));

    return {
        async next() {
            const { size, totalParts } = await detail;

            if (piece >= totalParts) {
                return undefined;
            }

            // Local copy of the piece we are working on.
            let p = piece;

            piece += 1;

            function openFile() {
                const start = p * o.partSize;
                const end = Math.min(start + o.partSize, size);

                const s = fs.createReadStream(o.data, {
                    start,
                    end: end - 1, // - 1 because end is inclusive
                });

                // The B2 client fetches the data length from the data object,
                // so we need to populate that.
                s.byteLength = end - start;

                return s;
            }

            const hash = new Promise((resolve, reject) => {
                const source = openFile();

                source.on('error', err => {
                    reject(err);
                    source.destroy();
                })
                .pipe(hashStream('sha1'))
                .on('hash', hash => {
                    resolve(hash.toString('hex'));
                })
                .resume();
            });

            return {
                // B2 pieces are 1-based
                number: p + 1,

                hash: await hash,

                obtain: openFile,

                destroy(o) {
                    o.destroy();
                },
            };
        },

        // No-op for this producer type.
        destroy() { },
    };
}

module.exports = async o =>
    (await getFileSize(o.data)) >= o.largeFileThreshold ? fileLargeInterface(o) :
    {
        size: () => getFileSize(o.data),
        makeStream: () => fs.createReadStream(o.data),
    };
