const Joi = require('@hapi/joi');
const memoize = require('memoizee');
const pRetry = require('p-retry');

const hashStream = require('./streams/hash');
const pUtils = require('./promise-utils');
const safePipe = require('./safe-pipe');
const sourceInterface = require('./source-interface');

// We store our private data on the client object itself, using a symbol to
// ensure no future attribute key conflicts.
const privSymbol = Symbol('upload-any-data');

function getPriv(o) {
    if (!o[privSymbol]) {
        const reauth = memoize(
            () => o.authorize(),
            { promise: true, maxAge: 1000 * 60 * 10 /* 10 minutes */ }
        );

        // Generic "token bucket" factory.  The argument is a function that
        // will be invoked to request a new token.  Returns a "borrow" function
        // that accepts a worker function.
        //
        // Works with both standard uploads (bucket tokens) and large-file
        // uploads (large-file tokens).
        function createBorrowFn(getTokenFn) {
            const queue = [];

            // The borrow function.  Invokes the argument with a token,
            // possibly multiple times if an error is thrown.
            return workerFn => {
                async function attempt() {
                    let token;

                    try {
                        token = queue.shift() || await getTokenFn();

                        const result = await workerFn(token);
                        queue.push(token);

                        return result;
                    } catch (err) {
                        if (token && (!err.config || err.config.url !== token.uploadUrl)) {
                            // The error doesn't seem related to B2 requests.
                            // Return the token and forward the error.
                            queue.push(token);
                            throw err;
                        }

                        const r = err.response || {};
                        const retryAfter = parseInt(r.headers && r.headers['retry-after']);

                        let retry;
                        let discard;

                        if (r.status === 401) {
                            // Reauthenticate and immediately recurse; this
                            // shouldn't "count" as a retry since it is an
                            // obvious error with a known fix.
                            await reauth();
                            return attempt();
                        } else if (r.status === 503 || err.code === 'ECONNREFUSED' || err.code === 'ETIMEDOUT') {
                            // The token expired or we couldn't reach the
                            // server.
                            retry = true;
                            discard = true;
                        } else if (r.status === 400 && r.data && r.data.message && r.data.message.startsWith('more than one upload using auth token')) {
                            // The server thinks there is an outstanding
                            // request using this same token. Likely a prior
                            // request already failed with ETIMEDOUT or
                            // ECONNRESET and we noticed before the server.
                            // This token might become valid again later, but
                            // if the token queue is currently empty we'll just
                            // wind up retrying with the same token. Therefore,
                            // discard it.
                            retry = true;
                            discard = true;
                        } else if (err.code === 'ECONNRESET' || err.code === 'EAI_AGAIN' || r.status === 500 || r.status === 429) {
                            // Retry in these scenarios but keep the token.
                            retry = true;
                        }

                        if (!discard) {
                            queue.push(token);
                        }

                        if (!retry) {
                            throw new pRetry.AbortError(err);
                        }

                        if (Number.isFinite(retryAfter)) {
                            // If retry-after was specified we delay and
                            // recurse ourselves.  This doesn't consume one of
                            // the p-retry retries, but we have no way to
                            // communicate the requested delay to p-retry so we
                            // have no choice.
                            await pUtils.delay(retryAfter);
                            return attempt();
                        }

                        throw err;
                    }
                }

                return pRetry(attempt, {
                    retries: 9, // 10 tries overall
                    maxTimeout: 30 * 1000,
                });
            };
        }

        // Memoized function to get a borrow function for a specific bucket ID.
        // Memoization allows multiple concurrent standard uploads into the
        // same bucket to share tokens.
        const getBucketBorrowFn = memoize(
            bucketId => createBorrowFn(
                async () => (await o.getUploadUrl({ bucketId })).data
            ),

            { primitive: true }
        );

        o[privSymbol] = {
            borrowBucketUploadToken(bucketId, workerFn) {
                return getBucketBorrowFn(bucketId)(workerFn);
            },

            createLargeFileBorrowFn(fileId) {
                return createBorrowFn(
                    async () => (await o.getUploadPartUrl({ fileId })).data
                );
            },
        };
    }

    return o[privSymbol];
}

// Define the schema for our upload options object.
const uploadOptionsSchema = Joi.object().required().keys({
    bucketId: Joi.string().required(),
    concurrency: Joi.number().integer().min(1).default(1),
    contentType: Joi.string().default('b2/x-auto'),
    data: Joi.alternatives().required().try(
        Joi.string().required(),
        Joi.object().required().instance(Buffer),
        Joi.object().required().unknown(true).raw().keys({
            pipe: Joi.function().required(),
        }),
    ),
    fileName: Joi.string().required(),
    largeFileThreshold: Joi.number().integer()
        .min(    Joi.ref('partSize', { adjust: v => v + 1 }))
        .default(Joi.ref('partSize', { adjust: v => v * 2 })),
    partSize: Joi.number().integer().required()
        .min(   5000000)  // 5MB
        .max(5000000000), // 5GB
});

async function doStandardUpload(o, si) {
    // TODO: If input is a file, hashes it first. Should hash on-the-fly and
    // append the hash.
    const [ hash, size ] = await Promise.all([
        new Promise((resolve, reject) => {
            safePipe(si.makeStream(), hashStream('sha1'))
                .on('error', reject)
                .on('hash', h => resolve(h.toString('hex')))
                .resume();
        }),

        si.size(),
    ]);

    return getPriv(o.self).borrowBucketUploadToken(o.bucketId, async token => {
        const data = si.makeStream();

        try {
            return await o.self.uploadFile({
                uploadUrl: token.uploadUrl,
                uploadAuthToken: token.authorizationToken,
                fileName: o.fileName,
                mime: o.contentType,
                hash,
                contentLength: size,
                data,
            });
        } finally {
            data.destroy();
        }
    });
}

async function doLargeUpload(o, si) {
    const fileId = (
        await o.self.startLargeFile({
            bucketId: o.bucketId,
            fileName: o.fileName,
        })
    ).data.fileId;

    const borrow = getPriv(o.self).createLargeFileBorrowFn(fileId);

    try {
        const partHashes = await new Promise((resolve, reject) => {
            // The number of "available workers" -- how many new concurrent
            // upload tasks we can start.
            let available = o.concurrency;

            const hashes = [];

            function fail(err) {
                si.destroy();
                reject(err);
            }

            async function doPart(part) {
                const data = await part.obtain();

                try {
                    await borrow(token =>
                        o.self.uploadPart({
                            partNumber: part.number,
                            uploadUrl: token.uploadUrl,
                            uploadAuthToken: token.authorizationToken,
                            hash: part.hash,
                            data,
                        })
                    );

                    return part.hash;
                } catch (err) {
                    part.destroy(data);
                    throw err;
                }
            }

            // I'm sure there's a better way to do this...
            async function startWorker() {
                if (available > 0) {
                    available -= 1;

                    // Ask for the next part. Note that we don't start another
                    // worker until we actually have a part; if there are no
                    // more parts, this simplifies the termination logic.
                    try {
                        const part = await si.next();

                        if (part) {
                            // doPart() returns a promise for the SHA1 hash of
                            // the part. Add it to the hashes array.
                            hashes.push(
                                doPart(part)
                                .then(v => {
                                    // On completion, return a token to the
                                    // bucket and try to start another worker.
                                    available += 1;
                                    startWorker();
                                    return v;
                                })
                                .catch(fail)
                            );

                            // Since we got a part, start another worker if one
                            // is available.
                            startWorker();
                        } else {
                            // There are no more parts. Setting available to
                            // NaN makes "available += 1" a no-op and
                            // "available > 0" always false, guaranteeing no
                            // more workers will start.
                            available = NaN;

                            // Resolve our promise as the array of hashes we
                            // need to complete the upload.
                            resolve(Promise.all(hashes));
                        }
                    } catch (err) {
                        fail(err);
                    }
                }
            }

            // Kick off the process.
            startWorker();
        });

        // "return await" so we can catch any errors.
        return await o.self.finishLargeFile({
            fileId,
            partSha1Array: partHashes,
        });
    } catch (err) {
        await o.self.cancelLargeFile({ fileId });
        throw err;
    }
}

async function upload(options) {
    const o = Joi.attempt(options, uploadOptionsSchema);

    o.self = this;

    const si = await sourceInterface(o);

    return si.next ? doLargeUpload(o, si) : doStandardUpload(o, si);
}

module.exports = upload;
