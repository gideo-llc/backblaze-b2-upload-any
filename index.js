const Joi = require('@hapi/joi');
const memoize = require('memoizee');
const pRetry = require('p-retry');
const { v4: uuidv4 } = require('uuid');

const hashStream = require('./streams/hash');
const httpsRequest = require('./https-client');
const pUtils = require('./promise-utils');
const safePipe = require('./safe-pipe');
const sourceInterface = require('./source-interface');
const { trace } = require('./log');

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
                        const detail = { err, ...err.logDetail };

                        if (token && (!err.config || err.config.url !== token.uploadUrl)) {
                            trace(detail, 'Error caught by a borrow function; unrelated to B2');

                            // The error doesn't seem related to B2 requests.
                            // Return the token and forward the error.
                            queue.push(token);
                            throw err;
                        }

                        const r = err.response || {};
                        const retryAfter = parseInt(r.headers && r.headers['retry-after']);

                        let retry = false;
                        let discard = false;

                        if (r.status === 401) {
                            trace(detail, 'Error caught by a borrow function; reauthenticating');

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

                        trace({ ...detail, retry, discard }, 'Error caught by a borrow function');

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
    trace(o.logDetail, 'Using standard upload');

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
            return await httpsRequest(
                token.uploadUrl,
                {
                    method: 'POST',
                    headers: {
                        authorization: token.authorizationToken,
                        'x-bz-file-name': o.fileName,
                        'content-type': o.contentType,
                        'content-length': size,
                        'x-bz-content-sha1': hash,
                    },
                    timeout: 15 * 1000,
                },
                data
            );

            trace(o.logDetail, 'Standard upload completed');
        } catch (err) {
            err.logDetail = o.logDetail;
            throw err;
        } finally {
            data.destroy();
        }
    });
}

async function doLargeUpload(o, si) {
    trace(o.logDetail, 'Using large upload');

    const fileId = (
        await o.self.startLargeFile({
            bucketId: o.bucketId,
            fileName: o.fileName,
            contentType: o.contentType,
        })
    ).data.fileId;

    o.logDetail.fileId = fileId;

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

            function doPart(part) {
                const detail = { ...o.logDetail, partNumber: part.number };

                trace(detail, 'Beginning part');

                return borrow(async token => {
                    try {
                        trace(detail, 'Obtaining part data');

                        const data = await part.obtain();

                        try {
                            trace(detail, 'Attempting part upload');

                            await httpsRequest(
                                token.uploadUrl,
                                {
                                    method: 'POST',
                                    headers: {
                                        authorization: token.authorizationToken,
                                        'x-bz-part-number': part.number,
                                        'content-length': data.byteLength || data.length,
                                        'x-bz-content-sha1': part.hash,
                                    },
                                    timeout: 15 * 1000,
                                },
                                data
                            );
                        } catch (err) {
                            part.destroy(data);
                            throw err;
                        }
                    } catch (err) {
                        err.logDetail = detail;
                        throw err;
                    }

                    trace(detail, 'Finished part');

                    return part.hash;
                })
                .catch(fail);
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
                                .catch(err => {
                                    // Fast-fail out of the whole process if
                                    // possible.  Propagate the error for
                                    // Promise.all() later.
                                    fail(err);
                                    throw err;
                                })
                            );

                            // Since we got a part, start another worker if one
                            // is available.
                            startWorker();
                        } else {
                            trace(o.logDetail, 'No more parts');

                            // There are no more parts. Setting available to
                            // NaN makes "available += 1" a no-op and
                            // "available > 0" always false, guaranteeing no
                            // more workers will start.
                            available = NaN;

                            // Resolve our promise as the array of hashes we
                            // need to complete the upload.  Note that we don't
                            // call resolve() right now because it's still
                            // possible for fail() to be called.  Once
                            // resolve() is called, reject() is a no-op which
                            // could cause errors to not only be ignored, but
                            // prevent the outer promise from being settled.
                            Promise.all(hashes).then(resolve, fail);
                        }
                    } catch (err) {
                        fail(err);
                    }
                }
            }

            // Kick off the process.
            startWorker();
        });

        trace(o.logDetail, 'Finishing large file');

        // "return await" so we can catch any errors.
        return await o.self.finishLargeFile({
            fileId,
            partSha1Array: partHashes,
        });

        trace(o.logDetail, 'Large upload complete');
    } catch (err) {
        trace({ ...o.logDetail, err }, 'Large upload failed; canceling large file');

        await o.self.cancelLargeFile({ fileId });
        throw err;
    }
}

async function upload(options) {
    const o = Joi.attempt(options, uploadOptionsSchema);

    o.self = this;
    o.logDetail = {
        correlationId: uuidv4(),
        fileName: o.fileName,
        bucketId: o.bucketId,
    };

    trace(o.logDetail, 'Beginning upload');

    const si = await sourceInterface(o);

    return si.next ? doLargeUpload(o, si) : doStandardUpload(o, si);
}

upload.install = function install(B2) {
    if (!B2.prototype.uploadAny) {
        const _authorize = B2.prototype.authorize;

        B2.prototype.authorize = function authorize() {
            return _authorize.apply(this, arguments)
            .then(r => {
                getPriv(this).partSize = r && r.data && r.data.recommendedPartSize || undefined;
                return r;
            });
        };

        B2.prototype.uploadAny = function (options) {
            return upload.call(this, {
                partSize: getPriv(this).partSize,
                ...options
            });
        };
    }

    return B2;
};

module.exports = upload;
