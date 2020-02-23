# @gideo-llc/backblaze-b2-upload-any

An intelligent upload function to be used with the [backblaze-b2](https://www.npmjs.com/package/backblaze-b2) module. This function can upload buffers, streams, and local files, and automatically uses B2's [large file upload](https://www.backblaze.com/b2/docs/large_files.html) mechanism when appropriate.

* **Upload token management:** Maintains a bucket of previously-used upload tokens that can be reused for future requests, allowing vastly-simplified parallel uploads without having to manually manage upload tokens.
* **Intelligent auto-retry:** If the destination pod is too busy to receive an upload, it will automatically be retried after obtaining a new upload token. Consecutive errors will result in exponential backoff. The `retry-after` HTTP response header is respected.
* **Automatic re-authorization:** A 401 error will cause automatic re-authorization. The interrupted upload will be transparently retried.
* **Stream support:** Uploaded files can be provided as streams.
* **File support:** Files on disk can be uploaded. This case has unique optimizations not possible with stream objects.
* **Automatic large-file uploads:** If uploaded data is larger than a specified threshold, it will be automatically uploaded in multiple pieces using B2's large file APIs. (When the data is provided as a stream, some sections of the stream may be buffered in memory. This is necessary to facilitate retries when a token expires.)

## Quick start

### Unintrusive

```js
const B2 = require('backblaze-b2');

B2.prototype.uploadAny = require('@gideo-llc/backblaze-b2-upload-any');

const b2 = new B2({
    applicationKeyId: '...',
    applicationKey: '...',
});

b2.authorize()
.then(r =>
    b2.uploadAny({
        bucketId: '...',
        fileName: 'foo',
        partSize: r.data.recommendedPartSize,
        data: /* buffer, stream, or file path */,
    })
)
.then(() => { console.log('Upload complete'); }, console.log);
```

### Intrusive

An `install` function is exported.  When called with the B2 constructor function, it will install two wrappers on its prototype:

* `authorize` is wrapped.  The recommended part size is extracted from the response and associated with the B2 client object.
* `uploadAny` is wrapped.  The default value of the options object's `partSize` attribute is set to the recommended part size that the `authorize` wrapper observed.

This simplifies correct usage of the `uploadAny` function as the recommended part size doesn't have to be passed around your application.

```js
const B2 = require('backblaze-b2');

require('@gideo-llc/backblaze-b2-upload-any').install(B2);

const b2 = new B2({
    applicationKeyId: '...',
    applicationKey: '...',
});

b2.authorize()
.then(() =>
    b2.uploadAny({
        bucketId: '...',
        fileName: 'foo',
        data: /* buffer, stream, or file path */,
    })
)
.then(() => { console.log('Upload complete'); }, console.log);
```

## Feature introduction

### Upload token management

B2 requires a separate upload URL and authorization token for each concurrent upload request; re-using a token that is currently being used by an outstanding request will return a 400 error. Additionally, a token may stop working at any time, which the server signals with a 503 error.

This function automatically handles these expected failure scenarios by taking any appropriate action and retrying the failed request.

Under the hood, there is a queue of upload URL/token pairs per bucket and per outstanding large-file upload. When a new upload request starts, a token is dequeued from this buffer (or a new token is acquired if the queue is empty). When the request successfully completes, the token used by that request is enqueued so that a future request may reuse it. If the upload request fails because the pod is too busy, the token is discarded and the request is retried after acquiring another token from the queue (or from B2 if the queue is empty).

With this mechanism, the pool of tokens will automatically grow as large as necessary to satisfy the concurrency needs of your application.

### Stream uploads

When uploading a file, the data can be provided as a stream. The usage of the upload function is exactly the same when using a stream.

Because upload requests frequently need to be retried due to a busy pod, some of the stream data may be buffered in memory. See the next section for details.

### Automatic large-file uploads

If an upload from any source (stream, buffer, or file) is larger than B2's recommended "large file" threshold, the client will automatically use the large file APIs to upload the content in pieces. This process is transparent to the caller.

If the source is a stream, note that parts of the large file upload must be held in memory to facilitate retries when a pod is too busy. (When uploading a buffer, no additional buffers will be allocated by this library; when uploading a file specified by a local path, the contents will be streamed directly to the server.)

The following attributes are used to control aspects of large file uploads:

* `concurrency`: The number of concurrent part uploads to perform. Defaults to 1.
* `largeFileThreshold`: The size (in bytes) required to use the large-file APIs. Files smaller than this are uploaded in a single `b2_upload_file` request.
* `partSize`: The size (in bytes) of each upload part. B2 permits the part size to be between 5MB and 5GB. Defaults to the recommended part size as returned by the B2 `authorize` API call. (Note that this value is the same as recommended in the B2 documentation -- currently 100MB. If you will be performing many concurrent stream uploads or have set `concurrency` quite high, consider specifying a smaller value to avoid exhausing the available memory in the JavaScript heap.)

Note that B2 only accepts up to 10,000 parts for a single file. If `contentLength / partSize > 10000` then the upload will eventually fail.

## API

This module exports a single function `uploadAny`, which must be called with a B2 object as the context. This function can be attached to the prototype of the `backblaze-b2` client.

### uploadAny(options)

Uploads a new file based on the specified options. `options` is an object with the following attributes:

* `bucketId`: String, required. The bucket ID to upload to.
* `concurrency`: Number. The number of concurrent part uploads that can be performed at once. Only used in large-file mode. Defaults to 1.
* `contentType`: String. The MIME type to record on the B2 object. Defaults to `b2/x-auto`.
* `data`: Buffer, readable stream, or string. Required.
  * If a buffer, the buffer's entire contents will be uploaded. Use the buffer's `.subarray()` method if a subsection of the buffer should be uploaded.
  * If a readable stream, all of the data produced the stream will be uploaded. The stream must not be in object mode.
  * If a string, the local file named by the string will be uploaded. (To upload the contents of a string, convert it to a buffer using the `Buffer.from(string, encoding)` function.)
* `fileName`: String, required. The name of the object to create in B2.
* `largeFileThreshold`: Number. The size in bytes at which to enable large-file mode. Must be greater than `partSize`. Defaults to `partSize * 2`.
* `partSize`: Number, required. The size of each part upload in bytes. Only used in large-file mode. Must be between 5,000,000 (5MB) and 5,000,000,000 (5GB).

Returns a promise for the JSON-decoded response body of either the `b2_upload_file` or `b2_finish_large_file` API call, depending on which mechanism is used.
