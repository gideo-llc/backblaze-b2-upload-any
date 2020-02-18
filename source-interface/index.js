const bufferSI = require('./buffer-source-interface');
const fileSI = require('./file-source-interface');
const streamSI = require('./stream-source-interface');

// Constructs a source object for the given upload options object.  This object
// must conform to one of two interfaces: the standard upload interface, or the
// large-file interface.  This interface allows the core upload functions to be
// indifferent about the source of the upload (buffer, file, or stream).
//
// The standard upload interface has the following attributes:
//
// * size: Function returning promise for the size of the upload, in bytes.
//
// * makeStream: Function returning readable stream that will produce the bytes
//   to upload.
//
// The large-file upload interface has the following attributes:
//
// * destroy: Function that will destroy any resources associated with the
//   source.  (This is called when the upload completes or fails.)
//
// * next: Function that returns a promise for an object that meets the
//   "large-file upload part interface" defined below, or undefined if there
//   are no more parts to upload.
//
// The large-file upload part interface has the following attributes:
//
// * number: Integer.  1-based index of this part.
//
// * hash: String.  Hex-encoded SHA-1 hash of the contents of this part.
//
// * obtain: Function returning either a buffer containing the contents of this
//   part, or a stream that will produce the contents.
//
// * destroy: Function that will destroy any resources used by the object
//   returned from obtain().

module.exports = o =>
    o.data instanceof Buffer ? bufferSI(o) :
    typeof o.data === 'string' ? fileSI(o) :
    streamSI(o);
