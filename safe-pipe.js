// Pipes multiple streams and propagates errors emitted from prior streams
// through the rest of the streams.  Returns the final stream.
module.exports = (...streams) =>
    streams.reduce((left, right) =>
        left.on('error', right.destroy.bind(right))
        .pipe(right)
    );
