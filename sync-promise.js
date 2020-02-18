function ignore() {}

// A higher order function that enforces only one outstanding asynchronous call
// to the argument can be made. Any call to the returned function will be
// effectively delayed until all prior calls have been made and their returned
// promises settled.
module.exports = fn => {
    let prior = Promise.resolve();

    return (...args) => {
        const result = prior.then(() => fn(...args));

        // Set up the promise we will wait for on the next call.  Note that we
        // ignore rejections as well as discarding the fulfilled value.
        //
        // Not ignoring the fulfilled value could hold onto a large result
        // object longer than necessary, when we only care about whether the
        // async operation is complete.
        //
        // We ignore the rejection because otherwise all future calls to this
        // function will be rejected with the same reason, which is bad.  Note
        // that we do not ignore rejections on the promise we return from the
        // function, so rejections can still be observed by the caller.
        //
        // tl;dr: The "prior" promise is only used to determine when to run the
        // next invocation.  We do not care what happened on the prior
        // invocation, only that the promise it returned has become settled.
        prior = result.then(ignore, ignore);

        return result;
    };
};
