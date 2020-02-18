// A few promise helpers since we don't depend on bluebird.
function fromCallback(fn) {
    return new Promise((resolve, reject) => {
        fn((err, value) => {
            err ? reject(err) : resolve(value);
        });
    });
}

function promisify(fn) {
    return (...args) => fromCallback(cb => fn(...args, cb));
}

function delay(ms) {
    return new Promise(resolve => { setTimeout(resolve, ms); });
}

module.exports = {
    fromCallback,
    promisify,
    delay,
};
