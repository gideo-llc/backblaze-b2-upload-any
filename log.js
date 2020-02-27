// A bunyan logger can be attached as the logger attribute of this module to
// enable logging.

const self = module.exports = {
    trace(...args) { return self.logger.trace(...args); },

    logger: { trace() {}, },
};
