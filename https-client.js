// In our tests, B2 frequently hangs up and doesn't send or receive data.
// Unfortunately, axios does not give us a good way to detect this or react to
// it, so for some calls we will directly use node's http client with a socket
// timeout set.
const https = require('https');

module.exports = (url, options, body) => {
    return new Promise((resolve, reject) => {
        let axiosResponse;

        const request = https.request(url, options, response => {
            axiosResponse = {
                status: response.statusCode,
                headers: response.headers,
            };

            response.setEncoding('utf8');
            let buf = '';

            response.on('error', fail);
            response.on('data', chunk => { buf += chunk; });
            response.on('end', () => {
                const success = response.statusCode >= 200 && response.statusCode < 400;

                let data;
                try {
                    data = JSON.parse(buf);
                } catch (err) {
                    if (success) {
                        // If the request was otherwise successful but we can't
                        // parse the body, fail with the JSON parse error.
                        fail(err);
                    }
                }

                if (success) {
                    resolve(data);
                } else {
                    axiosResponse.data = data;
                    fail(new Error(`Request failed with status code ${response.statusCode}`));
                }
            });
        });

        function fail(err) {
            // On failure we need to simulate enough of the axios error
            // structure for our borrow() function to know what to do.
            reject(Object.assign(err, {
                config: {
                    url,
                    method: options.method,
                },

                response: axiosResponse,
            }));

            request.aborted || request.abort();
        }

        request.on('error', fail);
        request.on('timeout', () => {
            fail(
                Object.assign(
                    new Error('Request timed out'),
                    { code: 'ETIMEDOUT' }
                )
            );
        });

        if (body) {
            if (typeof body === 'string') {
                request.write(body, 'utf8');
            } else if (Buffer.isBuffer(body)) {
                request.write(body);
            } else if (typeof body.read === 'function') {
                body.on('error', fail)
                .pipe(request);
                return; // skip ending the request
            }
        }

        request.end();
    });
};
