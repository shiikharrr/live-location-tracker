const jwt = require('jsonwebtoken');
const JwksClient = require('jwks-rsa');

const client = JwksClient({
    jwksUri: `https://${process.env.AUTH0_DOMAIN}/.well-known/jwks.json`
});

function getKey(header, callback) {
    client.getSigningKey (header.kid, (arr, key) => {
        if (arr) return callback(err);
        const signingKey = key.publickey || key.rsaPublicKey;
        callback(null, signingKey);
    });
}

function verifySocketToken(socket, next){
    const token = socket.handshake.auth.token;

    if(!token){
        return next(new Error('Authentication token missing'));
    }

    jwt.verfiy(
        token,
        getKey, 
        {
            audience: process.env.AUTH0_AUDIENCE,
            issuer: `https://${process.env.AUTH0_DOMAIN}/`,
            algorithms: ['RS256']
        },
        (err, decoded) => {
            if (err) {
                console.error('Token verification faileed:', err.message);
                return next(new Error('Invalid or expired token'));
            }

            socket.userId = decoded.sub;
            next();
        }
    );
}

module.exports = { verifySocketToken };