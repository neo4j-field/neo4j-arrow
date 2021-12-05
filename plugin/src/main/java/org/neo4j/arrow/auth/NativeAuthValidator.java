package org.neo4j.arrow.auth;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.logging.Log;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 *  A simple wrapper around using a Neo4j AuthManager for authenticating a client.
 *  <p>
 *  <i>Note: this does not check Authorization...it only Authenticates identities</i>>
 */
public class NativeAuthValidator
        implements BasicServerAuthHandler.BasicAuthValidator, BasicCallHeaderAuthenticator.CredentialValidator {

    private final Supplier<AuthManager> authManager;
    private final Log log;

    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Base64.Decoder decoder = Base64.getDecoder();

    public static ConcurrentHashMap<String, LoginContext> contextMap = new ConcurrentHashMap<>();

    public NativeAuthValidator(Supplier<AuthManager> authManager, Log log) {
        this.authManager = authManager;
        this.log = log;
    }

    /**
     * Generate a token representation of an identity and secret (password) as a byte array.
     * <p>
     * The token is in the form of the username, base64 encoded, a <pre>.</pre> character, and the
     * password, base64 encoded. (All in UTF-8 encoding.)
     * @param username identity of the client
     * @param password secret token or password provided by the client
     * @return new byte[] containing the token
     */
    @Override
    public byte[] getToken(String username, String password) {
        final String token =
                encoder.encodeToString(username.getBytes(StandardCharsets.UTF_8)) +
                "." +
                encoder.encodeToString(password.getBytes(StandardCharsets.UTF_8));

        return token.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Validate the given token.
     * <p>
     * Parses the given token byte array, assuming the format as defined by
     * {@link #getToken(String, String)}, and validates the password is correct for the username
     * contained within the token.
     *
     * @param token encoded identity and secret
     * @return Optional containing the username if valid, empty if not.
     */
    @Override
    public Optional<String> isValid(byte[] token) {
        final String in = new String(token, StandardCharsets.UTF_8);

        final String[] parts = in.split("\\.");
        assert parts.length == 2;

        final String username = new String(decoder.decode(parts[0]), StandardCharsets.UTF_8);
        final String password = new String(decoder.decode(parts[1]), StandardCharsets.UTF_8);

        try {
            // XXX Assumes no exception means success...highly suspect!!!
            validate(username, password);
        } catch (Exception e) {
            return Optional.empty();
        }

        return Optional.of(username);
    }

    /**
     * Validate the given username and password for the calling client by using the Neo4j
     * {@link #authManager} instance. (Does not support realms!)
     *
     * @param username identity of the client
     * @param password secret token or password provided by the client
     * @return an AuthResult that resolves to the username of the caller
     * @throws InvalidAuthTokenException if the basic auth token is malformed
     * @throws RuntimeException if the AuthManager service is unavailable or the identity cannot be
     * confirmed
     */
    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password)
            throws RuntimeException, InvalidAuthTokenException {
        final Map<String, Object> token = AuthToken.newBasicAuthToken(username, password);
        final AuthManager am = authManager.get();

        if (am == null) {
            log.error("Cannot get AuthManager from supplier!");
            throw CallStatus.INTERNAL.withDescription("authentication system unavailable").toRuntimeException();
        }
        final LoginContext context = am.login(token, new ArrowConnectionInfo());

        if (context.subject().getAuthenticationResult() == AuthenticationResult.SUCCESS) {
            // XXX: this is HORRIBLE /facepalm
            contextMap.put(context.subject().authenticatedUser(), context);
            return () -> context.subject().authenticatedUser();
        }
        throw CallStatus.UNAUTHENTICATED.withDescription("invalid username or password").toRuntimeException();
    }
}
