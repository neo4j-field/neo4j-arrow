package org.neo4j.arrow.auth;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 *  A simple wrapper around using a Neo4j AuthManager for authenticating a client.
 *  !!!Does not check Authorization!!!
 */
public class NativeAuthValidator
        implements BasicServerAuthHandler.BasicAuthValidator, BasicCallHeaderAuthenticator.CredentialValidator {

    private final Supplier<AuthManager> authManager;
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Base64.Decoder decoder = Base64.getDecoder();

    public NativeAuthValidator(Supplier<AuthManager> authManager) {
        this.authManager = authManager;
    }

    @Override
    public byte[] getToken(String username, String password) throws Exception {
        final String token =
                encoder.encodeToString(username.getBytes(StandardCharsets.UTF_8)) +
                "." +
                encoder.encodeToString(password.getBytes(StandardCharsets.UTF_8));

        return token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Optional<String> isValid(byte[] token) {
        final String in = new String(token, StandardCharsets.UTF_8);

        final String[] parts = in.split("\\.");
        assert parts.length == 2;

        final String username = new String(decoder.decode(parts[0]), StandardCharsets.UTF_8);
        final String password = new String(decoder.decode(parts[1]), StandardCharsets.UTF_8);

        try {
            // XXX assume no exception means success...highly suspect
            validate(username, password);
        } catch (Exception e) {
            return Optional.empty();
        }

        return Optional.of(username);
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
        final Map<String, Object> token = AuthToken.newBasicAuthToken(username, password);
        final LoginContext context = authManager.get().login(token, new ArrowConnectionInfo());

        switch (context.subject().getAuthenticationResult()) {
            case SUCCESS:
                return () -> username;
            default:
                throw CallStatus.UNAUTHENTICATED.withDescription("invalid username or password").toRuntimeException();
        }
    }
}
