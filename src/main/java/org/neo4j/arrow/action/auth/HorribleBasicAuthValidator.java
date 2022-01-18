package org.neo4j.arrow.action.auth;

import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

/**
 * Seriously, try not to use this. It's an auth validator that uses an in-memory (on jvm heap) token
 * containing a username and password for authenticating with the Arrow Flight service.
 * <p>
 *     <em>THIS SHOULD BE USED FOR TESTING OR LOCAL HACKING ONLY!</em>
 * </p>
 */
public class HorribleBasicAuthValidator
        implements BasicServerAuthHandler.BasicAuthValidator, BasicCallHeaderAuthenticator.CredentialValidator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HorribleBasicAuthValidator.class);

    // XXX you really shouldn't be using this auth validator! It simply keeps an in-memory token.
    /** The hardcoded auth token in the form of username:password. Set via environment variable. */
    private static final String HARDCODED_TOKEN = System.getenv()
            .getOrDefault("NEO4J_ARROW_TOKEN", "neo4j:password");

    @Override
    public byte[] getToken(String username, String password) {
        logger.debug("getToken called: username={}, password={}", username, password);
        final String token = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        logger.debug("token = {}", token);
        return token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Optional<String> isValid(byte[] token) {
        logger.debug("isValid called: token={}", token);

        // TODO: make an auth handler that isn't this silly
        if (Arrays.equals(HARDCODED_TOKEN.getBytes(StandardCharsets.UTF_8), token))
            return Optional.of("neo4j");
        else
            return Optional.empty();
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
        logger.debug("validate called for username={}", username);
        if (username.equals("neo4j") && password.equals("password"))
            return () -> username;
        else
            throw new Exception("Oh, fiddlesticks");
    }
}
