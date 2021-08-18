package org.neo4j.arrow.auth;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.security.AuthManager;

public class NativeAuthValidatorTest {
    @Test
    public void testTokenLifecycle() throws Exception {
        final String username = "dave";
        final String password = "so it goes!";

        final NativeAuthValidator validator = new NativeAuthValidator(() -> AuthManager.NO_AUTH, null);
        final byte[] out = validator.getToken(username, password);
        final String newUsername = validator.isValid(out).get();

        Assertions.assertEquals(username, newUsername, "usernames should match");
    }
}
