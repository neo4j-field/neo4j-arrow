package org.neo4j.arrow.auth;

import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.util.Optional;

// TODO: we need to hand out special tokens that map to drivers I think
public class ProxyAuthHandler
        implements BasicServerAuthHandler.BasicAuthValidator, BasicCallHeaderAuthenticator.CredentialValidator {


    @Override
    public byte[] getToken(String username, String password) throws Exception {
        return new byte[0];
    }

    @Override
    public Optional<String> isValid(byte[] token) {
        return Optional.empty();
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password) throws Exception {
        return null;
    }
}
