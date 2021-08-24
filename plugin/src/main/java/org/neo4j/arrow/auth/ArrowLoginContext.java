package org.neo4j.arrow.auth;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;

public class ArrowLoginContext extends LoginContext {
    public ArrowLoginContext(AuthSubject subject, ClientConnectionInfo connectionInfo) {
        super(subject, connectionInfo);
    }

    @Override
    public SecurityContext authorize(IdLookup idLookup, String dbName, AbstractSecurityLog securityLog) {
        return null;
    }
}
