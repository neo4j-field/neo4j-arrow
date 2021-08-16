package org.neo4j.arrow.auth;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;

public class ArrowConnectionInfo extends ClientConnectionInfo {
    @Override
    public String asConnectionDetails() {
        return "arrow";
    }

    @Override
    public String protocol() {
        return "arrow";
    }

    @Override
    public String connectionId() {
        return "arrow-tbd";
    }
}
