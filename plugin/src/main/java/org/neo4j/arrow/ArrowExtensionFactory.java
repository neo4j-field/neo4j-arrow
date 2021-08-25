package org.neo4j.arrow;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

public class ArrowExtensionFactory extends ExtensionFactory<ArrowExtensionFactory.Dependencies> {

    static {
        // XXX Neo4j's "Logger" is annoying me...need to figure out how to properly hook in Slf4j
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    }

    public ArrowExtensionFactory() {
        super(ExtensionType.GLOBAL, "arrowExtension");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new ArrowService(dependencies.dbms(), dependencies.logService());
    }

    public interface Dependencies {
        DatabaseManagementService dbms();
        LogService logService();
    }
}
