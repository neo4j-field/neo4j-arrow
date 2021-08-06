package org.neo4j.arrow;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

public class ArrowExtensionFactory extends ExtensionFactory<ArrowExtensionFactory.Dependencies> {

    public ArrowExtensionFactory() {
        super(ExtensionType.GLOBAL, "arrowExtension");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new ArrowCypherService(dependencies.dbms(), dependencies.logService());
    }

    public interface Dependencies {
        DatabaseManagementService dbms();
        LogService logService();
    }
}
