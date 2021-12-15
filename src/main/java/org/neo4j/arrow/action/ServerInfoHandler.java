package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.neo4j.arrow.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;

public class ServerInfoHandler implements ActionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ServerInfoHandler.class);

    public static final String SERVER_INFO = "info";

    protected static final String VERSION_KEY = "neo4j-arrow-version";
    private static final String MANIFEST_KEY_BASE = "neo4j-arrow";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<String> actionTypes() {
        return List.of(SERVER_INFO);
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(SERVER_INFO, "Get metadata on server info"));
    }

    protected static Map<String, String> cachedArrowVersion = null;

    protected synchronized static Map<String, String> getArrowVersion() {

        if (cachedArrowVersion != null) {
            return cachedArrowVersion;
        }

        final Map<String, String> map = new HashMap<>();

        try {
            var iterator = ServerInfoHandler.class.getProtectionDomain().getClassLoader()
                            .getResources("META-INF/MANIFEST.MF").asIterator();

            while (iterator.hasNext()) {
                var url = iterator.next();
                try {
                    final Manifest m = new Manifest(url.openStream());
                    m.getMainAttributes().forEach((k, v) -> {
                        if (k.toString().toLowerCase().startsWith(MANIFEST_KEY_BASE)) {
                            logger.debug("caching server info ({}: {})", k, v);
                            map.put(k.toString(), v.toString());
                        }
                    });
                } catch (Exception e) {
                    // XXX unhandled
                }
            }
        } catch (Exception e) {
            logger.error("failure getting manifest", e);
        }
        cachedArrowVersion = map;
        return cachedArrowVersion;
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        try {
            final byte[] payload = mapper.writeValueAsBytes(getArrowVersion());
            return Outcome.success(new Result(payload));
        } catch (JsonProcessingException e) {
            logger.error("json error", e);
        }
        return Outcome.failure(CallStatus.INTERNAL.withDescription("cannot get version details"));
    }
}
