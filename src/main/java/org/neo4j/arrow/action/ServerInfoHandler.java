package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class ServerInfoHandler implements ActionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ServerInfoHandler.class);

    public static final String SERVER_JOBS = "info.jobs";
    public static final String SERVER_VERSION = "info.version";

    protected static final String VERSION_KEY = "neo4j-arrow-version";
    private static final String MANIFEST_KEY_BASE = "neo4j-arrow";
    private static final ObjectMapper mapper = new ObjectMapper();

    /** Reference to the Producer's job map. Expected to be thread safe. */
    private final Map<?, Job> jobMap;

    public ServerInfoHandler(Map<?, Job> jobMap) {
        this.jobMap = jobMap;
    }

    @Override
    public List<String> actionTypes() {
        return List.of(SERVER_JOBS, SERVER_VERSION);
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(
                new ActionType(SERVER_JOBS, "List currently active Jobs"),
                new ActionType(SERVER_VERSION, "Get metadata on server info"));
    }

    protected static Map<String, String> cachedArrowVersion = null;

    protected Map<String, String> getJobs() {
        return jobMap.values()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        Job::getJobId,
                        (job) -> job.getStatus().toString()));
    }

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
            Map<String, String> map;
            switch (action.getType().toLowerCase()) {
                case SERVER_JOBS:
                    map = getJobs();
                    break;
                case SERVER_VERSION:
                    map = getArrowVersion();
                    break;
                default:
                    return Outcome.failure(CallStatus.UNKNOWN.withDescription("Unsupported action for job status handler"));
            }
            final byte[] payload = mapper.writeValueAsBytes(map);
            return Outcome.success(new Result(payload));
        } catch (JsonProcessingException e) {
            logger.error("json error", e);
            return Outcome.failure(CallStatus.INTERNAL);
        }
    }
}
