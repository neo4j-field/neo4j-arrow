package org.neo4j.arrow.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ServerInfoHandlerTest {

    @Test
    public void testGettingVersion()  {

        Map<String, String> map = ServerInfoHandler.getArrowVersion();
        Assertions.assertTrue(map.size() > 0);
        Assertions.assertEquals("cool", map.get(ServerInfoHandler.VERSION_KEY));
        System.out.println(map);
    }
}
