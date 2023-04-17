package stateapis;
import operators.stateless.Map;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@TestInstance(TestInstance.Lifecycle.PER_CLASS)

public class MapProxyTest {

    private MapProxy<String> m;
    private LocalKVProvider kvProvider;

    @BeforeEach
    void setUp() {
        kvProvider = new LocalKVProvider("test.db");
        m = new MapProxy<>("testKeyBase", kvProvider, new IKeyGetter() {
            @Override
            public String getCurrentKey() {
                return null;
            }

            @Override
            public boolean hasKeySelector() {
                return false;
            }
        });
        m.clear();
    }

    @AfterEach
    void tearDown() {
        m.clear();
        kvProvider.close();
    }

    @Test
    void testGet() {
        m.put("testKey", "testValue");
        assertEquals("testValue", m.get("testKey"));
    }

    @Test
    void testPut() {
        m.put("testKey", "testValue");
        assertEquals("testValue", kvProvider.get("testKeyBase:testKey",null));
    }

    @Test
    void testRemove() {
        m.put("testKey", "testValue");
        m.remove("testKey");
        assertNull(m.get("testKey"));
    }

    @Test
    void testClear() {
        m.put("testKey", "testValue");
        m.clear();
        assertTrue(m.isEmpty());
    }

    @Test
    void testKeys() {
        m.put("testKey1", "testValue1");
        m.put("testKey2", "testValue2");
        List<String> keys = m.keys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("testKeyBase:testKey1"));
        assertTrue(keys.contains("testKeyBase:testKey2"));
    }

    @Test
    void testIsEmpty() {
        assertTrue(m.isEmpty());
        m.put("testKey", "testValue");
        assertFalse(m.isEmpty());
    }

    @Test
    void testContainsKey() {
        assertFalse(m.containsKey("testKey"));
        m.put("testKey", "testValue");
        assertTrue(m.containsKey("testKey"));
    }

    @Test
    void testSize() {
        assertEquals(0, m.size());
        m.put("testKey", "testValue");
        assertEquals(1, m.size());
    }
}
