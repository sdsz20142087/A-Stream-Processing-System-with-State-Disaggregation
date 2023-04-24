package stateapis;

import operators.stateless.Map;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MapProxyTest {

    private MapProxy<String> m;
    private LocalKVProvider kvProvider;

    private ValidKeyGetter keyGetter = new ValidKeyGetter();

    @BeforeEach
    void setUp() {
        kvProvider = new LocalKVProvider("test.db");
        m = new MapProxy<>("testKeyBase", kvProvider, keyGetter);
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
        String currentKey = keyGetter.getCurrentKey();
        assertEquals("testValue", kvProvider.get(
                "testKeyBase" + currentKey+":testKey"
                , null));
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
        System.out.println(keys);
        assertEquals(2, keys.size());
        String currentKey = keyGetter.getCurrentKey();
        assertTrue(keys.contains("testKeyBase"+currentKey+":testKey1"));
        assertTrue(keys.contains("testKeyBase"+currentKey+":testKey2"));
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

    @Test
    void testManyObj(){
        m.put("testKey", "testValue");
        String currentKey = keyGetter.getCurrentKey();
        assertEquals("testValue", kvProvider.get(
                "testKeyBase" + currentKey+":testKey"
                , null));


        keyGetter.setObj(new String("test-2"));
        m.put("testKey", "testValue2");
        currentKey = keyGetter.getCurrentKey();
        assertEquals("testValue2", kvProvider.get(
                "testKeyBase" + currentKey+":testKey"
                , null));

        keyGetter.setObj(new String("test"));
        currentKey = keyGetter.getCurrentKey();
        assertEquals("testValue", kvProvider.get(
                "testKeyBase" + currentKey+":testKey"
                , null));
    }
}
