package stateapis;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DequeProxyTest {

    private DequeProxy<String> d;
    private LocalKVProvider kvProvider;

    private ValidKeyGetter keyGetter = new ValidKeyGetter();

    @BeforeEach
    void setUp() {
        kvProvider = new LocalKVProvider("test.db");
        d = new DequeProxy<>("test-keybase", kvProvider, keyGetter);
        d.clear();
    }

    @AfterEach
    void tearDown() {
        d.clear();
        kvProvider.close();
    }

    @Test
    void addFirst() {
        d.addFirst("a");
        d.addFirst("b");
        d.addFirst("c");
        assertEquals(d.size(), 3);
        assertEquals(d.peekFirst(), "c");
    }

    @Test
    void addLast() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.peekLast(), "c");
    }

    @Test
    void removeFirst() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.removeFirst(), "a");
        assertEquals(d.size(), 2);
        assertEquals(d.removeFirst(), "b");
        assertEquals(d.size(), 1);
        assertEquals(d.removeFirst(), "c");
        assertEquals(d.size(), 0);
    }

    @Test
    void removeLast() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.removeLast(), "c");
        assertEquals(d.size(), 2);
        assertEquals(d.removeLast(), "b");
        assertEquals(d.size(), 1);
        assertEquals(d.removeLast(), "a");
        assertEquals(d.size(), 0);
    }

    @Test
    void peekFirst() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.peekFirst(), "a");

    }

    @Test
    void peekLast() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.peekLast(), "c");
    }

    @Test
    void clear() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        d.clear();
        assertEquals(d.size(), 0);
    }

    @Test
    void isEmpty() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertFalse(d.isEmpty());
        d.clear();
        assertEquals(d.size(), 0);
        assertTrue(d.isEmpty());
    }

    @Test
    void size() {
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        d.clear();
        assertEquals(d.size(), 0);
    }

    @Test
    void testManyObj(){
        d.addLast("a");
        d.addLast("b");
        d.addLast("c");
        assertEquals(d.size(), 3);
        assertEquals(d.peekLast(), "c");

        keyGetter.setObj(new String("test-3"));
        assertEquals(d.size(), 0);
        d.addLast("d");
        assertEquals(d.size(), 1);

        keyGetter.setObj(new String("test"));
        assertEquals(d.size(), 3);
    }
}