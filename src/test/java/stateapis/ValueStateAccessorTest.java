package stateapis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ValueStateAccessorTest {
    private ValueStateAccessor<String> v;

    private LocalKVProvider kvProvider;

    private IKeyGetter keyGetter = new ValidKeyGetter();

    @BeforeEach
    void setUp() {
        kvProvider = new LocalKVProvider("test.db");
        v = new ValueStateAccessor<>("desc-name", kvProvider, "asdf", keyGetter);
        v.clear();
    }

    @AfterEach
    void tearDown() {
        v.clear();
        kvProvider.close();
    }

    @Test
    void testValue() {
        assertEquals("asdf", v.value());
    }

    @Test
    void testUpdate() {
        v.update("qwer");
        assertEquals("qwer", v.value());
    }

    @Test
    void testClear() {
        v.update("bbb");
        v.clear();
        assertEquals("asdf", v.value());
    }
}