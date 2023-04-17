package stateapis;

import DB.rocksDB.RocksDBHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import pb.Tm;
import utils.BytesUtil;
import utils.FatalUtil;
import java.util.ArrayList;
import java.util.List;

public class LocalKVProvider implements KVProvider {
    private RocksDB db;
    private final Logger logger = LogManager.getLogger();

    private String localAddr;

    public LocalKVProvider(String dbPath) {
        try {
            db = RocksDBHelper.getRocksDB(dbPath);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to open RocksDB", e);
        }
    }

    @Override
    public Object get(String stateKey, Object defaultValue) {
        try {
            byte[] value = db.get(stateKey.getBytes());
            if (value == null) {
                logger.info("Key not found in RocksDB, returning default value: " + defaultValue);
                return defaultValue;
            }
            // deserialize the value into an object
            Object v = BytesUtil.checkedObjectFromBytes(value);
            return v;
        } catch (Exception e) {
            FatalUtil.fatal("Failed to get value from RocksDB", e);
            return null;
        }
    }

    @Override
    public void put(String stateKey, Object rawObject) {
        try {
            // serialize the object into a byte array
            byte[] bytes = BytesUtil.checkedObjectToBytes(rawObject);
            db.put(stateKey.getBytes(), bytes);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to put value into RocksDB", e);
        }
    }

    @Override
    public void put(String stateKey, byte[] value) {
        try {
            db.put(stateKey.getBytes(), value);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to put value into RocksDB", e);
        }
    }

    @Override
    public List<String> listKeys(String prefix) {
        List<String> result = new ArrayList<>();
        RocksIterator it = db.newIterator();
        for (it.seek(prefix.getBytes()); it.isValid(); it.next()) {
            result.add(new String(it.key()));
        }
        return result;
    }

    @Override
    public void delete(String stateKey) {
        try {
            db.delete(stateKey.getBytes());
        } catch (Exception e) {
            FatalUtil.fatal("Failed to delete value from RocksDB", e);
        }
    }

    @Override
    public void clear(String prefix) {
        try {
            byte[] start = prefix.getBytes();
            // increment the last byte of the start byte array
            byte[] end = BytesUtil.increment(start);
            // beautiful impl, gets the next byte array
            db.deleteRange(start, end);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to clear values from RocksDB", e);
        }
    }

    @Override
    public void close() {
        try {
            db.close();
        } catch (Exception e) {
            FatalUtil.fatal("Failed to close RocksDB", e);
        }
    }

    @Override
    public void handleReconfig(Tm.ReconfigMsg msg) {
        /*
        For the localkvprovider, reconfig should migrate everything for that operator.
         */
        throw new UnsupportedOperationException("LocalKVProvider does not support migration");
    }

    @Override
    public void setLocalAddr(String addr) {
        this.localAddr = addr;
    }
}
