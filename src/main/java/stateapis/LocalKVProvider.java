package stateapis;

import DB.rocksDB.RocksDBHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class LocalKVProvider implements KVProvider {
    private RocksDB db;
    private Logger logger = LogManager.getLogger();

    public LocalKVProvider(String dbPath) {
        try {
            db = RocksDBHelper.getRocksDB(dbPath);
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to create RocksDB instance" + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public Object get(String stateKey, Object defaultValue) {
        try {
            byte[] value = db.get(stateKey.getBytes());
            if (value == null) {
                return defaultValue;
            }
            // deserialize the value into an object
            ByteArrayInputStream bis = new ByteArrayInputStream(value);
            ObjectInputStream ois;

            ois = new ObjectInputStream(bis);
            Object v = ois.readObject();
            return v;
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to get value from RocksDB" + e.getMessage());
            System.exit(1);
            return null;
        }
    }

    @Override
    public void put(String stateKey, Object rawObject) {
        try {
            // serialize the object into a byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(rawObject);
            byte[] bytes = baos.toByteArray();
            db.put(stateKey.getBytes(), bytes);
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to put value into RocksDB" + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void put(String stateKey, byte[] value) {
        try {
            db.put(stateKey.getBytes(), value);
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to put value into RocksDB" + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void delete(String stateKey) {
        try {
            db.delete(stateKey.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to delete value from RocksDB" + e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void clear(String prefix) {
        try {
            db.delete(prefix.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            logger.fatal("Failed to delete value prefix from RocksDB" + prefix + ":" + e.getMessage());
            System.exit(1);
        }
    }
}
