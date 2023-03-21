package DB.rocksDB;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

public class RocksDBHelper {

    public static RocksDB getRocksDB(String dbPath) throws Exception {
        RocksDB.loadLibrary();
        final Options opts = new Options().setCreateIfMissing(true);
        File dbDir = new File(dbPath);
        RocksDB db;
        db = RocksDB.open(opts, dbDir.getAbsolutePath());
        return db;
    }

    public static void main(String[] args){
        try{
            RocksDB d = getRocksDB("test.db");
            String key = "foo";
            String value = "bar";
            d.put(key.getBytes(), value.getBytes());
            System.out.println(new String(d.get(key.getBytes())));
        } catch (Exception e){
            System.out.println("error: " + e);
        }
    }
}
