package stateapis;

import DB.rocksDB.RocksDBHelper;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import pb.CPServiceGrpc;
import pb.Cp;
import utils.BytesUtil;
import utils.FatalUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class HybridMgrKVProvider implements KVProvider{
    private final CPServiceGrpc.CPServiceStub asyncStub;
    private final Logger logger = LogManager.getLogger();
    private RocksDB db;
    private final CPServiceGrpc.CPServiceBlockingStub blockingStub;

    private HashMap<String, String>  tableCache = new HashMap<>();
    public HybridMgrKVProvider(String cp_host, int cp_port) {
        String target = cp_host + ":" + cp_port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = CPServiceGrpc.newStub(channel);
        blockingStub = CPServiceGrpc.newBlockingStub(channel);
    }

    private String getRemoteTMStorage(String stateKey) {
        Cp.FindRemoteStateAddressRequest req = Cp.FindRemoteStateAddressRequest.newBuilder().setStateKey(stateKey).build();
        Cp.FindRemoteStateAddressResponse res;
        res = blockingStub.findRemoteStateAddress(req);
        String tmAddress = res.getAddress();

        try {
            db = RocksDBHelper.getRocksDB(tmAddress);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to open RocksDB", e);
        }
        return tmAddress;
    }

    @Override
    public Object get(String stateKey, Object defaultValue) {
        if(tableCache.containsKey(stateKey)) {
            try {
                return BytesUtil.checkedObjectFromBytes(tableCache.get(stateKey).getBytes());
            } catch (Exception e) {
                FatalUtil.fatal("Failed to get value from table cache", e);
                return null;
            }
        }
        else {
            getRemoteTMStorage(stateKey);
            try {
                byte[] value = db.get(stateKey.getBytes());
                if (value == null) {
                    logger.info("Key not found in RocksDB, returning default value");
                    return defaultValue;
                }
                Object v = BytesUtil.checkedObjectFromBytes(value);
                return v;
            } catch (Exception e) {
                FatalUtil.fatal("Failed to get value from remote RocksDB", e);
                return null;
            }
        }

    }

    @Override
    public void put(String stateKey, Object rawObject) {
        String tmAddress = getRemoteTMStorage(stateKey);
        tableCache.put(stateKey,tmAddress);

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
        String tmAddress = getRemoteTMStorage(stateKey);
        tableCache.put(stateKey,tmAddress);

        try {
            db.put(stateKey.getBytes(), value);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to put value into RocksDB", e);
        }
    }

    @Override
    public List<String> listKeys(String prefix) {
        List<String> result = new ArrayList<>();

        for (Map.Entry<String, String> entry : tableCache.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                result.add(key);
            }
        }

        if (db != null) {
            RocksIterator it = db.newIterator();
            for (it.seek(prefix.getBytes()); it.isValid(); it.next()) {
                result.add(new String(it.key()));
            }
        }
        return result;
    }

    @Override
    public void delete(String stateKey) {
        getRemoteTMStorage(stateKey);
        tableCache.remove(stateKey);

        try {
            db.delete(stateKey.getBytes());
        } catch (Exception e) {
            FatalUtil.fatal("Failed to delete value from RocksDB", e);
        }
    }

    @Override
    public void clear(String prefix) {

        tableCache.keySet().removeIf(key -> key.startsWith(prefix));

        if (db != null) {
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
    }

    @Override
    public void close() {
        if (db != null) {
            try {
                db.close();
            } catch (Exception e) {
                FatalUtil.fatal("Failed to close RocksDB", e);
            }
        }
    }
}
