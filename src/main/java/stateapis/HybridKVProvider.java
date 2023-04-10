package stateapis;

import DB.rocksDB.RocksDBHelper;
import io.grpc.*;
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

public class HybridNoMgrKVProvider implements KVProvider{
    private final CPServiceGrpc.CPServiceStub asyncStub;
    private RocksDB db;
    private final CPServiceGrpc.CPServiceBlockingStub blockingStub;

    private final Logger logger = LogManager.getLogger();
    public HybridNoMgrKVProvider(String cp_host, int cp_port, int actualPort) {
        String target = cp_host + ":" + cp_port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = CPServiceGrpc.newStub(channel);
        blockingStub = CPServiceGrpc.newBlockingStub(channel);
    }
    private void getRemoteTMStorage(String stateKey) {
        Cp.FindRemoteStateAddressRequest req = Cp.FindRemoteStateAddressRequest.newBuilder().setStateKey(stateKey).build();
        Cp.FindRemoteStateAddressResponse res;
        res = blockingStub.findRemoteStateAddress(req);
        String tmAddress = res.getAddress();

        try {
            db = RocksDBHelper.getRocksDB(tmAddress);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to open RocksDB", e);
        }

    }
    @Override
    public Object get(String stateKey, Object defaultValue) {
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

    @Override
    public void put(String stateKey, Object rawObject) {
        getRemoteTMStorage(stateKey);

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
        getRemoteTMStorage(stateKey);
        try {
            db.put(stateKey.getBytes(), value);
        } catch (Exception e) {
            FatalUtil.fatal("Failed to put value into RocksDB", e);
        }
    }

    @Override
    public List<String> listKeys(String prefix) {
        // how to get tm based on the keybase? keybase is not in the tm kv store
        List<String> result = new ArrayList<>();
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
        try {
            db.delete(stateKey.getBytes());
        } catch (Exception e) {
            FatalUtil.fatal("Failed to delete value from RocksDB", e);
        }
    }

    @Override
    public void clear(String prefix) {
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
