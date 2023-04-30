package stateapis;

import DB.rocksDB.RocksDBHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.FileUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import pb.Tm;
import utils.BytesUtil;
import utils.FatalUtil;
import utils.NodeBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class LocalKVProvider implements KVProvider {
    private RocksDB db;
    private final Logger logger = LogManager.getLogger();

    private final HashMap<String, Boolean> involvedOps = new HashMap<>();

    private String localAddr;

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete()){}
            //throw new FileNotFoundException("Failed to delete file: " + f);
    }
    public LocalKVProvider(String dbPath) {
        // remove whatever's in dbpath
        try {
            delete(new File(dbPath));
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
                //logger.info("Key {} not found in RocksDB, returning default value: " + defaultValue, stateKey);
                return defaultValue;
            }
            // deserialize the value into an object
            Object v = BytesUtil.checkedObjectFromBytes(value);
            return v;
        } catch (Exception e) {
            //logger.error("statekey: " + stateKey + " defaultValue: " + defaultValue + " " + e.getMessage(), e);
            //FatalUtil.fatal("Failed to get value from RocksDB", e);
            // FIXME: THIS BROKE
            return defaultValue;
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
        for (String opName : msg.getConfigMap().keySet()) {
            if (involvedOps.get(opName) == null || !involvedOps.get(opName)) {
                continue;
            }
            // the opName is now guaranteed to be in this TM
            /*
            get the operator's stage,
             loop through all the changed configs,
              connect to their TM to pull required data
             */
            List<String> targetTMs = msg.getConfigMap().get(opName).getPeerTMAddrsList();
            int stage = msg.getConfigMap().get(opName).getLogicalStage();
            assert stage > 0;
//            for(String targetAddr:targetTMs){
//                if(msg.getConfigMap().get(opName).getLocalTMAddr().equals(targetAddr)){
//                    continue;
//                }
//                RemoteStateClient client = new RemoteStateClient(targetAddr);
//
//                List<Tm.StateKV> newKVs = client.pullStates(stage, msg.getConfigMap().get(opName).getPartitionPlan());
//                for(Tm.StateKV kv:newKVs){
//                    String oldKey = kv.getKey();
//                    String[] parts = oldKey.split(":");
//                    String newKey = opName;
//                    for(int i=1;i<stage;i++){
//                        newKey += ":"+parts[i];
//                    }
//                    this.put(newKey, kv.getKeyBytes().toByteArray());
//                }
//                logger.info("Pulled " + newKVs.size() + " states from " + targetAddr + " for " + opName);
//            }

        }
        logger.info("Involved operators: " + involvedOps);
        Boolean isInvolved = this.involvedOps.get("SvCountOperator_1-1");
        if (isInvolved != null && isInvolved) {
            String addr = NodeBase.getHost() + ":8018";
            RemoteStateClient client = new RemoteStateClient(addr);
            logger.info("cfg:{}", msg.getConfigMap().get("SvCountOperator_1-1"));
            List<Tm.StateKV> newKVs = client.pullStates(3, msg.getConfigMap().get("SvCountOperator_1-1").getPartitionPlan());

            for (Tm.StateKV kv : newKVs) {
                String oldKey = kv.getKey();
                String[] parts = oldKey.split(":");
                String newKey = "SvCountOperator_1-1";
                for (int i = 1; i < parts.length; i++) {
                    newKey += ":" + parts[i];
                }
                this.put(newKey, kv.getKeyBytes().toByteArray());
                logger.info("newKey: {}", newKey);
            }
            logger.info("Pulled " + newKVs.size() + " states from " + "192.168.1.19:8018" + " for " + "SvCountOperator_1-1");
            logger.info("States: {}", newKVs);
        } else {
            logger.info("kv migrate: No need to consider since we're not involved");
        }
    }

    @Override
    public void addInvolvedOp(String opId, boolean hasKey) {
        this.involvedOps.put(opId, hasKey);
    }

    @Override
    public void removeInvolvedOp(String opId) {
        this.involvedOps.remove(opId);
    }

    @Override
    public void setLocalAddr(String addr) {
        this.localAddr = addr;
    }
}
