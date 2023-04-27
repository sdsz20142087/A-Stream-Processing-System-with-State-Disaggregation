package stateapis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pb.Tm;
import taskmanager.CPClient;
import utils.BytesUtil;
import utils.FatalUtil;
import utils.KeyUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class HybridKVProvider implements KVProvider {

    // The routing table cache, if any, is stored in the cpClient, the kvprovider doesn't care.
    private final CPClient cpClient;
    private KVProvider localKVProvider;
    private boolean migrate;

    private String localAddr;
    private final Logger logger = LogManager.getLogger();

    // map<TM ADDRESS, RemoteStateClient>
    private final HashMap<String, RemoteStateClient> remoteStateClientMap = new HashMap<>();

    private final HashMap<String,Boolean> involvedOps = new HashMap<>();

    private final HashSet<Integer> localMigratedCache = new HashSet<>();

    // The only difference between hybridNoMgr/hybridMgr is that whether they attempt to migrate,
    // this means hybridNoMgr has a subset of functions of hybridMgr
    public HybridKVProvider(KVProvider localKVProvider, CPClient cpClient, boolean migrate) {
        this.cpClient = cpClient;
        this.localKVProvider = localKVProvider;
        this.migrate = migrate;
    }
    private String getStateAddr(String prefix) {
        String addr = this.cpClient.getStateAddr(prefix);
        if (addr == null) {
            FatalUtil.fatal("Failed to get state addr from CP", null);
        }
        //logger.info("<{}> Got state addr: " + addr, this.localAddr);
        if (addr.equals(this.localAddr)) {
            return addr;
        }
        if (!remoteStateClientMap.containsKey(addr)) {
            remoteStateClientMap.put(addr, new RemoteStateClient(addr));
        }
        return addr;
    }

    @Override
    public Object get(String stateKey, Object defaultValue) {
        String addr = getStateAddr(stateKey);
        if (addr.equals(this.localAddr)) {
            return localKVProvider.get(stateKey, defaultValue);
        }
        int hash = KeyUtil.getHashFromKey(stateKey);
        if(this.migrate && localMigratedCache.contains(hash)){
            return localKVProvider.get(stateKey, defaultValue);
        }
        Object r = remoteStateClientMap.get(addr).get(stateKey);
        if(migrate){
            localMigratedCache.add(hash);
            localKVProvider.put(stateKey, r);
        }
        return r == null ? defaultValue : r;
    }

    @Override
    public void put(String stateKey, Object rawObject) {
        String addr = getStateAddr(stateKey);
        if (addr.equals(this.localAddr)) {
            localKVProvider.put(stateKey, rawObject);
            return;
        }
        int hash = KeyUtil.getHashFromKey(stateKey);
        if (this.migrate && localMigratedCache.contains(hash)) {
            localKVProvider.put(stateKey, rawObject);
            return;
        }
        if (this.migrate) {
            Object r = remoteStateClientMap.get(addr).get(stateKey);
            localKVProvider.put(stateKey, r);
            localMigratedCache.add(hash);
        } else {
            remoteStateClientMap.get(addr).put(stateKey, BytesUtil.ObjectToBytes(rawObject));
        }
    }


    @Override
    public void put(String stateKey, byte[] value) {
        String addr = getStateAddr(stateKey);
        if (addr.equals(this.localAddr)) {
            localKVProvider.put(stateKey, value);
            return;
        }
        int hash = KeyUtil.getHashFromKey(stateKey);
        if(this.migrate && localMigratedCache.contains(hash)){
            localKVProvider.put(stateKey, value);
            return;
        }
        if(this.migrate){
            Object r = remoteStateClientMap.get(addr).get(stateKey);
            localKVProvider.put(stateKey, r);
            localMigratedCache.add(hash);
        }else{
            remoteStateClientMap.get(addr).put(stateKey, value);
        }
    }

    @Override
    public List<String> listKeys(String prefix) {
        String addr = getStateAddr(prefix);
        if (addr.equals(this.localAddr)) {
            return localKVProvider.listKeys(prefix);
        }
        List<String> keys = remoteStateClientMap.get(addr).listKeys(prefix);
        return keys;
    }

    @Override
    public void delete(String stateKey) {
        String addr = getStateAddr(stateKey);
        if (addr.equals(this.localAddr)) {
            localKVProvider.delete(stateKey);
            return;
        }
        if(this.migrate && this.localMigratedCache.contains(KeyUtil.getHashFromKey(stateKey))){
            localKVProvider.delete(stateKey);
            return;
        } else {
            remoteStateClientMap.get(addr).delete(stateKey);
        }
    }

    @Override
    public void clear(String prefix) {
        String addr = getStateAddr(prefix);
        if (addr.equals(this.localAddr)) {
            localKVProvider.clear(prefix);
            return;
        }
        remoteStateClientMap.get(addr).clear(prefix);
    }

    @Override
    public void close() {
        this.localKVProvider.close();
    }

    @Override
    public void handleReconfig(Tm.ReconfigMsg msg) {
        //throw new UnsupportedOperationException("HybridKVProvider doesn't support handleMigration");
        // reset the table cache
        this.cpClient.RTCache.clear();
        this.localMigratedCache.clear();
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
