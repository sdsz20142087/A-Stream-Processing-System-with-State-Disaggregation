package taskmanager;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.Cp;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class CPClient {
    private final Logger logger = LogManager.getLogger();
    private final String target;
    private final int tm_port;
    private final CPServiceGrpc.CPServiceStub asyncStub;
    private final CPServiceGrpc.CPServiceBlockingStub blockingStub;

    private final boolean cached;

    // routing table cache
    // FIXME: we probably need a trie tree for this
    public ConcurrentHashMap<String, String> RTCache = new ConcurrentHashMap<>();

    public CPClient(String host, int cp_port, int tm_port, boolean cached) {
        this.cached = cached;
        target = host + ":" + cp_port;
        this.tm_port = tm_port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = CPServiceGrpc.newStub(channel);
        blockingStub = CPServiceGrpc.newBlockingStub(channel);
    }

    // returns the external ip addr of this client
    public String registerTM(String localAddress, String name) {
        logger.info("Registering TM at Control Plane");
        Cp.RegisterTMRequest req = Cp.RegisterTMRequest.newBuilder()
                .setAddress(localAddress)
                .setName(name+":"+this.tm_port)
                .setPort(this.tm_port)
                .build();
        Cp.RegisterTMResponse resp = blockingStub.registerTM(req);
        logger.info("Got response: " + resp);
        return resp.getExternalAddress();
    }

    public String getStateAddr(String keyPrefix) {
        if(cached && RTCache.containsKey(keyPrefix)) {
            return RTCache.get(keyPrefix);
        }
        // TODO: IMPLEMENT THIS
        //logger.info("Getting state TM addr from Control Plane");
        Cp.FindRemoteStateAddressRequest req = Cp.FindRemoteStateAddressRequest.
                newBuilder().setStateKey(keyPrefix).build();
        Cp.FindRemoteStateAddressResponse res = blockingStub.findRemoteStateAddress(req);
        if(cached){
            RTCache.put(keyPrefix, res.getAddress());
        }
        return res.getAddress();
    }

    public void flush(){
        this.RTCache.clear();
    }
}

