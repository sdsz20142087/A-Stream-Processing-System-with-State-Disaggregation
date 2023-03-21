package DB.etcdDB;

import controller.TMClient;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//import io.etcd.jetcd.test.EtcdClusterExtension;
//import org.junit.jupiter.api.extension.RegisterExtension;
public class DBTools {
    private static String[] endpoints;
    private static DBTools instance;
    private static KV kvClient;
    //    private Server DBToolsServer;
    private static final Logger logger = LogManager.getLogger();

    private DBTools() {
    }

    public static void init(String[] eps, boolean testConn) {
        logger.info("Initializing DBTools");
        endpoints = eps;
        Client client = Client.builder().endpoints(eps).build();
        String msg = "Connecting to etcd on " + Arrays.toString(eps);
        logger.info(msg);
        kvClient = client.getKVClient();
        logger.info("got client");
        if (testConn) {
            try {
                ByteSequence key = ByteSequence.from("test_key".getBytes());
                ByteSequence value = ByteSequence.from("test_value".getBytes());
                kvClient.put(key, value).get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                logger.info("put key-value pair");
                // get the CompletableFuture
                CompletableFuture<GetResponse> getFuture = kvClient.get(key);

                // get the value from CompletableFuture
                GetResponse response = getFuture.get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                logger.info(msg + " with response count=" + response.getCount());
            } catch (Exception e) {
                logger.fatal("Failed to " + msg, e);
                System.exit(1);
            }
        }
    }

    public static KV getKvClient() {
        return kvClient;
    }


    private static void clearDatabase() throws ExecutionException, InterruptedException {
        ByteSequence keyFrom = ByteSequence.from("".getBytes());
        ByteSequence keyTo = ByteSequence.from(new byte[]{(byte) 0xFF});
        kvClient.delete(keyFrom).get();
        logger.info("clean all data");
        // Close the client
        kvClient.close();
    }

    // TODO:
    public String registerTM(String clientName, TMClient tmClient) throws IOException {
        byte[] clientNameBytes = clientName.getBytes();

//        TMClientDB tmClientDB = new TMClientDB(tmClient.getHost(), tmClient.getPort());
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.writeObject(tmClientDB);
//        byte[] tmClientBytes = baos.toByteArray();
//
//        ByteSequence key = ByteSequence.from(clientNameBytes);
//        ByteSequence value = ByteSequence.from(tmClientBytes);
////        logger.info("try to get response");
//
//        CompletableFuture<GetResponse> keyExistsOrNot = kvClient.get(key);
//        GetResponse response = null;
//        try {
//            response = keyExistsOrNot.get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//        if (response.getCount() == 1) {
//            logger.info("connected successfully");
//            logger.info("Key exists, don't need to register this TM again");
//            return "Failed";
//        } else if (response.getCount() == 0) {
//            logger.info("connected successfully");
//        } else {
//            logger.info("connected failed");
//        }
//        try {
//            kvClient.put(key, value).get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
//        }
        logger.info("Successfully added TM");
        return "Succeed";
    }

    public void getTM(String clientName) {
//        if (kvClient == null) getDBClient();
//        byte[] clientNameBytes = clientName.getBytes();
//        ByteSequence key = ByteSequence.from(clientNameBytes);
//        CompletableFuture<GetResponse> getFuture = kvClient.get(key);
//        GetResponse response = null;
//        try {
//            response = getFuture.get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//        if (response.getCount() == 0) {
//            logger.info("Key not exists");
//            return;
//        }
//        byte[] valueBytes = response.getKvs().get(0).getValue().getBytes();
//        ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
//        ObjectInputStream ois;
//        TMClientDB tmClientDB;
//        try {
//            ois = new ObjectInputStream(bis);
//            tmClientDB = (TMClientDB) ois.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
//
//        TMClient tmClient = new TMClient(tmClientDB.address, tmClientDB.port);
//        logger.info(tmClient.getHost(), tmClient.getPort());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        System.out.println(new ArrayList<>(List.of(args)).toString());
        // usage: java -jar DBTools.jar http://localhost:2379
        DBTools.init(new String[]{args[0]}, true);
    }
}
