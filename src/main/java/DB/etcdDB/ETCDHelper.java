package DB.etcdDB;
import controller.TMClient;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//import io.etcd.jetcd.test.EtcdClusterExtension;
//import org.junit.jupiter.api.extension.RegisterExtension;
public class ETCDHelper {
    private static final String CLIENT_CONN_PORT = "8179";
    private static final String CLIENT_PEER_PORT = "8180";
    private static ETCDHelper instance;
    private static KV DBClient;
//    private Server DBToolsServer;
    private static final Logger logger = LogManager.getLogger();

    private static class TMClientDB implements Serializable {
        private String address;
        private Integer port;
        TMClientDB(String addr, Integer port) {
            this.address = addr;
            this.port = port;
        }
    }

    private ETCDHelper() {}

    public static ETCDHelper getInstance() {
        if (instance == null) {
            instance = new ETCDHelper();
            getDBClient();
        }
        return instance;
    }

//    private void startDBToolsServer() throws IOException {
//        DBToolsServer = ServerBuilder.forPort(Integer.parseInt(CLIENT_CONN_PORT))
//                .addService(new EtcdServiceImpl(this)).build();
//        DBToolsServer.start();
//        logger.info("DBClient server started on port " + CLIENT_CONN_PORT);
//    }

    private static void getDBClient() {
        if (DBClient == null) {
            Client client = Client.builder().target("ip:///localhost:" + CLIENT_CONN_PORT).build();
            DBClient = client.getKVClient();
            logger.info("Connected to etcd server, client address: ip:///localhost:" + CLIENT_CONN_PORT);
//            try {
//                clearDatabase();
//            } catch (ExecutionException | InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    private static void clearDatabase() throws ExecutionException, InterruptedException {
        ByteSequence keyFrom = ByteSequence.from("".getBytes());
        ByteSequence keyTo = ByteSequence.from(new byte[] {(byte)0xFF});
        DBClient.delete(keyFrom).get();
        logger.info("clean all data");
        // Close the client
//        client.close();
    }
    public String registerTM(String clientName, TMClient tmClient) throws IOException {
        if (DBClient == null) getDBClient();
        byte[] clientNameBytes = clientName.getBytes();

        TMClientDB tmClientDB = new TMClientDB(tmClient.getHost(), tmClient.getPort());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(tmClientDB);
        byte[] tmClientBytes = baos.toByteArray();

        ByteSequence key = ByteSequence.from(clientNameBytes);
        ByteSequence value = ByteSequence.from(tmClientBytes);
        CompletableFuture<GetResponse> keyExistsOrNot = DBClient.get(key);
        GetResponse response = null;
        try {
            response = keyExistsOrNot.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        if (response.getCount() == 1) {
            logger.info("Key exists, don't need to register this TM again");
            return "Failed";
        }
        try {
            DBClient.put(key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        logger.info("Successfully added TM");
        return "Succeed";
    }

    public void getTM(String clientName) {
        if (DBClient == null) getDBClient();
        byte[] clientNameBytes = clientName.getBytes();
        ByteSequence key = ByteSequence.from(clientNameBytes);
        CompletableFuture<GetResponse> getFuture = DBClient.get(key);
        GetResponse response = null;
        try {
            response = getFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        if (response.getCount() == 0) {
            logger.info("Key not exists");
            return;
        }
        byte[] valueBytes = response.getKvs().get(0).getValue().getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
        ObjectInputStream ois;
        TMClientDB tmClientDB;
        try {
            ois = new ObjectInputStream(bis);
            tmClientDB = (TMClientDB) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        TMClient tmClient = new TMClient(tmClientDB.address, tmClientDB.port);
        logger.info(tmClient.getHost(), tmClient.getPort());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//        DBTools.getInstance().startDBToolsServer();
//        TMClient tmClient = new TMClient()

//        ByteSequence key = ByteSequence.from("test_key".getBytes());
//        ByteSequence value = ByteSequence.from("test_value".getBytes());
//
//        // put the key-value
//        kvClient.put(key, value).get();
//
//        // get the CompletableFuture
//        CompletableFuture<GetResponse> getFuture = kvClient.get(key);
//
//        // get the value from CompletableFuture
//        GetResponse response = getFuture.get();
//
//        // delete the key
////        kvClient.delete(key).get();
//        System.out.println(response);
    }
}
