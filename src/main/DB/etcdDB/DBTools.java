package etcdDB;
//import io.*;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

//import io.etcd.jetcd.test.EtcdClusterExtension;
//import org.junit.jupiter.api.extension.RegisterExtension;
public class DBTools {
    private static final String CLIENT_CONN_PORT = "8179";
    private static final String CLIENT_PEER_PORT = "8180";
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Client client = Client.builder().target("ip:///localhost:" + CLIENT_CONN_PORT).build();
        KV kvClient = client.getKVClient();
        ByteSequence key = ByteSequence.from("test_key".getBytes());
        ByteSequence value = ByteSequence.from("test_value".getBytes());

        // put the key-value
        kvClient.put(key, value).get();

        // get the CompletableFuture
        CompletableFuture<GetResponse> getFuture = kvClient.get(key);

        // get the value from CompletableFuture
        GetResponse response = getFuture.get();

        // delete the key
//        kvClient.delete(key).get();
        System.out.println(response);
    }
}
