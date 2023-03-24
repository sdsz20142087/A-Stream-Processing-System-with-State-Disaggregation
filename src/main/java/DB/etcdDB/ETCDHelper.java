package DB.etcdDB;

import controller.TMClient;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ETCDHelper {
    private static String[] endpoints;
    private static KV kvClient;
    private static final Logger logger = LogManager.getLogger();

    public static void init(String[] eps, boolean testConn) {
        logger.info("Initializing DBTools");
        endpoints = eps;
        // configure GRPC to use PickFirstLB
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        Client client = Client.builder().endpoints(endpoints).build();
        String msg = "Connecting to etcd on " + Arrays.toString(eps);
        logger.info(msg);
        kvClient = client.getKVClient();
        logger.info("got client");
        if (testConn) {
            try {
                ByteSequence key = ByteSequence.from("test_key".getBytes());
                ByteSequence value = ByteSequence.from("test_value".getBytes());
                kvClient.put(key, value).get(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                logger.info("put key-value pair");
                // get the CompletableFuture
                CompletableFuture<GetResponse> getFuture = kvClient.get(key);

                // get the value from CompletableFuture
                GetResponse response = getFuture.get(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
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

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        System.out.println(new ArrayList<>(List.of(args)).toString());
        // usage: java -jar DBTools.jar http://localhost:2379
        ETCDHelper.init(new String[]{args[0]}, true);
        System.out.println("ok");
    }
}
