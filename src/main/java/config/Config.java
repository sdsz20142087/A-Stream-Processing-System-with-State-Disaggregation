package config;

import com.google.gson.Gson;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {
    public CPConfig controlPlane;
    public TMConfig taskManager;

    public PrometheusConfig prometheus;
    public String fileName;
    private static Config instance;

    public static Config LoadConfig(String configPath) {
        try {
            if(instance != null)
                return instance;
            // read from file
            String data = new String(Files.readAllBytes(Paths.get(configPath)));
            //instance = new Config();
            // parse json
            Gson gson = new Gson();
            instance = gson.fromJson(data, Config.class);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + configPath, e);
        }
    }

    public static Config getInstance() {
        if (instance == null)
            throw new RuntimeException("Config not loaded");
        return instance;
    }

    public static void main(String[] args) throws Exception {
        Config.LoadConfig("config.json");
        Gson gson = new Gson();
        System.out.println(gson.toJson(Config.getInstance()));
    }
}


