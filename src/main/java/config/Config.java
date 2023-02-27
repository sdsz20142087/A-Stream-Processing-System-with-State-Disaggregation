package config;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {
    public CPConfig controlPlane;
    public TMConfig taskManager;
    private static Config instance;
    public static void LoadConfig(String configPath){
        try{
            // read from file
            String data = new String(Files.readAllBytes(Paths.get(configPath)));
            //instance = new Config();
            // parse json
            Gson gson = new Gson();
            instance = gson.fromJson(data, Config.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load "+configPath, e);
        }
    }

    public static Config getInstance(){
        if (instance == null)
            throw new RuntimeException("Config not loaded");
        return instance;
    }

    public static void main(String[] args) throws Exception{
        Config.LoadConfig("config.json");
        Gson gson = new Gson();
        System.out.println(gson.toJson(Config.getInstance()));
    }
}


