package config;

public class TMConfig {
    public int operator_quota;
    public String cp_host;
    public int cp_port;
    public int tm_port;

    public int operator_bufferSize;

    public boolean useHybrid;

    public boolean useMigration;

    public boolean useCache;

    public String rocksDBPath;

    public int batch_size;

    public long batch_timeout_ms;
}
