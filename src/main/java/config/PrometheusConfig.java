package config;

public class PrometheusConfig {

    public boolean enabled;
    public int prometheus_port;
    public String prometheus_host;
    public String pushgateway_host;
    public int pushgateway_port;
    public String job_name;
}
