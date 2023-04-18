package config;

public class CPConfig {
    public String[] etcd_endpoints;
    public boolean test_etcd_conn;
    public int cp_grpc_port;
    public double scale_up_portion;
    public int control_port;
    public double watermark_interval;
    public int out_of_order_grace_period;
    public double consistent_control_grace_period;
}
