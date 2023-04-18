package operators;
import config.Config;
import config.PrometheusConfig;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

public class Prometheus {
//    private static final Histogram ingestTimestampHistogram = Histogram.build()
//            .name("ingest_latency")
//            .help("Latency of processing in milliseconds.")
//            .buckets(1,10,50,100,500,1000,1500,2000,2500,3000)
//            .register();
    private final String config_path = "config.json";
    private static final Gauge ingestTimestampGauge = Gauge.build()
            .name("latency_Gauge")
            .help("Latency of processing in milliseconds.")
            .register();
    public Prometheus(){
        PrometheusConfig pc = Config.LoadConfig(config_path).prometheus;
        try {
            HTTPServer server = new HTTPServer(pc.prometheus_host, pc.prometheus_port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void setIngestTimestampGauge(double value){
        ingestTimestampGauge.set(value);
    }
}
