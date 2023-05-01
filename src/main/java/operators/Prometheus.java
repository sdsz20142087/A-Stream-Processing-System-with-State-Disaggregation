package operators;
import config.Config;
import config.PrometheusConfig;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;

public class Prometheus {
    //    private static final Histogram ingestTimestampHistogram = Histogram.build()
//            .name("ingest_latency")
//            .help("Latency of processing in milliseconds.")
//            .buckets(1,10,50,100,500,1000,1500,2000,2500,3000)
//            .register();
    private final String config_path = "config.json";
    private final CollectorRegistry registry = new CollectorRegistry();
    private Gauge metric;
    private PushGateway gateway;
    private String job;
    private int cnt=0;

    //    private static final Gauge ingestTimestampGauge = Gauge.build()
//            .name("latency_Gauge")
//            .help("Latency of processing in milliseconds.")
//            .register();
    public Prometheus(){
        PrometheusConfig pc = Config.LoadConfig(config_path).prometheus;
        if(!pc.enabled)
            return;
        // "pushgateway_host"  --->  "pushgateway", the same as the name of container pushgateway
        this.gateway= new PushGateway(pc.pushgateway_host+":"+pc.pushgateway_port);
        this.job="pushgateway"; // must be  same as the job_name in prometheus.yml
        this.cnt=++cnt;
        this.metric = Gauge.build()
                .name(pc.job_name)
                .help("Latency of processing in milliseconds.")
                .labelNames("id")
                .register(registry);
    }
    public void setIngestTimestampGauge(double value){
        if(!Config.getInstance().prometheus.enabled)
            return;
        metric.labels(String.valueOf(cnt)).set(value);
        try {
            gateway.pushAdd(registry, job);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
