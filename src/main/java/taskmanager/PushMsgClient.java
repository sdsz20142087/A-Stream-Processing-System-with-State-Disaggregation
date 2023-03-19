package taskmanager;

import com.google.protobuf.Empty;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;

class PushMsgClient {
    private final TMServiceGrpc.TMServiceStub asyncStub;

    private final Logger logger;

    private final String target;

    public PushMsgClient(Logger logger, String target) {
        this.logger = logger;
        this.target = target;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = TMServiceGrpc.newStub(channel);
        logger.info("PushMsgClient created for " + target);
    }

    public void pushMsg(Tm.Msg msg) {
        asyncStub.pushMsg(msg, new StreamObserver<>() {
            @Override
            public void onNext(Empty empty) {}

            @Override
            public void onError(Throwable t) {
                logger.error("TM: pushClient: Failed to push message to TM at " + target
                        + "-->"+ t.getMessage());
            }

            @Override
            public void onCompleted() {
            }
        });
    }
}
