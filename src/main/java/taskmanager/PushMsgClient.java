package taskmanager;

import com.google.protobuf.Empty;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;

import java.util.List;

class PushMsgClient {
    private final TMServiceGrpc.TMServiceStub asyncStub;
    private final TMServiceGrpc.TMServiceBlockingStub blockingStub;

    private final Logger logger;

    private final String target;

    private final boolean useAsync;

    public PushMsgClient(Logger logger, String target, boolean useAsync) {
        this.useAsync = useAsync;
        this.logger = logger;
        this.target = target;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = TMServiceGrpc.newStub(channel);
        blockingStub = TMServiceGrpc.newBlockingStub(channel);
        logger.info("PushMsgClient created for " + target);
    }

    public void pushMsgList(Tm.MsgList msgs) {
        if(useAsync){
            asyncStub.pushMsgList(msgs, new StreamObserver<>() {
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
        } else {
            blockingStub.pushMsgList(msgs);
        }
    }
}
