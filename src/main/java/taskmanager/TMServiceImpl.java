package taskmanager;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase{
    private final Logger logger = LogManager.getLogger();
    public void getStatus(Tm.TMStatusRequest request,
                          StreamObserver<Tm.TMStatusResponse> responseObserver) {
        logger.info("got status request");
        Tm.TMStatusResponse.Builder b = Tm.TMStatusResponse.newBuilder();
        b.setOperatorCount(999);
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    /**
     */
    @Override
    public void addOperator(Tm.AddOperatorRequest request,
                            StreamObserver<Tm.StdResponse> responseObserver) {

    }

    /**
     */
    @Override
    public void removeOperator(Tm.RemoveOperatorRequest request,
                               StreamObserver<pb.Tm.StdResponse> responseObserver) {

    }

    @Override
    public void reConfigOperator(Tm.ReConfigOperatorRequest request,
                                 StreamObserver<Tm.StdResponse> responseObserver) {

    }
}
