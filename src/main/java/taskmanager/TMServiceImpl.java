package taskmanager;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;
import utils.TMException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase {
    private int operatorQuota;
    private HashMap<String, BaseOperator> operators;
    private Logger logger = LogManager.getLogger();

    public TMServiceImpl(int operatorQuota) {
        super();
        this.operatorQuota = operatorQuota;
        operators = new HashMap<>();
        logger.info("TM service started with operator quota: " + operatorQuota);
    }

    public void getStatus(Tm.TMStatusRequest request,
                          StreamObserver<Tm.TMStatusResponse> responseObserver) {
        logger.info("got status request");
        Tm.TMStatusResponse.Builder b = Tm.TMStatusResponse.newBuilder();
        b.setOperatorCount(999);
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    /**
     *
     */
    @Override
    public void addOperator(Tm.AddOperatorRequest request,
                            StreamObserver<Empty> responseObserver) {
        if (operators.size() >= operatorQuota) {
            responseObserver.onError(new TMException("operator quota exceeded"));
            return;
        }
        if (operators.containsKey(request.getConfig().getName())) {
            responseObserver.onError(new TMException("operator name already exists"));
            return;
        }
        logger.info(String.format("adding operator %d/%d",this.operators.size()+1,this.operatorQuota));
        // TODO: add operator, the control plane sends over serialized operator so
        // we can just deserialize it and add it to the operators map
        byte[] bytes = request.getObj().toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois;
        BaseOperator op;
        try {
            ois = new ObjectInputStream(bis);
            op = (BaseOperator) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            responseObserver.onError(e);
            return;
        }

        op.start();
        operators.put(op.getName(), op);

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     *
     */
    @Override
    public void removeOperator(Tm.RemoveOperatorRequest request,
                               StreamObserver<Empty> responseObserver) {

    }

    @Override
    public void reConfigOperator(Tm.ReConfigOperatorRequest request,
                                 StreamObserver<Empty> responseObserver) {

    }
}
