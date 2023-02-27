package operators;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import pb.OPServiceGrpc;
import pb.Op;

interface IOperator {
    void addInput(Op.Msg input);
}

public class OpServiceImpl extends OPServiceGrpc.OPServiceImplBase{
    private IOperator operator;

    public OpServiceImpl(IOperator operator) {
        this.operator = operator;
    }


    @Override
    public void pushMsg(Op.Msg request, StreamObserver<Empty> responseObserver) {
        // TODO: add buffer size checks, if buffer size is too large, pause the input stream?
        this.operator.addInput(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
