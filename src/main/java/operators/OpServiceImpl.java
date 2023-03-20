package operators;

//import com.google.protobuf.Empty;
//import io.grpc.stub.StreamObserver;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import pb.OPServiceGrpc;
//import pb.Op;
//import pb.Tm;
//
//import java.util.ArrayList;
//import java.util.List;

//interface IOperator {
//    void addInput(Tm.Msg input);
//    boolean checkBuffer();
//}

//public class OpServiceImpl extends OPServiceGrpc.OPServiceImplBase{
//    private IOperator operator;
//    protected Logger logger = LogManager.getLogger();
//    private List<Op.Msg> tempBuffer=new ArrayList<>();
//
//    public OpServiceImpl(IOperator operator) {
//        this.operator = operator;
//    }
//
//
//    @Override
//    public void pushMsg(Op.Msg request, StreamObserver<Empty> responseObserver) {
//        // TODO: add buffer size checks, if buffer size is too large, pause the input stream?
//        if(this.operator.checkBuffer()){
//            logger.info(request.getConfig().getName()+" buffer full, pausing input stream");
//            tempBuffer.add(request);
//            try {
//                Thread.sleep(1000);  // need UDF?
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }else{
//            if(tempBuffer.size()>0){
//                logger.info(request.getConfig().getName()+" buffer not full, resuming input stream");
//                for(Op.Msg msg:tempBuffer){
//                    this.operator.addInput(msg);
//                }
//                tempBuffer.clear();
//            }
//            this.operator.addInput(request);
//        }
//        responseObserver.onNext(Empty.getDefaultInstance());
//        responseObserver.onCompleted();
//    }
//}
