package operators;

import pb.Tm;

public class OutputMessage {
    private String opName;
    private Tm.Msg.Builder msg;
    private int key;

    public OutputMessage(String opName, Tm.Msg.Builder msg, int key) {
        this.opName = opName;
        this.msg = msg;
        this.key = key;
    }

    public String getOpName() {
        return opName;
    }

    public void setOpName(String opName) {
        this.opName = opName;
    }

    public Tm.Msg.Builder getMsg() {
        return msg;
    }

    public void setMsg(Tm.Msg.Builder msg) {
        this.msg = msg;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }
}
