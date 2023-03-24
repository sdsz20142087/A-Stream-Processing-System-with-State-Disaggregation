package utils;

import java.io.*;

public class BytesUtil {
    public static byte[] increment(byte[] inputArr) {
        byte[] ret = new byte[inputArr.length];
        int carry = 1;
        for (int i = inputArr.length - 1; i >= 0; i--) {
            if (inputArr[i] == (byte) 0xFF && carry == 1) {
                ret[i] = (byte) 0x00;
            } else {
                ret[i] = (byte) (inputArr[i] + carry);
                carry = 0;
            }
        }
        if (carry == 0) {
            return ret;
        } else {
            throw new ArrayIndexOutOfBoundsException("Overflow");
        }
    }

    public static byte[] ObjectToBytes(Object o) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(o);
            out.flush();
            byte[] ret = bos.toByteArray();
            bos.close();
            return ret;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] checkedObjectToBytes(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(o);
        out.flush();
        byte[] ret = bos.toByteArray();
        bos.close();
        return ret;
    }

    public static Object ObjectFromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = new ObjectInputStream(bis);
            Object o = in.readObject();
            bis.close();
            return o;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Object checkedObjectFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis);
        Object o = in.readObject();
        bis.close();
        return o;
    }
}
