package utils;

public class CPULoad {
    public static void loadCPU(int millis) {
        long sleepTime = millis*1000000L; // convert to nanoseconds
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) < sleepTime) {}
    }

    public static void main(String[] args) {
        System.out.println("start: " + System.currentTimeMillis());
        loadCPU(1000);
        System.out.println("end: " + System.currentTimeMillis());
    }
}
