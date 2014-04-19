package org.calrissian.flowbox.support;

public class StopGateWindow extends WindowBuffer {

    boolean stopped = false;

    int stopTicks = 0;


    public StopGateWindow(String groupedIndex, long size) {
        super(groupedIndex, size);
    }

    public StopGateWindow(String groupedIndex) {
        super(groupedIndex);
    }

    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void incrementStopTicks() {
        stopTicks+=1;
    }

    public int getStopTicks() {
        return stopTicks;
    }

    public void resetStopTicks() {
        stopTicks = 0;
    }
}
