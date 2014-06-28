/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.support.window;

public class SwitchWindow extends Window {

    boolean stopped = false;

    int stopTicks = 0;

    int evictionTicks = 0;


    public SwitchWindow(String groupedIndex, long size) {
        super(groupedIndex, size);
    }

    public SwitchWindow(String groupedIndex) {
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

    public int getEvictionTicks() {
        return evictionTicks;
    }

    public void incrementEvictionTicks() {
        this.evictionTicks++;
    }

    public void resetEvictionTicks() {
        evictionTicks = 0;
    }
}
