package org.apache.storm.scheduler;


import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;

public class SimpleWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        if (event != null){
            System.out.println("Received event: [" + event.getType().toString() + "] " +
                    event.getPath() + " " + event.getState().name());
        }
    }


}
