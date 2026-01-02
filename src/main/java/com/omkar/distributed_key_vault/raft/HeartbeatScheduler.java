package com.omkar.distributed_key_vault.raft;

import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class HeartbeatScheduler {
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> heartbeatTask;

    public synchronized void start(Runnable heartbeatAction) {
        stop();

        heartbeatTask = scheduler.scheduleAtFixedRate(
                heartbeatAction,
                0,
                500,
                TimeUnit.MILLISECONDS   // Raft heartbeat interval - 500ms for stability
        );
    }

    public synchronized void stop() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
    }
}
