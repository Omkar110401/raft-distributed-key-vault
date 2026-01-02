package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.config.NodeConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class ElectionTimer {
    private final ObjectProvider<RaftCoordinator> coordinatorProvider;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> electionTask;
    private final Random random = new Random();

    public ElectionTimer(ObjectProvider<RaftCoordinator> coordinatorProvider) {
        this.coordinatorProvider = coordinatorProvider;
    }

    @PostConstruct
    public void init() {
        reset();
    }

    public synchronized void reset() {
        stop();

        int timeout = 10000 + random.nextInt(10000); // 10–20 seconds

        electionTask = scheduler.schedule(
                () -> coordinatorProvider.getObject().onElectionTimeout(),
                timeout,
                TimeUnit.MILLISECONDS
        );
    }

    public synchronized void stop() {
        if (electionTask != null) {
            electionTask.cancel(true);
        }
    }
}
