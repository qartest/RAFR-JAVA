package t.boev.work.model.node;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import t.boev.work.config.RaftNetworkConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Getter
@RequiredArgsConstructor
public class HeartbeatTimer {
    private final ScheduledExecutorService scheduler=
            Executors.newSingleThreadScheduledExecutor();

    private final Runnable onTimeout;
    private final RaftNetworkConfig config;


    private ScheduledFuture<?> timeoutTask;

    public synchronized void reset(){
        if(timeoutTask != null){
            timeoutTask.cancel(false);
        }
        long timeout = config.getRaftProperties().timing().heartbeatInterval().toMillis();
        timeoutTask = scheduler.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);
    }


    public synchronized void stop(){
        if (timeoutTask != null){
            timeoutTask.cancel(false);
            timeoutTask = null;
        }
    }

    public void shutdown(){
        scheduler.shutdownNow();
    }
}
