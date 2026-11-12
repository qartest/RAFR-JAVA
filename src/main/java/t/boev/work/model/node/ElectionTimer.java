package t.boev.work.model.node;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import t.boev.work.config.RaftNetworkConfig;

import java.util.concurrent.*;

@Getter
@RequiredArgsConstructor
public class ElectionTimer {
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final Runnable onTimeout;
    private final RaftNetworkConfig config;

    private ScheduledFuture<?> timeoutTask;

    public synchronized void reset(){
        if(timeoutTask != null){
            timeoutTask.cancel(false);
        }

        long timeout = ThreadLocalRandom.current()
                .nextLong(config.getRaftProperties().timing().electionTimeoutMin().toMillis(),
                        config.getRaftProperties().timing().electionTimeoutMax().toMillis() + 1);


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
