package t.boev.work.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import t.boev.work.config.RaftNetworkConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class TimeService {
    private final RaftNetworkConfig config;

    private final ScheduledExecutorService schedulerHeartbeat =
            Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> timeoutTaskHeartbeat;

    private final ScheduledExecutorService schedulerElection =
            Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> timeoutTaskElection;

    public synchronized void resetHeartbeat(Runnable onTimeout){
        if(timeoutTaskHeartbeat != null){
            timeoutTaskHeartbeat.cancel(false);
        }
        long timeout = config.getRaftProperties().timing().heartbeatInterval().toMillis();
        timeoutTaskHeartbeat = schedulerHeartbeat.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopHeartbeat(){
        if (timeoutTaskHeartbeat != null){
            timeoutTaskHeartbeat.cancel(false);
            timeoutTaskHeartbeat = null;
        }
    }

    public void shutdownHeartbeat(){
        schedulerHeartbeat.shutdownNow();
    }

    public synchronized void resetElection(Runnable onTimeout){
        if(timeoutTaskElection != null){
            timeoutTaskElection.cancel(false);
        }
        long timeout = config.getRaftProperties().timing().heartbeatInterval().toMillis();
        timeoutTaskElection = schedulerElection.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopElection(){
        if (timeoutTaskElection != null){
            timeoutTaskElection.cancel(false);
            timeoutTaskElection = null;
        }
    }

    public void shutdownElection(){
        schedulerElection.shutdownNow();
    }

    @PreDestroy
    public void destroy(){
        shutdownElection();
        shutdownHeartbeat();
    }
}
