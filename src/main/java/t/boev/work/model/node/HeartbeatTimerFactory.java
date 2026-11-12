package t.boev.work.model.node;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import t.boev.work.config.RaftNetworkConfig;

@Component
@RequiredArgsConstructor
public class HeartbeatTimerFactory {
    private final RaftNetworkConfig raftNetworkConfig;
    private final RaftNode raftNode;

    public HeartbeatTimer create(Runnable onTimeout){
        return new HeartbeatTimer(onTimeout, raftNetworkConfig, raftNode);
    }

}
