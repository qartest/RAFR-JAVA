package t.boev.work.model.node;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import t.boev.work.config.RaftNetworkConfig;

@Component
@RequiredArgsConstructor
public class ElectionTimerFactory {
    private final RaftNetworkConfig config;

    public ElectionTimer create(Runnable onTimeout){
        return new ElectionTimer(onTimeout, config);
    }
}
