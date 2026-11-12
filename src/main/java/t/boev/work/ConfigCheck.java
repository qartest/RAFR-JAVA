package t.boev.work;


import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import t.boev.work.config.RaftNetworkConfig;
import t.boev.work.config.RaftProperties;

@Component
@RequiredArgsConstructor
public class ConfigCheck implements CommandLineRunner {
    private final RaftProperties properties;
    private final RaftNetworkConfig config;

    @Override
    public void run(String @NonNull ... args){
        System.out.println("=== RAFT CONFIG ===");
        System.out.println("Node ID: " + properties.nodeId());
        System.out.println("Peers: " + properties.nodes());
        System.out.println("Timing: " + properties.timing());

        System.out.println("\n\n\n" + config.getSelf());

    }
}
