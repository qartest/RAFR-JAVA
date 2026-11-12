package t.boev.work.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;


import java.net.URI;
import java.util.List;

@Component
@RequiredArgsConstructor
@Getter
public class RaftNetworkConfig {
    private final RaftProperties raftProperties;
    private NodeAddress self;
    private List<URI> hosts;

    @PostConstruct
    public void findSelf(){
        int selfId = raftProperties.nodeId();

        self = raftProperties.nodes().stream()
                .filter(node -> node.id() == selfId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Node not found"));
    }

    @PostConstruct
    public void hostProcessing(){
        hosts = raftProperties.nodes().stream().filter(node -> node.id() != raftProperties.nodeId())
                .map(this::buildNodeUri).toList();
    }

    private URI buildNodeUri(NodeAddress node){
        try {
            return UriComponentsBuilder
                    .newInstance()
                    .scheme("http")
                    .host(node.host())
                    .port(node.port())
                    .build()
                    .toUri();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid URI for Raft node: "
                            + node.host() + ":" + node.port(),
                    e
            );
        }
    }

}
