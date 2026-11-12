package t.boev.work.transport;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import t.boev.work.model.dto.AppendEntriesRequest;
import t.boev.work.model.dto.AppendEntriesResponse;
import t.boev.work.model.dto.VoteRequest;
import t.boev.work.model.dto.VoteResponse;

import java.net.URI;
import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RaftNodeClient {
    private final WebClient webClient;
    private final String requestVotePath = "/raft/request-vote";
    private final String appendEntriesPath  = "/raft/append-entries";

    public Mono<VoteResponse> requestVote(
            URI nodeUrl,
            VoteRequest request
    ){
        return webClient.post()
                .uri(connectString2URI(requestVotePath, nodeUrl))
                .bodyValue(request)
                .retrieve()
                .bodyToMono(VoteResponse.class)
                .timeout(Duration.ofMillis(500))
                .onErrorResume(error -> {
                    return Mono.empty();
                });
    }

    public Mono<AppendEntriesResponse> appendEntries(
            URI nodeUrl,
            AppendEntriesRequest request
    ){
        return webClient.post()
                .uri(connectString2URI(appendEntriesPath, nodeUrl))
                .bodyValue(request)
                .retrieve()
                .bodyToMono(AppendEntriesResponse.class)
                .timeout(Duration.ofMillis(500))
                .onErrorResume(error -> {
                    return Mono.empty();
                });
    }

    private URI connectString2URI(String path, URI uri){
        return UriComponentsBuilder.fromUri(uri).path(path).build().toUri();
    }
}


