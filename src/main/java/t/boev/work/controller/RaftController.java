package t.boev.work.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import t.boev.work.model.dto.AppendEntriesRequest;
import t.boev.work.model.dto.AppendEntriesResponse;
import t.boev.work.model.dto.VoteRequest;
import t.boev.work.model.dto.VoteResponse;
import t.boev.work.service.RaftService;

@RestController
@RequestMapping("/raft")
@RequiredArgsConstructor
public class RaftController {

    private final RaftService raftService;

    @PostMapping("/request-vote")
    public Mono<VoteResponse> requestVote(
            @RequestBody VoteRequest request
    ) {
        return raftService.onVote(request);
    }

    @PostMapping("/append-entries")
    public Mono<AppendEntriesResponse> appendEntries(
            @RequestBody AppendEntriesRequest request
    ) {
        return raftService.onAppendEntries(request);
    }


}
