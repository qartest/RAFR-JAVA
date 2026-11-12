package t.boev.work.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import t.boev.work.config.RaftNetworkConfig;
import t.boev.work.model.dto.AppendEntriesRequest;
import t.boev.work.model.dto.AppendEntriesResponse;
import t.boev.work.model.dto.VoteRequest;
import t.boev.work.model.dto.VoteResponse;
import t.boev.work.model.node.*;
import t.boev.work.transport.RaftNodeClient;

import java.net.URI;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RaftService {
    private final ElectionTimerFactory factory;
    private final HeartbeatTimerFactory heartbeatTimerFactory;
    private final RaftNetworkConfig raftNetworkConfig;
    private final RaftNodeClient raftNodeClient;

    private ElectionTimer electionTimer;
    private HeartbeatTimer heartbeatTimer;

    private long currentTerm = 0;
    private NodeState state = NodeState.FOLLOWER;
    private Integer votedFor = null;
    private int majority;

    private final Object stateLock = new Object();

    private int nodeId() {
        return raftNetworkConfig.getRaftProperties().nodeId();
    }

    @PostConstruct
    public void initElectionTimer() {
        int clusterSize = raftNetworkConfig.getHosts().size() + 1;
        majority = clusterSize / 2 + 1;

        electionTimer = factory.create(() ->
                        startElection()
                                .doOnError(error ->
                                        log.error("[Node {}] Election callback failed",
                                                nodeId(), error)
                                )
                                .subscribe()
        );

        heartbeatTimer = heartbeatTimerFactory.create(() ->
                heartbeat()
                        .doOnError(error ->
                                log.error("[Node {}] Heartbeat  failed",
                                        nodeId(), error)
                        )
                        .subscribe()
        );

        log.info(
                "[Node {}] Raft started. State={}, term={}, clusterSize={}, majority={}, peers={}",
                nodeId(), state, currentTerm, clusterSize, majority,
                raftNetworkConfig.getHosts()
        );

        log.info("[Node {}] Starting election timer", nodeId());
        electionTimer.reset();
    }

    @PreDestroy
    public void destroy() {
        log.info("[Node {}] Shutting down Raft service", nodeId());
        if (electionTimer != null) {
            log.debug("[Node {}] Shutting down election timer", nodeId());
            electionTimer.shutdown();
        }

        if (heartbeatTimer != null) {
            log.debug("[Node {}] Shutting down heartbeat timer", nodeId());
            heartbeatTimer.shutdown();
        }
    }

    public Mono<Void> startElection(){
        long nowTerm;
        int candidateId = nodeId();
        electionTimer.stop();
        synchronized (stateLock){
            if(state == NodeState.LEADER){
                log.debug(
                        "[Node {}] Election callback ignored: already LEADER in term {}",
                        candidateId, currentTerm
                );
                return Mono.empty();
            }

            currentTerm++;
            state = NodeState.CANDIDATE;
            votedFor = raftNetworkConfig.getRaftProperties().nodeId();
            nowTerm = currentTerm;
            log.info(
                    "[Node {}] Election started. term={}, votedFor=self, state=CANDIDATE",
                    candidateId, nowTerm
            );
        }
        VoteRequest request = new VoteRequest(nowTerm, raftNetworkConfig.getRaftProperties().nodeId());

        log.info(
                "[Node {}] Requesting votes from {} peers for term {}. Need {} votes including self",
                candidateId, raftNetworkConfig.getHosts().size(),
                currentTerm, majority
        );
        return requestVotes(raftNetworkConfig.getHosts(), request).doOnNext(voteResponses -> {
            boolean becameFollower = false;
            boolean retryElection = false;
            boolean becameLeader = false;

            synchronized (stateLock){
                long maxTerm = voteResponses.stream().mapToLong(VoteResponse::getTerm).max().orElse(currentTerm);

                if(maxTerm > currentTerm){
                    log.warn(
                            "[Node {}] Received higher term {} while election term is {}. Stepping down",
                            candidateId, maxTerm, currentTerm
                    );
                    currentTerm = maxTerm;
                    votedFor = null;
                    state = NodeState.FOLLOWER;
                    becameFollower = true;
                } else if(state == NodeState.CANDIDATE && currentTerm == nowTerm) {
                    long receivedVotes = voteResponses.stream()
                            .filter(r -> r.getTerm() == nowTerm)
                            .filter(VoteResponse::isVoteGranted)
                            .count();

                    long totalVotes = receivedVotes + 1;
                    log.info(
                            "[Node {}] Election results for term {}: votes={}/{}, majority={}",
                            candidateId, nowTerm, totalVotes,
                            raftNetworkConfig.getHosts().size() + 1, majority
                    );

                    if (totalVotes >= majority) {
                        state = NodeState.LEADER;
                        becameLeader = true;

                        log.info(
                                "[Node {}] *** BECAME LEADER *** term={}, votes={}",
                                candidateId, currentTerm, totalVotes
                        );
                    }
                    else {
                        retryElection = true;
                        state = NodeState.FOLLOWER;

                        log.warn(
                                "[Node {}] Election failed in term {}. Received {} votes, need {}. Returning to FOLLOWER",
                                candidateId, nowTerm, totalVotes, majority
                        );
                    }
                }
            }

            if (becameLeader) {
                log.info("[Node {}] Stopping election timer", candidateId);
                electionTimer.stop();

                log.info("[Node {}] Starting leader heartbeat timer", candidateId);
                heartbeatTimer.reset();

            } else if (becameFollower) {
                log.info("[Node {}] Resetting election timer after higher term", candidateId);
                electionTimer.reset();

            } else if (retryElection) {
                log.info("[Node {}] Resetting election timer after failed election", candidateId);
                electionTimer.reset();
            }

        }).then();

    }

    public Mono<Void> heartbeat(){
        long nowTerm;
        boolean notLeader = false;
        int leaderId = nodeId();

        synchronized (stateLock){
            if (state != NodeState.LEADER) {
                notLeader = true;
                log.debug(
                        "[Node {}] Heartbeat callback ignored: state={}, term={}",
                        leaderId, state, currentTerm
                );
            }
            nowTerm = currentTerm;
        }
        if(notLeader){
            heartbeatTimer.stop();
            electionTimer.reset();
            return Mono.empty();
        }
        AppendEntriesRequest request = new AppendEntriesRequest(raftNetworkConfig.getRaftProperties().nodeId(), nowTerm);
        log.debug(
                "[Node {}] Sending heartbeat to {} peers, term={}",
                leaderId, raftNetworkConfig.getHosts().size(), nowTerm
        );
        return appendEntriesRequest(raftNetworkConfig.getHosts(), request).doOnNext(responses -> {
            boolean becameFolder = false;

            synchronized (stateLock){

                if (state != NodeState.LEADER || currentTerm != nowTerm) {
                    becameFolder = true;
                    log.debug(
                            "[Node {}] Ignoring stale heartbeat results. State={}, currentTerm={}, heartbeatTerm={}",
                            leaderId, state, currentTerm, nowTerm
                    );
                }
                else{
                    long maxTerm = responses.stream().mapToLong(AppendEntriesResponse::getTerm).max().orElse(currentTerm);
                    if(maxTerm > currentTerm){
                        log.warn(
                                "[Node {}] Higher term {} received in heartbeat response (current term {}). Stepping down",
                                leaderId, maxTerm, currentTerm
                        );

                        currentTerm = maxTerm;
                        votedFor = null;
                        state = NodeState.FOLLOWER;
                        becameFolder = true;
                    }
                }
            }
            if(becameFolder){
                log.info("[Node {}] Stopping heartbeat timer", leaderId);
                heartbeatTimer.stop();
                log.info("[Node {}] Resetting election timer after stepping down", leaderId);
                electionTimer.reset();
            }
            synchronized (stateLock) {
                if (state == NodeState.LEADER) {
                    heartbeatTimer.reset();
                }
            }

        }).then();
    }


    public Mono<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest request){
        return Mono.fromSupplier(() -> {
            long responseTerm;
            synchronized (stateLock){
                log.debug(
                        "Получен AppendEntries: requestTerm={}, currentTerm={}, state={}",
                        request.getTerm(), currentTerm, state
                );
                if(request.getTerm() < currentTerm){
                    log.debug(
                            "AppendEntries отклонён: устаревший терм. requestTerm={}, currentTerm={}",
                            request.getTerm(), currentTerm
                    );
                    return new AppendEntriesResponse(currentTerm, false);
                }

                if(request.getTerm() > currentTerm){
                    log.info(
                            "Обнаружен более высокий терм при получении Entries: {} -> {}. Переход в FOLLOWER",
                            currentTerm, request.getTerm()
                    );
                    currentTerm = request.getTerm();
                    votedFor = null;
                }
                if (state != NodeState.FOLLOWER) {
                    log.info("Переход в FOLLOWER после получения AppendEntries");
                }
                state = NodeState.FOLLOWER;
                responseTerm = currentTerm;
                heartbeatTimer.stop();
                electionTimer.reset();
                log.debug(
                        "AppendEntries принят: term={}, election timer сброшен",
                        responseTerm
                );
            }

            return new AppendEntriesResponse(responseTerm, true);
        });
    }

    public Mono<VoteResponse> onVote(VoteRequest request){
        return Mono.fromSupplier(() -> {
            boolean granted;
            boolean resetTimer = false;
            boolean stopHeartbeat = false;

            synchronized (stateLock){
                log.debug(
                        "Получен RequestVote: candidateId={}, requestTerm={}, currentTerm={}, votedFor={}",
                        request.getCandidateId(), request.getTerm(), currentTerm, votedFor
                );
                if (request.getTerm() < currentTerm) {
                    log.debug(
                            "Голос отклонён: устаревший терм. candidateId={}, requestTerm={}, currentTerm={}",
                            request.getCandidateId(), request.getTerm(), currentTerm
                    );
                    return new VoteResponse(false, currentTerm);
                }
                if (request.getTerm() > currentTerm) {
                    log.info(
                            "Обнаружен более высокий терм: {} -> {}. Переход в FOLLOWER",
                            currentTerm, request.getTerm()
                    );
                    currentTerm = request.getTerm();
                    votedFor = null;
                    state = NodeState.FOLLOWER;
                    stopHeartbeat = true;
                    resetTimer = true;
                }
                if (votedFor == null || votedFor.equals(request.getCandidateId())) {
                    votedFor = request.getCandidateId();
                    granted = true;
                    resetTimer = true;
                    log.info(
                            "Голос выдан: candidateId={}, term={}",
                            request.getCandidateId(), currentTerm
                    );
                } else {
                    log.info(
                            "В голосе отказано: candidateId={}, term={}, уже проголосовали за candidateId={}",
                            request.getCandidateId(), currentTerm, votedFor
                    );
                    granted = false;
                }
            }

            if (stopHeartbeat) heartbeatTimer.stop();
            if (resetTimer)    electionTimer.reset();
            return new VoteResponse(granted, currentTerm);
        });

    }

    private Mono<List<VoteResponse>> requestVotes(List<URI> URI, VoteRequest request){
        return Flux.fromIterable(URI).flatMap(uri -> raftNodeClient.requestVote(uri, request)).collectList();

    }

    private Mono<List<AppendEntriesResponse>> appendEntriesRequest(List<URI> URI, AppendEntriesRequest request){
        return Flux.fromIterable(URI).flatMap(uri -> raftNodeClient.appendEntries(uri, request)).collectList();
    }

}
