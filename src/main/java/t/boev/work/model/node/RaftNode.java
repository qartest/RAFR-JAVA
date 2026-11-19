package t.boev.work.model.node;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import t.boev.work.config.RaftNetworkConfig;
import t.boev.work.model.dto.VoteResponse;
import t.boev.work.service.TimeService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Component
@RequiredArgsConstructor
@Slf4j
public class RaftNode {
    private long currentTerm = 0;
    private long commitIndex = 0;
    private long lastApplied = 0;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private List<Command> commands = new ArrayList<>();

    private NodeState state = NodeState.FOLLOWER;
    private Integer votedFor = null;
    private int majority;

    private final RaftNetworkConfig raftNetworkConfig;
    private final TimeService timeService;


    @PostConstruct
    public void initMajority(){
        int clusterSize = raftNetworkConfig.getHosts().size() + 1;
        majority = clusterSize / 2 + 1;
    }

    public List<Command> getCommands(){
        lock.readLock().lock();
        try{
            return List.copyOf(commands);
        }finally {
            lock.readLock().unlock();
        }
    }

    public boolean isLeader(){
        lock.readLock().lock();
        try{
            return (state == NodeState.LEADER);
        }finally {
            lock.readLock().unlock();
        }
    }

    public TermResponse getTerm(){
        lock.readLock().lock();
        try{
            return new TermResponse(state == NodeState.LEADER, currentTerm)
        }finally {
            lock.readLock().unlock();
        }
    }


    public boolean heartbeat(List<VoteResponse> voteResponses, long nowTerm){
        boolean becameFollower;
        boolean becameLeader;

        long maxTerm = voteResponses.stream().mapToLong(VoteResponse::getTerm).max().orElse(currentTerm);

        lock.writeLock().lock();
        try{
            if(maxTerm > currentTerm){
                log.warn(
                        "[Node {}] Received higher term {} while election term is {}. Stepping down",
                        candidateId, maxTerm, currentTerm
                );
                currentTerm = maxTerm;
                votedFor = null;
                state = NodeState.FOLLOWER;
                becameFollower = true;
            }else if (state == NodeState.CANDIDATE && currentTerm == nowTerm){
                long receivedVotes = voteResponses.stream()
                        .filter(r -> r.getTerm() == nowTerm)
                        .filter(VoteResponse::isVoteGranted)
                        .count();

                long totalVotes = receivedVotes + 1;

                if(totalVotes >= majority){
                    state = NodeState.LEADER;
                    becameLeader = true;

                    log.info(
                            "[Node {}] *** BECAME LEADER *** term={}, votes={}",
                            candidateId, currentTerm, totalVotes
                    );
                }
            }
        }finally {
            lock.writeLock().unlock();
        }

    }



}
