package t.boev.work.model.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class VoteResponse {
    boolean voteGranted;
    long term;
}
