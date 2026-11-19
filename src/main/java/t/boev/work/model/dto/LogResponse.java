package t.boev.work.model.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter

public class LogResponse {
    private String nodeId;
    private long currentTerm;
    private long commitIndex;
    private long lastApplied;

    private List<CommandResponse> entries;
}
