package t.boev.work.model.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter

public class CommandResponse {
    private long index;
    private long term;
    private String command;
}
