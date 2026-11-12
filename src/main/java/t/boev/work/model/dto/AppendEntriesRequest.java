package t.boev.work.model.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AppendEntriesRequest {
    int leaderId;
    long term;
}
