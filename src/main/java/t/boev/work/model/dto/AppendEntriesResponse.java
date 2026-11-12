package t.boev.work.model.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AppendEntriesResponse {
    long term;
    boolean goodTerm;
}
