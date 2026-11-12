package t.boev.work.transport;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;
import t.boev.work.config.RaftNetworkConfig;

@Component
@RequiredArgsConstructor
public class RaftPortCustomizer implements WebServerFactoryCustomizer<ConfigurableWebServerFactory> {
    private final RaftNetworkConfig raftNetworkConfig;

    @Override
    public void customize(ConfigurableWebServerFactory factory) {
        factory.setPort(raftNetworkConfig.getSelf().port());
    }
}
