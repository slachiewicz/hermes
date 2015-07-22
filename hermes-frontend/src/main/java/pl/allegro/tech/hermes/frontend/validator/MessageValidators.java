package pl.allegro.tech.hermes.frontend.validator;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.common.metric.Timers;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MessageValidators {

    private static final Logger logger = LoggerFactory.getLogger(MessageValidators.class);

    private final Map<TopicName, TopicMessageValidator> topicsWithValidators;
    private final TopicMessageValidatorFactory topicMessageValidatorFactory;
    private final HermesMetrics hermesMetrics;

    @Inject
    public MessageValidators(TopicMessageValidatorFactory topicMessageValidatorFactory, HermesMetrics hermesMetrics) {
        this.topicMessageValidatorFactory = topicMessageValidatorFactory;
        this.hermesMetrics = hermesMetrics;
        topicsWithValidators = new ConcurrentHashMap<>();
    }

    public void check(TopicName topicName, byte[] message) {
        Optional.ofNullable(topicsWithValidators.get(topicName)).ifPresent(validator -> {
            try (Timer.Context globalValidationTimerContext = hermesMetrics.timer(Timers.PRODUCER_VALIDATION_LATENCY).time();
                 Timer.Context topicValidationTimerContext = hermesMetrics.timer(Timers.PRODUCER_VALIDATION_LATENCY, topicName).time()) {
                validator.check(message);
            }
        });
    }
}
