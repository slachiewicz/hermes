package pl.allegro.tech.hermes.frontend.publishing;

import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.AsyncContext;
import java.util.Arrays;
import java.util.Map;

import static pl.allegro.tech.hermes.frontend.publishing.MessageState.State.SENDING_TO_KAFKA;
import static pl.allegro.tech.hermes.frontend.publishing.MessageState.State.SENDING_TO_KAFKA_PRODUCER_QUEUE;

@Singleton
public class MessagePublisher {

    private final BrokerMessageProducer brokerMessageProducer;

    @Inject
    public MessagePublisher(BrokerMessageProducer brokerMessageProducer) {
        this.brokerMessageProducer = brokerMessageProducer;
    }

    public void publish(Message message, Topic topic, MessageState messageState, AsyncContext asyncContext, Map<String, Long> milestones, PublishingCallback... callbacks) {
        messageState.setState(SENDING_TO_KAFKA_PRODUCER_QUEUE);
        brokerMessageProducer.send(message, topic, new PublishingCallback() {
            @Override
            public void onUnpublished(Exception exception) {
                milestones.put("MessagePublisher.onUnpublished.before.<<" + Thread.currentThread().getName() + ">>", System.nanoTime());
                asyncContext.start(() -> {
                            milestones.put("MessagePublisher.onUnpublished.async<<" + Thread.currentThread().getName() + ">>", System.nanoTime());
                            Arrays.stream(callbacks).forEach(c -> c.onUnpublished(exception));

                        }
                );
            }

            @Override
            public void onPublished(Message message, Topic topic) {
                milestones.put("MessagePublisher.onPublished.before.<<" + Thread.currentThread().getName() + ">>", System.nanoTime());
                asyncContext.start(() -> {
                    milestones.put("MessagePublisher.onPublished.async.<<" + Thread.currentThread().getName() + ">>", System.nanoTime());
                    Arrays.stream(callbacks).forEach(c -> c.onPublished(message, topic));
                });
            }
        });
        messageState.setState(SENDING_TO_KAFKA);
    }
}
