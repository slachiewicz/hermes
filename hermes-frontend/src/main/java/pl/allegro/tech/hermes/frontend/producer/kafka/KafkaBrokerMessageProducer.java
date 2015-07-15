package pl.allegro.tech.hermes.frontend.producer.kafka;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.message.wrapper.MessageContentWrapperProvider;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer;
import pl.allegro.tech.hermes.frontend.publishing.Message;
import pl.allegro.tech.hermes.frontend.publishing.PublishingCallback;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class KafkaBrokerMessageProducer implements BrokerMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBrokerMessageProducer.class);

    private final Producers producers;
    private final MessageContentWrapperProvider contentWrapperProvider;

    @Inject
    public KafkaBrokerMessageProducer(Producers producers, MessageContentWrapperProvider contentWrapperProvider, HermesMetrics metrics) {
        this.producers = producers;
        this.contentWrapperProvider = contentWrapperProvider;
        producers.registerGauges(metrics);
    }

    @Override
    public void send(Message message, Topic topic, final PublishingCallback... callbacks) {
        send(message, topic, Lists.newArrayList(callbacks));
    }

    private void send(Message message, Topic topic, final List<PublishingCallback> callbacks) {
        try {
            Map<String, Long> milestones = new HashMap<>();
            byte[] content = contentWrapperProvider
                .provide(topic.getContentType())
                .wrapContent(message.getData(), message.getId(), message.getTimestamp());
            milestones.put("KafkaBrokerMessageProducer.wrapContent", System.nanoTime());
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic.getQualifiedName(), content);
            producers.get(topic).send(producerRecord, new SendCallback(message, topic, callbacks, milestones));
            milestones.put("KafkaBrokerMessageProducer.send", System.nanoTime());
        } catch (Exception e) {
            callbacks.forEach(c -> c.onUnpublished(e));
        }
    }

    private static class SendCallback implements org.apache.kafka.clients.producer.Callback {
        
        private final Message message;
        private final Topic topic;
        private final List<PublishingCallback> callbacks;
        private final Map<String, Long> milestones;

        public SendCallback(Message message, Topic topic, List<PublishingCallback> callbacks, Map<String, Long> milestones) {
            this.message = message;
            this.topic = topic;
            this.callbacks = callbacks;
            this.milestones = milestones;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            milestones.put("SendCallback.onCompletion", System.nanoTime());
            if (e != null) {
                callbacks.forEach(c -> {
                    milestones.put(c.getClass().getName(), System.nanoTime());
                    c.onUnpublished(e);
                });
            } else {
                callbacks.forEach(c -> {
                    milestones.put(c.getClass().getName(), System.nanoTime());
                    c.onPublished(message, topic);
                });
            }
            LOGGER.debug("MESSAGE ID: {}, BROKER TRACE: {}", message.getId(),
                    Arrays.toString(milestones.entrySet().toArray()));
        }
    }
}
