package pl.allegro.tech.hermes.frontend.publishing;


import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.publishing.avro.AvroSchemaRepository;
import pl.allegro.tech.hermes.frontend.publishing.avro.JsonToAvroMessageConverter;

import javax.inject.Inject;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static pl.allegro.tech.hermes.api.Topic.ContentType.AVRO;

public class MessageContentTypeEnforcer {

    private final JsonToAvroMessageConverter messageConverter = new JsonToAvroMessageConverter();
    private final AvroSchemaRepository avroSchemaRepository;

    @Inject
    public MessageContentTypeEnforcer(AvroSchemaRepository avroSchemaRepository) {
        this.avroSchemaRepository = avroSchemaRepository;
    }

    public Message enforce(String messageContentType, Message message, Topic topic) {
        if (APPLICATION_JSON.equalsIgnoreCase(messageContentType) && AVRO == topic.getContentType()) {
            return messageConverter.convert(message, avroSchemaRepository.findSchema(topic));
        }
        return message;
    }
}
