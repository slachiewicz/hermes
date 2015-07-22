package pl.allegro.tech.hermes.frontend.validator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.apache.avro.Schema;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.publishing.avro.AvroSchemaRepository;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.inject.Inject;
import java.io.IOException;

public class TopicMessageValidatorFactory {
    private final JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.byDefault();
    private final ObjectMapper objectMapper;
    private final AvroSchemaRepository avroSchemaRepository;

    @Inject
    public TopicMessageValidatorFactory(ObjectMapper objectMapper, AvroSchemaRepository avroSchemaRepository) {
        this.objectMapper = objectMapper;
        this.avroSchemaRepository = avroSchemaRepository;
    }

    public TopicMessageValidator create(Topic topic) throws IOException, ProcessingException {
        Schema schema = avroSchemaRepository.findSchema(topic);

        switch (topic.getContentType()) {
            case JSON:
                return createJsonTopicMessageValidator(topic);
            case AVRO:
                return createAvroTopicMessageValidator(schema);
            default:
                throw new IllegalStateException("Unsupported content type " + topic.getContentType().name());
        }
    }

    private TopicMessageValidator createAvroTopicMessageValidator(Schema schema) {
        return new AvroTopicMessageValidator(schema);
    }

    private TopicMessageValidator createJsonTopicMessageValidator(Topic topic) throws IOException, ProcessingException {
        throw new NotImplementedException();

//        return new JsonTopicMessageValidator(
//            jsonSchemaFactory.getJsonSchema(objectMapper.readTree(topic.getMessageSchema())),
//            objectMapper);
    }
}
