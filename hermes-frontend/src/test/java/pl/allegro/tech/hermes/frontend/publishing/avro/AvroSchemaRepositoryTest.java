package pl.allegro.tech.hermes.frontend.publishing.avro;

import org.apache.avro.Schema;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.message.wrapper.AvroSchemaRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvroSchemaRepositoryTest {

    @Test
    public void shouldFindSchema() throws Exception {
        //given
        AvroSchemaRepository avroSchemaRepository = new AvroSchemaRepository("xxx");
        Topic topic = mock(Topic.class);
        when(topic.getQualifiedName()).thenReturn("kasia_test_monitoring_topic");

        //when
        Schema schema = avroSchemaRepository.findSchema(topic.getQualifiedName());

        //then
        assertThat(schema).isNotNull();
    }
}