package pl.allegro.tech.hermes.frontend.publishing.avro;

import org.apache.avro.Schema;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvroSchemaRepositoryTest {

    @Test
    public void shouldFindSchema() throws Exception {
        //given
        AvroSchemaRepository avroSchemaRepository = new AvroSchemaRepository();
        Topic topic = mock(Topic.class);
        when(topic.getQualifiedName()).thenReturn("kasia_test_monitoring_topic");

        //when
        Schema schema = avroSchemaRepository.findSchema(topic);

        //then
        assertThat(schema).isNotNull();
    }
}