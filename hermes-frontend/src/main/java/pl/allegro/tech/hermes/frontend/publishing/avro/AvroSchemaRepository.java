package pl.allegro.tech.hermes.frontend.publishing.avro;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.schemarepo.Subject;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.GsonJsonUtil;
import pl.allegro.tech.hermes.api.Topic;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AvroSchemaRepository {

    private RESTRepositoryClient restRepositoryClient =
            new RESTRepositoryClient("xxx", new GsonJsonUtil(), false);

    private LoadingCache<Topic, Schema> schemaCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<Topic, Schema>() {
                        public Schema load(Topic topic) throws IOException {

                            Subject schema = restRepositoryClient.lookup(topic.getQualifiedName());

                            if (schema == null) {
                                throw new IllegalStateException("Cannot find schema for avro topic");
                            }

                            return new Schema.Parser().parse(schema.latest().getSchema());
                        }
                    });

    public AvroSchemaRepository() {
    }


    public Schema findSchema(Topic topic) {
        try {
            return schemaCache.get(topic);
        } catch (ExecutionException e) {
            throw new RuntimeException("Errow while getting topic schema");
        }
    }
}
