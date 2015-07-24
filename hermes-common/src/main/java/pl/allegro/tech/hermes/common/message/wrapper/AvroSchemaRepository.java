package pl.allegro.tech.hermes.common.message.wrapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.schemarepo.Subject;
import org.schemarepo.client.RESTRepositoryClient;
import org.schemarepo.json.GsonJsonUtil;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AvroSchemaRepository {

    private RESTRepositoryClient restRepositoryClient;

    @Inject
    public AvroSchemaRepository(ConfigFactory configFactory) {
        this(configFactory.getStringProperty(Configs.SCHEMA_REGISTRY_SERVER_URI));
    }

    public AvroSchemaRepository(String serverUri) {
        this.restRepositoryClient = new RESTRepositoryClient(serverUri, new GsonJsonUtil(), false);
    }

    private LoadingCache<String, Schema> schemaCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, Schema>() {
                        public Schema load(String topic) throws IOException {

                            Subject schema = restRepositoryClient.lookup(topic);

                            if (schema == null) {
                                throw new IllegalStateException("Cannot find schema for avro topic");
                            }

                            return new Schema.Parser().parse(schema.latest().getSchema());
                        }
                    });

    public Schema findSchema(String topic) {
        try {
            return schemaCache.get(topic);
        } catch (ExecutionException e) {
            throw new RuntimeException("Errow while getting topic schema");
        }
    }
}
