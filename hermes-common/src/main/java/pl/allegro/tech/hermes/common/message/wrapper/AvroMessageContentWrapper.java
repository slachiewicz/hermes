package pl.allegro.tech.hermes.common.message.wrapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.ByteStreams.toByteArray;

public class AvroMessageContentWrapper implements MessageContentWrapper {

    private String schemaRepoUri;

    @Inject
    public AvroMessageContentWrapper(ConfigFactory configFactory) {
        schemaRepoUri = configFactory.getStringProperty(Configs.SCHEMA_REGISTRY_SERVER_URI);
    }

    @Override
    public UnwrappedMessageContent unwrapContent(byte[] data) {
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data, null);

        try {
            return new UnwrappedMessageContent(
                new MessageMetadata(binaryDecoder.readLong(), binaryDecoder.readString()), toByteArray(binaryDecoder.inputStream()));
        } catch (IOException exception) {
            throw new UnwrappingException("Could not read hermes avro message", exception);
        }
    }

    @Override
    public byte[] wrapContent(byte[] message, String id, long timestamp) {
        try {
            ByteArrayOutputStream wrappedMessage = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(wrappedMessage, null);
            encoder.writeFixed(message);

            AvroSchemaRepository avroSchemaRepository = new AvroSchemaRepository(schemaRepoUri);
            Schema schema = avroSchemaRepository.findSchema("pl.allegro.tech.hermes.schemaHackaton");

            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(message, null);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            GenericRecord genericRecord;

            try {
                genericRecord = datumReader.read(null, binaryDecoder);
                Map<String, String> map = new HashMap<>();
                map.put("timestamp", Long.toString(timestamp));
                map.put("messageId", id);
                genericRecord.put("__metadata", map);

                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
                ByteArrayOutputStream os = new ByteArrayOutputStream();

                BinaryEncoder e = EncoderFactory.get().binaryEncoder(os, null);

                writer.write(genericRecord, e);
                e.flush();
                byte[] byteData = os.toByteArray();

                return byteData;

            } catch (EOFException e) {
                throw new RuntimeException("WRAPPING ERROR", e);
            }


        } catch (IOException exception) {
            throw new WrappingException("Could not wrap avro message", exception);
        }
    }

//    public Schema getWrappedSchema(Schema messageSchema) {
//        return record("MessageWithMetadata")
//                .namespace("pl.allegro.tech.hermes")
//                .fields()
//                    .name("metadata").type().record("MessageMetadata")
//                        .fields()
//                            .name("timestamp").type().longType().noDefault()
//                            .name("id").type().stringType().noDefault()
//                        .endRecord().noDefault()
//                    .name("message").type(messageSchema).noDefault()
//                .endRecord();
//    }

}
