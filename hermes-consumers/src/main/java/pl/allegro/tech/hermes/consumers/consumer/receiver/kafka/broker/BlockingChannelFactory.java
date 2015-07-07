package pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.broker;

import com.google.common.net.HostAndPort;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.network.BlockingChannel;
import pl.allegro.tech.hermes.consumers.consumer.receiver.kafka.ReadingConsumerMetadataException;

public class BlockingChannelFactory {

    private final HostAndPort broker;
    private final int readTimeout;

    public BlockingChannelFactory(HostAndPort broker, int readTimeout) {
        this.broker = broker;
        this.readTimeout = readTimeout;
    }

    public BlockingChannel create(String consumerGroupId) {
        ConsumerMetadataResponse metadataResponse = readConsumerMetadata(consumerGroupId);

        if (metadataResponse.errorCode() != ErrorMapping.NoError()) {
            throw new ReadingConsumerMetadataException(metadataResponse.errorCode());
        }

        Broker coordinator = metadataResponse.coordinator();

        BlockingChannel blockingChannel = new BlockingChannel(coordinator.host(), coordinator.port(),
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                readTimeout);

        return blockingChannel;
    }

    private ConsumerMetadataResponse readConsumerMetadata(String consumerGroupId) {
        BlockingChannel channel = new BlockingChannel(broker.getHostText(), broker.getPort(),
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                readTimeout);

        channel.connect();
        channel.send(new ConsumerMetadataRequest(consumerGroupId, ConsumerMetadataRequest.CurrentVersion(), 0, "0"));
        ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());
        channel.disconnect();
        return metadataResponse;
    }
}