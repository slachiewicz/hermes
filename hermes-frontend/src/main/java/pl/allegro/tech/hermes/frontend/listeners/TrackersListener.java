package pl.allegro.tech.hermes.frontend.listeners;

import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.publishing.Message;
import pl.allegro.tech.hermes.tracker.frontend.Trackers;

public class TrackersListener implements BrokerAcknowledgeListener, BrokerTimeoutListener {

    private final Trackers trackers;

    public TrackersListener(Trackers trackers) {
        this.trackers = trackers;
    }

    @Override
    public void onAcknowledge(Message message, Topic topic) {
        trackers.get(topic).logPublished(message.getId(), topic.getName());
    }

    @Override
    public void onTimeout(Message message, Topic topic) {
        trackers.get(topic).logInflight(message.getId(), topic.getName());
    }

}
