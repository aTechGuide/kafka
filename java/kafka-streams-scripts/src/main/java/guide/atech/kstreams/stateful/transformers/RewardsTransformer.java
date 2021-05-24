package guide.atech.kstreams.stateful.transformers;

import guide.atech.kstreams.stateful.RewardsApp;
import guide.atech.schema.json.Notification;
import guide.atech.schema.json.PosInvoice;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@SuppressWarnings("unchecked")
public class RewardsTransformer implements ValueTransformer<PosInvoice, Notification> {

    private KeyValueStore<String, Double> stateStore;
    public static final  Double LOYALTY_FACTOR = 0.02;

    @Override
    public void init(ProcessorContext processorContext) {
        // Called only Once
        this.stateStore = (KeyValueStore<String, Double>) processorContext.getStateStore(RewardsApp.REWARDS_STORE_NAME);

    }

    @Override
    public Notification transform(PosInvoice posInvoice) {
        // It is called once for each Kafka message in the stream, and the message value is given. This is the place where we do all the business.
        Notification notification = new Notification()
                .withInvoiceNumber(posInvoice.getInvoiceNumber())
                .withCustomCardNo(posInvoice.getCustomCardNo())
                .withTotalAmount(posInvoice.getTotalAmount())
                .withEarnedLoyaltyPoints(posInvoice.getTotalAmount() * LOYALTY_FACTOR)
                .withTotalLoyaltyPoints(0.0);

        Double accumulatedRewards = stateStore.get(notification.getCustomCardNo());

        // Handling Null Rewards
        Double totalRewards;
        if (accumulatedRewards != null) {
            totalRewards = accumulatedRewards + notification.getEarnedLoyaltyPoints();
        } else {
            totalRewards = notification.getEarnedLoyaltyPoints();
        }

        // Storing back Rewards
        stateStore.put(notification.getCustomCardNo(), totalRewards);

        notification.setTotalLoyaltyPoints(totalRewards);
        return notification;
    }

    @Override
    public void close() {
        // Nothing to Close
    }
}
