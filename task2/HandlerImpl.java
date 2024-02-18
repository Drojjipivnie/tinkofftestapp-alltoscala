import ru.tinkoff.task1.models.Address;
import ru.tinkoff.task1.models.Event;
import ru.tinkoff.task1.models.Payload;
import ru.tinkoff.task1.models.Result;
import ru.tinkoff.task1.service.Client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class HandlerImpl implements Handler {

    private final static Duration DEFAULT_DURATION = Duration.ofMillis(500);

    private final AtomicBoolean canAcceptEvents = new AtomicBoolean(true);

    private final Queue<FailedEventInfo> failedEventsQueue;

    private final int maxFailedEventsHolding;

    private final Client client;

    public HandlerImpl(Client client, int maxFailedEventsHolding) {
        this.client = client;
        this.failedEventsQueue = new LinkedList<>();
        this.maxFailedEventsHolding = maxFailedEventsHolding;
    }

    @Override
    public Duration timeout() {
        return DEFAULT_DURATION; // ?
    }

    @Override
    public void performOperation() {
        new Thread(this::performAsyncRejectedHandling).start();

        Event event;
        while (canAcceptEvents.get() && (event = client.readData()) != null) {
            Payload payload = event.payload();
            List<Address> recipients = event.recipients();

            for (Address recipient : recipients) {
                Result result = client.sendData(recipient, payload);
                if (result == Result.REJECTED) {
                    failedEventsQueue.add(new FailedEventInfo(recipient, payload, Instant.now().plus(timeout())));
                }
            }
        }

    }

    private void performAsyncRejectedHandling() {
        while (true) {
            canAcceptEvents.set(failedEventsQueue.size() < maxFailedEventsHolding);

            if (!failedEventsQueue.isEmpty()) {
                ArrayList<FailedEventInfo> eventsFailedToResend = new ArrayList<>();

                FailedEventInfo failedEventInfo;
                while ((failedEventInfo = failedEventsQueue.poll()) != null) {
                    if (Instant.now().isAfter(failedEventInfo.timeToResend)) {
                        Result result = client.sendData(failedEventInfo.recipient(), failedEventInfo.payload());
                        if (result == Result.ACCEPTED) {
                            continue;
                        }
                    }

                    eventsFailedToResend.add(failedEventInfo);
                }

                failedEventsQueue.addAll(eventsFailedToResend);
            }

            LockSupport.parkNanos(timeout().toNanos());
        }
    }

    private record FailedEventInfo(Address recipient, Payload payload, Instant timeToResend) {
    }
}
