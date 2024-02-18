import ru.tinkoff.task1.models.ApplicationStatusResponse;
import ru.tinkoff.task1.models.Response;
import ru.tinkoff.task1.service.Client;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.concurrent.*;

public class HandlerImpl implements Handler {

    private final static long PERFORM_GLOBAL_TIMEOUT_MILLIS = 15_000;

    private final Client client;

    private final ExecutorService executorService;

    public HandlerImpl(Client client, ExecutorService executorService) {
        this.client = client;
        this.executorService = executorService;
    }


    @Override
    public ApplicationStatusResponse performOperation(String id) {
        return performOperation(id, PERFORM_GLOBAL_TIMEOUT_MILLIS, 0);
    }

    private ApplicationStatusResponse performOperation(String id, long timeoutMs, int retryCounter) {
        Instant timeBeforeExecution = Instant.now();

        CompletableFuture<Response> getApplicationStatus1Future = CompletableFuture.supplyAsync(() -> client.getApplicationStatus1(id), executorService);
        CompletableFuture<Response> getApplicationStatus2Future = CompletableFuture.supplyAsync(() -> client.getApplicationStatus2(id), executorService);

        Response response;
        try {
            response = (Response) CompletableFuture.anyOf(getApplicationStatus1Future, getApplicationStatus2Future).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return new ApplicationStatusResponse.Failure(durationPassedSince(timeBeforeExecution), retryCounter);
        }

        if (response instanceof Response.Failure) {
            return new ApplicationStatusResponse.Failure(durationPassedSince(timeBeforeExecution), retryCounter);
        }

        if (response instanceof Response.Success successfulResponse) {
            return new ApplicationStatusResponse.Success(successfulResponse.applicationId(), successfulResponse.applicationStatus());
        }

        // only retry left
        Response.RetryAfter retryAfterResponse = (Response.RetryAfter) response;

        Duration retryDelay = retryAfterResponse.delay();
        Duration durationPassedSince = durationPassedSince(timeBeforeExecution);
        long newTimeout = timeoutMs - durationPassedSince.toMillis();

        if (newTimeout - retryDelay.toMillis() <= 0) {
            return new ApplicationStatusResponse.Failure(durationPassedSince(timeBeforeExecution), retryCounter);
        } else {
            return performOperation(id, newTimeout, retryCounter + 1);
        }
    }

    private Duration durationPassedSince(Temporal fromTime) {
        return Duration.between(fromTime, Instant.now());
    }

}
