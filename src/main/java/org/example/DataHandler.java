package org.example;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataHandler implements Handler {

    private final Client client;
    private final ExecutorService executorService;
    private int timeoutInSeconds;

    public DataHandler(Client client) {
        this.client = client;
        this.executorService = Executors.newFixedThreadPool(10);
    }

    public DataHandler(Client client, ExecutorService executorService) {
        this.client = client;
        this.executorService = executorService;
    }

    public DataHandler(Client client, ExecutorService executorService, int timeoutInSeconds) {
        this.client = client;
        this.executorService = executorService;
        this.timeoutInSeconds = timeoutInSeconds;
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(timeoutInSeconds);
    }

    @Override
    public void performOperation() {
        Event event = client.readData();
        List<Address> recipients = event.recipients();
        Payload payload = event.payload();

        recipients
                .parallelStream()
                .forEach(recipient -> {
                    executorService.submit(() -> {
                        boolean isAccepted = false;
                        while (!isAccepted) {
                            Result result = client.sendData(recipient, payload);
                            if (result == Result.ACCEPTED) {
                                isAccepted = true;
                            } else {
                                try {
                                    Thread.sleep(timeout().toMillis());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    });
                });
    }
}
