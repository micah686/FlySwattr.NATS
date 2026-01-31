# Dead Letter Queue (DLQ) Demonstration

This document describes how to replicate the Dead Letter Queue (DLQ) functionality using the `FlySwattr.DlqSample` application.

## Overview

The sample demonstrates a robust "Poison Message" handling flow:
1.  **Publish**: A message is published to a NATS stream.
2.  **Consume**: A consumer attempts to process the message.
3.  **Fail**: The processor identifies a "poison" message (simulated by checking for the username "PoisonIvy") and throws an exception.
4.  **Retry**: The system retries the message a configured number of times (3 times in this sample).
5.  **DLQ Move**: Upon exhausting retries, the `DefaultDlqPoisonHandler` takes over.
    *   It publishes the failed message to a designated DLQ subject (`events.dlq`).
    *   It triggers a notification.
6.  **Store**: A custom `DlqStorageNotificationService` intercepts the notification and saves the metadata to a NATS Key-Value (KV) store via `IDlqStore`.
7.  **Inspect**: The application queries `IDlqStore` to list the failed messages, proving they were successfully captured.

## How to Run

1.  **Prerequisites**: Ensure a NATS server is running on `localhost:4222` (JetStream enabled).
2.  **Run the Sample**:
    ```bash
    dotnet run --project Samples/FlySwattr.DlqSample/FlySwattr.DlqSample/FlySwattr.DlqSample.csproj
    ```

## Expected Output

You should see output similar to the following:

```text
--- Setting up Topology ---
Stream 'EVENTS' created/verified.
Stream 'DLQ' created/verified.

--- Starting Consumer Host ---
...
--- Publishing Messages ---
Published Good User: BruceWayne
Published Poison User: PoisonIvy

[Consumer] Processing User: BruceWayne (u1)
[Consumer] User BruceWayne processed successfully.
[Consumer] Processing User: PoisonIvy (u2)
[Consumer] ☠️ POISON MESSAGE DETECTED! Throwing exception for PoisonIvy...
[Consumer] Processing User: PoisonIvy (u2)
... (Retries) ...

--- Waiting for Processing & DLQ (10s) ---
info: FlySwattr.DlqSample.DlqStorageNotificationService[0]
      Received DLQ notification for EVENTS-user-processor-2. Storing in DLQ Store...
info: FlySwattr.DlqSample.DlqStorageNotificationService[0]
      Successfully stored DLQ entry EVENTS-user-processor-2 in KV store.

--- Inspecting DLQ Store ---
Found 1 messages in DLQ Store:
 - ID: EVENTS-user-processor-2
   Stream/Consumer: EVENTS/user-processor
   Reason: I am allergic to Ivy!
   Stored At: 1/22/2026 10:00:05 AM +00:00
   -----------------------------

--- Stopping Host ---
```

## Key Components

-   **`Program.cs`**: Orchestrates the setup, publishing, and listing logic.
-   **`DlqStorageNotificationService.cs`**: Connects the transient DLQ event to the persistent `IDlqStore`.
-   **`UserCreatedEvent.cs`**: The data contract for the sample message.