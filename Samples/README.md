# FlySwattr.NATS Samples

This directory contains sample applications demonstrating the features of the FlySwattr.NATS library. These samples show how to implement Core NATS, JetStream, Key-Value Stores, Object Stores, and robust patterns like Dead Letter Queues (DLQ) and Request-Reply.

## Prerequisites

-   A running NATS server (with JetStream enabled).
-   .NET 8.0 SDK or later.

## 1. NatsSampler

The main interactive CLI tool for exploring NATS features. It can run in both interactive menu mode and non-interactive command-line mode.

### Usage

**Interactive Mode:**
```bash
dotnet run --project Samples/NatsSampler/NatsSampler.csproj
```

**Non-Interactive Mode:**
```bash
dotnet run --project Samples/NatsSampler/NatsSampler.csproj -- -n <command> <sub-command> [args]
```

### Commands

*   **Core Operations (`core`)**
    *   `publish [message]`: Publish a basic message.
    *   `headers [message]`: Publish a message with custom headers.
    *   `request [orderId]`: Send a request and wait for a reply (requires NatsSubscriber).
    *   `subscribe [seconds]`: Subscribe to `orders.>` for a duration.

*   **JetStream Operations (`js` / `jetstream`)**
    *   `list`: List all streams.
    *   `inspect [stream]`: View details of a specific stream.
    *   `publish [orderId]`: Publish a message with a specific ID for idempotency.
    *   `purge [stream]`: Remove all messages from a stream.
    *   `delete [stream]`: Delete a stream.

*   **Key-Value Store (`kv`)**
    *   `put <key> <value>`: Store a value (use dots `.` separators for keys, e.g., `config.app.theme`).
    *   `get <key>`: Retrieve a value.
    *   `delete <key>`: Remove a key.
    *   `list [pattern]`: List keys matching a wildcard pattern.
    *   `watch <key> [seconds]`: Watch for changes on a key.

*   **Object Store (`obj` / `object`)**
    *   `upload <file> [key]`: Upload a file.
    *   `download <key> [file]`: Download a file.
    *   `info <key>`: Get metadata for an object.
    *   `list`: List all objects.
    *   `delete <key>`: Remove an object.

*   **Dead Letter Queue (`dlq`)**
    *   `poison [reason]`: Publish a message designed to fail processing.
    *   `list`: List messages in the DLQ.
    *   `inspect <id>`: View details of a specific DLQ entry.
    *   `replay <id>`: Re-publish a failed message to its original subject.
    *   `archive <id>`: Archive a message without re-processing.
    *   `delete <id>`: Permanently remove a DLQ entry.

---

## 2. NatsSubscriber

A background service application that acts as a consumer. It listens for messages published by the Sampler and handles Request-Reply interactions.

### Usage

```bash
dotnet run --project Samples/NatsSubscriber/NatsSubscriber.csproj -- [options]
```

### Options

*   `--simulate-failures`, `-f`: Enables failure simulation mode. Messages containing "POISON" in their ID will intentionally fail to demonstrate DLQ mechanics.
*   `--subject`, `-s <pattern>`: Sets the subscription subject pattern (default: `orders.>`).

---

## 3. NatsWorker

Demonstrates the **Queue Group** pattern for load balancing. Run multiple instances of this app to see how NATS distributes tasks among available workers.

### Usage

```bash
dotnet run --project Samples/NatsWorker/NatsWorker.csproj -- [options]
```

**Interactive Controls:**
*   Press `p` to publish a batch of tasks to the queue.

### Options

*   `--worker-id`, `-w <id>`: Assign a specific ID to this worker instance (default: auto-generated).
*   `--group`, `-g <name>`: Specify the queue group name (default: `task-processors`).

---

## Running a Full Scenario

1.  **Start the Subscriber** (to handle requests and process orders):
    ```bash
    dotnet run --project Samples/NatsSubscriber/NatsSubscriber.csproj -- -f
    ```

2.  **Run the Sampler** (to generate traffic):
    ```bash
    dotnet run --project Samples/NatsSampler/NatsSampler.csproj
    ```

3.  **Test Request/Reply:**
    *   In Sampler: Select `Core Operations` -> `Request/Reply`.

4.  **Test DLQ:**
    *   In Sampler: Select `DLQ Operations` -> `Simulate Poison Message`.
    *   Observe the failure in the Subscriber window.
    *   In Sampler: `List DLQ Entries` to see the failed message.

5.  **Test Load Balancing:**
    *   Start two instances of `NatsWorker` in separate terminals.
    *   Press `p` in one of them to publish tasks.
    *   Observe tasks being distributed between the two workers.
