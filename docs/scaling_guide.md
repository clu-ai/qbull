# Scaling Guide for the QBull Worker System

## Introduction

As your system's workload increases (more jobs to process), you need a way to scale your workers' capacity to maintain good performance and prevent bottlenecks.

This system uses partitioned Redis Streams. A key concept is that **job processing *within* a single partition is sequential**, performed by the single consumer assigned to it. Therefore, to increase overall parallel processing, we need to distribute the work across *multiple partitions* and have *multiple workers* processing them concurrently.

## Core Concept: Horizontal Scaling via Partitioning

The primary strategy for scaling this system is **partition-based horizontal scaling**. It works like this:

1.  **Divide Work:** The job queue is logically divided into multiple separate "channels" or **partitions**. Each partition is an independent Redis Stream (e.g., `WHATSAPP:0`, `WHATSAPP:1`, ... `WHATSAPP:N-1`).
2.  **Distribute Jobs:** Each new job is assigned to a specific partition using a key (e.g., the recipient's phone number) and a hash function (`get_partition`). Ideally, different keys are distributed across different partitions.
3.  **Concurrent Processing:** Multiple **workers** (instances of the `run_workers.py` script, typically running in separate **Pods**) execute in parallel. Each worker acquires exclusive responsibility for processing a set of partitions.
4.  **Increased Parallelism:** By increasing the number of partitions and the number of workers, more jobs (with different keys) can be processed simultaneously by different workers on different partitions.

## Recommended Strategy: More Partitions, More Workers (Pods)

The most effective and resilient way to scale is to **increase the total number of partitions and run more worker instances (Pods), with each worker handling a relatively small number of partitions.**

* **Goal:** Maximize concurrency by distributing the load across many independent processing units and minimizing the impact if one unit fails.
* **Mechanism:**
    * Increase the total number of partitions (`PARTITIONS` in the configuration).
    * Keep the number of partitions per worker/Pod low (`PARTITIONS_PER_POD` in the configuration, e.g., 1, 2, or 4).
    * Run a sufficient number of `run_workers.py` instances (Pods) to cover the desired number of partitions.

* **Analogy:** Think of a supermarket. Instead of having 2 super-fast checkout lanes (2 workers handling many partitions each), it's better to have many standard lanes (many workers handling few partitions each). If one standard lane closes, the overall impact on the queue is smaller.

## Key Configuration

The most important variables for this scaling approach are defined in `config.py` or via environment variables:

* `PARTITIONS`:
    * **What it is:** The total number of partitions (Redis Streams) the queue will be divided into.
    * **Impact:** Defines the **maximum possible level of parallelism**. If you have 16 partitions, at most 16 consumers can be processing different jobs concurrently.
    * **Consideration:** Choose a number based on your expected load and desired parallelism. It's better to slightly overestimate than significantly underestimate. Changing this number in a running production system requires planning (potential migration or loss of unprocessed messages in old streams if not handled carefully). Values like 16, 32, 64, 128 are common.

* `PARTITIONS_PER_POD`:
    * **What it is:** How many partitions each worker instance (Pod) will attempt to acquire when it starts.
    * **Impact:** Determines how many workers you need to cover all partitions (`Number of Pods ≈ PARTITIONS / PARTITIONS_PER_POD`). It also affects the load on each Pod and resilience.
    * **Recommendation:** Keeping this number **low** (e.g., 1, 2, 4) is generally preferable. This distributes the load better and reduces the impact if a Pod fails (only a few partitions are affected).

## Deployment Examples (Conceptual)

Assuming each instance of `run_workers.py` runs in a Pod:

**Scenario 1: Moderate Load**

* You want to process up to 16 different jobs in parallel.
* You decide each Pod should handle 2 partitions for good resilience.
* **Configuration:**
    * `PARTITIONS = 16`
    * `PARTITIONS_PER_POD = 2`
* **Deployment:** You will need to run **8 Pods** (`16 / 2 = 8`).

**Scenario 2: High Load**

* You need a high degree of parallelism, say up to 64.
* Each Pod will handle 4 partitions.
* **Configuration:**
    * `PARTITIONS = 64`
    * `PARTITIONS_PER_POD = 4`
* **Deployment:** You will need to run **16 Pods** (`64 / 4 = 16`).

**Scenario 3: Low Load / Simplicity**

* You only need basic parallelism of 8.
* You want maximum granularity, with each Pod handling 1 partition.
* **Configuration:**
    * `PARTITIONS = 8`
    * `PARTITIONS_PER_POD = 1`
* **Deployment:** You will need to run **8 Pods**.

## Benefits of this Strategy

* **Increased Throughput:** More jobs processed per unit of time due to parallel execution.
* **Improved Resilience:** Failure of one Pod/worker only affects a small number of partitions temporarily until another worker can take over (or their TTLs expire).
* **Better Resource Utilization:** CPU/Memory/Network load is spread across multiple machines/Pods.
* **Flexible Scalability:** Easy to scale capacity up or down by adjusting the number of running Pods/worker instances (e.g., using Kubernetes Horizontal Pod Autoscaler), provided `PARTITIONS` is sufficiently high.

## Additional Considerations

* **Choosing `PARTITIONS`:** This is an important decision. A higher number allows for more future scalability but adds minor overhead in Redis (more streams, more lock keys). It's difficult to change without impact once in production.
* **Choosing `PARTITIONS_PER_POD`:** Small values (1-4) are generally more robust and balance the load better.
* **Orchestration:** You will need a way to run and manage multiple instances of `run_workers.py` (Kubernetes, Docker Swarm, systemd, supervisord, etc.).
* **Redis Capacity:** Ensure your Redis instance can handle the increased number of streams, keys, and potentially connections generated by your workers.
* **Key Distribution:** While hashing distributes keys well on average, if you have "hot keys" (very few phone numbers receive the vast majority of messages), those specific partitions can still become bottlenecks. More partitions help dilute this effect.

## Alternative (Advanced)

It is technically possible to have multiple consumers *within the same worker* reading from the *same partition* using Redis Consumer Group capabilities natively. This could help if a specific partition is a persistent bottleneck. However, it adds complexity to managing processes/tasks within the worker and is generally considered a secondary optimization compared to scaling by increasing the number of partitions and workers. The primary and recommended strategy is the one described above.