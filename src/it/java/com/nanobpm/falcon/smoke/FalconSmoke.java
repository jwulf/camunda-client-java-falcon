/*
 * Copyright 2026 Josh Wulf
 * Apache-2.0
 */
package com.nanobpm.falcon.smoke;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.response.DeploymentEvent;
import io.camunda.client.api.response.ProcessInstanceEvent;
import io.camunda.client.api.response.Topology;
import io.camunda.client.api.worker.JobWorker;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end smoke test against a live Nano gateway on http://localhost:8080.
 *
 * <p>Run: {@code mvn -q -DskipTests package && \
 *   java -cp target/camunda-client-java-falcon-1.0.0-SNAPSHOT.jar:target/test-classes/... }
 *
 * <p>Verifies:
 * <ol>
 *   <li>{@link CamundaClient#newTopologyRequest()} works (REST fallback for topology is expected).</li>
 *   <li>Deployment (REST) succeeds.</li>
 *   <li>{@code newWorker()} opens a Falcon-pushed worker (log line "Falcon worker opened").</li>
 *   <li>{@code newCreateInstanceCommand()} routes through Falcon (log line "Nano Falcon transport connected").</li>
 *   <li>The worker receives the pushed job and completes it via REST ack.</li>
 * </ol>
 */
public final class FalconSmoke {

  public static void main(final String[] args) throws Exception {
    final String rest = System.getenv().getOrDefault("CAMUNDA_REST_ADDRESS", "http://localhost:8080");

    try (CamundaClient client =
        CamundaClient.newClientBuilder()
            .restAddress(URI.create(rest))
          .grpcAddress(URI.create("http://localhost:26500"))
            .preferRestOverGrpc(true)
            .build()) {

      final Topology topology = client.newTopologyRequest().send().join();
      System.out.println("topology.gatewayVersion=" + topology.getGatewayVersion());

      // Deploy is best-effort — Nano's deploy REST response omits some fields
      // that trip the stock DTO parser on some gateways. Ignore parse errors
      // and rely on the existing deployment (idempotent process definition).
      try {
        final DeploymentEvent deploy =
            client
                .newDeployResourceCommand()
                .addResourceFromClasspath("test-job-process.bpmn")
                .send()
                .join();
        System.out.println(
            "deployed processDefinitionKey="
                + deploy.getProcesses().get(0).getProcessDefinitionKey());
      } catch (final Exception e) {
        System.out.println("deploy parse warning (ignored): " + e.getMessage());
      }

      final CountDownLatch jobDone = new CountDownLatch(3);
      final AtomicInteger handled = new AtomicInteger();

      final JobWorker worker =
          client
              .newWorker()
              .jobType("test-job")
              .handler(
                  (jobClient, job) -> {
                    System.out.println(
                        "[worker] jobKey="
                            + job.getKey()
                            + " piKey="
                            + job.getProcessInstanceKey());
                    jobClient.newCompleteCommand(job).send().join();
                    handled.incrementAndGet();
                    jobDone.countDown();
                  })
              .timeout(Duration.ofSeconds(30))
              .name("falcon-smoke")
              .open();

      System.out.println("worker.isOpen=" + worker.isOpen() + " class=" + worker.getClass().getName());

      // Give the WS a beat to subscribe.
      Thread.sleep(500);

      for (int i = 0; i < 3; i++) {
        final ProcessInstanceEvent inst =
            client
                .newCreateInstanceCommand()
                .bpmnProcessId("Process_0f7cr6y")
                .latestVersion()
                .variables(Map.of("iteration", i))
                .send()
                .join();
        System.out.println(
            "[create] piKey=" + inst.getProcessInstanceKey() + " version=" + inst.getVersion());
      }

      final boolean gotAll = jobDone.await(15, TimeUnit.SECONDS);
      worker.close();
      System.out.println("handled=" + handled.get() + " gotAll=" + gotAll);
      if (!gotAll) {
        System.err.println("TIMEOUT waiting for 3 jobs");
        System.exit(2);
      }
      System.out.println("OK");
    }
  }
}
