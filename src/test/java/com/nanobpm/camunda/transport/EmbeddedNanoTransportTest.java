/*
 * Copyright 2026 Josh Wulf
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.nanobpm.camunda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.nanobpm.camunda.falcon.FalconTransport;
import io.github.jwulf.nano.bernd.EmbeddedEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for {@link NanoTransport#embedded(Object)}. Drives a real
 * {@link EmbeddedEngine} through the transport surface a
 * {@link com.nanobpm.camunda.falcon.FalconTransport} exposes so worker-side
 * code can be shared 1:1 between remote and in-process modes.
 */
class EmbeddedNanoTransportTest {

  private static final String TRIVIAL_BPMN =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
        <bpmn:process id="p" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f</bpmn:outgoing></bpmn:startEvent>
          <bpmn:endEvent id="e"><bpmn:incoming>f</bpmn:incoming></bpmn:endEvent>
          <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static final String SERVICE_BPMN =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="svc" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
          <bpmn:serviceTask id="t" name="Do work">
            <bpmn:extensionElements><zeebe:taskDefinition type="do-work" /></bpmn:extensionElements>
            <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
          <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>
      """;

  @Test
  void factory_rejects_non_embedded_engine_argument() {
    assertThatThrownBy(() -> NanoTransport.embedded("not an engine"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("EmbeddedEngine");
  }

  @Test
  void createInstance_and_await_completion_on_straight_through_process() throws Exception {
    try (EmbeddedEngine engine = EmbeddedEngine.create();
        NanoTransport transport = NanoTransport.embedded(engine)) {

      transport.connect().get(2, TimeUnit.SECONDS);
      engine.deploy(TRIVIAL_BPMN);

      final FalconTransport.CreateInstanceInput in = new FalconTransport.CreateInstanceInput();
      in.processDefinitionId = "p";
      in.awaitCompletion = true;
      in.requestTimeoutMs = 5_000L;

      final FalconTransport.CommandResult res =
          transport.createInstance(in).get(5, TimeUnit.SECONDS);
      assertThat(res.status).isEqualTo(200);
      assertThat(res.body.get("processInstanceKey").asText()).isNotBlank();

      final FalconTransport.InstanceCompleted done =
          res.completionFuture.get(5, TimeUnit.SECONDS);
      assertThat(done.processCompleted).isTrue();
    }
  }

  @Test
  void subscribe_activates_completes_jobs_end_to_end() throws Exception {
    try (EmbeddedEngine engine = EmbeddedEngine.create();
        NanoTransport transport = NanoTransport.embedded(engine)) {

      transport.connect().get(2, TimeUnit.SECONDS);
      engine.deploy(SERVICE_BPMN);
      final String instanceKey = engine.createInstance("svc");

      final CountDownLatch received = new CountDownLatch(1);
      final List<String> keys = new ArrayList<>();

      final FalconTransport.Subscription sub =
          new FalconTransport.Subscription(
              "do-work", "worker-1", 10, 30_000L, null,
              (JsonNode job) -> {
                keys.add(job.get("jobKey").asText());
                received.countDown();
              });

      transport.subscribe(sub).get(2, TimeUnit.SECONDS);
      assertThat(received.await(5, TimeUnit.SECONDS))
          .as("worker should receive an activated job within 5s")
          .isTrue();

      final String jobKey = keys.get(0);
      transport.completeJob(jobKey, null).get(2, TimeUnit.SECONDS);
      transport.unsubscribe("do-work");

      // Wait for the engine to advance past the completed job to the end event.
      final long deadline = System.currentTimeMillis() + 5_000L;
      while (System.currentTimeMillis() < deadline && !engine.isCompleted(instanceKey)) {
        Thread.sleep(25);
      }
      assertThat(engine.isCompleted(instanceKey)).isTrue();
    }
  }

  @Test
  void failJob_propagates_to_engine() throws Exception {
    try (EmbeddedEngine engine = EmbeddedEngine.create();
        NanoTransport transport = NanoTransport.embedded(engine)) {

      transport.connect().get(2, TimeUnit.SECONDS);
      engine.deploy(SERVICE_BPMN);
      engine.createInstance("svc");

      final CountDownLatch received = new CountDownLatch(1);
      final List<String> keys = new ArrayList<>();
      final FalconTransport.Subscription sub =
          new FalconTransport.Subscription(
              "do-work", "worker-1", 5, 30_000L, null,
              j -> {
                keys.add(j.get("jobKey").asText());
                received.countDown();
              });
      transport.subscribe(sub).get(2, TimeUnit.SECONDS);
      assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();

      transport.failJob(keys.get(0), 2, "transient").get(2, TimeUnit.SECONDS);
      transport.unsubscribe("do-work");
      // failJob succeeded => no exception from the engine.
    }
  }

  @Test
  void close_stops_polling_and_flips_isOpen() {
    try (EmbeddedEngine engine = EmbeddedEngine.create()) {
      final NanoTransport transport = NanoTransport.embedded(engine);
      assertThat(transport.isOpen()).isTrue();
      transport.close();
      assertThat(transport.isOpen()).isFalse();
      assertThatThrownBy(() -> transport.createInstance(new FalconTransport.CreateInstanceInput()))
          .isInstanceOf(IllegalStateException.class);
    }
  }
}
