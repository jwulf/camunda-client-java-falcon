/*
 * Copyright 2026 Josh Wulf
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.camunda.client.impl.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.nanobpm.camunda.falcon.FalconTransport;
import io.camunda.client.api.JsonMapper;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobHandler;
import io.camunda.client.api.worker.JobWorker;
import io.camunda.client.impl.response.ActivatedJobImpl;
import io.camunda.client.protocol.rest.ActivatedJobResult;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Falcon-backed {@link JobWorker}: server-pushes jobs over the shared
 * Falcon WebSocket; ack (complete/fail/throwError) commands issued from
 * the user's handler go through the injected {@link JobClient}, which for
 * MVP still routes via REST (a follow-up will short-circuit acks through
 * the WebSocket for full round-trip savings).
 *
 * <p>If {@link #open()} fails, {@link JobWorkerBuilderImpl} catches the
 * exception and falls back to the stock REST/gRPC worker.
 */
final class FalconJobWorker implements JobWorker, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(FalconJobWorker.class);

  private final FalconTransport falcon;
  private final String jobType;
  private final String workerName;
  private final int maxJobsActive;
  private final long timeoutMs;
  private final List<String> fetchVariables;
  private final JobHandler handler;
  private final JobClient jobClient;
  private final ExecutorService jobHandlingExecutor;
  private final JsonMapper jsonMapper;

  private final AtomicBoolean open = new AtomicBoolean(false);

  FalconJobWorker(
      final FalconTransport falcon,
      final String jobType,
      final String workerName,
      final int maxJobsActive,
      final long timeoutMs,
      final List<String> fetchVariables,
      final JobHandler handler,
      final JobClient jobClient,
      final ExecutorService jobHandlingExecutor,
      final JsonMapper jsonMapper) {
    this.falcon = falcon;
    this.jobType = jobType;
    this.workerName = workerName;
    this.maxJobsActive = maxJobsActive;
    this.timeoutMs = timeoutMs;
    this.fetchVariables = fetchVariables;
    this.handler = handler;
    this.jobClient = jobClient;
    this.jobHandlingExecutor = jobHandlingExecutor;
    this.jsonMapper = jsonMapper;
  }

  void open() {
    final FalconTransport.Subscription sub = new FalconTransport.Subscription(
        jobType, workerName, maxJobsActive, timeoutMs, fetchVariables, this::onJobFrame);
    falcon.subscribe(sub).join();
    open.set(true);
    LOG.info("Falcon worker opened: jobType={} workerName={} credits={}",
        jobType, workerName, maxJobsActive);
  }

  private void onJobFrame(final JsonNode frame) {
    if (!open.get()) return;
    jobHandlingExecutor.execute(() -> dispatch(frame));
  }

  private void dispatch(final JsonNode frame) {
    final ActivatedJob job;
    try {
      final JsonNode jobNode = frame.has("job") ? frame.get("job") : frame;
      final ActivatedJobResult rest =
          jsonMapper.fromJson(jobNode.toString(), ActivatedJobResult.class);
      job = new ActivatedJobImpl(jsonMapper, rest);
    } catch (final Exception e) {
      LOG.warn("Falcon job frame parse failed: {}", e.getMessage());
      return;
    }
    try {
      handler.handle(new FalconJobClient(jobClient, falcon), job);
    } catch (final Exception userError) {
      LOG.warn("Falcon job handler threw for jobKey={} ({}); auto-failing",
          job.getKey(), userError.toString());
      try {
        falcon.failJob(
            String.valueOf(job.getKey()),
            Math.max(0, job.getRetries() - 1),
            userError.getMessage() == null
                ? userError.getClass().getSimpleName()
                : userError.getMessage());
      } catch (final Exception ignored) {
        // best effort — engine will time out the job at its deadline
      }
    }
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }

  @Override
  public void close() {
    if (open.compareAndSet(true, false)) {
      try {
        falcon.unsubscribe(jobType);
      } catch (final Exception ignored) {
        // best effort
      }
    }
  }
}
