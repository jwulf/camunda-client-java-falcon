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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nanobpm.camunda.falcon.FalconTransport;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process {@link NanoTransport} that runs the Nano engine inside the JVM via
 * {@code io.github.jwulf:nano-bernd}. Same command surface as
 * {@link FalconTransport}, so worker code written against {@code NanoTransport}
 * runs unchanged over a remote WebSocket or an embedded engine.
 *
 * <p><b>Runtime dependency:</b> requires the {@code io.github.jwulf:nano-bernd}
 * artifact on the classpath. That dep is declared {@code &lt;optional&gt;} on
 * this artifact — plain Falcon users don't pull it. Callers who want embedded
 * mode must add {@code nano-bernd} to their own {@code pom.xml} or
 * {@code build.gradle}.
 *
 * <p><b>Engine ownership:</b> this transport does <b>not</b> close the wrapped
 * engine on {@link #close()}. The caller who created the engine owns it, so a
 * single {@code EmbeddedEngine} can back multiple transports (typical case:
 * one engine per JVM, N transports per test).
 *
 * <p><b>ABI v2 limitations</b> (July 2026):
 * <ul>
 *   <li>{@code createInstance(variables)} — the engine's FFI does not accept
 *       variables on create; any {@code CreateInstanceInput.variables} map is
 *       ignored and a debug-log warning is emitted.</li>
 *   <li>{@code completeJob(variables)} — same story on the complete side.</li>
 *   <li>{@code throwError} — no engine-side counterpart; degraded to
 *       {@code failJob(retries=0, "throw: <code>: <msg>")}.</li>
 *   <li>{@code CommandResult.completionFuture} (await-completion) — polled at
 *       50 ms via the transport's scheduler until the engine reports the
 *       instance completed, then resolved with an empty variables map.</li>
 *   <li>{@code grantJobCredits} — no-op; the embedded engine has no credit
 *       protocol, workers poll on their own cadence.</li>
 * </ul>
 * These are documented in {@code docs/adr/0005-embedded-engine.md}; each will
 * unlock as later ABI versions expose them.
 *
 * <p>Reflection is used so this class compiles without the nano-bernd artifact
 * present at build time.
 */
final class EmbeddedNanoTransport implements NanoTransport {

  static final String EMBEDDED_ENGINE_CLASS = "io.github.jwulf.nano.bernd.EmbeddedEngine";
  static final String ACTIVATED_JOB_CLASS = "io.github.jwulf.nano.bernd.ActivatedJob";

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedNanoTransport.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long JOB_POLL_MS = 50L;
  private static final long COMPLETION_POLL_MS = 50L;

  // Reflected EmbeddedEngine methods (resolved once at factory time).
  private final Object engine;
  private final java.lang.reflect.Method createInstance;   // (String) -> String
  private final java.lang.reflect.Method correlateMessage; // (String, String) -> long
  private final java.lang.reflect.Method activateJobs;     // (String, String, int, long) -> List<?>
  private final java.lang.reflect.Method completeJob;      // (String) -> void
  private final java.lang.reflect.Method failJob;          // (String, int, String) -> void
  private final java.lang.reflect.Method isCompleted;      // (String) -> boolean

  // ActivatedJob reflection.
  private final java.lang.reflect.Method jobKey;
  private final java.lang.reflect.Method jobType;
  private final java.lang.reflect.Method jobInstanceKey;
  private final java.lang.reflect.Method jobElementInstanceKey;
  private final java.lang.reflect.Method jobElementId;
  private final java.lang.reflect.Method jobWorker;
  private final java.lang.reflect.Method jobDeadline;
  private final java.lang.reflect.Method jobRetries;
  private final java.lang.reflect.Method jobVariables;

  private final Map<String, SubscriptionState> subs = new ConcurrentHashMap<>();
  // Outstanding awaitCompletion futures — completed exceptionally on close()
  // so callers don't block forever if the transport is torn down mid-poll.
  private final Set<CompletableFuture<FalconTransport.InstanceCompleted>> pendingCompletions =
      ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "nano-embedded-scheduler");
        t.setDaemon(true);
        return t;
      });
  private final AtomicBoolean closed = new AtomicBoolean(false);

  static NanoTransport forEngine(final Object engine) {
    if (engine == null) throw new IllegalArgumentException("engine is null");
    final Class<?> expected;
    try {
      expected = Class.forName(EMBEDDED_ENGINE_CLASS);
    } catch (final ClassNotFoundException e) {
      throw new IllegalStateException(
          "nano-bernd is not on the classpath — add io.github.jwulf:nano-bernd to your pom to use NanoTransport.embedded()",
          e);
    }
    if (!expected.isInstance(engine)) {
      throw new IllegalArgumentException(
          "engine must be an "
              + EMBEDDED_ENGINE_CLASS
              + " instance, got "
              + engine.getClass().getName());
    }
    return new EmbeddedNanoTransport(engine);
  }

  private EmbeddedNanoTransport(final Object engine) {
    this.engine = engine;
    try {
      final Class<?> ee = engine.getClass();
      createInstance = ee.getMethod("createInstance", String.class);
      correlateMessage = ee.getMethod("correlateMessage", String.class, String.class);
      activateJobs =
          ee.getMethod("activateJobs", String.class, String.class, int.class, long.class);
      completeJob = ee.getMethod("completeJob", String.class);
      failJob = ee.getMethod("failJob", String.class, int.class, String.class);
      isCompleted = ee.getMethod("isCompleted", String.class);

      final Class<?> aj = Class.forName(ACTIVATED_JOB_CLASS);
      jobKey = aj.getMethod("key");
      jobType = aj.getMethod("type");
      jobInstanceKey = aj.getMethod("instanceKey");
      jobElementInstanceKey = aj.getMethod("elementInstanceKey");
      jobElementId = aj.getMethod("elementId");
      jobWorker = aj.getMethod("worker");
      jobDeadline = aj.getMethod("deadline");
      jobRetries = aj.getMethod("retries");
      jobVariables = aj.getMethod("variables");
    } catch (final NoSuchMethodException | ClassNotFoundException e) {
      throw new IllegalStateException(
          "nano-bernd surface incompatible with this transport (expected ABI v2)", e);
    }
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  @Override
  public CompletableFuture<Void> connect() {
    // The engine is already booted when the caller passed it in; connect is a no-op.
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return !closed.get();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    subs.values().forEach(s -> {
      if (s.pollHandle != null) s.pollHandle.cancel(false);
    });
    subs.clear();
    // Fail any awaitCompletion futures still waiting so callers unblock.
    final IllegalStateException reason = new IllegalStateException("transport closed");
    for (final CompletableFuture<FalconTransport.InstanceCompleted> f : pendingCompletions) {
      f.completeExceptionally(reason);
    }
    pendingCompletions.clear();
    scheduler.shutdownNow();
  }

  // -------------------------------------------------------------------------
  // Commands
  // -------------------------------------------------------------------------

  @Override
  public CompletableFuture<FalconTransport.CommandResult> createInstance(
      final FalconTransport.CreateInstanceInput input) {
    ensureOpen();
    if (input.processDefinitionId == null && input.processDefinitionKey == null) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("createInstance requires processDefinitionId or processDefinitionKey"));
    }
    if (input.variables != null && !input.variables.isEmpty()) {
      LOG.warn(
          "embedded transport: createInstance variables are ignored — ABI v2 does not accept them (dropped {} entries)",
          input.variables.size());
    }
    final String processId =
        input.processDefinitionId != null ? input.processDefinitionId : input.processDefinitionKey;
    final String instanceKey;
    try {
      instanceKey = (String) createInstance.invoke(engine, processId);
    } catch (final ReflectiveOperationException e) {
      return CompletableFuture.failedFuture(unwrap(e));
    }
    if (instanceKey == null || "0".equals(instanceKey)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("engine rejected createInstance(" + processId + ")"));
    }
    final FalconTransport.CommandResult result = new FalconTransport.CommandResult();
    result.status = 200;
    final ObjectNode body = JSON.createObjectNode();
    body.put("processInstanceKey", instanceKey);
    body.put("processDefinitionId", processId);
    result.body = body;

    if (input.awaitCompletion) {
      final CompletableFuture<FalconTransport.InstanceCompleted> completion =
          new CompletableFuture<>();
      pendingCompletions.add(completion);
      completion.whenComplete((v, err) -> pendingCompletions.remove(completion));
      pollForCompletion(instanceKey, completion, input.requestTimeoutMs);
      result.completionFuture = completion;
    }
    return CompletableFuture.completedFuture(result);
  }

  private void pollForCompletion(
      final String instanceKey,
      final CompletableFuture<FalconTransport.InstanceCompleted> completion,
      final Long timeoutMs) {
    final long deadline =
        timeoutMs == null ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
    scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        if (closed.get()) return;
        try {
          final boolean done = (boolean) isCompleted.invoke(engine, instanceKey);
          if (done) {
            final FalconTransport.InstanceCompleted ic = new FalconTransport.InstanceCompleted();
            ic.processInstanceKey = instanceKey;
            ic.processCompleted = true;
            ic.variables = JSON.createObjectNode();
            completion.complete(ic);
            return;
          }
        } catch (final ReflectiveOperationException e) {
          completion.completeExceptionally(unwrap(e));
          return;
        }
        if (System.currentTimeMillis() >= deadline) {
          completion.completeExceptionally(
              new java.util.concurrent.TimeoutException(
                  "instance " + instanceKey + " did not complete within " + timeoutMs + " ms"));
          return;
        }
        try {
          scheduler.schedule(this, COMPLETION_POLL_MS, TimeUnit.MILLISECONDS);
        } catch (final java.util.concurrent.RejectedExecutionException ree) {
          // Scheduler shut down between the closed-check and reschedule — close() will
          // complete the future exceptionally via pendingCompletions cleanup.
        }
      }
    }, COMPLETION_POLL_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<Void> subscribe(final FalconTransport.Subscription sub) {
    ensureOpen();
    final SubscriptionState existing = subs.get(sub.jobType);
    if (existing != null) {
      existing.subscription = sub;
      return CompletableFuture.completedFuture(null);
    }
    final SubscriptionState state = new SubscriptionState(sub);
    subs.put(sub.jobType, state);
    state.pollHandle =
        scheduler.scheduleWithFixedDelay(
            () -> pollJobs(state), 0L, JOB_POLL_MS, TimeUnit.MILLISECONDS);
    return CompletableFuture.completedFuture(null);
  }

  private void pollJobs(final SubscriptionState state) {
    if (closed.get()) return;
    final FalconTransport.Subscription sub = state.subscription;
    final List<?> jobs;
    try {
      jobs = (List<?>) activateJobs.invoke(
          engine, sub.jobType, sub.workerName, Math.max(1, sub.credits), sub.timeoutMs);
    } catch (final ReflectiveOperationException e) {
      LOG.warn("embedded activateJobs failed for {}: {}", sub.jobType, unwrap(e).getMessage());
      return;
    }
    for (final Object job : jobs) {
      if (closed.get()) return;
      try {
        sub.onJob.accept(toJobFrame(job));
      } catch (final Exception e) {
        LOG.warn("embedded onJob handler threw for jobType={}: {}", sub.jobType, e.getMessage());
      }
    }
  }

  private ObjectNode toJobFrame(final Object job) throws ReflectiveOperationException {
    final ObjectNode j = JSON.createObjectNode();
    j.put("type", (String) jobType.invoke(job));
    j.put("jobKey", (String) jobKey.invoke(job));
    j.put("processInstanceKey", (String) jobInstanceKey.invoke(job));
    j.put("elementInstanceKey", (String) jobElementInstanceKey.invoke(job));
    j.put("elementId", (String) jobElementId.invoke(job));
    j.put("worker", (String) jobWorker.invoke(job));
    j.put("deadline", (long) jobDeadline.invoke(job));
    j.put("retries", (int) jobRetries.invoke(job));
    // Fields not tracked by embedded engine (v2) — filled with defaults so
    // ActivatedJobResult deserialization on the client side succeeds.
    j.put("processDefinitionId", "");
    j.put("processDefinitionVersion", 0);
    j.put("processDefinitionKey", "0");
    j.put("tenantId", "<default>");
    j.set("customHeaders", JSON.createObjectNode());
    j.set("variables", JSON.valueToTree(jobVariables.invoke(job)));
    j.put("kind", "BPMN_ELEMENT");
    j.put("listenerEventType", "UNSPECIFIED");
    j.putNull("userTask");
    j.set("tags", JSON.createArrayNode());
    j.put("rootProcessInstanceKey", (String) jobInstanceKey.invoke(job));
    return j;
  }

  @Override
  public void unsubscribe(final String jobType) {
    final SubscriptionState state = subs.remove(jobType);
    if (state != null && state.pollHandle != null) state.pollHandle.cancel(false);
  }

  @Override
  public CompletableFuture<FalconTransport.CommandResult> completeJob(
      final String jobKey, final Map<String, Object> variables) {
    ensureOpen();
    if (variables != null && !variables.isEmpty()) {
      LOG.warn(
          "embedded transport: completeJob variables are ignored — ABI v2 does not accept them (dropped {} entries)",
          variables.size());
    }
    try {
      completeJob.invoke(engine, jobKey);
    } catch (final ReflectiveOperationException e) {
      return CompletableFuture.failedFuture(unwrap(e));
    }
    return CompletableFuture.completedFuture(ok());
  }

  @Override
  public CompletableFuture<FalconTransport.CommandResult> failJob(
      final String jobKey, final int retries, final String errorMessage) {
    ensureOpen();
    try {
      failJob.invoke(engine, jobKey, retries, errorMessage);
    } catch (final ReflectiveOperationException e) {
      return CompletableFuture.failedFuture(unwrap(e));
    }
    return CompletableFuture.completedFuture(ok());
  }

  @Override
  public CompletableFuture<FalconTransport.CommandResult> throwError(
      final String jobKey, final String errorCode, final String errorMessage) {
    // ABI v2 has no throwError; degrade to a zero-retry fail carrying the code.
    final String msg = "throw: " + errorCode + (errorMessage == null ? "" : ": " + errorMessage);
    return failJob(jobKey, 0, msg);
  }

  @Override
  public void grantJobCredits(final String jobType, final int n) {
    // Embedded engine has no credit protocol; each subscription polls at its own cadence.
  }

  /** Convenience: correlate a message. Not part of the {@link NanoTransport} contract yet. */
  public long correlateMessage(final String name, final String correlationKey) {
    ensureOpen();
    try {
      return (long) correlateMessage.invoke(engine, name, correlationKey);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(unwrap(e));
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void ensureOpen() {
    if (closed.get()) throw new IllegalStateException("transport closed");
  }

  private static FalconTransport.CommandResult ok() {
    final FalconTransport.CommandResult r = new FalconTransport.CommandResult();
    r.status = 200;
    r.body = JSON.createObjectNode();
    return r;
  }

  private static Throwable unwrap(final ReflectiveOperationException e) {
    final Throwable cause = e.getCause();
    return cause != null ? cause : e;
  }

  private static final class SubscriptionState {
    volatile FalconTransport.Subscription subscription;
    volatile ScheduledFuture<?> pollHandle;

    SubscriptionState(final FalconTransport.Subscription subscription) {
      this.subscription = subscription;
    }
  }
}
