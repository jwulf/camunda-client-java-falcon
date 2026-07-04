/*
 * Copyright 2026 Josh Wulf
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.nanobpm.camunda.falcon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nano Falcon Protocol client transport.
 *
 * <p>One persistent WebSocket to a nanobpmn gateway multiplexes:
 * <ul>
 *   <li><b>createInstance</b>: correlation-tracked; the server replies with a
 *       {@code commandResult} frame carrying the same {@code corr} id and, when
 *       {@code awaitCompletion=true}, later an {@code instanceCompleted} frame.</li>
 *   <li><b>job subscriptions</b>: {@code subscribe} once; the server pushes
 *       {@code job} frames and grants an initial submission-credit window via
 *       {@code welcome}. Each ack (completeJob/failJob/throwError) implicitly
 *       replenishes one delivery credit.</li>
 * </ul>
 *
 * <p>Mirrors {@code server/src/falcon.rs} on the engine side and the Rust /
 * TypeScript SDK transports on the client side. Uses {@code java.net.http}
 * WebSocket — no additional Maven dependencies beyond JDK 17.
 *
 * <p>Failures on the initial {@link #connect() connect} are surfaced to the
 * caller so the SDK can fall back to REST (matching the JS/Rust SDK
 * behaviour).
 */
public final class FalconTransport implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(FalconTransport.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private final URI url;
  private final HttpClient http;
  private volatile WebSocket ws;
  private volatile boolean open;
  private volatile boolean closed;

  private final AtomicInteger corr = new AtomicInteger();
  private final Map<Integer, CompletableFuture<CommandResult>> pending = new ConcurrentHashMap<>();
  private final Map<Integer, CompletableFuture<InstanceCompleted>> awaits = new ConcurrentHashMap<>();
  private final Map<String, Subscription> subs = new ConcurrentHashMap<>();

  /** Server-granted admission-credit window; blocks creates when exhausted. */
  private final AtomicLong credits = new AtomicLong();
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> creditWaiters = new ConcurrentLinkedQueue<>();

  private final CompletableFuture<Void> connected = new CompletableFuture<>();

  /** Accumulate multi-frame text messages. */
  private final StringBuilder textBuffer = new StringBuilder();

  public FalconTransport(final URI falconUri) {
    this.url = falconUri;
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();
  }

  /**
   * Open the WebSocket and complete when the server has sent {@code welcome}.
   * The returned future rejects on connect / handshake failure so the caller
   * can fall back to REST.
   */
  public CompletableFuture<Void> connect() {
    if (closed) {
      return CompletableFuture.failedFuture(new IllegalStateException("falcon closed"));
    }
    if (open) {
      return CompletableFuture.completedFuture(null);
    }
    http.newWebSocketBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .buildAsync(url, new Listener())
        .whenComplete((sock, err) -> {
          if (err != null) {
            connected.completeExceptionally(
                new RuntimeException("falcon connect failed: " + url + " (" + err.getMessage() + ")", err));
          } else {
            this.ws = sock;
          }
        });
    return connected;
  }

  // -------------------------------------------------------------------------
  // Create instance
  // -------------------------------------------------------------------------

  /**
   * Create a process instance over the Falcon stream. When {@code awaitCompletion}
   * is set on {@code input}, the returned {@link CommandResult#completion}
   * completes when the server pushes {@code instanceCompleted}.
   */
  public CompletableFuture<CommandResult> createInstance(final CreateInstanceInput input) {
    return connect().thenCompose(v -> acquireCredit(input.submitTimeoutMs))
        .thenCompose(v -> doCreate(input));
  }

  private CompletableFuture<CommandResult> doCreate(final CreateInstanceInput in) {
    final int c = corr.incrementAndGet();
    final CompletableFuture<CommandResult> result = new CompletableFuture<>();
    pending.put(c, result);

    CompletableFuture<InstanceCompleted> completion = null;
    if (in.awaitCompletion) {
      completion = new CompletableFuture<>();
      awaits.put(c, completion);
    }
    final CompletableFuture<InstanceCompleted> completionRef = completion;

    final ObjectNode frame = JSON.createObjectNode();
    frame.put("type", "createInstance");
    frame.put("corr", c);
    putNullable(frame, "processDefinitionId", in.processDefinitionId);
    putNullable(frame, "processDefinitionKey", in.processDefinitionKey);
    frame.set("variables", in.variables == null ? null : JSON.valueToTree(in.variables));
    frame.put("awaitCompletion", in.awaitCompletion);
    frame.set("fetchVariables", in.fetchVariables == null ? null : JSON.valueToTree(in.fetchVariables));
    if (in.requestTimeoutMs != null) {
      frame.put("requestTimeout", in.requestTimeoutMs);
    }
    sendFrame(frame);

    return result.thenApply(r -> {
      r.completionFuture = completionRef;
      return r;
    });
  }

  private static void putNullable(final ObjectNode f, final String k, final String v) {
    if (v == null) f.putNull(k);
    else f.put(k, v);
  }

  // -------------------------------------------------------------------------
  // Job worker subscription
  // -------------------------------------------------------------------------

  public CompletableFuture<Void> subscribe(final Subscription sub) {
    subs.put(sub.jobType, sub);
    return connect().thenRun(() -> sendSubscribe(sub));
  }

  public void unsubscribe(final String jobType) {
    subs.remove(jobType);
  }

  private void sendSubscribe(final Subscription sub) {
    final ObjectNode f = JSON.createObjectNode();
    f.put("type", "subscribe");
    f.put("jobType", sub.jobType);
    f.put("jobCredits", sub.credits);
    f.put("worker", sub.workerName);
    f.put("timeout", sub.timeoutMs);
    f.set("fetchVariable", sub.fetchVariables == null ? null : JSON.valueToTree(sub.fetchVariables));
    sendFrame(f);
  }

  public CompletableFuture<CommandResult> completeJob(final String jobKey, final Map<String, Object> variables) {
    final int c = corr.incrementAndGet();
    final CompletableFuture<CommandResult> result = new CompletableFuture<>();
    pending.put(c, result);
    final ObjectNode f = JSON.createObjectNode();
    f.put("type", "completeJob");
    f.put("corr", c);
    f.put("jobKey", jobKey);
    f.set("variables", variables == null ? null : JSON.valueToTree(variables));
    sendFrame(f);
    return result;
  }

  public CompletableFuture<CommandResult> failJob(final String jobKey, final int retries, final String errorMessage) {
    final int c = corr.incrementAndGet();
    final CompletableFuture<CommandResult> result = new CompletableFuture<>();
    pending.put(c, result);
    final ObjectNode f = JSON.createObjectNode();
    f.put("type", "failJob");
    f.put("corr", c);
    f.put("jobKey", jobKey);
    f.put("retries", retries);
    putNullable(f, "errorMessage", errorMessage);
    sendFrame(f);
    return result;
  }

  public CompletableFuture<CommandResult> throwError(final String jobKey, final String errorCode, final String errorMessage) {
    final int c = corr.incrementAndGet();
    final CompletableFuture<CommandResult> result = new CompletableFuture<>();
    pending.put(c, result);
    final ObjectNode f = JSON.createObjectNode();
    f.put("type", "throwError");
    f.put("corr", c);
    f.put("jobKey", jobKey);
    f.put("errorCode", errorCode);
    putNullable(f, "errorMessage", errorMessage);
    sendFrame(f);
    return result;
  }

  public void grantJobCredits(final String jobType, final int n) {
    final ObjectNode f = JSON.createObjectNode();
    f.put("type", "jobCredits");
    f.put("jobType", jobType);
    f.put("n", n);
    sendFrame(f);
  }

  // -------------------------------------------------------------------------
  // Wire
  // -------------------------------------------------------------------------

  private void sendFrame(final ObjectNode f) {
    if (ws == null || !open) {
      LOG.debug("dropping Falcon frame; socket not open: {}", f.get("type"));
      return;
    }
    try {
      final String json = JSON.writeValueAsString(f);
      LOG.debug("falcon → {}", json);
      ws.sendText(json, true);
    } catch (final Exception e) {
      LOG.warn("falcon send failed", e);
    }
  }

  private CompletableFuture<Void> acquireCredit(final Long timeoutMs) {
    if (credits.getAndUpdate(v -> v > 0 ? v - 1 : v) > 0) {
      return CompletableFuture.completedFuture(null);
    }
    final CompletableFuture<Void> waiter = new CompletableFuture<>();
    creditWaiters.add(waiter);
    if (timeoutMs != null && timeoutMs > 0) {
      waiter.orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
          .exceptionally(t -> { creditWaiters.remove(waiter); return null; });
    }
    return waiter;
  }

  private void releaseCreditWaiters() {
    while (credits.get() > 0) {
      final CompletableFuture<Void> w = creditWaiters.poll();
      if (w == null) return;
      credits.decrementAndGet();
      w.complete(null);
    }
  }

  private void handle(final JsonNode frame) {
    final String type = frame.path("type").asText("");
    LOG.debug("falcon ← type={} raw={}", type, frame);
    switch (type) {
      case "welcome":
        open = true;
        credits.set(frame.path("submissionCredits").asLong(0));
        for (final Subscription s : subs.values()) {
          sendSubscribe(s);
        }
        releaseCreditWaiters();
        connected.complete(null);
        break;
      case "commandResult": {
        final int c = frame.path("corr").asInt(0);
        final CompletableFuture<CommandResult> p = pending.remove(c);
        if (p != null) {
          final CommandResult r = new CommandResult();
          r.status = frame.path("status").asInt(0);
          r.body = frame.path("body");
          p.complete(r);
        }
        break;
      }
      case "instanceCompleted": {
        final int c = frame.path("corr").asInt(0);
        final CompletableFuture<InstanceCompleted> cb = awaits.remove(c);
        if (cb != null) {
          final InstanceCompleted ic = new InstanceCompleted();
          ic.processCompleted = frame.path("processCompleted").asBoolean(false);
          ic.processInstanceKey = frame.path("processInstanceKey").asText("");
          ic.variables = frame.path("variables");
          cb.complete(ic);
        }
        break;
      }
      case "job": {
        final JsonNode job = frame.get("job");
        if (job == null) break;
        final String jobType = job.path("type").asText();
        final Subscription sub = subs.get(jobType);
        if (sub != null) {
          sub.onJob.accept(job);
        }
        break;
      }
      case "submissionCredits":
        credits.addAndGet(frame.path("n").asLong(0));
        releaseCreditWaiters();
        break;
      case "heartbeat":
        // reply to keep the link alive
        final ObjectNode f = JSON.createObjectNode();
        f.put("type", "heartbeat");
        sendFrame(f);
        break;
      default:
        // pressure / unknown: ignore
    }
  }

  @Override
  public void close() {
    closed = true;
    open = false;
    if (ws != null) {
      try {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
      } catch (final Exception ignored) {
        // best effort
      }
    }
    for (final CompletableFuture<CommandResult> p : pending.values()) {
      p.completeExceptionally(new RuntimeException("falcon closed"));
    }
    pending.clear();
    for (final CompletableFuture<InstanceCompleted> a : awaits.values()) {
      a.completeExceptionally(new RuntimeException("falcon closed"));
    }
    awaits.clear();
  }

  // -------------------------------------------------------------------------
  // Types
  // -------------------------------------------------------------------------

  public static final class CreateInstanceInput {
    public String processDefinitionId;
    public String processDefinitionKey;
    public Map<String, Object> variables;
    public boolean awaitCompletion;
    public java.util.List<String> fetchVariables;
    public Long requestTimeoutMs;
    public Long submitTimeoutMs;
  }

  public static final class CommandResult {
    public int status;
    public JsonNode body;
    /** Set when {@code awaitCompletion=true}; completes on {@code instanceCompleted}. */
    public CompletableFuture<InstanceCompleted> completionFuture;
  }

  public static final class InstanceCompleted {
    public boolean processCompleted;
    public String processInstanceKey;
    public JsonNode variables;
  }

  public static final class Subscription {
    public final String jobType;
    public final String workerName;
    public final int credits;
    public final long timeoutMs;
    public final java.util.List<String> fetchVariables;
    public final Consumer<JsonNode> onJob;

    public Subscription(final String jobType, final String workerName, final int credits,
                        final long timeoutMs, final java.util.List<String> fetchVariables,
                        final Consumer<JsonNode> onJob) {
      this.jobType = jobType;
      this.workerName = workerName;
      this.credits = credits;
      this.timeoutMs = timeoutMs;
      this.fetchVariables = fetchVariables;
      this.onJob = onJob;
    }
  }

  // -------------------------------------------------------------------------
  // WebSocket listener
  // -------------------------------------------------------------------------

  private final class Listener implements WebSocket.Listener {
    @Override
    public void onOpen(final WebSocket webSocket) {
      webSocket.request(1);
      // welcome frame will follow; connected future completes then.
    }

    @Override
    public CompletionStage<?> onText(final WebSocket webSocket, final CharSequence data, final boolean last) {
      textBuffer.append(data);
      if (last) {
        final String msg = textBuffer.toString();
        textBuffer.setLength(0);
        try {
          handle(JSON.readTree(msg));
        } catch (final Exception e) {
          LOG.debug("falcon: bad frame, ignoring", e);
        }
      }
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(final WebSocket webSocket, final int statusCode, final String reason) {
      open = false;
      if (!closed) {
        LOG.info("falcon socket closed (status={}, reason={})", statusCode, reason);
      }
      // Fail every outstanding await so callers unblock into the REST fallback.
      for (final CompletableFuture<CommandResult> p : pending.values()) {
        p.completeExceptionally(new RuntimeException("falcon closed"));
      }
      pending.clear();
      for (final CompletableFuture<InstanceCompleted> a : awaits.values()) {
        a.completeExceptionally(new RuntimeException("falcon closed"));
      }
      awaits.clear();
      return null;
    }

    @Override
    public void onError(final WebSocket webSocket, final Throwable error) {
      LOG.warn("falcon socket error", error);
      if (!connected.isDone()) {
        connected.completeExceptionally(error);
      }
    }
  }

  // ---- WebSocket.Listener uses j.u.c.CompletionStage; imported above. ----
}
