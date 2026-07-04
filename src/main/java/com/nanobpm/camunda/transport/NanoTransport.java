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

import com.nanobpm.camunda.falcon.FalconTransport;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Client transport to a Nano engine. Two implementations ship in this artifact:
 *
 * <ul>
 *   <li>{@link #falcon(URI)} — long-lived WebSocket to a remote {@code nanobpmn}
 *       gateway. Uses the Falcon Protocol for createInstance + job worker
 *       multiplexing. Backed by {@link FalconTransport}.</li>
 *   <li>{@link #embedded(Object)} — in-process engine (codename <b>Bernd</b>)
 *       running the Nano wasm blob inside the JVM via
 *       {@code io.github.jwulf:nano-bernd}. No network, no gateway process.
 *       Requires the {@code nano-bernd} artifact on the runtime classpath
 *       (declared {@code &lt;optional&gt;} on this artifact).</li>
 * </ul>
 *
 * <p>Both implementations expose the same lifecycle so callers can swap
 * transports without changing worker or command-code paths — see the nano-ide
 * JVM template for the same-source remote-vs-embedded pattern.
 *
 * <p>The {@link #embedded(Object)} factory accepts {@code Object} rather than
 * a strongly-typed {@code EmbeddedEngine} parameter so this class compiles
 * even when {@code nano-bernd} is not on the classpath. The instance is
 * validated by reflection and rejected with a clear message when the required
 * type is missing.
 */
public interface NanoTransport extends AutoCloseable {

  /** Establish the underlying connection (WebSocket handshake, or engine boot). */
  CompletableFuture<Void> connect();

  /**
   * Whether the transport is available to accept commands. The exact
   * semantics vary by implementation:
   *
   * <ul>
   *   <li>{@code FalconTransport} — {@code true} only after the WebSocket
   *       has completed its {@code welcome} handshake with the gateway, and
   *       {@code false} once {@link #close()} is called or the socket drops.</li>
   *   <li>{@code EmbeddedNanoTransport} — {@code true} from construction
   *       until {@link #close()} (the wrapped {@code EmbeddedEngine} is
   *       assumed to be ready when the caller passes it in).</li>
   * </ul>
   *
   * In both cases {@code isOpen() == false} means the transport will reject
   * further command calls (usually with {@code IllegalStateException}).
   */
  boolean isOpen();

  CompletableFuture<FalconTransport.CommandResult> createInstance(
      FalconTransport.CreateInstanceInput input);

  CompletableFuture<Void> subscribe(FalconTransport.Subscription subscription);

  void unsubscribe(String jobType);

  CompletableFuture<FalconTransport.CommandResult> completeJob(
      String jobKey, Map<String, Object> variables);

  CompletableFuture<FalconTransport.CommandResult> failJob(
      String jobKey, int retries, String errorMessage);

  CompletableFuture<FalconTransport.CommandResult> throwError(
      String jobKey, String errorCode, String errorMessage);

  void grantJobCredits(String jobType, int n);

  @Override
  void close();

  /** Remote Falcon transport to the given WebSocket URI. */
  static NanoTransport falcon(final URI wsUri) {
    return new FalconTransport(wsUri);
  }

  /**
   * In-process Nano engine transport. The argument must be an
   * {@code io.github.jwulf.nano.bernd.EmbeddedEngine} instance; users are
   * responsible for its lifecycle (this transport does <b>not</b> close the
   * engine on {@link #close()}, so a single engine can back multiple
   * transports).
   *
   * @throws IllegalStateException if {@code nano-bernd} is not on the classpath
   * @throws IllegalArgumentException if {@code engine} is not an
   *     {@code EmbeddedEngine} instance
   */
  static NanoTransport embedded(final Object engine) {
    return EmbeddedNanoTransport.forEngine(engine);
  }
}
