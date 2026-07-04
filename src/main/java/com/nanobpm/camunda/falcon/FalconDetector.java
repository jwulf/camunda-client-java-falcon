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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

/**
 * Probe {@code GET /v2/topology} for a nanobpmn gateway. When the response
 * body contains a top-level {@code "nano"} object with a {@code falconPath}
 * string, this gateway supports the Falcon Protocol.
 *
 * <p>Detection is a single HTTP call. Callers cache the result; the SDK
 * probes at most once per {@code CamundaClient} instance.
 */
public final class FalconDetector {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** The default Falcon WebSocket path when the server does not advertise one. */
  public static final String DEFAULT_FALCON_PATH = "/falcon";

  private FalconDetector() {}

  /**
   * Probe the configured REST address for Nano advertisement. Returns
   * {@link NanoInfo} when the server is a nanobpmn gateway; {@code null}
   * for stock Camunda 8, unreachable servers, {@code https://} addresses
   * (this MVP is plaintext-only), or any error.
   */
  public static NanoInfo probe(final URI restAddress) {
    if (restAddress == null) {
      return null;
    }
    if ("https".equalsIgnoreCase(restAddress.getScheme())) {
      return null;
    }
    try {
      final URI topology = normalise(restAddress).resolve("v2/topology");
      final HttpClient http = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(3))
          .build();
      final HttpRequest req = HttpRequest.newBuilder(topology)
          .timeout(Duration.ofSeconds(5))
          .header("Accept", "application/json")
          .GET()
          .build();
      final String body = http.send(req, BodyHandlers.ofString()).body();
      if (body == null || body.isEmpty()) {
        return null;
      }
      final JsonNode root = JSON.readTree(body);
      final JsonNode nano = root.get("nano");
      if (nano == null || nano.isNull()) {
        return null;
      }
      final String falconPath = nano.hasNonNull("falconPath")
          ? nano.get("falconPath").asText()
          : DEFAULT_FALCON_PATH;
      final String engine = nano.hasNonNull("engine")
          ? nano.get("engine").asText()
          : "nanobpmn";
      final String version = nano.hasNonNull("version")
          ? nano.get("version").asText()
          : null;
      final URI ws = buildFalconUri(restAddress, falconPath);
      return new NanoInfo(engine, version, ws);
    } catch (final Exception e) {
      return null;
    }
  }

  /** Normalise so {@code resolve("v2/...")} works irrespective of trailing slash. */
  static URI normalise(final URI restAddress) {
    final String s = restAddress.toString();
    if (s.endsWith("/")) {
      return restAddress;
    }
    return URI.create(s + "/");
  }

  /** Build the {@code ws://host:port<path>} URI from a REST base and path. */
  static URI buildFalconUri(final URI restAddress, final String path) {
    final String scheme = "https".equalsIgnoreCase(restAddress.getScheme()) ? "wss" : "ws";
    final int port = restAddress.getPort();
    final StringBuilder sb = new StringBuilder(scheme).append("://").append(restAddress.getHost());
    if (port > 0) {
      sb.append(':').append(port);
    }
    sb.append(path.startsWith("/") ? path : "/" + path);
    return URI.create(sb.toString());
  }

  /** What the probe learned about a nanobpmn gateway. */
  public static final class NanoInfo {
    public final String engine;
    public final String version; // nullable
    public final URI falconUri;

    NanoInfo(final String engine, final String version, final URI falconUri) {
      this.engine = engine;
      this.version = version;
      this.falconUri = falconUri;
    }
  }
}
