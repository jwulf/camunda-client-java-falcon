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

/**
 * Environment-variable / system-property gates for the Falcon transport.
 *
 * <ul>
 *   <li>{@code CAMUNDA_FORCE_REST} — if truthy, Falcon is never used even
 *       when the gateway advertises it (escape hatch for proxy-blocked WS
 *       environments). Matches the JS and Rust SDK semantics.</li>
 *   <li>{@code CAMUNDA_FALCON} — legacy alias; falsy values disable Falcon.
 *       {@code CAMUNDA_FORCE_REST} wins when both are set.</li>
 * </ul>
 *
 * Values are read from the process environment, falling back to system
 * properties (for tests). Truthy = anything that is not empty / {@code 0} /
 * {@code off} / {@code false} / {@code no} (case-insensitive).
 */
public final class FalconConfig {

  private FalconConfig() {}

  public static boolean falconEnabled() {
    if (isTruthy(getVar("CAMUNDA_FORCE_REST"))) {
      return false;
    }
    final String falcon = getVar("CAMUNDA_FALCON");
    if (falcon == null) {
      return true;
    }
    return isTruthy(falcon);
  }

  private static String getVar(final String key) {
    final String env = System.getenv(key);
    if (env != null && !env.isEmpty()) {
      return env;
    }
    return System.getProperty(key);
  }

  private static boolean isTruthy(final String v) {
    if (v == null) {
      return false;
    }
    switch (v.trim().toLowerCase()) {
      case "":
      case "0":
      case "off":
      case "false":
      case "no":
        return false;
      default:
        return true;
    }
  }
}
