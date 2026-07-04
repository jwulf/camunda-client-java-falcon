/*
 * Copyright 2026 Josh Wulf
 * Apache-2.0
 */
package io.camunda.client.impl.worker;

import com.nanobpm.camunda.falcon.FalconTransport;
import io.camunda.client.api.CamundaFuture;
import io.camunda.client.api.command.ActivateJobsCommandStep1;
import io.camunda.client.api.command.CompleteJobCommandStep1;
import io.camunda.client.api.command.FailJobCommandStep1;
import io.camunda.client.api.command.StreamJobsCommandStep1;
import io.camunda.client.api.command.ThrowErrorCommandStep1;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.response.CompleteJobResponse;
import io.camunda.client.api.response.FailJobResponse;
import io.camunda.client.api.response.ThrowErrorResponse;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.impl.http.HttpCamundaFuture;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Ack-over-Falcon JobClient wrapper. Delegates {@code newActivateJobsCommand} /
 * {@code newStreamJobsCommand} to the underlying REST/gRPC client (they aren't
 * used in the Falcon push flow, and Falcon has no activate frame anyway), but
 * intercepts {@code newCompleteCommand} / {@code newFailCommand} /
 * {@code newThrowErrorCommand} so acks travel on the same WebSocket that
 * delivered the job. This closes the full job lifecycle over Falcon for
 * benchmarking parity with C8 REST/gRPC.
 *
 * <p>The command builders returned here are {@link Proxy}-backed to avoid
 * hand-writing every setter in the fluent {@code *Step1}/{@code *Step2}
 * interfaces (dozens of methods across three interfaces). Setters we care
 * about (variables, retries, errorMessage, errorCode) are captured; unknown
 * setters no-op and return the same proxy so chaining keeps working. Only
 * {@code send()} does real work.
 */
final class FalconJobClient implements JobClient {

  private final JobClient delegate;
  private final FalconTransport falcon;

  FalconJobClient(final JobClient delegate, final FalconTransport falcon) {
    this.delegate = delegate;
    this.falcon = falcon;
  }

  @Override
  public CompleteJobCommandStep1 newCompleteCommand(final long jobKey) {
    return complete(String.valueOf(jobKey));
  }

  @Override
  public CompleteJobCommandStep1 newCompleteCommand(final ActivatedJob job) {
    return complete(String.valueOf(job.getKey()));
  }

  @Override
  public FailJobCommandStep1 newFailCommand(final long jobKey) {
    return fail(String.valueOf(jobKey));
  }

  @Override
  public FailJobCommandStep1 newFailCommand(final ActivatedJob job) {
    return fail(String.valueOf(job.getKey()));
  }

  @Override
  public ThrowErrorCommandStep1 newThrowErrorCommand(final long jobKey) {
    return throwError(String.valueOf(jobKey));
  }

  @Override
  public ThrowErrorCommandStep1 newThrowErrorCommand(final ActivatedJob job) {
    return throwError(String.valueOf(job.getKey()));
  }

  @Override
  public ActivateJobsCommandStep1 newActivateJobsCommand() {
    return delegate.newActivateJobsCommand();
  }

  @Override
  public StreamJobsCommandStep1 newStreamJobsCommand() {
    return delegate.newStreamJobsCommand();
  }

  // ---------------------------------------------------------------------------
  // Proxy-backed fluent commands
  // ---------------------------------------------------------------------------

  private CompleteJobCommandStep1 complete(final String jobKey) {
    return (CompleteJobCommandStep1) makeProxy(
        new Class<?>[] {CompleteJobCommandStep1.class},
        new AckHandler(jobKey) {
          @Override
          @SuppressWarnings("unchecked")
          Object doSend() {
            final Map<String, Object> vars = variables;
            final HttpCamundaFuture<CompleteJobResponse> f = new HttpCamundaFuture<>();
            falcon
                .completeJob(jobKey, vars)
                .whenComplete((r, err) -> completeAck(f, r, err, EMPTY_COMPLETE));
            return f;
          }
        });
  }

  private FailJobCommandStep1 fail(final String jobKey) {
    return (FailJobCommandStep1) makeProxy(
        new Class<?>[] {FailJobCommandStep1.class, FailJobCommandStep1.FailJobCommandStep2.class},
        new AckHandler(jobKey) {
          @Override
          @SuppressWarnings("unchecked")
          Object doSend() {
            final HttpCamundaFuture<FailJobResponse> f = new HttpCamundaFuture<>();
            falcon
                .failJob(jobKey, retries, errorMessage)
                .whenComplete((r, err) -> completeAck(f, r, err, EMPTY_FAIL));
            return f;
          }
        });
  }

  private ThrowErrorCommandStep1 throwError(final String jobKey) {
    return (ThrowErrorCommandStep1) makeProxy(
        new Class<?>[] {
          ThrowErrorCommandStep1.class, ThrowErrorCommandStep1.ThrowErrorCommandStep2.class
        },
        new AckHandler(jobKey) {
          @Override
          @SuppressWarnings("unchecked")
          Object doSend() {
            final HttpCamundaFuture<ThrowErrorResponse> f = new HttpCamundaFuture<>();
            falcon
                .throwError(jobKey, errorCode, errorMessage)
                .whenComplete((r, err) -> completeAck(f, r, err, EMPTY_THROW));
            return f;
          }
        });
  }

  private static Object makeProxy(final Class<?>[] ifaces, final AckHandler h) {
    final Object proxy = Proxy.newProxyInstance(
        FalconJobClient.class.getClassLoader(), ifaces, h);
    h.self = proxy;
    return proxy;
  }

  private static final CompleteJobResponse EMPTY_COMPLETE = new CompleteJobResponse() {};
  private static final FailJobResponse EMPTY_FAIL = new FailJobResponse() {};
  private static final ThrowErrorResponse EMPTY_THROW = new ThrowErrorResponse() {};

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void completeAck(
      final HttpCamundaFuture f,
      final FalconTransport.CommandResult r,
      final Throwable err,
      final Object emptyResponse) {
    if (err != null) {
      f.completeExceptionally(err);
      return;
    }
    if (r.status >= 400) {
      final String msg = r.body == null ? "" : r.body.toString();
      f.completeExceptionally(new RuntimeException("Falcon ack failed: status=" + r.status + " " + msg));
      return;
    }
    f.complete(emptyResponse);
  }

  /**
   * State-collecting InvocationHandler for the three fluent ack commands.
   * Captures the setter values we care about, and dispatches on {@code send()}.
   */
  private abstract static class AckHandler implements InvocationHandler {
    final String jobKey;
    Object self;
    Map<String, Object> variables;
    int retries;
    String errorMessage;
    String errorCode;

    AckHandler(final String jobKey) {
      this.jobKey = jobKey;
    }

    abstract Object doSend();

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(final Object proxy, final Method method, final Object[] args) {
      final String name = method.getName();
      if ("send".equals(name)) {
        return doSend();
      }
      if ("sendWithResult".equals(name)) {
        // FinalCommandStep.sendWithResult() is defaulted to send().join(); we
        // preserve that contract by evaluating our future eagerly.
        return ((CamundaFuture<?>) doSend()).join();
      }
      if ("variables".equals(name) && args != null && args.length == 1) {
        variables = coerceVars(args[0]);
        return self;
      }
      if ("variable".equals(name) && args != null && args.length == 2) {
        if (variables == null) variables = new HashMap<>();
        variables.put(String.valueOf(args[0]), args[1]);
        return self;
      }
      if ("retries".equals(name) && args != null && args.length == 1) {
        retries = ((Number) args[0]).intValue();
        return self;
      }
      if ("errorMessage".equals(name) && args != null && args.length == 1) {
        errorMessage = (String) args[0];
        return self;
      }
      if ("errorCode".equals(name) && args != null && args.length == 1) {
        errorCode = (String) args[0];
        return self;
      }
      // Object methods
      if ("toString".equals(name)) return "FalconAck(jobKey=" + jobKey + ")";
      if ("hashCode".equals(name)) return System.identityHashCode(proxy);
      if ("equals".equals(name)) return proxy == args[0];
      // Default: chain-through (all other setters return the same step
      // interface for chaining — safe to no-op and return the proxy).
      return self;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> coerceVars(final Object v) {
      if (v == null) return null;
      if (v instanceof Map) return (Map<String, Object>) v;
      // Object/String/InputStream variants: Falcon carries JSON, so let the
      // caller serialize. For MVP-perf we assume Map-based variables.
      throw new IllegalArgumentException(
          "FalconJobClient: only Map<String,Object> variables are supported over Falcon in this MVP; got "
              + v.getClass().getName());
    }
  }
}
