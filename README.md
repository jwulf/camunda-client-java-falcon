# camunda-client-java-falcon

**Falcon-aware, drop-in replacement for [`io.camunda:camunda-client-java`](https://github.com/camunda/camunda/tree/main/clients/java).**

This artifact wraps the stock Camunda Java client and adds transparent
**Nano Falcon Protocol** support. Point your app at a Camunda 8 gateway →
you get REST/gRPC (unchanged). Point the same app at a
Nano gateway → it auto-upgrades to the
Falcon WebSocket transport (server-pushed jobs, credit-metered command
stream). Zero code changes.

## Install

Replace your existing dependency:

```xml
<!-- before -->
<dependency>
  <groupId>io.camunda</groupId>
  <artifactId>camunda-client-java</artifactId>
  <version>8.9.11</version>
</dependency>

<!-- after -->
<dependency>
  <groupId>io.github.jwulf</groupId>
  <artifactId>camunda-client-java-falcon</artifactId>
  <version>1.0.0</version>
</dependency>
```

No import changes. All `io.camunda.client.*` classes still resolve — this
jar shades the stock client with three classes patched.

## Behaviour

| Gateway                         | Transport used                    |
| ------------------------------- | --------------------------------- |
| Camunda 8 (REST preferred)      | REST                              |
| Camunda 8 (default)             | gRPC                              |
| Nano                            | **Falcon** (WebSocket)            |
| Nano + `CAMUNDA_FORCE_REST=1`   | REST                              |
| Nano + Falcon WS handshake fails | REST (sticky, one-time warn)     |

Detection is a single `GET /v2/topology` on first command; if the response
carries a `nano.falconPath` the client opens one shared WebSocket per
`CamundaClient` instance.

## Environment variables

| Variable              | Meaning                                                             |
| --------------------- | ------------------------------------------------------------------- |
| `CAMUNDA_FORCE_REST`  | Truthy = never use Falcon even against a Nano gateway.              |
| `CAMUNDA_FALCON`      | Falsy (`0`/`off`/`false`/`no`) = disable Falcon detection entirely. |

`CAMUNDA_FORCE_REST` also disables Falcon on the JS SDK
(`@nanobpm/sdk`) and the Rust SDK (`camunda-orchestration-sdk`), so the
same knob works across your polyglot fleet.

## What lands over Falcon (MVP)

* `newCreateInstanceCommand()` — the common shape: process definition,
  variables, fetch variables, no `awaitCompletion`/`tenantId`/`tags`. All
  other shapes fall through to REST for now.
* `newWorker()` — server-pushed jobs; ack commands (complete/fail/throwError)
  still go via REST on the injected `JobClient` for MVP.

Everything else uses the stock REST/gRPC paths, unchanged.

## Embedded engine (Bernd)

Beyond the auto-detected Falcon transport, this artifact also ships a
direct `NanoTransport` abstraction with two implementations:

* `NanoTransport.falcon(URI)` — remote WebSocket (wraps `FalconTransport`).
* `NanoTransport.embedded(EmbeddedEngine)` — in-process engine, no gateway
  process, via [`io.github.jwulf:nano-bernd`](https://central.sonatype.com/artifact/io.github.jwulf/nano-bernd).

The dep on `nano-bernd` is declared `<optional>` — plain Falcon users don't
pull it. To use embedded mode, add it explicitly:

```xml
<dependency>
  <groupId>io.github.jwulf</groupId>
  <artifactId>nano-bernd</artifactId>
  <version>0.2.0</version>
</dependency>
```

Then:

```java
try (var engine = EmbeddedEngine.create();
     var transport = NanoTransport.embedded(engine)) {
  engine.deploy(bpmnXml);
  var in = new FalconTransport.CreateInstanceInput();
  in.processDefinitionId = "my-process";
  in.awaitCompletion = true;
  var result = transport.createInstance(in).get();
  result.completionFuture.get(); // resolves when the instance completes
}
```

Worker code written against `NanoTransport.subscribe(...)` runs 1:1 over
either transport, so a single application can toggle remote-vs-embedded
by swapping the factory call.

Current ABI v2 limitations (see [`docs/adr/0005-embedded-engine.md`](https://github.com/jwulf/nano-bpm/blob/main/docs/adr/0005-embedded-engine.md)):
variables on create / complete are dropped, `throwError` degrades to
`failJob(retries=0)`, `awaitCompletion` polls at 50 ms.

## Building

```
mvn -DskipTests package
```

Produces `target/camunda-client-java-falcon-<version>.jar` — a single
shaded jar that includes the stock client minus 3 patched classes.

## Rebasing on new Camunda releases

See [`PATCHING.md`](./PATCHING.md).

## License

Apache-2.0. See [`LICENSE`](./LICENSE). Includes derivative works of
`io.camunda:camunda-client-java` under the same license — see
[`NOTICE`](./NOTICE).
