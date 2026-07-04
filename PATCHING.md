# Rebasing on new Camunda Client releases

This artifact vendors three files from `io.camunda:camunda-client-java`
and applies surgical Falcon patches to each. When Camunda ships a new
version, follow this recipe.

## Files we patch

1. `io/camunda/client/impl/CamundaClientImpl.java` — Falcon state
   (`falconInfo`, `falconProbed`, `falconDead`, `falconTransport`),
   `falconTransport()` / `jsonMapper()` accessors, `close()` calls
   `falconTransport.close()` first, `newCreateInstanceCommand()` and
   `newWorker()` pass `this` as an extra arg.
2. `io/camunda/client/impl/command/CreateProcessInstanceCommandImpl.java`
   — extra 7th constructor param `CamundaClientImpl falconAwareClient`
   (backward-compatible overload retained), `send()` prefers Falcon when
   available, `sendFalconRequest()` helper.
3. `io/camunda/client/impl/worker/JobWorkerBuilderImpl.java` — extra 6th
   constructor param, `open()` prefers `FalconJobWorker` when transport is
   up (falls through on failure).

## Recipe

1. Bump `<camunda.version>` in `pom.xml`.
2. `cd ~/workspace/camunda && git fetch --tags && git checkout <new-tag>`.
3. Re-extract stock copies of the three files:
   ```
   for f in \
     clients/java/src/main/java/io/camunda/client/impl/CamundaClientImpl.java \
     clients/java/src/main/java/io/camunda/client/impl/command/CreateProcessInstanceCommandImpl.java \
     clients/java/src/main/java/io/camunda/client/impl/worker/JobWorkerBuilderImpl.java; do
     dst="../camunda-client-java-falcon/src/main/java/${f#clients/java/src/main/java/}"
     git show HEAD:$f > "$dst.stock.new"
   done
   ```
4. `diff` each `.stock.new` against the currently patched file and
   re-apply the marked patch blocks (each patch block in the current
   files is preceded by a `// -------------------- Falcon ...` marker
   comment).
5. `mvn -DskipTests package` — inspect `target/*.jar` for the three
   patched classes (there should be exactly one copy of each in the
   shaded jar — grep with `unzip -l`).
6. Run the integration tests.
7. Bump `<version>` in `pom.xml`, tag `vX.Y.Z`, push.

## Tips

* Every patch site in the three files carries a `Falcon` marker comment
  — grep for `Falcon` to enumerate them.
* Every reference to our own package (`com.nanobpm.camunda.falcon.*`) is
  written as a fully-qualified name to keep the import block byte-clean
  against upstream diffs.
* `FalconJobWorker` lives in `io.camunda.client.impl.worker` because it
  needs package-private access — no upstream diff to reconcile.
