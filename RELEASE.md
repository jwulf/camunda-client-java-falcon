# Releasing `camunda-client-java-falcon`

Tag-triggered publish to Maven Central (Central Portal). Merging to `main`
does **not** ship anything — a tag push is the explicit "yes, publish this"
gesture, and the [`maven-central` GitHub Environment][env] then requires a
reviewer approval before the secrets (Central token + GPG key) are exposed
to the workflow.

[env]: https://github.com/jwulf/camunda-client-java-falcon/settings/environments

## Prerequisites (one-time)

Secrets on the `maven-central` environment:

| Secret            | Source                                                                 |
|-------------------|------------------------------------------------------------------------|
| `CENTRAL_USERNAME`| User-token username from <https://central.sonatype.com/usertoken>      |
| `CENTRAL_PASSWORD`| User-token password (regenerate on rotation)                           |
| `GPG_PRIVATE_KEY` | `gpg --armor --export-secret-keys <keyid>`                             |
| `GPG_PASSPHRASE`  | Passphrase for the above key                                           |

The public half of the GPG key must be published to a keyserver Central
verifies against (`keys.openpgp.org` and `keyserver.ubuntu.com`).

## Cutting a release

From a clean `main` checkout with your working tree empty:

```bash
# 1. Confirm you're on main and up to date
git checkout main
git pull --ff-only

# 2. Drop -SNAPSHOT from pom.xml (edit or sed)
sed -i '' 's|<version>X.Y.Z-SNAPSHOT</version>|<version>X.Y.Z</version>|' pom.xml

# 3. Commit + tag + push both
git commit -asm "release X.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z — <one-line summary>"
git push origin main
git push origin vX.Y.Z

# 4. Bump to next -SNAPSHOT and push
sed -i '' 's|<version>X.Y.Z</version>|<version>X.Y.(Z+1)-SNAPSHOT</version>|' pom.xml
git commit -asm "back to snapshot: X.Y.(Z+1)-SNAPSHOT"
git push origin main
```

macOS `sed` needs `-i ''`; GNU sed on Linux uses `-i` (no arg).

## After pushing the tag

1. **Approve the deployment.** The `release` workflow will start and pause
   at the `maven-central` environment gate. Open the workflow run and
   click **Review deployments → Approve**.
2. **Watch the job.** It runs `mvn deploy` with the Central Portal plugin
   (`autoPublish=true`, `waitUntil=published`) — so a green job means the
   artifact is live on Maven Central within a few minutes, no manual
   staging-close step required.
3. **Verify.**
   ```bash
   curl -sfI https://repo1.maven.org/maven2/com/nanobpm/camunda/camunda-client-java-falcon/X.Y.Z/camunda-client-java-falcon-X.Y.Z.pom
   ```
   Indexing on <https://central.sonatype.com/artifact/com.nanobpm.camunda/camunda-client-java-falcon>
   can lag by 15–30 min after the artifact is queryable via the direct URL.

## Versioning

Semantic versioning against the public Java API:

* **Patch** — bugfix only, no new public types/methods.
* **Minor** — new public API additions (backwards-compatible).
  Example: `1.1.0` shipped the `NanoTransport` interface + factories.
* **Major** — removed or changed existing public API signatures.

If a release changes the `nano-bernd` dependency major or the minimum
Java version, that's a **major** bump.

## Coordinating with upstream Nano releases

This SDK ships independently of `processos/nano`, but the embedded
transport (`NanoTransport.embedded(...)`) delegates to
`io.github.jwulf:nano-bernd`, which is cut from the `nano-bpm` repo.
When embedded-side changes span both repos:

1. Release `nano-bernd` first (its own `RELEASE.md`).
2. Bump `nano-bernd.version` in this `pom.xml`.
3. Merge and follow the "Cutting a release" flow above.

## Rollback

Maven Central artifacts are immutable — you cannot delete or overwrite a
published version. If a release is broken, cut a new patch release with
the fix. In severe cases (leaked secret, license violation) contact
<central-support@sonatype.com> to request a takedown.
