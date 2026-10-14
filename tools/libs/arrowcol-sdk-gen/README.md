# Arrowcol SDK generation tool

Thin TypeScript workflow wrapper around the single Rust generator pipeline. It
does not parse or transform catalog schemas itself.

```bash
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts generate-fixture
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts check
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts compatibility
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts test
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts package-artifact
```

The `test` workflow copies generated packages into `target/` before invoking
Cargo, preventing validation-created lockfiles or build state from changing the
generated artifact directory.
