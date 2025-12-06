<p align="center">
  <img src="assets/logo_blue.png" width="300" />
</p>

<p align="center">
  <a href="LICENSE.md" target="_blank">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue" />
  </a>

  <img src="https://img.shields.io/badge/status-alpha-orange" />
  
  <img src="https://img.shields.io/badge/language-Swift-F05138" />

  <img src="https://img.shields.io/badge/platform-Linux-lightgrey" />
</p>

# Alarik - a High-Performance S3-Compatible Object Storage

Alarik is a high-performance, S3-compatible object storage system written in **Swift**, licensed under the **Apache 2.0** license. It aims to deliver exceptional speed, developer-friendly ergonomics, and a modern cloud-native core.

## Why Alarik?

Recent shifts in the ecosystem-especially surrounding MinIO-have revealed how fragile it is to depend on a single “reference” implementation for S3-compatible storage. These changes highlighted structural, licensing, and philosophical issues that many teams had long overlooked.

**Alarik exists to provide a modern, transparent, community-driven alternative.**
Developers and organizations need an S3-compatible store that is fast, simple to operate, easy to extend, and genuinely open-source. No licensing traps, no moving goalposts.

The goal: a self-hosted, high-speed S3 system built for today’s workloads, without the enterprise upsell.

## Why Swift?

Swift brings a rare combination of **performance**, **safety**, and **developer ergonomics**:

-   **Fast**: Compiles to highly optimized native code with performance comparable to Rust or Go.
-   **Safe**: Memory-safe by design, eliminating entire classes of vulnerabilities common in C/C++ ecosystems.
-   **Modern tooling**: Clear syntax, excellent async/await model, first-class concurrency, and mature package management.
-   **Great for systems programming**: SwiftNIO and related libraries provide extremely efficient networking and I/O foundations.

Swift is an ideal fit for a new generation of cloud-native storage software.

## Future of Alarik

We are the ones behind the German Accounting-Software [belegFuchs](https://belegfuchs.de), and although we currently run MinIO in production, we are planning to migrate to Alarik in the future. This isn’t a marketing slogan - it’s a commitment that directly shapes our roadmap.

Because we rely on S3-compatible storage every day, we are fully invested in ensuring that Alarik continues to evolve: solid performance, long-term stability, and an open development model without licensing uncertainty. Our own planned adoption is a practical reason why we are committed to keeping Alarik actively maintained and moving forward.

**TL;DR:** Alarik is here to stay - it’s not going anywhere.

## Performance

Initial benchmarks on a **MacBook Pro M4 Pro (12-core)** show Alarik outperforming MinIO and even other high-performance storage projects like **RustFS** (in comparable configurations).

Detailed benchmark results will be published soon.

## State of Development

Alarik is currently in **Alpha** - under rapid, active development.

-   Expect breaking changes and incomplete feature sets.
-   Core S3-compatible storage functionality is already operational.
-   Stabilization, documentation, and production-grade hardening are in progress.

Contributions, issues, feedback, and real-world testing are **highly encouraged**.

## Contributing

We welcome contributions of any size. Please:

-   Use clear, descriptive commit messages
-   Open an issue before starting larger work
-   Follow Swift best practices
-   Add tests for new functionality where appropriate
-   Keep pull requests focused and incremental

More detailed contributing guidelines will be added soon.

## Running Alarik

You can run Alarik in **development** or **production** mode.
All modes are available via Docker Compose, and development mode can also run natively via Swift.

### Development Mode

Development mode is optimized for local iteration. It starts the backend, the console UI, and any supporting services.

#### Option 1 - Run via Docker Compose

```bash
docker compose -f docker-compose.dev.yml up --build -d
```

This starts the full development environment, including the API server and the web console.

#### Option 2 - Run the Swift server locally

```bash
cd alarik
swift run
```

> **Note:** Native builds are **not supported on Windows**.
> On Windows, please use Docker (Option 1).

#### Running the Console UI (local dev - Nuxt)

```bash
cd console
npm install
npm run dev
```

This starts the development UI with hot reloading.

### Testing

Testing with `swift test` will fail. Please use `test.sh`. All tests need to pass before committing.

### Production Mode

For production deployments, use the hardened Compose configuration:

```bash
docker compose -f docker-compose.deploy.yml up --build -d
```

This launches a production-ready instance with optimized settings and persistent data volumes.

---

## ⭐️ Stay Updated

More documentation, benchmarks, SDKs, and deployment guides are on the way.

If you believe in a future of open, community-driven, high-performance object storage, consider giving the repo a ⭐ and contributing!
