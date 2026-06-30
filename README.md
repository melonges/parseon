<p align="center">
  <img width="128" height="128" src="https://github.com/user-attachments/assets/c7c2198d-0be3-42c4-ba22-aa4f6d7e7e84" alt="Parseon logo" />
</p>

<h1 align="center">Parseon</h1>

<p align="center">
  <strong>Turn EVM calldata into queryable data.</strong><br />
  Parseon is a small, self-hosted EVM indexer built in Rust.
</p>

<p align="center">
  <img alt="Early development" src="https://img.shields.io/badge/status-early_development-151515?style=flat-square" />
  <img alt="Rust" src="https://img.shields.io/badge/Rust-151515?style=flat-square&logo=rust&logoColor=white" />
  <img alt="EVM" src="https://img.shields.io/badge/EVM-151515?style=flat-square&logo=ethereum&logoColor=white" />
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-151515?style=flat-square&logo=postgresql&logoColor=white" />
</p>

---

> [!WARNING]
> **Parseon is in early development.** Expect breaking changes, incomplete features, and evolving APIs and storage formats. It is not ready for production or critical workloads.

Define a monitor with a contract address and Solidity function signature. Parseon follows finalized blocks, decodes matching transactions, and stores their parameters in PostgreSQL for querying through its HTTP API.

## The direction

Parseon is being built to make focused onchain indexing simple: describe the calls you care about, run one service, and own the resulting data.

The project is moving quickly. Issues, ideas, and early contributions are welcome.
