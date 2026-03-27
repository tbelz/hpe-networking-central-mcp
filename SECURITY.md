# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it
responsibly by emailing the maintainer directly. **Do not open a public issue.**

## Credential Handling

This MCP server requires API credentials to communicate with HPE Aruba
Networking Central and the HPE GreenLake Platform. Follow these guidelines:

- **Never commit `.env` files** — they are gitignored by default.
- Store credentials in `.env` or inject them as environment variables.
- Use the provided `.env.example` as a template.
- Rotate credentials if you suspect they have been exposed.

## Container Security

The Docker image does not embed any credentials. All secrets are provided
at runtime via environment variables or `--env-file`.
