# Mirth / OIE channel samples

Drop **anonymized** channel and code-template exports (XML) here so the `migration-engineer` can size the migration effort (Phase 0, spike S6) and test the importer.

Rules:

- Everything in this directory except this README is git-ignored.
- Remove credentials, hostnames, IP addresses and any patient data before adding a file. Channel exports can contain connection strings and passwords.
- Never commit derived files that still contain sensitive configuration. Derived fixtures must be synthetic.
- If no samples are provided, spike S6 works from public documentation of the channel format only, and its estimates are marked low confidence.
