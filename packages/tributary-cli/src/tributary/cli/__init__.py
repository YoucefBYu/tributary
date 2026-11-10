"""Tributary command line client. Everything goes through the control plane API; nothing here reads
PostgreSQL, Kafka or ClickHouse (invariant 3 stays a property of the data plane, not of this tool)."""
