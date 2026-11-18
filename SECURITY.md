# Security

Garden Friends has a public, read-only display and a password-protected settings area.

## Administration

The first administrator is created with a one-time setup code printed to the application logs and stored in the private data directory. Use that code at **Statistics → Settings**, then choose a password of at least 12 characters. Connection tokens are never sent back to the browser after saving. Leaving a secret field blank keeps its saved value; **Clear** explicitly removes it.

Settings, the administrator password hash, session signing material and camera snapshots belong in the persistent data volume. Keep that volume private and back it up separately. None of it belongs in a public Git repository.

Use HTTPS when accessing settings outside a trusted local network. Put public deployments behind a maintained reverse proxy with TLS. The supplied container does not open a router port or create a tunnel.

## Connection access

An authenticated administrator can configure and test internal service URLs. Treat administrator access as access to the application network. InfluxDB needs read permission to display existing detections and write permission only when ingestion is enabled. MQTT and Frigate credentials should be scoped to the services required by this installation.

## Reporting a problem

Contact the maintainer privately through the contact options on [their GitHub profile](https://github.com/ranolph) before publishing a report containing credentials, private addresses, snapshots or an exploitable vulnerability. Do not place secrets in a public issue.
