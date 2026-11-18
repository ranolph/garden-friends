# Deploying Garden Friends

## Requirements

- Docker Engine and Docker Compose, or Python 3.11+ in an isolated environment.
- An existing InfluxDB 2.x server, organisation, bucket and suitable API token.
- An MQTT broker for live ingestion and Home Assistant discovery.
- BirdNET-Go or another compatible audio producer, and optionally Frigate.

The browser settings page configures all service connections. The deployment file controls the host port and persistent storage location.

## Docker Compose

```bash
docker compose up -d --build
docker compose logs garden-friends
```

Open `http://YOUR_SERVER:8090`, choose **Statistics → Settings**, and use the initial setup code from the logs to create your administrator.

The supplied deployment uses a named volume at `/data` for administrator state, connection settings and camera snapshots. The artwork folder is mounted read-only at `/app/species_art`. Removing or recreating the application container does not remove the named data volume.

If you need a different host port, copy `.env.example` to `.env` and change `GARDEN_FRIENDS_PORT`. Keep `.env` out of version control. On TrueNAS, store the project and persistent application data on a data pool, then deploy the provided Compose configuration through the supported application interface for your TrueNAS version.

## Service addresses

Use addresses reachable from the Garden Friends container. `localhost` inside the container refers to Garden Friends itself. Use your InfluxDB/MQTT server's LAN address or a Docker hostname on a shared network.

For Home Assistant image links, enter the public or LAN URL that Home Assistant can reach in **Public site URL**. The browser and Home Assistant can use different network paths; choose a URL appropriate for the consumers of those image links.

For Frigate, enter the address and token appropriate to the Frigate API you intend to use. Keep unprotected internal API ports on trusted networks.

## Read-only mode and ingestion

Garden Friends starts in read-only mode. It can query the existing InfluxDB catalogue without opening the ingestion subscription. Configure and test the database first, then enable ingestion when this is the instance intended to process incoming messages.

Do not run two independent copies of the ingestion bridge against the same incoming detection stream unless you deliberately want that behaviour. A separate read-only display is safe for checking the new UI against an existing catalogue.

## Updating

Keep a backup of the data volume and your artwork. Replace the repository code with the desired release, then rebuild and recreate the application:

```bash
docker compose up -d --build
docker compose logs --tail 50 garden-friends
```

Connection changes made in Settings do not require an image rebuild. Saving them triggers a graceful application-process restart inside the existing container.

## Health and troubleshooting

`/api/health` reports whether the application process is running, whether InfluxDB is configured, and MQTT state. It is not proof that detections are currently arriving or database writes are succeeding.

| Symptom | Check |
| --- | --- |
| Empty display on first startup | Configure InfluxDB, test its token/bucket, and choose a time window that contains detections. |
| InfluxDB connection test fails | Container-reachable URL, organisation/bucket spelling, token permissions and HTTPS trust. |
| MQTT stays disconnected | Broker address/port, credentials, TLS option and network reachability. |
| No audio detections | Actual producer topics, ingestion enabled, and source payload compatibility. |
| Frigate snapshots unavailable | Frigate API URL/token, camera event availability and the server's public URL. |
| Settings disappear after recreation | Ensure the same persistent `/data` volume is mounted. |
| A browser shows an older page | Refresh the page; on desktop, use a hard reload. |

Logs may contain the initial setup code. Treat them as administrative material until setup is complete.

## Python deployment

Install the requirements in a virtual environment. Run `python server.py`, not a second concurrent ingestion copy. Set `GARDEN_FRIENDS_DATA_DIR` to a private persistent folder if the default `data/` directory is unsuitable.

The supervisor forwards shutdown signals to the application, allows it to close MQTT/InfluxDB clients, and restarts it after a settings-save request. It does not require access to the Docker socket.
