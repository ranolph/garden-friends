# Contributing

Keep changes small enough to review and explain the user-visible result. For a bug report, include the Garden Friends version, browser, deployment method and the steps that reproduce it. Remove credentials, private camera images and sensitive network details first.

Run the Python checks from the repository root:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q tests
```

Validate JavaScript syntax after editing the settings page:

```bash
node --check static/garden-settings.js
```

Changes to observation windows need timezone and daylight-saving examples. Changes to settings must preserve authentication, secret redaction and the distinction between reading detections and enabling ingestion. Artwork filenames and aliases are part of the application contract; include corresponding masks or metadata changes when required.

See [NOTICE.md](NOTICE.md) before contributing code or artwork. A contribution licence has not yet been selected by the maintainer.
