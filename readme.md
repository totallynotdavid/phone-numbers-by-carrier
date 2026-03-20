# robot

Given a list of RUCs, this tool:

- Queries OSIPTEL
- Paginates through all results
- Aggregates carrier counts
- Writes results to CSV

Built for high-volume runs using multiple workers and sticky proxy sessions.

## Quickstart

```bash
uv run robot \
  --input sample.csv \
  --output out.csv \
  --workers 3 \
  --page-size 100 \
  --session-budget 4 \
  --wait-min-s 6 \
  --wait-max-s 10 \
  --ban-cooldown-s 180 \
  --env-file .env
```

## Setup

1. Install mise

```bash
curl https://mise.run | sh
```

2. Install toolchain and dependencies

```bash
mise install
uv sync
```

3. Install Chromium and unzip (Ubuntu)

```bash
sudo add-apt-repository ppa:xtradeb/apps -y
sudo apt update
sudo apt install -y chromium unzip
```

4. Install SeleniumBase driver

```bash
uv run sbase get uc_driver 146
```

## Environment

```bash
cp .env.example .env
```

### Required

```
GEONODE_USER=<value>
GEONODE_PASS=<value>
GEONODE_GATEWAY=fr|fr_whitelist|us|sg
GEONODE_TYPE=residential|datacenter|mix
GEONODE_COUNTRY=<country_code_or_empty>
GEONODE_LIFETIME=3..1440
```

### Optional

```
CHROME_BINARY=/absolute/path/to/chrome
```

<br/>

## Output

* Successful rows append to `out.csv`
* Failed rows append to `out.errors.csv`
* Existing RUCs in `out.csv` are skipped
* `out.csv` is validated on startup

  * invalid header, width, or types will fail fast

<br/>

## How it works

1. Load completed RUCs from `out.csv` as checkpoint
2. Start producer and N worker processes
3. Each worker owns one sticky proxy slot
4. Worker opens a browser session through that proxy
5. Browser generates reCAPTCHA token
6. httpx sends OSIPTEL POST through same proxy session
7. Pagination loops until all rows are retrieved
8. Results are aggregated and written to CSV
9. On failure or ban, worker rotates session and retries

<br/>

## Error handling and retries

* Each RUC can be retried up to 3 times
* On ban or transport failure:

  * browser session is closed
  * sticky session is released
  * a new session is created

<br/>

## Notes

* Browser is required for token generation
* Requests must reuse the same proxy session as the browser
* Headers and cookies are critical for successful requests
* Rate limiting and proxy quality directly affect success rate

<br/>

## Debugging

Enable structured logs:

```bash
uv run robot --input sample.csv --output out.csv --debug --env-file .env
```

<br/>

## Development

```bash
mise format
mise check
```

```
