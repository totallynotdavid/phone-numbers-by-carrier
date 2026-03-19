NAME
    robot - OSIPTEL RUC line counter with sticky GeoNode sessions

SYNOPSIS
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

PREREQUISITES
    1. Install mise
       curl https://mise.run | sh

    2. Toolchain and deps
       mise install
       uv sync

    3. Chromium and unzip (Ubuntu)
       sudo add-apt-repository ppa:xtradeb/apps -y
       sudo apt update
       sudo apt install -y chromium unzip

    4. SeleniumBase driver bundle (Chrome 146 in this repo)
       uv run sbase get uc_driver 146

ENVIRONMENT
    Copy template:
      cp .env.example .env

    Required:
      GEONODE_USER=<value>
      GEONODE_PASS=<value>
      GEONODE_GATEWAY=fr|fr_whitelist|us|sg
      GEONODE_TYPE=residential|datacenter|mix
      GEONODE_COUNTRY=<country_code_or_empty>
      GEONODE_LIFETIME=3..1440

    Optional:
      CHROME_BINARY=/absolute/path/to/chrome

OUTPUT
    - Success rows append to out.csv
    - Failure rows append to out.errors.csv
    - Existing RUCs already present in out.csv are skipped automatically
    - out.csv is validated on startup (header, row width, row types). Invalid file fails fast

HOW IT WORKS
    1. app.run loads completed RUCs from out.csv (checkpoint)
    2. dispatcher starts producer + N worker processes
    3. each worker owns one sticky slot (port 10000 + slot_id - 1)
    4. worker opens one browser session through that sticky proxy
    5. browser session generates captcha token for each page
    6. httpx sends OSIPTEL POST through the same proxy session
    7. paginator loops draw/start/length until all rows are read
    8. parser aggregates carrier counts and writer flushes rows
    9. on ban or transport failure, worker closes browser, releases sticky session, then retries with a new sticky session (up to 3 attempts per RUC)

NOTES FROM IMPLEMENTATION (GROUPED)
    Confirmed and still true:
    - Stable browser path uses SeleniumBase SB with uc=True, headed=True, xvfb=True, then activate_cdp_mode(home_url)
    - Readiness gate checks scripts count, grecaptcha object, and hidden recaptcha key
    - Token generation uses grecaptcha.execute(hiddenRecaptchaKey, {action: hiddenAction})
    - Request body must keep models[GoogleCaptchaTokenOLD] as empty string
    - GeoNode username format includes type/country/session and optional filters

    Historical but stale now:
    - "Use native UI-triggered AJAX capture only" is no longer the pipeline design
      Current design is browser token generation plus httpx POST through same proxy
    - "scripts/, tests/, .debug removed" is not currently true (.debug exists)

    Useful debugging observations:
    - Driver(uc=True).get() was less reliable than SB UC + CDP in this project
    - ReCAPTCHAv3Utils.request is closure-scoped and not callable from window
    - get_cookie_string may be empty while get_all_cookies still has required cookies

OPERATIONS
    Enable structured debug logs:
      uv run robot --input sample.csv --output out.csv --debug --env-file .env

    Run checks:
      mise format
      mise check
