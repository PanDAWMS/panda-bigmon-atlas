# Deployment configuration

Existing machines continue using `atlas/settings/local.py` until explicitly
switched. Configuration loading does not depend on development mode. Nothing
here starts workers, schedulers, migrations or changes a running deployment.

## Manual deployment

`config/common.env.example`, `local.env.example` and `secrets.env.example` were
converted from the supplied redacted configuration. All 14 database aliases are
preserved. `common.env` holds shared endpoints, schemas and accounts; `local.env`
holds paths and deployment choices; `secrets.env` holds credentials. Actual
`.env` files are ignored by Git and excluded from Docker builds.

Working copies already exist under `deploy/config/`. For another machine, copy
the three examples without the `.example` suffix into a protected directory,
such as `/etc/prodsys`, readable by the application account. Fill in **all** `***`
credentials, including those embedded in broker URLs. The loader rejects missing
or placeholder credentials. Do not copy one machine's paths without reviewing
them. In particular, the supplied local example enables development middleware
and fake login; production must explicitly use `PRODSYS_DEVELOPMENT=false`,
`PRODSYS_IS_TEST_SERVER=false` and an empty `PRODSYS_FAKE_LOGIN_AS_USER`.

Set `ATLAS_CONFIG_DIR=/etc/prodsys` in the environment of Apache/mod_wsgi,
Celery workers, beat and management/cron commands, then restart the relevant
process. For mod_wsgi set the process environment before importing settings;
request-level Apache `SetEnv` is not a substitute for the Python process's
`os.environ`. Keep the normal `DJANGO_SETTINGS_MODULE=atlas.settings`.

Resolution order:

```
code defaults < common.env < local.env < secrets.env < process environment
```

All three files are required when `ATLAS_CONFIG_DIR` is set. File-based mode
never falls back to `local.py`. To use injected environment variables only, set
`PRODSYS_CONFIG_MODE=environment` without `ATLAS_CONFIG_DIR`. To retain the old
behavior, leave both selectors unset (or select `PRODSYS_CONFIG_MODE=legacy`).

Validate files without Django, Oracle initialization or external connections:

```sh
ATLAS_CONFIG_DIR=/etc/prodsys python3 -c 'from atlas.configuration import load_environment; load_environment(); print("Configuration valid")'
```

## Variable format

Most settings retain their original Python name with a `PRODSYS_` prefix:
`PRODSYS_MY_SECRET_KEY`, `PRODSYS_JEDI_BASE_URL`, etc. Nested dictionaries use
`__FIELD`, for example `PRODSYS_IAM__SECRET` and `PRODSYS_ATLAS_OS__CA_CERT`.
Application code reads the resolved values from `django.conf.settings`; settings
construction uses `atlas.settings.runtime` to avoid circular Django imports.

* Booleans accept `true`, `false`, `1`, `0` (case insensitive).
* Lists use JSON, for example `PRODSYS_ADMIN_MAILS='["ops@example.org"]'`.
* Database fields use `DB_CONN_<ALIAS>_<FIELD>`; aliases retain underscores and
  become lowercase Django aliases. `DB_CONN_DEV_DB_WR_PASSWORD` is supported.
* `ENGINE` accepts the existing Django backend path. Alternatively `VENDOR`
  accepts `oracle`, `postgresql` or `sqlite`.
* `OPTIONS` and `TEST` database fields are JSON objects, e.g.
  `DB_CONN_DEFT_OPTIONS='{"threaded":true}'`.
* Files accept literal `KEY=value`, optional `export`, quoted values and comments.
  No shell commands, variable interpolation or multiline values are evaluated.
  Quote whitespace, `#` and JSON; escape single quotes using shell-style quoting.
* Duplicate keys within a file, unknown prefixed keys and malformed types fail
  startup. Error messages identify keys without printing their values.

`PRODSYS_LOG_MODE=console` removes file handlers, including dynamically registered
application loggers. The default `file` mode retains existing file logging.
`PRODSYS_DEBUG` can control Django debugging independently of development auth.
After Django configures logging, logger `atlas.settings.runtime` emits a DEBUG
message identifying the selected method: legacy `atlas.settings.local`,
environment files, or process environment only. It logs no configuration values.
In file mode this goes to `LOG_ROOT/logfile.atlas.settings.runtime`; in console
mode it goes to stderr.
`PRODSYS_ORACLE_CONFIG_DIR` is the directory containing `tnsnames.ora`; omit it to
retain the Oracle client default. `PRODSYS_USE_ORACLEDB` retains its existing role
of selecting whether to initialize the thick client.

AMI, CRIC, JIRA and iDDS endpoints now have variables too. Their existing values
remain code defaults; set `PRODSYS_AMI_API_V2_BASE_URL`,
`PRODSYS_AMI_API_V2_BASE_URL_REPLICA`, `PRODSYS_CRIC_BASE_URL`,
`PRODSYS_JIRA_AUTH_URL`, `PRODSYS_JIRA_ISSUE_URL`, or
`PRODSYS_IDDS_MESSAGING_HOST/PORT/USERNAME/QUEUE` to override them.

## OpenShift

Use the same common and secret files, replacing local.env with a reviewed copy
of `config/openshift-local.env.example`. Configure the actual Route host and CSRF
origin, mounted certificate/CA paths, Oracle client/TNS files and analysis scripts.
The app must be able to read these with OpenShift's assigned user ID.

Create resources by parsing the files into Kubernetes values. **Do not use
`--from-env-file` on these quoted files**: that command retains quotes, which
would corrupt JSON lists and credential values.

```sh
python3 tools/render_config.py /etc/prodsys/common.env prodsys-common | oc apply -f -
python3 tools/render_config.py /etc/prodsys/local.env prodsys-local | oc apply -f -
python3 tools/render_config.py /etc/prodsys/secrets.env prodsys-secrets --secret | oc apply -f -
```

Add this to each relevant workload's container specification:

```yaml
envFrom:
  - configMapRef:
      name: prodsys-common
  - configMapRef:
      name: prodsys-local
  - secretRef:
      name: prodsys-secrets
env:
  - name: PRODSYS_CONFIG_MODE
    value: environment
```

Later sources override earlier sources; explicit `env` entries override `envFrom`.
Do not set `ATLAS_CONFIG_DIR` in this environment-only arrangement. Helm can
render these same variable names from non-secret values and reference an
existing Secret. Certificates/private keys/proxies are separate mounted files;
public CA bundles can come from ConfigMaps. Restart workloads after changes.

This refactor does not replace the image's `top` entrypoint or Apache/OIDC setup.
Before deploying the app, provide a real web command and preserve the trusted
identity headers expected by the authentication backend. Keep image/dependency
upgrades and database changes separate from configuration extraction.

## Parallel operation

Switch an existing instance to file-based configuration first. Bring up the
OpenShift web workload on its own hostname, initially leaving workers, beat,
cron and message consumers on the existing deployment. Shared database/broker
connections mean requests can create real production work even when no new
workers are started. Preserve compatible task/application versions, database
schemas and any required shared files. Move background workloads individually;
there must be one active beat scheduler and one owner of each periodic job.
Retain compatible Django keys/session storage where shared sessions are needed.

Rollback a configuration-mode switch by restoring the legacy selectors and
restarting with the original local.py. Existing machines can stay in legacy mode
while OpenShift uses environment mode.

## Export another configuration

```sh
python3 tools/export_local_config.py /path/to/local.py /new/protected/directory
```

The exporter accepts literal assignments only and never executes Python. It
creates files with mode 0600 and refuses overwrites. Review its classification;
never commit files generated from a configuration containing real credentials.

## Checks

```sh
python3 -m unittest discover -s tests/configuration -v
```

These checks exercise both loading modes without databases or network services.
