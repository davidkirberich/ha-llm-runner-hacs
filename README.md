# HA LLM Runner — Home Assistant integration

This is an **optional** HACS companion for the [HA LLM Runner add-on](https://github.com/davidkirberich/ha-llm-runner-addon). It adds the response-only Home Assistant action `llm_runner.execute`; the add-on continues to work without this integration.

## Requirements

- Home Assistant **2025.3.0 or newer**.
- The HA LLM Runner add-on **1.5.0 or newer** (MQTT protocol v1).
- Home Assistant's built-in MQTT integration configured, connected to the same broker as the add-on.

The integration reuses Home Assistant's MQTT connection. It does not ask for, store, or publish broker credentials. It creates no duplicate entities and starts no subprocess.

The integration includes the add-on's original icon and logo in
`custom_components/llm_runner/brand/`. Home Assistant 2026.3 and newer supports these
local brand images; older supported versions may show a generic integration icon.

## Install with HACS

1. In HACS, open **Integrations** and select the three-dot menu → **Custom repositories**.
2. Add `https://github.com/davidkirberich/ha-llm-runner-hacs` with category **Integration**.
3. Download **HA LLM Runner**, then restart Home Assistant.
4. In **Settings → Devices & services → Add integration**, search for **HA LLM Runner**.
5. Confirm the integration is connected. It waits up to 20 seconds for the add-on's online availability and a valid task catalog from the same runner session. If either is unavailable, setup reports an error; check the add-on and MQTT connection, then reload the integration.

The setup form has no broker fields because the existing MQTT integration provides the connection. Only one runner entry is supported.

## Execute a task and capture its response

Task selection uses a **dropdown** populated from the runner's live MQTT catalog.
Options show the task IDs, which are also what the saved YAML contains (`task_id`).
Added, renamed and removed tasks update the action description automatically through
Home Assistant's service-description API. Reopen an already-open action editor if
its options have not refreshed. The list clears while the runner is unavailable.
The integration also validates the selected ID against the current catalog before publishing.

For example, if a task named `weather_summary` returns an object with a `summary` field:

```yaml
sequence:
  - action: llm_runner.execute
    data:
      task_id: weather_summary
      timeout: 180
      variables:
        location: "{{ states('weather.home') }}"
    response_variable: weather_run

  - action: persistent_notification.create
    data:
      title: Weather summary
      message: "{{ weather_run.result.summary }}"
```

`response_variable` is optional. Set it when a later automation step needs the response; it receives a mapping with `task_id`, `run_id`, `result`, `queued_at`, `finished_at`, and `duration`, with `started_at` and `request_id` included when available. If omitted, the action still waits for the task to finish but discards its response. Errors, timeouts, disconnects, runner restarts, invalid protocol messages, and unknown task IDs fail the action instead of returning a misleading success.

Supported action data:

| Field | Meaning |
| --- | --- |
| `task_id` | Required exact task ID from the runner's live catalog. |
| `timeout` | Positive finite number of seconds; defaults to `180`. Includes time queued at the add-on. A timeout stops waiting only; it does **not** cancel the task. |
| `files` | Optional mapping of configured file-input aliases to paths, for example `image: /config/www/weather/latest.jpg`. |
| `variables` | Optional mapping of configured variable aliases to string values. |
| `request_id` | Optional non-empty string of at most 4096 characters, echoed for caller-side correlation. |

## Protocol and lifecycle behavior

The integration subscribes to fixed topics through Home Assistant's MQTT API and uses QoS 1, non-retained request publishes:

- Request: `ha_llm_runner/run/<task_id>` with a newly generated UUID-hex `run_id`.
- Correlated lifecycle replies: non-retained `ha_llm_runner/events`.
- Runner availability: retained `ha_llm_runner/availability`.
- Public task catalog: retained `ha_llm_runner/tasks`.
- Per-task retained lifecycle status: `ha_llm_runner/status/<task_id>`. This is informational only and is never used as a correlated response.

Replies are accepted only for the matching `run_id`, `task_id`, and current runner `session_id`. Retained event messages are ignored. Availability and catalog may arrive in either order, but must identify the same session; stale catalogs from another session cannot make setup ready. A disconnect, offline status, or runner session change fails pending calls explicitly. Duplicate-run handling and its bounded process-lifetime cache are owned by the add-on; the integration never retries or resubmits a request.

Invocation files and variables use the add-on's configured input aliases. The add-on remains responsible for validating those inputs, task IDs, and request IDs. A caller's timeout does not cancel or otherwise change an invocation already queued by the add-on.

## Development checks

### Direct deployment over SSH

For development, use the Windows PowerShell script (OpenSSH Client and `tar` required):

```powershell
.\scripts\deploy-local.ps1 -CheckOnly
.\scripts\deploy-local.ps1
.\scripts\deploy-local.ps1 -Remove
```

The default connection is `root@homeassistant.local`, port 22. Override it with
`-HostName`, `-User` or `-Port`. The SSH environment must expose the HA configuration
folder as `/homeassistant` or `/config` and provide Bash, tar, mktemp and jq.

Deployment copies only this integration to `custom_components/llm_runner`.
The deployed manifest is automatically labelled `<version>-preview.<commit>`,
with `.dirty` appended if the checkout has uncommitted changes. For example,
`1.0.0-preview.6394746.dirty`. The repository manifest is not changed. This marks
manual builds independently of the current Git branch; HACS releases retain their
normal version. The label appears after the manual Core restart.
Existing files are backed up under `llm_runner_deploy_backups/` in the HA configuration
folder. **The script never restarts Home Assistant.** Restart Core manually after
deployment, then add the integration under **Settings > Devices & services** on
first installation. Later deployments preserve the existing integration entry.

Before `-Remove`, delete the integration entry under **Settings > Devices & services**.
The script refuses removal while an entry remains, then moves the integration files
to a backup and asks you to restart Core manually. It never edits `.storage`,
automations or the add-on. Automations using `llm_runner.execute` must be adapted
before removing the integration. Backups are not automatically deleted. HACS updates
can overwrite directly deployed code; use one update method at a time.

Script checks (mocked deployment/removal; no HA files changed):

```powershell
.\tests\test-deploy-local.ps1
```

The test suite uses Python's standard `unittest` runner and an injectable in-memory MQTT boundary; it does not require a broker or Home Assistant installation:

```powershell
python -m unittest discover -s tests -v
```

Home Assistant Core's full runtime and its MQTT service are not installed or runnable in this Windows checkout. The focused tests exercise protocol validation, correlation, immediate replies, concurrent calls, failure paths, ordering, and cleanup with a fake broker. Validate the integration in a supported Home Assistant environment before deployment.

## License

Starting with release 1.0.0, this project uses the [MIT License](LICENSE).
