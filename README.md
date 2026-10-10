# HA LLM Runner

An **optional companion integration** for the
[HA LLM Runner add-on](https://github.com/davidkirberich/ha-llm-runner-addon).
The add-on remains fully functional without this integration and owns task
configuration, LLM providers, execution, processors, memory, audit, and the web UI.
This integration will not duplicate those features.

## Status and planned behavior

**This repository is a scaffold only; it does not provide working functionality.**
The `llm_runner.execute` action and UI setup flow are **not implemented yet**.
There are no action handlers, entities, MQTT topics, or message formats.
The manifest declares a dependency on Home Assistant's existing MQTT integration;
`config_flow` remains disabled until the future UI flow is implemented.

Eventually, a response-capable `llm_runner.execute` action will start an add-on
task through Home Assistant's MQTT integration, asynchronously wait for its
correlated completion, and return the result through `response_variable`.
Implementation requires a coordinated protocol change in the add-on and will
be delivered separately.

## Planned installation

These are the intended distribution paths, not instructions to activate a
working integration today. Installing the scaffold files does not enable any
actions or setup flow. Do not add YAML configuration for this scaffold.

- **Manual:** copy `custom_components/llm_runner/` into
  `<config>/custom_components/llm_runner/`, then restart Home Assistant.
- **HACS:** add `https://github.com/davidkirberich/ha-llm-runner-hacs` as a custom
  repository of type **Integration**, download HA LLM Runner, then restart Home
  Assistant. This repository is not claimed to be in HACS's default catalog.

Once implemented, setup will use **Settings → Devices & services → Add integration**
and require the add-on and Home Assistant's MQTT integration to be configured.
No setup entry is available yet.

## License

[MIT](LICENSE).
