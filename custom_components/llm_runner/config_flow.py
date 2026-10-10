"""Config flow for HA LLM Runner."""

from __future__ import annotations

from homeassistant import config_entries
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigFlowResult
import voluptuous as vol

from .const import DOMAIN


class LlmRunnerConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Set up the integration against the already-configured HA MQTT broker."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, str] | None = None
    ) -> ConfigFlowResult:
        """Offer one runner connection using Home Assistant's MQTT integration."""
        await self.async_set_unique_id("single_runner")
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")
        if user_input is not None:
            if not mqtt.is_connected(self.hass):
                return self.async_show_form(
                    step_id="user",
                    data_schema=vol.Schema({}),
                    errors={"base": "mqtt_unavailable"},
                )
            return self.async_create_entry(title="HA LLM Runner", data={})
        return self.async_show_form(step_id="user", data_schema=vol.Schema({}))
