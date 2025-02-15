"""Vk tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_vk import streams


class TapVk(Tap):
    """Vk tap class."""

    name = "tap-vk"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "group_id",
            th.IntegerType,
            title="Аккаунт в vk",
            description="Аккаунт в vk",
        ),
        th.Property(
            "app_id",
            th.IntegerType,
            title="Приложение в vk",
            description="Приложение в vk",
        ),
        th.Property(
            "params",
            th.ObjectType(
                th.Property(
                    "fields",
                    th.StringType,
                    title="Параметры запроса",
                    description="Параметры запроса",
                )
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.VkStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
           streams.GroupStatStream(self),
           streams.GroupsStream(self),
           streams.GroupPostsStream(self),
           streams.GroupPostsCommentsStream(self),
           #streams.StoryStream(self)
        ]


if __name__ == "__main__":
    TapVk.cli()
